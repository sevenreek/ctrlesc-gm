import asyncio
from abc import ABC, abstractclassmethod, abstractmethod, abstractproperty
from uuid import uuid4
import signal
from gmqtt import Client as MQTTClient, constants as MQTT
from os import getpid
from settings import Settings
import redis.asyncio as redis
from types import CoroutineType
from log import log
from datetime import datetime
from weakref import ReferenceType
from enum import StrEnum
from typing import Any
from collections import defaultdict
from itertools import chain
from functools import cached_property
import json


class MQTTMessageHandler:
    def __init__(self):
        self.handlers: dict[str, list[CoroutineType]] = defaultdict(list)

    def add_handler(self, topic: str, coro: CoroutineType):
        self.handlers[topic].append(coro)

    def remove_handler(self, topic: str, coro: CoroutineType):
        self.handlers.get(topic, []).remove(coro)

    async def handle(self, topic: str, payload: bytes):
        handlers_for_topic = list(
            chain(
                *(
                    handler
                    for handler_topic, handler in self.handlers.items()
                    if topic.startswith(handler_topic)
                )
            )
        )
        log.info(
            f"Received message in topic {repr(topic)} for {len(handlers_for_topic)} handlers."
        )
        await asyncio.gather(
            *(handler(topic, payload) for handler in handlers_for_topic)
        )


class LifecycleElement:
    def __init__(
        self,
        room_orchestrator_ref: ReferenceType["RoomOrchestrator"],
    ):
        self._room_orchestartor_ref = room_orchestrator_ref
        self._registered_handlers: list[MQTTMessageHandler] = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def reset(self):
        pass

    @cached_property
    def room_slug(self):
        return self.room_orchestrator.settings.room_slug

    @cached_property
    def room_key(self):
        return f"room:{self.room_slug}"

    @property
    def room_orchestrator(self):
        ro = self._room_orchestartor_ref()
        return ro

    @property
    def mqtt(self):
        return self.room_orchestrator.mqtt

    @property
    def mqtt_handler(self):
        return self.room_orchestrator.mqtt_handler

    @property
    def redis(self):
        return self.room_orchestrator.redis


class GameElement(LifecycleElement):
    def __init__(
        self,
        room_orchestrator_ref: ReferenceType["RoomOrchestrator"],
        element_slug: str,
    ):
        super().__init__(room_orchestrator_ref)
        self.element_slug = element_slug

    @cached_property
    def base_topic(self):
        return f"room/{self.room_slug}/{self.element_slug}"

    @cached_property
    def status_topic(self):
        return f"{self.base_topic}/status"


class GameObject(GameElement):
    pass


class Puzzle(GameElement):
    def __init__(
        self,
        room_orchestrator_ref: ReferenceType["RoomOrchestrator"],
        element_slug: str,
    ):
        super().__init__(room_orchestrator_ref, element_slug)
        self.event_handlers: dict[str, CoroutineType] = {}
        self.completed = False

    class Events(StrEnum):
        EVENT_STATE_CHANGED = "state"
        EVENT_COMPLETED = "completed"

    async def trigger_event(self, event_type: Events, detail: Any = None):
        try:
            await self.event_handlers[str(event_type)](self, detail)
        except (KeyError, TypeError):
            pass

    async def complete(self):
        prev_completed = self.completed
        self.completed = True
        if not prev_completed:
            log.info(f"Puzzle {repr(self.element_slug)} was completed.")
            await self.trigger_event(Puzzle.Events.EVENT_COMPLETED)

    def set_event_handler(self, event_type: Events, coro: CoroutineType):
        self.event_handlers[str(event_type)] = coro


class Stage(LifecycleElement):
    def __init__(
        self,
        room_orchestrator_ref: ReferenceType["RoomOrchestrator"],
        stage_slug: str,
        puzzles: list[Puzzle],
    ):
        super().__init__(room_orchestrator_ref)
        self.slug = stage_slug
        self.puzzles = puzzles
        for puzzle in self.puzzles:
            puzzle.set_event_handler(
                Puzzle.Events.EVENT_COMPLETED, self.on_puzzle_complete
            )
            puzzle.set_event_handler(
                Puzzle.Events.EVENT_STATE_CHANGED, self.on_puzzle_state_changed
            )

    async def start(self):
        await asyncio.gather(*(p.start() for p in self.puzzles))

    async def stop(self):
        await asyncio.gather(*(p.stop() for p in self.puzzles))

    async def reset(self):
        await asyncio.gather(*(p.reset() for p in self.puzzles))

    async def complete(self):
        log.info(f"Stage {repr(self.slug)} was completed.")
        await self.room_orchestrator.finish_stage(self.slug)
        update_topic = (
            f"room/completion/{self.room_orchestrator.settings.room_slug}/{self.slug}"
        )
        await self.redis.publish(update_topic, int(True))

    async def on_puzzle_complete(self, puzzle: Puzzle, detail: Any):
        await self.redis.json().set(
            self.room_key,
            f'$.stages[?(@.slug=="{self.slug}")].puzzles[?(@.slug =="{puzzle.element_slug}")].completed',
            True,
        )
        update_topic = f"room/completion/{self.room_orchestrator.settings.room_slug}/{self.slug}/{puzzle.element_slug}"
        await self.redis.publish(update_topic, int(True))
        all_completed = all((puzzle.completed for puzzle in self.puzzles))
        if all_completed:
            await self.complete()

    async def on_puzzle_state_changed(self, puzzle: Puzzle, detail: Any):
        await self.redis.json().set(
            self.room_key,
            f'$.stages[?(@.slug=="{self.slug}")].puzzles[?(@.slug=="{puzzle.element_slug}")].state',
            detail,
        )
        update_topic = f"room/state/{self.room_orchestrator.settings.room_slug}/{self.slug}/{puzzle.element_slug}"
        await self.redis.publish(update_topic, json.dumps(detail))


class RoomOrchestrator(ABC):
    def __init__(self, settings: Settings):
        self.settings = settings
        self._active_stage_index: int | None = None
        self._stages: list[Stage] = []
        self._game_objects: list[GameObject] = []
        self._loop = asyncio.get_event_loop()
        self.mqtt = MQTTClient(f"{self.settings.room_slug}-{getpid()}")
        self.mqtt.on_message = self.on_message
        self.redis = redis.Redis(
            host=self.settings.redis_url,
            port=int(self.settings.redis_port),
            decode_responses=True,
            encoding="utf-8",
        )
        self.running = False
        self.mqtt_handler = MQTTMessageHandler()
        self._loop.add_signal_handler(signal.SIGTERM, self._stop_signal)
        self._loop.add_signal_handler(signal.SIGINT, self._stop_signal)

    @property
    def active_stage(self) -> Stage:
        try:
            return self._stages[self._active_stage_index]
        except (IndexError, TypeError):
            return None

    async def add_game_element(self, ge: GameObject):
        self._game_objects.append(ge)
        await ge.start()

    async def remove_game_element(self, ge: GameObject):
        self._game_objects.remove(ge)
        await ge.stop()

    async def finish_stage(self, stage_slug: str) -> None:
        if stage_slug != self.active_stage.slug:
            raise ValueError(
                f"Stage {self.active_stage.slug} is not active. Cannot finish."
            )
        await self.load_stage(self._active_stage_index + 1)

    async def load_stage(self, stage_index: int) -> None:
        if self.active_stage:
            await self.active_stage.stop()
        self._active_stage_index = stage_index
        await self.active_stage.start()

    def start_loop(self, stage_list: list[Stage], *, from_stage: int = 0) -> None:
        self._stages = stage_list
        self.running = True
        self._loop.run_until_complete(self.start_mqtt())
        self._loop.run_until_complete(self.start())
        self._loop.run_until_complete(self.load_stage(from_stage))
        log.info(f"Starting loop for {self.settings.room_slug}.")
        self._loop.create_task(self.health_check_update())
        self._loop.run_forever()

    def _stop_signal(self):
        log.error("Received stop signal.")
        self.stop_loop()

    async def stop(self):
        log.info("Stopping.")
        await self.mqtt.disconnect()
        self._loop.stop()
        log.info("Stopped.")

    def stop_loop(self) -> None:
        task = asyncio.create_task(self.stop())

    async def start_mqtt(self) -> None:
        await self.mqtt.connect(self.settings.mqtt_url, int(self.settings.mqtt_port))
        self.mqtt.subscribe(f"room/{self.settings.room_slug}/#")

    async def start(self) -> None:
        pass

    async def on_message(
        self, client: MQTTClient, topic: str, payload: bytes, qos: int, properties
    ):
        log.debug(topic, payload.decode())
        await self.mqtt_handler.handle(topic, payload)
        return MQTT.PubAckReasonCode.SUCCESS

    async def health_check_update(self):
        topic = f"room/health/{self.settings.room_slug}"
        now = datetime.now().isoformat()
        while self._loop.is_running():
            await self.redis.set(topic, now)
            await self.redis.publish(topic, now)
            await asyncio.sleep(self.settings.health_check_period / 1000.0)
