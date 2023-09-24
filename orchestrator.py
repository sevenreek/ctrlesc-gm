import asyncio
from abc import ABC, abstractclassmethod, abstractmethod, abstractproperty
from typing import Any, Callable
from collections import defaultdict
from uuid import uuid4
from queue import PriorityQueue
import signal
from gmqtt import Client as MQTTClient, constants as MQTT
from os import getpid
from settings import Settings
import redis.asyncio as redis
from types import CoroutineType


class EventHook:
    def __init__(self, event: str, handler_coro: CoroutineType):
        self._event = event
        self._handler = handler_coro
        self._uuid = uuid4()
        self._valid = True

    def invalidate(self):
        self._valid = False

    @property
    def uuid(self):
        return self._uuid

    @property
    def event(self):
        return self._event

    @property
    def valid(self):
        return self._valid

    async def call(self, topic: str, payload: bytes):
        await self._handler()


class MQTTMessageHandler:
    def __init__(self):
        self.handlers: list[EventHook] = []

    def add_handler(self, topic: str, coro: CoroutineType) -> EventHook:
        hook = EventHook(topic, coro)
        self.handlers.append(hook)
        return hook

    def remove_handler(self, hook: EventHook):
        self.handlers.remove(hook)
        hook.invalidate()

    async def handle(self, topic: str, payload: bytes):
        handlers_for_topic = (h for h in self.handlers if topic.startswith(h.event))
        await asyncio.gather(
            *(handler(topic, payload) for handler in handlers_for_topic)
        )


class StageOrchestrator(ABC):
    async def start(self):
        pass

    async def exit(self):
        pass


class RoomOrchestrator(ABC):
    def __init__(self, stage_list: list[StageOrchestrator], settings: Settings):
        self.settings = settings
        self.active_stage_index: int | None = None
        self.stage_list: list[StageOrchestrator] = stage_list
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
    def active_stage(self) -> StageOrchestrator:
        try:
            return self.stage_list[self.active_stage_index]
        except (IndexError, TypeError):
            return None

    async def finish_stage(self) -> None:
        await self.load_stage(self.active_stage_index + 1)

    async def load_stage(self, stage_index: int) -> None:
        if self.active_stage:
            await self.active_stage.exit()
        self.active_stage_index = stage_index
        await self.active_stage.start()

    def start_loop(self, *, from_stage: int = 0) -> None:
        self.running = True
        self._loop.run_until_complete(self.start())
        self._loop.run_until_complete(self.load_stage(from_stage))
        self._loop.run_forever()

    def _stop_signal(self):
        self.stop_loop()

    async def stop(self):
        await self.mqtt.disconnect()
        self._loop.stop()

    def stop_loop(self) -> None:
        task = asyncio.create_task(self.stop())

    async def start(self) -> None:
        await self.mqtt.connect(self.settings.mqtt_url, int(self.settings.mqtt_port))
        self.mqtt.subscribe(f"rooms/{self.settings.room_slug}/#")

    async def on_message(
        self, client: MQTTClient, topic: str, payload: bytes, qos: int, properties
    ):
        print(topic, payload.decode())
        await self.mqtt_handler.handle(topic, payload)
        return MQTT.PubAckReasonCode.SUCCESS
