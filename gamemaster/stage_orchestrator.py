import asyncio
from types import CoroutineType
from log import log
from enum import StrEnum
from typing import Any
from collections import defaultdict
from itertools import chain
from functools import cached_property
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from room_orchestrator import RoomOrchestrator


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
        room_orchestrator: "RoomOrchestrator",
    ):
        self.room_orchestrator = room_orchestrator
        self._registered_handlers: list[MQTTMessageHandler] = []

    def reset_internal_state(self):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def reset(self):
        self.reset_internal_state()

    @property
    def room_slug(self):
        return self.room_orchestrator.settings.room_slug

    @property
    def room_key(self):
        return f"room:{self.room_slug}"

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
        room_orchestrator: "RoomOrchestrator",
        element_slug: str,
    ):
        super().__init__(room_orchestrator)
        self.element_slug = element_slug

    @cached_property
    def base_topic(self):
        return f"room/{self.room_slug}/{self.element_slug}"

    @cached_property
    def status_topic(self):
        return f"{self.base_topic}/status"


class GameObject(GameElement):
    pass


class PuzzleOrchestrator(GameElement):
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
        element_slug: str,
    ):
        super().__init__(room_orchestrator, element_slug)
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
            await self.trigger_event(PuzzleOrchestrator.Events.EVENT_COMPLETED, True)

    def set_event_handler(self, event_type: Events, coro: CoroutineType):
        self.event_handlers[str(event_type)] = coro

    async def skip(self):
        await self.complete()

    async def reset(self):
        self.completed = False


class StageOrchestrator(LifecycleElement):
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
        stage_slug: str,
        puzzles: list[PuzzleOrchestrator],
    ):
        super().__init__(room_orchestrator)
        self.slug = stage_slug
        self.puzzles = puzzles
        for puzzle in self.puzzles:
            puzzle.set_event_handler(
                PuzzleOrchestrator.Events.EVENT_COMPLETED, self.on_puzzle_complete
            )
            puzzle.set_event_handler(
                PuzzleOrchestrator.Events.EVENT_STATE_CHANGED,
                self.on_puzzle_state_changed,
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

    async def on_puzzle_complete(self, puzzle: PuzzleOrchestrator, detail: bool):
        await self.room_orchestrator.update_puzzle(
            self.slug, puzzle.element_slug, completed=detail
        )
        all_completed = all((puzzle.completed for puzzle in self.puzzles))
        if all_completed:
            await self.complete()

    async def on_puzzle_state_changed(self, puzzle: PuzzleOrchestrator, detail: dict):
        await self.room_orchestrator.update_puzzle(
            self.slug, puzzle.element_slug, state=detail
        )
