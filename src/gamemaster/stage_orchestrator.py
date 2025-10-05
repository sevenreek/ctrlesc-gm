import asyncio
from enum import StrEnum
from typing import Any, TYPE_CHECKING

from gamemaster.ents.base import MQTTMessageHandlerType, LifecycleElement
from gamemaster.log import log
from gamemaster.ents import GameElement, events
from gamemaster.puzzles import PuzzleOrchestrator


if TYPE_CHECKING:
    from gamemaster.room_orchestrator import RoomOrchestrator


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
                events.EVENT_COMPLETED,
                self.on_puzzle_complete
            )
            puzzle.set_event_handler(
                events.EVENT_STATE_CHANGED,
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

    async def on_puzzle_state_changed(self, puzzle: PuzzleOrchestrator, detail: dict[str, Any]):
        await self.room_orchestrator.update_puzzle(
            self.slug, puzzle.element_slug, state=detail
        )
