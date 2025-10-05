from typing import TYPE_CHECKING, Any

from escmodels.base.entity import AnyEntityConfig
from gamemaster.ents.base import GameElement
from gamemaster.stage_orchestrator import StageOrchestrator, PuzzleOrchestrator
from gamemaster.puzzles import DigitalState, SpeechDetection, Sequence
import escmodels.base as base

if TYPE_CHECKING:
    from gamemaster.room_orchestrator import RoomOrchestrator


class RoomBuilder:
    def __init__(self, room_orchestator: "RoomOrchestrator"):
        self.ro = room_orchestator

    def generate_stages_from_json(self, stages_data: list[dict[str, Any]]):
        return [self.generate_stage(stage_data) for stage_data in stages_data]

    def generate_stage(self, stage: dict[str, Any]) -> StageOrchestrator:
        puzzles = stage["puzzles"]
        slug = stage["slug"]
        return StageOrchestrator(
            self.ro,
            slug,
            [self.generate_puzzle(puzzle_data) for puzzle_data in puzzles],
        )

    def generate_puzzle(self, puzzle: dict[str, Any]) -> PuzzleOrchestrator:
        type: str = puzzle["type"]
        slug: str = puzzle["slug"]
        match type:
            case "digitalState":
                name_map: dict[str, str] = puzzle["name_map"]
                return DigitalState(self.ro, slug, list(name_map.keys()))
            case "sequence":
                target_sequence: list[str] = puzzle["target_state"]
                return Sequence(self.ro, slug, target_sequence)
            case "speechDetection":
                return SpeechDetection(self.ro, slug)
            case _:
                raise TypeError(f"Unrecognized puzzle type {type} for {slug}")


class PydanticRoomBuilder:
    def __init__(self, room_orchestator: "RoomOrchestrator"):
        self.ro = room_orchestator

    def generate_stages_from_json(self, stages_data: list[base.StageConfig]):
        return [self.generate_stage(stage_data) for stage_data in stages_data]

    def generate_stage(self, stage: base.StageConfig) -> StageOrchestrator:
        puzzles = [self.generate_puzzle(puzzle_data) for puzzle_data in stage.puzzles]
        return StageOrchestrator(self.ro, stage.slug, puzzles)

    def generate_puzzle(self, puzzle: base.AnyPuzzleConfig) -> PuzzleOrchestrator:
        match puzzle.type:
            case base.PuzzleType.DIGITAL_STATE:
                name_map: dict[str, str] = puzzle.name_map
                return DigitalState(self.ro, puzzle.slug, list(name_map.keys()))
            case base.PuzzleType.SEQUENCE:
                target_sequence: list[str] = puzzle.extras.target_state
                return Sequence(self.ro, puzzle.slug, target_sequence)
            case base.PuzzleType.SPEECH_DETECTION:
                return SpeechDetection(self.ro, puzzle.slug)
        raise ValueError(f"Puzzle type {puzzle.type} not supported.")

    def generate_entity(self, entity: AnyEntityConfig) -> GameElement:
        pass