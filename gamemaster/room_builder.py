from stage_orchestrator import StageOrchestrator, PuzzleOrchestrator
from typing import TYPE_CHECKING
from puzzles import DigitalState, SpeechDetection, Sequence
from escmodels.room import StageConfig
from escmodels.puzzle import AnyPuzzleConfig, PuzzleType

if TYPE_CHECKING:
    from room_orchestrator import RoomOrchestrator


class RoomBuilder:
    def __init__(self, room_orchestator: "RoomOrchestrator"):
        self.ro = room_orchestator

    def generate_stages_from_json(self, stages_data: list[dict]):
        return [self.generate_stage(stage_data) for stage_data in stages_data]

    def generate_stage(self, stage: dict) -> StageOrchestrator:
        puzzles = stage["puzzles"]
        slug = stage["slug"]
        return StageOrchestrator(
            self.ro,
            slug,
            [self.generate_puzzle(puzzle_data) for puzzle_data in puzzles],
        )

    def generate_puzzle(self, puzzle: dict) -> PuzzleOrchestrator:
        type = puzzle["type"]
        slug = puzzle["slug"]
        match type:
            case "digitalState":
                name_map: dict = puzzle["name_map"]
                return DigitalState(self.ro, slug, name_map.keys())
            case "sequence":
                target_sequence: list[str] = puzzle["target_state"]
                return Sequence(self.ro, slug, target_sequence)
            case "speechDetection":
                return SpeechDetection(self.ro, slug)


class PydanticRoomBuilder:
    def __init__(self, room_orchestator: "RoomOrchestrator"):
        self.ro = room_orchestator

    def generate_stages_from_json(self, stages_data: list[StageConfig]):
        return [self.generate_stage(stage_data) for stage_data in stages_data]

    def generate_stage(self, stage: StageConfig) -> StageOrchestrator:
        puzzles = [self.generate_puzzle(puzzle_data) for puzzle_data in stage.puzzles]
        return StageOrchestrator(self.ro, stage.slug, puzzles)

    def generate_puzzle(self, puzzle: AnyPuzzleConfig) -> PuzzleOrchestrator:
        match puzzle.type:
            case PuzzleType.DIGITAL_STATE:
                name_map: dict = puzzle.name_map
                return DigitalState(self.ro, puzzle.slug, name_map.keys())
            case PuzzleType.SEQUENCE:
                target_sequence: list[str] = puzzle.extras.target_state
                return Sequence(self.ro, puzzle.slug, target_sequence)
            case PuzzleType.SPEECH_DETECTION:
                return SpeechDetection(self.ro, puzzle.slug)
            case _:
                raise ValueError(f"Puzzle type {puzzle.type} not supported.")
