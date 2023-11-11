from .orchestrator import Stage, RoomOrchestrator, ReferenceType, log, Puzzle
from rooms.puzzles import DigitalState, SpeechDetection, Sequence


class RoomBuilder:
    def __init__(self, room_orchestator_ref: ReferenceType[RoomOrchestrator]):
        self.ro = room_orchestator_ref

    def generate_stages_from_json(self, stages_data: list[dict]):
        return [self.generate_stage(stage_data) for stage_data in stages_data]

    def generate_stage(self, stage: dict) -> Stage:
        puzzles = stage["puzzles"]
        slug = stage["slug"]
        return Stage(
            self.ro,
            slug,
            [self.generate_puzzle(puzzle_data) for puzzle_data in puzzles],
        )

    def generate_puzzle(self, puzzle: dict) -> Puzzle:
        component = puzzle["component"]
        type = component["type"]
        slug = puzzle["slug"]
        match type:
            case "digitalState":
                name_map: dict = component["nameMap"]
                return DigitalState(self.ro, slug, name_map.keys())
            case "sequence":
                target_sequence: list[str] = component["targetSequence"]
                return Sequence(self.ro, slug, target_sequence)
            case "speechDetection":
                return SpeechDetection(self.ro, slug)
