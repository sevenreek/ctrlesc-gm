from ..orchestrator import Stage, RoomOrchestrator, ReferenceType, log
from ..puzzles import DigitalState


def generate_stages(room_orchestrator_ref: ReferenceType[RoomOrchestrator]):
    ro = room_orchestrator_ref
    return [
        Stage(ro, "unlock_cells", [DigitalState(ro, "cells", ["left", "right"])]),
        Stage(
            ro,
            "open_chest",
            [DigitalState(ro, "chest", ["chest"])],
        ),
        Stage(
            ro,
            "lower_coffin",
            [DigitalState(ro, "coffin", ["coffin"])],
        ),
    ]
