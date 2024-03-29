from stage_orchestrator import PuzzleOrchestrator, StageOrchestrator
from settings import settings
from log import log
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from room_orchestrator import RoomOrchestrator


class MQTTBasedPuzzle(PuzzleOrchestrator):
    async def on_message(self, topic: str, payload: bytes):
        pass

    # override
    async def start(self):
        self.mqtt_handler.add_handler(self.status_topic, self.on_message)
        self.mqtt.publish(f"{self.base_topic}/start", None, 1)

    # override
    async def stop(self):
        self.mqtt_handler.remove_handler(self.status_topic, self.on_message)
        self.mqtt.publish(f"{self.base_topic}/stop", None, 1)


class DigitalState(MQTTBasedPuzzle):
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
        element_slug: str,
        index_map: list[str],
    ):
        super().__init__(room_orchestrator, element_slug)
        self.index_map = list(index_map)
        self.reset_internal_state()

    def reset_internal_state(self):
        self.state = [False for _ in range(len(self.index_map))]

    # override
    async def on_message(self, topic: str, payload: bytes):
        subtopic = topic.removeprefix(self.status_topic)
        match subtopic:
            case "":
                await self.update_state(payload)
            case "/done":
                await self.complete()
            case _:
                log.error(
                    f"Puzzle<{self.__class__.__name__}> {self.element_slug} received unhandled topic {topic} with payload {payload}."
                )

    async def update_state(self, data: bytes):
        new_state = [bool(byte) for byte in data]
        if len(new_state) != len(self.index_map):
            log.error(
                f"Puzzle<{self.__class__.__name__}> {self.element_slug} received a state update {new_state} which does not match expected state."
            )
            return
        self.state = new_state
        json_data = {self.index_map[i]: value for i, value in enumerate(self.state)}
        await self.trigger_event(
            PuzzleOrchestrator.Events.EVENT_STATE_CHANGED, json_data
        )
        if all(self.state):
            await self.complete()


class Sequence(MQTTBasedPuzzle):
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
        element_slug: str,
        target_sequence: list[str],
    ):
        super().__init__(room_orchestrator, element_slug)
        self.target_sequence = target_sequence
        self.reset_internal_state()

    def reset_internal_state(self):
        self.state = [None for _ in range(len(self.target_sequence))]

    # override
    async def on_message(self, topic: str, payload: bytes):
        subtopic = topic.removeprefix(self.status_topic)
        match subtopic:
            case "":
                await self.update_state(payload)
            case "/done":
                await self.complete()
            case _:
                log.error(
                    f"Puzzle<{self.__class__.__name__}> {self.element_slug} received unhandled topic {topic} with payload {payload}."
                )

    async def update_state(self, data: bytes):
        self.state = int.from_bytes(data, settings.endianness, signed=True)
        json_data = {
            "sequence": [str(sequence_element) for sequence_element in self.state]
        }
        await self.trigger_event(
            PuzzleOrchestrator.Events.EVENT_STATE_CHANGED, json_data
        )
        if all(self.state):
            await self.complete()


class SpeechDetection(MQTTBasedPuzzle):
    pass
