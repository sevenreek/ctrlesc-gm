
from collections import deque
from dataclasses import dataclass
import json
from os import path
from typing import override, TYPE_CHECKING

import nava

from gamemaster.ents.base import MQTTGameElement
from gamemaster.log import log
from escmodels.base.entity import LayerState, SoundPlayerConfig

if TYPE_CHECKING:
    from gamemaster.room_orchestrator import RoomOrchestrator


@dataclass
class Sound():
    id: int
    track: str
    loop: bool


class Layer():
    def __init__(self, track_dir: str, volume: float):
        self.track_dir = track_dir
        self.playing: list[Sound] = []
        self.queue: deque[Sound] = deque()
        self.volume = volume
    
    def get_track(self, track: str) -> str:
        return path.join(self.track_dir, track)
    
    def play(self, track: str):
        nava.play(self.get_track(track))

    def stop(self):
        pass

    def enqueue(self, track: str):
        pass

    def dequeue(self, track: str):
        pass
    
    def drop(self):
        pass

class SoundPlayer(MQTTGameElement):
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
        element_slug: str,
        config: SoundPlayerConfig,
    ):
        super().__init__(room_orchestrator, element_slug)
        self.config = config
        self.layers = {
            layer_key: Layer(
                volume=cfg.volume
            )
            for layer_key, cfg in self.config.layers.items()
        }

    @override
    async def on_message(self, topic: str, payload: bytes):
        subtopic = topic.removeprefix(self.status_topic)
        data = json.loads(payload.decode())
        match subtopic:
            case "/volume":
                await self.change_volume(data)
            case "/play":
                await self.play_track(layer=data['layer'], track=data['track'])
            case "/stop_layer":
                await self.drop_queue(data['layer'])
            case "/skip":
                await self.skip(data['layer'])
            case _:
                log.error(
                    f"SoundPlayer<{self.__class__.__name__}> {self.element_slug} received unhandled topic {topic} with payload {payload}."
                )

    async def change_volume(self, layers: dict[str, float]):
        pass

    async def play_track(self, layer: str, track: str, loop: bool = False):
        pass

    async def enqueue_track(self, layer: str, track: str):
        pass

    async def dequeue_track(self, layer: str, track: str):
        pass

    async def drop_queue(self, layer: str):
        pass

    async def skip(self, layer: str):
        pass

    async def update_state(self, data: bytes):
        pass