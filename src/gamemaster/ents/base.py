from enum import StrEnum
from functools import cached_property
from typing import TYPE_CHECKING, Awaitable, Callable, override

from gamemaster.log import log

if TYPE_CHECKING:
    from gamemaster.room_orchestrator import RoomOrchestrator

type MQTTTopicType = str
type MQTTPayloadType = bytes
type MQTTMessageHandlerType = Callable[[MQTTTopicType, MQTTPayloadType], Awaitable[None]]


class LifecycleElement:
    def __init__(
        self,
        room_orchestrator: "RoomOrchestrator",
    ):
        self.room_orchestrator = room_orchestrator
        self._registered_handlers: list[MQTTMessageHandlerType] = []

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



class MQTTGameElement(GameElement):
    async def on_message(self, topic: str, payload: bytes):
        pass

    @override
    async def start(self):
        self.mqtt_handler.add_handler(self.status_topic, self.on_message)
        self.mqtt.publish(f"{self.base_topic}/start", None, 1)

    @override
    async def stop(self):
        self.mqtt_handler.remove_handler(self.status_topic, self.on_message)
        self.mqtt.publish(f"{self.base_topic}/stop", None, 1)


class ElementEvents(StrEnum):
    STATE_CHANGED = "state"