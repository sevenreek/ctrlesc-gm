import asyncio
from abc import ABC
import signal
from gmqtt import Client as MQTTClient, constants as MQTT
from os import getpid
from settings import Settings
import redis.asyncio as redis
from log import log
from datetime import datetime
from escmodels.room import RoomConfig
from escmodels.util import generate_room_initial_state
from room_builder import PydanticRoomBuilder
from stage_orchestrator import StageOrchestrator, GameObject, MQTTMessageHandler
import json


class RoomOrchestrator(ABC):
    def __init__(self, settings: Settings, config: RoomConfig):
        self.settings = settings
        self.config = config
        self._active_stage_index: int | None = None
        self._stages: list[StageOrchestrator] = PydanticRoomBuilder(
            self
        ).generate_stages_from_json(config.stages)
        self._game_objects: list[GameObject] = []
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

    async def load_state(self):
        self.state = generate_room_initial_state(self.config)
        await self.reset_room_state_redis()

    @property
    def active_stage(self) -> StageOrchestrator:
        try:
            return self._stages[self._active_stage_index]
        except (IndexError, TypeError):
            return None

    async def add_game_element(self, ge: GameObject):
        self._game_objects.append(ge)
        await ge.start()

    async def remove_game_element(self, ge: GameObject):
        self._game_objects.remove(ge)
        await ge.stop()

    async def finish_stage(self, stage_slug: str) -> None:
        if stage_slug != self.active_stage.slug:
            raise ValueError(
                f"Stage {self.active_stage.slug} is not active. Cannot finish."
            )
        await self.load_stage(self._active_stage_index + 1)

    async def load_stage(self, stage_index: int) -> None:
        if self.active_stage:
            await self.active_stage.stop()
        self._active_stage_index = stage_index
        await self.active_stage.start()

    def start_loop(self, *, from_stage: int = 0) -> None:
        self.running = True
        self._loop.run_until_complete(self.start_mqtt())
        self._loop.run_until_complete(self.load_state())
        self._loop.run_until_complete(self.start())
        self._loop.run_until_complete(self.load_stage(from_stage))
        log.info(f"Starting loop for {self.settings.room_slug}.")
        self._loop.create_task(self.health_check_update())
        self._loop.create_task(self.handle_requests())
        self._loop.run_forever()

    def _stop_signal(self):
        log.error("Received stop signal.")
        self.stop_loop()

    async def stop(self):
        log.info("Stopping.")
        await self.mqtt.disconnect()
        self._loop.stop()
        log.info("Stopped.")

    def stop_loop(self) -> None:
        task = asyncio.create_task(self.stop())

    async def start_mqtt(self) -> None:
        await self.mqtt.connect(self.settings.mqtt_url, int(self.settings.mqtt_port))
        self.mqtt.subscribe(f"room/{self.settings.room_slug}/#")

    async def start(self) -> None:
        pass

    async def on_message(
        self, client: MQTTClient, topic: str, payload: bytes, qos: int, properties
    ):
        log.debug(topic, payload.decode())
        await self.mqtt_handler.handle(topic, payload)
        return MQTT.PubAckReasonCode.SUCCESS

    async def health_check_update(self):
        topic = f"room/health/{self.settings.room_slug}"
        now = datetime.now().isoformat()
        while self._loop.is_running():
            await self.redis.set(topic, now)
            await self.redis.publish(topic, now)
            await asyncio.sleep(self.settings.health_check_period / 1000.0)

    async def handle_requests(self):
        client = self.redis.client()
        async with client.pubsub() as ps:
            await ps.psubscribe(f"room/request/{self.settings.room_slug}/*")
            while self._loop.is_running():
                message = await ps.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    continue
                channel = message["channel"]
                message_id = channel.split("/")[-1]
                data = json.loads(message["data"])
                log.info(data)
                match data["action"]:
                    case "skip":
                        pass
                    case "start":
                        pass
                    case "stop":
                        pass
                    case "pause":
                        pass
                    case "add":
                        pass
                ack_channel = f"room/ack/{self.settings.room_slug}/{message_id}"
                delivered_count = await client.publish(ack_channel, "hello")

    def room_key(self, slug: str):
        return f"room:{slug}"

    async def reset_room_state_redis(self):
        await self.redis.json().set(
            f"room:{self.settings.room_slug}",
            "$",
            generate_room_initial_state(self.config),
        )
