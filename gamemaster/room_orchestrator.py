from pydantic import ValidationError
import asyncio
from abc import ABC
import signal
from gmqtt import Client as MQTTClient, constants as MQTT
from os import getpid
from settings import Settings
import redis.asyncio as redis
from log import log
from datetime import datetime
from escmodels.room import RoomConfig, TimerState, StageState, RoomState
from escmodels.util import generate_room_initial_state
from escmodels.requests import (
    SkipPuzzleRequest,
    TimerAddRequest,
    TimerStateRequest,
    RequestResult,
)
from room_builder import PydanticRoomBuilder
from stage_orchestrator import (
    PuzzleOrchestrator,
    StageOrchestrator,
    GameObject,
    MQTTMessageHandler,
)
import json
import pydantic


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
                try:
                    result = await self.handle_request(data)
                except ValidationError as e:
                    result = RequestResult(False, str(e))
                ack_channel = f"room/ack/{self.settings.room_slug}/{message_id}"
                delivered_count = await client.publish(
                    ack_channel, result.model_dump_json()
                )

    def update_puzzle(self, stage: str, puzzle: str, state: dict):
        pass

    def update_stage(self, stage: str, state: dict):
        pass

    def find_puzzle(self, stage: str, puzzle: str) -> PuzzleOrchestrator:
        try:
            stage_orchestrator = next(s for s in self._stages if s.slug == stage)
            puzzle_orchestrator = next(
                p for p in stage_orchestrator.puzzles if p.element_slug == puzzle
            )
            return puzzle_orchestrator
        except StopIteration:
            raise KeyError(f"Puzzle {stage}/{puzzle} not found.")

    async def handle_request(self, request: dict) -> RequestResult:
        result = RequestResult()
        if request["action"] == "skip":
            request: SkipPuzzleRequest = SkipPuzzleRequest.model_validate(request)
            try:
                await self.find_puzzle().skip()
            except KeyError:
                result.success = False
                result.error = (
                    f"Could not find puzzle {request.puzzle} in stage {request.stage}."
                )
        elif request["action"] == "start":
            if self.state.state in [
                TimerState.ACTIVE,
                TimerState.FINISHED,
                TimerState.STOPPED,
            ]:
                result.success = False
                result.error = (
                    f"Could not start room that's in state {self.state.state}"
                )
            elif self.state.state in [TimerState.READY]:
                await self.load_stage(0)
            await self.update_room(
                start_timestamp=datetime.now().isoformat(), state=TimerState.ACTIVE
            )
        elif request["action"] == "pause":
            if self.state.state in [
                TimerState.PAUSED,
                TimerState.FINISHED,
                TimerState.STOPPED,
                TimerState.READY,
            ]:
                result.success = False
                result.error = (
                    f"Could not pause room that's in state {self.state.state}"
                )
            elif self.state.state in [TimerState.ACTIVE]:
                total_time_elapsed = (
                    self.state.time_elapsed_on_pause
                    + (
                        datetime.now()
                        - datetime.fromisoformat(self.state.start_timestamp)
                    ).seconds
                )
                await self.update_room(
                    time_elapsed_on_pause=total_time_elapsed, state=TimerState.PAUSED
                )
        elif request["action"] == "stop":
            if self.state.state in [TimerState.STOPPED]:
                result.success = False
                result.error = f"Could not stop room that's in state {self.state.state}"
            else:
                await self.update_room(state=TimerState.STOPPED)
        elif request["action"] == "add":
            request: TimerAddRequest = TimerAddRequest.model_validate(request)
            await self.update_room(
                extra_time=self.state.extra_time + request.minutes * 60
            )
        return result

    async def update_room(self, *, stages: list[dict] | None = None, **kwargs):
        """Updates the room's state. Trusts that kwargs are prevalidated."""
        self.state = self.state.model_copy(update=kwargs)
        if stages:
            self.state.stages = [StageState.model_validate(s) for s in stages]
        await self.redis.json().set(
            f"room:{self.settings.room_slug}", "$", self.state.model_dump()
        )
        update_topic = f"room/state/{self.settings.room_slug}"
        await self.redis.publish(update_topic, json.dumps(kwargs))

    def room_key(self, slug: str):
        return f"room:{slug}"

    async def reset_room_state_redis(self):
        await self.update_room(**generate_room_initial_state(self.config).model_dump())
