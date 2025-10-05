import json
import asyncio
import signal
from itertools import chain
from abc import ABC
from typing import Any, Unpack, cast
from collections import defaultdict
from datetime import datetime
from os import getpid
from escmodels.base.room import RoomStateUpdateKwargs
from gamemaster.ents.base import GameElement, MQTTMessageHandlerType

from snowflake import SnowflakeGenerator

from pydantic import ValidationError
from sqlalchemy import update, select
from gmqtt import Client as MQTTClient, constants as MQTT
import redis.asyncio as redis

import escmodels.base as base
from escmodels.db.models import (
    Game,
    GameEvent,
    GameEventType,
    GameResult,
    StageCompletion,
    Stage,
    Room,
)
from escmodels.requests import (
    AnyRoomActionRequestDict,
    SkipPuzzleRequest,
    TimerAddRequest,
    RequestResult,
)

from gamemaster.db import obtain_session
from gamemaster.log import log
from gamemaster.settings import Settings
from gamemaster.room_builder import PydanticRoomBuilder
from gamemaster.stage_orchestrator import (
    PuzzleOrchestrator,
    StageOrchestrator,
)

class MQTTHandler:
    def __init__(self):
        self.handlers: dict[str, list[MQTTMessageHandlerType]] = defaultdict(list)

    def add_handler(self, topic: str, coro: MQTTMessageHandlerType):
        self.handlers[topic].append(coro)

    def remove_handler(self, topic: str, coro: MQTTMessageHandlerType):
        self.handlers.get(topic, []).remove(coro)

    async def handle(self, topic: str, payload: bytes):
        handlers_for_topic = list(
            chain(
                *(
                    handler
                    for handler_topic, handler in self.handlers.items()
                    if topic.startswith(handler_topic)
                )
            )
        )
        log.info(
            f"Received message in topic {repr(topic)} for {len(handlers_for_topic)} handlers."
        )
        await asyncio.gather(
            *(handler(topic, payload) for handler in handlers_for_topic)
        )


class RoomOrchestrator(ABC):
    def __init__(self, settings: Settings, config: base.RoomConfig):
        self.settings = settings
        self.config = config
        self.idgen = SnowflakeGenerator(
            self.config.id, epoch = int(datetime(2020, 1, 1).timestamp())
        )
        self._stages: list[StageOrchestrator] = PydanticRoomBuilder(
            self
        ).generate_stages_from_json(config.stages)
        self._entities: list[GameElement] = []
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
        self.mqtt_handler = MQTTHandler()
        self._loop.add_signal_handler(signal.SIGTERM, self._stop_signal)
        self._loop.add_signal_handler(signal.SIGINT, self._stop_signal)
        self.stage_db_ids: list[int] = []
        self.stage_completions: list[int] = []

    async def load_state(self):
        self.state = base.generate_room_initial_state(self.config)
        self.puzzle_state_map = {
            stage.slug: {puzzle.slug: puzzle for puzzle in stage.puzzles}
            for stage in self.state.stages
        }
        await self.reset_room_state_redis()

    @property
    def room_slug(self):
        return self.settings.room_slug

    @property
    def room_key(self):
        return f"room:{self.room_slug}"

    @property
    def active_stage_index(self):
        return self.state.active_stage

    @property
    def active_stage(self) -> StageOrchestrator | None:
        if self.active_stage_index is None:
            return None
        return self._stages[self.active_stage_index]

    @property
    def last_stage_completion(self) -> int:
        if not len(self.stage_completions):
            return 0
        return self.stage_completions[-1]

    async def add_game_element(self, ge: GameElement):
        self._entities.append(ge)
        await ge.start()

    async def remove_game_element(self, ge: GameElement):
        self._entities.remove(ge)
        await ge.stop()

    def get_gametime_elapsed_seconds(self) -> int:
        """Return the number of seconds passed in gametime.
        Excludes time spent in pause. If the game has not started
        returns 0.
        """
        if not self.state.start_timestamp:
            return 0
        return int(
            (datetime.now() - datetime.fromisoformat(self.state.start_timestamp)).total_seconds()
            - self.state.time_elapsed_on_pause
        )

    async def reset_room(self):
        await self.reset_room_state_redis()
        await asyncio.gather(
            *(stage.reset() for stage in self._stages),
            *(element.reset() for element in self._entities),
        )

    async def finish_game(self, success: bool | None = False):
        new_state: base.TimerState = base.TimerState.FINISHED
        total_time_elapsed = self.get_gametime_elapsed_seconds()
        stop_timestamp = datetime.now()
        update_values: dict[str, Any] = {
            "seconds_taken": total_time_elapsed,
            "ended_on": stop_timestamp,
        }
        if success is None:  # stopped
            new_state = base.TimerState.STOPPED
            update_values["result"] = GameResult.STOPPED
        elif success:  # game won
            update_values["result"] = GameResult.COMPLETED
        else:  # timed out
            update_values["result"] = GameResult.TIMEDOUT

        statement = (
            update(Game)
            .where(Game.id == self.state.active_game_id)
            .values(update_values)
        )
        async with obtain_session() as session:
            await session.execute(statement)
            await session.commit()
        await self.update_room(
            state=new_state,
            stop_timestamp=stop_timestamp.isoformat(),
            time_elapsed_on_pause=total_time_elapsed,
        )

    async def save_db_event(self, type: GameEventType, data: Any = None):
        if self.state.active_game_id is None:
            raise RuntimeError("Cannot create events without a game running.")
        async with obtain_session() as session:
            event = GameEvent(
                game_id=self.state.active_game_id,
                created_on=datetime.now(),
                gametime=self.get_gametime_elapsed_seconds(),
                type=type,
                data=data,
            )
            session.add(event)
            await session.commit()

    async def finish_stage(self, stage_slug: str) -> None:
        assert self.active_stage is not None and self.active_stage_index is not None, "Finishing game but no active stage"
        if stage_slug != self.active_stage.slug:
            raise ValueError(
                f"Stage {self.active_stage.slug} is not active. Cannot finish."
            )
        stage_gametime = self.get_gametime_elapsed_seconds()
        async with obtain_session() as session:
            completion = StageCompletion(
                stage_id=self.stage_db_ids[self.active_stage_index],
                game_id=self.state.active_game_id,
                duration=stage_gametime - self.last_stage_completion,
                gametime=stage_gametime,
            )
            session.add(completion)
            await session.commit()
        self.stage_completions.append(stage_gametime)
        await self.redis.rpush(f"{self.room_key}/stage_completions", stage_gametime)
        update_topic = f"room/completions/{self.settings.room_slug}"
        await self.redis.publish(update_topic, json.dumps(stage_gametime))
        if self.active_stage_index == len(self.state.stages) - 1:
            await self.finish_game(True)
        else:
            await self.load_stage(self.active_stage_index + 1)

    async def load_stage(self, stage_index: int) -> None:
        if self.active_stage:
            await self.active_stage.stop()
        await self.update_room(active_stage=stage_index)
        await self.active_stage.start()

    def start_loop(self, *, from_stage: int = 0) -> None:
        self.running = True
        self._loop.run_until_complete(self.start_mqtt())
        self._loop.run_until_complete(self.load_state())
        self._loop.run_until_complete(self.start())
        self._loop.run_until_complete(self.load_stage(from_stage))
        log.info(f"Starting loop for {self.room_slug}.")
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
        _ = asyncio.create_task(self.stop())

    async def start_mqtt(self) -> None:
        await self.mqtt.connect(self.settings.mqtt_url, int(self.settings.mqtt_port))
        self.mqtt.subscribe(f"room/{self.room_slug}/#")

    async def start(self) -> None:
        await self.drop_stage_completions_list()
        async with obtain_session() as session:
            sql = (
                select(Stage)
                .join(Room, Stage.room_id == Room.id)
                .where(Room.slug == self.room_slug)
                .order_by(Stage.index)
            )
            result = await session.execute(sql)
            self.stage_db_ids = [stage.id for stage in result.scalars()]

    async def on_message(
        self, client: MQTTClient, topic: str, payload: bytes, qos: int, properties
    ):
        log.debug(topic, payload.decode())
        await self.mqtt_handler.handle(topic, payload)
        return MQTT.PubAckReasonCode.SUCCESS

    async def health_check_update(self):
        topic = f"room/health/{self.room_slug}"
        now = datetime.now().isoformat()
        while self._loop.is_running():
            await self.redis.set(topic, now)
            await self.redis.publish(topic, now)
            await asyncio.sleep(self.settings.health_check_period / 1000.0)

    async def handle_requests(self):
        client = self.redis.client()
        async with client.pubsub() as ps:
            await ps.psubscribe(f"room/request/{self.room_slug}/*")
            while self._loop.is_running():
                message = cast(dict[str, str] | None, await ps.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                ))
                if message is None:
                    continue
                try:
                    channel = message["channel"]
                    message_id = channel.split("/")[-1]
                    data = json.loads(message["data"])
                    result = await self.handle_request(data)
                except Exception as e:
                    result = RequestResult(success=False, error=str(e))
                    log.error(str(e))
                ack_channel = f"room/ack/{self.room_slug}/{message_id}"
                _delivered_count = await client.publish(
                    ack_channel, result.model_dump_json()
                )

    async def update_puzzle(self, stage: str, puzzle: str, /, **kwargs: Any):
        puzzle_state = self.puzzle_state_map[stage][puzzle]
        puzzle_state.__dict__.update(kwargs)
        await self.redis.json().set(
            self.room_key,
            f'$.stages[?(@.slug=="{stage}")].puzzles[?(@.slug=="{puzzle}")]',
            puzzle_state.model_dump(),
        )
        update_topic = f"room/state/{self.room_slug}/{stage}/{puzzle}"
        await self.redis.publish(update_topic, json.dumps(kwargs))

    def find_puzzle(self, stage: str, puzzle: str) -> PuzzleOrchestrator:
        try:
            stage_orchestrator = next(s for s in self._stages if s.slug == stage)
            puzzle_orchestrator = next(
                p for p in stage_orchestrator.puzzles if p.element_slug == puzzle
            )
            return puzzle_orchestrator
        except StopIteration:
            raise KeyError(f"Puzzle {stage}/{puzzle} not found.")

    async def drop_stage_completions_list(self):
        self.stage_completions = []
        await self.redis.delete(f"{self.room_key}/stage_completions")

    async def handle_request(self, request: AnyRoomActionRequestDict) -> RequestResult:
        result = RequestResult()
        try:
            if request["action"] == "skip":
                skip_request = SkipPuzzleRequest.model_validate(request)
                try:
                    await self.find_puzzle(
                        skip_request.stage, skip_request.puzzle
                    ).skip()
                except KeyError:
                    raise ValueError(
                        f"Could not find puzzle {skip_request.puzzle} in stage {skip_request.stage}."
                    )
            elif request["action"] == "start":
                if self.state.state in [
                    base.TimerState.ACTIVE,
                    base.TimerState.FINISHED,
                    base.TimerState.STOPPED,
                ]:
                    raise ValueError(
                        f"Could not start room that's in state {self.state.state}"
                    )
                elif self.state.state in [base.TimerState.READY]:
                    async with obtain_session() as session:
                        game_id = next(self.idgen)
                        start_timestamp = datetime.now()
                        game = Game(
                            room_slug=self.room_slug,
                            id=game_id,
                            started_on=start_timestamp,
                        )
                        session.add(game)
                        await session.commit()
                        await self.update_room(
                            start_timestamp=start_timestamp.isoformat(),
                            state=base.TimerState.ACTIVE,
                            active_game_id=game_id,
                        )
                        await self.load_stage(0)
                elif self.state.state in (base.TimerState.PAUSED):
                    elapsed = self.state.time_elapsed_on_pause
                    if self.state.pause_timestamp:
                        elapsed += (datetime.now() - datetime.fromisoformat(self.state.pause_timestamp)).total_seconds()
                    await self.update_room(
                        state=base.TimerState.ACTIVE,
                        pause_timestamp=None,
                        time_elapsed_on_pause=int(elapsed),
                    )

            elif request["action"] == "pause":
                if self.state.state in [
                    base.TimerState.PAUSED,
                    base.TimerState.FINISHED,
                    base.TimerState.STOPPED,
                    base.TimerState.READY,
                ]:
                    raise ValueError(
                        f"Could not pause room that's in state {self.state.state}"
                    )
                elif self.state.state in [base.TimerState.ACTIVE]:
                    await self.update_room(
                        state=base.TimerState.PAUSED,
                        pause_timestamp=datetime.now().isoformat()
                    )
            elif request["action"] == "stop":
                if self.state.state in [base.TimerState.STOPPED]:
                    raise ValueError(
                        f"Could not stop room that's in state {self.state.state}"
                    )
                else:
                    await self.finish_game(None)
            elif request["action"] == "add":
                valid_request: TimerAddRequest = TimerAddRequest.model_validate(request)
                await self.update_room(
                    extra_time=self.state.extra_time + valid_request.minutes * 60
                )
            elif request["action"] == "reset":
                await self.reset_room()
        except Exception as e:
            result.success = False
            result.error = str(e)
        return result

    def update_puzzle_state_map(self):
        self.puzzle_state_map = {
            stage.slug: {puzzle.slug: puzzle for puzzle in stage.puzzles}
            for stage in self.state.stages
        }

    async def update_room(self, **kwargs: Unpack[RoomStateUpdateKwargs]):
        """Updates the room's state. Trusts that kwargs are prevalidated."""

        if "state" in kwargs and self.state.active_game_id:
            await self.save_db_event(
                GameEventType.TIMER_CHANGED, {"state": kwargs["state"]}
            )
        self.state = self.state.model_copy(update=kwargs)
        redis_update_data = cast(dict[str, Any], kwargs)
        if stages := kwargs.get("stages"):
            self.state.stages = [base.StageState.model_validate(s) for s in stages]
            self.update_puzzle_state_map()
            redis_update_data["stages"] = [
                stage.model_dump() for stage in self.state.stages
            ]
        await self.redis.json().set(self.room_key, "$", self.state.model_dump())
        update_topic = f"room/state/{self.settings.room_slug}"
        await self.redis.publish(update_topic, json.dumps(redis_update_data))

    async def reset_room_state_redis(self):
        await self.update_room(
            **base.generate_room_initial_state(self.config).model_dump()
        )
