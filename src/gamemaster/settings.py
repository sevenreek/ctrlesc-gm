from typing import Literal
from logging import INFO as LOG_INFO

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    rooms_config_directory: str = "/home/sevenreek/Development/ctrlesc/ctrlesc-api/src/api/public/config/rooms"
    room_slug: str = "demonic-presence"
    redis_url: str = "localhost"
    redis_port: str = "6379"
    mqtt_url: str = "localhost"
    mqtt_port: str = "1883"
    health_check_period: int = 5000  # ms
    log_level: str | int = LOG_INFO
    endianness: Literal["big", "little"] = "big"
    db_user: str = "postgres"
    db_password: str = "masterkey"
    db_host: str = "localhost:5555"
    db_name: str = "postgres"


settings = Settings()
