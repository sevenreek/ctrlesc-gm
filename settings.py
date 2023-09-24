from pydantic_settings import BaseSettings
from logging import DEBUG as LOG_DEBUG, INFO as LOG_INFO


class Settings(BaseSettings):
    room_slug: str = "demonic-presence"
    redis_url: str = "localhost"
    redis_port: str = "6379"
    mqtt_url: str = "localhost"
    mqtt_port: str = "1883"
    health_check_period: int = 5000  # ms
    log_level: str | int = LOG_INFO


settings = Settings()
