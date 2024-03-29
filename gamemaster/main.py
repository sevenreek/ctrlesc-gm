from log import log
from room_orchestrator import RoomOrchestrator
from settings import settings
import json
import os
import escmodels.base as base


def main():
    log.info(f"Started room orchestrator {settings.room_slug}.")
    room_config_filepath = os.path.join(
        settings.rooms_config_directory, f"{settings.room_slug}.json"
    )
    with open(room_config_filepath) as file:
        room_data = json.load(file)
    orchestrator = RoomOrchestrator(settings, base.RoomConfig.model_validate(room_data))
    try:
        orchestrator.start_loop()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.critical(f"Raised unhandled exception. {e}")
    finally:
        orchestrator.stop_loop()
        log.warning(f"Stopped room orchestrator {settings.room_slug}.")


if __name__ == "__main__":
    main()
