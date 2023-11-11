from log import log
from rooms.orchestrator import RoomOrchestrator
from settings import settings
from rooms import RoomBuilder
import weakref
import json
import os


def main():
    log.info(f"Started room orchestrator {settings.room_slug}.")
    orchestrator = RoomOrchestrator(settings)
    orchestartor_ref = weakref.ref(orchestrator)
    room_config_filepath = os.path.join(
        settings.rooms_config_directory, f"{settings.room_slug}.json"
    )
    with open(room_config_filepath) as file:
        room_data = json.load(file)
    builder = RoomBuilder(orchestartor_ref)
    stages = builder.generate_stages_from_json(room_data["stages"])
    try:
        orchestrator.start_loop(stages)
    except KeyboardInterrupt:
        orchestrator.stop_loop()
    finally:
        log.warn(f"Stopped room orchestrator {settings.room_slug}.")


if __name__ == "__main__":
    main()
