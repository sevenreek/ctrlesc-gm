from log import log
from rooms.orchestrator import RoomOrchestrator
from settings import settings
from rooms.demonic_presence import generate_stages
import weakref


def main():
    log.info(f"Started room orchestrator {settings.room_slug}.")
    orchestrator = RoomOrchestrator(settings)
    stages = generate_stages(weakref.ref(orchestrator))
    try:
        orchestrator.start_loop(stages)
    except KeyboardInterrupt:
        orchestrator.stop_loop()
    finally:
        log.warn(f"Stopped room orchestrator {settings.room_slug}.")


if __name__ == "__main__":
    main()
