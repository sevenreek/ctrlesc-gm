from orchestrator import RoomOrchestrator, StageOrchestrator
from settings import settings
import logging as log


def main():
    log.basicConfig(encoding="utf-8", level=settings.log_level)
    stages = [StageOrchestrator(), StageOrchestrator()]
    orchestrator = RoomOrchestrator(stages, settings)
    try:
        orchestrator.start_loop()
    except KeyboardInterrupt:
        orchestrator.stop_loop()


if __name__ == "__main__":
    main()
