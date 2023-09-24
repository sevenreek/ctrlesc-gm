from orchestrator import RoomOrchestrator, StageOrchestrator
from settings import settings


def main():
    stages = [StageOrchestrator(), StageOrchestrator()]
    orchestrator = RoomOrchestrator(stages, settings)
    try:
        orchestrator.start_loop()
    except KeyboardInterrupt:
        orchestrator.stop_loop()


if __name__ == "__main__":
    main()
