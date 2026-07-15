import asyncio
import signal

from app.config import get_settings
from app.database import Database
from app.instagram import InstagramClient
from app.logger import configure_logging, get_logger
from app.metrics import Metrics
from app.publisher import Publisher
from app.repository import Repository
from app.scheduler import Scheduler


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("main")

    database = Database(settings)
    await database.connect()

    metrics = Metrics()
    repository = Repository(database, settings)
    instagram = InstagramClient(settings)
    publisher = Publisher(settings, repository, instagram, metrics)
    await publisher.cleanup_stale_video_processing_files()
    scheduler = Scheduler(settings, repository, publisher, metrics)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, scheduler.stop)

    try:
        logger.info("service_started", service=settings.service_name, environment=settings.environment)
        await scheduler.run_forever()
    finally:
        await instagram.close()
        await database.close()
        logger.info("service_stopped")


if __name__ == "__main__":
    asyncio.run(main())
