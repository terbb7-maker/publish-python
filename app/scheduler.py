import asyncio
from collections import defaultdict
from time import monotonic

from app.config import Settings
from app.logger import get_logger
from app.metrics import Metrics
from app.publisher import Publisher
from app.repository import Repository


class Scheduler:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        publisher: Publisher,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._publisher = publisher
        self._metrics = metrics
        self._logger = get_logger("scheduler")
        self._stop = asyncio.Event()
        self._semaphore = asyncio.Semaphore(settings.concurrency)
        self._account_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._task_accounts: dict[asyncio.Task[None], str] = {}
        self._task_jobs: dict[asyncio.Task[None], str] = {}
        self._next_recovery_at = 0.0

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        self._logger.info(
            "scheduler_started",
            worker_id=self._settings.worker_id,
            concurrency=self._settings.concurrency,
            max_claim=self._settings.batch_size,
        )

        in_flight: set[asyncio.Task[None]] = set()
        while not self._stop.is_set():
            try:
                await self._recover_if_due()
                claimed = await self._fill_open_slots(in_flight)

                if not in_flight:
                    if not claimed:
                        await self._sleep(self._settings.empty_poll_interval_seconds)
                    continue

                done, pending = await asyncio.wait(
                    in_flight,
                    timeout=self._settings.poll_interval_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                in_flight = pending
                self._handle_finished_tasks(done)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._metrics.inc("scheduler.error")
                self._logger.error("scheduler_loop_error", error=str(error), exc_info=True)
                await self._sleep(self._settings.empty_poll_interval_seconds)

        if in_flight:
            self._logger.info(
                "scheduler_draining",
                worker_id=self._settings.worker_id,
                running=len(in_flight),
            )
            done, pending = await asyncio.wait(in_flight, timeout=self._settings.publish_timeout_seconds)
            self._handle_finished_tasks(done)
            for task in pending:
                task.cancel()
            while pending:
                cancelled, pending = await asyncio.wait(
                    pending,
                    timeout=1,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                self._handle_finished_tasks(cancelled)
                if pending:
                    self._logger.warning(
                        "scheduler_waiting_for_cancelled_tasks",
                        worker_id=self._settings.worker_id,
                        pending=len(pending),
                    )

        self._logger.info("scheduler_stopped", worker_id=self._settings.worker_id)

    async def _run_job(self, job_id: str, social_account_id: str) -> None:
        lock = self._account_locks[social_account_id]
        async with lock:
            await self._publisher.publish(job_id)

    async def _fill_open_slots(self, in_flight: set[asyncio.Task[None]]) -> int:
        total_claimed = 0
        while True:
            available = self._settings.concurrency - len(in_flight)
            if available <= 0:
                break

            jobs = await self._repository.claim_due_jobs(min(self._settings.batch_size, available))
            if not jobs:
                break

            total_claimed += len(jobs)
            for job in jobs:
                await self._semaphore.acquire()
                task = asyncio.create_task(self._run_job(job.id, job.social_account_id))
                in_flight.add(task)
                self._task_accounts[task] = job.social_account_id
                self._task_jobs[task] = job.id

        self._metrics.inc("claim.jobs", total_claimed)
        self._logger.info(
            "claim_finished",
            worker_id=self._settings.worker_id,
            claimed=total_claimed,
            running=len(in_flight),
            available=max(self._settings.concurrency - len(in_flight), 0),
            metrics=self._metrics.snapshot(),
        )
        return total_claimed

    async def _recover_if_due(self) -> None:
        now = monotonic()
        if now < self._next_recovery_at:
            return

        recovered = await self._recover()
        self._next_recovery_at = now + self._settings.empty_poll_interval_seconds
        if recovered:
            self._logger.info(
                "recovery_finished",
                worker_id=self._settings.worker_id,
                recovered=recovered,
            )

    async def _recover(self) -> int:
        unknown = await self._repository.mark_unknown_publishing_as_failed()
        recovered = await self._repository.recover_stale_jobs()
        if unknown:
            self._metrics.inc("recover.unknown_publish", unknown)
        if recovered:
            self._metrics.inc("recover.stale", recovered)
        return recovered + unknown

    def _handle_finished_tasks(self, tasks: set[asyncio.Task[None]]) -> None:
        for task in tasks:
            account_id = self._task_accounts.get(task)
            job_id = self._task_jobs.get(task)
            if task.cancelled():
                self._logger.warning("job_task_cancelled", account_id=account_id, job_id=job_id)
                self._release_task_resources(task)
                continue
            result = task.exception()
            if result:
                self._logger.error(
                    "job_task_failed",
                    account_id=account_id,
                    job_id=job_id,
                    error=str(result),
                    error_type=type(result).__name__,
                )
            self._release_task_resources(task)

    def _release_task_resources(self, task: asyncio.Task[None]) -> None:
        account_id = self._task_accounts.pop(task, None)
        self._task_jobs.pop(task, None)
        self._semaphore.release()
        self._cleanup_account_lock(account_id)

    def _cleanup_account_lock(self, account_id: str | None) -> None:
        if not account_id:
            return
        lock = self._account_locks.get(account_id)
        if lock and not lock.locked():
            self._account_locks.pop(account_id, None)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return
