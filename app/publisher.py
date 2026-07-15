import asyncio
import base64
import hashlib
import random
from datetime import UTC, datetime, timedelta

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.config import Settings
from app.instagram import FAILED_STATUSES, READY_STATUSES, InstagramClient, InstagramError
from app.logger import get_logger
from app.metrics import Metrics, Timer
from app.repository import JobContext, Repository
from app.video_processor import ProcessedVideo, VideoProcessor


class Publisher:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        instagram: InstagramClient,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._instagram = instagram
        self._metrics = metrics
        self._logger = get_logger("publisher")
        self._video_processor = VideoProcessor(settings, metrics)

    async def cleanup_stale_video_processing_files(self) -> None:
        await self._video_processor.cleanup_stale_files()

    async def publish(self, job_id: str) -> None:
        timer = Timer()
        try:
            job = await self._repository.get_context(job_id)
        except Exception as error:
            await self._repository.mark_context_load_failed(job_id, "Falha ao carregar contexto do job.")
            self._metrics.inc("publish.context_error")
            self._logger.error("job_context_error", job_id=job_id, error=str(error), exc_info=True)
            return

        heartbeat = asyncio.create_task(self._heartbeat(job.id))

        try:
            async with asyncio.timeout(self._settings.publish_timeout_seconds):
                outcome = await self._publish_job(job)

            if outcome == "completed":
                self._metrics.inc("publish.success")
                self._logger.info(
                    "job_completed",
                    job_id=job.id,
                    ms=timer.ms(),
                )
            else:
                self._metrics.inc("publish.failed")
                self._logger.warning(
                    "job_finished_without_publish",
                    job_id=job.id,
                    outcome=outcome,
                    ms=timer.ms(),
                )

        except InstagramError as error:
            await self._handle_instagram_error(job, error)
            details = error.details or {}
            self._logger.warning(
                "job_instagram_error",
                job_id=job.id,
                code=error.code,
                message=error.message,
                category=error.category,
                retryable=error.retryable,
                http_status=error.http_status,
                details=error.details,
                fbtrace_id=details.get("fbtrace_id"),
                meta_code=details.get("meta_code"),
                meta_subcode=details.get("meta_subcode"),
                error_user_title=details.get("error_user_title"),
                error_user_msg=details.get("error_user_msg"),
                raw_error=details.get("raw_error"),
                ms=timer.ms(),
            )
        except asyncio.TimeoutError:
            await self._retry_or_fail(
                job,
                "publisher_timeout",
                "Timeout geral da publicacao.",
                retryable=True,
                metadata={"provider_status": "timeout"},
            )
            self._logger.warning("job_timeout", job_id=job.id, ms=timer.ms())
        except Exception as error:
            await self._retry_or_fail(
                job,
                "publisher_unexpected_error",
                "Falha inesperada no publisher.",
                retryable=True,
                metadata={"provider_status": "unexpected_error", "error": str(error)},
            )
            self._logger.error("job_unexpected_error", job_id=job.id, error=str(error), exc_info=True)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _publish_job(self, job: JobContext) -> str:
        if job.provider_media_id:
            await self._repository.mark_completed(
                job.id,
                job.provider_media_id,
                {"provider_status": "completed_from_checkpoint"},
            )
            return "completed"

        if job.provider_status == "publishing":
            await self._repository.mark_failed(
                job.id,
                "publish_result_unknown",
                "Resultado da chamada media_publish desconhecido; nao republicado para evitar duplicidade.",
            )
            return "failed"

        self._validate(job)
        token = decrypt_token(
            job.access_token_encrypted or "",
            self._settings.meta_token_encryption_key.get_secret_value(),
        )

        signed_url = await self._repository.signed_media_url(job)
        media_url = signed_url
        processed_video: ProcessedVideo | None = None
        temporary_storage_path: str | None = None
        temporary_processing_uuid: str | None = None

        try:
            container_id = job.provider_container_id
            if not container_id:
                processed_video = await self._video_processor.process(
                    signed_url,
                    job.mime_type,
                    job.id,
                )
                if self._settings.video_processing_dry_run:
                    await self._repository.mark_completed(
                        job.id,
                        (
                            f"video-processing-dry-run-{processed_video.processing_uuid}"
                            if processed_video
                            else f"video-processing-dry-run-fallback-{job.id}"
                        ),
                        {
                            "provider_status": (
                                "video_processing_dry_run"
                                if processed_video
                                else "video_processing_dry_run_fallback"
                            ),
                            "processing_uuid": processed_video.processing_uuid if processed_video else None,
                            "random_seed": processed_video.random_seed if processed_video else None,
                            "original_sha256": processed_video.original_sha256 if processed_video else None,
                            "processed_sha256": processed_video.final_sha256 if processed_video else None,
                            "original_size_bytes": (
                                processed_video.original_size_bytes if processed_video else None
                            ),
                            "processed_size_bytes": processed_video.final_size_bytes if processed_video else None,
                            "processing_ms": processed_video.ffmpeg_ms if processed_video else None,
                            "total_ms": processed_video.total_ms if processed_video else None,
                        },
                    )
                    self._logger.info(
                        "video_processing_dry_run_completed",
                        job_id=job.id,
                        campaign_id=job.campaign_id,
                        media_asset_id=job.media_asset_id,
                        processing_uuid=processed_video.processing_uuid if processed_video else None,
                        random_seed=processed_video.random_seed if processed_video else None,
                        used_fallback=processed_video is None,
                        original_sha256=processed_video.original_sha256 if processed_video else None,
                        processed_sha256=processed_video.final_sha256 if processed_video else None,
                        original_size_bytes=processed_video.original_size_bytes if processed_video else None,
                        processed_size_bytes=processed_video.final_size_bytes if processed_video else None,
                        total_ms=processed_video.total_ms if processed_video else None,
                    )
                    return "completed"

                if processed_video:
                    media_url, temporary_storage_path = await self._temporary_processed_media_url(
                        job,
                        processed_video,
                        signed_url,
                    )
                    temporary_processing_uuid = processed_video.processing_uuid

                container_timer = Timer()
                self._logger.info(
                    "container_create_started",
                    job_id=job.id,
                    campaign_id=job.campaign_id,
                    meta_app_id=job.meta_app_id,
                    instagram_user_id=job.instagram_user_id,
                    media_type=job.campaign_type,
                    mime_type=job.mime_type,
                    video_processing_enabled=self._settings.enable_video_processing,
                    video_processing_used=processed_video is not None and media_url != signed_url,
                    media_url="[redacted]",
                )
                try:
                    container_id = await self._instagram.create_container(
                        token,
                        job.instagram_user_id,
                        media_url,
                        job.mime_type,
                        job.campaign_type,
                        job.caption,
                    )
                except InstagramError as error:
                    self._log_instagram_stage_error(
                        "container_create_failed",
                        job,
                        error,
                        container_timer,
                        media_type=job.campaign_type,
                        mime_type=job.mime_type,
                    )
                    raise
                self._logger.info(
                    "container_created",
                    job_id=job.id,
                    campaign_id=job.campaign_id,
                    meta_app_id=job.meta_app_id,
                    instagram_user_id=job.instagram_user_id,
                    provider_container_id=container_id,
                    media_type=job.campaign_type,
                    mime_type=job.mime_type,
                    video_processing_used=processed_video is not None and media_url != signed_url,
                    media_url="[redacted]",
                    ms=container_timer.ms(),
                )
                await self._repository.patch_metadata(
                    job.id,
                    {
                        "provider_container_id": container_id,
                        "provider_status": "container_created",
                        "provider_container_created_at": now_iso(),
                    },
                )

            await self._poll_container(job, token, container_id)
            await self._repository.patch_metadata(
                job.id,
                {
                    "provider_container_id": container_id,
                    "provider_status": "publishing",
                    "provider_publish_started_at": now_iso(),
                },
            )

            publish_timer = Timer()
            self._logger.info(
                "media_publish_started",
                job_id=job.id,
                campaign_id=job.campaign_id,
                meta_app_id=job.meta_app_id,
                instagram_user_id=job.instagram_user_id,
                provider_container_id=container_id,
            )
            try:
                media_id = await self._instagram.publish_container(
                    token,
                    job.instagram_user_id,
                    container_id,
                )
            except InstagramError as error:
                self._log_instagram_stage_error(
                    "media_publish_failed",
                    job,
                    error,
                    publish_timer,
                    container_id=container_id,
                )

                if error.category in {"timeout", "temporary"}:
                    await self._repository.mark_failed(
                        job.id,
                        "publish_result_unknown",
                        "Resultado da chamada media_publish desconhecido; nao republicado para evitar duplicidade.",
                        {
                            "provider_container_id": container_id,
                            "provider_status": "publish_result_unknown",
                        },
                    )
                    return "failed"

                raise

            self._logger.info(
                "media_publish_completed",
                job_id=job.id,
                campaign_id=job.campaign_id,
                meta_app_id=job.meta_app_id,
                instagram_user_id=job.instagram_user_id,
                provider_container_id=container_id,
                provider_media_id=media_id,
                ms=publish_timer.ms(),
            )
            await self._repository.mark_completed(
                job.id,
                media_id,
                {
                    "provider_container_id": container_id,
                    "provider_media_id": media_id,
                    "provider_status": "completed",
                },
            )
            await self._repository.log_event(
                job,
                "publisher.job.completed",
                "success",
                "Publicacao enviada ao Instagram.",
                {
                    "provider_media_id": media_id,
                    "provider_container_id": container_id,
                    "meta_app_id": job.meta_app_id,
                },
            )
            return "completed"
        finally:
            if temporary_storage_path:
                await self._remove_temporary_storage(job, temporary_storage_path, temporary_processing_uuid)
            await self._video_processor.cleanup(job.id, processed_video)

    async def _temporary_processed_media_url(
        self,
        job: JobContext,
        processed_video: ProcessedVideo,
        fallback_url: str,
    ) -> tuple[str, str | None]:
        timer = Timer()
        try:
            temporary_upload = await self._video_processor.upload_temporary(
                job.storage_bucket,
                processed_video.output_path,
                job.mime_type,
                self._settings.worker_id,
                job.id,
                processed_video.processing_uuid,
            )
            self._logger.info(
                "video_temporary_upload_finished",
                job_id=job.id,
                campaign_id=job.campaign_id,
                media_asset_id=job.media_asset_id,
                storage_bucket=job.storage_bucket,
                storage_path=temporary_upload.storage_path,
                processing_uuid=processed_video.processing_uuid,
                upload_ms=temporary_upload.upload_ms,
                ms=timer.ms(),
            )
            return temporary_upload.signed_url, temporary_upload.storage_path
        except Exception as error:
            self._logger.error(
                "processing_failed",
                job_id=job.id,
                campaign_id=job.campaign_id,
                media_asset_id=job.media_asset_id,
                stage="temporary_storage_upload",
                processing_uuid=processed_video.processing_uuid,
                error_category="upload_error",
                error=str(error),
                ms=timer.ms(),
                exc_info=True,
            )
            return fallback_url, None

    async def _remove_temporary_storage(
        self,
        job: JobContext,
        storage_path: str,
        processing_uuid: str | None,
    ) -> None:
        try:
            await self._video_processor.remove_temporary_storage(
                job.storage_bucket,
                storage_path,
                job.id,
                processing_uuid,
            )
        except Exception as error:
            self._logger.warning(
                "temporary_file_remove_failed",
                job_id=job.id,
                campaign_id=job.campaign_id,
                processing_uuid=processing_uuid,
                storage_bucket=job.storage_bucket,
                path=storage_path,
                kind="temporary_storage_object",
                error=str(error),
            )

    async def _poll_container(self, job: JobContext, token: str, container_id: str) -> None:
        delay = self._settings.polling_initial_seconds
        polling_timer = Timer()
        self._logger.info(
            "container_polling_started",
            job_id=job.id,
            campaign_id=job.campaign_id,
            instagram_user_id=job.instagram_user_id,
            provider_container_id=container_id,
            initial_delay_seconds=delay,
            max_attempts=self._settings.polling_max_attempts,
        )
        for attempt in range(1, self._settings.polling_max_attempts + 1):
            attempt_timer = Timer()
            try:
                status = await self._instagram.get_container_status(token, container_id)
            except InstagramError as error:
                self._log_instagram_stage_error(
                    "container_polling_failed",
                    job,
                    error,
                    attempt_timer,
                    container_id=container_id,
                    attempt=attempt,
                    elapsed_total_ms=polling_timer.ms(),
                    next_delay_seconds=delay,
                )
                raise
            self._logger.info(
                "container_poll",
                job_id=job.id,
                campaign_id=job.campaign_id,
                instagram_user_id=job.instagram_user_id,
                provider_container_id=container_id,
                status=status or "PROCESSING",
                attempt=attempt,
                next_delay_seconds=delay,
                attempt_ms=attempt_timer.ms(),
                elapsed_total_ms=polling_timer.ms(),
            )

            if status in READY_STATUSES:
                await self._repository.patch_metadata(
                    job.id,
                    {"provider_container_id": container_id, "provider_status": "container_ready"},
                )
                self._logger.info(
                    "container_polling_ready",
                    job_id=job.id,
                    campaign_id=job.campaign_id,
                    instagram_user_id=job.instagram_user_id,
                    provider_container_id=container_id,
                    status=status,
                    attempt=attempt,
                    elapsed_total_ms=polling_timer.ms(),
                )
                return
            if status in FAILED_STATUSES:
                self._logger.error(
                    "container_polling_refused",
                    job_id=job.id,
                    campaign_id=job.campaign_id,
                    instagram_user_id=job.instagram_user_id,
                    provider_container_id=container_id,
                    status=status,
                    attempt=attempt,
                    elapsed_total_ms=polling_timer.ms(),
                )
                raise InstagramError(
                    code="container_failed",
                    message="A Meta recusou o processamento da midia.",
                    retryable=False,
                    category="media_invalid",
                    details={"container_id": container_id, "status": status},
                )

            await asyncio.sleep(delay)
            delay = min(delay * 2, self._settings.polling_max_seconds)

        self._logger.warning(
            "container_polling_timeout",
            job_id=job.id,
            campaign_id=job.campaign_id,
            instagram_user_id=job.instagram_user_id,
            provider_container_id=container_id,
            attempts=self._settings.polling_max_attempts,
            elapsed_total_ms=polling_timer.ms(),
        )
        raise InstagramError(
            code="container_timeout",
            message="A Meta nao concluiu o processamento da midia dentro do tempo esperado.",
            retryable=True,
            category="temporary",
            retry_after_seconds=self._settings.retry_base_seconds,
            details={"container_id": container_id},
        )

    def _log_instagram_stage_error(
        self,
        event: str,
        job: JobContext,
        error: InstagramError,
        timer: Timer,
        container_id: str | None = None,
        **extra: object,
    ) -> None:
        details = error.details or {}
        raw_error = details.get("raw_error")
        self._logger.error(
            event,
            job_id=job.id,
            campaign_id=job.campaign_id,
            instagram_user_id=job.instagram_user_id,
            provider_container_id=container_id,
            http_status=error.http_status,
            code=error.code,
            category=error.category,
            retryable=error.retryable,
            message=error.message,
            details=details,
            fbtrace_id=details.get("fbtrace_id"),
            meta_code=details.get("meta_code"),
            meta_subcode=details.get("meta_subcode"),
            error_user_title=details.get("error_user_title"),
            error_user_msg=details.get("error_user_msg"),
            raw_error=raw_error,
            ms=timer.ms(),
            **extra,
        )

    async def _heartbeat(self, job_id: str) -> None:
        delay = float(self._settings.heartbeat_interval_seconds)
        while True:
            try:
                await asyncio.sleep(delay)
                await self._repository.heartbeat(job_id)
                delay = float(self._settings.heartbeat_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                delay = min(
                    delay * 2 + random.uniform(0, self._settings.retry_jitter_seconds or 1),
                    float(self._settings.retry_max_seconds),
                )
                self._logger.warning(
                    "heartbeat_failed",
                    job_id=job_id,
                    error=str(error),
                    retry_in_seconds=round(delay, 2),
                )

    async def _handle_instagram_error(self, job: JobContext, error: InstagramError) -> None:
        if error.category == "token_expired":
            await self._repository.mark_failed(job.id, error.code, error.message, {"provider_status": error.category})
            self._metrics.inc("publish.token_expired")
            return

        if error.category in {"media_invalid", "permission", "validation"} or not error.retryable:
            await self._repository.mark_failed(
                job.id,
                error.code,
                error.message,
                {"provider_status": error.category, "details": error.details or {}},
            )
            self._metrics.inc("publish.failed")
            return

        await self._retry_or_fail(
            job,
            error.code,
            error.message,
            retryable=True,
            retry_after_seconds=error.retry_after_seconds,
            metadata={"provider_status": error.category, "details": error.details or {}},
        )

    async def _retry_or_fail(
        self,
        job: JobContext,
        code: str,
        message: str,
        retryable: bool,
        retry_after_seconds: int | None = None,
        metadata: dict | None = None,
    ) -> None:
        if retryable and job.attempt_count < self._settings.max_attempts:
            retry_at = datetime.now(UTC) + timedelta(
                seconds=retry_after_seconds or retry_delay(job.attempt_count, self._settings)
            )
            await self._repository.schedule_retry(job, code, message, retry_at, metadata)
            self._metrics.inc("publish.retry")
            return

        await self._repository.mark_failed(job.id, code, message, metadata)
        self._metrics.inc("publish.failed")

    def _validate(self, job: JobContext) -> None:
        if job.social_account_status != "active" or job.connection_status != "active":
            raise InstagramError(
                code="account_inactive",
                message="Conta Instagram inativa.",
                retryable=False,
                category="validation",
            )
        if job.requires_reconnect_at or not job.access_token_encrypted:
            raise InstagramError(
                code="token_missing",
                message="Conta Instagram exige reconexao.",
                retryable=False,
                category="token_expired",
            )
        if job.token_expires_at and job.token_expires_at <= datetime.now(UTC):
            raise InstagramError(
                code="token_expired",
                message="Token expirado.",
                retryable=False,
                category="token_expired",
            )
        if job.media_status != "ready" or job.media_deleted_at:
            raise InstagramError(
                code="media_invalid",
                message="Midia invalida ou removida.",
                retryable=False,
                category="media_invalid",
            )
        if job.campaign_type == "reel" and not job.mime_type.startswith("video/"):
            raise InstagramError(
                code="reel_requires_video",
                message="Reel exige video.",
                retryable=False,
                category="media_invalid",
            )


def retry_delay(attempt_count: int, settings: Settings) -> int:
    base = min(settings.retry_base_seconds * (2 ** max(attempt_count - 1, 0)), settings.retry_max_seconds)
    jitter = random.randint(0, settings.retry_jitter_seconds) if settings.retry_jitter_seconds else 0
    return base + jitter


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def decrypt_token(payload: str, secret: str) -> str:
    version, iv_raw, tag_raw, encrypted_raw = payload.split(":")
    if version != "v1":
        raise ValueError("Invalid encrypted token version.")
    key = hashlib.sha256(secret.encode("utf-8")).digest()
    iv = decode_base64url(iv_raw)
    tag = decode_base64url(tag_raw)
    encrypted = decode_base64url(encrypted_raw)
    return AESGCM(key).decrypt(iv, encrypted + tag, None).decode("utf-8")


def decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
