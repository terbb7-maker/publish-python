import asyncio
import hashlib
import json
import random
import secrets
import time
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from supabase import Client, create_client

from app.config import Settings
from app.logger import get_logger
from app.metrics import Metrics


METADATA_FIELDS = (
    "title",
    "artist",
    "album",
    "comment",
    "description",
    "copyright",
    "encoded_by",
    "encoder",
    "composer",
    "publisher",
    "software",
    "creation_time",
    "uuid",
    "build_id",
    "session_id",
)


GENERATION_4_METADATA_FIELDS = (
    "album_artist",
    "genre",
    "organization",
    "keywords",
    "vendor",
    "device",
    "make",
    "model",
    "track",
    "disc",
    "network",
    "synopsis",
    "rating",
    "episode_id",
    "modification_time",
)


BRAND_PROFILES = (
    ("mp42", "mp42isomavc1"),
    ("isom", "isomiso2avc1mp41"),
    ("iso6", "iso6mp41avc1mp42"),
)


DEVICE_PROFILES = (
    {"device": "iPhone", "make": "Apple", "model": "iPhone 15 Pro", "vendor": "appl"},
    {"device": "Samsung", "make": "Samsung", "model": "Galaxy S24", "vendor": "sams"},
    {"device": "Google Pixel", "make": "Google", "model": "Pixel 8 Pro", "vendor": "goog"},
    {"device": "Motorola", "make": "Motorola", "model": "Edge 50", "vendor": "moto"},
    {"device": "Xiaomi", "make": "Xiaomi", "model": "Redmi Note 13", "vendor": "xiai"},
)


ERROR_DOWNLOAD = "download_error"
ERROR_FFMPEG = "ffmpeg_error"
ERROR_FFPROBE = "ffprobe_error"
ERROR_UPLOAD = "upload_error"
ERROR_CLEANUP = "cleanup_error"
ERROR_VALIDATION = "validation_error"
ERROR_TIMEOUT = "timeout_error"
ERROR_UNEXPECTED = "unexpected_error"


@dataclass(slots=True)
class VideoProbe:
    path: Path
    container: str
    duration_seconds: float
    bitrate: int | None
    video_codec: str | None
    width: int | None
    height: int | None
    fps: float | None
    audio_codec: str | None
    audio_bitrate: int | None
    has_video: bool
    has_audio: bool


@dataclass(slots=True)
class ProcessedVideo:
    input_path: Path
    output_path: Path
    processing_uuid: str
    random_seed: int
    metadata: dict[str, str]
    encoder_profile: dict[str, Any]
    original_probe: VideoProbe | None
    processed_probe: VideoProbe | None
    download_ms: int
    ffmpeg_ms: int
    total_ms: int
    original_size_bytes: int
    final_size_bytes: int
    original_sha256: str
    final_sha256: str
    original_hashes: dict[str, str]
    final_hashes: dict[str, str]
    frame_hashes: dict[str, dict[str, str]]


@dataclass(slots=True)
class TemporaryVideoUpload:
    storage_path: str
    signed_url: str
    upload_ms: int


class VideoProcessingError(Exception):
    def __init__(
        self,
        category: str,
        message: str,
        processing_uuid: str,
        elapsed_ms: int,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.processing_uuid = processing_uuid
        self.elapsed_ms = elapsed_ms


class VideoProcessor:
    def __init__(self, settings: Settings, metrics: Metrics | None = None) -> None:
        self._settings = settings
        self._metrics = metrics
        self._logger = get_logger("video_processor")
        self._temp_root = Path(settings.temp_directory)
        self._supabase: Client | None = None

    async def cleanup_stale_files(self) -> None:
        started = time.perf_counter()
        cutoff = datetime.now(UTC) - timedelta(minutes=self._settings.temp_file_max_age_minutes)
        removed = 0
        root = self._temp_root / "video-processing"
        if not root.exists():
            return

        for path in sorted(root.glob("**/*"), reverse=True):
            try:
                if not path.exists():
                    continue
                modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
                if modified > cutoff:
                    continue
                if path.is_file():
                    path.unlink()
                    removed += 1
                    self._logger.info(
                        "temporary_file_removed",
                        path=str(path),
                        kind="startup_stale_file",
                        modified_at=modified.isoformat(),
                    )
                elif path.is_dir():
                    path.rmdir()
                    removed += 1
                    self._logger.info(
                        "temporary_file_removed",
                        path=str(path),
                        kind="startup_stale_directory",
                        modified_at=modified.isoformat(),
                    )
            except OSError:
                continue
            except Exception as error:
                self._record_metric("video_processing.cleanup_failed")
                self._logger.warning(
                    "temporary_file_remove_failed",
                    path=str(path),
                    kind="startup_cleanup",
                    error_category=ERROR_CLEANUP,
                    error=str(error),
                )

        self._record_metric("video_processing.cleanup_ms", elapsed_ms(started))
        self._logger.info(
            "video_processing_startup_cleanup_finished",
            temp_directory=str(root),
            removed=removed,
            max_age_minutes=self._settings.temp_file_max_age_minutes,
            cleanup_ms=elapsed_ms(started),
        )

    async def process(self, source_url: str, mime_type: str, job_id: str) -> ProcessedVideo | None:
        if not self._settings.enable_video_processing or not mime_type.startswith("video/"):
            return None

        started = time.perf_counter()
        processing_uuid = str(uuid4())
        random_seed = self._settings.video_random_seed or secrets.randbits(64)
        rng = random.Random(random_seed)
        work_dir = self._temp_root / "video-processing" / job_id / processing_uuid
        extension = extension_for(mime_type, source_url)
        input_path = work_dir / f"source-{processing_uuid}{extension}"
        output_path = work_dir / f"processed-{processing_uuid}.mp4"
        metadata = random_metadata(self._settings, rng, processing_uuid, random_seed)
        encoder_profile = random_encoder_profile(self._settings, rng)
        download_ms = 0
        ffmpeg_ms = 0
        original_probe: VideoProbe | None = None
        processed_probe: VideoProbe | None = None
        original_hashes: dict[str, str] = {}
        final_hashes: dict[str, str] = {}
        frame_hashes: dict[str, dict[str, str]] = {}

        self._record_metric("video_processing.started")
        self._logger.info(
            "video_processing_started",
            job_id=job_id,
            processing_uuid=processing_uuid,
            random_seed=random_seed,
            mime_type=mime_type,
            mode=self._settings.video_processing_mode,
            preset=self._settings.processing_preset,
            temp_directory=str(work_dir),
            ffmpeg_path=self._settings.ffmpeg_path,
            ffprobe_path=self._settings.ffprobe_path,
            ffmpeg_timeout_seconds=self._settings.ffmpeg_timeout_seconds,
            dry_run=self._settings.video_processing_dry_run,
            randomize_metadata=self._settings.video_randomize_metadata,
            rebuild_container=self._settings.video_rebuild_container,
            randomize_encoder=self._settings.video_randomize_encoder,
            randomize_audio=self._settings.video_randomize_audio,
            randomize_timestamps=self._settings.video_randomize_timestamps,
            randomize_uuid=self._settings.video_randomize_uuid,
            randomize_brands=self._settings.video_randomize_brands,
        )

        try:
            work_dir.mkdir(parents=True, exist_ok=True)
            download_started = time.perf_counter()
            await self._download(source_url, input_path, job_id, processing_uuid)
            download_ms = elapsed_ms(download_started)
            self._record_metric("video_processing.download_ms", download_ms)

            original_size = file_size(input_path)
            original_hashes = await asyncio.to_thread(file_hashes, input_path)
            original_hash = original_hashes["sha256"]
            original_probe = await self._probe(input_path, job_id, processing_uuid, "original")
            self._validate_original_probe(original_probe, processing_uuid, elapsed_ms(started))
            filter_profile = build_video_filters(
                self._settings,
                rng,
                original_probe,
            )
            encoder_profile["video_filters"] = filter_profile["filters"]
            encoder_profile["video_filter_graph"] = filter_profile["graph"]
            encoder_profile["fps_target"] = filter_profile["fps_target"]
            self._logger.info(
                "temporary_file_created",
                job_id=job_id,
                processing_uuid=processing_uuid,
                path=str(input_path),
                kind="downloaded_source",
                size_bytes=original_size,
                **hash_log_fields("original", original_hashes, self._settings),
                download_ms=download_ms,
                **probe_log_fields("original", original_probe),
            )

            self._logger.info("metadata_removed", job_id=job_id, processing_uuid=processing_uuid, strategy="-map_metadata -1")
            self._logger.info(
                "metadata_generated",
                job_id=job_id,
                processing_uuid=processing_uuid,
                metadata=metadata,
                metadata_keys=list(metadata.keys()),
            )
            self._logger.info(
                "encoder_randomized",
                job_id=job_id,
                processing_uuid=processing_uuid,
                enabled=self._settings.video_randomize_encoder,
                encoder_profile=encoder_profile,
            )
            self._logger.info(
                "video_processing_filters",
                job_id=job_id,
                processing_uuid=processing_uuid,
                filters=encoder_profile.get("video_filters", []),
                fps_target=encoder_profile.get("fps_target"),
            )
            self._logger.info(
                "audio_reencoded",
                job_id=job_id,
                processing_uuid=processing_uuid,
                enabled=self._settings.video_randomize_audio,
                has_audio=original_probe.has_audio,
                audio_codec=encoder_profile["audio_codec"],
                audio_bitrate=encoder_profile["audio_bitrate"],
                audio_sample_rate=encoder_profile["audio_sample_rate"],
            )

            ffmpeg_started = time.perf_counter()
            await self._run_ffmpeg(
                input_path,
                output_path,
                metadata,
                encoder_profile,
                job_id,
                processing_uuid,
                original_probe.has_audio,
            )
            ffmpeg_ms = elapsed_ms(ffmpeg_started)
            self._record_metric("video_processing.ffmpeg_ms", ffmpeg_ms)

            final_size = file_size(output_path)
            final_hashes = await asyncio.to_thread(file_hashes, output_path)
            final_hash = final_hashes["sha256"]
            processed_probe = await self._probe(output_path, job_id, processing_uuid, "processed")
            self._validate_processed_probe(
                original_probe,
                processed_probe,
                output_path,
                processing_uuid,
                elapsed_ms(started),
            )

            self._logger.info(
                "temporary_file_created",
                job_id=job_id,
                processing_uuid=processing_uuid,
                path=str(output_path),
                kind="processed_video",
                size_bytes=final_size,
                **hash_log_fields("processed", final_hashes, self._settings),
                **probe_log_fields("processed", processed_probe),
            )
            frame_hashes = await self._frame_hashes(
                input_path,
                output_path,
                original_probe,
                processed_probe,
                job_id,
                processing_uuid,
            )
            self._logger.info(
                "container_rebuilt",
                job_id=job_id,
                processing_uuid=processing_uuid,
                major_brand=encoder_profile["major_brand"],
                compatible_brands=encoder_profile["compatible_brands"],
                movflags="+faststart+use_metadata_tags",
                ffmpeg_ms=ffmpeg_ms,
            )

            total_ms = elapsed_ms(started)
            self._record_metric("video_processing.success")
            self._record_metric("video_processing.total_ms", total_ms)
            self._logger.info(
                "video_processing_finished",
                job_id=job_id,
                processing_uuid=processing_uuid,
                random_seed=random_seed,
                video_processing_seed=random_seed,
                video_processing_uuid=processing_uuid,
                video_processing_duration=total_ms,
                video_processing_profile=encoder_profile.get("device_profile"),
                video_processing_filters=encoder_profile.get("video_filters", []),
                video_processing_hash_before=original_hashes,
                video_processing_hash_after=final_hashes,
                video_processing_size_before=original_size,
                video_processing_size_after=final_size,
                video_processing_audio_changed=original_probe.has_audio,
                video_processing_encoder={
                    key: encoder_profile.get(key)
                    for key in (
                        "preset",
                        "crf",
                        "video_profile",
                        "video_level",
                        "gop",
                        "keyint_min",
                        "bframes",
                        "cabac",
                        "open_gop",
                        "scene_cut",
                    )
                },
                ffmpeg_version=await self._ffmpeg_version(),
                download_ms=download_ms,
                processing_ms=ffmpeg_ms,
                ffmpeg_ms=ffmpeg_ms,
                total_ms=total_ms,
                original_size=original_size,
                processed_size=final_size,
                original_sha256=original_hash,
                processed_sha256=final_hash,
                original_md5=original_hashes["md5"],
                processed_md5=final_hashes["md5"],
                original_crc32=original_hashes["crc32"],
                processed_crc32=final_hashes["crc32"],
                frame_hashes=frame_hashes,
                output_path=str(output_path),
                **probe_comparison_fields(original_probe, processed_probe),
            )
            return ProcessedVideo(
                input_path=input_path,
                output_path=output_path,
                processing_uuid=processing_uuid,
                random_seed=random_seed,
                metadata=metadata,
                encoder_profile=encoder_profile,
                original_probe=original_probe,
                processed_probe=processed_probe,
                download_ms=download_ms,
                ffmpeg_ms=ffmpeg_ms,
                total_ms=total_ms,
                original_size_bytes=original_size,
                final_size_bytes=final_size,
                original_sha256=original_hash,
                final_sha256=final_hash,
                original_hashes=original_hashes,
                final_hashes=final_hashes,
                frame_hashes=frame_hashes,
            )
        except VideoProcessingError as error:
            self._record_failure_metric(error.category)
            self._logger.error(
                "processing_failed",
                job_id=job_id,
                processing_uuid=error.processing_uuid,
                error_category=error.category,
                exception=str(error),
                elapsed_ms=error.elapsed_ms,
                input_path=str(input_path),
                output_path=str(output_path),
                download_ms=download_ms,
                ffmpeg_ms=ffmpeg_ms,
                exc_info=True,
            )
            await self.cleanup(job_id, fallback_processed_video(
                input_path,
                output_path,
                processing_uuid,
                random_seed,
                metadata,
                encoder_profile,
                original_probe,
                processed_probe,
                original_hashes,
                final_hashes,
                frame_hashes,
                download_ms,
                ffmpeg_ms,
                elapsed_ms(started),
            ))
            self._record_metric("video_processing.fallback")
            return None
        except Exception as error:
            self._record_failure_metric(ERROR_UNEXPECTED)
            self._logger.error(
                "processing_failed",
                job_id=job_id,
                processing_uuid=processing_uuid,
                error_category=ERROR_UNEXPECTED,
                exception=str(error),
                elapsed_ms=elapsed_ms(started),
                input_path=str(input_path),
                output_path=str(output_path),
                download_ms=download_ms,
                ffmpeg_ms=ffmpeg_ms,
                exc_info=True,
            )
            await self.cleanup(job_id, fallback_processed_video(
                input_path,
                output_path,
                processing_uuid,
                random_seed,
                metadata,
                encoder_profile,
                original_probe,
                processed_probe,
                original_hashes,
                final_hashes,
                frame_hashes,
                download_ms,
                ffmpeg_ms,
                elapsed_ms(started),
            ))
            self._record_metric("video_processing.fallback")
            return None

    async def upload_temporary(
        self,
        bucket: str,
        local_path: Path,
        mime_type: str,
        worker_id: str,
        job_id: str,
        processing_uuid: str,
    ) -> TemporaryVideoUpload:
        timer = time.perf_counter()
        storage_path = (
            f"_publisher_tmp/{worker_id}/{job_id}/{processing_uuid}/"
            f"{processing_uuid}{local_path.suffix or '.mp4'}"
        )

        def upload() -> None:
            with local_path.open("rb") as file:
                response = self._client().storage.from_(bucket).upload(
                    storage_path,
                    file,
                    {
                        "content-type": mime_type,
                        "x-upsert": "true",
                    },
                )
                if isinstance(response, dict) and response.get("error"):
                    raise RuntimeError(str(response["error"]))

        try:
            await asyncio.to_thread(upload)
            signed_url = await self.signed_storage_url(bucket, storage_path)
            upload_ms = elapsed_ms(timer)
            self._record_metric("video_processing.upload_ms", upload_ms)
            self._logger.info(
                "temporary_file_created",
                job_id=job_id,
                processing_uuid=processing_uuid,
                path=storage_path,
                kind="temporary_storage_object",
                bucket=bucket,
                upload_ms=upload_ms,
            )
            return TemporaryVideoUpload(storage_path=storage_path, signed_url=signed_url, upload_ms=upload_ms)
        except Exception as error:
            self._record_failure_metric(ERROR_UPLOAD)
            raise VideoProcessingError(
                ERROR_UPLOAD,
                str(error),
                processing_uuid,
                elapsed_ms(timer),
            ) from error

    async def signed_storage_url(self, bucket: str, path: str) -> str:
        response = await asyncio.to_thread(
            lambda: self._client().storage.from_(bucket).create_signed_url(
                path,
                self._settings.media_signed_url_ttl_seconds,
            )
        )
        signed_url = response.get("signedURL") if isinstance(response, dict) else None
        if not signed_url:
            raise RuntimeError("Could not create signed URL for processed video.")
        return str(signed_url)

    async def remove_temporary_storage(self, bucket: str, path: str, job_id: str, processing_uuid: str | None = None) -> None:
        started = time.perf_counter()
        try:
            await asyncio.to_thread(lambda: self._client().storage.from_(bucket).remove([path]))
            cleanup_ms = elapsed_ms(started)
            self._record_metric("video_processing.cleanup_ms", cleanup_ms)
            self._logger.info(
                "temporary_file_removed",
                job_id=job_id,
                processing_uuid=processing_uuid,
                path=path,
                kind="temporary_storage_object",
                bucket=bucket,
                cleanup_ms=cleanup_ms,
            )
        except Exception as error:
            self._record_failure_metric(ERROR_CLEANUP)
            self._logger.warning(
                "temporary_file_remove_failed",
                job_id=job_id,
                processing_uuid=processing_uuid,
                path=path,
                kind="temporary_storage_object",
                bucket=bucket,
                error_category=ERROR_CLEANUP,
                exception=str(error),
                elapsed_ms=elapsed_ms(started),
            )

    async def cleanup(self, job_id: str, processed: ProcessedVideo | None) -> None:
        if not processed:
            return

        started = time.perf_counter()
        for path in (processed.input_path, processed.output_path):
            try:
                if path.exists():
                    size = file_size(path)
                    path.unlink()
                    self._logger.info(
                        "temporary_file_removed",
                        job_id=job_id,
                        processing_uuid=processed.processing_uuid,
                        path=str(path),
                        kind="local_file",
                        size_bytes=size,
                    )
            except Exception as error:
                self._record_failure_metric(ERROR_CLEANUP)
                self._logger.warning(
                    "temporary_file_remove_failed",
                    job_id=job_id,
                    processing_uuid=processed.processing_uuid,
                    path=str(path),
                    kind="local_file",
                    error_category=ERROR_CLEANUP,
                    exception=str(error),
                    elapsed_ms=elapsed_ms(started),
                )

        await asyncio.to_thread(
            remove_empty_parents,
            processed.input_path.parent,
            self._temp_root,
        )
        self._record_metric("video_processing.cleanup_ms", elapsed_ms(started))

    async def _download(self, source_url: str, destination: Path, job_id: str, processing_uuid: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=self._settings.http_timeout_seconds) as client:
                async with client.stream("GET", source_url) as response:
                    response.raise_for_status()
                    with destination.open("wb") as file:
                        async for chunk in response.aiter_bytes():
                            file.write(chunk)
        except Exception as error:
            raise VideoProcessingError(ERROR_DOWNLOAD, str(error), processing_uuid, 0) from error

        self._logger.info(
            "video_source_downloaded",
            job_id=job_id,
            processing_uuid=processing_uuid,
            path=str(destination),
            size_bytes=file_size(destination),
        )

    async def _run_ffmpeg(
        self,
        input_path: Path,
        output_path: Path,
        metadata: dict[str, str],
        profile: dict[str, Any],
        job_id: str,
        processing_uuid: str,
        has_audio: bool,
    ) -> None:
        started = time.perf_counter()
        command = build_ffmpeg_command(
            self._settings,
            input_path,
            output_path,
            metadata,
            profile,
            has_audio,
        )
        self._logger.info(
            "video_processing_ffmpeg_command",
            job_id=job_id,
            processing_uuid=processing_uuid,
            command=safe_command(command),
        )
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self._settings.ffmpeg_timeout_seconds,
            )
        except TimeoutError as error:
            process.kill()
            await process.communicate()
            raise VideoProcessingError(
                ERROR_TIMEOUT,
                f"FFmpeg timed out after {self._settings.ffmpeg_timeout_seconds}s",
                processing_uuid,
                elapsed_ms(started),
            ) from error

        stdout_text = stdout.decode("utf-8", errors="replace")
        stderr_text = stderr.decode("utf-8", errors="replace")

        if process.returncode != 0:
            raise VideoProcessingError(
                ERROR_FFMPEG,
                f"FFmpeg failed with exit code {process.returncode}: {stderr_text[:4000]}",
                processing_uuid,
                elapsed_ms(started),
            )

        self._logger.info(
            "video_ffmpeg_completed",
            job_id=job_id,
            processing_uuid=processing_uuid,
            command=safe_command(command),
            stdout=stdout_text[:2000],
            stderr=stderr_text[:4000],
            processing_ms=elapsed_ms(started),
        )

    async def _probe(self, path: Path, job_id: str, processing_uuid: str, label: str) -> VideoProbe:
        started = time.perf_counter()
        command = [
            self._settings.ffprobe_path,
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(path),
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                raise VideoProcessingError(
                    ERROR_FFPROBE,
                    stderr.decode("utf-8", errors="replace")[:4000],
                    processing_uuid,
                    elapsed_ms(started),
                )
            payload = json.loads(stdout.decode("utf-8", errors="replace") or "{}")
            probe = parse_probe(path, payload)
            self._logger.info(
                "video_probe_finished",
                job_id=job_id,
                processing_uuid=processing_uuid,
                label=label,
                ffprobe_ms=elapsed_ms(started),
                **probe_log_fields(label, probe),
            )
            return probe
        except VideoProcessingError:
            raise
        except Exception as error:
            raise VideoProcessingError(
                ERROR_FFPROBE,
                str(error),
                processing_uuid,
                elapsed_ms(started),
            ) from error

    async def _frame_hashes(
        self,
        original_path: Path,
        processed_path: Path,
        original_probe: VideoProbe,
        processed_probe: VideoProbe,
        job_id: str,
        processing_uuid: str,
    ) -> dict[str, dict[str, str]]:
        if not self._settings.enable_frame_hash:
            return {}

        started = time.perf_counter()
        try:
            original_hashes = await self._perceptual_hashes(original_path, original_probe)
            processed_hashes = await self._perceptual_hashes(processed_path, processed_probe)
            result = {"original": original_hashes, "processed": processed_hashes}
            self._logger.info(
                "video_frame_hash_finished",
                job_id=job_id,
                processing_uuid=processing_uuid,
                frame_hashes=result,
                frame_hash_ms=elapsed_ms(started),
            )
            return result
        except Exception as error:
            self._logger.warning(
                "video_frame_hash_failed",
                job_id=job_id,
                processing_uuid=processing_uuid,
                error_category="frame_hash_error",
                exception=str(error),
                elapsed_ms=elapsed_ms(started),
            )
            return {}

    async def _perceptual_hashes(self, path: Path, probe: VideoProbe) -> dict[str, str]:
        duration = max(probe.duration_seconds, 0.001)
        timestamps = {
            "first": 0.0,
            "middle": max(duration / 2, 0.0),
            "last": max(duration - 0.05, 0.0),
        }
        hashes: dict[str, str] = {}
        for label, timestamp in timestamps.items():
            hashes[label] = await self._perceptual_hash(path, timestamp)
        return hashes

    async def _perceptual_hash(self, path: Path, timestamp: float) -> str:
        command = [
            self._settings.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{timestamp:.3f}",
            "-i",
            str(path),
            "-frames:v",
            "1",
            "-vf",
            "scale=16:16,format=gray",
            "-f",
            "rawvideo",
            "pipe:1",
        ]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
        if process.returncode != 0 or not stdout:
            raise RuntimeError(stderr.decode("utf-8", errors="replace")[:1000])
        average = sum(stdout) / len(stdout)
        bits = "".join("1" if value >= average else "0" for value in stdout)
        return f"{int(bits, 2):0{len(bits) // 4}x}"

    async def _ffmpeg_version(self) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                self._settings.ffmpeg_path,
                "-version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=5)
            return stdout.decode("utf-8", errors="replace").splitlines()[0][:240]
        except Exception:
            return "unknown"

    def _validate_original_probe(
        self,
        probe: VideoProbe,
        processing_uuid: str,
        elapsed: int,
    ) -> None:
        if not probe.path.exists():
            raise VideoProcessingError(ERROR_VALIDATION, "Source file does not exist.", processing_uuid, elapsed)
        if not probe.has_video or not probe.video_codec:
            raise VideoProcessingError(ERROR_VALIDATION, "Source file has no valid video stream.", processing_uuid, elapsed)
        if not probe.duration_seconds or probe.duration_seconds <= 0:
            raise VideoProcessingError(ERROR_VALIDATION, "Source duration is invalid.", processing_uuid, elapsed)
        if not probe.width or not probe.height:
            raise VideoProcessingError(ERROR_VALIDATION, "Source resolution is invalid.", processing_uuid, elapsed)
        if not probe.container:
            raise VideoProcessingError(ERROR_VALIDATION, "Source container is invalid.", processing_uuid, elapsed)

    def _validate_processed_probe(
        self,
        original: VideoProbe,
        processed: VideoProbe,
        output_path: Path,
        processing_uuid: str,
        elapsed: int,
    ) -> None:
        if not output_path.exists() or file_size(output_path) <= 0:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed file does not exist or is empty.", processing_uuid, elapsed)
        if not processed.has_video or not processed.video_codec:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed file has no valid video stream.", processing_uuid, elapsed)
        if not processed.duration_seconds or processed.duration_seconds <= 0:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed duration is invalid.", processing_uuid, elapsed)
        if not processed.width or not processed.height:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed resolution is invalid.", processing_uuid, elapsed)
        if original.has_audio and not processed.has_audio:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed file lost the original audio stream.", processing_uuid, elapsed)
        if not processed.container:
            raise VideoProcessingError(ERROR_VALIDATION, "Processed container is invalid.", processing_uuid, elapsed)

    def _client(self) -> Client:
        if not self._supabase:
            self._supabase = create_client(
                str(self._settings.supabase_url),
                self._settings.supabase_service_role_key.get_secret_value(),
            )
        return self._supabase

    def _record_metric(self, key: str, value: int = 1) -> None:
        if self._metrics:
            self._metrics.inc(key, value)

    def _record_failure_metric(self, category: str) -> None:
        self._record_metric("video_processing.failed")
        if category == ERROR_TIMEOUT:
            self._record_metric("video_processing.timeout")
        elif category == ERROR_FFPROBE:
            self._record_metric("video_processing.ffprobe_failed")
        elif category == ERROR_CLEANUP:
            self._record_metric("video_processing.cleanup_failed")


def build_ffmpeg_command(
    settings: Settings,
    input_path: Path,
    output_path: Path,
    metadata: dict[str, str],
    profile: dict[str, Any],
    has_audio: bool,
) -> list[str]:
    command = [
        settings.ffmpeg_path,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "+genpts",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
    ]

    for key, value in metadata.items():
        command.extend(["-metadata", f"{key}={value}"])

    command.extend(
        [
            "-metadata:s:v:0",
            f"handler_name={profile['video_handler_name']}",
            "-metadata:s:v:0",
            f"vendor_id={profile['vendor_id']}",
            "-metadata",
            f"major_brand={profile['major_brand']}",
            "-metadata",
            f"compatible_brands={profile['compatible_brands']}",
            "-metadata",
            f"minor_version={profile['minor_version']}",
            "-vf",
            profile["video_filter_graph"],
            "-c:v",
            "libx264",
            "-preset",
            profile["preset"],
            "-crf",
            str(profile["crf"]),
            "-g",
            str(profile["gop"]),
            "-keyint_min",
            str(profile["keyint_min"]),
            "-sc_threshold",
            str(profile["scene_cut"]),
            "-refs",
            str(profile["refs"]),
            "-bf",
            str(profile["bframes"]),
            "-trellis",
            str(profile["trellis"]),
            "-subq",
            str(profile["subme"]),
            "-coder",
            "1" if profile["cabac"] else "0",
            "-profile:v",
            profile["video_profile"],
            "-level:v",
            profile["video_level"],
            "-pix_fmt",
            "yuv420p",
            "-b:v",
            profile["target_bitrate"],
            "-maxrate",
            profile["maxrate"],
            "-bufsize",
            profile["bufsize"],
            "-threads",
            str(profile["threads"]),
            "-x264-params",
            (
                f"keyint={profile['gop']}:min-keyint={profile['keyint_min']}:"
                f"scenecut={profile['scene_cut']}:rc-lookahead={profile['lookahead']}:"
                f"slices={profile['slices']}:sliced-threads=1:"
                f"aq-mode={profile['aq_mode']}:aq-strength={profile['aq_strength']}:"
                f"psy-rd={profile['psy_rd']}:me={profile['me']}:merange={profile['merange']}:"
                f"deblock={profile['deblock']}:open-gop={profile['open_gop']}:"
                f"cabac={profile['cabac']}:ipratio={profile['ipratio']}:pbratio={profile['pbratio']}:"
                f"qpmin={profile['qpmin']}:qpmax={profile['qpmax']}"
            ),
        ]
    )

    if has_audio:
        command.extend(
            [
                "-metadata:s:a:0",
                f"handler_name={profile['audio_handler_name']}",
                "-af",
                profile["audio_filter_graph"],
                "-c:a",
                profile["audio_codec"],
                "-b:a",
                profile["audio_bitrate"],
                "-ar",
                str(profile["audio_sample_rate"]),
            ]
        )

    command.extend(
        [
            "-movflags",
            "+faststart+use_metadata_tags",
            "-brand",
            profile["major_brand"],
            "-avoid_negative_ts",
            "make_zero",
            str(output_path),
        ]
    )
    return command


def random_metadata(settings: Settings, rng: random.Random, processing_uuid: str, random_seed: int) -> dict[str, str]:
    now = datetime.now(UTC)
    creation_time = now.isoformat().replace("+00:00", "Z")
    modification_time = (now + timedelta(milliseconds=rng.randint(1, 999))).isoformat().replace("+00:00", "Z")
    device_profile = rng.choice(DEVICE_PROFILES) if settings.enable_device_profile else DEVICE_PROFILES[0]
    metadata = {
        "title": f"Terbb Publish {random_token(rng, 12)}",
        "artist": f"Creator {random_token(rng, 10)}",
        "album": f"Campaign {random_token(rng, 10)}",
        "album_artist": f"Creator Group {random_token(rng, 8)}",
        "genre": rng.choice(("Social", "Lifestyle", "Creator", "Business", "Video")),
        "comment": f"Prepared by Terbb {processing_uuid}",
        "description": f"Session {random_token(rng, 20)} at {creation_time}",
        "keywords": ",".join(f"k{random_token(rng, 4)}" for _ in range(4)),
        "copyright": f"Copyright {now.year} {random_token(rng, 16)}",
        "encoded_by": f"terbb-python-publisher-{random_token(rng, 10)}",
        "encoder": f"ffmpeg-terbb-{random_token(rng, 10)}",
        "composer": f"Composer {random_token(rng, 10)}",
        "publisher": f"Publisher {random_token(rng, 10)}",
        "organization": f"Org {random_token(rng, 10)}",
        "software": f"Terbb Video Processor {random_token(rng, 10)}",
        "vendor": device_profile["vendor"],
        "device": device_profile["device"],
        "make": device_profile["make"],
        "model": device_profile["model"],
        "track": str(rng.randint(1, 99)),
        "disc": str(rng.randint(1, 9)),
        "network": f"Network {random_token(rng, 8)}",
        "synopsis": f"Clip {random_token(rng, 18)}",
        "rating": rng.choice(("clean", "general", "unrated")),
        "episode_id": random_token(rng, 14),
        "creation_time": creation_time,
        "modification_time": modification_time,
        "uuid": processing_uuid,
        "build_id": f"{random_seed:x}-{random_token(rng, 16)}",
        "session_id": random_token(rng, 24),
    }
    if not settings.video_randomize_metadata or not settings.enable_metadata_randomization:
        return {"creation_time": creation_time, "uuid": processing_uuid}
    if not settings.video_randomize_uuid:
        metadata.pop("uuid", None)
    if not settings.video_randomize_timestamps:
        metadata.pop("creation_time", None)
    return metadata


def random_encoder_profile(settings: Settings, rng: random.Random) -> dict[str, Any]:
    presets = {
        "fast": {
            "presets": ("veryfast", "faster", "fast"),
            "crf": (20, 24),
            "gop": (48, 72),
            "lookahead": (8, 18),
            "threads": (2, 4),
            "bitrate": (3500, 5200),
        },
        "balanced": {
            "presets": ("fast", "medium"),
            "crf": (18, 22),
            "gop": (54, 84),
            "lookahead": (16, 28),
            "threads": (2, 6),
            "bitrate": (4500, 7000),
        },
        "quality": {
            "presets": ("medium", "slow"),
            "crf": (17, 20),
            "gop": (60, 96),
            "lookahead": (24, 40),
            "threads": (2, 8),
            "bitrate": (6000, 9000),
        },
    }
    selected = presets[settings.processing_preset]
    major_brand, compatible_brands = rng.choice(BRAND_PROFILES)
    if not settings.video_randomize_brands:
        major_brand, compatible_brands = ("mp42", "mp42isomavc1")

    if settings.video_randomize_encoder:
        preset = rng.choice(selected["presets"])
        crf = rng.randint(*selected["crf"])
        gop = rng.randint(*selected["gop"])
        lookahead = rng.randint(*selected["lookahead"])
        threads = rng.randint(*selected["threads"])
        slices = rng.randint(1, 3)
        scene_cut = rng.randint(32, 48)
        refs = rng.randint(2, 4)
        bframes = rng.randint(2, 4)
        cabac = rng.choice((0, 1))
        open_gop = rng.choice((0, 1))
        trellis = rng.randint(1, 2)
        subme = rng.randint(6, 9)
        aq_mode = rng.choice((1, 2, 3))
        aq_strength = round(rng.uniform(0.80, 1.15), 2)
        psy_rd = f"{round(rng.uniform(0.75, 1.05), 2)},{round(rng.uniform(0.00, 0.12), 2)}"
        me = rng.choice(("hex", "umh"))
        merange = rng.randint(16, 28)
        deblock = f"{rng.randint(-2, 1)},{rng.randint(-2, 1)}"
        ipratio = round(rng.uniform(1.30, 1.45), 2)
        pbratio = round(rng.uniform(1.20, 1.35), 2)
        qpmin = rng.randint(8, 12)
        qpmax = rng.randint(44, 48)
        target_bitrate_number = rng.randint(*selected["bitrate"])
    else:
        preset = selected["presets"][0]
        crf = sum(selected["crf"]) // 2
        gop = sum(selected["gop"]) // 2
        lookahead = sum(selected["lookahead"]) // 2
        threads = selected["threads"][0]
        slices = 1
        scene_cut = 40
        refs = 3
        bframes = 3
        cabac = 1
        open_gop = 0
        trellis = 1
        subme = 7
        aq_mode = 1
        aq_strength = 1.0
        psy_rd = "1.00,0.00"
        me = "hex"
        merange = 16
        deblock = "0,0"
        ipratio = 1.4
        pbratio = 1.3
        qpmin = 10
        qpmax = 46
        target_bitrate_number = sum(selected["bitrate"]) // 2

    audio_rates = (44100, 48000) if settings.video_randomize_audio else (48000,)
    audio_bitrates = ("128k", "160k", "192k") if settings.video_randomize_audio else ("160k",)
    audio_volume = round(rng.uniform(0.995, 1.005), 4) if settings.enable_audio_variation else 1.0
    audio_delay_ms = rng.randint(0, 4) if settings.enable_audio_variation else 0
    maxrate_number = int(target_bitrate_number * rng.uniform(1.08, 1.22))
    bufsize_number = maxrate_number * rng.randint(2, 3)
    device_profile = rng.choice(DEVICE_PROFILES) if settings.enable_device_profile else DEVICE_PROFILES[0]

    return {
        "preset": preset,
        "crf": crf,
        "gop": gop,
        "keyint_min": max(1, gop // 2),
        "scene_cut": scene_cut,
        "refs": refs,
        "bframes": bframes,
        "cabac": cabac,
        "open_gop": open_gop,
        "trellis": trellis,
        "subme": subme,
        "aq_mode": aq_mode,
        "aq_strength": aq_strength,
        "psy_rd": psy_rd,
        "me": me,
        "merange": merange,
        "deblock": deblock,
        "ipratio": ipratio,
        "pbratio": pbratio,
        "qpmin": qpmin,
        "qpmax": qpmax,
        "lookahead": lookahead,
        "target_bitrate": f"{target_bitrate_number}k",
        "maxrate": f"{maxrate_number}k",
        "bufsize": f"{bufsize_number}k",
        "threads": threads,
        "slices": slices,
        "video_profile": "high",
        "video_level": "4.1",
        "audio_codec": "aac",
        "audio_bitrate": rng.choice(audio_bitrates),
        "audio_sample_rate": rng.choice(audio_rates),
        "audio_volume": audio_volume,
        "audio_delay_ms": audio_delay_ms,
        "audio_filter_graph": build_audio_filter(audio_volume, audio_delay_ms),
        "major_brand": major_brand,
        "compatible_brands": compatible_brands,
        "minor_version": rng.randint(0, 1024),
        "video_handler_name": f"VideoHandler-{random_token(rng, 8)}",
        "audio_handler_name": f"AudioHandler-{random_token(rng, 8)}",
        "vendor_id": random_token(rng, 8),
        "device_profile": device_profile,
        "video_filters": [],
        "video_filter_graph": "format=yuv420p",
        "fps_target": None,
    }


def build_video_filters(settings: Settings, rng: random.Random, probe: VideoProbe) -> dict[str, Any]:
    width = even_dimension(probe.width or 1080)
    height = even_dimension(probe.height or 1920)
    filters: list[dict[str, Any]] = []
    graph: list[str] = []

    if settings.enable_crop and settings.enable_video_variation and width > 16 and height > 16:
        crop_pixels = rng.choice((2, 4))
        crop_w = even_dimension(max(width - crop_pixels, 16))
        crop_h = even_dimension(max(height - crop_pixels, 16))
        max_x = max(width - crop_w, 0)
        max_y = max(height - crop_h, 0)
        x = rng.randint(0, max_x) if max_x else 0
        y = rng.randint(0, max_y) if max_y else 0
        graph.append(f"crop={crop_w}:{crop_h}:{x}:{y}")
        graph.append(f"scale={width}:{height}:flags=bicubic")
        filters.append({"type": "crop_invisible", "pixels": crop_pixels, "x": x, "y": y})

    if settings.enable_scale_variation and settings.enable_video_variation:
        factor = rng.choice((0.998, 0.999, 1.001, 1.002))
        scaled_w = even_dimension(max(round(width * factor), 16))
        scaled_h = even_dimension(max(round(height * factor), 16))
        if scaled_w == width:
            scaled_w = even_dimension(width + rng.choice((-2, 2)))
        if scaled_h == height:
            scaled_h = even_dimension(height + rng.choice((-2, 2)))
        graph.append(f"scale={scaled_w}:{scaled_h}:flags=bicubic")
        graph.append(f"scale={width}:{height}:flags=bicubic")
        filters.append({"type": "scale_micro", "factor": factor, "width": scaled_w, "height": scaled_h})

    if settings.enable_color_variation and settings.enable_video_variation:
        candidates = [
            ("brightness", f"eq=brightness={rng.uniform(-0.03, 0.03):.4f}"),
            ("contrast", f"eq=contrast={rng.uniform(0.97, 1.03):.4f}"),
            ("saturation", f"eq=saturation={rng.uniform(0.97, 1.03):.4f}"),
            ("gamma", f"eq=gamma={rng.uniform(0.99, 1.01):.4f}"),
            ("sharpen", f"unsharp=3:3:{rng.uniform(0.05, 0.16):.3f}:3:3:0.0"),
            ("denoise", f"hqdn3d={rng.uniform(0.10, 0.35):.3f}:{rng.uniform(0.10, 0.35):.3f}:{rng.uniform(0.15, 0.40):.3f}:{rng.uniform(0.15, 0.40):.3f}"),
            ("temperature", f"colorchannelmixer=rr={rng.uniform(0.997, 1.003):.4f}:bb={rng.uniform(0.997, 1.003):.4f}"),
            ("shadows", f"eq=brightness={rng.uniform(-0.004, 0.004):.4f}:gamma_weight={rng.uniform(0.98, 1.00):.4f}"),
        ]
        count = rng.randint(2, 5)
        for name, expression in rng.sample(candidates, count):
            graph.append(expression)
            filters.append({"type": name, "expression": expression})

    fps_target = choose_fps_target(settings, rng, probe.fps)
    if fps_target:
        graph.append(f"fps=fps={fps_target:.3f}")
        filters.append({"type": "fps_micro", "fps": round(fps_target, 3)})

    graph.append("format=yuv420p")
    return {"graph": ",".join(graph), "filters": filters, "fps_target": fps_target}


def choose_fps_target(settings: Settings, rng: random.Random, source_fps: float | None) -> float | None:
    if not settings.enable_fps_variation or not settings.enable_video_variation:
        return None
    if not source_fps:
        return None
    if 58 <= source_fps <= 61:
        return rng.choice((59.94, 59.98, 60.0, 60.01))
    if 28 <= source_fps <= 31:
        return rng.choice((29.97, 29.98, 30.0, 30.01))
    delta = rng.choice((-0.02, -0.01, 0.01, 0.02))
    return max(1.0, source_fps + delta)


def build_audio_filter(volume: float, delay_ms: int) -> str:
    filters = [f"volume={volume:.4f}"]
    if delay_ms > 0:
        filters.append(f"adelay=delays={delay_ms}:all=1")
        filters.append("atrim=start=0")
    return ",".join(filters)


def even_dimension(value: int) -> int:
    value = max(int(value), 2)
    return value if value % 2 == 0 else value - 1


def parse_probe(path: Path, payload: dict[str, Any]) -> VideoProbe:
    streams = payload.get("streams") or []
    video_stream = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    audio_stream = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)
    fmt = payload.get("format") or {}
    return VideoProbe(
        path=path,
        container=str(fmt.get("format_name") or ""),
        duration_seconds=parse_float(fmt.get("duration")),
        bitrate=parse_int(fmt.get("bit_rate")),
        video_codec=str(video_stream.get("codec_name")) if video_stream else None,
        width=parse_int(video_stream.get("width")) if video_stream else None,
        height=parse_int(video_stream.get("height")) if video_stream else None,
        fps=parse_fps(video_stream.get("avg_frame_rate") or video_stream.get("r_frame_rate")) if video_stream else None,
        audio_codec=str(audio_stream.get("codec_name")) if audio_stream else None,
        audio_bitrate=parse_int(audio_stream.get("bit_rate")) if audio_stream else None,
        has_video=video_stream is not None,
        has_audio=audio_stream is not None,
    )


def hash_log_fields(prefix: str, hashes: dict[str, str], settings: Settings) -> dict[str, Any]:
    if not settings.enable_hash_logging:
        return {}
    return {
        f"{prefix}_sha256": hashes.get("sha256"),
        f"{prefix}_md5": hashes.get("md5"),
        f"{prefix}_crc32": hashes.get("crc32"),
    }


def probe_log_fields(prefix: str, probe: VideoProbe) -> dict[str, Any]:
    return {
        f"{prefix}_container": probe.container,
        f"{prefix}_duration": probe.duration_seconds,
        f"{prefix}_fps": probe.fps,
        f"{prefix}_resolution": f"{probe.width}x{probe.height}" if probe.width and probe.height else None,
        f"{prefix}_bitrate": probe.bitrate,
        f"{prefix}_video_codec": probe.video_codec,
        f"{prefix}_audio_codec": probe.audio_codec,
        f"{prefix}_audio_bitrate": probe.audio_bitrate,
        f"{prefix}_has_audio": probe.has_audio,
    }


def probe_comparison_fields(original: VideoProbe, processed: VideoProbe) -> dict[str, Any]:
    return {
        "codec_original": original.video_codec,
        "codec_final": processed.video_codec,
        "container_original": original.container,
        "container_final": processed.container,
        "duration": processed.duration_seconds,
        "fps_original": original.fps,
        "fps_final": processed.fps,
        "resolution": f"{processed.width}x{processed.height}" if processed.width and processed.height else None,
        "bitrate_original": original.bitrate,
        "bitrate_final": processed.bitrate,
        "audio_codec": processed.audio_codec,
        "audio_bitrate": processed.audio_bitrate,
    }


def fallback_processed_video(
    input_path: Path,
    output_path: Path,
    processing_uuid: str,
    random_seed: int,
    metadata: dict[str, str],
    encoder_profile: dict[str, Any],
    original_probe: VideoProbe | None,
    processed_probe: VideoProbe | None,
    original_hashes: dict[str, str],
    final_hashes: dict[str, str],
    frame_hashes: dict[str, dict[str, str]],
    download_ms: int,
    ffmpeg_ms: int,
    total_ms: int,
) -> ProcessedVideo:
    return ProcessedVideo(
        input_path=input_path,
        output_path=output_path,
        processing_uuid=processing_uuid,
        random_seed=random_seed,
        metadata=metadata,
        encoder_profile=encoder_profile,
        original_probe=original_probe,
        processed_probe=processed_probe,
        download_ms=download_ms,
        ffmpeg_ms=ffmpeg_ms,
        total_ms=total_ms,
        original_size_bytes=file_size(input_path),
        final_size_bytes=file_size(output_path),
        original_sha256=original_hashes.get("sha256", ""),
        final_sha256=final_hashes.get("sha256", ""),
        original_hashes=original_hashes,
        final_hashes=final_hashes,
        frame_hashes=frame_hashes,
    )


def extension_for(mime_type: str, source_url: str) -> str:
    if mime_type == "video/quicktime":
        return ".mov"
    if mime_type in {"video/x-m4v", "video/m4v"}:
        return ".m4v"

    path = urlsplit(source_url).path.lower()
    for extension in (".mp4", ".mov", ".m4v"):
        if path.endswith(extension):
            return extension
    return ".mp4"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_hashes(path: Path) -> dict[str, str]:
    sha256_digest = hashlib.sha256()
    md5_digest = hashlib.md5(usedforsecurity=False)
    crc = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256_digest.update(chunk)
            md5_digest.update(chunk)
            crc = zlib.crc32(chunk, crc)
    return {
        "sha256": sha256_digest.hexdigest(),
        "md5": md5_digest.hexdigest(),
        "crc32": f"{crc & 0xFFFFFFFF:08x}",
    }


def elapsed_ms(started: float) -> int:
    return round((time.perf_counter() - started) * 1000)


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def remove_empty_parents(start: Path, stop: Path) -> None:
    current = start
    while current != stop and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def random_token(rng: random.Random, length: int) -> str:
    alphabet = "0123456789abcdef"
    return "".join(rng.choice(alphabet) for _ in range(length))


def parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_fps(value: Any) -> float | None:
    if not value:
        return None
    try:
        numerator, denominator = str(value).split("/")
        denominator_float = float(denominator)
        if denominator_float == 0:
            return None
        return round(float(numerator) / denominator_float, 3)
    except (TypeError, ValueError):
        return parse_float(value)


def safe_command(command: list[str]) -> list[str]:
    sanitized: list[str] = []
    redact_next = False
    for value in command:
        if redact_next:
            sanitized.append("[path]")
            redact_next = False
            continue
        if value == "-i":
            sanitized.append(value)
            redact_next = True
            continue
        if value.startswith("/") or value.startswith("."):
            sanitized.append("[path]")
            continue
        sanitized.append(value)
    return sanitized
