import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.database import Database
from app.logger import redact


ACTIVE_STATUSES = ("scheduled", "running")
CLAIMABLE_STATUSES = ("scheduled",)
RUNNING_STATUS = "running"
COMPLETED_STATUS = "completed"
FAILED_STATUS = "failed"
CANCELLED_STATUS = "cancelled"


@dataclass(slots=True)
class ClaimedJob:
    id: str
    campaign_id: str
    social_account_id: str


@dataclass(slots=True)
class JobContext:
    id: str
    campaign_id: str
    campaign_account_id: str
    social_account_id: str
    media_asset_id: str
    status: str
    attempt_count: int
    scheduled_for_utc: datetime
    caption: str
    campaign_type: str
    storage_bucket: str
    storage_path: str
    mime_type: str
    media_status: str
    media_deleted_at: datetime | None
    instagram_user_id: str
    connected_account_id: str
    meta_app_id: str | None
    access_token_encrypted: str | None
    token_expires_at: datetime | None
    requires_reconnect_at: datetime | None
    social_account_status: str
    connection_status: str
    owner_user_id: str | None
    workspace_id: str | None
    metadata: dict[str, Any]

    @property
    def provider_container_id(self) -> str | None:
        value = self.metadata.get("provider_container_id")
        return str(value) if value else None

    @property
    def provider_media_id(self) -> str | None:
        value = self.metadata.get("provider_media_id")
        return str(value) if value else None

    @property
    def provider_status(self) -> str | None:
        value = self.metadata.get("provider_status")
        return str(value) if value else None


def now_utc() -> datetime:
    return datetime.now(UTC)


def json_metadata(value: dict[str, Any]) -> str:
    return json.dumps(value, default=str)


CLAIM_SQL = """
with ranked as (
  select
    cj.id,
    row_number() over (
      partition by cj.social_account_id
      order by cj.scheduled_for_utc asc, cj.sequence_number asc
    ) as account_rank
  from public.campaign_jobs cj
  join public.campaigns c on c.id = cj.campaign_id
  join public.campaign_accounts ca on ca.id = cj.campaign_account_id
  where cj.deleted_at is null
    and c.deleted_at is null
    and ca.deleted_at is null
    and c.status = any($3::public.campaign_status[])
    and ca.status = 'active'
    and cj.status = any($4::public.campaign_job_status[])
    and cj.scheduled_for_utc <= now()
    and not exists (
      select 1
      from public.campaign_jobs active
      where active.social_account_id = cj.social_account_id
        and active.deleted_at is null
        and active.status = $5::public.campaign_job_status
        and coalesce(active.reserved_at, active.started_at, active.updated_at)
          > now() - make_interval(secs => $6::integer)
    )
),
due as (
  select cj.id
  from public.campaign_jobs cj
  join ranked on ranked.id = cj.id
  where ranked.account_rank = 1
  order by cj.scheduled_for_utc asc, cj.sequence_number asc
  limit $1::integer
  for update of cj skip locked
)
update public.campaign_jobs cj
   set status = $5::public.campaign_job_status,
       reserved_at = now(),
       reserved_by = $2::text,
       started_at = now(),
       attempt_count = cj.attempt_count + 1,
       last_error_code = null,
       last_error_message_safe = null,
       metadata_safe = coalesce(cj.metadata_safe, '{}'::jsonb)
         || jsonb_build_object(
              'last_heartbeat_at'::text, to_jsonb(now()::timestamptz),
              'provider_status'::text, coalesce(cj.metadata_safe->>'provider_status', 'running'::text)
            )
  from due
 where cj.id = due.id
returning cj.id, cj.campaign_id, cj.social_account_id;
"""

CONTEXT_SQL = """
select
  cj.id,
  cj.campaign_id,
  cj.campaign_account_id,
  cj.social_account_id,
  cj.media_asset_id,
  cj.status,
  cj.attempt_count,
  cj.scheduled_for_utc,
  cj.metadata_safe,
  c.name as campaign_name,
  c.description as campaign_description,
  c.campaign_type,
  c.owner_user_id,
  c.workspace_id,
  ma.storage_bucket,
  ma.storage_path,
  ma.mime_type,
  ma.status as media_status,
  ma.deleted_at as media_deleted_at,
  sa.external_account_id as instagram_user_id,
  sa.status as social_account_status,
  cauth.id as connected_account_id,
  cauth.meta_app_id,
  cauth.access_token_encrypted,
  cauth.token_expires_at,
  cauth.requires_reconnect_at,
  cauth.status as connection_status,
  cauth.owner_user_id as connection_owner_user_id,
  cauth.workspace_id as connection_workspace_id
from public.campaign_jobs cj
join public.campaigns c on c.id = cj.campaign_id
join public.campaign_accounts ca on ca.id = cj.campaign_account_id
join public.media_assets ma on ma.id = cj.media_asset_id
join public.social_accounts sa on sa.id = cj.social_account_id
join public.connected_accounts cauth on cauth.id = sa.connected_account_id
where cj.id = $1::uuid
  and cj.reserved_by = $2::text
  and cj.status = $3::public.campaign_job_status
  and cj.deleted_at is null
  and c.deleted_at is null
  and ca.deleted_at is null
limit 1;
"""


class Repository:
    def __init__(self, database: Database, settings: Settings) -> None:
        self._database = database
        self._settings = settings

    async def claim_due_jobs(self, limit: int) -> list[ClaimedJob]:
        async with self._database.acquire() as connection:
            rows = await connection.fetch(
                CLAIM_SQL,
                limit,
                self._settings.worker_id,
                list(ACTIVE_STATUSES),
                list(CLAIMABLE_STATUSES),
                RUNNING_STATUS,
                self._settings.lease_seconds,
            )
        jobs = [
            ClaimedJob(
                id=str(row["id"]),
                campaign_id=str(row["campaign_id"]),
                social_account_id=str(row["social_account_id"]),
            )
            for row in rows
        ]
        await self.refresh_campaigns({job.campaign_id for job in jobs})
        return jobs

    async def get_context(self, job_id: str) -> JobContext:
        async with self._database.acquire() as connection:
            row = await connection.fetchrow(CONTEXT_SQL, job_id, self._settings.worker_id, RUNNING_STATUS)
        if not row:
            raise RuntimeError(f"Job not found or not reserved by this worker: {job_id}")

        data = dict(row)
        metadata = data.get("metadata_safe") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        caption = str(data["campaign_description"] or data["campaign_name"] or "")

        return JobContext(
            id=str(data["id"]),
            campaign_id=str(data["campaign_id"]),
            campaign_account_id=str(data["campaign_account_id"]),
            social_account_id=str(data["social_account_id"]),
            media_asset_id=str(data["media_asset_id"]),
            status=str(data["status"]),
            attempt_count=int(data["attempt_count"]),
            scheduled_for_utc=data["scheduled_for_utc"],
            caption=caption,
            campaign_type=str(data["campaign_type"]),
            storage_bucket=str(data["storage_bucket"]),
            storage_path=str(data["storage_path"]),
            mime_type=str(data["mime_type"]),
            media_status=str(data["media_status"]),
            media_deleted_at=data["media_deleted_at"],
            instagram_user_id=str(data["instagram_user_id"]),
            connected_account_id=str(data["connected_account_id"]),
            meta_app_id=str(data["meta_app_id"]) if data["meta_app_id"] else None,
            access_token_encrypted=data["access_token_encrypted"],
            token_expires_at=data["token_expires_at"],
            requires_reconnect_at=data["requires_reconnect_at"],
            social_account_status=str(data["social_account_status"]),
            connection_status=str(data["connection_status"]),
            owner_user_id=str(data["connection_owner_user_id"] or data["owner_user_id"])
            if data["connection_owner_user_id"] or data["owner_user_id"]
            else None,
            workspace_id=str(data["connection_workspace_id"] or data["workspace_id"])
            if data["connection_workspace_id"] or data["workspace_id"]
            else None,
            metadata=metadata,
        )

    async def heartbeat(self, job_id: str) -> None:
        async with self._database.acquire() as connection:
            await connection.execute(
                """
                update public.campaign_jobs
                   set reserved_at = now(),
                       metadata_safe = coalesce(metadata_safe, '{}'::jsonb)
                         || jsonb_build_object(
                              'last_heartbeat_at'::text,
                              to_jsonb(now()::timestamptz)
                            )
                 where id = $1::uuid
                   and reserved_by = $2::text
                   and status = $3::public.campaign_job_status
                   and deleted_at is null;
                """,
                job_id,
                self._settings.worker_id,
                RUNNING_STATUS,
            )

    async def patch_metadata(self, job_id: str, patch: dict[str, Any]) -> None:
        async with self._database.acquire() as connection:
            await connection.execute(
                """
                update public.campaign_jobs
                   set reserved_at = now(),
                       metadata_safe = coalesce(metadata_safe, '{}'::jsonb) || $3::jsonb
                 where id = $1::uuid
                   and reserved_by = $2::text
                   and status = $4::public.campaign_job_status
                   and deleted_at is null;
                """,
                job_id,
                self._settings.worker_id,
                json_metadata(redact(patch)),
                RUNNING_STATUS,
            )

    async def mark_completed(self, job_id: str, provider_media_id: str, metadata: dict[str, Any]) -> None:
        await self._finish(
            job_id,
            COMPLETED_STATUS,
            None,
            None,
            {
                **metadata,
                "provider_media_id": provider_media_id,
                "provider_status": "completed",
                "completed_at": now_utc().isoformat(),
            },
        )

    async def mark_cancelled(self, job_id: str, reason: str) -> None:
        await self._finish(
            job_id,
            CANCELLED_STATUS,
            "cancelled",
            reason,
            {"provider_status": "cancelled", "last_error": reason},
        )

    async def mark_failed(self, job_id: str, code: str, message: str, metadata: dict[str, Any] | None = None) -> None:
        await self._finish(
            job_id,
            FAILED_STATUS,
            code,
            message,
            {"provider_status": "failed", "last_error": message, **(metadata or {})},
        )

    async def mark_context_load_failed(self, job_id: str, message: str) -> None:
        await self._finish(
            job_id,
            FAILED_STATUS,
            "context_load_failed",
            message,
            {"provider_status": "failed", "last_error": message},
        )

    async def schedule_retry(
        self,
        job: JobContext,
        code: str,
        message: str,
        retry_at: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        async with self._database.acquire() as connection:
            await connection.execute(
                """
                update public.campaign_jobs
                   set status = $2::public.campaign_job_status,
                       scheduled_for_utc = $3::timestamptz,
                       reserved_at = null,
                       reserved_by = null,
                       started_at = null,
                       finished_at = null,
                       last_error_code = $4::text,
                       last_error_message_safe = $5::text,
                       metadata_safe = coalesce(metadata_safe, '{}'::jsonb) || $6::jsonb
                 where id = $1::uuid
                   and status = $7::public.campaign_job_status
                   and reserved_by = $8::text
                   and deleted_at is null;
                """,
                job.id,
                "scheduled",
                retry_at,
                code,
                message,
                json_metadata(
                    redact(
                        {
                            "provider_status": "retry_scheduled",
                            "last_error": message,
                            "retry_at": retry_at.isoformat(),
                            **(metadata or {}),
                        }
                    )
                ),
                RUNNING_STATUS,
                self._settings.worker_id,
            )
        await self.refresh_campaign(job.campaign_id)

    async def recover_stale_jobs(self) -> int:
        async with self._database.acquire() as connection:
            rows = await connection.fetch(
                """
                update public.campaign_jobs cj
                   set status = $2::public.campaign_job_status,
                       reserved_at = null,
                       reserved_by = null,
                       started_at = null,
                       metadata_safe = coalesce(cj.metadata_safe, '{}'::jsonb)
                         || jsonb_build_object(
                              'provider_status'::text,
                              case
                                when cj.metadata_safe->>'provider_status' = 'publishing'
                                  then 'publish_result_unknown'::text
                                else 'recovered'::text
                              end,
                              'last_recovered_at'::text, to_jsonb(now()::timestamptz),
                              'last_recovered_by'::text, $3::text
                            ),
                       last_error_code = case
                         when cj.metadata_safe->>'provider_status' = 'publishing'
                           then 'publish_result_unknown'
                         else cj.last_error_code
                       end,
                       last_error_message_safe = case
                         when cj.metadata_safe->>'provider_status' = 'publishing'
                           then 'Resultado da chamada media_publish desconhecido; retry automatico bloqueado para evitar duplicidade.'
                         else cj.last_error_message_safe
                       end
                  from public.campaigns c,
                       public.campaign_accounts ca
                 where cj.campaign_id = c.id
                   and cj.campaign_account_id = ca.id
                   and cj.deleted_at is null
                   and c.deleted_at is null
                   and ca.deleted_at is null
                   and c.status = any($5::public.campaign_status[])
                   and ca.status = 'active'
                   and cj.status = $1::public.campaign_job_status
                   and coalesce(cj.reserved_at, cj.started_at, cj.updated_at) < now() - make_interval(secs => $4::integer)
                   and coalesce(cj.metadata_safe->>'provider_media_id', ''::text) = ''::text
                   and coalesce(cj.metadata_safe->>'provider_status', ''::text) <> 'publishing'::text
                returning cj.id, cj.campaign_id;
                """,
                RUNNING_STATUS,
                "scheduled",
                self._settings.worker_id,
                self._settings.lease_seconds,
                list(ACTIVE_STATUSES),
            )
        await self.refresh_campaigns({str(row["campaign_id"]) for row in rows})
        return len(rows)

    async def mark_unknown_publishing_as_failed(self) -> int:
        async with self._database.acquire() as connection:
            rows = await connection.fetch(
                """
                update public.campaign_jobs
                   set status = $2::public.campaign_job_status,
                       reserved_at = null,
                       reserved_by = null,
                       finished_at = now(),
                       last_error_code = 'publish_result_unknown',
                       last_error_message_safe = 'Resultado da chamada media_publish desconhecido; bloqueado para evitar republicacao.',
                       metadata_safe = coalesce(metadata_safe, '{}'::jsonb)
                         || jsonb_build_object(
                              'provider_status'::text,
                              'failed'::text
                            )
                 where deleted_at is null
                   and status = $1::public.campaign_job_status
                   and coalesce(metadata_safe->>'provider_status', ''::text) = 'publishing'::text
                   and coalesce(reserved_at, started_at, updated_at) < now() - make_interval(secs => $3::integer)
                returning id, campaign_id;
                """,
                RUNNING_STATUS,
                FAILED_STATUS,
                self._settings.lease_seconds,
            )
        await self.refresh_campaigns({str(row["campaign_id"]) for row in rows})
        return len(rows)

    async def refresh_campaign(self, campaign_id: str | None) -> None:
        if not campaign_id:
            return

        await self.refresh_campaigns({campaign_id})

    async def refresh_campaigns(self, campaign_ids: set[str]) -> None:
        for campaign_id in campaign_ids:
            try:
                async with self._database.acquire() as connection:
                    await connection.execute(
                        "select app_private.refresh_campaign_counters($1::uuid);",
                        campaign_id,
                    )
            except Exception:
                continue

    async def log_event(
        self,
        job: JobContext,
        event_key: str,
        level: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            async with self._database.acquire() as connection:
                await connection.execute(
                    """
                    insert into public.campaign_events (
                      campaign_id,
                      campaign_job_id,
                      actor_user_id,
                      event_key,
                      level,
                      message_safe,
                      metadata_safe
                    )
                    values (
                      $1::uuid,
                      $2::uuid,
                      $3::uuid,
                      $4::text,
                      $5::text,
                      $6::text,
                      $7::jsonb
                    );
                    """,
                    job.campaign_id,
                    job.id,
                    job.owner_user_id,
                    event_key,
                    level,
                    message,
                    json_metadata(redact(metadata or {})),
                )
        except Exception:
            return

    async def signed_media_url(self, job: JobContext) -> str:
        if not self._database.supabase:
            raise RuntimeError("Supabase storage client is not connected.")

        response = await asyncio.to_thread(
            lambda: self._database.supabase.storage.from_(job.storage_bucket).create_signed_url(
                job.storage_path,
                self._settings.media_signed_url_ttl_seconds,
            )
        )
        if isinstance(response, dict):
            signed_url = response.get("signedURL") or response.get("signedUrl") or response.get("signed_url")
        else:
            signed_url = getattr(response, "signed_url", None) or getattr(response, "signedURL", None)
        if not signed_url:
            raise RuntimeError("Could not create signed media URL.")
        return str(signed_url)

    async def _finish(
        self,
        job_id: str,
        status: str,
        error_code: str | None,
        error_message: str | None,
        metadata: dict[str, Any],
    ) -> None:
        async with self._database.acquire() as connection:
            row = await connection.fetchrow(
                """
                update public.campaign_jobs
                   set status = $2::public.campaign_job_status,
                       reserved_at = null,
                       reserved_by = null,
                       finished_at = now(),
                       last_error_code = $3::text,
                       last_error_message_safe = $4::text,
                       metadata_safe = coalesce(metadata_safe, '{}'::jsonb) || $5::jsonb
                 where id = $1::uuid
                   and status = $6::public.campaign_job_status
                   and reserved_by = $7::text
                   and deleted_at is null
                returning campaign_id;
                """,
                job_id,
                status,
                error_code,
                error_message,
                json_metadata(redact(metadata)),
                RUNNING_STATUS,
                self._settings.worker_id,
            )
        if row:
            await self.refresh_campaign(str(row["campaign_id"]))
