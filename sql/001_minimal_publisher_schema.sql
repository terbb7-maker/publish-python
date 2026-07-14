-- Minimal schema support for the Python Publisher.
-- Apply once before running the simplified publisher in production.

alter type public.campaign_job_status add value if not exists 'completed';

create index if not exists idx_campaign_jobs_publisher_due
  on public.campaign_jobs(status, scheduled_for_utc, social_account_id, sequence_number)
  where deleted_at is null
    and status in ('scheduled');

create index if not exists idx_campaign_jobs_publisher_account_running
  on public.campaign_jobs(social_account_id, status, reserved_at)
  where deleted_at is null
    and status in ('running');

create index if not exists idx_campaign_jobs_publisher_metadata_container
  on public.campaign_jobs((metadata_safe->>'provider_container_id'))
  where deleted_at is null
    and metadata_safe ? 'provider_container_id';

create index if not exists idx_campaign_jobs_publisher_metadata_media
  on public.campaign_jobs((metadata_safe->>'provider_media_id'))
  where deleted_at is null
    and metadata_safe ? 'provider_media_id';
