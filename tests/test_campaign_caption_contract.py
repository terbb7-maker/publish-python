import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from app.repository import CONTEXT_SQL, Repository


class FakeConnection:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    async def fetchrow(self, *_args: Any) -> dict[str, Any]:
        return self._row


class FakeAcquire:
    def __init__(self, row: dict[str, Any]) -> None:
        self._connection = FakeConnection(row)

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *_args: Any) -> None:
        return None


class FakeDatabase:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self._row)


def context_row(caption: str | None) -> dict[str, Any]:
    return {
        "id": "11111111-1111-1111-1111-111111111111",
        "campaign_id": "22222222-2222-2222-2222-222222222222",
        "campaign_account_id": "33333333-3333-3333-3333-333333333333",
        "social_account_id": "44444444-4444-4444-4444-444444444444",
        "media_asset_id": "55555555-5555-5555-5555-555555555555",
        "status": "running",
        "attempt_count": 1,
        "scheduled_for_utc": datetime.now(UTC),
        "metadata_safe": {},
        "campaign_caption": caption,
        "campaign_type": "reel",
        "owner_user_id": "66666666-6666-6666-6666-666666666666",
        "workspace_id": None,
        "storage_bucket": "media-assets",
        "storage_path": "users/owner/video.mp4",
        "mime_type": "video/mp4",
        "media_status": "ready",
        "media_deleted_at": None,
        "instagram_user_id": "17841400000000000",
        "social_account_status": "active",
        "connected_account_id": "77777777-7777-7777-7777-777777777777",
        "meta_app_id": None,
        "access_token_encrypted": "encrypted-token",
        "token_expires_at": None,
        "requires_reconnect_at": None,
        "connection_status": "active",
        "connection_owner_user_id": "66666666-6666-6666-6666-666666666666",
        "connection_workspace_id": None,
    }


class CampaignCaptionContractTest(unittest.IsolatedAsyncioTestCase):
    def test_context_query_never_selects_internal_campaign_fields_as_caption(self) -> None:
        normalized_sql = " ".join(CONTEXT_SQL.split())

        self.assertIn("c.caption as campaign_caption", normalized_sql)
        self.assertNotIn("c.name as campaign_name", normalized_sql)
        self.assertNotIn("c.description as campaign_description", normalized_sql)

    async def test_context_uses_only_campaign_caption(self) -> None:
        settings = SimpleNamespace(worker_id="test-worker")
        repository = Repository(FakeDatabase(context_row("Legenda real #terbb")), settings)

        context = await repository.get_context("11111111-1111-1111-1111-111111111111")

        self.assertEqual(context.caption, "Legenda real #terbb")

    async def test_missing_caption_never_falls_back_to_name_or_description(self) -> None:
        settings = SimpleNamespace(worker_id="test-worker")
        repository = Repository(FakeDatabase(context_row(None)), settings)

        context = await repository.get_context("11111111-1111-1111-1111-111111111111")

        self.assertEqual(context.caption, "")


if __name__ == "__main__":
    unittest.main()
