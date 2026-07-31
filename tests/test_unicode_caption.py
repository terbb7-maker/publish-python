import logging
import unittest
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx

from app.caption import caption_diagnostics
from app.instagram import InstagramClient
from app.logger import configure_logging, get_logger


UNICODE_CAPTION = "Olá 👩🏽‍💻 🇧🇷 你好\n#lançamento @terbb"


class UnicodeCaptionTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_container_sends_exact_unicode_caption_in_query(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["content_type"] = request.headers.get("content-type")
            captured["content"] = request.content
            captured["query"] = request.url.query
            return httpx.Response(200, json={"id": "container-1"})

        client = object.__new__(InstagramClient)
        client._settings = SimpleNamespace(retry_base_seconds=1)
        client._base_url = "https://graph.instagram.com/v23.0"
        client._logger = get_logger("instagram-test")
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        try:
            response = await client.create_container(
                access_token="secret-token",
                instagram_user_id="instagram-user",
                media_url="https://storage.example/video.mp4",
                mime_type="video/mp4",
                campaign_type="reel",
                caption=UNICODE_CAPTION,
            )
        finally:
            await client._client.aclose()

        content = captured["content"]
        self.assertEqual(response, "container-1")
        self.assertIsInstance(content, bytes)
        self.assertEqual(content, b"")
        self.assertIsNone(captured["content_type"])
        query = captured["query"]
        self.assertIsInstance(query, bytes)
        decoded = parse_qs(query.decode("ascii"), keep_blank_values=True)
        self.assertEqual(decoded["caption"], [UNICODE_CAPTION])
        self.assertEqual(decoded["media_type"], ["REELS"])
        self.assertEqual(decoded["video_url"], ["https://storage.example/video.mp4"])
        self.assertEqual(
            decoded["caption"][0].encode("utf-8"),
            UNICODE_CAPTION.encode("utf-8"),
        )

    async def test_unread_streaming_requests_and_logger_failures_do_not_interrupt_publish_flow(
        self,
    ) -> None:
        class UnreadStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"streaming-body"

        class StubClient:
            def __init__(self):
                self.calls: list[tuple[str, str]] = []

            async def request(self, method, url, params=None, data=None):
                self.calls.append((method, url))
                request = httpx.Request(method, url, stream=UnreadStream())
                if url.endswith("/media_publish"):
                    payload = {"id": "media-1"}
                elif url.endswith("/media"):
                    payload = {"id": "container-1"}
                else:
                    payload = {"status_code": "FINISHED"}
                return httpx.Response(200, json=payload, request=request)

        class FailingLogger:
            def info(self, event, **context):
                raise RuntimeError("logger unavailable")

            def error(self, event, **context):
                raise RuntimeError("logger unavailable")

        client = object.__new__(InstagramClient)
        client._settings = SimpleNamespace(retry_base_seconds=1)
        client._base_url = "https://graph.instagram.com/v23.0"
        client._logger = FailingLogger()
        stub_client = StubClient()
        client._client = stub_client

        container_id = await client.create_container(
            access_token="secret-token",
            instagram_user_id="instagram-user",
            media_url="https://storage.example/video.mp4",
            mime_type="video/mp4",
            campaign_type="reel",
            caption=UNICODE_CAPTION,
        )
        status = await client.get_container_status("secret-token", container_id)
        media_id = await client.publish_container("secret-token", "instagram-user", container_id)

        self.assertEqual(container_id, "container-1")
        self.assertEqual(status, "FINISHED")
        self.assertEqual(media_id, "media-1")
        self.assertEqual(
            stub_client.calls,
            [
                ("POST", "https://graph.instagram.com/v23.0/instagram-user/media"),
                ("GET", "https://graph.instagram.com/v23.0/container-1"),
                ("POST", "https://graph.instagram.com/v23.0/instagram-user/media_publish"),
            ],
        )

    def test_diagnostics_are_stable_without_exposing_caption(self) -> None:
        first = caption_diagnostics(UNICODE_CAPTION)
        second = caption_diagnostics(UNICODE_CAPTION)

        self.assertEqual(first, second)
        self.assertEqual(first["caption_utf8_length"], len(UNICODE_CAPTION.encode("utf-8")))
        self.assertNotIn(UNICODE_CAPTION, first.values())

    def test_http_client_loggers_never_emit_request_urls_at_info(self) -> None:
        configure_logging("info")

        self.assertGreaterEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertGreaterEqual(logging.getLogger("httpcore").level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()
