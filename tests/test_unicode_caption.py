import unittest
from types import SimpleNamespace

import httpx

from app.caption import caption_diagnostics
from app.instagram import InstagramClient
from app.logger import get_logger


UNICODE_CAPTION = "Olá 👩🏽‍💻 🇧🇷 你好\n#lançamento @terbb"


class UnicodeCaptionTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_uses_multipart_with_exact_utf8_caption_bytes(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["content_type"] = request.headers.get("content-type")
            captured["content"] = request.content
            return httpx.Response(200, json={"id": "container-1"})

        client = object.__new__(InstagramClient)
        client._settings = SimpleNamespace(retry_base_seconds=1)
        client._base_url = "https://graph.instagram.com/v23.0"
        client._logger = get_logger("instagram-test")
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        try:
            response = await client._request(
                "POST",
                "/instagram-user/media",
                data={
                    "caption": UNICODE_CAPTION,
                    "media_type": "REELS",
                    "video_url": "https://storage.example/video.mp4",
                    "access_token": "secret-token",
                },
            )
        finally:
            await client._client.aclose()

        content = captured["content"]
        self.assertEqual(response, {"id": "container-1"})
        self.assertIsInstance(content, bytes)
        self.assertTrue(str(captured["content_type"]).startswith("multipart/form-data; boundary="))
        self.assertIn(UNICODE_CAPTION.encode("utf-8"), content)
        self.assertNotIn(b"%F0%9F", content)

    def test_diagnostics_are_stable_without_exposing_caption(self) -> None:
        first = caption_diagnostics(UNICODE_CAPTION)
        second = caption_diagnostics(UNICODE_CAPTION)

        self.assertEqual(first, second)
        self.assertEqual(first["caption_utf8_length"], len(UNICODE_CAPTION.encode("utf-8")))
        self.assertNotIn(UNICODE_CAPTION, first.values())


if __name__ == "__main__":
    unittest.main()
