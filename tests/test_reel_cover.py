import unittest

from app.instagram import InstagramClient


class ReelCoverPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_reel_container_includes_cover_url(self) -> None:
        client = object.__new__(InstagramClient)
        captured: dict[str, object] = {}

        async def request(method: str, path: str, params=None, data=None):
            captured.update({"method": method, "path": path, "params": params, "data": data})
            return {"id": "container-1"}

        client._request = request  # type: ignore[method-assign]

        result = await client.create_container(
            "token",
            "instagram-user",
            "https://storage.example/video.mp4",
            "video/mp4",
            "reel",
            "Legenda",
            "https://storage.example/cover.jpg",
        )

        self.assertEqual(result, "container-1")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/instagram-user/media")
        self.assertEqual(captured["data"]["caption"], "Legenda")  # type: ignore[index]
        self.assertEqual(captured["data"]["cover_url"], "https://storage.example/cover.jpg")  # type: ignore[index]

    async def test_reel_container_omits_cover_when_not_configured(self) -> None:
        client = object.__new__(InstagramClient)
        captured: dict[str, object] = {}

        async def request(method: str, path: str, params=None, data=None):
            captured.update({"data": data})
            return {"id": "container-2"}

        client._request = request  # type: ignore[method-assign]

        await client.create_container(
            "token",
            "instagram-user",
            "https://storage.example/video.mp4",
            "video/mp4",
            "reel",
            "",
        )

        self.assertNotIn("cover_url", captured["data"])  # type: ignore[operator]

    async def test_non_reel_never_sends_cover_url(self) -> None:
        client = object.__new__(InstagramClient)
        captured: dict[str, object] = {}

        async def request(method: str, path: str, params=None, data=None):
            captured.update({"data": data})
            return {"id": "container-3"}

        client._request = request  # type: ignore[method-assign]

        await client.create_container(
            "token",
            "instagram-user",
            "https://storage.example/video.mp4",
            "video/mp4",
            "feed",
            "Legenda",
            "https://storage.example/cover.jpg",
        )

        self.assertNotIn("cover_url", captured["data"])  # type: ignore[operator]


if __name__ == "__main__":
    unittest.main()
