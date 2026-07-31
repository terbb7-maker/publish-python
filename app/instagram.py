import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from app.caption import caption_diagnostics
from app.config import Settings
from app.logger import get_logger


READY_STATUSES = {"FINISHED", "PUBLISHED", "READY"}
FAILED_STATUSES = {"ERROR", "EXPIRED", "FAILED"}
SENSITIVE_KEYS = {
    "access_token",
    "app_secret",
    "authorization",
    "client_secret",
    "code",
    "code_verifier",
    "cover_url",
    "image_url",
    "media_url",
    "refresh_token",
    "signed_url",
    "token",
    "video_url",
}


@dataclass(slots=True)
class InstagramError(Exception):
    code: str
    message: str
    retryable: bool
    category: str
    http_status: int | None = None
    retry_after_seconds: int | None = None
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


class InstagramClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base_url = f"https://graph.instagram.com/{settings.instagram_api_version}"
        self._logger = get_logger("instagram")
        self._client = httpx.AsyncClient(
            timeout=settings.http_timeout_seconds,
            limits=httpx.Limits(
                max_connections=max(settings.concurrency * 2, 10),
                max_keepalive_connections=max(settings.concurrency, 5),
            ),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def create_container(
        self,
        access_token: str,
        instagram_user_id: str,
        media_url: str,
        mime_type: str,
        campaign_type: str,
        caption: str,
        cover_url: str | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "caption": caption,
            "media_type": media_type(campaign_type, mime_type),
        }
        if mime_type.startswith("image/"):
            payload["image_url"] = media_url
        else:
            payload["video_url"] = media_url
        if campaign_type == "reel" and cover_url:
            payload["cover_url"] = cover_url

        response = await self._request(
            "POST",
            f"/{instagram_user_id}/media",
            json_body=payload,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        container_id = response.get("id")
        if not container_id:
            raise InstagramError(
                code="container_missing",
                message="A Instagram API nao retornou o container.",
                retryable=True,
                category="temporary",
            )
        return str(container_id)

    async def get_container_status(self, access_token: str, container_id: str) -> str:
        response = await self._request(
            "GET",
            f"/{container_id}",
            params={"fields": "status_code,status", "access_token": access_token},
        )
        return str(response.get("status_code") or response.get("status") or "").upper()

    async def publish_container(self, access_token: str, instagram_user_id: str, container_id: str) -> str:
        response = await self._request(
            "POST",
            f"/{instagram_user_id}/media_publish",
            data={"creation_id": container_id, "access_token": access_token},
        )
        media_id = response.get("id")
        if not media_id:
            raise InstagramError(
                code="media_id_missing",
                message="A Instagram API nao retornou o ID da publicacao.",
                retryable=True,
                category="temporary",
            )
        return str(media_id)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        caption_source = (
            json_body
            if json_body and isinstance(json_body.get("caption"), str)
            else params
            if params and isinstance(params.get("caption"), str)
            else data
        )
        caption = (
            str(caption_source["caption"])
            if caption_source and isinstance(caption_source.get("caption"), str)
            else None
        )
        caption_utf8_preserved = caption_matches_original_payload(caption_source, caption)
        caption_transport = (
            "json"
            if json_body and isinstance(json_body.get("caption"), str)
            else "query"
            if params and isinstance(params.get("caption"), str)
            else "form"
            if data and isinstance(data.get("caption"), str)
            else None
        )
        try:
            self._logger.info(
                "instagram_http_request_started",
                method=method,
                url=sanitize_url(url),
                path=path,
                params=sanitize_mapping(params),
                body=sanitize_mapping(json_body if json_body is not None else data),
                content_type=(
                    "application/json"
                    if json_body is not None
                    else "application/x-www-form-urlencoded"
                    if data is not None
                    else None
                ),
                caption_request=caption_diagnostics(caption) if caption is not None else None,
                caption_transport=caption_transport,
            )
        except Exception:
            pass
        try:
            response = await self._client.request(
                method,
                url,
                params=params,
                data=data,
                json=json_body,
                headers=headers,
            )
        except httpx.TimeoutException as error:
            raise InstagramError(
                code="timeout",
                message="Timeout ao chamar a Instagram API.",
                retryable=True,
                category="timeout",
                retry_after_seconds=self._settings.retry_base_seconds,
            ) from error
        except httpx.HTTPError as error:
            raise InstagramError(
                code="http_error",
                message="Falha HTTP ao chamar a Instagram API.",
                retryable=True,
                category="temporary",
                retry_after_seconds=self._settings.retry_base_seconds,
                details={"error_type": type(error).__name__},
            ) from error

        payload = parse_json(response)
        if response.is_error or payload.get("error"):
            self._log_meta_raw_response(
                method,
                path,
                response,
                payload,
                caption,
                caption_utf8_preserved,
            )
            raise map_instagram_error(response.status_code, payload)
        try:
            self._logger.info(
                "instagram_http_response_succeeded",
                method=method,
                path=path,
                url=sanitize_url(str(response.url)),
                http_status=response.status_code,
                headers=relevant_response_headers(response.headers),
                payload=sanitize_mapping(payload),
                request_content_type=response.request.headers.get("content-type"),
                caption_request=caption_diagnostics(caption) if caption is not None else None,
                caption_utf8_preserved=caption_utf8_preserved,
                caption_transport=caption_transport,
                caption_response={
                    "echoed_by_meta": False,
                    "reason": "container_response_does_not_include_caption",
                }
                if caption is not None
                else None,
            )
        except Exception:
            pass
        return payload

    def _log_meta_raw_response(
        self,
        method: str,
        path: str,
        response: httpx.Response,
        payload: dict[str, Any],
        caption: str | None,
        caption_utf8_preserved: bool | None,
    ) -> None:
        try:
            payload_safe = sanitize_mapping(payload)
            raw_response_block = (
                "================ META RAW RESPONSE ================\n"
                f"HTTP STATUS: {response.status_code}\n"
                f"{json.dumps(payload_safe, ensure_ascii=False, default=str, indent=2)}\n"
                "=================================================="
            )
            self._logger.error(
                "meta_raw_response",
                method=method,
                path=path,
                url=sanitize_url(str(response.url)),
                http_status=response.status_code,
                headers=relevant_response_headers(response.headers),
                payload=payload_safe,
                raw_response_block=raw_response_block,
                request_content_type=response.request.headers.get("content-type"),
                caption_request=caption_diagnostics(caption) if caption is not None else None,
                caption_utf8_preserved=caption_utf8_preserved,
                caption_response={
                    "echoed_by_meta": False,
                    "reason": "meta_error_response_does_not_echo_caption",
                }
                if caption is not None
                else None,
            )
        except Exception:
            pass


def parse_json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw": response.text}
    return payload if isinstance(payload, dict) else {"data": payload}


def relevant_response_headers(headers: httpx.Headers) -> dict[str, str]:
    relevant_names = {
        "content-type",
        "x-app-usage",
        "x-business-use-case-usage",
        "x-fb-debug",
        "x-fb-rev",
        "x-fb-trace-id",
        "x-page-usage",
    }
    return {key: value for key, value in headers.items() if key.lower() in relevant_names}


def sanitize_mapping(value: Any) -> Any:
    if isinstance(value, list):
        return [sanitize_mapping(item) for item in value]
    if isinstance(value, dict):
        return {
            key: (
                "[redacted]"
                if key.lower() in SENSITIVE_KEYS
                else caption_diagnostics(str(item))
                if key.lower() == "caption" and item is not None
                else sanitize_mapping(item)
            )
            for key, item in value.items()
        }
    return value


def form_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def caption_matches_original_payload(
    data: dict[str, Any] | None,
    caption: str | None,
) -> bool | None:
    if caption is None:
        return None

    try:
        original = data.get("caption") if data is not None else None
        return isinstance(original, str) and form_value(original).encode(
            "utf-8", errors="strict"
        ) == caption.encode("utf-8", errors="strict")
    except (AttributeError, TypeError, UnicodeError):
        return False


def sanitize_url(value: str) -> str:
    parsed = urlsplit(value)
    query = urlencode(
        [
            (
                key,
                "[redacted]"
                if key.lower() in SENSITIVE_KEYS
                else "[caption-redacted]"
                if key.lower() == "caption"
                else item,
            )
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def map_instagram_error(status: int, payload: dict[str, Any]) -> InstagramError:
    error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
    code = error.get("code")
    message = str(error.get("message") or "A Instagram API recusou a requisicao.")

    details = {
        "http_status": status,
        "meta_code": error.get("code"),
        "meta_subcode": error.get("error_subcode"),
        "meta_type": error.get("type"),
        "meta_error_data": error.get("error_data"),
        "fbtrace_id": error.get("fbtrace_id"),
        "error_user_title": error.get("error_user_title"),
        "error_user_msg": error.get("error_user_msg"),
        "is_transient": error.get("is_transient"),
        "raw_error": error,
        "raw_payload": payload,
    }

    if status == 429 or code in {4, 17, 32, 613, 80002}:
        return InstagramError(
            code="rate_limited",
            message=message,
            retryable=True,
            category="rate_limit",
            http_status=status,
            retry_after_seconds=900,
            details=details,
        )
    if code == 190:
        return InstagramError(
            code="token_expired",
            message=message,
            retryable=False,
            category="token_expired",
            http_status=status,
            details=details,
        )
    if code in {10, 200}:
        return InstagramError(
            code="permission_missing",
            message=message,
            retryable=False,
            category="permission",
            http_status=status,
            details=details,
        )
    if status >= 500:
        return InstagramError(
            code="temporary_failure",
            message=message,
            retryable=True,
            category="temporary",
            http_status=status,
            retry_after_seconds=300,
            details=details,
        )
    return InstagramError(
        code="invalid_request",
        message=message,
        retryable=False,
        category="validation",
        http_status=status,
        details=details,
    )


def media_type(campaign_type: str, mime_type: str) -> str:
    if campaign_type == "reel":
        return "REELS"
    if campaign_type == "story":
        return "STORIES"
    if mime_type.startswith("video/"):
        return "VIDEO"
    return "IMAGE"
