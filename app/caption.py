import hashlib
from typing import Any


def caption_diagnostics(value: str | None) -> dict[str, Any]:
    caption = value or ""
    encoded = caption.encode("utf-8", errors="strict")

    return {
        "present": value is not None,
        "caption_unicode_length": len(caption),
        "caption_utf8_length": len(encoded),
        "caption_sha256": hashlib.sha256(encoded).hexdigest(),
        "caption_utf8_valid": True,
    }
