from __future__ import annotations

import re

_URL_WITH_CREDENTIALS_RE = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<user>[^/\s:@]+):(?P<secret>[^@\s/]+)@", re.IGNORECASE)
_DSN_RE = re.compile(r"\b(?:postgresql|postgres|mysql|mssql|sqlserver)://[^\s]+", re.IGNORECASE)
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_CONNECTION_STRING_SECRET_RE = re.compile(
    r"\b(AccountKey|SharedAccessSignature|Password|ClientSecret)=([^;\s]+)",
    re.IGNORECASE,
)
_NAMED_SECRET_RE = re.compile(
    r"(?i)\b(api[-_]?key|apikey|provider[-_]?key|access[-_]?token|refresh[-_]?token|claim[-_]?token|client[-_]?secret|password|secret)\s*[:=]\s*([^\s,;&]+)"
)
_QUERY_SECRET_RE = re.compile(
    r"([?&](?:api_key|apikey|key|token|sig|signature|code|client_secret|password|claimToken)=)([^&#\s]+)",
    re.IGNORECASE,
)


def redact_text(value: object, *, max_length: int = 2_000) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "unknown error"

    text = _DSN_RE.sub("[REDACTED_DSN]", text)
    text = _URL_WITH_CREDENTIALS_RE.sub(r"\g<scheme>[REDACTED_USER]:[REDACTED_SECRET]@", text)
    text = _BEARER_RE.sub("Bearer [REDACTED]", text)
    text = _CONNECTION_STRING_SECRET_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    text = _QUERY_SECRET_RE.sub(lambda match: f"{match.group(1)}[REDACTED]", text)
    text = _NAMED_SECRET_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)

    if len(text) > max_length:
        return text[: max(0, max_length - 3)] + "..."
    return text


def safe_exception_message(exc: BaseException, *, phase: str | None = None, max_length: int = 2_000) -> str:
    prefix = f"{phase}: " if phase else ""
    return redact_text(f"{prefix}{type(exc).__name__}: {exc}", max_length=max_length)

