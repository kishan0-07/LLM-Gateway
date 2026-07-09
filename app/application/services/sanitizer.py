import re

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")

_PATTERNS = [
    (_EMAIL_RE, "[EMAIL]"),
    (_PHONE_RE, "[PHONE]"),
    (_SSN_RE, "[SSN]"),
]


def sanitize(text: str, max_length: int = 500) -> str:
    truncated = text[:max_length]
    for pattern, replacement in _PATTERNS:
        truncated = pattern.sub(replacement, truncated)
    return truncated