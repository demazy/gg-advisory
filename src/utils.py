import hashlib, re
import urllib.parse
from datetime import datetime, timezone
from typing import Optional


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def today_iso(tz: Optional[str] = None) -> str:
    # GitHub runners are UTC; filename tags should be date-only
    return datetime.now(timezone.utc).date().isoformat()


def normalise_domain(url: str) -> str:
    """Normalise a URL's domain for consistent counting / caps (e.g., strip www.)."""
    dom = urllib.parse.urlparse(url).netloc.lower()
    if dom.startswith("www."):
        dom = dom[4:]
    dom = dom.rstrip(".")
    return dom
