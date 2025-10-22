import hashlib
import re
from datetime import datetime, timezone
from typing import Optional

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def today_iso(tz: Optional[str] = None) -> str:
    # GitHub runners are UTC; date-only slug is fine for filenames
    return datetime.now(timezone.utc).date().isoformat()
