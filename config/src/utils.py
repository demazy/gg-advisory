import hashlib, re
from datetime import datetime, timezone
from typing import Optional

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def today_iso(tz: Optional[str] = None) -> str:
    # GitHub runners are UTC; filename tags should be date-only
    return datetime.now(timezone.utc).date().isoformat()
