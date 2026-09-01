# -*- coding: utf-8 -*-
"""Resilient zero-paid-API verification core for GG Advisory Grants Radar.

Design principles:
- config/grants.yaml is the canonical factual registry.
- Live checks use direct official/administering-source HTTP only.
- A fresh, previously verified evidence snapshot can bridge temporary WAF/TLS/timeout failures.
- Publication blocks on explicit contradictions, stale snapshots, missing evidence contracts,
  or genuinely new high-signal discovery links.
- Discovery compares current monitored index links against a dated baseline inventory; it
  does not reclassify every historical catalogue link as a new grant on every run.
- No OpenAI API or other paid model API is used.
"""
from __future__ import annotations

PIPELINE_VERSION = "7.0-snapshot-sentinel"

import hashlib, json, os, re, time
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import requests, yaml
from bs4 import BeautifulSoup
try:
    import trafilatura
except Exception:
    trafilatura = None
try:
    import pymupdf as fitz
except Exception:
    try:
        import fitz
    except Exception:
        fitz = None

UA = os.getenv(
    "GRANTS_HTTP_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36 GGAdvisoryRadar/7.0",
)
CONNECT_TIMEOUT = float(os.getenv("GRANTS_CONNECT_TIMEOUT", "8"))
READ_TIMEOUT = float(os.getenv("GRANTS_READ_TIMEOUT", "22"))
MAX_BYTES = int(os.getenv("GRANTS_MAX_BYTES", "5000000"))
RETRIES = int(os.getenv("GRANTS_HTTP_RETRIES", "1"))
MAX_HTTP_REQUESTS = int(os.getenv("GRANTS_MAX_HTTP_REQUESTS", "120"))
_HTTP_COUNT = 0

ALLOWED_STATUS = {"Open now", "Rolling", "Opening soon", "Closed, monitor", "Paused", "Archived"}
ALLOWED_TYPES = {"grant", "repayable_grant", "accelerator", "incubator", "equity", "debt_equity"}
ALLOWED_LEVELS = {"national", "act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"}
FACT_FINGERPRINT_FIELDS = (
    "name", "admin", "level", "type", "status", "amount", "deadline",
    "deadline_type", "deadline_label", "target_stage",
)


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical_url(url: str) -> str:
    if not url:
        return ""
    u, _ = urldefrag(str(url).strip())
    p = urlparse(u)
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+", "/", p.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return f"{(p.scheme or 'https').lower()}://{host}{path}"


def domain(url: str) -> str:
    h = urlparse(url).netloc.lower()
    return h[4:] if h.startswith("www.") else h


def yaml_load(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def yaml_dump(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False, width=140), encoding="utf-8")


def json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_date(v: Any) -> Optional[date]:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def age_days(verified_date: Any, as_of: date) -> int:
    d = parse_date(verified_date)
    if not d:
        return 999999
    return max(0, (as_of - d).days)


def record_visible(record: Dict[str, Any], verified: date) -> bool:
    if record.get("include_in_report") is False:
        return False
    sf = parse_date(record.get("show_from"))
    su = parse_date(record.get("show_until"))
    status = clean(record.get("status")).lower()
    if sf and verified < sf:
        return False
    if su and verified > su and not status:
        return False
    if status == "archived" and not record.get("include_archived"):
        return False
    return True


def factual_fingerprint(record: Dict[str, Any]) -> str:
    payload = {k: record.get(k) for k in FACT_FINGERPRINT_FIELDS}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class FetchResult:
    requested_url: str
    final_url: str
    ok: bool
    http_status: Optional[int]
    content_type: str
    title: str
    text: str
    fetched_at: str
    sha256: str
    error: str = ""
    raw_html: str = ""

    def public_dict(self, include_text: bool = False) -> Dict[str, Any]:
        d = asdict(self)
        if not include_text:
            d.pop("text", None)
            d.pop("raw_html", None)
        return d


def reset_http_counter() -> None:
    global _HTTP_COUNT
    _HTTP_COUNT = 0


def http_count() -> int:
    return _HTTP_COUNT


def _request(url: str) -> requests.Response:
    global _HTTP_COUNT
    last = None
    for attempt in range(RETRIES + 1):
        _HTTP_COUNT += 1
        if _HTTP_COUNT > MAX_HTTP_REQUESTS:
            raise RuntimeError(f"HTTP_BUDGET_EXCEEDED:{_HTTP_COUNT}>{MAX_HTTP_REQUESTS}")
        try:
            r = requests.get(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-AU,en;q=0.9",
                    "Cache-Control": "no-cache",
                },
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True,
                stream=True,
            )
            chunks = []
            total = 0
            for chunk in r.iter_content(65536):
                if not chunk:
                    continue
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_BYTES:
                    break
            r._content = b"".join(chunks)
            r._content_consumed = True
            return r
        except Exception as exc:
            last = exc
            if attempt < RETRIES:
                time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(str(last) if last else "request failed")


def _html_text(raw: str, url: str) -> Tuple[str, str]:
    title = ""
    soup = None
    try:
        soup = BeautifulSoup(raw, "html.parser")
        if soup.title:
            title = clean(soup.title.get_text(" "))
        for t in soup(["script", "style", "noscript", "svg"]):
            t.decompose()
    except Exception:
        soup = None
    text = ""
    if trafilatura:
        try:
            text = trafilatura.extract(
                raw, url=url, include_links=False, include_images=False, favor_precision=False
            ) or ""
        except Exception:
            text = ""
    if not text and soup is not None:
        text = soup.get_text("\n")
    return title, re.sub(r"\n{3,}", "\n\n", text or "").strip()


def fetch_url(url: str) -> FetchResult:
    ts = now_utc_iso()
    try:
        r = _request(url)
        status = int(r.status_code)
        ctype = (r.headers.get("content-type") or "").lower()
        data = bytes(r.content or b"")
        final = r.url or url
        if status >= 400:
            return FetchResult(url, final, False, status, ctype, "", "", ts, "", f"HTTP {status}")
        raw = ""
        title = ""
        text = ""
        if "pdf" in ctype or final.lower().endswith(".pdf"):
            if fitz:
                try:
                    doc = fitz.open(stream=data, filetype="pdf")
                    text = "\n".join(p.get_text("text") or "" for p in doc)
                except Exception:
                    text = ""
        else:
            raw = data.decode(r.encoding or "utf-8", errors="replace")
            title, text = _html_text(raw, final)
        norm = clean(text)
        ok = len(norm) >= 80
        return FetchResult(
            url, final, ok, status, ctype, title, text, ts,
            hashlib.sha256(norm.encode("utf-8", errors="ignore")).hexdigest() if norm else "",
            "" if ok else "source_text_too_short", raw,
        )
    except Exception as exc:
        return FetchResult(url, url, False, None, "", "", "", ts, "", "request_error:" + clean(exc))


def extract_links(fetch: FetchResult, allowed_domains: Sequence[str]) -> List[Tuple[str, str]]:
    if not fetch.raw_html:
        return []
    allowed = {d.lower().removeprefix("www.") for d in allowed_domains}
    soup = BeautifulSoup(fetch.raw_html, "html.parser")
    out: Dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        u = urljoin(fetch.final_url, str(a.get("href") or ""))
        u, _ = urldefrag(u)
        if urlparse(u).scheme not in {"http", "https"}:
            continue
        d = domain(u)
        if allowed and not any(d == x or d.endswith("." + x) for x in allowed):
            continue
        if re.search(r"\.(jpg|jpeg|png|gif|svg|zip|docx?|xlsx?|pptx?)$", urlparse(u).path, re.I):
            continue
        out[canonical_url(u)] = clean(a.get_text(" "))
    return sorted(out.items())


def _matches_any(text: str, patterns: Sequence[str]) -> bool:
    if not patterns:
        return True
    for pat in patterns:
        try:
            if re.search(pat, text, re.I | re.S):
                return True
        except re.error:
            if clean(pat).lower() in clean(text).lower():
                return True
    return False


def validate_record_schema(record: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    if not clean(record.get("id")):
        issues.append("missing:id")
    if clean(record.get("level")).lower() not in ALLOWED_LEVELS:
        issues.append("invalid:level")
    if clean(record.get("type")).lower() not in ALLOWED_TYPES:
        issues.append("invalid:type")
    if clean(record.get("status")) and clean(record.get("status")) not in ALLOWED_STATUS:
        issues.append("invalid:status")
    if not re.match(r"^https?://", clean(record.get("url"))):
        issues.append("invalid:url")
    if clean(record.get("deadline_type")) == "fixed" and not parse_date(record.get("deadline")):
        issues.append("fixed_deadline_missing")
    return issues


def _future_dates(text: str, verified: date) -> List[date]:
    out: List[date] = []
    months = {
        "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,
        "august":8,"september":9,"october":10,"november":11,"december":12,
        "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,
        "oct":10,"nov":11,"dec":12,
    }
    for m in re.finditer(
        r"\b([0-3]?\d)\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})\b",
        text, re.I,
    ):
        try:
            d = date(int(m.group(3)), months[m.group(2).lower()], int(m.group(1)))
            if d >= verified:
                out.append(d)
        except Exception:
            pass
    return sorted(set(out))


def explicit_status_contradictions(record: Dict[str, Any], page_text: str, verified: date) -> List[str]:
    low = clean(page_text).lower()
    status = clean(record.get("status"))
    issues: List[str] = []
    open_mark = bool(re.search(r"\b(applications? (?:are )?(?:now )?open|apply now|open for applications|apply at any time)\b", low))
    closed_mark = bool(re.search(r"\b(currently closed|applications? (?:are |have )?(?:now )?closed|no longer accepting new applications)\b", low))
    paused_mark = bool(re.search(r"\b(paused to new applications|undergoing an evaluation|not accepting new applications)\b", low))
    if status in {"Open now", "Rolling"} and (closed_mark or paused_mark):
        issues.append("live_status_contradiction:registry_open_source_closed_or_paused")
    if status == "Closed, monitor" and open_mark:
        future = _future_dates(page_text, verified)
        if future or re.search(r"\bapply now\b", low):
            issues.append("live_status_contradiction:registry_closed_source_open")
    if status == "Paused" and open_mark and not paused_mark:
        issues.append("live_status_contradiction:registry_paused_source_open")
    if status == "Opening soon" and open_mark:
        issues.append("live_status_contradiction:registry_opening_soon_source_open")
    return issues


def _snapshot_valid(record: Dict[str, Any], snapshot: Dict[str, Any], verified: date, max_age: int) -> Tuple[bool, str]:
    if not snapshot:
        return False, "snapshot_missing"
    if age_days(snapshot.get("verified_date"), verified) > max_age:
        return False, f"snapshot_stale:{age_days(snapshot.get('verified_date'), verified)}d>{max_age}d"
    expected = clean(snapshot.get("record_fingerprint"))
    actual = factual_fingerprint(record)
    if expected and expected != actual:
        return False, "snapshot_fingerprint_mismatch"
    return True, ""


def verify_record(
    record: Dict[str, Any],
    contract: Dict[str, Any],
    snapshot: Dict[str, Any],
    verified: date,
    fetcher=fetch_url,
) -> Dict[str, Any]:
    issues = validate_record_schema(record)
    warnings: List[str] = []
    max_age = int(contract.get("snapshot_max_age_days", 45))
    source_urls = [clean(x) for x in (contract.get("source_urls") or []) if clean(x)]
    if not source_urls:
        source_urls = [clean(record.get("url"))]
    fetched: List[FetchResult] = []
    live: List[FetchResult] = []
    seen = set()
    for u in source_urls:
        cu = canonical_url(u)
        if not cu or cu in seen:
            continue
        seen.add(cu)
        fr = fetcher(u)
        fetched.append(fr)
        if fr.ok:
            live.append(fr)

    snap_ok, snap_reason = _snapshot_valid(record, snapshot, verified, max_age)
    if not live:
        if snap_ok:
            warnings.append("fresh_snapshot_fallback:all_live_sources_unavailable")
            return {
                "id": record.get("id"), "name": record.get("name"), "pass": not issues,
                "issues": issues, "warnings": warnings, "verification_mode": "fresh_snapshot_fallback",
                "snapshot_verified_date": snapshot.get("verified_date"),
                "sources": [x.public_dict(False) for x in fetched],
                "live_confirmation": False,
            }
        issues.append("all_live_sources_unavailable:" + snap_reason)
        return {
            "id": record.get("id"), "name": record.get("name"), "pass": False,
            "issues": issues, "warnings": warnings, "verification_mode": "failed",
            "snapshot_verified_date": snapshot.get("verified_date"),
            "sources": [x.public_dict(False) for x in fetched], "live_confirmation": False,
        }

    all_text = "\n".join((x.title + "\n" + x.text) for x in live)
    name_patterns = contract.get("name_patterns_any") or []
    identity_live = [x for x in live if _matches_any(x.title + "\n" + x.text, name_patterns)] if name_patterns else list(live)
    identity_text = "\n".join((x.title + "\n" + x.text) for x in identity_live) or all_text
    name_ok = _matches_any(identity_text, name_patterns)
    if not name_ok:
        # Identity failure is hard only if snapshot cannot bridge it.
        if snap_ok:
            warnings.append("identity_not_reconfirmed_live:using_fresh_snapshot")
        else:
            issues.append("identity_not_confirmed_live")

    issues.extend(explicit_status_contradictions(record, identity_text, verified))

    status_patterns = contract.get("status_patterns_any") or []
    status_ok = _matches_any(identity_text, status_patterns)
    if status_patterns and not status_ok:
        if snap_ok:
            warnings.append("status_not_reconfirmed_live:using_fresh_snapshot")
        else:
            issues.append("status_not_confirmed_live")

    amount_patterns = contract.get("amount_patterns_any") or []
    amount_ok = _matches_any(identity_text, amount_patterns)
    if amount_patterns and not amount_ok:
        if snap_ok:
            warnings.append("amount_not_reconfirmed_live:using_fresh_snapshot")
        else:
            issues.append("amount_not_confirmed_live")

    deadline_patterns = contract.get("deadline_patterns_any") or []
    deadline_ok = _matches_any(identity_text, deadline_patterns)
    if deadline_patterns and not deadline_ok:
        # Only a *deadline-specific* date different from the registry is a hard contradiction.
        # Programme pages often contain many unrelated future webinar, outcome and project dates.
        known = parse_date(record.get("deadline"))
        deadline_dates: List[date] = []
        date_pat = r"([0-3]?\d)\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})"
        for m in re.finditer(r"(?:applications?\s+(?:close|closing|closed)|closes?|closing\s+date|deadline|apply\s+by)[^\n.]{0,90}?" + date_pat, identity_text, re.I):
            try:
                token = m.group(2).lower()
                months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12,"jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12}
                deadline_dates.append(date(int(m.group(3)), months[token], int(m.group(1))))
            except Exception:
                pass
        deadline_dates = sorted(set(d for d in deadline_dates if d >= verified))
        if known and deadline_dates and any(d != known for d in deadline_dates):
            issues.append("live_deadline_contradiction:" + ",".join(d.isoformat() for d in deadline_dates[:4]))
        elif snap_ok:
            warnings.append("deadline_not_reconfirmed_live:using_fresh_snapshot")
        else:
            issues.append("deadline_not_confirmed_live")

    full_live = name_ok and status_ok and amount_ok and deadline_ok and not issues
    mode = "live_confirmed" if full_live else ("live_plus_fresh_snapshot" if snap_ok and not issues else "failed")
    return {
        "id": record.get("id"), "name": record.get("name"), "pass": not issues,
        "issues": issues, "warnings": warnings, "verification_mode": mode,
        "snapshot_verified_date": snapshot.get("verified_date"),
        "sources": [x.public_dict(False) for x in fetched],
        "live_confirmation": bool(full_live),
        "live_source_urls": [x.final_url for x in live],
    }


def candidate_score(url: str, anchor: str, source_cfg: Dict[str, Any], scope_cfg: Dict[str, Any]) -> int:
    text = (url + " " + anchor).lower()
    fterms = [str(x).lower() for x in scope_cfg.get("funding_terms", [])]
    cterms = [str(x).lower() for x in scope_cfg.get("climate_terms", [])]
    score = 0
    if any(x in text for x in fterms):
        score += 2
    if any(x in text for x in cterms):
        score += 3
    for x in source_cfg.get("url_terms") or []:
        if str(x).lower() in text:
            score += 1
    for x in source_cfg.get("exclude_url_terms") or []:
        if str(x).lower() in text:
            score -= 4
    return score


def discover_source(
    source_cfg: Dict[str, Any],
    scope_cfg: Dict[str, Any],
    baseline: Dict[str, Any],
    decisions: Dict[str, Any],
    registry_urls: Sequence[str],
    verified: date,
    fetcher=fetch_url,
) -> Dict[str, Any]:
    fetched: List[Dict[str, Any]] = []
    links: Dict[str, str] = {}
    urls = list(source_cfg.get("index_urls") or []) + list(source_cfg.get("fallback_index_urls") or [])
    for u in urls:
        fr = fetcher(u)
        fetched.append(fr.public_dict(False))
        if not fr.ok:
            continue
        for link, anchor in extract_links(fr, source_cfg.get("allowed_domains") or []):
            links[link] = anchor

    live_ok = any(x.get("ok") for x in fetched)
    base_age = age_days(baseline.get("verified_date"), verified)
    max_age = int(source_cfg.get("snapshot_max_age_days", 45))
    snapshot_ok = base_age <= max_age
    covered = live_ok or snapshot_ok
    warnings: List[str] = []
    errors: List[str] = []
    if not live_ok:
        if snapshot_ok:
            warnings.append(f"fresh_source_inventory_snapshot_fallback:{base_age}d")
        else:
            errors.append(f"source_unreachable_and_snapshot_stale:{base_age}d>{max_age}d")

    current_candidates: Dict[str, Dict[str, Any]] = {}
    if source_cfg.get("discovery") is not False and live_ok:
        min_score = int(source_cfg.get("min_url_score", 2))
        for u, a in sorted(links.items()):
            sc = candidate_score(u, a, source_cfg, scope_cfg)
            if sc >= min_score:
                current_candidates[canonical_url(u)] = {"url": canonical_url(u), "anchor": a, "score": sc}

    known = set(canonical_url(x) for x in (baseline.get("known_urls") or []) if x)
    reg = set(canonical_url(x) for x in registry_urls if x)
    rows: List[Dict[str, Any]] = []
    new_unresolved: List[str] = []
    for cu, c in sorted(current_candidates.items()):
        if cu in known:
            res = "known_baseline"
            reason = "Present in the dated monitored-source inventory; not newly surfaced in this run."
        elif cu in reg:
            res = "include_or_match"
            reason = "Matches a programme already present in the canonical registry."
        else:
            d = decisions.get(cu) or {}
            res = d.get("action", "unresolved")
            reason = d.get("reason", "New high-signal link since the dated discovery baseline; editorial adjudication required.")
            if res == "unresolved":
                new_unresolved.append(cu)
        rows.append({**c, "resolution": res, "reason": reason})

    baseline_candidate = dict(baseline)
    if live_ok and not new_unresolved:
        baseline_candidate["verified_date"] = verified.isoformat()
        baseline_candidate["known_urls"] = sorted(known | set(current_candidates))

    return {
        "source_id": source_cfg.get("id"),
        "jurisdiction": source_cfg.get("jurisdiction"),
        "required": bool(source_cfg.get("required")),
        "covered": covered,
        "live_ok": live_ok,
        "coverage_mode": "live" if live_ok else ("fresh_snapshot" if snapshot_ok else "failed"),
        "errors": errors,
        "warnings": warnings,
        "fetched_indexes": fetched,
        "candidates": rows,
        "new_unresolved": new_unresolved,
        "baseline_candidate": baseline_candidate,
    }
