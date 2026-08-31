# -*- coding: utf-8 -*-
"""Shared primitives for the audited Grants & Accelerators Radar pipeline.

The audit is deliberately fail-closed. A report can only be published when:
- every visible programme has a successfully fetched primary/administering source;
- every critical factual field has literal source evidence;
- an independent model validation finds no unsupported material claim;
- every mandatory discovery source was scanned successfully; and
- there are no unresolved in-scope discovery candidates or contradictions.

This does NOT make a mathematically absolute claim that no programme exists outside
of the monitored source universe. It makes completeness measurable against the
explicit mandatory discovery universe in config/grants_sources.yaml.
"""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import requests
import yaml
from bs4 import BeautifulSoup

try:
    import trafilatura
except Exception:  # pragma: no cover
    trafilatura = None

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None


UA = os.getenv(
    "GRANTS_HTTP_UA",
    "Mozilla/5.0 (compatible; GGAdvisoryFundingRadar/2.0; +https://gg-advisory.com.au)",
)
CONNECT_TIMEOUT = float(os.getenv("GRANTS_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = float(os.getenv("GRANTS_READ_TIMEOUT", "30"))
MAX_BYTES = int(os.getenv("GRANTS_MAX_BYTES", str(4_000_000)))
RETRIES = int(os.getenv("GRANTS_HTTP_RETRIES", "2"))
MODEL = os.getenv("GRANTS_MODEL", os.getenv("MODEL", "gpt-4o")).strip()
VALIDATOR_MODEL = os.getenv("GRANTS_VALIDATOR_MODEL", MODEL).strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

ALLOWED_STATUS = {
    "Open now", "Rolling", "Opening soon", "Closed, monitor", "Paused", "Archived"
}
ALLOWED_TYPES = {
    "grant", "repayable_grant", "accelerator", "incubator", "equity", "debt_equity"
}
ALLOWED_LEVELS = {"national", "act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"}

# Every factual field shown in the report must be evidenced. Editorial fields are
# checked by the independent validator but are not required to have a one-to-one
# literal quote.
CRITICAL_FIELDS = ("name", "admin", "level", "type", "status", "amount", "deadline_type", "deadline_label", "target_stage")


def required_evidence_fields(record: Dict[str, Any]) -> Tuple[str, ...]:
    """Return every report-visible structured field that must have literal live-source evidence."""
    fields = list(CRITICAL_FIELDS)
    if clean(record.get("deadline_type")).lower() == "fixed" or clean(record.get("deadline")):
        fields.append("deadline")
    return tuple(fields)


def factual_sentences(record: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Return prose claims that must each be tied to a literal source quote.

    `why_it_matters` is deliberately excluded because it is an editorial assessment; the
    independent adversarial validator still rejects it if it introduces unsupported facts.
    """
    out: List[Tuple[str, str]] = []
    for field in ("description", "signals"):
        text = clean(record.get(field))
        if not text:
            continue
        # Conservative sentence split. Each non-trivial sentence is a separately auditable claim.
        parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text)
        for i, sentence in enumerate(parts, 1):
            sentence = clean(sentence)
            if len(sentence) >= 12:
                out.append((f"{field}:{i}", sentence))
    return out


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


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def normalise_for_match(v: Any) -> str:
    s = clean(v).lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def canonical_url(url: str) -> str:
    if not url:
        return ""
    u, _ = urldefrag(url.strip())
    p = urlparse(u)
    scheme = p.scheme.lower() or "https"
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+", "/", p.path or "/")
    if path != "/":
        path = path.rstrip("/")
    # Tracking query parameters are not provenance.
    return f"{scheme}://{host}{path}"


def domain(url: str) -> str:
    h = urlparse(url).netloc.lower()
    return h[4:] if h.startswith("www.") else h


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def parse_date(v: Any) -> Optional[date]:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def yaml_load(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def yaml_dump(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False, width=120),
        encoding="utf-8",
    )


def json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _request(url: str) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(
                url,
                headers={"User-Agent": UA, "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8"},
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True,
                stream=True,
            )
            chunks: List[bytes] = []
            total = 0
            for chunk in r.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_BYTES:
                    break
            r._content = b"".join(chunks)  # type: ignore[attr-defined]
            r._content_consumed = True  # type: ignore[attr-defined]
            return r
        except Exception as exc:  # pragma: no cover - network dependent
            last_exc = exc
            if attempt < RETRIES:
                time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(str(last_exc) if last_exc else "request failed")


def _extract_pdf_text(data: bytes) -> str:
    if not fitz:
        return ""
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        return "\n".join(page.get_text("text") or "" for page in doc)
    except Exception:
        return ""


def _extract_html_text(raw_html: str, url: str) -> Tuple[str, str]:
    title = ""
    try:
        soup = BeautifulSoup(raw_html, "html.parser")
        if soup.title:
            title = clean(soup.title.get_text(" "))
        for tag in soup(["script", "style", "noscript", "svg", "form"]):
            tag.decompose()
    except Exception:
        soup = None

    text = ""
    if trafilatura:
        try:
            text = trafilatura.extract(
                raw_html,
                url=url,
                include_links=False,
                include_images=False,
                favor_precision=True,
            ) or ""
        except Exception:
            text = ""
    if not text and soup is not None:
        text = soup.get_text("\n")
    return title, re.sub(r"\n{3,}", "\n\n", text or "").strip()


def fetch_url(url: str) -> FetchResult:
    fetched_at = now_utc_iso()
    try:
        r = _request(url)
        status = int(r.status_code)
        ctype = (r.headers.get("content-type") or "").lower()
        data = bytes(r.content or b"")
        final = r.url or url
        if status >= 400:
            return FetchResult(url, final, False, status, ctype, "", "", fetched_at, sha256_text(""), f"HTTP {status}")

        if "pdf" in ctype or final.lower().endswith(".pdf"):
            text = _extract_pdf_text(data)
            title = ""
            raw_html = ""
        else:
            raw_html = data.decode(r.encoding or "utf-8", errors="replace")
            title, text = _extract_html_text(raw_html, final)
        ok = len(clean(text)) >= 120
        return FetchResult(
            requested_url=url,
            final_url=final,
            ok=ok,
            http_status=status,
            content_type=ctype,
            title=title,
            text=text,
            fetched_at=fetched_at,
            sha256=sha256_text(clean(text)),
            error="" if ok else "Source text too short to verify",
            raw_html=raw_html,
        )
    except Exception as exc:
        return FetchResult(url, url, False, None, "", "", "", fetched_at, sha256_text(""), clean(exc))


def extract_links(fetch: FetchResult, allowed_domains: Sequence[str]) -> List[Tuple[str, str]]:
    if not fetch.raw_html:
        return []
    allowed = {d.lower().removeprefix("www.") for d in allowed_domains}
    soup = BeautifulSoup(fetch.raw_html, "html.parser")
    out: Dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = urljoin(fetch.final_url, str(a.get("href") or ""))
        href, _ = urldefrag(href)
        d = domain(href)
        if allowed and not any(d == x or d.endswith("." + x) for x in allowed):
            continue
        p = urlparse(href)
        if p.scheme not in {"http", "https"}:
            continue
        if re.search(r"\.(?:jpg|jpeg|png|gif|svg|zip|docx?|xlsx?|pptx?)$", p.path, re.I):
            continue
        anchor = clean(a.get_text(" "))
        out[canonical_url(href)] = anchor
    return [(u, a) for u, a in out.items() if u]


def _parse_sitemap_xml(text: str) -> Tuple[List[str], List[str]]:
    locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", text, flags=re.I | re.S)
    locs = [html.unescape(clean(x)) for x in locs]
    child_sitemaps = [u for u in locs if u.lower().endswith(".xml") or "sitemap" in u.lower()]
    pages = [u for u in locs if u not in child_sitemaps]
    return child_sitemaps, pages


def discover_sitemap_urls(
    sitemap_url: str,
    allowed_domains: Sequence[str],
    max_sitemaps: int = 12,
    max_urls: int = 10000,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    queue = [sitemap_url]
    seen: set[str] = set()
    pages: List[str] = []
    meta: List[Dict[str, Any]] = []
    while queue and len(seen) < max_sitemaps and len(pages) < max_urls:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)
        f = fetch_url(u)
        meta.append(f.public_dict())
        if not f.ok and not f.raw_html:
            # XML often has too little visible text for fetch_url.ok; use raw content
            try:
                r = _request(u)
                txt = (r.content or b"").decode("utf-8", errors="replace")
            except Exception:
                continue
        else:
            txt = f.raw_html or f.text
        children, urls = _parse_sitemap_xml(txt)
        for c in children:
            if domain(c) in {x.lower().removeprefix("www.") for x in allowed_domains}:
                queue.append(c)
        for p in urls:
            if domain(p) in {x.lower().removeprefix("www.") for x in allowed_domains}:
                pages.append(canonical_url(p))
    return sorted(set(pages))[:max_urls], meta


def relevant_url_score(url: str, anchor: str, source_cfg: Dict[str, Any]) -> int:
    s = normalise_for_match(f"{url} {anchor}")
    include = source_cfg.get("url_terms") or [
        "grant", "grants", "fund", "funding", "accelerator", "innovation", "program", "programme",
        "commercialisation", "commercialization", "clean", "climate", "energy", "net zero", "renewable",
        "decarbon", "circular", "hydrogen", "solar", "battery", "manufactur", "venture",
    ]
    exclude = source_cfg.get("exclude_url_terms") or [
        "news", "media", "privacy", "terms", "contact", "about", "careers", "events", "alumni", "recipient",
    ]
    score = sum(2 for t in include if normalise_for_match(t) in s)
    score -= sum(2 for t in exclude if normalise_for_match(t) in s)
    return score


def relevant_text_score(text: str, scope: Dict[str, Any]) -> int:
    s = normalise_for_match(text[:30000])
    funding_terms = scope.get("funding_terms") or [
        "grant", "funding", "accelerator", "investment", "equity", "loan", "concessional", "tax offset",
    ]
    climate_terms = scope.get("climate_terms") or [
        "climate", "clean energy", "renewable", "net zero", "low emissions", "decarbon", "circular economy",
        "recycling", "hydrogen", "battery", "solar", "green metals", "sustainable fuel", "energy transition",
    ]
    founder_terms = scope.get("venture_terms") or [
        "startup", "start up", "small business", "sme", "commercialisation", "commercialization", "research",
        "manufacturer", "business", "venture", "technology",
    ]
    return (
        sum(2 for t in funding_terms if normalise_for_match(t) in s)
        + sum(2 for t in climate_terms if normalise_for_match(t) in s)
        + sum(1 for t in founder_terms if normalise_for_match(t) in s)
    )


def compact_source_text(text: str, max_chars: int = 18000) -> str:
    """Prioritise lines likely to contain the facts needed for verification."""
    lines = [clean(x) for x in (text or "").splitlines() if clean(x)]
    keys = re.compile(
        r"\b(status|open|closed|application|deadline|closing|due|funding|grant|investment|amount|eligible|eligibility|"
        r"stage|round|cohort|administer|program|programme|accelerator|commercial|fund|support|202[5-9])\b",
        re.I,
    )
    picked: List[str] = []
    # Always keep the beginning for page identity and summary.
    picked.extend(lines[:60])
    for i, line in enumerate(lines):
        if keys.search(line):
            for j in range(max(0, i - 1), min(len(lines), i + 3)):
                if lines[j] not in picked:
                    picked.append(lines[j])
        if sum(len(x) + 1 for x in picked) >= max_chars:
            break
    out = "\n".join(picked)
    return out[:max_chars]


def _openai_json(system: str, user: str, *, model: str, max_tokens: int = 3500) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required for grants verification")
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
    }
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json=payload,
        timeout=(CONNECT_TIMEOUT, 90),
    )
    if r.status_code >= 400:
        raise RuntimeError(f"OpenAI HTTP {r.status_code}: {r.text[:500]}")
    data = r.json()
    content = data["choices"][0]["message"]["content"]
    try:
        return json.loads(content)
    except Exception as exc:
        raise RuntimeError(f"OpenAI returned invalid JSON: {exc}; content={content[:600]}")


def classify_candidate(
    *,
    url: str,
    title: str,
    text: str,
    jurisdiction: str,
    scope_text: str,
    model: Optional[str] = None,
    reverse_prompt: bool = False,
) -> Dict[str, Any]:
    system = (
        "You are a strict funding-programme scope classifier. Use ONLY the supplied source text. "
        "Do not use memory or infer facts not present. The purpose is to avoid both false inclusions and missed programmes. "
        "Return JSON only."
    )
    if reverse_prompt:
        instruction = (
            "First try to prove that this page is OUTSIDE the defined radar scope. If you cannot prove exclusion from the source text, "
            "then assess whether it is an in-scope funding pathway."
        )
    else:
        instruction = (
            "First try to prove that this page is an IN-SCOPE funding pathway. If the evidence is insufficient, do not guess."
        )
    user = f"""
RADAR SCOPE
{scope_text}

JURISDICTION HINT: {jurisdiction}
URL: {url}
TITLE: {title}

SOURCE TEXT
{text[:18000]}

{instruction}
Return exactly these keys:
{{
  "in_scope": true|false,
  "confidence": 0.0-1.0,
  "program_name": "",
  "program_type": "grant|repayable_grant|accelerator|incubator|equity|debt_equity|other",
  "reason": "short evidence-based reason",
  "evidence_quote": "verbatim source quote, <=240 characters"
}}
"""
    return _openai_json(system, user, model=model or MODEL, max_tokens=900)


def extract_program_from_sources(
    *,
    current: Optional[Dict[str, Any]],
    source_bundle: Sequence[Tuple[str, str]],
    verified: date,
    jurisdiction_hint: str,
    scope_text: str,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """Extract a proposed programme record with per-field literal evidence."""
    bundle_text = []
    for idx, (url, text) in enumerate(source_bundle, 1):
        bundle_text.append(f"\n--- SOURCE {idx} ---\nURL: {url}\n{compact_source_text(text)}")
    current_json = json.dumps(current or {}, ensure_ascii=False, indent=2)
    system = (
        "You are a forensic funding-programme data extractor. Use ONLY the supplied primary/administering-body sources. "
        "Never fill a factual field from memory. Prefer the newest explicit source when sources conflict. "
        "If a material conflict cannot be resolved from dates or explicit supersession language, mark unresolved_conflict=true. "
        "Every report-visible structured factual field must have a short VERBATIM quote and source URL. Every factual sentence in description and signals must also appear in claim_evidence with the EXACT sentence text plus a literal supporting quote. Return JSON only."
    )
    user = f"""
VERIFICATION DATE: {verified.isoformat()}
JURISDICTION HINT: {jurisdiction_hint}
RADAR SCOPE:
{scope_text}

CURRENT RECORD (may be stale and must NOT be trusted):
{current_json}

PRIMARY SOURCE BUNDLE:
{''.join(bundle_text)[:50000]}

Extract a CURRENT record. Status must be one of: Open now, Rolling, Opening soon, Closed, monitor, Paused, Archived.
Use "Closed, monitor" when the current round is closed but the programme is recurring or worth monitoring.
Use "Archived" only when the source says there will be no future rounds / no new funding, or the programme is definitively ended.
If a fixed deadline has passed and there is no explicit future round open, status cannot be Open now.

Return exactly:
{{
  "record": {{
    "id": "stable slug; preserve current id when supplied",
    "name": "",
    "admin": "",
    "level": "national|act|nsw|nt|qld|sa|tas|vic|wa",
    "type": "grant|repayable_grant|accelerator|incubator|equity|debt_equity",
    "status": "",
    "amount": "",
    "deadline": "YYYY-MM-DD or null",
    "deadline_type": "fixed|rolling|tbc",
    "deadline_label": "",
    "target_stage": "",
    "url": "canonical primary programme URL",
    "description": "2-4 concise factual sentences; no unsupported detail",
    "why_it_matters": "1-2 concise editorial sentences; do not introduce new factual claims",
    "signals": "optional; only if source explicitly supports what to watch next",
    "show_from": "YYYY-MM-DD or null",
    "show_until": null,
    "last_verified": "{verified.isoformat()}"
  }},
  "evidence": {{
    "name": {{"source_url":"", "quote":""}},
    "admin": {{"source_url":"", "quote":""}},
    "level": {{"source_url":"", "quote":""}},
    "type": {{"source_url":"", "quote":""}},
    "status": {{"source_url":"", "quote":""}},
    "amount": {{"source_url":"", "quote":""}},
    "deadline": {{"source_url":"", "quote":""}},
    "deadline_type": {{"source_url":"", "quote":""}},
    "deadline_label": {{"source_url":"", "quote":""}},
    "target_stage": {{"source_url":"", "quote":""}}
  }},
  "claim_evidence": [
    {{"path":"description:1", "claim":"EXACT full sentence from record.description", "source_url":"", "quote":"verbatim supporting quote"}},
    {{"path":"signals:1", "claim":"EXACT full sentence from record.signals", "source_url":"", "quote":"verbatim supporting quote"}}
  ],
  "supporting_claims": [{{"claim":"", "source_url":"", "quote":""}}],
  "unresolved_conflict": false,
  "conflict_notes": [],
  "overall_confidence": 0.0-1.0
}}
"""
    return _openai_json(system, user, model=model or MODEL, max_tokens=3200)


def independent_validate(
    *,
    record: Dict[str, Any],
    source_bundle: Sequence[Tuple[str, str]],
    verified: date,
    scope_text: str,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    bundle = []
    for idx, (url, text) in enumerate(source_bundle, 1):
        bundle.append(f"\n--- SOURCE {idx} ---\nURL: {url}\n{compact_source_text(text)}")
    system = (
        "You are an independent adversarial auditor. You did not create the proposed programme record. "
        "Use ONLY the supplied source bundle. Your job is to find unsupported, stale, contradictory or misleading claims. "
        "Fail the record if any material factual statement is unsupported. Return JSON only."
    )
    user = f"""
VERIFICATION DATE: {verified.isoformat()}
RADAR SCOPE:
{scope_text}

PROPOSED RECORD:
{json.dumps(record, ensure_ascii=False, indent=2)}

SOURCE BUNDLE:
{''.join(bundle)[:50000]}

Check all report-visible fields, including the prose description and signals. Treat why_it_matters as editorial but fail it if it contains a factual assertion not supported by the sources.
Return exactly:
{{
  "supported": true|false,
  "confidence": 0.0-1.0,
  "field_checks": {{
    "name": {{"supported":true|false,"reason":""}},
    "admin": {{"supported":true|false,"reason":""}},
    "status": {{"supported":true|false,"reason":""}},
    "amount": {{"supported":true|false,"reason":""}},
    "deadline_label": {{"supported":true|false,"reason":""}},
    "target_stage": {{"supported":true|false,"reason":""}},
    "description": {{"supported":true|false,"reason":""}},
    "why_it_matters": {{"supported":true|false,"reason":""}},
    "signals": {{"supported":true|false,"reason":""}}
  }},
  "contradictions": [],
  "material_issues": []
}}
"""
    return _openai_json(system, user, model=model or VALIDATOR_MODEL, max_tokens=1700)


def evidence_quote_is_literal(quote: str, source_text: str) -> bool:
    q = normalise_for_match(quote)
    s = normalise_for_match(source_text)
    return bool(q and len(q) >= 8 and q in s)


def validate_record_schema(record: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    for k in ("id", "name", "admin", "level", "type", "status", "amount", "deadline_label", "target_stage", "url", "description", "why_it_matters"):
        if not clean(record.get(k)):
            issues.append(f"missing:{k}")
    if clean(record.get("level")).lower() not in ALLOWED_LEVELS:
        issues.append("invalid:level")
    if clean(record.get("type")).lower() not in ALLOWED_TYPES:
        issues.append("invalid:type")
    if clean(record.get("status")) not in ALLOWED_STATUS:
        issues.append("invalid:status")
    dtype = clean(record.get("deadline_type")).lower()
    if dtype not in {"fixed", "rolling", "tbc"}:
        issues.append("invalid:deadline_type")
    if dtype == "fixed" and not parse_date(record.get("deadline")):
        issues.append("fixed_deadline_missing_date")
    dl = parse_date(record.get("deadline"))
    status = clean(record.get("status"))
    if dl and status in {"Open now", "Opening soon"} and dl < date.today():
        # Runtime audit has a verification-date-specific version of this check; this catches plainly stale fixtures.
        issues.append("open_status_with_past_deadline")
    if not re.match(r"^https?://", clean(record.get("url"))):
        issues.append("invalid:url")
    return issues


def record_similarity(a: Dict[str, Any], b_name: str, b_url: str) -> float:
    au = canonical_url(clean(a.get("url")))
    bu = canonical_url(b_url)
    if au and bu and au == bu:
        return 1.0
    an = normalise_for_match(a.get("name"))
    bn = normalise_for_match(b_name)
    return SequenceMatcher(None, an, bn).ratio() if an and bn else 0.0


def slugify(name: str) -> str:
    s = normalise_for_match(name).replace(" ", "-")
    return s[:72].strip("-") or "funding-program"


def scope_text(scope_cfg: Dict[str, Any]) -> str:
    scope = scope_cfg.get("scope") or {}
    return clean(scope.get("definition")) or (
        "Australian funding pathways materially relevant to climate-tech founders, technology developers, research spin-outs, "
        "clean-energy/industrial decarbonisation ventures and investors. Includes grants, commercialisation programmes, accelerators, "
        "equity investment and concessional debt/equity pathways. Excludes consumer rebates, household schemes, ordinary procurement, "
        "awards without funding, generic training, and business-efficiency rebates that do not finance climate technology development, "
        "commercialisation, manufacturing or deployment."
    )
