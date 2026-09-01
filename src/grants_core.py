# -*- coding: utf-8 -*-
"""Zero-API verification core for the GG Advisory Grants Radar.

The production path deliberately does not call OpenAI or any other paid model API.
Publication quality is enforced by direct official-source fetches, field-level evidence
contracts, deterministic change heuristics, discovery reconciliation and fail-closed gates.
"""
from __future__ import annotations

PIPELINE_VERSION = "6.0-zero-api-official-source"

import hashlib, html, json, os, re, time
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
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

UA = os.getenv("GRANTS_HTTP_UA", "Mozilla/5.0 (compatible; GGAdvisoryFundingRadar/6.0; +https://gg-advisory.com.au)")
CONNECT_TIMEOUT = float(os.getenv("GRANTS_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = float(os.getenv("GRANTS_READ_TIMEOUT", "35"))
MAX_BYTES = int(os.getenv("GRANTS_MAX_BYTES", "5000000"))
RETRIES = int(os.getenv("GRANTS_HTTP_RETRIES", "2"))
MAX_HTTP_REQUESTS = int(os.getenv("GRANTS_MAX_HTTP_REQUESTS", "180"))
_HTTP_COUNT = 0

STOP = set("a an and are as at be by for from in into is it its of on or the this to up via with per current program programme grant grants fund funding".split())

ALLOWED_STATUS = {"Open now","Rolling","Opening soon","Closed, monitor","Paused","Archived"}
ALLOWED_TYPES = {"grant","repayable_grant","accelerator","incubator","equity","debt_equity"}
ALLOWED_LEVELS = {"national","act","nsw","nt","qld","sa","tas","vic","wa"}

def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def canonical_url(url: str) -> str:
    if not url: return ""
    u,_=urldefrag(url.strip()); p=urlparse(u)
    host=p.netloc.lower()
    if host.startswith("www."): host=host[4:]
    path=re.sub(r"/+","/",p.path or "/")
    if path!="/": path=path.rstrip("/")
    return f"{(p.scheme or 'https').lower()}://{host}{path}"

def domain(url: str) -> str:
    h=urlparse(url).netloc.lower()
    return h[4:] if h.startswith("www.") else h

def yaml_load(path: Path) -> Dict[str,Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def yaml_dump(path: Path, data: Dict[str,Any]) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(yaml.safe_dump(data,allow_unicode=True,sort_keys=False,width=120),encoding="utf-8")

def json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,indent=2,ensure_ascii=False),encoding="utf-8")

def parse_date(v: Any) -> Optional[date]:
    if not v: return None
    try: return date.fromisoformat(str(v)[:10])
    except Exception: return None

def record_visible(record: Dict[str,Any], verified: date) -> bool:
    if record.get("include_in_report") is False: return False
    sf=parse_date(record.get("show_from")); su=parse_date(record.get("show_until"))
    status=clean(record.get("status")).lower()
    if sf and verified<sf: return False
    if su and verified>su and not status: return False
    if status=="archived" and not record.get("include_archived"): return False
    return True

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
    def public_dict(self, include_text=False):
        d=asdict(self)
        if not include_text:
            d.pop("text",None); d.pop("raw_html",None)
        return d

def reset_http_counter() -> None:
    global _HTTP_COUNT
    _HTTP_COUNT=0

def http_count() -> int:
    return _HTTP_COUNT

def _request(url: str) -> requests.Response:
    global _HTTP_COUNT
    _HTTP_COUNT += 1
    if _HTTP_COUNT > MAX_HTTP_REQUESTS:
        raise RuntimeError(f"HTTP_BUDGET_EXCEEDED:{_HTTP_COUNT}>{MAX_HTTP_REQUESTS}")
    last=None
    for attempt in range(RETRIES+1):
        try:
            r=requests.get(url,headers={"User-Agent":UA,"Accept":"text/html,application/pdf;q=0.9,*/*;q=0.8"},
                           timeout=(CONNECT_TIMEOUT,READ_TIMEOUT),allow_redirects=True,stream=True)
            chunks=[]; total=0
            for chunk in r.iter_content(65536):
                if not chunk: continue
                chunks.append(chunk); total += len(chunk)
                if total>MAX_BYTES: break
            r._content=b"".join(chunks); r._content_consumed=True
            return r
        except Exception as exc:
            last=exc
            if attempt<RETRIES: time.sleep(0.8*(attempt+1))
    raise RuntimeError(str(last) if last else "request failed")

def _html_text(raw: str, url: str) -> Tuple[str,str]:
    title=""
    try:
        soup=BeautifulSoup(raw,"html.parser")
        if soup.title: title=clean(soup.title.get_text(" "))
        for t in soup(["script","style","noscript","svg","form"]): t.decompose()
    except Exception:
        soup=None
    text=""
    if trafilatura:
        try: text=trafilatura.extract(raw,url=url,include_links=False,include_images=False,favor_precision=True) or ""
        except Exception: text=""
    if not text and soup is not None: text=soup.get_text("\n")
    return title,re.sub(r"\n{3,}","\n\n",text or "").strip()

def fetch_url(url: str) -> FetchResult:
    ts=now_utc_iso()
    try:
        r=_request(url); status=int(r.status_code); ctype=(r.headers.get("content-type") or "").lower()
        data=bytes(r.content or b""); final=r.url or url
        if status>=400: return FetchResult(url,final,False,status,ctype,"","",ts,"",f"HTTP {status}")
        if "pdf" in ctype or final.lower().endswith(".pdf"):
            text=""
            if fitz:
                try:
                    doc=fitz.open(stream=data,filetype="pdf")
                    text="\n".join(p.get_text("text") or "" for p in doc)
                except Exception: text=""
            title=""; raw=""
        else:
            raw=data.decode(r.encoding or "utf-8",errors="replace"); title,text=_html_text(raw,final)
        norm=clean(text)
        return FetchResult(url,final,len(norm)>=100,status,ctype,title,text,ts,
                           hashlib.sha256(norm.encode("utf-8",errors="ignore")).hexdigest(),
                           "" if len(norm)>=100 else "source_text_too_short",raw)
    except Exception as exc:
        return FetchResult(url,url,False,None,"","","",ts,"","request_error:"+clean(exc))

def extract_links(fetch: FetchResult, allowed_domains: Sequence[str]) -> List[Tuple[str,str]]:
    if not fetch.raw_html: return []
    allowed={d.lower().removeprefix("www.") for d in allowed_domains}
    soup=BeautifulSoup(fetch.raw_html,"html.parser"); out={}
    for a in soup.find_all("a",href=True):
        u=urljoin(fetch.final_url,str(a.get("href") or "")); u,_=urldefrag(u)
        if urlparse(u).scheme not in {"http","https"}: continue
        d=domain(u)
        if allowed and not any(d==x or d.endswith("."+x) for x in allowed): continue
        if re.search(r"\.(jpg|jpeg|png|gif|svg|zip|docx?|xlsx?|pptx?)$",urlparse(u).path,re.I): continue
        out[canonical_url(u)]=clean(a.get_text(" "))
    return sorted(out.items())

def _tokens(text: str) -> List[str]:
    s=clean(text).lower().replace("&"," and ")
    toks=re.findall(r"[a-z0-9]+(?:\.[0-9]+)?",s)
    return [t for t in toks if len(t)>1 and t not in STOP]

def support_score(support: str, page_text: str) -> float:
    st=_tokens(support); pt=set(_tokens(page_text))
    if not st: return 1.0
    # Weight dates/numbers/currency-like tokens more strongly by requiring their presence.
    uniq=list(dict.fromkeys(st))
    weighted=0.0; got=0.0
    for t in uniq:
        w=2.2 if re.search(r"\d",t) else 1.0
        weighted += w
        if t in pt: got += w
    return got/weighted if weighted else 1.0

def validate_record_schema(record: Dict[str,Any]) -> List[str]:
    issues=[]; rid=clean(record.get("id"))
    if not rid: issues.append("missing:id")
    if clean(record.get("level")).lower() not in ALLOWED_LEVELS: issues.append("invalid:level")
    if clean(record.get("type")).lower() not in ALLOWED_TYPES: issues.append("invalid:type")
    if clean(record.get("status")) and clean(record.get("status")) not in ALLOWED_STATUS: issues.append("invalid:status")
    if not re.match(r"^https?://",clean(record.get("url"))): issues.append("invalid:url")
    if clean(record.get("deadline_type"))=="fixed" and not parse_date(record.get("deadline")): issues.append("fixed_deadline_missing")
    return issues

def _future_dates(text: str, verified: date) -> List[date]:
    out=[]
    month_names={"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
                 "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12}
    for m in re.finditer(r"\b([0-3]?\d)\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})\b",text,re.I):
        try:
            d=date(int(m.group(3)),month_names[m.group(2).lower()],int(m.group(1)))
            if d>=verified: out.append(d)
        except Exception: pass
    for m in re.finditer(r"\b(20\d{2})[-/](0?[1-9]|1[0-2])[-/]([0-3]?\d)\b",text):
        try:
            d=date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
            if d>=verified: out.append(d)
        except Exception: pass
    return sorted(set(out))

def change_heuristics(record: Dict[str,Any], page_text: str, verified: date) -> List[str]:
    """Flag obvious stale-status/deadline situations. Conservative: warnings become hard
    failures only when there is a clearly newer application date contradicting a closed record."""
    issues=[]; txt=clean(page_text); low=txt.lower()
    status=clean(record.get("status"))
    dl=parse_date(record.get("deadline"))
    future=_future_dates(txt,verified)
    if status=="Closed, monitor" and future:
        if re.search(r"\b(applications?|apply|closing|closes|deadline)\b",low):
            if not dl or max(future)>dl:
                issues.append("possible_new_application_round:"+max(future).isoformat())
    if status=="Open now" and dl and dl < verified:
        issues.append("open_status_with_past_deadline")
    if status=="Paused" and re.search(r"\bapplications? (are )?(now )?open\b",low):
        issues.append("paused_record_but_page_says_applications_open")
    return issues

def verify_record(record: Dict[str,Any], contract: Dict[str,Any], verified: date, fetcher=fetch_url) -> Dict[str,Any]:
    issues=validate_record_schema(record); field_results={}; cache={}
    min_score=float(contract.get("min_support_score",0.62))
    for field,row in (contract.get("fields") or {}).items():
        u=clean(row.get("source_url")); support=clean(row.get("support"))
        if not u or not support:
            issues.append(f"contract_missing:{field}"); continue
        cu=canonical_url(u)
        if cu not in cache: cache[cu]=fetcher(u)
        fr=cache[cu]
        if not fr.ok:
            issues.append(f"source_unavailable:{field}:{fr.error}")
            field_results[field]={"pass":False,"score":0.0,"source_url":u}
            continue
        sc=support_score(support,fr.text)
        passed=sc>=min_score
        if not passed: issues.append(f"evidence_support_changed:{field}:{sc:.3f}")
        field_results[field]={"pass":passed,"score":round(sc,3),"source_url":fr.final_url}
    primary=cache.get(canonical_url(record.get("url","")))
    if primary is None:
        primary=fetcher(record.get("url","")); cache[canonical_url(record.get("url",""))]=primary
    if not primary.ok: issues.append("primary_source_unavailable:"+primary.error)
    else: issues.extend(change_heuristics(record,primary.text,verified))
    return {"id":record.get("id"),"name":record.get("name"),"pass":not issues,"issues":issues,
            "field_checks":field_results,"sources":[v.public_dict(False) for v in cache.values()]}

def candidate_score(url: str, anchor: str, source_cfg: Dict[str,Any], scope_cfg: Dict[str,Any]) -> int:
    text=(url+" "+anchor).lower()
    fterms=[str(x).lower() for x in scope_cfg.get("funding_terms",[])]
    cterms=[str(x).lower() for x in scope_cfg.get("climate_terms",[])]
    score=0
    if any(x in text for x in fterms): score += 2
    if any(x in text for x in cterms): score += 3
    for x in source_cfg.get("url_terms") or []:
        if str(x).lower() in text: score += 1
    for x in source_cfg.get("exclude_url_terms") or []:
        if str(x).lower() in text: score -= 4
    return score

def discover_source(source_cfg: Dict[str,Any], scope_cfg: Dict[str,Any], decisions: Dict[str,Any], fetcher=fetch_url) -> Dict[str,Any]:
    links={}; fetched=[]; errors=[]
    for u in source_cfg.get("index_urls") or []:
        fr=fetcher(u); fetched.append(fr.public_dict(False))
        if not fr.ok:
            errors.append(f"{u}:{fr.error}"); continue
        for link,anchor in extract_links(fr,source_cfg.get("allowed_domains") or []):
            links[link]=anchor
    candidates=[]
    if source_cfg.get("discovery") is False:
        return {"source_id":source_cfg.get("id"),"jurisdiction":source_cfg.get("jurisdiction"),"required":bool(source_cfg.get("required")),"ok":bool(fetched) and any(x.get("ok") for x in fetched),"errors":errors,"fetched_indexes":fetched,"candidates":[]}
    min_score=int(source_cfg.get("min_url_score",2))
    for u,a in sorted(links.items()):
        sc=candidate_score(u,a,source_cfg,scope_cfg)
        if sc<min_score: continue
        d=decisions.get(canonical_url(u)) or {}
        candidates.append({"url":u,"anchor":a,"score":sc,"resolution":d.get("action","unresolved"),
                           "reason":d.get("reason","new high-signal candidate not yet adjudicated")})
    return {"source_id":source_cfg.get("id"),"jurisdiction":source_cfg.get("jurisdiction"),
            "required":bool(source_cfg.get("required")),"ok":bool(fetched) and any(x.get("ok") for x in fetched),
            "errors":errors,"fetched_indexes":fetched,"candidates":candidates}
