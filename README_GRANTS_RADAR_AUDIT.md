# GG Advisory Grants Radar audit pipeline v3.1

**v3.1 adds a fail-fast package-version preflight.** It prevents a new workflow file from accidentally running older v2 crawler code. The workflow must print `Audited package preflight PASS: 3.1-web-search` before any research begins.

# GG Advisory Grants Radar — audited v3

## Purpose

This pipeline produces the branded **Grants & Accelerators Radar PDF only**. It does not run the former monthly intelligence digest.

The publication rule is fail-closed: **no PDF, commit or email is allowed unless every audit gate passes**.

## Why v3 replaced v2

The v2 trial on 31 August 2026 correctly blocked publication, but it exposed two design problems:

1. Whole-site sitemap crawling treated hundreds of news, case-study and unrelated pages as possible funding programmes. The run surfaced 506 unresolved candidates and took almost an hour. That does not improve completeness; it creates a noisy universe that cannot be meaningfully adjudicated.
2. The v2 extractor demanded literal snippets for derived labels such as jurisdiction, funding type and best-fit stage. Those labels can be factually supported without appearing verbatim on an official page, so the gate produced systematic false failures.

v3 keeps the strict publication gate but changes the evidence architecture.

## v3 audit architecture

### 1. Mandatory official-domain source searches

Every configured source group is searched using the OpenAI Responses API `web_search` tool with an `allowed_domains` filter. The model cannot search arbitrary third-party sites for the programme facts used by the Radar.

Each mandatory source must complete successfully and meet the configured coverage-confidence threshold.

### 2. Fresh verification of every tracked programme

Every currently visible tracked programme is re-searched on its official/administering domains. The stored YAML record is treated as potentially stale.

The updater must produce:

- current programme name and administrator;
- current status;
- funding amount;
- deadline and deadline type;
- target stage;
- current primary URL;
- concise description and signals;
- field-level official-source evidence;
- no unresolved source conflict;
- extraction confidence at or above the hard threshold.

The updater does not publish anything.

### 3. Independent jurisdiction cross-check

After source-by-source discovery, a separate search is run for each jurisdiction:

- national
- ACT
- NSW
- NT
- QLD
- SA
- TAS
- VIC
- WA

This second search is given the programmes already found and is explicitly instructed to look for successor rounds, renamed programmes and omissions.

Any additional candidate must be reconciled, verified and either added, matched to an existing programme, or explicitly excluded. An unresolved candidate blocks publication.

### 4. Independent adversarial programme audit

A second model, using a separate prompt and fresh live web search, re-checks every programme that would appear in the PDF. It is instructed to try to disprove the proposed record and detect newer rounds, closures, pauses, revised amounts or revised deadlines.

Publication fails if:

- the independent auditor rejects any programme;
- any material field is unsupported;
- the auditor finds a different current value for a material field;
- confidence is below the hard threshold;
- any source URL falls outside the allowed official/administering domains;
- any contradiction remains unresolved.

### 5. Hard publication gate

Only after all source, jurisdiction, candidate and programme gates pass can the workflow:

1. replace `config/grants.yaml` with the verified candidate file;
2. write the PASS audit hash into the configuration;
3. generate the branded PDF;
4. commit the verified data, audit and PDF;
5. email the PDF.

## Completeness claim

No automated or manual process can mathematically prove that an unannounced, private, unindexed or newly published programme outside its search universe does not exist.

A v3 **PASS** therefore means something specific and auditable:

- every configured mandatory official/administering source group completed a restricted live-web completeness search;
- every Australian jurisdiction completed an independent cross-check search;
- every candidate surfaced by those searches was reconciled;
- every published programme passed a second independent live-web audit;
- no unresolved candidate or contradiction remained.

That is the strongest defensible completeness claim for a recurring automated Radar. The PDF itself states this limitation rather than claiming impossible absolute omniscience.

## Models

The workflow defaults to:

- discovery/extraction: `gpt-5.6-luna`
- independent audit: `gpt-5.6-terra`

Both can be overridden with `GRANTS_WEB_MODEL` and `GRANTS_WEB_AUDIT_MODEL`.

## Test suite

Run:

```bash
PYTHONPATH=src pytest -q tests
```

The v3 package currently contains 16 deterministic tests covering schema rules, canonical URL matching, source-domain restrictions, jurisdiction coverage and hard confidence thresholds.

## First run

Keep the workflow manual while validating it. A FAIL is a valid outcome and should not be bypassed. Download the `grants-radar-audit-YYYY-MM-DD` artifact to inspect the failed sources, unresolved candidates and programme-level issues.

## v3.2 hardening

The Responses API web-search tool is now forced with `tool_choice: "required"`. The pipeline also verifies that an actual `web_search_call` occurred, requires official source URLs for coverage/record validation, runs a live smoke test before the expensive audit, and prints the first real API/verification error instead of only `coverage_ok=False`. The default research model is GPT-5.6 Terra and the independent adversarial audit model is GPT-5.6 Sol.
