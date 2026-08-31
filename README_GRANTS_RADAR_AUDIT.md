# GG Advisory Grants & Accelerators Radar — audited pipeline

This patch replaces the old `grants-radar` workflow with a **fail-closed, PDF-only** funding-radar pipeline.

## Publication rule

The workflow creates and emails the branded PDF **only when every audit gate passes**. If any source cannot be scanned, any programme cannot be verified, any factual field lacks live primary-source evidence, any contradiction remains, or any discovery candidate remains unresolved, the run fails **before PDF generation and email**.

## What a PASS means

A PASS means, for the verification date of that run:

1. **100% of report-visible programmes** passed schema, source-provenance and independent validation checks.
2. **100% of structured factual fields** shown in the report have literal evidence on a live primary/administering-body source.
3. **100% of factual sentences in `description` and `signals`** have an explicit evidence mapping to a literal source quote.
4. **100% of mandatory discovery sources** in `config/grants_sources.yaml` were successfully scanned without truncation or candidate fetch failures.
5. **100% of discovered candidate URLs** in the configured directory/sitemap universe are explicitly reconciled as matched to an existing record, independently excluded as out of scope, or verified and added; there are zero silent drops and zero unresolved candidates.
6. There are no unresolved source contradictions, duplicate programme IDs/URLs, stale verification dates, or inconsistent open-status/past-deadline combinations.
7. The final PDF is built from the exact candidate YAML that passed the audit, and the audit hash is written into `config/grants.yaml`.

A PASS deliberately **does not claim mathematical omniscience**. No system can prove that an unannounced, private, unindexed, or newly-created opportunity outside the monitored universe does not exist. Completeness is therefore measured against an explicit, auditable source universe. The core scope is national, state and territory pathways plus major Australian climate-tech/deep-tech accelerators and investment routes. Local-government-only micro-grants are outside the core scope unless surfaced by a mandatory national/state catalogue or explicitly added to the source universe.

## Pipeline

```text
mandatory official/program source universe
        ↓
full discovery scan (indexes + selected sitemaps)
        ↓
verify every existing programme against live sources
        ↓
dual independent classification of every untracked discovered candidate
        ↓
proposed config + evidence ledger + candidate ledger + coverage ledger
        ↓
independent adversarial audit
        ↓
PASS? ── no → upload audit artefacts only; NO PDF; NO EMAIL
  │
 yes
  ↓
replace config/grants.yaml with audited candidate
        ↓
build branded PDF
        ↓
commit audited data + audit report + PDF
        ↓
email PDF only
```

## Files in this patch

- `.github/workflows/grants-radar.yml` — audited PDF-only GitHub Action, manual-only initially.
- `config/grants_sources.yaml` — mandatory completeness/discovery universe and scope.
- `src/grants_core.py` — fetching, provenance, evidence, classification and validation primitives.
- `src/update_grants.py` — discovery and first-pass verification/update stage.
- `src/audit_grants.py` — independent fail-closed audit.
- `src/build_grants_pdf.py` — branded PDF generator, gated by a passing audit JSON.
- `tests/` — deterministic audit tests.
- `assets/gg-advisory-logo.png` — report logo.

The existing `config/grants.yaml` is included only as the starting baseline. The workflow updates it **only after audit PASS**.

## GitHub setup

Replace/add the files at the same paths in the repository, commit to `main`, then create a **new** workflow run from:

**Actions → grants-radar-audited → Run workflow → main**

Do not use **Re-run jobs** on an old workflow run, because GitHub re-runs the workflow definition from the old commit.

Required repository secrets:

- `OPENAI_API_KEY`
- `MAIL_USERNAME`
- `MAIL_PASSWORD`

The workflow is manual-only. Its monthly cron remains commented until you are satisfied with the audit behaviour.

## Expected first-run behaviour

The first audited run may fail. That is intentional. A failure means the pipeline found something it could not verify or adjudicate with the required confidence. Download the `grants-radar-audit-YYYY-MM-DD` artefact from the GitHub run. It contains:

- `grants-verified-candidate.yaml`
- `grants-evidence.json`
- `grants-candidates.json`
- `grants-source-coverage.json`
- `grants-audit-YYYY-MM-DD.json`
- `grants-audit-YYYY-MM-DD.md`

Do **not** weaken the gate merely to make the report publish. Resolve the source, classification or provenance issue instead.
