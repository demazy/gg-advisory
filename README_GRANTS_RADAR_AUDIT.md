# GG Advisory Grants & Accelerators Radar - canonical audited pipeline

## Fixed architecture

Two different artefacts have two different jobs:

- `config/grants.yaml` is the **canonical factual registry and change ledger**. Current programme facts remain as flat fields. Material changes are appended to each record's `history` array with the verification date, before/after values and official source URLs.
- the latest approved Radar PDF is the **layout reference only**. It is never used as a factual data source. The pipeline compares the newly generated PDF against the previous approved PDF for page geometry, cover treatment, section structure, logo presence, anchor position and off-page text.

A successful run follows this sequence:

```text
config/grants.yaml
    -> source-level official-domain completeness searches
    -> fresh verification of every currently tracked pathway
    -> explicit disposition for every previously tracked pathway
    -> material changes appended to YAML history
    -> jurisdiction-level completeness cross-checks
    -> verify every genuinely new candidate
    -> independent adversarial live-web audit
    -> HARD FACTUAL GATE
    -> build branded Radar PDF
    -> compare against previous approved Radar layout
    -> HARD LAYOUT GATE
    -> commit audited grants.yaml + audit + PDF
    -> promote PDF as next layout reference
    -> email PDF only
```

Nothing previously tracked may silently disappear. A tracked pathway must be unchanged, updated, status-changed, renamed/superseded, reopened or archived. Archived pathways stay in the YAML history even when hidden from the current PDF.

## OpenAI API contract

The pipeline uses the Responses API in two stages because hosted web search and JSON mode cannot be combined in the same request:

1. mandatory `web_search`, restricted to configured official/administering domains, with `tool_choice: required` and source capture;
2. a separate no-tool JSON structuring request using only the evidence returned by stage 1.

Discovery/extraction uses `gpt-5.6-terra`; the independent adversarial audit uses `gpt-5.6-sol`.

## Publication gates

Publication fails if any of these is not satisfied:

- any required official-domain source search fails;
- any jurisdiction cross-check fails;
- any previously tracked current pathway lacks an explicit disposition;
- any material change lacks a valid YAML history event;
- any tracked current pathway fails fresh verification;
- any published or newly archived transition fails the independent audit;
- any discovery candidate remains unresolved;
- any duplicate current programme ID/URL exists;
- the PDF deviates materially from the approved layout reference or contains off-page text.

The completeness statement is intentionally bounded: the system audits 100% of the configured mandatory official-source/jurisdiction search universe and 100% of the records it publishes. It cannot mathematically prove that an unannounced or unindexed programme outside that universe does not exist.

## Mutable state

Do **not** replace `config/grants.yaml` when installing this package. It is the canonical registry.

The workflow uses `assets/reference/radar-layout-reference.pdf` when present. On the first run, if it is absent, it uses `assets/reference/radar-layout-reference-fallback.pdf` (the last approved GG Advisory design supplied with the package). After a successful factual and layout audit, the new PDF is promoted to `radar-layout-reference.pdf` for the next run.

## Required GitHub secrets

- `OPENAI_API_KEY`
- `MAIL_USERNAME`
- `MAIL_PASSWORD`

The workflow is manual-only (`workflow_dispatch`) while the production pipeline is being validated.
