# GG Advisory Grants & Accelerators Radar - canonical audited pipeline

## Fixed architecture

Two artefacts have deliberately different roles:

- `config/grants.yaml` is the **canonical factual registry and change ledger**. Current programme facts remain as flat fields. Material changes are appended to each record's `history` array with the verification date, before/after values and official source URLs.
- the latest approved Radar PDF is the **layout reference only**. It is never used as a factual data source. The newly generated PDF is checked against it for geometry, cover treatment, section structure, logo presence, anchor position and off-page text.

A successful v5.1 run follows this sequence:

```text
config/grants.yaml
    -> search EVERY mandatory configured source group once on official/administering domains
       (the same source search re-verifies all baseline programmes on that source AND discovers new ones)
    -> targeted fallback search only when a baseline programme was omitted/unsupported by its source batch
    -> explicit disposition for every previously tracked pathway
    -> material changes appended to YAML history
    -> reconcile every new discovery from already-retrieved source evidence
    -> deterministic jurisdiction coverage aggregation over mandatory sources
    -> SECOND independent adversarial live-web audit in small jurisdiction batches
    -> HARD FACTUAL GATE
    -> build branded Radar PDF
    -> compare against previous approved Radar layout
    -> HARD LAYOUT GATE
    -> commit audited grants.yaml + audit + PDF
    -> promote PDF as next layout reference
    -> email PDF only
```

Nothing previously tracked may silently disappear. A tracked pathway must be unchanged, updated, status-changed, renamed/superseded, reopened or archived. Archived pathways remain in the YAML history even when hidden from the current PDF.

## Why v5.1 batches live research

The first v5 production run demonstrated that one search per source **plus** one search per tracked record **plus** one search per discovered candidate **plus** one search per audit record creates an uncontrolled API-call fan-out. v5.1 removes that architecture.

There is now one primary live research call per mandatory source group. New candidates do not each trigger another web search. The independent audit remains a genuinely separate live-web pass, but checks several records per jurisdiction batch. A targeted single-record search is reserved for a baseline record that the source batch could not support.

Model self-reported confidence is retained as a diagnostic signal. It is not treated as a calibrated probability and no longer creates false failures merely because a model reports 0.86 rather than 0.90. Hard failure is based on missing official provenance, missing required field evidence, material unresolved contradiction, very low confidence, or an independent validator rejecting a material fact.

## OpenAI API contract

The pipeline uses the Responses API in two stages because hosted web search and JSON mode cannot be combined in the same request:

1. mandatory `web_search`, restricted to configured official/administering domains, with `tool_choice: required` and source capture;
2. a separate no-tool JSON structuring request using only the evidence returned by stage 1.

Discovery uses `gpt-5.6-terra`; the independent adversarial audit uses `gpt-5.6-sol`.

If the API returns `credit_balance_exhausted` / `insufficient_quota`, the pipeline fails immediately with `OPENAI_API_QUOTA_OR_SPEND_LIMIT` instead of retrying a non-recoverable billing error. The GitHub secret therefore needs an API project/organisation with available API credits; a ChatGPT subscription is separate from API billing.

## Publication gates

Publication fails if any of these is not satisfied:

- any required official-domain source search fails or returns no official provenance;
- any jurisdiction lacks full coverage from its configured mandatory source set;
- any previously tracked current pathway lacks an explicit disposition;
- any material change lacks a valid YAML history event;
- any tracked current pathway has unresolved hard verification/evidence issues;
- any published or newly archived transition fails the second independent live-web audit;
- any discovery candidate remains unresolved;
- any duplicate current programme ID/URL exists;
- the PDF breaks the approved structural/layout contract or contains off-page text.

The completeness statement is intentionally bounded: the system audits the configured mandatory source/search universe and every record it publishes. It cannot mathematically prove that an unannounced or unindexed programme outside that universe does not exist.

## Mutable state

Do **not** replace `config/grants.yaml` when installing this package. It is the canonical registry.

The workflow uses `assets/reference/radar-layout-reference.pdf` when present. On the first run, if it is absent, it uses `assets/reference/radar-layout-reference-fallback.pdf`. After a successful factual and layout audit, the new PDF is promoted to `radar-layout-reference.pdf` for the next run.

## Required GitHub secrets

- `OPENAI_API_KEY`
- `MAIL_USERNAME`
- `MAIL_PASSWORD`

The workflow remains manual-only (`workflow_dispatch`) while the production pipeline is being validated.
