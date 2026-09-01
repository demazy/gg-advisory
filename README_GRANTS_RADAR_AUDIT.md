# GG Advisory Grants Radar - snapshot-sentinel pipeline

Pipeline: `7.0-snapshot-sentinel`

This package produces only the GG Advisory Grants & Accelerators Radar PDF. It does not run a monthly news digest and does not create DOCX or HTML outputs.

## Editorial model

- `config/grants.yaml` remains the canonical programme registry and change-history ledger.
- Current official/administering-body pages are checked directly with deterministic identity, status, amount and deadline sentinels.
- If an official site is temporarily inaccessible or only partly machine-readable, a matching evidence snapshot may bridge the check for at most 45 days. The snapshot fingerprint must exactly match the current canonical factual fields.
- Explicit live contradictions always block publication.
- Discovery compares monitored official index links with a dated baseline inventory. Historical links already present in that inventory are not reclassified as new every run; genuinely new high-signal links must be explicitly reconciled before publication.
- The audit stage performs no second HTTP crawl. It independently validates the captured evidence, source/jurisdiction coverage, registry continuity, history, candidate reconciliation and uniqueness.
- There is no OpenAI API or other paid model dependency.

## Publication gate

A PDF is generated only when every visible record passes verification, every mandatory source group and jurisdiction remains covered, no genuinely new discovery candidate is unresolved, registry continuity/history/uniqueness pass, and the resulting PDF passes the layout-continuity audit.

The workflow is manual-only (`workflow_dispatch`). On success it emails only the PDF.

## Regression replay

The deterministic test suite includes the captured 2026-09-01 v6 failure set: 27 visible programme failures, 275 links that v6 incorrectly treated as unresolved, and 9 technically blocked required source groups. The replay must resolve those catalogue links through the dated inventory, bridge the technical source blocks only through fresh matching snapshots, preserve NT jurisdiction coverage through its monitored source group, and reach a publishable audit without a second network crawl.
