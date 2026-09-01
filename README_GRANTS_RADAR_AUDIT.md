# GG Advisory Grants & Accelerators Radar - zero-API production pipeline

Pipeline: `6.0-zero-api-official-source`

## Production principle

`config/grants.yaml` is the canonical factual registry and history ledger. The normal Radar run uses no OpenAI API and no other paid model API.

Publication requires:
- direct re-fetch of every published programme's configured official/administering source evidence;
- field-level evidence-contract support for name, administrator, status, amount, deadline information and target stage;
- an independent second direct-source fetch/audit;
- all 31 configured source groups reachable;
- every high-signal discovered URL reconciled by an explicit decision;
- zero unresolved candidates;
- no duplicate IDs/URLs;
- the branded PDF layout continuity gate to pass.

The workflow is fail-closed. If a page changes materially or a new high-signal programme appears, the PDF is not published until the registry/evidence contract is reviewed.

## Completeness statement

Completeness is defined against the configured monitored official/source universe and curated Radar scope. It is not a mathematical claim that no unannounced, private or unindexed programme exists.

## Bootstrap

The installer contains a 2026-09-01 bootstrap registry built from the successful v5.1 primary official-source verification (31/31 source searches and zero tracked-programme extraction failures), with corrections from the independent audit results that completed before the prior API balance was exhausted.

The installer replaces `config/grants.yaml` only when its SHA-256 is the known pre-v6 canonical baseline:
`0b6172c3295bdc0dadc64356aca68770de8daa24f63cf47a9258265fae06b6c0`.
Otherwise it stops rather than overwrite an unknown registry state.
