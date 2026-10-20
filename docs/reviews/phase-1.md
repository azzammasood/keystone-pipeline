# Phase 1 review

- Date: 2026-10-20. Reviewer: an independent subagent in a fresh context, read-only, no network.
- The reviewer checked 22 numbers against the raw measurements (`data/spike/`), including every
  cell of the completeness table, the medians, the 822-hour sweep, the yearly means and the mirror
  counts. All 22 matched. Of 14 era-map field claims, 12 matched. Privacy passed (the fixture holds
  only counts and hashes; scripts store only key paths). ruff, format, mypy and pytest passed
  (8 passed, 1 network test skipped).

## Findings and fixes
1. Major: the era map wrongly marked Push `payload.before` and `repository_id` as new in E3.
   Fixed: `before` is E2 and E3; `repository_id` is late E2 (by 2024-06) and E3.
2. Major: the E3 claims rested on one hour. IssueCommentEvent is present in the other 16 E3 hours.
   Fixed: `era_fields.py` rerun on all 33 hours; IssueComment E3 is now "yes, volume reduced";
   first-response time is kept in E3, gated by completeness; the "47k" sentence is replaced.
3. Major: the boundary numbers had no evidence file. Fixed: `scripts/spike/boundary_heads.py`
   writes `data/spike/boundary_heads.csv` (93 probes, including the 2011-02-11 404s and the mirror
   probes); every documented boundary value was re-checked against it.
4. Major: ADR-009 did not follow the rule, because the whole archive also fits in 7 days or less
   at $0. Fixed: the backfill window is the whole archive (estimate 4.86 TB, about 4.0 days); the
   score windows stay in ADR-012; "minimum 12 months" is added.
5. Minor: the size estimate could not be reproduced, and some labels were imprecise. Fixed:
   `scripts/spike/size_estimate.py` (method in its docstring) gives 4.86 TB; the DuckDB median is
   labelled for hours with more than 100k events (96 MB/s over all hours); the C2 drop is now
   35 to 43%; the serial parse time is 2.8 days for the whole window; the boundary is "bracketed
   12h to 18h".
6. Major: deliverables were missing or thin. Fixed: section 8b of the spike report adds free
   stores (GitHub Releases, HF datasets, R2), the OpenLineage support matrix (Flink confirmed,
   others marked "to verify"), the Actions limits (job time, minutes, concurrency, storage), and
   this machine against the Compose profiles, with DataHub's documented needs. Flink and Redpanda
   memory stay unmeasured until Phase 4, as recorded.

## Outstanding for the gate
- Not reviewable here: the GitHub CI result (the owner must check). The Databricks and BigQuery
  rungs are skipped for lack of credentials (recorded).
