# Phase 7 match-rate report

Generated 2026-10-22T03:30:23+00:00 from the warehouse marts (measured counts).

## Packages to repositories

In scope (critical or with a non-malware OSV advisory): **4,736** packages; decided (auto_exact or accepted): **4,383** (**92.5%**), of which auto_exact 276. Decided with a GitHub id: 4,382; that id seen in the loaded GH Archive data: 1,750.

| Scope | Ecosystem | Packages | Candidate | auto_exact | accepted | suggested only | rejected only | none | match rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| critical | Go | 3 | 2 | 0 | 2 | 0 | 0 | 1 | 66.7% |
| critical | Maven | 6 | 5 | 0 | 5 | 0 | 0 | 1 | 83.3% |
| critical | NuGet | 4 | 4 | 0 | 4 | 0 | 0 | 0 | 100.0% |
| critical | Packagist | 3 | 3 | 0 | 3 | 0 | 0 | 0 | 100.0% |
| critical | PyPI | 522 | 497 | 53 | 436 | 8 | 0 | 25 | 93.7% |
| critical | RubyGems | 2 | 2 | 0 | 2 | 0 | 0 | 0 | 100.0% |
| critical | crates.io | 1 | 1 | 0 | 1 | 0 | 0 | 0 | 100.0% |
| critical | npm | 2,295 | 2,278 | 0 | 2,273 | 5 | 0 | 17 | 99.0% |
| osv | Go | 17 | 17 | 0 | 17 | 0 | 0 | 0 | 100.0% |
| osv | Hex | 1 | 1 | 0 | 1 | 0 | 0 | 0 | 100.0% |
| osv | Maven | 49 | 45 | 0 | 44 | 1 | 0 | 4 | 89.8% |
| osv | NuGet | 141 | 134 | 0 | 134 | 0 | 0 | 7 | 95.0% |
| osv | Packagist | 4 | 4 | 0 | 4 | 0 | 0 | 0 | 100.0% |
| osv | Pub | 1 | 1 | 0 | 1 | 0 | 0 | 0 | 100.0% |
| osv | PyPI | 1,613 | 1,417 | 223 | 1,112 | 82 | 0 | 196 | 82.8% |
| osv | RubyGems | 7 | 4 | 0 | 4 | 0 | 0 | 3 | 57.1% |
| osv | SwiftURL | 4 | 4 | 0 | 4 | 0 | 0 | 0 | 100.0% |
| osv | crates.io | 24 | 24 | 0 | 24 | 0 | 0 | 0 | 100.0% |
| osv | npm | 39 | 37 | 0 | 36 | 1 | 0 | 2 | 92.3% |
| osv_malware_only | PyPI | 11,799 | 0 | 0 | 0 | 0 | 0 | 11,799 | 0.0% |

Candidates by rule:

| Status | Rule | Candidates |
| --- | --- | ---: |
| accepted | most_sources | 12 |
| accepted | single_source | 4,095 |
| auto_exact | sources_agree | 276 |
| rejected | outvoted | 27 |
| suggested | contested | 45 |
| suggested | fix_reference_only | 7 |
| suggested | single_source_unverified | 36 |
| suggested | tie | 67 |

npm cross-check (npm registry `repository` vs ecosyste.ms):

| Outcome | Packages |
| --- | ---: |
| differ | 1 |
| ecosystems_names_no_repo | 1 |
| neither_names_a_repo | 19 |
| same_id_renamed | 2 |
| same_name | 2,311 |

Repository ids of claimed names:

| Rule | Names | GH Archive disagrees |
| --- | ---: | ---: |
| ecosystems_current | 3,162 | 2 |
| ecosystems_previous_name | 245 | 0 |
| gharchive_latest_holder | 1 | 0 |
| gharchive_unique_name | 29 | 0 |
| not_seen | 56 | 0 |

## Renames

| Status | Rule | Renames | Repos | Also in ecosyste.ms |
| --- | --- | ---: | ---: | ---: |
| accepted | ecosystems_previous_name | 1,207 | 983 | 1,207 |
| auto_exact | same_id_new_name | 3,088 | 3,022 | 73 |

## GH Archive events without a repository id

194,416 events in 101,786 name-hours; no candidate id in the loaded data: 151,725 events (85,350 name-hours).

| Status | Rule | Name-hours | Events |
| --- | --- | ---: | ---: |
| accepted | unique_name_near | 5 | 10 |
| auto_exact | spell_covers | 3,356 | 6,684 |
| suggested | several_ids | 99 | 211 |
| suggested | unique_name_far | 1,427 | 2,640 |
| suggested | unique_name_id_existed | 11,549 | 33,146 |

## Review and the language-model rung

Review CSVs: 155 package candidates, 4,054 names (suggested rows). Ties waiting for the language-model rung: 32 packages.
