# Data Contract Platform — Evaluation

Generated: 2026-11-14T15:54:59+00:00  
Config: 600 rows/table, seed 11, 9 tables across 5 upstream systems  
Runtime: 13.94s

> The two headline metrics are measured against ground truth declared by the change
> simulator, independently of the diff engine and validator. Section 5 reports negative
> controls — deliberately weaker detectors — so the benchmark can be seen to separate
> good detectors from weak ones. Section 6 states the blind spots the platform does not
> claim to cover. Read section 6 before quoting section 1.

## 1. Breaking-change identification

| Metric | Contract only | With submission sample |
|---|---|---|
| Accuracy | 1.0000 | 1.0000 |
| Precision (breaking) | 1.0000 | 1.0000 |
| Recall (breaking) | 1.0000 | 1.0000 |
| F1 (breaking) | 1.0000 | 1.0000 |
| False positive rate | 0.0000 | 0.0000 |
| False negative rate | 0.0000 | 0.0000 |
| Scored samples | 254 | 254 |

Confusion matrix (with sample): TP=159 FP=0 TN=95 FN=0

### Per-mutation accuracy

| Mutation | Samples | Correct | Accuracy |
|---|---|---|---|
| add_nullable_column | 9 | 9 | 1.0000 |
| add_required_column | 9 | 9 | 1.0000 |
| compound_compatible | 9 | 9 | 1.0000 |
| compound_mixed | 7 | 7 | 1.0000 |
| description_change | 9 | 9 | 1.0000 |
| drop_add_decoy | 6 | 6 | 1.0000 |
| drop_and_add_unrelated | 7 | 7 | 1.0000 |
| drop_column | 9 | 9 | 1.0000 |
| enum_add_value | 7 | 7 | 1.0000 |
| enum_remove_value | 7 | 7 | 1.0000 |
| enum_reorder | 7 | 7 | 1.0000 |
| freshness_equivalent | 2 | 2 | 1.0000 |
| freshness_relax | 4 | 4 | 1.0000 |
| freshness_tighten | 4 | 4 | 1.0000 |
| narrow_and_widen | 4 | 4 | 1.0000 |
| noop | 9 | 9 | 1.0000 |
| precision_narrow | 6 | 6 | 1.0000 |
| primary_key_change | 9 | 9 | 1.0000 |
| range_tighten | 7 | 7 | 1.0000 |
| range_widen | 7 | 7 | 1.0000 |
| rename_column | 9 | 9 | 1.0000 |
| rename_hard | 9 | 9 | 1.0000 |
| rename_plus_drop | 9 | 9 | 1.0000 |
| rename_swap | 7 | 7 | 1.0000 |
| rename_with_tightening | 9 | 9 | 1.0000 |
| reorder_columns | 9 | 9 | 1.0000 |
| required_flag_swap | 4 | 4 | 1.0000 |
| required_relax | 9 | 9 | 1.0000 |
| required_tighten | 4 | 4 | 1.0000 |
| rule_added | 7 | 7 | 1.0000 |
| timestamp_to_date | 2 | 2 | 1.0000 |
| type_narrow | 6 | 6 | 1.0000 |
| type_widen | 4 | 4 | 1.0000 |
| unique_key_added | 9 | 9 | 1.0000 |
| unique_key_removed | 2 | 2 | 1.0000 |
| unique_relax | 8 | 8 | 1.0000 |
| unique_tighten | 9 | 9 | 1.0000 |
| rename_swap (excluded) | 2 | 0 | n/a |
| rename_null_column (excluded) | 1 | 0 | n/a |

### Rename detection

| Setting | Rename cases | Detected | Recall | Spurious | Precision |
|---|---|---|---|---|---|
| Contract only | 36 | 27 | 0.7500 | 6 | 0.8182 |
| With submission sample | 36 | 36 | 1.0000 | 0 | 1.0000 |

1 rename case(s) are excluded from the recall denominator as structurally undecidable: renames of all-NULL columns carry no data evidence; they can only be declared with an explicit renamed_from hint

### Excluded as undecidable (3)

These cases are removed from accuracy and rename recall entirely — scoring them either way would be dishonest, because no contract-aware observer has the information to decide.

- order_core.order_items / rename_swap: order_id and sku_id have identical declarations; the contract cannot distinguish a value swap from an untouched table
- payment_core.payments / rename_null_column: paid_at is entirely NULL, so name-similarity and value-overlap are both silent; only an explicit renamed_from hint can identify this rename
- merchant.merchant_daily_stats / rename_swap: txn_count and refund_count have identical declarations; the contract cannot distinguish a value swap from an untouched table

## 2. Violation detection

- Injections: 121 across 9 tables
- Detected: 121  →  **detection rate 1.0000**
- Clean-data rows validated: 5400, tables with false positives: 0
- Legal-but-unusual probes: 27, false positives: 0 (rate 0.0000)
- Dirty-but-contract-compliant probes: 9, flagged: 0 (rate 0.0000)

| Injection | Cases | Detected | Detection rate |
|---|---|---|---|
| boundary_plus_epsilon | 7 | 7 | 1.0000 |
| cross_field_inconsistency | 2 | 2 | 1.0000 |
| duplicate_primary_key | 9 | 9 | 1.0000 |
| duplicate_unique | 8 | 8 | 1.0000 |
| enum_case_mismatch | 7 | 7 | 1.0000 |
| invalid_enum | 7 | 7 | 1.0000 |
| invalid_timestamp | 8 | 8 | 1.0000 |
| negative_value | 7 | 7 | 1.0000 |
| null_in_required | 9 | 9 | 1.0000 |
| null_in_unique | 8 | 8 | 1.0000 |
| numeric_as_string | 7 | 7 | 1.0000 |
| scale_overflow | 6 | 6 | 1.0000 |
| schema_drift | 18 | 18 | 1.0000 |
| stale_data | 4 | 4 | 1.0000 |
| type_mismatch | 7 | 7 | 1.0000 |
| value_out_of_range | 7 | 7 | 1.0000 |

## 3. Stability across seeds and table sizes

| Metric | Min | Mean | Max |
|---|---|---|---|
| breaking_accuracy | 1.0000 | 1.0000 | 1.0000 |
| breaking_precision | 1.0000 | 1.0000 | 1.0000 |
| breaking_recall | 1.0000 | 1.0000 | 1.0000 |
| rename_recall | 1.0000 | 1.0000 | 1.0000 |
| rename_recall_contract_only | 0.7222 | 0.7389 | 0.7500 |
| violation_detection_rate | 1.0000 | 1.0000 | 1.0000 |
| clean_false_positive_rate | 0.0000 | 0.0000 | 0.0000 |

Seeds: [11, 29, 47, 83, 131]; rows/table: [200, 800]; runs: 10

The row that varies is `rename_recall_contract_only`: hard renames can only be recovered from value overlap, so the submission sample is what closes the gap.

## 4. Validation throughput

45000 rows across 9 tables in 0.456s → **98682 rows/s** single-process.


## 5. Negative controls

| Control | Metric | Score |
|---|---|---|
| name-similarity-only rename matcher (no sample data) | rename recall | 0.7297 |
| ↳ the real detector on the same cases (with sample) | rename recall | 1.0000 |
| shape-only validator (NULLs and Python types; no enum/range/unique/freshness) | detection rate | 0.4369 |

Controls are intentionally weaker detectors. Their lower scores show the headline metrics measure the real detectors, not the harness.

## 6. Known blind spots

**rename of an entirely NULL column**  
*Why:* no name resemblance and no values to compare; the two versions are observationally identical  
*Mitigation:* submit an explicit `renamed_from` hint; the diff engine honours it at confidence 1.0

**value swap between two identically-declared columns**  
*Why:* the declaration is unchanged by the swap; nothing declares which column means what  
*Mitigation:* declare a cross-field expression rule (e.g. `created_at <= updated_at`); the sampler catches it as a data violation

**semantic meaning change under an unchanged shape**  
*Why:* `status='paid'` silently meaning something new is not a schema change at all  
*Mitigation:* attach a business rule or data predicate to the contract; contract review at submission is the only real safeguard

**declared type change when the sample predates the change**  
*Why:* value-level evidence is absent, so only declaration diffing applies  
*Mitigation:* declaration diffing always runs and does catch it; the sample is used for renames, not type decisions

