# 数据契约平台 · Data Contract Platform

A data contract platform for a payment company's data middle platform, built for
the specific failure mode in the brief: **dozens of upstream business databases
change schemas and values without notice, and downstream reports and risk
features silently compute wrong numbers until someone notices in production.**

The platform puts a machine-checkable contract in front of every upstream table,
gates every change against it, routes breaking changes through downstream
sign-off, and samples real data daily so drift is caught before it reaches a
report.

```
                 submit (contract + sample)          daily sampler
upstream  ──────────────────────────────────►  ┌──────────────────────┐
service        │  sample gate   diff engine     │  contract validation │
               │  (blocks bad   (rename detect, │  (types/enums/ranges │
               │   examples)     breaking vs     │   /unique/freshness/ │
               ▼                 compatible)     │   cross-field)       │
        ┌──────────────┐              │         └──────────┬───────────┘
        │  compatible  │              │ breaking           │ violations
        │  → activate  │              ▼                    ▼
        └──────────────┘      ┌───────────────┐     ┌───────────────┐
                              │   sign-off    │     │  dedupe +     │
                              │ all consumers │     │  severity     │
                              │   must approve│     │  routing      │
                              └───────┬───────┘     └───────┬───────┘
                                      │ approved            │
                                      ▼                     ▼
                              parallel rollout        owner / consumer
                              (old + new both         channels, 24h
                               validate for N days)   quiet window
```

## Quick start

```bash
pip install fastapi uvicorn pydantic pyyaml jinja2 httpx

# 1. stand up 5 mock upstream systems (9 tables, real SQLite DBs) and register contracts
python -m dcp.cli seed --rows 400 --validate

# 2. walk the whole workflow with a narrated demo
python scripts/demo.py

# 3. open the admin console
python -m dcp.cli serve --port 8765     # http://127.0.0.1:8765

# 4. measure accuracy end-to-end
python -m dcp.cli evaluate --rows 600 --output-dir reports
#    → reports/evaluation.md
```

Other useful commands:

```bash
python -m dcp.cli simulate --count 10 --auto-sign --validate   # random upstream churn
python -m dcp.cli validate --contract payment_core.payments    # daily sampler for one table
python -m dcp.cli status                                       # platform state summary
python -m pytest tests/ -q                                     # 135 tests
```

## The contract DSL

A contract is YAML or JSON. Everything in it is enforced; anything the platform
cannot enforce is rejected at parse time rather than silently ignored.

```yaml
contract_id: order_core.orders
upstream_system: order_core
table: orders
description: Order header written by the order service.

columns:
  - name: order_id
    type: string          # string|int|bigint|decimal|bool|date|timestamp|json|bytes
    unique: true
  - name: total_amount
    type: decimal
    minimum: 0
    maximum: 100000
    precision: 12         # 12 significant digits...
    scale: 2              # ...with 2 after the point; more is a violation
  - name: status
    type: string
    enum: [created, paid, shipped, completed, cancelled]
  - name: paid_at
    type: timestamp
    required: false
  - name: updated_at
    type: timestamp

primary_key: [order_id]
unique_keys:
  - [order_id, status]

freshness:                # the data must be no older than this
  max_age: PT24H          # ISO-8601 duration
  column: updated_at
  severity: high

rules:                    # table-level checks the sampler enforces
  - name: paid_after_created
    kind: expression
    expression: paid_at is null or paid_at >= created_at
    severity: high
  - name: order_id_not_null
    kind: not_null
    column: order_id
    severity: critical

consumers:                # every one of these must approve a breaking change
  - reporting.dw_orders
  - risk.feature_order_velocity
  - finance.recon_daily
owners:
  - order_core.team
```

Rule kinds: `not_null`, `enum`, `range`, `unique`, `expression`, `row_count`,
`null_ratio`. Expressions are parsed with Python's `ast` and evaluated in a
whitelist — no `eval`, no function calls, no attribute access.

## What each requirement maps to

| # | Requirement | Where |
|---|---|---|
| 1 | types, nullability, enums, ranges, unique keys, freshness | `dcp/contract.py` |
| 2 | automatic compatible vs breaking classification | `dcp/diff.py` |
| 3 | sign-off by all registered consumers, parallel old+new versions | `dcp/platform.py` (`submit`, `decide`, `finalize_transitions`) |
| 4 | daily sampling of real data, violation drilldown to the row | `dcp/validator.py`, `dcp/platform.py` (`_validate_version`) |
| 5 | rename detection instead of drop+add | `dcp/diff.py` (`detect_renames`) |
| 6 | submission sample checked against the new contract | `dcp/validator.py` (`validate_submission_sample`), `Platform.submit` |
| 7 | alerts by owner/severity with deduplication | `dcp/alerts.py` |
| 8 | violation trends per upstream system | `dcp/platform.py` (`dashboard`), console 总览 |

### 2 — compatible vs breaking

The classifier tracks direction, not just difference:

| Change | Classified |
|---|---|
| add nullable column; widen `int→bigint`, `date→timestamp`; add enum value; widen range; drop NOT NULL / UNIQUE; relax freshness; reorder columns | compatible |
| add NOT NULL column; drop column; narrow type; remove enum value; tighten range; add NOT NULL / UNIQUE; reduce decimal scale; change primary key; add rule; tighten freshness | breaking |

Adding a *rule* counts as breaking on purpose: the upstream must now be able to
satisfy it.

### 5 — rename detection

A dropped + added pair is scored on three signals:

```
score = 0.45·name_similarity + 0.25·constraint_similarity + 0.30·value_overlap
```

with two guards learned from the benchmark:

* **Value veto.** When both submissions ship a sample and the sampled values
  barely overlap, the pair is *not* a rename no matter how similar the names
  look. This is what stops a `user_id → user_id_hash` decoy from being waved
  through as "just a rename".
* **Empty-column asymmetry.** A populated column does not become an all-NULL
  column via renaming; that pairing scores zero.
* **Explicit hints.** `renamed_from: old_name` on the new column is accepted at
  confidence 1.0, which is the escape hatch for the one genuinely undecidable
  case (renaming an all-NULL column).

### 3 — sign-off semantics

* Consumers are read from the contract's `consumers` list at submission time.
* The version stays `pending_signoff` until **every** registered consumer
  approves. Any single rejection freezes it as `rejected` and notifies the
  upstream owner with the rejecting consumer's comment.
* On full approval the new version becomes `active` and the previous one becomes
  `parallel` for a configurable window (default 7 days, `DCP_TRANSITION_DAYS`).
  Both versions are validated on every sampler run, so a downstream that has not
  migrated shows up as violations on the *old* version rather than as silence.
* On expiry the old version is `superseded`.
* A second submission while one is awaiting sign-off is refused — otherwise the
  new diff would be taken against the still-active baseline and conflate two
  unrelated change sets.

### 7 — alert deduplication

* An alert's identity is the **problem** (`contract + version + rule + column`),
  never the row. 500 bad rows in one column is one alert with
  `occurrence_count=500`.
* Severity picks the audience: `critical` → owners + all consumers;
  `high` → owners; `medium`/`low` → daily digest.
* Re-notification is rate-limited by a quiet window (default 24h). Repeats
  inside the window are stored with `suppressed=1` so the history is auditable
  without paging anyone again.
* When a problem stops appearing, the alert resolves **once**. Workflow notices
  (sign-off requests, approvals, rejections) are exempt from resolution by a
  clean data run.

## The change simulator and the measurements

`dcpsim/simulator.py` implements 39 structural mutations and 15 dirty-data
injections, each carrying an **independently declared** ground-truth label. The
harness never asks the diff engine what it thinks; it compares against the label
written by whoever made the change.

Mutation families include honestly hard cases:

* `rename_hard` — `created_at → gmt_create`, no name resemblance at all. Only
  value overlap can catch it.
* `drop_add_decoy` — drop `user_id`, add a semantically different
  `user_id_hash`. Name-based matchers see a rename and are *wrong*.
* `rename_swap` — swap two same-typed columns' names and values.
* `rename_null_column` — rename an all-NULL column (undecidable; excluded from
  scoring and reported as such).
* `compound_mixed` — a compatible-looking change plus one breaking change in the
  same release.
* `rename_plus_drop` — a rename and a destructive drop together.
* `noop`, `reorder_columns`, `enum_reorder`, `freshness_equivalent`
  (`PT24H` vs `P1D`) — changes that must **not** be reported.

### Latest results (`reports/evaluation.md`, seed 11, 600 rows/table)

| Metric | Contract only | With submission sample |
|---|---|---|
| Breaking-change accuracy | 1.0000 | 1.0000 |
| Precision / recall (breaking) | 1.0000 / 1.0000 | 1.0000 / 1.0000 |
| Rename recall | 0.7500 | **1.0000** |
| Rename precision | 0.8182 | **1.0000** |
| Rename cases (scored / excluded as undecidable) | 36 / 4 | 36 / 4 |
| Violation detection rate | — | **1.0000** (121/121) |

| False positives on clean data | — | 0 |
| False positives on dirty-but-compliant data | — | 0 |

Stable across seeds `[11, 29, 47, 83, 131]` and 200/800 rows per table.
Validation throughput ≈ **100k rows/s** single-process.

**Why the perfect numbers are worth trusting — and their limits.**

* The ground truth is declared by the mutation author, not derived from the
  detector, so the two cannot agree by construction.
* Negative controls are included in the report: a declaration-only rename
  matcher scores 0.73 recall on the same cases (the real detector: 1.00), and a
  shape-only validator scores 0.44 detection (the real one: 1.00). If the
  benchmark were trivially easy, the controls would score 1.00 too.
* The contract-only column is the honest floor: hard renames genuinely cannot be
  detected without sample data, which is precisely why requirement 6 (ship a
  sample) exists.
* `reports/evaluation.md` §6 lists the blind spots the platform does **not**
  claim to cover — renames of all-NULL columns, value swaps between identically
  declared columns, and semantic changes under an unchanged shape.
* A synthetic simulator cannot reach 100% of real-world weirdness. The honest
  claim is: *on this mutation catalogue, at these sizes, the measured rates are
  as reported, the controls behave as expected, and the failure modes are named.*

## Admin console

`http://127.0.0.1:8765` — seven views, no build step, no external assets:

* **总览** — contract counts, open alerts, suppressed duplicates, per-upstream
  violation trend and ranking.
* **契约目录** — every table with status, active version, consumers, owners;
  per-contract detail modal with column definitions, rules, version history and
  the recorded diff.
* **变更会签** — every pending breaking change with its change list, all
  consumers, and approve/reject buttons.
* **校验运行** — run history with row counts, violation counts, role
  (`active`/`previous` during a parallel window) and a jump into filtered
  violations.
* **违约看板** — filterable violation list; clicking a row primary key opens the
  full row snapshot for that exact offending record.
* **告警** — alert feed including suppressed duplicates and resolution notices.
* **提交契约** — paste YAML or JSON plus an optional sample; the server applies
  the same gate as the CLI and shows the classification result.

## Authentication

Unset by default for local use. Set `DCP_ADMIN_TOKEN` to require
`X-Admin-Token` on write endpoints. `X-Consumer-Token` is derived as
`HMAC-SHA256(DCP_CONSUMER_SECRET, consumer_name)` so one consumer cannot sign on
another's behalf.

## Layout

```
dcp/
  contract.py    contract DSL: parsing, strict validation, canonical hashing
  diff.py        change classification + rename detection
  validator.py   row sampling, constraint checks, submission gate
  expr.py        whitelisted expression evaluator
  store.py       SQLite persistence
  alerts.py      severity routing, deduplication, resolution
  platform.py    workflow state machine (submit / sign-off / rollout / validate)
  api.py         FastAPI admin backend
  static/        console (vanilla JS)
  cli.py         seed / simulate / validate / evaluate / serve / status
dcpsim/
  upstreams.py   5 mock upstream systems, valid data generation, materialization
  simulator.py   structural mutations + dirty-data injections with ground truth
dcpeval/
  harness.py     measurement harness, negative controls, stability sweep
scripts/demo.py  narrated end-to-end walkthrough
tests/           135 tests
reports/         generated evaluation.md / evaluation.json
```

## Notes on design choices

* **Contracts describe logical guarantees, not physical DDL.** `schema_hash`
  ignores `sql_type` and enum ordering, so a `VARCHAR(64)` spelling change or a
  reshuffled enum list is not a version bump. Physical types are still persisted
  for materialization.
* **Sampling is deterministic.** Rows are ordered by `md5(row)` before the first
  N are taken, so re-running a validation reproduces the same sample and the
  same violation counts.
* **Stored violations are capped per rule** (default 50) so one badly broken
  column cannot bloat the database; `by_rule` still reports the true count.
* **Attribute-level drilldown is preserved.** Every stored violation carries the
  offending row's primary key and a snapshot of the row, which is what the
  console's 违约看板 uses.
