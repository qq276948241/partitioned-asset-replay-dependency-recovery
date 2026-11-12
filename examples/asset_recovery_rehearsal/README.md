# Asset Recovery Rehearsal

A locally runnable drill of **dependent backfills over date-partitioned assets**: how
to recover from missing upstream partitions, interrupted runs, cancelled runs, and
late source corrections — and how to prove that the results are correct and that
each one was built from the right inputs.

Everything runs on the local machine with Dagster's embedded SQLite instance. No cloud
account, no container, no network service beyond fetching Python packages once.

This is an executable case, not a plan. The point of the drill is the **artifacts it
leaves behind**: a dependency map, per-request execution records, and a verification
report that compares every result against an independent recomputation.

---

## The mini graph

Two source assets and two derived assets, all on a fixed 12-day window
(2026-10-01 … 2026-10-12):

```
raw_orders (source) ── trailing 7-day window ──▶ orders_window_7d ──┐
                                                                    ├──▶ daily_settlement
raw_fx    (source) ─────────── identity ──────────────────────────────┘
```

- `orders_window_7d/<day>` reads `raw_orders` for the **7-day trailing window** ending
  on that day. At the start of the window Dagster clamps the range, so `2026-10-01`
  reads only one upstream partition. The dependency map lists every clamped day
  explicitly instead of pretending the window is always seven.
- `daily_settlement/<day>` reads `orders_window_7d/<day>` **and** `raw_fx/<day>`, both
  identity-mapped.

The window makes two failure modes unavoidable and the drill meaningful:

1. **A missing upstream partition blocks a fan-out of downstream partitions.**
   If `raw_orders/2026-10-04` never arrives, then every window covering it
   (`2026-10-04` … `2026-10-10`) cannot be honestly computed.
2. **A correction fans out too.** Correcting `raw_orders/2026-10-02` changes every
   window that covers it, and each of those changes a settlement.

---

## What the drill proves

| Concern | How the rehearsal settles it |
| --- | --- |
| Which downstream partitions are genuinely computable | The planner resolves each window against real revisions and marks blocked partitions `not_computable` with the exact missing inputs. Only `computable` items are launched. |
| Recovery recomputes only the affected range | Plans are built on the current lake. A partition whose committed fingerprint still matches today's inputs is `already_current` and is not recomputed; only genuinely changed partitions run. |
| Interrupts at read / write / publish | Each derived computation stages a runner-visible input manifest, then values, then a commit marker. Faults can fire at any of the three stages. |
| Half-finished material is never consumed | Input selection consults only *committed* published results. Staged attempts that never committed are quarantined before recovery; published files without a commit marker are reported as `untrusted_file`. |
| Version and reason are recorded per run | Every run carries `rehearsal/asset`, `rehearsal/partition`, `rehearsal/reason`, `rehearsal/request` tags; every result records the exact revisions it read. |
| Repeat requests do not double-publish | A result whose input fingerprint and value hash already match is not rewritten. A second identical request schedules zero work. |
| Cancelled runs do not advance the partition | The cancel drill cancels a live Dagster run and confirms its status is `CANCELED`, that no result was published, and that recovery can finish the partition cleanly. |
| Results are compared to a full rebuild | Verification recomputes every derived partition straight from source revisions — never reading a published file — and compares both values and claimed input versions. |
| Green UI is not accepted as truth | `stale_input` catches a run that succeeded and produced *matching numbers* from a superseded revision. `value_mismatch` catches wrong numbers. Both fail verification. |

---

## Quick start (clean directory)

```bash
cd examples/asset_recovery_rehearsal
./run.sh bootstrap     # creates ./.venv and ./.rehearsal, installs dependencies
./run.sh drill         # runs the entire rehearsal, writes reports
```

`drill` finishes with the verification verdict (`verification exit=0`) and prints the
report directory. Everything it creates lives under `.rehearsal/` (or
`$REHEARSAL_WORKSPACE` if set), so resetting is `./run.sh reset`.

To inspect in the UI:

```bash
./run.sh ui           # starts Dagster on http://localhost:3001 in the background
./run.sh down         # stops it
```

By default `bootstrap` installs the **editable Dagster from this repository**, so the
drill runs against the local source. To run against a released package instead:

```bash
REHEARSAL_USE_REPO=0 ./run.sh reset
```

No images, clusters, or cloud credentials are involved at any point.

---

## The drill, step by step

`./run.sh drill` runs these in order; each also runs on its own via
`./run.sh <command>` or `python -m rehearsal.cli <command>`.

1. **generate** — creates source revisions for 22 of the 24 source partitions and
   *withholds* `raw_orders/2026-10-04` and `raw_fx/2026-10-07`. The gap is intentional
   and recorded (`01_sample_generation.json`).
2. **map** — writes the dependency map derived from the live asset graph
   (`dependency_map.json`).
3. **plan + run** — evaluates every derived partition. Ten are computable (windows not
   touching the gap, plus the settlements that only need those windows); fourteen are
   blocked and listed with their missing inputs (`02_initial_plan.json`). Runs launch
   one Dagster run per computable partition, in asset order, so a settlement never
   starts before its window is published (`03_initial_run.json`).
4. **fail ×3** — injects an interruption at each stage:
   - `read` on `orders_window_7d/2026-10-05` — leaves a manifest-only staging attempt.
   - `write` on `daily_settlement/2026-10-01` — leaves manifest + values, still no commit.
   - `publish` on `daily_settlement/2026-10-02` — writes a result-shaped file with
     `committed: false`, then dies. This is the dangerous case: the file *exists* but
     must not be trusted.
   Each writes `05_fault_<stage>.json` including the staging state.
5. **restore-gaps** — delivers the two withheld source partitions, as a late upstream
   arrival (`06_gap_restoration.json`).
6. **recover** — quarantines every uncommitted attempt, then plans and runs only the
   newly computable partitions. The three fault drills' partial attempts are moved
   aside, and the publish-interrupted settlement is recomputed (`04_recovery.json`).
7. **cancel** — starts a real run, cancels it from outside, and records the
   `CANCELED` status, that nothing was published, and that a partial attempt was left
   (`08_cancel.json`).
8. **correct** — appends a corrected `raw_orders/2026-10-02` revision (`seq 1 → 2`)
   and records the affected scope: 7 windows + 7 settlements (`07_correction.json`).
9. **verify before repair** — runs verification while the stale results are still in
   place. It **fails** (`08_pre_repair_verification.json`, 14 `value_mismatch`): the
   drill shows the check catching the problem rather than trusting a green UI.
10. **repair-scope** — recomputes **only** that affected scope, in dependency order
    (`07_scoped_repair.json`).
11. **verify** — recomputes all 24 derived partitions from source and compares
    (`09_verification.json`). Result: `passed=True`, 24/24 `match`.

---

## Where the evidence lives

Everything is under `$REHEARSAL_WORKSPACE` (default `.rehearsal/`):

```
reports/
  00_drill_summary.json     every step and its exit code
  01_sample_generation.json which revisions were created, which gaps withheld
  02_initial_plan.json      computable vs blocked, with missing inputs
  03_initial_run.json       per-partition run ids, statuses, accepted results
  04_recovery.json          quarantined attempts + scoped recomputation
  05_fault_read.json        staging state after a read-stage interrupt
  05_fault_write.json       staging state after a write-stage interrupt
  05_fault_publish.json     untrusted published file after a publish interrupt
  06_gap_restoration.json   late-arriving source revisions
  07_correction.json        corrected revision + affected-scope derivation
  07_scoped_repair.json     exactly what ran for the correction
  08_cancel.json            cancelled run status and leftover partial material
  08_pre_repair_verification.json   verification *failing* on the stale results
  09_verification.json      per-partition comparison: values and input versions
  dependency_map.json       edges, mappings, clamped windows, and the three rules
state/plans/                every execution plan by name
journal/runs.jsonl          append-only record of every launched work item
lake/<asset>/<partition>/   immutable source revisions (rev-1.json, rev-2.json, …)
staging/                    in-progress attempts (quarantined and removed on recovery)
published/<asset>/<partition>.json   committed derived results
dagster_home/               the real Dagster instance: run records, event log, UI
```

A run record is not fabricated: `dagster_home/` is the actual instance. You can open
it with `./run.sh ui` and see the same runs the reports reference by id.

---

## How to check it yourself

**Read one plan.** Open `02_initial_plan.json`. Blocked partitions name the missing
input that blocks them; the run report's `outcomes` contains only computable items.
Nothing that reads `raw_orders/2026-10-04` was run before the gap was restored.

**Read one verification row.** In `09_verification.json`, a `match` row has
`value_match: true` and `stale_inputs: []`. A `stale_input` row has
`value_match: true` but a non-empty `stale_inputs` list — that is the
"succeeded but read the wrong revision" case, and it fails verification even though a
naive value check would pass.

**Compare recovery scope to the affected scope.** `07_correction.json`'s
`affected_scope.affected` and `07_scoped_repair.json`'s `outcomes` must cover the same
partitions — the repair does not touch partitions outside the correction's fan-out.

**Confirm idempotency.** Run the drill, then `./run.sh drill` again's `run` step
schedules zero work because every committed fingerprint still matches. The published
files are not rewritten.

---

## Interruption model

Derived computations use a three-step protocol so that every interruption is
observable and recoverable:

1. **read** — the exact input revisions are staged to
   `staging/<asset>/<partition>/<attempt>/input_manifest.json`.
2. **write** — the computed values are staged to `values.json`.
3. **publish** — a commit marker is staged; only a committed result is published to
   `published/`.

The publish stage deliberately writes a provisional file **before** the commit marker
when the fault is configured for it, so that a crash at that exact point leaves a file
whose `committed` flag is `false`. Input selection (`trustworthy_partitions`) and
verification both refuse it.

Faults are chosen per work item via environment variables
(`REHEARSAL_FAULT_STAGE`, `_ACTION`, `_ASSET`, `_PARTITION`), which is how the
`fail` and `cancel` commands are driven.

---

## Automated checks

```bash
./run.sh test
```

Eleven tests run against a real local Dagster instance, each in its own workspace:

| Test | Guards against |
| --- | --- |
| `test_missing_partitions_are_not_computed` | A blocked partition being computed on partial inputs |
| `test_gap_fill_recovery_is_scoped` | Recomputing already-current results during gap recovery |
| `test_repeat_request_does_not_republish` | A duplicate request rewriting identical results |
| `test_canceled_run_does_not_advance_partition` | A cancelled run leaving a trustable result; recovery correctness |
| `test_stale_revision_is_detected_and_repaired` | A correction going unnoticed; scoped repair over-running |
| `test_stale_input_detected_even_when_values_match` | Succeeded-but-stale runs that a value check would pass |
| `test_untrusted_published_file_blocks_downstream` | Consuming a file that was never committed |
| `test_untrusted_upstream_window_blocks_downstream` | A settlement reading an uncommitted upstream window |
| `test_failed_upstream_skips_downstream_sibling` | A failure partway through a plan letting downstream run on a stale upstream |
| `test_dependency_map_records_boundaries_and_rules` | The map losing boundary clamping or a rule |
| `test_cli_drill_produces_reports` | The operator commands not writing the promised artifacts |

Tests take about two minutes because they launch real runs, not because anything is
slow in principle.

---

## State rules: what is kept, what is recomputed

- **Kept**: committed source revisions; committed published results whose recorded
  input fingerprint still matches today's inputs (`already_current`); quarantined
  partial attempts (kept for inspection outside the staging area).
- **Recomputed**: any derived partition whose resolved inputs changed; any partition
  that never committed; any partition whose published file lacks a commit marker.
- **Never consumed as input**: uncommitted staging attempts; published files with
  `committed: false`; results whose recorded revisions are not the latest.

---

## Design notes

- **The dependency map is derived from the same asset definitions execution uses.**
  Asset edges and partition mappings live in `src/rehearsal/assets.py`; the map and
  planner read them back. They cannot drift.
- **The planner walks the graph in dependency order.** A derived input that is
  scheduled earlier in the same plan satisfies its dependents, so one plan expresses
  a whole chain. Idempotency is checked against the plan, not the on-disk lake, so a
  downstream partition whose upstream is being recomputed in this plan is correctly
  treated as changed.
- **Values are pure data; versions are provenance.** `values` contains only what the
  computation produced. Which revisions produced it lives in `inputs`. That separation
  is what makes "same numbers, wrong revision" detectable.
- **Source revisions are immutable.** A correction appends `seq+1`; older revisions
  stay readable, so a result that claims an old revision is identifiable after the
  fact.

## Limitations

- The demo's computation is intentionally trivial arithmetic; the drill is about
  dependency and provenance mechanics, not the transform.
- Genuine process kills (`kill -9`) can leave Dagster's event log mid-write. The
  drill's publish interruption therefore exits through Dagster's own failure path
  rather than a raw kill, so the recorded run status is always real and readable.
- Run-to-run performance is dominated by launching one Python process per partition;
  a production deployment would batch partitions per run. The per-partition recording
  is deliberate here so each decision maps to one observable run.
