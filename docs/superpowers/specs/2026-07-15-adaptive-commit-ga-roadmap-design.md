# Adaptive-Commit OSS-GA Roadmap — Meta-Spec

**Status:** Design approved 2026-07-15. This is a *roadmap* (meta-spec): it decomposes and
sequences the remaining production-readiness work into independently-designable sub-projects.
Each sub-project gets its own brainstorm → spec → plan → subagent-driven execution cycle. This
document does not itself contain implementation-level tasks.

**Goal:** Take the adaptive commit mode from "feature-complete on a branch, coexistence-audited"
to **production-ready for an OSS-core GA**.

**North star (unchanged):** adaptive = crash-safe, no-corruption recovery **+ good performance**.
NOSYNC stays the default; adaptive is opt-in. The durable epoch is a *recovery* mechanism, not a
backup. Backup stays as-is. Replication is the *future* recovery vector (out of scope here).

## Scope decisions (locked during brainstorming)

1. **Excluded:** integration/merge of the 124-commit stack (tracked separately; operator-driven).
2. **Target = OSS-core GA first.** Adaptive as an opt-in OSS commit mode is the v1 bar. Enterprise
   needs *coexistence-safety* only (the epoch, which ships in shared core, must not break
   replication/backup/roles). Full Enterprise-adaptive (S4 retention-floor composition,
   adaptive-on-primary-with-replication) is **v-next**, not this roadmap.
3. **Ordering = two parallel tracks** with one go/no-go gate (see Structure).

## Decomposition

Six near-term sub-projects, each independently designable/testable/shippable:

| ID | Sub-project | Track |
|----|-------------|-------|
| SP-C | Performance validation & tuning | Prove-it |
| SP-D | Durability / crash validation | Prove-it |
| SP-B | Recovery hardening & test-debt | Harden-it |
| SP-E | Upgrade & mixed-version compatibility | Harden-it |
| SP-A | Enterprise coexistence-safety **validation** (narrowed) | Harden-it |
| SP-F | Observability & docs | Harden-it (metrics early, docs late) |

**Explicitly deferred (NOT this roadmap):** SP-G `read_durability='replicated'` read-path;
SP-H in-place recovery from replication; full Enterprise-adaptive (S4 retention, adaptive-on-primary).

## Structure — two concurrent tracks, one go/no-go gate

```
                 ┌─────────────────────── TRACK 1: PROVE-IT (GA go/no-go) ───────────────────────┐
   SP-F(metrics) │  SP-C Performance ─┐                                                           │
   ───early────► │                    ├──► GA-viability verdict ──► (blocker ⇒ reshape Track 2)   │
                 │  SP-D Crash/durab. ─┘                                                           │
                 └───────────────────────────────────────────────────────────────────────────────┘
                 ┌─────────────────────── TRACK 2: HARDEN-IT (parallel) ────────────────────────┐
                 │  SP-B Hardening   │  SP-E Upgrade/compat  │  SP-A Coexistence-safety  │ SP-F docs (late) │
                 └───────────────────────────────────────────────────────────────────────────────┘
```

- **Track 1 (Prove-it)** answers "is adaptive crash-safe and fast enough to ship?" Its verdict
  gates the GA decision; a blocker reshapes Track 2 scope.
- **Track 2 (Harden-it)** runs in parallel and is largely independent of Track 1's verdict.
- **The one hard cross-dependency:** SP-F's *metrics slice* (durable-frontier lag, recovery
  incarnation, epoch cadence) must land **early** — both Prove-it sub-projects need it to interpret
  their own results. The rest of SP-F (user docs) is late GA-polish.

## Track 1 — Prove-it

### SP-C — Performance validation & tuning
**Goal:** prove the "good performance" half of the north star.
**Scope / work:**
- Run `CommitModeBenchmark` across modes (NOSYNC / ASYNC / SYNC / ADAPTIVE) × workloads
  (high-ingest, small-batch, wide-table, out-of-order/O3).
- Measure: throughput, p99 commit latency, epoch overhead, recovery time.
- Characterize the group-commit window **W** (the RPO ↔ throughput curve; W=0 is zero-loss).
- **Settle the open lazy-apply question:** confirm whether the O3/block apply path still fsyncs
  under ADAPTIVE (a documented concern from Plan 2B). If it does, that is either a bug to fix or a
  cost to document — decide with data.
- Output: tuning guidance for `cairo.adaptive.epoch.interval.ms` and the group-commit window.

**Acceptance bar:** adaptive throughput competitive with NOSYNC on core workloads (single-digit-%
delta target; **the exact threshold is fixed in SP-C's own spec** with the benchmark environment);
bounded recovery time; no unexpected fsync in the hot apply path; a published RPO/throughput curve.
**Depends on:** SP-F metrics slice.

### SP-D — Durability / crash validation
**Goal:** prove "crash-safe, no corruption" beyond simulated faults.
**Scope / work:**
- Expand the `CrashFaultFilesFacade` injection matrix: injection points (mid-epoch, mid-group-commit
  flush, torn WAL-e record, torn `.epoch` copy, partial column msync) × schemas (O3, mat-views,
  indexed, parquet).
- Add an **adaptive crash-fuzz**: randomized ops + random crash point → recover → oracle (every
  acked txn survives; zero corruption; adaptive == SYNC committed state; loses < NOSYNC).
- Add a **soak** (long-running ingest + periodic crash+recover).
- **Spec the real power-loss protocol + oracle.** Hardware execution is external/operator-run, but
  the workload, the fault schedule, the oracle, and the pass/fail bar are all designable now and are
  a deliverable of this sub-project.

**Acceptance bar:** crash-fuzz runs N hours with zero corruption (**N fixed in SP-D's spec**); the
oracle holds across the full expanded matrix; the power-loss protocol document is ready to hand to
hardware. **Depends on:** benefits from SP-B but not blocked by it.

## Track 2 — Harden-it

### SP-B — Recovery hardening & test-debt
**Goal:** close the known correctness gaps and test debt surfaced by the coexistence review.
**Scope / work:**
- **Demote-vs-in-flight-apply race** (review Finding 2): an apply batch that passed
  `isLocalDurabilityEnabled()` before the ctor installed `REPLICA_SKIP` can re-create the epoch trio
  after `ReplicaRoleState.openLoops` cleared it. Benign today (re-created epoch is same-lineage), but
  make the clear a *hard guarantee* — quiesce apply, or re-clear after a barrier.
- **C2 `TxReader` invariant** (review Finding 3): the C2 skip is safe only because
  `unsafeLoadAll()` can never return a clean seqTxn below the epoch for a single-lineage post-crash
  `_txn`. Pin the invariant with an assert/comment so a future slot-selection change can't silently
  break it.
- **Deferred S5.1 test gaps:** `SequencerMetadata.create` rebase-path durability sites (replica-
  reachable only via Enterprise `REBASE WAL INTO`); `_view`/`WalEvents` skip tests. Either test them
  or explicitly waive with mechanism-level coverage + rationale.

**Acceptance bar:** Findings 2 & 3 resolved with tests; S5.1 gaps tested or waived-with-coverage.

### SP-E — Upgrade & mixed-version compatibility
**Goal:** safe rollout and rollback across versions.
**Scope / work:**
- **Forward-compat of on-disk artifacts:** an older binary opening a db written by adaptive must not
  choke on `_snapshot` / `.epoch` / the `_event`/`_txn`/`_cv` CRC trailers. The trailers are
  magic-gated — verify the old-reader path treats them as inert/ignored.
- **Meta-format v2→3** (commit-mode field): old readers must read UNSET → global commit mode (already
  designed; verify under a real old binary).
- **Rolling upgrade:** mixed-version cluster (old ↔ new) behaves.
- **Downgrade path:** turning adaptive off must drain cleanly and leave the epoch artifacts inert.

**Acceptance bar:** the {old,new} binary × {adaptive on,off} db-open matrix is clean; a documented
upgrade + downgrade runbook exists.

### SP-A — Enterprise coexistence-safety **validation** (narrowed for OSS-GA)
**Goal:** prove the epoch (shipping in shared core) doesn't break Enterprise, even though Enterprise
doesn't *use* adaptive as its commit mode yet.
**Scope / work (validation, not features):**
- Run the ENT replication / backup / role-transition suites with the epoch code present and confirm
  no regressions. Formalize the clean-isolation result already observed in the broad-suite run.
- Produce a documented "coexistence-safe" sign-off enumerating each Enterprise subsystem × the
  epoch's producer/consumer/clearer touchpoints (building on this session's audit map).

**Explicitly out (→ v-next Enterprise-adaptive):** S4 retention-floor composition
(`min(durable-epoch, uploaded-frontier)` on a replicating primary); adaptive-on-primary. These are
*features* enabling Enterprise to run adaptive, not coexistence-safety.
**Acceptance bar:** ENT suites green with the epoch present; coexistence-safe sign-off document.

### SP-F — Observability & docs
**Goal:** operable and documented.
**Scope / work — two slices with different timing:**
- **Metrics slice (EARLY, Track-1 dependency):** durable-frontier lag, recovery incarnation, epoch
  cadence — exposed as metrics (Prometheus) + already-present `wal_tables()` columns. Needed to
  interpret SP-C/SP-D results and to alert ops on "durable frontier falling behind."
- **User docs (LATE, GA-polish):** the commit mode, the RPO knob (group-commit window), config keys,
  and an ops runbook (what the metrics mean, recovery expectations).

**Acceptance bar:** metrics emitted + scrapeable with alerting guidance; user-facing docs published.

## GA acceptance bar (definition of done)

Adaptive commit mode is GA-ready for OSS-core when **all** hold:
- Opt-in; NOSYNC remains default (blast radius contained).
- **Crash-safe:** SP-D oracle holds across the expanded matrix; power-loss protocol ready.
- **Performant:** SP-C bar met; lazy-apply question settled; RPO/throughput curve published.
- **Upgrade-safe:** SP-E matrix clean; upgrade/downgrade runbook.
- **Coexistence-safe:** SP-A sign-off + SP-B gaps closed.
- **Observable + documented:** SP-F metrics + docs shipped.

## Execution model

This roadmap is the meta-spec. Each sub-project then runs the full cycle independently:
brainstorm → spec (`docs/superpowers/specs/`) → writing-plans → subagent-driven execution. The two
tracks may proceed concurrently; within Track 2 the sub-projects are mutually independent except for
SP-F's metrics slice, which is scheduled first. The Prove-it verdict is a checkpoint: a
show-stopper there (perf regression or crash-safety hole) reopens design before further hardening.

## Open decisions (resolved in each sub-project's own spec, not here)

These are deliberately *not* fixed at the roadmap level — they are the first decision in the named
sub-project's brainstorm, because they require that sub-project's environment/data:
- **SP-C:** the exact throughput-delta threshold vs NOSYNC, and the benchmark hardware/workload set.
- **SP-D:** the crash-fuzz duration N and the soak duration; the power-loss hardware protocol details.
- **SP-E:** the exact set of "old" binary versions in the compatibility matrix.
