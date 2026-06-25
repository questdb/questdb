# Always-On Crash-Recovery Snapshots via an `adaptive` Commit Mode

**Status:** design / approved for planning
**Date:** 2026-06-25
**Branch:** `nw_adaptive_commit` (off `nw_sync_batch`)
**Depends on:** the corruption-audit cluster — `nw_crash_consistency`, `nw_cv_checksum`, `nw_sync_cheaper`, `nw_sync_opt`, `nw_sync_batch` (see [§13 Dependencies](#13-dependencies--the-branch-consolidation))

---

## 1. Summary

Deliver the roadmap's "lightweight always-on snapshots" as a new **`adaptive` commit mode** that gives
crash-safe recovery **without increasing read latency and without large write overhead**.

The core move is to split QuestDB's two durability frontiers:

- **WAL commit is made durable** (fsync of the small, append-only log: segment data → WAL‑e events → sequencer record), so every acked transaction is replayable.
- **WAL apply stays lazy** (msync only, as today), because the materialized table is a *rebuildable cache* of the durable log.

The fsync cost lands on the small log, not on the large materialization, and never on the read path.
A background **epoch** mechanism periodically pins a durable, consistent materialized state so crash recovery
replays only a bounded WAL tail. Reader visibility is **gated on the durable frontier** so a crash can never
"un-see" a row a client already read.

This is **orthogonal to `CHECKPOINT`** (which remains the external-filesystem-snapshot backup tool); `adaptive`
is the continuous, internal crash-recovery axis and reuses checkpoint's scoreboard-pinning plumbing.

## 2. Goals & non-goals

**Goals**
- A new per-database / per-table commit mode `adaptive` (`CommitMode.ADAPTIVE = 3`).
- Recovery guarantee: *every acked (WAL-committed) transaction is recoverable; the materialized table is a rebuildable cache of the durable log.*
- No read-path latency cost; write overhead close to `NOSYNC` (fsync the log, not the table).
- Always-on, automatic — no operator action, no external FS snapshot, no indefinite purge freeze.
- Bounded boot-time recovery cost (via epochs) and bounded WAL retention.
- Reader visibility that is **crash-monotonic** (no phantom or non-monotonic reads).

**Non-goals (v1)**
- No new user-facing snapshot CRUD SQL. "Always-on" = automatic; recovery is on boot.
- No arbitrary point-in-time **rewind** UX in OSS (`RESTORE ... AS OF t`). The *substrate* (epoch + retained WAL) already powers Enterprise PITR (`PointInTimeRecoveryConfiguration`); the OSS local-rewind verb is a clean future layer.
- **No Enterprise replication/backup _implementation_ in v1** — but v1 ships the integration *seams* (see [§17](#17-enterprise-integration-seams-v2)) so the existing pipeline plugs in as **v2**. Enterprise may go further than OSS.
- No speculative `read_durability='latest'` reader mode (designed for, deferred — see [§7](#7-read-durability-semantics)).
- Does not change `NOSYNC`/`ASYNC`/`SYNC` semantics; `NOSYNC` stays the default for v1.
- Does not replace `CHECKPOINT`.

## 3. Background — durability model today

Confirmed by source read (locations on `nw_sync_batch`):

- `CommitMode`: `ASYNC = 0`, `SYNC = 1`, `NOSYNC = 2` (`CommitMode.java`). Parsed in `PropServerConfiguration.getCommitMode` (≈ line 2568), default `NOSYNC` (`DefaultCairoConfiguration.getCommitMode`).
- `.sync()` bottoms out at `msync(addr, len, MS_SYNC|MS_ASYNC)` (`files.c:271`). **SYNC = msync, no `fsync`/`fdatasync` on the normal commit path** (per `CORRUPTION_AUDIT.md` §0).
- The table records its applied sequencer position: `_txn` stores `seqTxn` at `TX_OFFSET_SEQ_TXN_64` (`TxReader.getSeqTxn`, `TxWriter.setSeqTxn`). The `_txn` body checksum covers only `[80,116)` (`TxWriter.java:374` comment) — i.e. **audit #9** (no full-body integrity), a hard precondition for safe lazy apply.
- WAL apply replays a contiguous sequencer range: `ApplyWal2TableJob` loops `for (seqTxn = initialSeqTxn; seqTxn < lastSeqTxn; seqTxn++)` (≈ line 160). Apply is already resume-from-a-seqTxn.
- `TxnScoreboardV2` exposes `acquireTxn(id, txn)`, `releaseTxn(id, txn)`, `getMin()` — the pin primitive for retaining a version against purge.
- `WalPurgeJob` currently floors WAL retention at **`lastAppliedTxn`** (≈ line 575, `getCurrentSeqPart(lastAppliedTxn, ...)`). **This is the one floor that must change** (see [§8](#8-gc-coordination)).

Two frontiers exist already; `adaptive` makes one of them crash-consistent rather than inventing a new artifact:

```
client ─▶ WAL segment (.d/.i) ─▶ WAL-e events ─▶ sequencer txnlog ─┐  txn N
            append                 append            append         │
                                                                    ▼
                                          ApplyWal2TableJob (applies txns in order)
                                                                    │
                                                                    ▼
                                   table partitions + _txn/_cv  (MVCC A/B commit point)
                                                                    │
                   readers ◀───────────────────────────────────────┘  read materialized state only
```

Readers never touch the WAL — they read materialized partitions at the `_txn` commit point. That is *why*
this can be zero read-overhead: all new machinery lives on the write/GC/recovery side, behind the MVCC
commit point readers already use.

## 4. The durability contract (`adaptive` mode)

```
WRITE PATH (per commit)                                    DURABILITY
  WalWriter appends rows  → segment .d/.i        ┐
  writes WAL-e events record (+CRC)              ├─ fsync, data→events→seq order,   ◀─ BARRIER
  publishes sequencer txn record (+CRC)          ┘  group-batched across writers       ack AFTER this

APPLY PATH (async · ApplyWal2TableJob)
  materialize txn → partitions + _txn/_cv (MVCC)    msync only  ◀─ re-derivable from the durable log

EPOCH (background · every T ms / N txns / M bytes)
  pick a consistent applied _txn → fsync that partition cut + _txn/_cv (+CRC)
  write _snapshot marker (A/B + CRC), scoreboard-pin it, release the prior epoch  ◀─ FAST-BOOT ANCHOR
```

**Guarantee:** every acked transaction is recoverable; the table is a rebuildable cache of the durable log.

**RPO knob.** The group-commit window `W` trades RPO for throughput:
- `W = 0` → fsync-before-ack → **exactly zero loss**.
- `W > 0` → ack after a batched fsync → loss bound **≤ `W`** ("very close to zero").

**Why "adaptive" — it adapts on four axes:**
1. **What** to fsync — the durable log vs the re-derivable table.
2. **When** — the group-commit window scales with load (auto RPO/throughput trade).
3. **How** — the sync syscall strategy adapts to the filesystem: `syncfs` / ext4 `fast_commit` fallback / `sync_file_range` / `fdatasync`, auto-detected (from `nw_sync_batch`).
4. **Epoch cadence** — adapts to write volume, bounding boot replay and WAL retention.

## 5. Architecture & components

Six well-bounded units, each independently testable.

### 5.1 `WalCommitDurability` — the RPO barrier
- **Does:** collects concurrent WAL commits within window `W` and issues one ordered fsync batch (segment data → events → sequencer record), then acks. Group commit so N concurrent writers share ≈ one device flush.
- **Interface:** `commit(segment, events, seqRecord) → durableSeqTxn`.
- **Depends on:** `FilesFacade` + the Goal-2 cheaper-sync primitives (`fdatasync`, `sync_file_range`, `syncfs`).
- **Invariant:** ack happens strictly after the batch fsync returns (INV‑1).

### 5.2 `WalIntegrity` — torn-tail detection
- **Does:** writes a per-record CRC footer on WAL‑e and txnlog records (audit #10), and validates segment column file lengths against the declared `[rowLo,rowHi)` before mapping (audit #6). Pure / CPU-only.
- **Interface:** `appendCrc(record)`, `verify(record) → intact?`, `validateSegmentLen(fd, declaredHi) → ok?`.
- **Used by:** recovery, to find the last intact `seqTxn` (the durable WAL frontier).

### 5.3 `SnapshotEpochJob` — the fast-boot anchor
- **Does:** periodically (every `T` ms / `N` txns / `M` bytes) advances a durable epoch: picks a consistent applied `_txn`, fsyncs that partition cut + `_txn`/`_cv` (validated by body checksum #9), writes the `_snapshot` marker durably, pins the epoch txn in the scoreboard, then releases the prior epoch's pin.
- **State:** `durableEpoch{seqTxn, txn, ts}` per table.
- **Interface:** `advance(tableToken)`.
- **Invariant:** the new marker is fsync'd before the prior pin is released (INV‑5) — there is always a valid durable anchor.

### 5.4 `RecoveryCoordinator` — boot path
- **Does, per `adaptive` table:** ① read `_snapshot` (A/B + CRC; fall back to the oldest pinned `_txn` if both sides torn) → `epoch`; ② open the table at `epoch.txn`, validating `_txn`/`_cv` body checksum (#9) + column lengths (#3/#6); ③ compute the **durable WAL frontier** = highest `seqTxn` whose seq record + events + segment byte-range are all CRC-/length-intact (`WalIntegrity`); ④ replay `(epoch.seqTxn, frontier]` via `ApplyWal2TableJob` with `initialSeqTxn = epoch.seqTxn`.
- **Interface:** `recover(tableToken) → restoredSeqTxn`.
- **Invariants:** apply is idempotent/restartable (over-replay safe, INV‑6); under-replay is blocked by #9. Also fixes audit #5 (fsync restored state before clearing the recovery marker).

### 5.5 GC coordination
- **Does:** `WalPurgeJob` floors WAL retention at `min(durableEpoch.seqTxn)` across consumers (not `lastAppliedTxn`); partition-purge honors the epoch scoreboard pin and the durable read-frontier pin (see [§7](#7-read-durability-semantics)).
- **Invariant:** WAL is never purged below the minimum durable epoch seqTxn (INV‑2).

### 5.6 `_snapshot` marker file
- **Does:** the durable per-table pointer to the fast-boot anchor.
- **Format:** small, A/B-versioned + CRC (reuses `_txn`'s proven atomic single-version-word flip; see [§9](#9-on-disk-_snapshot-marker)).

## 6. Data flows

**Commit (RPO barrier).** append rows → write events (+CRC) → publish seq record (+CRC) → group-fsync(data → events → seq) → **ack**. `durableSeqTxn` advances on fsync completion.

**Epoch (bounds boot + retention).** freeze a consistent applied `_txn` → fsync partition cut + `_txn`/`_cv` → write+fsync `_snapshot` → `scoreboard.acquireTxn(epochId, epochTxn)` → `releaseTxn` prior epoch → advance the WAL-purge floor.

**Recover (RPO≈0).** open-at-epoch (fall back to prior if torn) → scan WAL forward to the first record failing CRC/length = frontier → `ApplyWal2TableJob(initialSeqTxn = epoch.seqTxn, lastSeqTxn = frontier+1)` → table at the durable frontier → serve reads.

## 7. Read-durability semantics

`adaptive`'s visibility frontier (applied → `_txn`) can run ahead of its durability frontier (WAL-fsync'd / epoch'd). Left unmanaged, a reader pinned to visibility could see a row a crash then un-sees:

```
seqTxn:   ── E ─────────── D ─────────── A ──▶      E=epoch, D=durable WAL frontier, A=applied/visible
   rows seen with seqTxn ∈ (E, D] : reapplied on recovery → transient stale (non-monotonic read)
   rows seen with seqTxn ∈ (D, A] : never WAL-durable     → PHANTOM (gone for good)
```

**Decision (v1): prevent by construction — gate visibility on durability.** A new reader opens at the latest
`_txn` whose `seqTxn ≤ durableSeqTxn`; that version is scoreboard-pinned so it stays readable while apply runs
ahead. Then visibility can never exceed durability, so after any crash the restored state is ≥ everything any
reader ever saw — **both phantom and non-monotonic reads are impossible.**

- **Invariant:** reader-visible commit watermark ≤ `durableSeqTxn` (INV‑3).
- **Cost is freshness, not latency.** Reads run at full speed; they trail live writes by ≤ `W` (≈ fsync latency at `W=0`). This preserves the "no read-latency increase" constraint.
- **Implementation latitude:** either gate apply (`appliedSeqTxn ≤ durableSeqTxn`, simplest) or let apply run ahead and clamp only the *reader-visible* version (decouples apply throughput from fsync; needs the durable-frontier version pinned). The plan picks one; both satisfy INV‑3.

**Multi-tier durability (the watermark already exists).** QuestDB already tracks an *uploaded* durable frontier — `DurableAckRegistry.getDurablyUploadedSeqTxn`, surfaced to opt-in clients via the QWP `X-QWP-Request-Durable-Ack` frame. `adaptive` adds a *local-fsync* tier under the **same** abstraction, giving a tier-ordered chain `applied ≥ localDurable ≥ uploaded`. v1 gates visibility on `durableSeqTxn` (the `localDurable` tier) and extends `DurableAckRegistry` to report it, rather than inventing a parallel watermark. The gate generalizes to a per-session `read_durability` tier — `local` (power-loss safe, OSS default) vs `replicated` (node-loss / failover-consistent, Enterprise v2) vs `latest` (speculative, deferred) — because a primary-visible-but-unreplicated row is the cluster analog of a phantom. See [§17](#17-enterprise-integration-seams-v2).

**Exposed for observability (v1):** the `durableSeqTxn` (local-fsync) tier via the extended `DurableAckRegistry`, plus a **recovery-incarnation** counter that bumps on every rollback.

**Deferred:** `read_durability='replicated'` (Enterprise v2) and `read_durability='latest'` (speculative). Not needed for OSS v1 (YAGNI).

## 8. GC coordination

The single critical change: **in `adaptive`, the `WalPurgeJob` floor drops from `lastAppliedTxn` to `min(durableEpoch.seqTxn)`.** Apply is no longer a durability point, so retaining WAL only to `lastAppliedTxn` would discard the sole durable copy of applied-but-not-epoch'd data. Retaining to the durable epoch is also what *bounds* WAL growth — without epochs, WAL would grow unbounded. Non-`adaptive` tables keep today's floor.

This composes with the existing two-part retention: `WalPurgeJob.getSafeToPurgeUpToTxn` already takes a `min` over consumers (active readers + dependent mat-views via `appliedToViewTxn`), so `adaptive` adds `durableEpoch.seqTxn` as one more `min` term; and `WalDirectoryPolicy.isInUse` independently holds a segment in use (Enterprise's `UploadWalDirectoryPolicy` keeps it until uploaded). The epoch `min`-term and the in-use policy are orthogonal — both must clear before a segment is purged.

## 9. On-disk: `_snapshot` marker

- One small file per table directory. Layout mirrors `_txn`'s atomic A/B scheme: two record slots + a single aligned, fenced version word selecting the live slot; each slot CRC-covered (full body, unlike the current `_txn` gap #9).
- Record body: `{ epochSeqTxn:i64, epochTxn:i64, ts:i64, formatVersion:i32, crc:i64 }`.
- Written and fsync'd before the prior epoch pin is released (INV‑5). Absent marker ⇒ epoch 0 ⇒ full WAL replay (= today's behavior; the back-compat path).
- Additive only: no `_meta`/`_txn` format bump.

## 10. Configuration

- `cairo.commit.mode = adaptive` (global) — add `ADAPTIVE = 3` and parse `"adaptive"` in `getCommitMode`.
- Per-table override: `CREATE TABLE ... WITH commit_mode='adaptive'` and `ALTER TABLE ... SET PARAM commit_mode='adaptive'`. Mixed-mode databases are supported (it is per-table).
- `cairo.adaptive.commit.group.window.us` — the window `W` (`0` = fsync-before-ack = zero loss). Default: small (e.g. 1–5 ms), tunable.
- `cairo.adaptive.epoch.interval.ms` / `.txns` / `.bytes` — epoch cadence (bounds boot replay + WAL retention).
- Sync strategy (`syncfs` / ext4 `fast_commit` fallback / `sync_file_range` / `fdatasync`) auto-detected per filesystem; reuse `nw_sync_batch`'s detection + the nobarrier startup warning.

## 11. Observability

Extend `wal_tables()` with: `commit_mode`, `durableSeqTxn` (the local-fsync tier of `DurableAckRegistry`), `lastEpochSeqTxn`, `walRetentionTxn`, `recoveryIncarnation`. (Enterprise additionally surfaces the `uploaded` tier, already present.) No new SQL verbs in v1.

## 12. Back-compat & rollout

- `adaptive` is **opt-in**; `NOSYNC` stays the default for v1. Promoting `adaptive` to the default is a later, separately-justified decision (it would materially improve out-of-box crash safety at low cost).
- `_snapshot` is additive; absent ⇒ today's full-replay behavior. No format bump. A database may mix modes per table.
- Rollout order: land the dependency layer (§13) first, then `adaptive` on top.

## 13. Dependencies — the branch consolidation

`adaptive` is the umbrella the corruption-audit cluster has been building toward. It **builds on**, and does not re-spec, that work:

| Layer | Branch(es) | Role for `adaptive` |
|---|---|---|
| Audit base + harness + benches | `nw_crash_consistency` | `CORRUPTION_AUDIT.md`, `CommitModeBenchmark`, power-cut harness foundation |
| Phase C integrity — `_cv` body checksum | `nw_cv_checksum` | makes torn `_cv` bodies detectable |
| Phase A/C — record CRC, `_txn` body checksum (#9), WAL‑e/txnlog CRC (#10), V2 seq sync (#8), length validation (#3/#6) | audit Phases A+C | the **preconditions** for safe lazy apply + torn-frontier detection |
| Goal 2 — cheaper SYNC | `nw_sync_cheaper` (fdatasync, append-scoped msync), `nw_sync_opt` (msync pipelining), `nw_sync_batch` (syncfs batching, ext4 `fast_commit` detection, power-cut harness) | the **"how to sync" strategies** `adaptive` chooses among on axis 3 |

Net: these stop being five loose experiments and become this mode's foundation layer. (The `nw_sync_opt` vs `nw_sync_batch` Goal-2 strategies are alternative implementations of axis 3 — reconcile/merge them during the dependency-landing step.)

**Enterprise-critical within this layer:** replication uses the **V2 split txnlog** (`txnPartSize > 0`), so audit **#8 (V2 sequencer publishes a durable maxTxn over an unsynced record)** is not merely a precondition — it sits on the `adaptive` commit path, and the durable-frontier computation must understand part files. The WAL-record CRC (#10) should **be** the transfer-layer checksum (`transfer/ChecksumMode`), verified on upload and restore — one checksum, not two.

## 14. Invariants (backbone for implementation + tests)

- **INV‑1** A txn is acked only after its WAL (segment data → events → seq record) is durable, in data-before-pointer order.
- **INV‑2** WAL is never purged below the minimum durable epoch seqTxn.
- **INV‑3** Reader-visible commit watermark ≤ `durableSeqTxn`.
- **INV‑4** Every WAL/seq/segment record carries a CRC / length guard; recovery stops at the first that fails.
- **INV‑5** The `_snapshot` marker is updated atomically (A/B + CRC) and fsync'd before the prior epoch pin is released.
- **INV‑6** Apply is idempotent/restartable from any `seqTxn ≤ appliedSeqTxn` (over-replay safe).

## 15. Testing strategy

Reuse the `nw_sync_batch` power-cut harness verbatim (`dmsetup suspend --nolockfs` + `drop_writes`; qemu / `pkill -9`; ext4/xfs-parameterized).

- **Crash-injection points:** mid-segment-append · mid-events · mid-seq-publish (pre/post group-fsync) · mid-apply · mid-epoch-fsync · mid-marker-flip.
- **The oracle (per injected crash):**
  - every acked txn is present after recovery (INV‑1);
  - no reader ever observed a row absent post-recovery — verified by recording reads pre-crash and checking INV‑3;
  - no torn/garbage read ever surfaces — always a loud `CairoException` or correct data, **never silent corruption** (INV‑4);
  - `adaptive` converges to the **same** committed state as `SYNC`, and loses **strictly less** than `NOSYNC`.
- **Negative control (per the verify-severity-empirically discipline):** each test first proves the crash loses/torns data *without* `adaptive`, then shows `adaptive` recovers it.
- **Bench:** extend `CommitModeBenchmark` with an `adaptive` arm (note `nw_varchar-corruption` already has WIP there). Targets: within a few % of `NOSYNC` write throughput (we fsync the small log, not the big table); materially cheaper than `SYNC`. Boot-recovery time bounded by epoch cadence.

## 16. Open questions / risks

- **Apply-gating vs visibility-clamping** for INV‑3 (§7) — pick during planning; affects apply throughput vs version-retention complexity.
- **Epoch consistency** — selecting a genuinely consistent `_txn` cut while apply runs concurrently (lean on the existing checkpoint quiesce/scoreboard plumbing).
- **Goal-2 strategy reconciliation** — `nw_sync_opt` (pipelining) vs `nw_sync_batch` (syncfs) as axis-3 implementations: one, both (FS-selected), or merge.
- **`W>0` semantics** — document precisely that acks may trail by ≤ `W`; ensure client-facing ack timing matches the durability point.
- **Mat views / V2 sequencer** — confirm epoch + durable-frontier semantics compose with mat-view refresh state and the V2 split txnlog (audit #8); see [§17](#17-enterprise-integration-seams-v2).

## 17. Enterprise integration seams (v2)

v1 is OSS-core; Enterprise replication / backup / PITR is **v2**. Most of that machinery already exists (`questdb-ent/.../cairo/wal/transfer/` — `WalUploader`, `WalDownloader`, `UploadWalDirectoryPolicy`, `ChecksumMode`, `PointInTimeRecoveryConfiguration`; `.../cairo/backup/` — `DatabaseBackupAgent`, `CheckpointManifest`). v1's job is to expose the **seams** so v2 snaps on without rework. Enterprise may go further than OSS.

- **S1 · Durability as a tier-ordered frontier.** Generalize `DurableAckRegistry` to report a per-table chain `applied ≥ localDurable ≥ uploaded`. v1 wires the new `localDurable` tier (group-fsync of the WAL); Enterprise's upload pipeline already provides `uploaded` (`getDurablyUploadedSeqTxn`). Both feed the QWP durable-ack frame and the [§7](#7-read-durability-semantics) gate. *Seam:* per-tier accessors on `DurableAckRegistry`.
- **S2 · `read_durability='replicated'` (failover-consistent reads).** With S1, gating visibility on the `uploaded` tier means a promoted replica never loses a row a client read on the old primary. Pure config over the same gate; Enterprise-only (needs the upload frontier). *Seam:* the gate takes a tier parameter; v1 hard-codes `local`.
- **S3 · Epoch = incremental-backup base.** `SnapshotEpochJob` produces exactly the "consistent `_txn` cut at a known seqTxn" that `DatabaseBackupAgent` builds today via `checkpointCreate(incremental=true)`. Shape the epoch so the backup `WalUploader` can adopt it as a base and ship epoch + WAL `(prevEpoch.seqTxn, epoch.seqTxn]` to object store — continuous PITR without the periodic heavyweight checkpoint. *Seam:* epoch exposes `{seqTxn, txn, consistent-cut handle}`; reconcile with `CheckpointManifest`.
- **S4 · Retention composition.** Already structured ([§8](#8-gc-coordination)): the epoch `min`-term coexists with `UploadWalDirectoryPolicy`'s hold-until-uploaded. *Seam:* none beyond the epoch term — document that both gates apply.
- **S5 · Replica-side adaptive.** A replica's source of truth is object storage, so it runs lazy-apply + epochs for fast restart but **skips the local WAL-commit fsync** (recovery = re-fetch via `WalDownloader`). *Seam:* `WalCommitDurability` is pluggable per role — full barrier on a primary, no-op on a replica.
- **S6 · Integrity = transfer checksum.** The audit's WAL-record CRC (#10) and `transfer/ChecksumMode` are the same need; the checksum written locally is the one verified on upload and on restore, protecting the object-store copy too. *Seam:* a single `WalIntegrity` CRC consumed by both recovery and transfer.

**v2 risks:** V2 split-txnlog durable-frontier semantics (#8); epoch ↔ `CheckpointManifest` format reconciliation; confirm `localDurable` epochs stay node-local (proposed: only WAL + backup bases upload, not per-node epochs).
