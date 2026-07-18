# SP-E — Old-binary compatibility matrix: REAL-BINARY results

**Status:** executed 2026-07-18. OSS core, branch `nw_adaptive_commit`. Companion to
[SP-E upgrade-compat design](2026-07-17-adaptive-sp-e-upgrade-compat-design.md) and the
[upgrade/downgrade runbook](2026-07-17-adaptive-sp-e-upgrade-downgrade-runbook.md).

This closes the one thing SP-E flagged as **un-runnable in-process**: the `{old-binary}` half of the
`{old,new} binary × {adaptive on,off}` open matrix. `AdaptiveUpgradeCompatTest` proves each gate
*in code* ("a reader that does not know the artifact ignores it"); this document proves the same
thing on **real, published, pre-adaptive QuestDB release binaries** opening a database that the
current adaptive core actually wrote.

## Verdict

**PASS — all four old release cores open + read the adaptive-written database cleanly.** No core
crashed, refused to open, or reported corruption on any adaptive artifact. Every adaptive artifact
(`_snapshot`, `_txn.epoch`, `_cv.epoch`, the `_txn`/`_cv` CRC trailers, the meta-v3 `commit_mode`
field) is treated as inert. After each old core reads the db, all six on-disk artifacts are
**byte-identical**; the only file any old core mutates is the root `_upgrade.d` engine-migration
marker (standard QuestDB downgrade bookkeeping, not a table artifact).

| old core | db `t` (20k rows, adaptive/WAL) | db `cm` (`commit_mode='adaptive'`, field=3) | adaptive artifacts after read |
|---|---|---|---|
| **9.4.3** (N-1 release) | PASS | PASS | all 6 byte-identical; `_upgrade.d` unchanged |
| **9.4.0** (latest GA) | PASS | PASS | all 6 byte-identical; only `_upgrade.d` re-stamped |
| **9.3.5** | PASS | PASS | all 6 byte-identical; only `_upgrade.d` re-stamped |
| **9.2.3** | PASS | PASS | all 6 byte-identical; only `_upgrade.d` re-stamped |

The one design-doc target NOT covered here — the pre-adaptive `9.4.4-SNAPSHOT @ cdb0ea073b` fork
point — is an *unpublished snapshot* and needs a source build; see [Gap](#gap-the-fork-point-binary).
The released binaries tested are *older* meta-minor-version readers than that fork point, so they are
a **stricter** forward-compat test, not a weaker one.

## What was written (by the current adaptive core)

Writer core: `questdb-9.4.4-SNAPSHOT` (`BuildInformationHolder = 9.4.4-SNAPSHOT:6feac1fa1d156b2308b53ad28f1ee5a5bb7ffcbb:25.0.3`), the adaptive branch build in `~/.m2`.

**DB `t`** — `org.questdb.CrashIngestWriter -DcommitMode=adaptive -Dgroup.window.us=0 -Dmax.rows=20000`
→ `/tmp/spe-adaptive-db`. A WAL table `t (id long, v long, s symbol index, ts timestamp)` partitioned
by DAY, 20 000 rows committed through the adaptive WAL + apply + group-commit path (C=20, Wm=20).
Adaptive artifacts confirmed present:

```
/tmp/spe-adaptive-db/t~1/_snapshot      (4096 B)   epoch marker (SnapshotMarker, A/B + CRC)
/tmp/spe-adaptive-db/t~1/_txn.epoch     (4096 B)   durable-epoch copy of _txn
/tmp/spe-adaptive-db/t~1/_cv.epoch      (4096 B)   durable-epoch copy of _cv
/tmp/spe-adaptive-db/t~1/_txn           (4096 B)   carries the TX_OFFSET_BODY_CHECKSUM_64 trailer
/tmp/spe-adaptive-db/t~1/_cv            (4096 B)   carries the CV_CHECKSUM_MAGIC trailer
/tmp/spe-adaptive-db/t~1/_meta          (4096 B)   meta minor version v3 (commit_mode field present)
```

`_meta` field decode (offsets from `TableUtils`):

| field | offset | value | meaning |
|---|---|---|---|
| `META_OFFSET_VERSION` | 12 | **426** | table-format version = `ColumnType.VERSION` (identical in every core tested) |
| `META_OFFSET_META_FORMAT_MINOR_VERSION` (high short) | 41 | **3** | `META_FORMAT_MINOR_VERSION_COMMIT_MODE` → meta is **v3** |
| `META_OFFSET_COMMIT_MODE` | 53 | **-1** | `CommitMode.UNSET` — `t` has no per-table override; adaptive came from the global config |

**DB `cm`** — a second db written `WITH commit_mode='adaptive'` (`spe.CmWriter` → `/tmp/spe-adaptive-cm`),
so the v3 `commit_mode` field on disk actually holds `CommitMode.ADAPTIVE (3)`, not UNSET. Same epoch
trio present. This is the strongest form of artifact #1 to hand an old (pre-v3) reader:

| field | offset | value |
|---|---|---|
| `META_OFFSET_META_FORMAT_MINOR_VERSION` (high short) | 41 | 3 (v3) |
| `META_OFFSET_COMMIT_MODE` | 53 | **3** (`CommitMode.ADAPTIVE`) |

## Old binaries obtained + how

`mvn org.apache.maven.plugins:maven-dependency-plugin:3.6.1:copy -Dartifact=org.questdb:questdb:<ver>
-DoutputDirectory=/tmp/spe-oldbins` — fetched from Maven Central (network available). Each is a
genuine published release (real git SHA in its `BuildInformationHolder`, JDK17/25 build):

| version | `BuildInformationHolder` (version:sha:jdk) | jar bytes | `ColumnType.VERSION` |
|---|---|---|---|
| **9.4.0** (latest GA)   | `9.4.0:726862924e16f9d331171a489201d538081cc268:25.0.2` | 32,168,347 | 426 |
| **9.3.5**               | `9.3.5:4bb96297b8908ba355692ef3f663281123296b7e:17.0.9` | 30,942,037 | 426 |
| **9.2.3**               | `9.2.3:d655b294c12607804528fb05cd25278037c7e1db:17.0.9` | 29,681,688 | 426 |
| **9.4.3** (bonus, N-1)  | `9.4.3:33fa1320b75697c004626233baeab636b27f53f7:25.0.2` | 33,827,017 | 426 |

`9.4.0`, `9.3.5`, `9.2.3` are exactly the design-doc's proposed released set; `9.4.3` was added as the
release immediately preceding the `9.4.4-SNAPSHOT` adaptive branch point. `9.3.6` is **not** a
published release (the Maven copy errored — snapshot-only), so it was skipped. All four predate the v3
`commit_mode` field (which, per the design doc, has never shipped in a release).

## Method

Because a Maven `questdb` jar is a library, the "old reader path" is exercised directly (as SP-E's
design allows): a small probe is **compiled against, and run on, each old jar** so there is zero
API-drift risk — the SQL/reader API is byte-identical across 9.2.3…9.4.0 (verified: the 5-arg
`SqlExecutionContextImpl.with(...)`, `SqlCompilerImpl(engine)`, `compile(...)`,
`CompiledQuery.getRecordCursorFactory()`, `getFactoryProvider().getSecurityContextFactory().getRootContext()`).

- `spe.OpenProbe` (db `t`): opens `CairoEngine` on the db root → `engine.getReader("t")` (forces reads
  of `_meta`+commit_mode, `_txn`+CRC trailer, `_cv`+CRC trailer, and proves the table-dir scan does not
  choke on the extra `_snapshot`/`.epoch` files) → then a full-scan aggregate
  `select count(), sum(id), sum(v), min(ts), max(ts), count_distinct(s) from t` that reads every row of
  every column file. Asserts the exact deterministic oracle.
- `spe.ReadProbe` (db `cm`): generic open + `count()`/`sum(v)`.
- Each old core runs against a **fresh `cp -a` copy** of the pristine db (`/tmp/spe-open-<ver>`,
  `/tmp/spe-cmopen-<ver>`); the pristine dbs are never opened by an old binary, and a full-tree md5
  before/after each run reports whether the old core mutated anything.

**Oracle** (db `t`, from the current adaptive core reading its own db) — every old core must reproduce
these to the bit:

```
READER_OPEN_OK size=20000 partitions=1 columns=4
QUERY_OK count=20000 sum_id=199990000 sum_v=530860607842390000 min_ts=1704067200000000 max_ts=1704087199000000 distinct_s=4
```

(`sum_v` overflows int64; two's-complement addition is associative, so the wrapped value is
deterministic regardless of scan parallelism.)

## Results — real output

### DB `t` (20 000-row adaptive/WAL table, full epoch + CRC artifacts)

**9.4.3** — no migration, nothing mutated:
```
=== OpenProbe tag=9.4.3 root=/tmp/spe-open-9.4.3 ===
READER_OPEN_OK size=20000 partitions=1 columns=4 metadataVersion=0
QUERY_OK count=20000 sum_id=199990000 sum_v=530860607842390000 min_ts=1704067200000000 max_ts=1704087199000000 distinct_s=4
RESULT=PASS
PROBE_EXIT=0
DB_MUTATED=no
```

**9.2.3** (oldest core) — opens cleanly; runs its engine migration; artifacts untouched:
```
2026-... i.q.c.CairoEngine loaded 973 functions
2026-... i.q.c.m.EngineMigration upgrading database [version=427]
2026-... i.q.c.m.EngineMigration upgrading [path=/tmp/spe-open-9.2.3/t~1, fromVersion=426, toVersion=427]
2026-... i.q.c.m.EngineMigration upgraded tables to 427
READER_OPEN_OK size=20000 partitions=1 columns=4 metadataVersion=0
QUERY_OK count=20000 sum_id=199990000 sum_v=530860607842390000 min_ts=1704067200000000 max_ts=1704087199000000 distinct_s=4
RESULT=PASS
PROBE_EXIT=0
DB_MUTATED=yes        # (only ./_upgrade.d — see below)
```

**9.4.0** and **9.3.5** — identical `QUERY_OK`/`RESULT=PASS`. 9.4.0 additionally logs an engine
migration `fromVersion=426, toVersion=428` and a `generating parquet metadata files [.../t~1/_txn]`
step, which was a **no-op** for this native-partition table (no file added or changed — see integrity
diff). 9.3.5 logs `upgrading database [version=426] … upgraded tables to 426`.

### DB `cm` (meta commit_mode field = ADAPTIVE(3))

All four cores: `READER_OPEN_OK size=3 columns=2` → `QUERY_OK count=3 sum_v=6` → `RESULT=PASS`. e.g.
9.2.3:
```
=== ReadProbe tag=9.2.3 table=cm root=/tmp/spe-cmopen-9.2.3 ===
2026-... i.q.c.m.EngineMigration upgrading [path=/tmp/spe-cmopen-9.2.3/cm~1, fromVersion=426, toVersion=427]
READER_OPEN_OK size=3 columns=2
QUERY_OK count=3 sum_v=6
RESULT=PASS
```
A pre-v3 reader reads the populated `commit_mode=ADAPTIVE` field's table without choking — the field
(and the whole v3 meta tail) is inert.

## Artifact integrity after old-core open

Full-tree diff of each old-core copy vs. the pristine db:

| old core | files added | files removed | files changed | `_meta`/`_txn`/`_cv`/`_snapshot`/`_txn.epoch`/`_cv.epoch` |
|---|---|---|---|---|
| 9.4.3 | none | none | **none** | all SAME (byte-identical) |
| 9.4.0 | none | none | `./_upgrade.d` | all SAME |
| 9.3.5 | none | none | `./_upgrade.d` | all SAME |
| 9.2.3 | none | none | `./_upgrade.d` | all SAME |

**The only mutation is the root `_upgrade.d` marker.** Its layout is `[int ColumnType.VERSION | int
engine migrationVersion]`. The pristine adaptive db is `[426 | 429]`. An old core reads the table
format (426 — which it fully understands, so **no table migration**: `_meta` stays byte-identical) and
re-stamps *only* the engine `migrationVersion` at offset 4 to its own level:

| core | engine migrationVersion it stamps | `_upgrade.d` offset-4 |
|---|---|---|
| adaptive (writer) | 429 | 429 (pristine) |
| 9.4.3 | 429 | 429 → unchanged (`DB_MUTATED=no`) |
| 9.4.0 | 428 | 429 → 428 |
| 9.2.3 | 427 | 429 → 427 |
| 9.3.5 | 426 | 429 → 426 |

This is QuestDB's normal engine-migration bookkeeping on a version change, independent of adaptive; it
never touches a table artifact. The "`EngineMigration upgrading … fromVersion=426 toVersion=427/428`"
log lines refer to this **engine** counter, *not* the table `_meta` format version (which is 426 in
every core and was not rewritten). On a real downgrade the operator's data dir would get this same
harmless re-stamp; re-upgrading to the adaptive core simply re-runs the engine migrations forward
(idempotent). Our test ran on copies, so `/tmp/spe-adaptive-db` and `/tmp/spe-adaptive-cm` remain
pristine.

## Relationship to the in-code proof

`AdaptiveUpgradeCompatTest` (in-process) proves the *gate* behind each artifact:
`testMetaCommitModeFieldIsInertOnPreV3Meta` (v3 field A/B-gated to UNSET on a pre-v3 meta),
`testStraySnapshotAndEpochArtifactsAreInertOnNormalOpen`, and the two downgrade tests; the `_event`
/`_txn`/`_cv` CRC trailers are cited to `WalEventChecksumTest` / `TxnTest` / `ColumnVersionWriterTest`.
This document is the **external confirmation** the design explicitly deferred ("the `{old-binary}`
column itself is the external protocol"): on real 9.2.3/9.3.5/9.4.0/9.4.3 binaries, the gates hold and
the whole adaptive artifact set is inert. In-code gate + external real-binary run now agree.

## Gap: the fork-point binary

The design doc names `9.4.4-SNAPSHOT @ cdb0ea073b` (the pre-adaptive master fork point, meta **v2**) as
"the most important old binary." It was **not** tested here: it is an *unpublished* SNAPSHOT — the only
`9.4.4-SNAPSHOT` in the local Maven repo is the *adaptive* build itself — so it cannot be fetched and
would require a source build:

```
git worktree add /tmp/spe-fork cdb0ea073b && cd /tmp/spe-fork
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -q -pl core -am -DskipTests package
# then run spe.OpenProbe / spe.ReadProbe against a copy of the adaptive db with that jar
```

This gap is low-risk: the four released binaries tested are *older* meta-minor-version readers than the
v2 fork point (they predate the v2 TABLE_FORMAT field too, per the design doc), and they opened the v3
db cleanly — an even less-aware reader passing is a strictly stronger result than the v2 fork point
would give.

## Reproduce (operator protocol)

1. Write the adaptive db with the current core (lean classpath):
   ```
   export JAVA_TOOL_OPTIONS="--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
   CP="benchmarks/target/classes:$(cat benchmarks/target/bench-cp.txt)"
   java -DcommitMode=adaptive -Dgroup.window.us=0 -Dmax.rows=20000 -cp "$CP" org.questdb.CrashIngestWriter <db-root>
   ```
   Confirm `find <db-root> -name _snapshot -o -name '*.epoch'` lists the trio and `_meta` offset-41
   high short is 3 (meta v3).
2. Fetch each old release: `mvn …:copy -Dartifact=org.questdb:questdb:<ver> -DoutputDirectory=<dir>`.
3. For each old jar, **on a copy of the db** (`cp -a`, never the original — an old core re-stamps
   `_upgrade.d`), open + read with that jar's `CairoEngine`/`TableReader` + a full-scan aggregate, and
   assert it opens, reads, and returns the expected values with no error naming `_snapshot`, `.epoch`,
   `commit_mode`, or a bad `_txn`/`_cv`.
4. Diff the copy vs. the original: expect *no* files added/removed and only `_upgrade.d` changed.

**PASS** = every old core opens + reads cleanly. Observed: 4/4 released cores PASS on both dbs.

## Scope / cleanup

Validation only — no production-code change (SP-E verifies gates that already exist). Probe sources and
scratch dbs lived under `/tmp/spe-*` and are removed after the run; nothing was written under `/data`
or `benchmarks/target`. Enterprise inherits the gates via the submodule with no ent-side change.
