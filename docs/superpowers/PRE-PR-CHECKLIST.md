# Pre-PR checklist — composite partitioning branch

Written after a defect sat on this branch for weeks because every regression filter used here was
`Composite*`/`O3*`/`Wal*`/`MatView*`-shaped. Two commits changed `LATEST ON` metadata, broke seven
`griffin` tests, and nothing anyone ran would have shown it.

## 1. Run the SQL suite, not just the composite suites

```bash
export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
export QDB_TEST_TMPDIR=/dev/shm
mvn -o -pl core test -Dtest='io.questdb.test.griffin.**' -DfailIfNoTests=false
```

~24,500 tests, a few minutes. **Any read-side change belongs here**, because the interval cursors,
frame cursors and factory metadata this branch touches are used by *every filtered query*, not only
composite ones. The composite suites cannot see that blast radius by construction.

Then the storage-side sweep:

```bash
mvn -o -pl core test -DfailIfNoTests=false -Dtest='Composite*,Interval*,*IntervalTest,LatestBy*,\
O3*,Wal*,TableReaderTest,TableWriterTest,AlterTable*,*MatView*,LiveView*,SampleBy*,\
!SampleByConfigTest,!SampleByNanoTimestampConfigTest,!LiveViewMatViewDisabledTest,\
!LiveViewIngestRejectTest,!LiveViewRefreshDisabledTest,!MatViewReloadOnRestartTest,\
!WalTablesInitialisationTest,!AlterTableSetTypeRestartTest'
```

## 2. Know which failures are the machine, not the code

Suites that boot a `ServerMain` bind fixed ports (9000, 9003, 9090). If anything else on the box is
already listening — a containerised QuestDB, another worktree's server — they fail with
`could not bind socket`, which reads exactly like a product failure.

```bash
ss -ltnp | grep -E ':(9000|9003|9090)'   # check BEFORE blaming the code
```

Do not kill what you find; it is usually not yours. Exclude the suites (list above) or run on a
quiet box. Everything else in a run must be green.

## 3. Attribute a failure before fixing it

`git stash` is **not** a reliable way to test "is this mine?" in a worktree with uncommitted work —
it silently produced a false pass here. Use one of:

- `git show <commit>:<path> > <path>` to drop in an older version, then `git checkout HEAD -- <path>`
  to restore (exact, for committed files);
- `cp` the file aside and back;
- a detached worktree at the merge-base for a whole-tree comparison:
  `git worktree add --detach /tmp/mb $(git merge-base HEAD origin/master)`.

That is how the seven `griffin` failures were shown to pre-date this session's work and to post-date
the merge-base — i.e. introduced by the branch, not by the environment or by upstream.

## 4. A new test must be shown to fail

Every test added for a fix should be run **with the fix reverted**, and the result recorded in the
commit message. On this branch that discipline caught three tests of my own that passed against a
build with the defect still present.

Watch for the two ways a "backward scan" test silently is not one:

- `ORDER BY ts DESC, a DESC, b DESC` — a **multi-key** sort plans as a sort over a **forward** scan;
- `SELECT * FROM (... ORDER BY ts DESC) ORDER BY ts, ...` — an outer sort lets the optimiser **drop**
  the inner one.

Use a single sort key, project only `ts` so tied timestamps cannot make the comparison flap, and
assert the plan. `AbstractCompositeTwinTest` does all three; prefer it over hand-rolling.

## 5. Fuzz changes need a spread of seeds

Run the whole family (`-Dtest='Composite*'`), not one class: `CompositeFuzzUnstableTest` alone draws
far fewer random seeds, and a bad anti-vacuity floor only showed up suite-wide.

## Still outstanding

- IntelliJ java-lint formatter parity has never been run locally on this branch.
- OSS ↔ enterprise version coupling: a core version bump needs the companion ent branch.
