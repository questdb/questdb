/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Randomized adaptive crash-fuzz (SP-D increment D2). Builds on {@link AbstractAdaptiveCrashSweepTest}'s
 * exhaustive per-op sweep driver with a randomized/fuzzed workload harness (see
 * {@link io.questdb.test.cairo.fuzz.FuzzRunner} / {@link io.questdb.test.fuzz.FuzzTransaction}) — this
 * task (1 of 7) establishes only the base class and the two primitives every later task in this file
 * relies on: a canonical committed-state {@link #fingerprint} and a {@link #lastMatch} membership check
 * over a recorded fingerprint history.
 */
public class RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    @Before
    public void setUpFuzzer() {
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.clearSeeds();
    }

    @After
    public void tearDownFuzzer() {
        fuzzer.after();
    }

    // Default = full destructive op library; the machinery self-check (Task 3) flips this to run a
    // minimal insert/O3 profile. Field lives here; Task 3 only toggles it.
    private boolean fuzzOverrideMinimal = false;

    // Canonical committed-state fingerprint: full ordered dump to a String.
    private String fingerprint(String table) throws SqlException {
        StringSink fp = new StringSink();
        printSql("select * from " + table + " order by ts", fp);
        return fp.toString();
    }

    // Largest index whose recorded fingerprint equals `state`; -1 if none (conservative on coincident txns).
    private static int lastMatch(ObjList<String> history, CharSequence state) {
        for (int i = history.size() - 1; i >= 0; i--) {
            if (TestUtils.equals(history.getQuick(i), state)) {
                return i;
            }
        }
        return -1;
    }

    // Uses the 22-arg overload (FuzzRunner.java:757) — the ONLY one that enables partitionToParquet
    // (12th), partitionToNative (13th), setParquetEncoding (20th), and addCoveringIndex (22nd). The
    // 16-arg overload silently leaves those at 0, so parquet/covering-index would NOT be exercised.
    private void configureFuzz() {
        if (fuzzOverrideMinimal) {
            //   cancel notSet null  rollbk cAdd cRem cRen cTyp  data eqTs pDrop pPq  pNat trunc tDrop ttl  repl symV qry  pEnc tFmt cIdx
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,  0, 0, 0, 0,   0.6, 0.0, 0, 0,   0, 0, 0, 0,   0, 0, 0, 0,   0, 0);
        } else {
            fuzzer.setFuzzProbabilities(
                    0.05, 0.2, 0.05, 0.0,       // cancelRows, notSet, nullSet, rollback(=0: clean seqTxn map)
                    0.1, 0.05, 0.05, 0.05,      // colAdd, colRemove, colRename, colTypeChange
                    0.5, 0.0, 0.05, 0.03,       // dataAdd, equalTsRows(=0: canonical dump), partitionDrop, partitionToParquet
                    0.03, 0.05, 0.0, 0.05,      // partitionToNative, truncate, tableDrop(=0), setTtl
                    0.1, 0.0, 0.0, 0.02,        // replaceInsert(dedup), symbolAccessValidation, query, setParquetEncoding
                    0.0, 0.03);                 // setTableFormat(=0), addCoveringIndex
        }
        //                     isO3, fuzzRowCount, txns, strLen, symStrLen, symCount, initialRows=0, partitions
        fuzzer.setFuzzCounts(true, 200, 20, 4, 4, 4, 0, 3);
    }

    private ObjList<FuzzTransaction> generateTxns(Rnd rnd, String walTableName) throws Exception {
        configureFuzz();
        fuzzer.createInitialTableWal(walTableName, 0);   // 0 initial rows → deterministic (no nondeterministic data_temp seed)
        return fuzzer.generateTransactions(walTableName, rnd);
    }

    private ObjList<String> buildTwinFingerprints(String twinName, ObjList<FuzzTransaction> txns, Rnd applyRnd) throws Exception {
        fuzzer.createInitialTableWal(twinName, 0);
        ObjList<String> history = new ObjList<>();
        history.add(fingerprint(twinName));                          // fp[0] = empty
        final ObjList<FuzzTransaction> one = new ObjList<>();
        for (int i = 0, n = txns.size(); i < n; i++) {
            one.clear();
            one.add(txns.getQuick(i));
            fuzzer.applyToWal(one, twinName, 1, applyRnd);
            drainWalQueue();
            history.add(fingerprint(twinName));                      // fp[i+1] = state after txn i
        }
        execute("drop table " + twinName);                          // crash(dbRoot) must not see the twin
        return history;
    }

    @Test
    public void testFingerprintMembershipPrimitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fp (ts timestamp, v long) timestamp(ts) partition by day wal");
            ObjList<String> history = new ObjList<>();
            history.add(fingerprint("fp"));                          // fp[0] empty
            for (int i = 0; i < 3; i++) {
                execute("insert into fp values (" + (i * 1_000_000L) + ", " + i + ")");
                drainWalQueue();
                history.add(fingerprint("fp"));                      // fp[1..3]
            }
            // the current (full) state must match the last snapshot
            Assert.assertEquals(3, lastMatch(history, fingerprint("fp")));
            // an intermediate snapshot is found at its own index
            Assert.assertEquals(1, lastMatch(history, history.getQuick(1)));
            // a fabricated state matches nothing
            Assert.assertEquals(-1, lastMatch(history, "not a real dump"));

            // lastMatch must return the LARGEST matching index, not merely the first: append a second
            // snapshot coincident with history[1] at a higher index and confirm the match follows it
            // there (Tasks 5-7 rely on "largest match", not "first match").
            int coincidentIndex = history.size();
            history.add(history.getQuick(1));
            Assert.assertEquals(coincidentIndex, lastMatch(history, history.getQuick(1)));
        });
    }

    @Test
    public void testTwinFingerprintsDeterministic() throws Exception {
        assertMemoryLeak(() -> {
            final long s0 = 42L, s1 = 99L;
            ObjList<String> h1 = runTwinOnce("wal_a", "twin_a", s0, s1);
            ObjList<String> h2 = runTwinOnce("wal_b", "twin_b", s0, s1);
            Assert.assertEquals("fp history length must be deterministic", h1.size(), h2.size());
            for (int i = 0; i < h1.size(); i++) {
                Assert.assertTrue("fp[" + i + "] must be identical across two runs of the same seed",
                        TestUtils.equals(h1.getQuick(i), h2.getQuick(i)));
            }
            Assert.assertTrue("fp history must be non-trivial", h1.size() > 3);
        });
    }

    private ObjList<String> runTwinOnce(String walName, String twinName, long s0, long s1) throws Exception {
        Rnd genRnd = new Rnd(s0, s1);
        ObjList<FuzzTransaction> txns = generateTxns(genRnd, walName);
        return buildTwinFingerprints(twinName, txns, new Rnd(s0, s1));
    }
}
