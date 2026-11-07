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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzConvertPartitionToParquetOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Generator-coverage pin for the replace-over-Parquet crash fuzzing (CLAUDE.md #11).
 * <p>
 * {@link RandomizedAdaptiveCrashFuzzTest#configureFuzz()} keeps {@code replaceInsert = 0.0} in the shared adaptive
 * crash sweep, so that sweep never draws a replace-range commit over a converted Parquet partition. This pin drives
 * the SAME full-library config through the extracted statics
 * ({@link RandomizedAdaptiveCrashFuzzTest#setFullLibraryFuzzProbabilities} /
 * {@link RandomizedAdaptiveCrashFuzzTest#setFullLibrarySweepFuzzCounts}) with replace enabled at 0.1 and asserts the
 * generator actually EMITS the shape: a {@link FuzzConvertPartitionToParquetOperation} with cutoff {@code C} followed
 * by a later transaction whose replace range overlaps the converted day window {@code (C - 1 day, C)}.
 * <p>
 * It fails first at {@code replaceInsert = 0.0} (no transaction carries a replace range, so the count is exactly 0)
 * and passes at 0.1 (the generator draws the shape). Because it mirrors {@code configureFuzz()} through the same
 * extracted statics, it genuinely guards the real config: if a future change stops the generator emitting a replace
 * range overlapping a converted partition, the count drops back to 0 at 0.1 and this pin goes red.
 */
public class ReplaceOverParquetShapeCoverageTest extends AbstractCairoTest {

    // Fixed seed: a generator-coverage pin must be reproducible, so a run that finds no shape reproduces.
    private static final long SEED0 = 0x5f3759dfL;
    private static final long SEED1 = 0x9e3779b9L;
    private static final String SHAPE_TABLE = "rop_shapes";
    private static final int WORKLOADS = 1500;

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

    @Test
    public void testGeneratorEmitsReplaceOverParquetShapes() throws Exception {
        assertMemoryLeak(() -> {
            // Fail-first: with replace disabled (exactly configureFuzz()'s shared setting) NO workload carries a
            // replace range, so the generator cannot emit a single replace-over-Parquet shape.
            final int disabled = countReplaceOverParquetShapes(0.0);
            Assert.assertEquals("replace disabled (0.0) must emit zero replace-over-Parquet shapes", 0, disabled);

            // With replace enabled at 0.1 the generator draws replace-over-Parquet shapes. The test asserts presence,
            // not a count: the count is the generator's own output for the fixed seed and moves with any change to its
            // draws, while presence is what lets a sweep with replace enabled fuzz the shape. Every seed tried gave 38
            // or more shapes of WORKLOADS, so the count reaches 0 only when the shape becomes rare or unreachable.
            final int enabled = countReplaceOverParquetShapes(0.1);
            LOG.info().$("[rop-shapes] replace-over-parquet shapes at 0.1: ").$(enabled).$(" of ").$(WORKLOADS).$();
            Assert.assertTrue("replace enabled (0.1) must emit replace-over-Parquet shapes, got " + enabled, enabled > 0);
        });
    }

    // Generates WORKLOADS full-library workloads with replaceInsert at replaceInsertProb and counts those that carry a
    // convert-to-Parquet followed by a later overlapping replace range. Deterministic: one master Rnd on the fixed seed
    // seeds every workload's generation.
    private int countReplaceOverParquetShapes(double replaceInsertProb) throws SqlException {
        fuzzer.clearSeeds();
        RandomizedAdaptiveCrashFuzzTest.setFullLibraryFuzzProbabilities(fuzzer, replaceInsertProb);
        RandomizedAdaptiveCrashFuzzTest.setFullLibrarySweepFuzzCounts(fuzzer);

        execute("drop table if exists " + SHAPE_TABLE);
        fuzzer.createInitialTableWal(SHAPE_TABLE, 0);
        drainWalQueue();

        final TableToken token = engine.verifyTableName(SHAPE_TABLE);
        final long oneDay;
        try (TableMetadata md = engine.getTableMetadata(token)) {
            oneDay = ColumnType.getTimestampDriver(md.getTimestampType()).fromDays(1);
        }

        final Rnd master = new Rnd(SEED0, SEED1);
        int shapes = 0;
        for (int i = 0; i < WORKLOADS; i++) {
            final ObjList<FuzzTransaction> txns = fuzzer.generateTransactions(SHAPE_TABLE, new Rnd(master.nextLong(), master.nextLong()));
            try {
                if (hasReplaceOverParquetShape(txns, oneDay)) {
                    shapes++;
                }
            } finally {
                Misc.freeObjListAndClear(txns);
            }
        }
        return shapes;
    }

    // True if some transaction converts a day to Parquet with cutoff C and a strictly later transaction commits a
    // replace range [lo, hi) overlapping the converted day window (C - oneDay, C): lo < C and hi > C - oneDay.
    private static boolean hasReplaceOverParquetShape(ObjList<FuzzTransaction> txns, long oneDay) {
        boolean sawConvert = false;
        for (int t = 0, n = txns.size(); t < n; t++) {
            final FuzzTransaction txn = txns.getQuick(t);
            // A replace in THIS transaction counts only against a convert in an EARLIER one.
            if (sawConvert && txn.hasReplaceRange()) {
                final long lo = txn.getReplaceLoTs();
                final long hi = txn.getReplaceHiTs();
                for (int t2 = 0; t2 < t; t2++) {
                    final long cutoff = convertCutoff(txns.getQuick(t2));
                    // Overlap of [lo, hi) with the converted day window (cutoff - oneDay, cutoff).
                    if (cutoff != Long.MIN_VALUE && lo < cutoff && hi > cutoff - oneDay) {
                        return true;
                    }
                }
            }
            if (convertCutoff(txn) != Long.MIN_VALUE) {
                sawConvert = true;
            }
        }
        return false;
    }

    // The convert-to-Parquet cutoff of a transaction, or Long.MIN_VALUE if it does not convert a partition to Parquet.
    private static long convertCutoff(FuzzTransaction txn) {
        for (int i = 0, n = txn.operationList.size(); i < n; i++) {
            final FuzzTransactionOperation op = txn.operationList.getQuick(i);
            if (op instanceof FuzzConvertPartitionToParquetOperation) {
                return ((FuzzConvertPartitionToParquetOperation) op).getCutoffTimestamp();
            }
        }
        return Long.MIN_VALUE;
    }
}
