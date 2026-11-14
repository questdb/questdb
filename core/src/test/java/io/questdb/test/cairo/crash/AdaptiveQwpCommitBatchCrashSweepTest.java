/*+*****************************************************************************
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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.LongList;
import io.questdb.test.cutlass.websocket.QwpIngressTestConnection;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Sweeps a power loss across every durability op of two QWP commit batches of three tables, the second a group
 * window after the first, under ADAPTIVE with and without a group window. Each batch prepares its tables outside
 * the role-switch lock, sequences them under it, and runs a due group flush after the unlock. The prepare phase and
 * the flush loop catch what a failed flush throws, so the workload rethrows the crash the facade fired, which the
 * sweep then sees as propagated.
 * <p>
 * The oracle, for each table: each of its transactions is all or nothing, they survive as a prefix in commit order,
 * and recovery applies exactly the transactions the sequencer recorded, with the table not suspended: a sequencer
 * record that outlives its WAL rows fails it. Across the sweep, a later crash point never recovers fewer rows, the
 * first loses rows and the last recovers every row.
 */
public class AdaptiveQwpCommitBatchCrashSweepTest extends AbstractAdaptiveCrashSweepTest {
    private static final String[] TABLES = {"swa", "swb", "swc"};
    // Two transactions of two rows per table.
    private static final int ALL_ROWS = TABLES.length * 4;
    private static final long T0 = 1_000_000_000L;
    private static final long WINDOW_US = 50_000L;

    @Test
    public void testCommitBatchSweepRecoversWithGroupWindow() throws Exception {
        assertSweep(WINDOW_US);
    }

    @Test
    public void testCommitBatchSweepRecoversWithoutGroupWindow() throws Exception {
        assertSweep(0);
    }

    // Two one-row frames per table, interleaved: table t gets v = firstValue + t and firstValue + t + 3.
    private static byte[][] batchFrames(String[] tables, int firstValue) {
        final byte[][] frames = new byte[2 * tables.length][];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = QwpIngressTestConnection.oneRowFrame(tables[i % tables.length], firstValue + i, (firstValue + i) * 1_000_000L);
        }
        return frames;
    }

    private void assertSweep(long groupWindowUs) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        runWithCrashFacade(() -> {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertEquals(groupWindowUs, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
            final SweepResult r = forEachAdaptiveCrashPoint(new CommitBatchWorkload());
            final int[] recoveredByK = r.recoveredByK();
            LOG.info().$("[qwp commit batch sweep] W=").$(groupWindowUs).$(", N=").$(r.n)
                    .$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(recoveredByK)).$();
            Assert.assertFalse("the workload is small", r.truncated);
            Assert.assertEquals("the sweep covers every crash point", 1, r.firstSweptK);
            Assert.assertEquals("the sweep covers every crash point", r.sweptPoints, r.lastSweptK);
            for (int k = r.firstSweptK + 1; k <= r.lastSweptK; k++) {
                Assert.assertTrue("a later crash point recovers no fewer rows [k=" + k + ", recoveredByK="
                        + Arrays.toString(recoveredByK) + ']', recoveredByK[k] >= recoveredByK[k - 1]);
            }
            Assert.assertTrue("the first crash point loses rows [recoveredByK=" + Arrays.toString(recoveredByK) + ']',
                    recoveredByK[r.firstSweptK] < ALL_ROWS);
            Assert.assertEquals("the last crash point recovers every row [recoveredByK=" + Arrays.toString(recoveredByK) + ']',
                    ALL_ROWS, recoveredByK[r.lastSweptK]);
        });
    }

    private final class CommitBatchWorkload implements AdaptiveCrashWorkload {
        private String[] tables;

        @Override
        public void commit() throws Exception {
            final byte[][] first = batchFrames(tables, 1);
            final byte[][] second = batchFrames(tables, 11);
            final byte[][] frames = new byte[first.length + second.length][];
            System.arraycopy(first, 0, frames, 0, first.length);
            System.arraycopy(second, 0, frames, first.length, second.length);
            setCurrentMicros(T0);
            try (QwpIngressTestConnection connection = new QwpIngressTestConnection(engine, configuration, 4096, frames)) {
                connection.deliverFrames(first.length);
                rethrowFiredCrash();
                // a window later: the writers' group flush falls due in this batch
                setCurrentMicros(T0 + WINDOW_US);
                connection.deliverFrames(second.length);
            }
            rethrowFiredCrash();
        }

        @Override
        public int oracle(int k, int n) {
            int rows = 0;
            for (int t = 0; t < tables.length; t++) {
                final LongList values = readValues(tables[t]);
                final long lastTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t]));
                final String context = "k=" + k + " of " + n + ", table=" + tables[t] + ", values=" + values + ", lastTxn=" + lastTxn
                        + ", op=" + phaseDurabilityOp(k);
                Assert.assertFalse("the table is not suspended after recovery [" + context + ']',
                        engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tables[t])));
                Assert.assertEquals("a transaction is all or nothing [" + context + ']', 0, values.size() % 2);
                // A sequencer record without its rows means the record became durable before the WAL data.
                Assert.assertEquals("exactly the transactions the sequencer recorded [" + context + ']', 2 * lastTxn, values.size());
                // the transactions survive as a prefix: first batch's two rows, then the second's
                final long[] expected = {1 + t, 4 + t, 11 + t, 14 + t};
                Assert.assertTrue("at most both transactions [" + context + ']', values.size() <= expected.length);
                for (int i = 0, m = values.size(); i < m; i++) {
                    Assert.assertEquals("a prefix in commit order [" + context + ']', expected[i], values.getQuick(i));
                }
                rows += values.size();
            }
            return rows;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            tables = new String[TABLES.length];
            final TableToken[] tokens = new TableToken[TABLES.length];
            for (int t = 0; t < TABLES.length; t++) {
                tables[t] = TABLES[t] + iteration;
                execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                tokens[t] = engine.verifyTableName(tables[t]);
                // The writer opens before the commit phase; the pool hands it to the connection.
                try (WalWriter ignore = engine.getWalWriter(tokens[t])) {
                    Assert.assertFalse(ignore.isDistressed());
                }
            }
            return tokens;
        }

        private LongList readValues(String table) {
            final LongList values = new LongList();
            try (RecordCursorFactory f = select("SELECT v FROM " + table)) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    while (c.hasNext()) {
                        values.add(c.getRecord().getLong(0));
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            return values;
        }

        // The prepare phase and the flush loop catch a failed flush; a fired crash is rethrown here explicitly.
        private void rethrowFiredCrash() {
            final CrashSimulationError fired = crashFf.getFiredCrash();
            if (fired != null) {
                throw fired;
            }
        }
    }
}
