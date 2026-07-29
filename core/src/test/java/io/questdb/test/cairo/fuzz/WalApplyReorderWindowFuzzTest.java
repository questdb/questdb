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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WalApplyReorderWindowFuzzTest extends AbstractFuzzTest {

    @Test
    public void testWalApplyReorderWindowV1() throws Exception {
        runReorderWindowFuzz(false);
    }

    @Test
    public void testWalApplyReorderWindowV2() throws Exception {
        runReorderWindowFuzz(true);
    }

    private static void assertFullyDrained(int timerCount) {
        final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
        engine.getTableSequencerAPI().forAllWalTables(
                tableTokenBucket,
                true,
                (tableId, tableToken, lastTxn) -> {
                    final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
                    Assert.assertEquals(
                            "reorder state [table=" + tableToken.getTableName() + ']',
                            SeqTxnTracker.REORDER_NONE,
                            tracker.getReorderState()
                    );
                    Assert.assertNull(
                            "reorder timer [table=" + tableToken.getTableName() + ']',
                            tracker.getReorderTimer()
                    );
                    if (lastTxn > -1) {
                        Assert.assertTrue(
                                "WAL table not fully applied [table=" + tableToken.getTableName()
                                        + ", writerTxn=" + tracker.getWriterTxn()
                                        + ", seqTxn=" + lastTxn + ']',
                                tracker.getWriterTxn() >= lastTxn
                        );
                    }
                }
        );
        Assert.assertEquals(timerCount, engine.getTimerShards().size());
        Assert.assertEquals(
                0,
                TestUtils.getMetricValue(engine, "questdb_wal_apply_reorder_waiting_tables")
        );
    }

    private static void assertSequencerVersion(String tableName, int expectedVersion) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, 0)) {
            Assert.assertEquals(expectedVersion, cursor.getVersion());
        }
    }

    private void runReorderWindowFuzz(boolean useV2Sequencer) throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int parallelWalCount = 2 + rnd.nextInt(2);
        final int v2SeqPartTxnCount = 10 + rnd.nextInt(91);
        final long walApplyReorderWindowMicros = (1L + rnd.nextInt(100)) * 1_000L;

        setFuzzProbabilities(
                0.0,
                0.2,
                0.2,
                0.35,
                0.12,
                0.12,
                0.03,
                0.0,
                1.0,
                0.2,
                0.0,
                0.0,
                0.0,
                0.1,
                0.5,
                0.0,
                0.45,
                0.0,
                0.0
        );
        fuzzer.setFuzzCounts(
                true,
                2_000,
                250,
                16,
                12,
                64,
                100,
                4,
                parallelWalCount,
                0
        );
        setFuzzProperties(rnd);
        node1.setProperty(
                PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT,
                useV2Sequencer ? v2SeqPartTxnCount : 0
        );
        node1.setProperty(
                PropertyKey.CAIRO_WAL_APPLY_REORDER_WINDOW,
                walApplyReorderWindowMicros
        );

        LOG.info().$("WAL apply reorder fuzz configuration [windowMicros=")
                .$(walApplyReorderWindowMicros)
                .$(", seqPartTxnCount=")
                .$(useV2Sequencer ? v2SeqPartTxnCount : 0)
                .$(", walWriters=")
                .$(parallelWalCount)
                .I$();

        final int timerCount = engine.getTimerShards().size();
        runFuzz(rnd);
        drainWalQueue();

        final String tableNameBase = getTestName();
        final int expectedSequencerVersion = useV2Sequencer
                ? WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2
                : WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;
        assertSequencerVersion(tableNameBase + "_wal", expectedSequencerVersion);
        assertSequencerVersion(tableNameBase + "_wal_parallel", expectedSequencerVersion);
        assertFullyDrained(timerCount);
    }
}
