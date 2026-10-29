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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.wal.WalUtils.*;

@RunWith(Parameterized.class)
public class WalTxnDetailsDeltaTest extends AbstractCairoTest {
    private final boolean isPartitioned;

    public WalTxnDetailsDeltaTest(boolean isPartitioned) {
        this.isPartitioned = isPartitioned;
    }

    @Parameterized.Parameters(name = "partitioned={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Test
    public void testControlKindsDoNotOpenWalFiles() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path journalPath = new Path();
                    Path tablePath = new Path();
                    TableTransactionLogFile log = newLog();
                    WalTxnDetails details = new WalTxnDetails(configuration, 1000)
            ) {
                createLog(journalPath, log);
                int[] walIds = {METADATA_WALID, DELTA_TABLE_SWITCH_WAL_ID, DROP_TABLE_WAL_ID};
                for (int i = 0; i < walIds.length; i++) {
                    log.addEntry(0, walIds[i], i + 1, 0, 100, 0, 0, 0);
                }
                tablePath.of(root);
                try (TransactionLogCursor cursor = log.getCursor(0, journalPath)) {
                    details.readObservableTxnMeta(tablePath, cursor, tablePath.size(), 0, Long.MIN_VALUE, Long.MAX_VALUE);
                }
                Assert.assertEquals(walIds.length, details.getLastSeqTxn());
                for (int i = 0; i < walIds.length; i++) {
                    long seqTxn = i + 1;
                    Assert.assertEquals(walIds[i], details.getWalId(seqTxn));
                    Assert.assertEquals(i + 1, details.getSegmentId(seqTxn));
                    Assert.assertEquals(WalTxnType.NONE, details.getWalTxnType(seqTxn));
                    Assert.assertEquals(WalTxnDetails.FORCE_FULL_COMMIT, details.getCommitToTimestamp(seqTxn));
                    Assert.assertEquals(0, details.getSegmentRowHi(seqTxn) - details.getSegmentRowLo(seqTxn));
                }
            }
        });
    }

    @Test
    public void testDeltaKindsFailApplyBeforeOpeningWalFiles() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, isPartitioned ? 2 : 0);
            for (int walId : new int[]{DELTA_TABLE_SWITCH_WAL_ID}) {
                String tableName = "delta_kind_" + -walId;
                execute("create table " + tableName + " (ts timestamp) timestamp(ts) partition by DAY WAL");
                TableToken token = engine.verifyTableName(tableName);
                engine.getTableSequencerAPI().releaseAll();
                try (Path path = new Path(); TableTransactionLogFile log = newLog()) {
                    path.of(root).concat(token).concat(SEQ_DIR);
                    log.open(path);
                    if (walId == DELTA_TABLE_SWITCH_WAL_ID) {
                        log.addDeltaTableSwitchEntry(0, 100);
                    } else {
                        log.addEntry(0, walId, 1, 0, 100, 0, 0, 0);
                    }
                }
                engine.notifyWalTxnCommitted(token);
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
                String errorMessage = engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage();
                TestUtils.assertContains(errorMessage, "unsupported sequencer transaction kind for WAL apply");
                TestUtils.assertContains(errorMessage, "walId=" + walId);
                try (TableWriter writer = getWriter(token)) {
                    Assert.assertEquals(0, writer.getAppliedSeqTxn());
                }
            }
        });
    }

    @Test
    public void testPackingPreservesKindsAndCoordinates() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    TableTransactionLogFile log = newLog();
                    DirectLongList packed = new DirectLongList(32, MemoryTag.NATIVE_DEFAULT)
            ) {
                createLog(path, log);
                int[] walIds = {7, 9, Integer.MAX_VALUE - 3, METADATA_WALID, 1,
                        Integer.MAX_VALUE - 4, DELTA_TABLE_SWITCH_WAL_ID, Integer.MAX_VALUE - 1, DROP_TABLE_WAL_ID, Integer.MAX_VALUE};
                for (int i = 0; i < walIds.length; i++) {
                    log.addEntry(0, walIds[i], i + 11, i + 21, 100, 0, 0, 0);
                }
                try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                    Assert.assertEquals(walIds.length, WalTxnDetails.loadTxns(cursor, walIds.length, packed));
                }
                boolean[] seen = new boolean[walIds.length];
                for (int i = 0; i < walIds.length; i++) {
                    long coordinates = packed.get(i * 2L);
                    long position = packed.get(i * 2L + 1);
                    if (i > 0) {
                        Assert.assertTrue(Long.compareUnsigned(packed.get((i - 1) * 2L), coordinates) <= 0);
                    }
                    int originalIndex = Numbers.decodeHighInt(position);
                    Assert.assertFalse(seen[originalIndex]);
                    seen[originalIndex] = true;
                    Assert.assertEquals(walIds[originalIndex], Numbers.decodeHighInt(coordinates) + MIN_WAL_ID);
                    Assert.assertEquals(originalIndex + 11, Numbers.decodeLowInt(coordinates));
                    Assert.assertEquals(originalIndex + 21, Numbers.decodeLowInt(position));
                }
            }
        });
    }

    @Test
    public void testSwitchForcesPreviousWalCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp) timestamp(ts) partition by DAY WAL");
            TableToken token = engine.verifyTableName("x");
            try (WalWriter writer = engine.getWalWriter(token)) {
                writer.newRow(200).append();
                writer.commit();
                writer.newRow(100).append();
                writer.commit();
            }
            try (
                    Path journalPath = new Path();
                    Path tablePath = new Path();
                    TableTransactionLogFile log = newLog();
                    WalTxnDetails details = new WalTxnDetails(configuration, 1000)
            ) {
                createLog(journalPath, log);
                try (TransactionLogCursor original = engine.getTableSequencerAPI().getCursor(token, 0)) {
                    copyNextEntry(original, log);
                    log.addDeltaTableSwitchEntry(0, 100);
                    copyNextEntry(original, log);
                }
                tablePath.of(root).concat(token);
                try (TransactionLogCursor cursor = log.getCursor(0, journalPath)) {
                    details.readObservableTxnMeta(tablePath, cursor, tablePath.size(), 0, Long.MIN_VALUE, Long.MAX_VALUE);
                }
                Assert.assertEquals(3, details.getLastSeqTxn());
                Assert.assertEquals(WalTxnDetails.FORCE_FULL_COMMIT, details.getCommitToTimestamp(1));
                Assert.assertEquals(WalTxnDetails.FORCE_FULL_COMMIT, details.getCommitToTimestamp(2));
                Assert.assertEquals(WalTxnType.DATA, details.getWalTxnType(1));
                Assert.assertEquals(WalTxnType.NONE, details.getWalTxnType(2));
                Assert.assertEquals(100, details.getMinTimestamp(3));
                Assert.assertEquals(1, details.calculateInsertTransactionBlock(1, TableWriterPressureControl.EMPTY, 1000, Long.MIN_VALUE));
            }
        });
    }

    @Test
    public void testUnknownKindsFailLookahead() throws Exception {
        assertMemoryLeak(() -> {
            for (int walId : new int[]{0, -5}) {
                try (
                        Path path = new Path();
                        Path tablePath = new Path();
                        TableTransactionLogFile log = newLog();
                        WalTxnDetails details = new WalTxnDetails(configuration, 1000)
                ) {
                    path.of(root).concat("unknown_" + -walId);
                    Assert.assertEquals(0, configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode()));
                    log.create(path, 100);
                    log.open(path);
                    log.addEntry(0, walId, 0, 0, 100, 0, 0, 0);
                    tablePath.of(root);
                    try (TransactionLogCursor cursor = log.getCursor(0, path)) {
                        CairoException error = Assert.assertThrows(CairoException.class,
                                () -> details.readObservableTxnMeta(tablePath, cursor, tablePath.size(), 0, Long.MIN_VALUE, Long.MAX_VALUE));
                        TestUtils.assertContains(error.getFlyweightMessage(), "unsupported sequencer transaction kind [walId=" + walId + ']');
                    }
                }
            }
        });
    }

    private static void copyNextEntry(TransactionLogCursor source, TableTransactionLogFile target) {
        Assert.assertTrue(source.hasNext());
        target.addEntry(source.getStructureVersion(), source.getWalId(), source.getSegmentId(), source.getSegmentTxn(), source.getCommitTimestamp(), 0, 0, 1);
    }

    private void createLog(Path path, TableTransactionLogFile log) {
        path.of(root).concat("journal");
        Assert.assertEquals(0, configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode()));
        log.create(path, 100);
        log.open(path);
    }

    private TableTransactionLogFile newLog() {
        return isPartitioned
                ? new TableTransactionLogV2(configuration, 2, DefaultWalDirectoryPolicy.INSTANCE)
                : new TableTransactionLogV1(configuration);
    }
}
