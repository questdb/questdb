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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WalWriterIndexTest extends AbstractCairoTest {
    private final byte indexType;

    public WalWriterIndexTest(byte indexType) {
        this.indexType = indexType;
    }

    @Parameterized.Parameters(name = "index={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexType.BITMAP}, {IndexType.POSTING}
        });
    }

    @Test
    public void testAllowsLocalParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            drainWalQueue();
            addIndexedColumn(token);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    @Test
    public void testAllowsPendingDataAndUpdate() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            execute("INSERT INTO t VALUES (3, '2020-01-02T01:00:00')");
            execute("UPDATE t SET x = 42 WHERE x = 1");
            addIndexedColumn(token);
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("SELECT x FROM t WHERE x = 42").returns("x\n42\n");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    @Test
    public void testAllowsPendingSqlAlter() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            execute("ALTER TABLE t SET PARAM maxUncommittedRows = 10");
            addIndexedColumn(token);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
                Assert.assertTrue(metadata.isColumnIndexed(metadata.getColumnIndexQuiet("k")));
            }
        });
    }

    @Test
    public void testAllowsPendingStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            execute("ALTER TABLE t ADD COLUMN plain INT");
            addIndexedColumn(token);
            drainWalQueue();
            try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
                Assert.assertEquals(4, metadata.getColumnCount());
            }
        });
    }

    @Test
    public void testAllowsRemoteNativePartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            addIndexedColumn(token);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        });
    }

    @Test
    public void testRejectsDeltaPartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(0);
                final CairoException error = Assert.assertThrows(CairoException.class, () -> writer.addColumn(
                        "k", ColumnType.SYMBOL, configuration.getDefaultSymbolCapacity(),
                        configuration.getDefaultSymbolCacheFlag(), indexType, configuration.getIndexValueBlockSize(),
                        false, false, sqlExecutionContext.getSecurityContext()));
                Assert.assertEquals(CairoException.METADATA_VALIDATION_RECOVERABLE, error.getErrno());
                Assert.assertEquals(-1, writer.getMetadata().getColumnIndexQuiet("k"));
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            assertRejected(token, "cannot create index");
        });
    }

    @Test
    public void testRejectsIndexedColumnInList() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(0);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            final long sequenced = engine.getTableSequencerAPI().lastTxn(token);
            assertExceptionNoLeakCheck(
                    "ALTER TABLE t ADD COLUMN plain LONG, k SYMBOL INDEX TYPE " + (indexType == IndexType.BITMAP ? "BITMAP" : "POSTING"),
                    -1, "cannot create index", sqlExecutionContext
            );
            Assert.assertEquals(sequenced, engine.getTableSequencerAPI().lastTxn(token));
            try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("plain"));
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("k"));
            }
        });
    }

    @Test
    public void testRejectsRemoteParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            drainWalQueue();
            try (TableWriter writer = getWriter("t")) {
                writer.getTxWriter().setPartitionRemote(0, true);
                writer.getTxWriter().setPartitionParquetGenerated(0, false);
                writer.bumpPartitionTableVersion();
                writer.commit();
            }
            assertRejected(token, "cannot create index");
        });
    }

    private void addColumn(WalWriter writer, byte type) {
        writer.addColumn("k", ColumnType.SYMBOL, configuration.getDefaultSymbolCapacity(),
                configuration.getDefaultSymbolCacheFlag(), type, configuration.getIndexValueBlockSize(),
                false, sqlExecutionContext.getSecurityContext());
    }

    private void addIndexedColumn(TableToken token) {
        try (WalWriter writer = engine.getWalWriter(token)) {
            addColumn(writer, indexType);
        }
    }

    private void assertRejected(TableToken token, String message) {
        final long sequenced = engine.getTableSequencerAPI().lastTxn(token);
        final long version;
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
            version = metadata.getMetadataVersion();
        }
        try (WalWriter writer = engine.getWalWriter(token)) {
            assertRejected(writer, message);
        }
        Assert.assertEquals(sequenced, engine.getTableSequencerAPI().lastTxn(token));
        Assert.assertEquals(token, engine.getTableSequencerAPI().reload(token));
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
            Assert.assertEquals(version, metadata.getMetadataVersion());
            Assert.assertEquals(-1, metadata.getColumnIndexQuiet("k"));
        }
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
    }

    private void assertRejected(WalWriter writer, String message) {
        final CairoException error = Assert.assertThrows(CairoException.class, () -> addColumn(writer, indexType));
        Assert.assertEquals(CairoException.METADATA_VALIDATION_RECOVERABLE, error.getErrno());
        TestUtils.assertContains(error.getFlyweightMessage(), message);
        Assert.assertFalse(writer.isDistressed());
        Assert.assertEquals(-1, writer.getMetadata().getColumnIndexQuiet("k"));
    }

    private TableToken createTable() throws Exception {
        execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES (1, '1970-01-01T00:00:00'), (2, '2020-01-02T00:00:00')");
        drainWalQueue();
        return engine.verifyTableName("t");
    }
}
