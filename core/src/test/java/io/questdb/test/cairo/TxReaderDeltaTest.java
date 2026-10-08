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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.getPartitionTableSizeOffset;

public class TxReaderDeltaTest extends AbstractCairoTest {
    @Test
    public void testAppendAndActiveFlag() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            setDelta(writer, 0);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertTrue(reader.hasAnyDelta());
            final long partitionVersion = reader.getPartitionTableVersion();

            // Extending the list reloads the former last partition too.
            writer.setPartitionHasDelta(0, false);
            appendPartition(writer, Micros.DAY_MICROS);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(partitionVersion, reader.getPartitionTableVersion());
            Assert.assertFalse(reader.hasAnyDelta());

            setDelta(writer, 1);
            commit(writer);
            for (int i = 0; i < 3; i++) {
                Assert.assertTrue(reader.unsafeLoadAll());
                Assert.assertEquals(partitionVersion, reader.getPartitionTableVersion());
                Assert.assertTrue(reader.hasAnyDelta());
            }

            writer.setPartitionHasDelta(1, false);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertFalse(reader.hasAnyDelta());
            Assert.assertFalse(writer.hasAnyDelta());
        });
    }

    @Test
    public void testCopyAndClear() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            setDelta(writer, 0);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            try (TxReader copy = new TxReader(configuration.getFilesFacade())) {
                copy.loadAllFrom(reader);
                Assert.assertTrue(copy.hasAnyDelta());

                writer.setPartitionHasDelta(0, false);
                commit(writer);
                Assert.assertTrue(reader.unsafeLoadAll());
                Assert.assertFalse(reader.hasAnyDelta());
                Assert.assertTrue(copy.hasAnyDelta());

                copy.loadAllFrom(reader);
                Assert.assertFalse(copy.hasAnyDelta());
                setDelta(writer, 0);
                copy.loadAllFrom(writer);
                Assert.assertTrue(copy.hasAnyDelta());
                copy.clear();
                Assert.assertFalse(copy.hasAnyDelta());
            }
        });
    }

    @Test
    public void testDeltaActiveCount() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            // DELTA_SWITCH sets delta-write mode before any Delta data exists. A repeated set counts once.
            writer.setPartitionDeltaActiveByTimestamp(0);
            writer.setPartitionDeltaActiveByTimestamp(0);
            commit(writer);
            Assert.assertTrue(writer.hasAnyDeltaActive());
            Assert.assertFalse(writer.hasAnyDelta());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(1, reader.getDeltaActiveCount());
            Assert.assertFalse(reader.hasAnyDelta());
            final long partitionVersion = reader.getPartitionTableVersion();

            // Extending the list reloads the former last partition.
            appendPartition(writer, Micros.DAY_MICROS);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(partitionVersion, reader.getPartitionTableVersion());
            Assert.assertEquals(1, reader.getDeltaActiveCount());

            // The last partition's offset-3 word refreshes without a version change.
            writer.setPartitionDeltaActiveByTimestamp(Micros.DAY_MICROS);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(partitionVersion, reader.getPartitionTableVersion());
            Assert.assertEquals(2, reader.getDeltaActiveCount());

            try (TxReader copy = new TxReader(configuration.getFilesFacade())) {
                copy.loadAllFrom(reader);
                Assert.assertTrue(copy.hasAnyDeltaActive());
                copy.clear();
                Assert.assertFalse(copy.hasAnyDeltaActive());
            }

            writer.removeAttachedPartitions(0);
            commit(writer);
            Assert.assertTrue(writer.hasAnyDeltaActive());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(1, reader.getDeltaActiveCount());

            writer.truncate(writer.getColumnVersion(), new ObjList<>());
            Assert.assertFalse(writer.hasAnyDeltaActive());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(0, reader.getDeltaActiveCount());
        });
    }

    @Test
    public void testFailedReload() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            setDelta(writer, 0);
            commit(writer);
            // Fail the version check after the partition records have been read.
            reader.versionReadsUntilFailure = 3;
            Assert.assertFalse(reader.unsafeLoadAll());
            Assert.assertFalse(reader.hasAnyDelta());
            Assert.assertFalse(reader.hasAnyDeltaActive());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertTrue(reader.hasAnyDelta());
            Assert.assertTrue(reader.hasAnyDeltaActive());
        });
    }

    @Test
    public void testFullReloadAndRemoval() throws Exception {
        withTxn((writer, reader, path) -> {
            for (int i = 0; i < 3; i++) {
                appendPartition(writer, i * Micros.DAY_MICROS);
            }
            setDelta(writer, 0);
            setDelta(writer, 1);
            setDelta(writer, 1);
            writer.bumpPartitionTableVersion();
            commit(writer);
            Assert.assertTrue(writer.hasAnyDelta());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertTrue(reader.hasAnyDelta());

            writer.removeAttachedPartitions(0);
            commit(writer);
            Assert.assertTrue(writer.hasAnyDelta());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertTrue(reader.hasAnyDelta());

            writer.removeAttachedPartitions(Micros.DAY_MICROS);
            commit(writer);
            Assert.assertFalse(writer.hasAnyDelta());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertFalse(reader.hasAnyDelta());

            setDelta(writer, 0);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertTrue(reader.hasAnyDelta());
            writer.truncate(writer.getColumnVersion(), new ObjList<>());
            Assert.assertFalse(writer.hasAnyDelta());
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertFalse(reader.hasAnyDelta());
        });
    }

    @Test
    public void testLegacyClearedFlag() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            Assert.assertTrue(reader.unsafeLoadAll());
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                // Older writers stored -1 in the partition's offset-3 word.
                mem.putLong(reader.getPartitionVersionOffset(), -1L);
                mem.close(false);
            }
            reader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertFalse(reader.getPartitionHasDelta(0));
            Assert.assertFalse(reader.hasAnyDelta());
        });
    }

    @Test
    public void testLookupDoesNotScan() throws Exception {
        withTxn((writer, reader, path) -> {
            for (int i = 0; i < 3; i++) {
                appendPartition(writer, i * Micros.DAY_MICROS);
            }
            Assert.assertTrue(reader.unsafeLoadAll());
            reader.isFlagReadAllowed = false;
            Assert.assertFalse(reader.hasAnyDelta());
            Assert.assertFalse(reader.hasAnyDeltaActive());
            reader.isFlagReadAllowed = true;

            setDelta(writer, 2);
            commit(writer);
            Assert.assertTrue(reader.unsafeLoadAll());
            reader.isFlagReadAllowed = false;
            Assert.assertTrue(reader.hasAnyDelta());
            Assert.assertTrue(reader.hasAnyDeltaActive());
        });
    }

    @Test
    public void testWriterReloadDropsActiveTail() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            // Delta-write mode without Delta data: only the delta-active count holds the tail.
            writer.updatePartitionSizeByTimestamp(Micros.DAY_MICROS, 1);
            writer.setPartitionDeltaActiveByTimestamp(Micros.DAY_MICROS);
            Assert.assertTrue(writer.hasAnyDeltaActive());
            Assert.assertFalse(writer.hasAnyDelta());

            Assert.assertTrue(writer.unsafeLoadAll());
            Assert.assertEquals(1, writer.getPartitionCount());
            Assert.assertFalse(writer.hasAnyDeltaActive());
        });
    }

    @Test
    public void testWriterReloadDropsTail() throws Exception {
        withTxn((writer, reader, path) -> {
            appendPartition(writer, 0);
            writer.updatePartitionSizeByTimestamp(Micros.DAY_MICROS, 1);
            setDelta(writer, 1);
            Assert.assertTrue(writer.hasAnyDelta());

            Assert.assertTrue(writer.unsafeLoadAll());
            Assert.assertEquals(1, writer.getPartitionCount());
            Assert.assertFalse(writer.hasAnyDelta());
        });
    }

    private static void appendPartition(TxWriter writer, long timestamp) {
        writer.updatePartitionSizeByTimestamp(timestamp, 1);
        writer.finishPartitionSizeUpdate(0, timestamp);
        commit(writer);
    }

    private static void commit(TxWriter writer) {
        writer.finishPartitionSizeUpdate();
        writer.commit(new ObjList<>());
    }

    private static void setDelta(TxWriter writer, int partitionIndex) {
        writer.setPartitionDeltaActiveByTimestamp(writer.getPartitionTimestampByIndex(partitionIndex));
        writer.setPartitionHasDelta(partitionIndex, true);
    }

    private void withTxn(DeltaTest test) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(engine.verifyTableName("x")).concat(TXN_FILE_NAME);
                try (
                        TxWriter writer = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                        TestReader reader = new TestReader(ff)
                ) {
                    reader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    Assert.assertTrue(reader.unsafeLoadAll());
                    Assert.assertFalse(reader.hasAnyDelta());
                    test.run(writer, reader, path);
                }
            }
        });
    }

    @FunctionalInterface
    private interface DeltaTest {
        void run(TxWriter writer, TestReader reader, Path path) throws Exception;
    }

    private static class TestReader extends TxReader {
        private boolean isFlagReadAllowed = true;
        private int versionReadsUntilFailure;

        private TestReader(FilesFacade ff) {
            super(ff);
        }

        @Override
        public boolean getPartitionHasDeltaByRawIndex(int indexRaw) {
            Assert.assertTrue("aggregate lookup must not scan partition flags", isFlagReadAllowed);
            return super.getPartitionHasDeltaByRawIndex(indexRaw);
        }

        @Override
        public boolean isPartitionDeltaActiveByRawIndex(int indexRaw) {
            Assert.assertTrue("aggregate lookup must not scan partition flags", isFlagReadAllowed);
            return super.isPartitionDeltaActiveByRawIndex(indexRaw);
        }

        @Override
        public long unsafeReadVersion() {
            final long version = super.unsafeReadVersion();
            if (versionReadsUntilFailure > 0 && --versionReadsUntilFailure == 0) {
                return version + 1;
            }
            return version;
        }

        private int getDeltaActiveCount() {
            return partitionDeltaActiveCount;
        }

        private long getPartitionVersionOffset() {
            return getBaseOffset() + getPartitionTableSizeOffset(getSymbolColumnCount())
                    + Integer.BYTES + PARTITION_VERSION_OFFSET * Long.BYTES;
        }
    }
}
