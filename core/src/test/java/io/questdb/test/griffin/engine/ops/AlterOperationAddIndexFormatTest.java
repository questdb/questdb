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

package io.questdb.test.griffin.engine.ops;

import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.UpdateOperator;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Chars;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the WAL-event serialization layout for ADD INDEX with the
 * {@code replicaOnly} flag.
 *
 * <p>Layout (new, slot 2 = coverCount, slot 3 = replicaOnly):
 * <pre>extraInfo = [blockSize, indexType, coverCount, replicaOnly]</pre>
 * Covering column names live in {@code extraStrInfo} starting at index 1.
 *
 * <p>Backward-compat guarantee: a PRE-feature binary serialized
 * {@code [blockSize, indexType, coverCount]} (3 slots, no {@code replicaOnly}).
 * A NEW binary must decode {@code coverCount} from slot 2 correctly and
 * treat the absent slot 3 as {@code replicaOnly=false}.
 */
public class AlterOperationAddIndexFormatTest {

    private static final int BLOCK_SIZE = 256;
    private static final TableToken TABLE_TOKEN = new TableToken(
            "test_table", "test_table~1", null, 42, false, false, false
    );

    // -----------------------------------------------------------------------
    // Forward round-trip: serialize with new layout, deserialize, verify.
    // -----------------------------------------------------------------------

    @Test
    public void testNewLayoutPlainAddIndex_roundTrip() throws Exception {
        // Plain ADD INDEX: coverCount=0, replicaOnly=false.
        TestUtils.assertMemoryLeak(() -> {
            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofAddIndex(0, TABLE_TOKEN, TABLE_TOKEN.getTableId(),
                    "col1", BLOCK_SIZE, IndexType.BITMAP, null, false);
            AlterOperation src = builder.build();

            try (MemoryCARW mem = new MemoryCARWImpl(256, 1, MemoryTag.NATIVE_DEFAULT)) {
                src.serializeBody(mem);
                long size = mem.getAppendOffset();

                AlterOperation dst = new AlterOperation();
                dst.deserializeBody(mem, 0L, size);

                // Apply against a capturing stub and verify the decoded values.
                CapturingMetadataService svc = new CapturingMetadataService();
                dst.apply(svc, true);

                Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
                Assert.assertEquals("indexType", IndexType.BITMAP, svc.capturedIndexType);
                Assert.assertNull("coveringColumnNames should be null", svc.capturedCoveringNames);
                Assert.assertFalse("replicaOnly should be false", svc.capturedReplicaOnly);
            }
        });
    }

    @Test
    public void testNewLayoutReplicaOnlyAddIndex_roundTrip() throws Exception {
        // replicaOnly=true, coverCount=0.
        TestUtils.assertMemoryLeak(() -> {
            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofAddIndex(0, TABLE_TOKEN, TABLE_TOKEN.getTableId(),
                    "col1", BLOCK_SIZE, IndexType.BITMAP, null, true);
            AlterOperation src = builder.build();

            try (MemoryCARW mem = new MemoryCARWImpl(256, 1, MemoryTag.NATIVE_DEFAULT)) {
                src.serializeBody(mem);
                long size = mem.getAppendOffset();

                AlterOperation dst = new AlterOperation();
                dst.deserializeBody(mem, 0L, size);

                CapturingMetadataService svc = new CapturingMetadataService();
                dst.apply(svc, true);

                Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
                Assert.assertEquals("indexType", IndexType.BITMAP, svc.capturedIndexType);
                Assert.assertNull("coveringColumnNames should be null", svc.capturedCoveringNames);
                Assert.assertTrue("replicaOnly should be true", svc.capturedReplicaOnly);
            }
        });
    }

    @Test
    public void testNewLayoutCoveringReplicaOnlyAddIndex_builderPath() throws Exception {
        // Covering ADD INDEX with replicaOnly=true, via the builder (ObjCharSequenceList path).
        // This exercises the non-deserialized path where strings are real distinct objects.
        TestUtils.assertMemoryLeak(() -> {
            ObjList<CharSequence> coverNames = new ObjList<>();
            coverNames.add("a");
            coverNames.add("b");

            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofAddIndex(0, TABLE_TOKEN, TABLE_TOKEN.getTableId(),
                    "col1", BLOCK_SIZE, IndexType.POSTING, coverNames, true);
            AlterOperation src = builder.build();

            CapturingMetadataService svc = new CapturingMetadataService();
            src.apply(svc, true);

            Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
            Assert.assertEquals("indexType", IndexType.POSTING, svc.capturedIndexType);
            Assert.assertNotNull("coveringColumnNames", svc.capturedCoveringNames);
            Assert.assertEquals("coverCount", 2, svc.capturedCoveringNames.size());
            Assert.assertEquals("cover[0]", "a", svc.capturedCoveringNames.get(0).toString());
            Assert.assertEquals("cover[1]", "b", svc.capturedCoveringNames.get(1).toString());
            Assert.assertTrue("replicaOnly should be true", svc.capturedReplicaOnly);
        });
    }

    @Test
    public void testNewLayoutCoveringReplicaOnlyAddIndex_deserializePath() throws Exception {
        // Covering ADD INDEX with replicaOnly=true: serialize+deserialize and assert
        // coverCount and replicaOnly are decoded correctly. Name identity is not
        // asserted here because DirectCharSequenceList returns a shared DirectString
        // (pre-existing behavior); name correctness is covered by the builder-path test.
        TestUtils.assertMemoryLeak(() -> {
            ObjList<CharSequence> coverNames = new ObjList<>();
            coverNames.add("a");
            coverNames.add("b");

            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofAddIndex(0, TABLE_TOKEN, TABLE_TOKEN.getTableId(),
                    "col1", BLOCK_SIZE, IndexType.POSTING, coverNames, true);
            AlterOperation src = builder.build();

            try (MemoryCARW mem = new MemoryCARWImpl(512, 1, MemoryTag.NATIVE_DEFAULT)) {
                src.serializeBody(mem);
                long size = mem.getAppendOffset();

                AlterOperation dst = new AlterOperation();
                dst.deserializeBody(mem, 0L, size);

                CapturingMetadataService svc = new CapturingMetadataService();
                dst.apply(svc, true);

                Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
                Assert.assertEquals("indexType", IndexType.POSTING, svc.capturedIndexType);
                Assert.assertNotNull("coveringColumnNames", svc.capturedCoveringNames);
                Assert.assertEquals("coverCount must be 2 from new slot 2", 2, svc.capturedCoveringNames.size());
                Assert.assertTrue("replicaOnly should be true from new slot 3", svc.capturedReplicaOnly);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Backward-compat: simulate a PRE-feature event (old 3-slot layout).
    // The OLD binary wrote [blockSize, indexType, coverCount] into extraInfo
    // and the covering names into extraStrInfo after the index column name.
    // The NEW binary must decode this as coverCount=<correct> and replicaOnly=false.
    // -----------------------------------------------------------------------

    @Test
    public void testBackwardCompatOldLayout_plainAddIndex() throws Exception {
        // Old event: [blockSize, indexType, 0]  (coverCount=0, no replicaOnly slot)
        TestUtils.assertMemoryLeak(() -> {
            LongList extraInfo = new LongList();
            extraInfo.add(BLOCK_SIZE);           // slot 0: blockSize
            extraInfo.add(IndexType.BITMAP);     // slot 1: indexType
            extraInfo.add(0);                    // slot 2: coverCount=0 (old layout)
            // NO slot 3 — replicaOnly absent in old format

            ObjList<CharSequence> extraStrInfo = new ObjList<>();
            extraStrInfo.add("col1");            // index 0: column name

            AlterOperation op = buildOldLayoutOp(extraInfo, extraStrInfo);
            CapturingMetadataService svc = new CapturingMetadataService();
            op.apply(svc, true);

            Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
            Assert.assertEquals("indexType", IndexType.BITMAP, svc.capturedIndexType);
            Assert.assertNull("coveringColumnNames should be null for coverCount=0", svc.capturedCoveringNames);
            Assert.assertFalse("replicaOnly must be false for old 3-slot event", svc.capturedReplicaOnly);
        });
    }

    @Test
    public void testBackwardCompatOldLayout_coveringAddIndex() throws Exception {
        // Old event: [blockSize, indexType, 2] + covering names in extraStrInfo.
        // This is the bug scenario: coverCount=2 at slot 2. The new binary must
        // read coverCount=2 (not interpret it as replicaOnly=true), and
        // replicaOnly=false (slot 3 absent => default false).
        TestUtils.assertMemoryLeak(() -> {
            LongList extraInfo = new LongList();
            extraInfo.add(BLOCK_SIZE);           // slot 0: blockSize
            extraInfo.add(IndexType.POSTING);    // slot 1: indexType
            extraInfo.add(2);                    // slot 2: coverCount=2 (old layout)
            // NO slot 3 — replicaOnly absent in old format

            ObjList<CharSequence> extraStrInfo = new ObjList<>();
            extraStrInfo.add("col1");            // index 0: column name
            extraStrInfo.add("cov_a");           // index 1: covering column 0
            extraStrInfo.add("cov_b");           // index 2: covering column 1

            AlterOperation op = buildOldLayoutOp(extraInfo, extraStrInfo);
            CapturingMetadataService svc = new CapturingMetadataService();
            op.apply(svc, true);

            Assert.assertEquals("blockSize", BLOCK_SIZE, svc.capturedBlockSize);
            Assert.assertEquals("indexType", IndexType.POSTING, svc.capturedIndexType);
            Assert.assertNotNull("coveringColumnNames should not be null", svc.capturedCoveringNames);
            Assert.assertEquals("coverCount must be 2 from old slot 2", 2, svc.capturedCoveringNames.size());
            Assert.assertEquals("cover[0]", "cov_a", Chars.toString(svc.capturedCoveringNames.get(0)));
            Assert.assertEquals("cover[1]", "cov_b", Chars.toString(svc.capturedCoveringNames.get(1)));
            // THE KEY ASSERTION: the old event's slot-2 value (coverCount=2) must NOT
            // be misread as replicaOnly=true. The absent slot 3 must decode as false.
            Assert.assertFalse("replicaOnly must be false (slot 3 absent in old event)", svc.capturedReplicaOnly);
        });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Constructs an {@link AlterOperation} with ADD_INDEX command directly from
     * pre-built {@code extraInfo}/{@code extraStrInfo} lists, bypassing the
     * builder. This simulates what {@code deserializeBody} produces when
     * replaying an event written by an old (pre-feature) binary.
     */
    private static AlterOperation buildOldLayoutOp(LongList extraInfo, ObjList<CharSequence> extraStrInfo) {
        AlterOperation op = new AlterOperation(extraInfo, extraStrInfo);
        op.of(
                io.questdb.tasks.TableWriterTask.CMD_ALTER_TABLE,
                AlterOperation.ADD_INDEX,
                TABLE_TOKEN,
                TABLE_TOKEN.getTableId(),
                0 /* tableNamePosition */
        );
        return op;
    }

    /**
     * Minimal {@link MetadataService} stub that captures the arguments passed to
     * {@link MetadataService#addIndex(CharSequence, int, byte, ObjList, boolean)}.
     * All other methods throw {@link UnsupportedOperationException}.
     */
    private static class CapturingMetadataService implements MetadataService {
        int capturedBlockSize = -1;
        byte capturedIndexType = -1;
        ObjList<CharSequence> capturedCoveringNames = null;
        boolean capturedReplicaOnly = false;

        @Override
        public void addColumn(CharSequence columnName, int columnType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, boolean isDedupKey, SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType) {
            throw new UnsupportedOperationException("use 5-arg addIndex");
        }

        @Override
        public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize, byte indexType,
                             @Nullable ObjList<CharSequence> coveringColumnNames, boolean replicaOnly) {
            this.capturedBlockSize = indexValueBlockSize;
            this.capturedIndexType = indexType;
            // Materialize strings immediately: in the deserialized path, entries in
            // coveringColumnNames are DirectString references that share a single
            // backing buffer; they become invalid once another getStrA() call is made.
            if (coveringColumnNames != null) {
                ObjList<CharSequence> copy = new ObjList<>(coveringColumnNames.size());
                for (int i = 0, n = coveringColumnNames.size(); i < n; i++) {
                    copy.add(Chars.toString(coveringColumnNames.get(i)));
                }
                this.capturedCoveringNames = copy;
            } else {
                this.capturedCoveringNames = null;
            }
            this.capturedReplicaOnly = replicaOnly;
        }

        @Override
        public AttachDetachStatus attachPartition(long partitionTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeCacheFlag(int columnIndex, boolean isCacheOn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeColumnType(CharSequence columnName, int newType, int symbolCapacity, boolean symbolCacheFlag, byte indexType, int indexValueBlockCapacity, boolean isSequential, SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeSymbolCapacity(CharSequence columnName, int symbolCapacity, SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean convertPartitionNativeToParquet(long partitionTimestamp, @Nullable CharSequence bloomFilterColumns, double bloomFilterFpp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean convertPartitionParquetToNative(long partitionTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttachDetachStatus detachPartition(long partitionTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disableDeduplication() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropIndex(@NotNull CharSequence columnName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceRemovePartitions(LongList partitionTimestamps) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMetaMaxUncommittedRows() {
            return 0;
        }

        @Override
        public int getMetaTableFormat() {
            return 0;
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return null;
        }

        @Override
        public int getPartitionBy() {
            return 0;
        }

        @Override
        public TableToken getTableToken() {
            return TABLE_TOKEN;
        }

        @Override
        public int getTtlHoursOrMonths() {
            return 0;
        }

        @Override
        public int getTimestampType() {
            return 0;
        }

        @Override
        public UpdateOperator getUpdateOperator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName, SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removePartition(long partitionTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setColumnParquetEncoding(CharSequence columnName, int parquetEncodingConfig) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMatViewRefresh(int refreshType, int timerInterval, char timerUnit, long timerStartUs, @Nullable CharSequence timerTimeZone, int periodLength, char periodLengthUnit, int periodDelay, char periodDelayUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMatViewRefreshLimit(int limitHoursOrMonths) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMatViewRefreshTimer(long startUs, int interval, char unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMetaMaxUncommittedRows(int maxUncommittedRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMetaO3MaxLag(long o3MaxLagUs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMetaTableFormat(int tableFormat) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMetaTtl(int ttlHoursOrMonths) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void squashPartitions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void tick() {
        }
    }
}
