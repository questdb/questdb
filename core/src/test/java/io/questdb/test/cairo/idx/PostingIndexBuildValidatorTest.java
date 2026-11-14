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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexBuildValidator;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.idx.PostingIndexUtils.*;

public class PostingIndexBuildValidatorTest extends AbstractCairoTest {
    @Test
    public void testAddIndexDropsLeftoverEntryAtCommittedTxn() throws Exception {
        // Merge-append WAL tables build the last partition's index through the writer that does not follow the column.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(() -> {
            assertAddIndexDropsLeftoverEntry("historic", "BYPASS WAL", 0, false);
            assertAddIndexDropsLeftoverEntry("last_followed", "BYPASS WAL", 1, false);
            assertAddIndexDropsLeftoverEntry("last_merge_append", "WAL", 1, false);
        });
    }

    @Test
    public void testAddIndexDropsLeftoverEntryOfParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            assertAddIndexDropsLeftoverEntry("parquet", "BYPASS WAL", 0, true);
            assertAddIndexDropsLeftoverEntry("parquet_wal", "WAL", 0, true);
        });
    }

    @Test
    public void testBitmapAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', '2024-01-01'), (NULL, '2024-01-01T01:00'), ('a', '2024-01-02')");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE BITMAP");
            assertQuery("SELECT ts FROM x WHERE s = 'a'").noLeakCheck().timestamp("ts")
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            2024-01-02T00:00:00.000000Z
                            """);
            assertQuery("SELECT ts FROM x WHERE s IS NULL").noLeakCheck().timestamp("ts")
                    .returns("ts\n2024-01-01T01:00:00.000000Z\n");
        });
    }

    @Test
    public void testCompleteCoveringIndex() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> assertComplete(reader, path, true));
    }

    @Test
    public void testCompleteCoveringIndexWithColumnVersions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, v INT, w VARCHAR)");
            execute("INSERT INTO x VALUES ('a', 1, 'one'), (NULL, 2, NULL), ('b', NULL, '🐘'), ('a', 4, '')");
            execute("ALTER TABLE x ALTER COLUMN s TYPE SYMBOL");
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v, w)");
            engine.releaseAllWriters();
            try (TableReader reader = getReader("x"); Path path = new Path(); ColumnVersionReader staleVersions = new ColumnVersionReader()) {
                path.of(configuration.getDbRoot()).concat(reader.getTableToken()).concat("default");
                final int columnIndex = reader.getMetadata().getColumnIndex("s");
                final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(0, reader.getMetadata().getWriterIndex(columnIndex));
                Assert.assertTrue(columnNameTxn > -1);
                assertComplete(reader, path, true);
                Assert.assertFalse(PostingIndexBuildValidator.isComplete(configuration.getFilesFacade(), path, "s", columnNameTxn,
                        reader.getTxn(), 4, reader.getMetadata().getColumnMetadata(columnIndex).getCoveringColumnIndices(), staleVersions, 0));
            }
        });
    }

    @Test
    public void testCompleteCoveringIndexWithNullsAndColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL)");
            execute("INSERT INTO x VALUES ('a'), (NULL)");
            execute("ALTER TABLE x ADD COLUMN v LONG, w VARCHAR");
            execute("INSERT INTO x VALUES ('b', NULL, '🐘'), ('a', 4, '')");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v, w)");
            engine.releaseAllWriters();
            try (TableReader reader = getReader("x"); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(reader.getTableToken()).concat("default");
                assertComplete(reader, path, true);
            }
            assertQuery("SELECT v, w FROM x WHERE s = 'a'").noLeakCheck().noRandomAccess().expectSize()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tw
                            null\t
                            4\t
                            """);
        });
    }

    @Test
    public void testCompletePlainIndex() throws Exception {
        final String[] encodings = {"", "DELTA", "EF"};
        for (int i = 0; i < encodings.length; i++) {
            withIndex("x" + i, encodings[i], (reader, path) -> assertComplete(reader, path, true));
        }
    }

    @Test
    public void testCompleteSingleNullRowAndEmptyIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, w VARCHAR)");
            execute("INSERT INTO x VALUES (NULL, NULL, NULL)");
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v, w)");
            engine.releaseAllWriters();
            try (TableReader reader = getReader("x"); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(reader.getTableToken()).concat("default");
                Assert.assertTrue(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 1));
            }
            execute("TRUNCATE TABLE x");
            engine.releaseAllWriters();
            try (TableReader reader = getReader("x"); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(reader.getTableToken()).concat("default");
                Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 0));
            }
        });
    }

    @Test
    public void testCoverInfoMismatch() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> {
            final int plen = path.size();
            // .pci layout: magic, cover count, then one covered column index per cover
            final long countOffset = Integer.BYTES;
            final long firstCoverOffset = 2L * Integer.BYTES;
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(configuration.getFilesFacade(), coverInfoFileName(path, "s", -1), MemoryTag.MMAP_DEFAULT);
                mem.jumpTo(mem.size());
                path.trimTo(plen);
                mem.putInt(0, 0);
                assertComplete(reader, path, false);
                mem.putInt(0, COVER_INFO_MAGIC);
                mem.putInt(countOffset, 1);
                assertComplete(reader, path, false);
                mem.putInt(countOffset, 2);
                mem.putInt(firstCoverOffset, 2);
                assertComplete(reader, path, false);
                mem.putInt(firstCoverOffset, 1);
                assertComplete(reader, path, true);
            }
        });
    }

    @Test
    public void testCoverInfoOpenFailure() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> {
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    return Utf8s.endsWithAscii(name, ".pci") ? -1 : super.openRO(name);
                }
            };
            Assert.assertFalse(isComplete(ff, reader, path, reader.getTxn(), 4));
            assertComplete(reader, path, true);
        });
    }

    @Test
    public void testDifferentCoveringDefinition() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> {
            IntList covers = new IntList();
            covers.add(2);
            covers.add(1);
            Assert.assertFalse(PostingIndexBuildValidator.isComplete(configuration.getFilesFacade(), path, "s", -1,
                    reader.getTxn(), 4, covers, reader.getColumnVersionReader(), 0));
            covers.clear();
            Assert.assertFalse(PostingIndexBuildValidator.isComplete(configuration.getFilesFacade(), path, "s", -1,
                    reader.getTxn(), 4, covers, reader.getColumnVersionReader(), 0));
        });
    }

    @Test
    public void testIncompleteFileSet() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            final int plen = path.size();
            final PostingIndexChainEntry.Snapshot head = readHead(path);
            try (Path file = new Path(); Path backup = new Path()) {
                for (int kind = 0; kind < 4; kind++) {
                    file.of(path);
                    long requiredSize = switch (kind) {
                        case 0 -> {
                            keyFileName(file, "s", -1);
                            yield KEY_FILE_RESERVED;
                        }
                        case 1 -> {
                            valueFileName(file, "s", -1, head.sealTxn);
                            yield head.valueMemSize;
                        }
                        case 2 -> {
                            coverInfoFileName(file, "s", -1);
                            // magic and cover count, then the two covered column indices
                            yield (2L + 2) * Integer.BYTES;
                        }
                        default -> {
                            coverDataFileName(file, "s", 1, -1, -1, head.sealTxn);
                            yield head.coverFileEndOffsets.getQuick(1);
                        }
                    };
                    backup.of(file).put(".backup");
                    Assert.assertEquals(0, ff.rename(file.$(), backup.$()));
                    assertComplete(reader, path, false);
                    Assert.assertTrue(ff.copy(backup.$(), file.$()) >= 0);
                    final long fd = ff.openRW(file.$(), 0);
                    Assert.assertTrue(fd >= 0);
                    try {
                        Assert.assertTrue(ff.truncate(fd, requiredSize - 1));
                        assertComplete(reader, path, false);
                        Assert.assertTrue(ff.truncate(fd, 0));
                        assertComplete(reader, path, false);
                    } finally {
                        ff.close(fd);
                    }
                    Assert.assertTrue(ff.removeQuiet(file.$()));
                    Assert.assertEquals(0, ff.rename(backup.$(), file.$()));
                    assertComplete(reader, path, true);
                    Assert.assertEquals(plen, path.size());
                }
            }
        });
    }

    @Test
    public void testInvalidEntryMetadata() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> withKey(path, (mem, header, head) -> {
            assertRejectedLong(reader, path, mem, head.offset + V2_ENTRY_OFFSET_LEN, head.len - 1);
            assertRejectedInt(reader, path, mem, head.offset + V2_ENTRY_OFFSET_GEN_COUNT, 0);
            assertRejectedInt(reader, path, mem, head.offset + V2_ENTRY_OFFSET_GEN_COUNT, 2);
            assertRejectedInt(reader, path, mem, head.offset + V2_ENTRY_OFFSET_COVERING_FORMAT, 2);
            assertRejectedInt(reader, path, mem, head.offset + V2_ENTRY_OFFSET_COVERING_FORMAT,
                    PostingIndexChainEntry.packCoveringFormat(COVERING_FORMAT_DEALIASED, 1));
            assertRejectedLong(reader, path, mem, head.offset + V2_ENTRY_OFFSET_SEAL_TXN, -1);
            assertRejectedLong(reader, path, mem, head.offset + V2_ENTRY_OFFSET_VALUE_MEM_SIZE, 0);
            assertRejectedInt(reader, path, mem, head.offset + V2_ENTRY_OFFSET_KEY_COUNT, 0);
            assertRejectedLong(reader, path, mem, head.genDirOffset + GEN_DIR_OFFSET_FILE_OFFSET, 1);
            assertRejectedLong(reader, path, mem, head.genDirOffset + GEN_DIR_OFFSET_SIZE, head.valueMemSize - 1);
            assertComplete(reader, path, true);
        }));
    }

    @Test
    public void testInvalidHeaderMetadata() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> withKey(path, (mem, header, head) -> {
            final long page = header.pageOffset;
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_FORMAT_VERSION, 0);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_REGION_BASE, KEY_FILE_RESERVED - 1);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET, V2_NO_HEAD);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_REGION_LIMIT, mem.size() + 1);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_REGION_LIMIT, Long.MIN_VALUE);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_REGION_LIMIT, head.offset + head.len - 1);
            assertRejectedLong(reader, path, mem, page + V2_HEADER_OFFSET_HEAD_ENTRY_OFFSET, Long.MAX_VALUE);
            assertComplete(reader, path, true);
            mem.putLong(PAGE_A_OFFSET + V2_HEADER_OFFSET_SEQUENCE_START, 1);
            mem.putLong(PAGE_B_OFFSET + V2_HEADER_OFFSET_SEQUENCE_START, 1);
            assertComplete(reader, path, false);
        }));
    }

    @Test
    public void testKeyMapFailure() throws Exception {
        withIndex("", (reader, path) -> {
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    return -1;
                }
            };
            Assert.assertFalse(isComplete(ff, reader, path, reader.getTxn(), 4));
            assertComplete(reader, path, true);
        });
    }

    @Test
    public void testKeyOpenFailure() throws Exception {
        withIndex("", (reader, path) -> {
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    return -1;
                }
            };
            Assert.assertFalse(isComplete(ff, reader, path, reader.getTxn(), 4));
            assertComplete(reader, path, true);
        });
    }

    @Test
    public void testLegacyCoveringEntry() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> withKey(path, (mem, header, head) -> {
            // Rewrite just the layout as a legacy writer would: gen-dir first, cover footer last.
            final long legacyGen = head.offset + V2_ENTRY_HEADER_SIZE;
            mem.putLong(legacyGen + GEN_DIR_OFFSET_FILE_OFFSET, 0);
            mem.putLong(legacyGen + GEN_DIR_OFFSET_SIZE, head.valueMemSize);
            mem.putInt(legacyGen + GEN_DIR_OFFSET_KEY_COUNT, head.keyCount);
            mem.putInt(legacyGen + GEN_DIR_OFFSET_MIN_KEY, 0);
            mem.putInt(legacyGen + GEN_DIR_OFFSET_MAX_KEY, head.keyCount - 1);
            mem.putLong(legacyGen + GEN_DIR_OFFSET_TXN_AT_SEAL, head.txnAtSeal);
            mem.putLong(legacyGen + GEN_DIR_OFFSET_MAX_VALUE, head.maxValue);
            PostingIndexChainEntry.writeHeader(mem, head.offset, head.sealTxn, head.txnAtSeal,
                    head.valueMemSize, head.maxValue, head.keyCount, 1, head.blockCapacity,
                    COVERING_FORMAT_LEGACY, head.prevEntryOffset, head.coverFileEndOffsets);
            mem.putLong(header.pageOffset + V2_HEADER_OFFSET_FORMAT_VERSION, V2_FORMAT_VERSION);
            assertComplete(reader, path, true);
        }));
    }

    @Test
    public void testOnlyHeaderMaxValueReachedEnd() throws Exception {
        // index() calls setMaxValue before seal() flushes the final pending rows. A crash in between
        // leaves the entry header advertising all rows, but its last published gen still covers a prefix.
        withIndex("", (reader, path) -> {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                writer.of(path, "s", -1, true);
                writer.setNextTxnAtSeal(reader.getTxn());
                writer.add(1, 0);
                writer.add(0, 1);
                writer.seal();
                writer.setMaxValue(3);
            }
            assertComplete(reader, path, false);
        });
    }

    @Test
    public void testPartialRows() throws Exception {
        withIndex("", (reader, path) -> {
            Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 5));
            Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 3));
            Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 0));
        });
    }

    @Test
    public void testRetryAfterCommitRebuildsInterruptedBuild() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 1);
        // Spill flushes grow every partition's leftover chain past the first page of its .pk. A rebuild that kept
        // the chain would truncate it, then read the chain's head from beyond the page truncate() leaves mapped.
        setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        final MetaSwapFailingFacade ff = new MetaSwapFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // three daily partitions of 10_000 rows, 'a' on even v and NULL on odd v
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a'::STRING ELSE NULL END, x, timestamp_sequence('2024-01-01', 8_640_000L)
                    FROM long_sequence(30_000)
                    """);
            engine.releaseAllWriters();
            final String sql = "ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)";
            assertAddIndexFailsAtMetadataSwap(ff, sql);
            engine.releaseAllWriters();
            // The commit moves the table to the txn the leftover entries carry, so they no longer look abandoned.
            execute("INSERT INTO x VALUES ('a', 30_002, '2024-01-03T23:59:59')");
            execute(sql);
            assertQuery("SELECT count(), sum(v) FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().withPlanContaining("CoveringIndex")
                    .returns("count\tsum\n15001\t225045002\n");
            assertQuery("SELECT count(), sum(v) FROM x WHERE s IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\n15000\t225000000\n");
        });
    }

    @Test
    public void testRetryRebuildsLeftoverChainTornByPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 1);
        final MetaSwapFailingFacade ff = new MetaSwapFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // three daily partitions of 10_000 rows, 'a' on even v and NULL on odd v
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a'::STRING ELSE NULL END, x, timestamp_sequence('2024-01-01', 8_640_000L)
                    FROM long_sequence(30_000)
                    """);
            engine.releaseAllWriters();
            final String sql = "ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)";
            assertAddIndexFailsAtMetadataSwap(ff, sql);
            engine.releaseAllWriters();
            // A power loss kept the header and the size of the .pk the build left in a historic and in the active
            // partition, but not the page their newest entries were on, which reads back as zeros.
            final long historicSealTxn = loseHeadEntryPage("2024-01-01");
            final long activeSealTxn = loseHeadEntryPage("2024-01-03");
            execute(sql);
            assertQuery("SELECT count(), sum(v) FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().withPlanContaining("CoveringIndex")
                    .returns("count\tsum\n15000\t225015000\n");
            assertQuery("SELECT count(), sum(v) FROM x WHERE s IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\n15000\t225000000\n");
            // The rebuilt chains start past every sealTxn the torn ones used.
            Assert.assertTrue(readPostingSealTxn("2024-01-01") > historicSealTxn);
            Assert.assertTrue(readPostingSealTxn("2024-01-03") > activeSealTxn);
        });
    }

    @Test
    public void testRetryReusesCompletePartitionAfterMetadataFailure() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 1);
        setProperty(PropertyKey.CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX, 256);
        final AtomicBoolean isCounting = new AtomicBoolean();
        final AtomicLong symbolFd = new AtomicLong(-1);
        final AtomicLong symbolBytesRead = new AtomicLong();
        final MetaSwapFailingFacade ff = new MetaSwapFailingFacade() {
            @Override
            public boolean close(long fd) {
                symbolFd.compareAndSet(fd, -1);
                return super.close(fd);
            }

            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (isCounting.get() && Utf8s.containsAscii(name, "2024-01-01") && Utf8s.endsWithAscii(name, "s.d")) {
                    symbolFd.set(fd);
                }
                return fd;
            }

            @Override
            public long read(long fd, long buf, long size, long offset) {
                final long count = super.read(fd, buf, size, offset);
                if (fd == symbolFd.get()) {
                    symbolBytesRead.addAndGet(count);
                }
                return count;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a'::STRING ELSE NULL END, x, timestamp_sequence('2024-01-01', 8_640_000L)
                    FROM long_sequence(20_000)
                    """);
            assertQuery("SELECT s IS NULL AS is_null, s = '' AS is_empty FROM x WHERE v = 1")
                    .noLeakCheck().returns("is_null\tis_empty\ntrue\tfalse\n");
            engine.releaseAllWriters();
            final String sql = "ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)";
            assertAddIndexFailsAtMetadataSwap(ff, sql);
            engine.releaseAllWriters();
            isCounting.set(true);
            execute(sql);
            isCounting.set(false);
            Assert.assertEquals("retry must not scan the completed historical partition", 0, symbolBytesRead.get());
            assertQuery("SELECT count(), sum(v) FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().withPlanContaining("CoveringIndex")
                    .returns("count\tsum\n10000\t100010000\n");
            assertQuery("SELECT count(), sum(v) FROM x WHERE s IS NULL")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\n10000\t100000000\n");
        });
    }

    @Test
    public void testRetryReusesCompositePartition() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_SWAP_FILE_COUNT, 1);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1G");
        final MetaSwapFailingFacade ff = new MetaSwapFailingFacade();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute("""
                    INSERT INTO x
                    SELECT CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x, timestamp_sequence('2024-01-01', 21_600_000_000L)
                    FROM long_sequence(12)
                    """);
            drainWalQueue();
            // Replaces a row of 2024-01-02, which leaves the partition composite, with a dead row in its extent.
            execute("INSERT INTO x VALUES ('a', 99, '2024-01-02T06:00')");
            drainWalQueue();
            try (TableReader reader = getReader("x")) {
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(1));
                Assert.assertTrue(reader.getPartitionPhysicalRowCount(1) > reader.getPartitionRowCountFromMetadata(1));
            }
            final TableToken tableToken = engine.verifyTableName("x");
            // The build completes and the metadata swap fails: the table suspends, and the build stays on disk.
            ff.isFailing = true;
            execute("ALTER TABLE x ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();
            ff.isFailing = false;
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            final long sealTxn = readPostingSealTxn("2024-01-02");
            Assert.assertTrue(sealTxn > -1);
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals("reuse the build over the full physical extent", sealTxn, readPostingSealTxn("2024-01-02"));
            assertQuery("SELECT v, ts FROM x WHERE s = 'a'")
                    .noLeakCheck().noRandomAccess().expectSize().timestamp("ts").withPlanContaining("CoveringIndex")
                    .returns("""
                            v\tts
                            2\t2024-01-01T06:00:00.000000Z
                            4\t2024-01-01T18:00:00.000000Z
                            99\t2024-01-02T06:00:00.000000Z
                            8\t2024-01-02T18:00:00.000000Z
                            10\t2024-01-03T06:00:00.000000Z
                            12\t2024-01-03T18:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSparseGenerationIsNotFinishedBuild() throws Exception {
        withIndex("", (reader, path) -> {
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                writer.of(path, "s", -1, true);
                writer.setNextTxnAtSeal(reader.getTxn());
                writer.add(1, 0);
                writer.add(0, 1);
                writer.add(2, 2);
                writer.add(1, 3);
                writer.commit();
            }
            assertComplete(reader, path, false);
        });
    }

    @Test
    public void testUnpublishedCoverExtent() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> withKey(path, (mem, header, head) -> {
            PostingIndexChainEntry.writeCoverEndOffset(mem, head.offset, 1, 1, 0, head.coveringFormat);
            assertComplete(reader, path, false);
        }));
    }

    @Test
    public void testWrongTransaction() throws Exception {
        withIndex("INCLUDE (v, w)", (reader, path) -> {
            Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn() - 1, 4));
            Assert.assertFalse(isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn() + 1, 4));
        });
    }

    private static void assertAddIndexFailsAtMetadataSwap(MetaSwapFailingFacade ff, String sql) throws Exception {
        ff.isFailing = true;
        try {
            execute(sql);
            Assert.fail("metadata publication must fail");
        } catch (CairoException e) {
            Assert.assertEquals("Cannot open indexed file. Max number of attempts reached [1]. Last file tried: "
                    + ff.failedFile, e.getFlyweightMessage().toString());
        } finally {
            ff.isFailing = false;
        }
    }

    private static void assertComplete(TableReader reader, Path path, boolean isExpectedComplete) {
        final String before = path.toString();
        Assert.assertEquals(isExpectedComplete, isComplete(configuration.getFilesFacade(), reader, path, reader.getTxn(), 4));
        Assert.assertEquals(before, path.toString());
    }

    private static void assertRejectedInt(TableReader reader, Path path, MemoryCMARW mem, long offset, int value) {
        final int saved = mem.getInt(offset);
        mem.putInt(offset, value);
        try {
            assertComplete(reader, path, false);
        } finally {
            mem.putInt(offset, saved);
        }
    }

    private static void assertRejectedLong(TableReader reader, Path path, MemoryCMARW mem, long offset, long value) {
        final long saved = mem.getLong(offset);
        mem.putLong(offset, value);
        try {
            assertComplete(reader, path, false);
        } finally {
            mem.putLong(offset, saved);
        }
    }

    private static boolean isComplete(FilesFacade ff, TableReader reader, Path path, long txn, long rowCount) {
        final int columnIndex = reader.getMetadata().getColumnIndex("s");
        final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(0, reader.getMetadata().getWriterIndex(columnIndex));
        return PostingIndexBuildValidator.isComplete(ff, path, "s", columnNameTxn, txn, rowCount,
                reader.getMetadata().getColumnMetadata(columnIndex).getCoveringColumnIndices(), reader.getColumnVersionReader(), 0);
    }

    // Zeroes the page of the partition's .pk that holds its head entry, and returns that entry's sealTxn.
    private static long loseHeadEntryPage(String partitionName) throws Exception {
        final long[] sealTxn = {-1};
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName("x")).concat(partitionName);
            withKey(path, (mem, header, head) -> {
                sealTxn[0] = head.sealTxn;
                final long pageLo = header.headEntryOffset & ~(PAGE_SIZE - 1L);
                for (long offset = pageLo, pageHi = Math.min(pageLo + PAGE_SIZE, mem.size()); offset < pageHi; offset += Long.BYTES) {
                    mem.putLong(offset, 0);
                }
            });
        }
        Assert.assertTrue(sealTxn[0] > -1);
        return sealTxn[0];
    }

    private static PostingIndexChainEntry.Snapshot readHead(Path path) throws Exception {
        final PostingIndexChainEntry.Snapshot result = new PostingIndexChainEntry.Snapshot();
        withKey(path, (mem, header, head) -> PostingIndexChainEntry.read(mem, head.offset, head.coverCount, result));
        return result;
    }

    private static long readPostingSealTxn(String partitionName) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName("x")).concat(partitionName);
            return readSealTxnFromKeyFile(configuration.getFilesFacade(), keyFileName(path, "s", -1));
        }
    }

    private static void withKey(Path path, KeyAction action) throws Exception {
        final int plen = path.size();
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(configuration.getFilesFacade(), keyFileName(path, "s", -1), MemoryTag.MMAP_DEFAULT);
            mem.jumpTo(mem.size());
            path.trimTo(plen);
            final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            Assert.assertTrue(PostingIndexChainHeader.readUnderSeqlock(mem, header));
            final PostingIndexChainEntry.Snapshot head = new PostingIndexChainEntry.Snapshot();
            PostingIndexChainEntry.read(mem, header.headEntryOffset, head);
            action.run(mem, header, head);
        } finally {
            path.trimTo(plen);
        }
    }

    // An interrupted build left an entry in the partition's .pk, a commit has since reached the txn that tags it, and
    // the .pv it names is gone. ADD INDEX must drop the entry rather than open it.
    private void assertAddIndexDropsLeftoverEntry(String tableName, String walClause, int partitionIndex, boolean isParquet) throws Exception {
        execute("CREATE TABLE " + tableName + " (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
        execute("""
                INSERT INTO %s VALUES
                ('a', 1, '2024-01-01'),
                ('b', 2, '2024-01-01T12:00'),
                ('a', 3, '2024-01-02'),
                ('b', 4, '2024-01-02T12:00')
                """.formatted(tableName));
        drainWalQueue();
        if (isParquet) {
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
        }
        engine.releaseAllWriters();
        final TableToken tableToken = engine.verifyTableName(tableName);
        final long txn;
        final long partitionTimestamp;
        final long partitionNameTxn;
        try (TableReader reader = getReader(tableName)) {
            txn = reader.getTxn();
            partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            Assert.assertEquals(isParquet, reader.getTxFile().isPartitionParquet(partitionIndex));
        }
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
            final int plen = path.size();
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                writer.of(path, "s", -1, true);
                writer.setNextTxnAtSeal(txn);
                writer.add(1, 0);
                writer.add(1, 1);
                writer.setMaxValue(1);
                writer.seal();
            }
            final long sealTxn = readSealTxnFromKeyFile(ff, keyFileName(path.trimTo(plen), "s", -1));
            Assert.assertTrue(sealTxn > -1);
            Assert.assertTrue(ff.exists(valueFileName(path.trimTo(plen), "s", -1, sealTxn)));
            Assert.assertTrue(ff.removeQuiet(path.$()));
        }
        execute("ALTER TABLE " + tableName + " ALTER COLUMN s ADD INDEX TYPE POSTING");
        drainWalQueue();
        Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        assertQuery("SELECT v FROM " + tableName + " WHERE s = 'a'")
                .noLeakCheck()
                .withPlanContaining("Index forward scan on: s")
                .returns("""
                        v
                        1
                        3
                        """);
    }

    private void withIndex(String indexClause, IndexAction action) throws Exception {
        withIndex("x", indexClause, action);
    }

    private void withIndex(String tableName, String indexClause, IndexAction action) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + tableName + " (s SYMBOL, v LONG, w VARCHAR)");
            execute("INSERT INTO " + tableName + " VALUES ('a', 1, 'one'), (NULL, 2, NULL), ('b', NULL, '🐘'), ('a', 4, '')");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN s ADD INDEX TYPE POSTING " + indexClause);
            engine.releaseAllWriters();
            try (TableReader reader = getReader(tableName); Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(reader.getTableToken()).concat("default");
                action.run(reader, path);
            }
        });
    }

    @FunctionalInterface
    private interface IndexAction {
        void run(TableReader reader, Path path) throws Exception;
    }

    @FunctionalInterface
    private interface KeyAction {
        void run(MemoryCMARW mem, PostingIndexChainHeader.Snapshot header, PostingIndexChainEntry.Snapshot head) throws Exception;
    }

    private static class MetaSwapFailingFacade extends TestFilesFacadeImpl {
        private final StringSink failedFile = new StringSink();
        private volatile boolean isFailing;

        @Override
        public long openRW(LPSZ name, int opts) {
            if (isFailing && Utf8s.containsAscii(name, "_meta.swp")) {
                failedFile.clear();
                failedFile.put(name);
                return -1;
            }
            return super.openRW(name, opts);
        }
    }
}
