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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mig.Mig940;
import io.questdb.cairo.mig.MigrationContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Java-level regression guard for the CRC32 verification that
 * {@link ParquetMetaFileReader#resolveFooter(long)} performs on first open
 * (the {@code verifyChecksum0} call at the top of the method). Each test
 * flips a single byte in a real on-disk {@code _pm}, opens it through the
 * production JNI path, and asserts a clean {@link CairoException} surfaces.
 * Without that wiring, the same flip would pass structural validation and be
 * served as authoritative metadata.
 */
public class ParquetMetaCrcCorruptionTest extends AbstractCairoTest {

    @Test
    public void testCrcCorruptionDetectedOnFirstOpen() throws Exception {
        // Flip a byte inside the column descriptor area (offset 32 — first
        // byte after the fixed header). That region lies inside the CRC
        // coverage [HEADER_CRC_AREA_OFF=8, snapshotEnd-8). The Rust
        // verify_checksum step recomputes CRC32 over the file and rejects.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> assertCrcFlipRejected(32L));
    }

    @Test
    public void testCrcCorruptionInFooterSection() throws Exception {
        // Flip a byte inside the row group block area (offset 96 — for a
        // two-column file this lands just past the column descriptors, in
        // the row group's chunk metadata). That region is data the parser
        // copies through without a structural validity check, so the failure
        // surfaces as a CRC mismatch rather than a flag/feature rejection.
        // Mirrors the Rust unit test crc_corruption_detected via the JNI
        // surface.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> assertCrcFlipRejected(96L));
    }

    @Test
    public void testCrcRefusedLetsMig940RegenerateAfterFlip() throws Exception {
        // After a CRC flip, Mig940 must treat the _pm as stale (its
        // isParquetMetadataStale helper swallows CairoException as "stale")
        // and regenerate it from data.parquet. This proves the migration is
        // the documented recovery path for CRC-detected corruption rather
        // than a hard failure.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

            final FilesFacade ff = configuration.getFilesFacade();
            final TableToken token = engine.verifyTableName("t");

            long partitionTs;
            long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();

            flipByte(ff, token, partitionTs, partitionNameTxn, 32L);

            // Confirm a direct open through the production JNI path observes
            // the CRC mismatch BEFORE the migration runs.
            assertReadFails(ff, token, partitionTs, partitionNameTxn);

            runMig940(token);

            // After Mig940, the regenerated _pm must open cleanly.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                long size = ParquetMetaFileReader.readParquetMetaFileSize(ff, path.$());
                Assert.assertTrue("regenerated _pm should have positive size", size > 0);
                long addr = TableUtils.mapRO(ff, path.$(), LOG, size, MemoryTag.MMAP_DEFAULT);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(addr, size);
                    Assert.assertTrue(reader.resolveFooter(Long.MAX_VALUE));
                    reader.clear();
                } finally {
                    ff.munmap(addr, size, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    private void assertCrcFlipRejected(long flipOffsetFromOrigin) throws Exception {
        execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES(1, '2024-06-10T00:00:00.000000Z')");
        execute("INSERT INTO t VALUES(2, '2024-06-11T00:00:00.000000Z')");
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts > 0");

        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken token = engine.verifyTableName("t");

        long partitionTs;
        long partitionNameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
            partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
        }
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();

        flipByte(ff, token, partitionTs, partitionNameTxn, flipOffsetFromOrigin);
        assertReadFails(ff, token, partitionTs, partitionNameTxn);
    }

    private void assertReadFails(FilesFacade ff, TableToken token, long partitionTs, long partitionNameTxn) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            ParquetMetaFileReader reader = new ParquetMetaFileReader();
            long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), reader);
            Assert.assertNotEquals("openAndMapRO should not skip the file", 0L, addr);
            long size = reader.getFileSize();
            try {
                reader.resolveFooter(Long.MAX_VALUE);
                Assert.fail("expected CairoException from CRC verification");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("checksum"));
            } finally {
                reader.clear();
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    private void flipByte(FilesFacade ff, TableToken token, long partitionTs, long partitionNameTxn, long offsetFromOrigin) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            long fd = ff.openRW(path.$(), 0);
            Assert.assertTrue("openRW _pm: " + path, fd >= 0);
            try {
                long fileSize = ff.length(fd);
                long absOffset = offsetFromOrigin >= 0 ? offsetFromOrigin : fileSize + offsetFromOrigin;
                Assert.assertTrue("flip offset within file", absOffset >= 0 && absOffset < fileSize);
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(1, ff.read(fd, buf, 1, absOffset));
                    byte b = Unsafe.getByte(buf);
                    Unsafe.putByte(buf, (byte) (b ^ 0xFF));
                    Assert.assertEquals(1, ff.write(fd, buf, 1, absOffset));
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    private void runMig940(TableToken token) {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        engine.releaseInactive();
        long tempMem = Unsafe.malloc(1024, MemoryTag.NATIVE_MIG_MMAP);
        try (
                MemoryMARW rwMem = Vm.getCMARWInstance();
                MemoryARW tempVirtualMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_MIG_MMAP);
                Path tablePath = new Path().of(configuration.getDbRoot()).concat(token).slash();
                Path tablePath2 = new Path().of(configuration.getDbRoot()).concat(token).slash()
        ) {
            MigrationContext ctx = new MigrationContext(engine, tempMem, 1024, tempVirtualMem, rwMem);
            ctx.of(tablePath, tablePath2, -1);
            Mig940.migrate(ctx);
        } finally {
            Unsafe.free(tempMem, 1024, MemoryTag.NATIVE_MIG_MMAP);
        }
    }
}
