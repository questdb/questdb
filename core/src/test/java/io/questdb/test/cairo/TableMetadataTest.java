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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TableMetadataTest extends AbstractCairoTest {
    // the header fields up to and including the dedup mode, the last field that the minor version gates
    private static final int HEADER_SIZE = 64;
    private final boolean walEnabled;

    public TableMetadataTest(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @Test
    public void testFuzzIsMetaFormatUpToDate() throws Exception {
        Assume.assumeFalse(walEnabled); // The test doesn't deal with tables

        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            try (MemoryCARW metaMem = Vm.getCARWInstance(128, 1, MemoryTag.NATIVE_DEFAULT)) {
                int falseNegativeCount = 0;
                for (int i = 0; i < 65_536; i++) {
                    int columnCount = rnd.nextInt(1000);
                    long metadataVersion = rnd.nextLong(100);
                    int metaFormatMinorVersion = TableUtils.calculateMetaFormatMinorVersionField(metadataVersion, columnCount);
                    int garbageMetaFormatMinorVersion = rnd.nextInt();

                    metaMem.putInt(TableUtils.META_OFFSET_COUNT, columnCount);
                    metaMem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, metadataVersion);

                    metaMem.putInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, metaFormatMinorVersion);
                    assertTrue(TableUtils.isMetaFormatUpToDate(metaMem));

                    metaMem.putInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, 0);
                    assertFalse(TableUtils.isMetaFormatUpToDate(metaMem));

                    metaMem.putInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, garbageMetaFormatMinorVersion);
                    if (TableUtils.isMetaFormatUpToDate(metaMem)) {
                        falseNegativeCount++;
                    }
                }
                Assert.assertTrue("Detected more than 5 false negatives on checksum field validation",
                        falseNegativeCount <= 5);
            }
        });
    }

    @Test
    public void testResetMetadataVersionCompletesInterruptedStamp() throws Exception {
        Assume.assumeFalse(walEnabled); // The test doesn't deal with tables

        assertMemoryLeak(() -> {
            try (VersionWriteFailingMemory metaMem = new VersionWriteFailingMemory()) {
                writeHeader(
                        metaMem,
                        3,
                        5,
                        Numbers.encodeLowHighShorts(
                                TableUtils.checksumForMetaFormatMinorVersionField(5, 3),
                                TableUtils.META_FORMAT_MINOR_VERSION_LATEST
                        )
                );
                assertTrue(TableUtils.isMetaFormatUpToDate(metaMem));

                // the process dies between the two writes
                metaMem.isVersionWriteFailing = true;
                try {
                    TableUtils.resetMetadataVersion(metaMem, 0);
                    Assert.fail("the version write must fail");
                } catch (IllegalStateException ignore) {
                }
                // the checksum for the new version went first, the version is still the old one
                Assert.assertEquals(5, metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION));
                Assert.assertEquals(
                        Numbers.encodeLowHighShorts(
                                TableUtils.checksumForMetaFormatMinorVersionField(0, 3),
                                TableUtils.META_FORMAT_MINOR_VERSION_LATEST
                        ),
                        metaMem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION)
                );
                assertFalse(TableUtils.isMetaFormatUpToDate(metaMem));

                // the retried table type conversion repeats the call, which completes the stamp
                metaMem.isVersionWriteFailing = false;
                TableUtils.resetMetadataVersion(metaMem, 0);
                Assert.assertEquals(0, metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION));
                assertTrue(TableUtils.isMetaFormatUpToDate(metaMem));
            }
        });
    }

    @Test
    public void testResetMetadataVersionKeepsTrustedFieldTrusted() throws Exception {
        Assume.assumeFalse(walEnabled); // The test doesn't deal with tables

        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final byte[] headerBefore = new byte[HEADER_SIZE];
            try (MemoryCARW metaMem = Vm.getCARWInstance(128, 1, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < 10_000; i++) {
                    for (int offset = 0; offset < HEADER_SIZE; offset += Integer.BYTES) {
                        metaMem.putInt(offset, rnd.nextInt());
                    }
                    final int columnCount = 1 + rnd.nextInt(1000);
                    final long metadataVersion = rnd.nextLong(1_000_000);
                    final long newMetadataVersion = rnd.nextBoolean() ? 0 : rnd.nextLong(1_000_000);
                    // a release that predates the latest minor version, this one, or a newer one wrote the field
                    final short minorVersion = (short) (1 + rnd.nextInt(TableUtils.META_FORMAT_MINOR_VERSION_LATEST + 1));
                    writeHeader(
                            metaMem,
                            columnCount,
                            metadataVersion,
                            Numbers.encodeLowHighShorts(TableUtils.checksumForMetaFormatMinorVersionField(metadataVersion, columnCount), minorVersion)
                    );
                    for (int offset = 0; offset < HEADER_SIZE; offset++) {
                        headerBefore[offset] = metaMem.getByte(offset);
                    }

                    TableUtils.resetMetadataVersion(metaMem, newMetadataVersion);
                    // a repeated call, e.g. by a retried table type conversion, changes nothing
                    TableUtils.resetMetadataVersion(metaMem, newMetadataVersion);

                    Assert.assertEquals(newMetadataVersion, metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION));
                    Assert.assertEquals(
                            Numbers.encodeLowHighShorts(TableUtils.checksumForMetaFormatMinorVersionField(newMetadataVersion, columnCount), minorVersion),
                            metaMem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION)
                    );
                    assertTrue(TableUtils.hasParquetEncodingConfig(metaMem));
                    Assert.assertEquals(minorVersion >= TableUtils.META_FORMAT_MINOR_VERSION_LATEST, TableUtils.isMetaFormatUpToDate(metaMem));
                    assertOnlyVersionAndChecksumChanged(metaMem, headerBefore);
                }
            }
        });
    }

    @Test
    public void testResetMetadataVersionKeepsUntrustedFieldUntrusted() throws Exception {
        Assume.assumeFalse(walEnabled); // The test doesn't deal with tables

        assertMemoryLeak(() -> {
            try (MemoryCARW metaMem = Vm.getCARWInstance(128, 1, MemoryTag.NATIVE_DEFAULT)) {
                // A release that predates the field leaves zeros there, while a checksum is never zero.
                writeHeader(metaMem, 3, 5, 0);
                TableUtils.resetMetadataVersion(metaMem, 0);
                Assert.assertEquals(0, metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION));
                Assert.assertEquals(0, metaMem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION));
                assertFalse(TableUtils.hasParquetEncodingConfig(metaMem));

                // A checksum that does not match the version, e.g. one that a version reset without a re-stamp
                // left behind, must not turn trusted for the new version.
                final int staleField = Numbers.encodeLowHighShorts(
                        TableUtils.checksumForMetaFormatMinorVersionField(7, 3),
                        TableUtils.META_FORMAT_MINOR_VERSION_LATEST
                );
                writeHeader(metaMem, 3, 5, staleField);
                TableUtils.resetMetadataVersion(metaMem, 9);
                Assert.assertEquals(9, metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION));
                Assert.assertEquals(staleField, metaMem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION));
                assertFalse(TableUtils.hasParquetEncodingConfig(metaMem));
            }
        });
    }

    @Test
    public void testTableReaderMetadataPool() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "x";
            execute("create table x (a int, b long, c double, d symbol capacity 10, e string, ts timestamp) timestamp (ts) partition by WEEK " + (walEnabled ? "WAL" : ""));
            TableToken tt = engine.verifyTableName(tableName);
            int maxUncommitted = 1234;

            try (TableMetadata m1 = engine.getTableMetadata(tt)) {
                Assert.assertEquals(configuration.getMaxUncommittedRows(), m1.getMaxUncommittedRows());
                Assert.assertEquals(configuration.getO3MaxLag(), m1.getO3MaxLag());
                Assert.assertEquals(PartitionBy.WEEK, m1.getPartitionBy());

                execute("alter table x set param maxUncommittedRows = " + maxUncommitted);
                execute("alter table x set param o3MaxLag = 50s");

                if (walEnabled) {
                    try (TableMetadata m2 = engine.getTableMetadata(tt)) {
                        Assert.assertEquals(configuration.getMaxUncommittedRows(), m2.getMaxUncommittedRows());
                        drainWalQueue();
                        Assert.assertEquals(configuration.getMaxUncommittedRows(), m2.getMaxUncommittedRows());
                    }
                }

                try (TableMetadata m2 = engine.getTableMetadata(tt)) {
                    Assert.assertEquals(maxUncommitted, m2.getMaxUncommittedRows());
                    if (!walEnabled) {
                        // Not effective for WAL mode
                        Assert.assertEquals(50 * 1_000_000, m2.getO3MaxLag());
                    }
                }

                // No change on existing metadata
                Assert.assertEquals(configuration.getMaxUncommittedRows(), m1.getMaxUncommittedRows());
                Assert.assertEquals(configuration.getO3MaxLag(), m1.getO3MaxLag());
                Assert.assertEquals(PartitionBy.WEEK, m1.getPartitionBy());
            }

            try (TableMetadata m2 = engine.getTableMetadata(tt)) {
                Assert.assertEquals(maxUncommitted, m2.getMaxUncommittedRows());

                if (!walEnabled) {
                    // Not effective for WAL mode
                    Assert.assertEquals(50 * 1_000_000, m2.getO3MaxLag());
                }
                try (TableMetadata m1 = engine.getTableMetadata(tt)) {
                    Assert.assertEquals(maxUncommitted, m1.getMaxUncommittedRows());

                    if (!walEnabled) {
                        // Not effective for WAL mode
                        Assert.assertEquals(50 * 1_000_000, m1.getO3MaxLag());
                    }
                }
            }

            execute("alter table x add column f int");
            try (TableRecordMetadata m1 = engine.getLegacyMetadata(tt)) {
                // No delay in meta changes for WAL tables
                Assert.assertEquals(m1.getColumnCount() - 1, m1.getColumnIndex("f"));
            }
        });
    }

    private static void assertOnlyVersionAndChecksumChanged(MemoryCARW metaMem, byte[] headerBefore) {
        for (int offset = 0; offset < HEADER_SIZE; offset++) {
            final boolean isVersion = offset >= TableUtils.META_OFFSET_METADATA_VERSION
                    && offset < TableUtils.META_OFFSET_METADATA_VERSION + Long.BYTES;
            // the checksum is the low short of the little-endian minor version field
            final boolean isChecksum = offset >= TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION
                    && offset < TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION + Short.BYTES;
            if (!isVersion && !isChecksum) {
                Assert.assertEquals("offset " + offset, headerBefore[offset], metaMem.getByte(offset));
            }
        }
    }

    private static void writeHeader(MemoryCARW metaMem, int columnCount, long metadataVersion, int metaFormatMinorVersionField) {
        metaMem.putInt(TableUtils.META_OFFSET_COUNT, columnCount);
        metaMem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, metadataVersion);
        metaMem.putInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION, metaFormatMinorVersionField);
    }

    // Fails the metadata version write on demand, which leaves the memory as a process death right before that
    // write would.
    private static class VersionWriteFailingMemory extends MemoryCARWImpl {
        private boolean isVersionWriteFailing;

        private VersionWriteFailingMemory() {
            super(128, 1, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void putLong(long offset, long value) {
            if (isVersionWriteFailing && offset == TableUtils.META_OFFSET_METADATA_VERSION) {
                throw new IllegalStateException("simulated process death");
            }
            super.putLong(offset, value);
        }
    }
}
