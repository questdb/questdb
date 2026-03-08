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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
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
}
