/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Regression test for the legacy Required BOOLEAN parquet corruption.
 * <p>
 * The fixture {@code resources/parquet-c3-legacy/legacy_bool~11.zip} is a real table written by
 * QuestDB 9.4.3, which writes BOOLEAN as parquet Required (max def level 0, no definition-level
 * stream). The table holds only a designated timestamp plus a single BOOLEAN column {@code b} --
 * deliberately none of the other no-null-sentinel types (BYTE/SHORT/CHAR/SYMBOL). It has two parquet
 * partitions, each split into several row groups (converted with a small row-group size):
 * <ul>
 *     <li>{@code 2024-01-01}: 120 rows, 3 row groups, {@code b} present for every row.</li>
 *     <li>{@code 2024-01-02}: 180 rows, 4 row groups, {@code b} added after the first 60 rows
 *     (column top 60). The legacy Required encoding cannot carry the column-top NULL, so those 60
 *     rows were materialised as {@code false}.</li>
 * </ul>
 * <p>
 * {@code O3PartitionJob.hasLegacyRequiredNoSentinelColumn()} forces a full re-encode (Required ->
 * Optional) when a rewritten legacy partition still stores a no-null-sentinel column Required, but it
 * lists BYTE/SHORT/CHAR/SYMBOL and omits BOOLEAN. Because this table's only such column is BOOLEAN,
 * {@code forceFullReencode} stays false. A non-conversion schema change (ADD COLUMN) plus an O3
 * insert then routes the non-merged row groups through {@code copyRowGroupWithNullColumns()}, which
 * raw-copies the legacy Required boolean pages under a footer that {@code setTargetSchema()} freshly
 * built declaring BOOLEAN Optional. The reader mis-decodes the def-level-less pages as if they
 * carried a definition-level stream, corrupting {@code b}.
 * <p>
 * {@link #testO3RewriteCorruptsLegacyRequiredBoolean()} reproduces that corruption: it FAILS on the
 * unfixed code and passes once BOOLEAN is added to the guard so the partition is fully re-encoded.
 */
public class LegacyBoolParquetRewriteTest extends AbstractCairoTest {

    // Unpack the archived 9.4.3 table directory into the engine's db root and let the registry
    // discover it from its _name file. The fixture is a single zip whose entries are rooted at the
    // table directory (legacy_bool~11/...).
    private static void loadFixture() throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = LegacyBoolParquetRewriteTest.class.getResourceAsStream("/parquet-c3-legacy/legacy_bool~11.zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (!ze.isDirectory()) {
                        final File dir = dest.getParentFile();
                        if (!dir.exists()) {
                            Assert.assertTrue(dir.mkdirs());
                        }
                        try (FileOutputStream fos = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                fos.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
    }

    // Reads every value of every row through CursorPrinter and returns the row count. count() reads
    // only row counts and would not surface a per-value decode fault.
    private static long readAll(StringSink sink) throws Exception {
        long rows = 0;
        try (
                RecordCursorFactory factory = select("select * from legacy_bool", sqlExecutionContext);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata meta = factory.getMetadata();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.clear();
                CursorPrinter.println(record, meta, sink); // accesses every column of the row
                Assert.assertTrue(sink.length() > 0);
                rows++;
            }
        }
        return rows;
    }

    @Test
    public void testO3RewriteCorruptsLegacyRequiredBoolean() throws Exception {
        // Reproduces the legacy Required BOOLEAN corruption. The only no-null-sentinel column here is
        // BOOLEAN, which hasLegacyRequiredNoSentinelColumn() omits, so forceFullReencode stays false.
        // ADD COLUMN (a plain schema change, no type conversion) leaves the parquet files missing
        // column x -> a later O3 merge sees hasMissingColumns (hasSchemaChange) with
        // hasTypeConvertedColumns=false and forceFullReencode=false, routing the non-merged row groups
        // through copyRowGroupWithNullColumns(): legacy Required boolean pages under a fresh Optional
        // footer. With the fix (BOOLEAN added to the guard) the partition is re-encoded to Optional
        // and the data survives.
        assertMemoryLeak(() -> {
            loadFixture();

            execute("alter table legacy_bool add column x int");
            drainWalQueue();

            // Golden native copy of the intact 300 rows (the parquet is still readable at this point).
            execute("create table golden as (select * from legacy_bool)");

            // Out-of-order insert inside day1's range forces a rewrite of the parquet partition. The
            // row group holding the new row is merged (re-encoded); the others are raw-copied via
            // copyRowGroupWithNullColumns -> Required boolean pages under an Optional footer.
            execute("insert into legacy_bool (ts) values ('2024-01-01T00:00:00.500000Z')");
            drainWalQueue();

            assertQuery("select count() c from legacy_bool").noRandomAccess().expectSize().returns("c\n301\n");

            // The original 300 rows must survive the rewrite byte-for-byte. FAILS on the unfixed code:
            // the raw-copied Required boolean row groups read as garbage under the Optional footer.
            assertSqlCursors(
                    "select * from golden order by ts",
                    "select * from legacy_bool where ts != '2024-01-01T00:00:00.500000Z' order by ts"
            );
        });
    }

    @Test
    public void testReadLegacyRequiredBooleanParquet() throws Exception {
        // Sanity: the fixture decodes correctly before any rewrite. Proves the fixture is valid
        // independently of the corruption bug.
        assertMemoryLeak(() -> {
            loadFixture();

            final StringSink sink = new StringSink();
            Assert.assertEquals(300, readAll(sink));

            // 40 trues in day1 (x % 3 == 0 over 120 rows) + 30 in day2 (x % 4 == 0 over 120 rows).
            assertQuery("select count() c from legacy_bool where b = true")
                    .noRandomAccess().expectSize().returns("c\n70\n");
            // The 60 column-top rows of day2 (b added after the first 60 rows) read false: the legacy
            // Required encoding has no null sentinel for BOOLEAN.
            assertQuery("select count() c from legacy_bool where ts in '2024-01-02' and ts < '2024-01-02T01:00:00.000000Z' and b = false")
                    .noRandomAccess().expectSize().returns("c\n60\n");
        });
    }
}
