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
import io.questdb.griffin.SqlException;
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
 * Regression test for the C3 upgrade-corruption bug. The fixture
 * {@code resources/parquet-c3-legacy/legacy_all~18.zip} is a real table written by QuestDB 9.4.2,
 * which predates the convention that BYTE/SHORT/CHAR are stored Optional in parquet. Its single
 * partition was converted to parquet with a small row-group size, so the no-null-sentinel columns
 * are stored Required (parquet max def level 0) across 15 row groups. A second set of all column
 * types was added after the first 100 rows, giving those columns a column top of 100.
 * <p>
 * When such a partition is rewritten (here via an out-of-order insert), the schema migration to
 * Optional must not be combined with raw-copying the legacy Required pages: that would leave an
 * Optional footer over def-level-less pages, which the reader mis-decodes. The fix forces a full
 * re-encode of every row group to Optional. This test proves the data survives the rewrite intact.
 */
public class LegacyRequiredParquetRewriteTest extends AbstractCairoTest {

    // Unpack the archived 9.4.2 table directory into the engine's db root and let the registry
    // discover it from its _name file. The fixture is a single zip whose entries are rooted at
    // the table directory (legacy_all~18/...).
    private static void loadFixture() throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = LegacyRequiredParquetRewriteTest.class.getResourceAsStream("/parquet-c3-legacy/legacy_all~18.zip")) {
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

    // Cumulative list of ALTER COLUMN TYPE conversions applied one at a time. Each entry is a valid
    // conversion of a column to a different type; after each we re-read every value of every row to
    // isolate the column whose conversion breaks the (lazy) parquet read.
    private static final String[][] CONVERSIONS = {
            {"c_bool", "int"}, {"c_byte", "string"}, {"c_short", "long"}, {"c_char", "string"},
            {"c_int", "long"}, {"c_long", "double"}, {"c_float", "double"}, {"c_double", "string"},
            {"c_date", "timestamp"}, {"c_string", "varchar"}, {"c_symbol", "varchar"}, {"c_varchar", "string"},
            {"c_uuid", "string"}, {"c_l256", "string"}, {"c_ip", "string"}, {"c_geo1", "string"},
            {"c_geo2", "string"}, {"c_geo4", "string"}, {"c_geo8", "string"}, {"c_dec", "string"},
            {"c_bin", "string"}, {"c_arr", "string"},
            {"n_bool", "int"}, {"n_byte", "string"}, {"n_short", "long"}, {"n_char", "string"},
            {"n_int", "long"}, {"n_long", "double"}, {"n_float", "double"}, {"n_double", "string"},
            {"n_date", "timestamp"}, {"n_string", "varchar"}, {"n_symbol", "varchar"}, {"n_varchar", "string"},
            {"n_uuid", "string"}, {"n_l256", "string"}, {"n_ip", "string"}, {"n_geo1", "string"},
            {"n_geo2", "string"}, {"n_geo4", "string"}, {"n_geo8", "string"}, {"n_dec", "string"},
            {"n_bin", "string"}, {"n_arr", "string"},
    };

    // Reads every value of every row of legacy_all through CursorPrinter and returns the row count.
    // count()/count_distinct only read row counts or single columns and would not catch a per-value
    // decode fault.
    private static long readAll(StringSink sink) throws Exception {
        long rows = 0;
        try (
                RecordCursorFactory factory = select("select * from legacy_all", sqlExecutionContext);
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

    // Differential oracle for the lazy parquet read path over the legacy Required fixture. A native
    // CTAS copy of the legacy data, converted via the native ALTER kernel, is the oracle; legacy_all
    // is converted in place and stays parquet (ALTER COLUMN TYPE is lazy on a parquet partition), so
    // its read materialises each conversion through the lazy single-pass decode arm. Each entry must
    // target a DISTINCT column (a column has one type), so one fixture load covers the whole batch.
    private void assertLegacyConversionsMatchNative(String[][] conversions) throws Exception {
        loadFixture();
        execute("create table golden as (select * from legacy_all)");
        for (String[] conv : conversions) {
            execute("alter table golden alter column " + conv[0] + " type " + conv[1]);
            execute("alter table legacy_all alter column " + conv[0] + " type " + conv[1]);
        }
        drainWalQueue();
        for (String[] conv : conversions) {
            assertSqlCursors(
                    "select ts, " + conv[0] + " from golden order by ts",
                    "select ts, " + conv[0] + " from legacy_all order by ts"
            );
        }
    }

    @Test
    public void testReadLegacyRequiredParquetPartition() throws Exception {
        // Isolation: reading the legacy Required parquet (all column types, Required
        // BYTE/SHORT/CHAR, second column set with column top 100) must decode correctly,
        // including after each column is type-converted one at a time. Decode path only -- no rewrite.
        assertMemoryLeak(() -> {
            loadFixture();

            final StringSink sink = new StringSink();
            Assert.assertEquals(150, readAll(sink));

            // Column tops: the second set was added after the first 100 rows -> NULL for the
            // nullable members of those rows.
            assertQuery("select count() from legacy_all where n_int = null").noRandomAccess().expectSize().returns("count\n100\n");
            assertQuery("select count() from legacy_all where n_int != null").noRandomAccess().expectSize().returns("count\n50\n");
            // The Required no-sentinel columns (BYTE/SHORT/CHAR) decode to varied values, not a
            // corrupted constant.
            assertQuery("select (count_distinct(c_byte) > 1) c, (count_distinct(c_short) > 1) s, (count_distinct(c_char) > 1) ch from legacy_all")
                    .noRandomAccess().expectSize().returns("c\ts\tch\ntrue\ttrue\ttrue\n");

            // Convert columns one by one and re-read every value after each conversion. Conversions
            // that the type system rejects are skipped; a supported conversion whose lazy read is
            // broken will fault on the re-read, naming the column here.
            for (String[] conv : CONVERSIONS) {
                try {
                    execute("alter table legacy_all alter column " + conv[0] + " type " + conv[1]);
                } catch (SqlException unsupported) {
                    continue;
                }
                drainWalQueue();
                Assert.assertEquals("re-read after converting " + conv[0] + " -> " + conv[1], 150, readAll(sink));
            }
        });
    }

    // The tests below cover the fixed->fixed pairs that decode straight to the target in a single
    // pass (plan_decode_conversion -> DecodeAs::Target): BYTE/SHORT -> LONG/TIMESTAMP and
    // FLOAT/DOUBLE -> {BYTE,SHORT,INT,LONG,DATE,TIMESTAMP}, exercised against the legacy Required
    // fixture. The Optional-schema matrix in ParquetColumnTypeConversionTest#testFixedWithAllEncodings
    // decodes Optional pages only; here the same single-pass arms must decode legacy Required pages
    // (BYTE/SHORT/FLOAT/DOUBLE stored without def levels) and still match the native ALTER. The
    // FLOAT/DOUBLE sources additionally cover a Required NULL carried as an in-band NaN.
    //
    // Sources are the c_* columns (data in every row) plus the sentinel-bearing n_float / n_double
    // column tops (their NULL survives a native CTAS copy as NaN). The no-sentinel column-top columns
    // (n_byte, n_short) are deliberately excluded: a BYTE/SHORT column top in a legacy Required file
    // reads as the stored 0 (it has no def level to carry NULL -- see
    // testBooleanColumnTopReadsFalseAndConvertsToZero), so a native CTAS copy flattens the column top
    // to 0 while the lazy conversion materialises it as the target NULL sentinel. That is a
    // pre-existing divergence (the pre-flip two-pass decoder produces the same null) unrelated to the
    // single-pass decode, so the CTAS oracle does not hold for those columns.

    @Test
    public void testLegacySinglePassToIntLongMatchNative() throws Exception {
        assertMemoryLeak(() -> assertLegacyConversionsMatchNative(new String[][]{
                {"c_float", "int"}, {"c_double", "long"},
                {"c_byte", "long"}, {"c_short", "timestamp"},
                {"n_float", "int"}, {"n_double", "long"},
        }));
    }

    @Test
    public void testLegacySinglePassToLongIntMatchNative() throws Exception {
        assertMemoryLeak(() -> assertLegacyConversionsMatchNative(new String[][]{
                {"c_float", "long"}, {"c_double", "int"},
                {"c_byte", "timestamp"}, {"c_short", "long"},
                {"n_float", "long"}, {"n_double", "int"},
        }));
    }

    @Test
    public void testLegacySinglePassToByteShortMatchNative() throws Exception {
        // -> byte / -> short exercise the narrow range-check converter arms (different range
        // constants from -> int/-> long). -> date / -> timestamp share the i64 path with -> long,
        // already covered above. A column has one type, so c_float and c_double appear once here.
        assertMemoryLeak(() -> assertLegacyConversionsMatchNative(new String[][]{
                {"c_float", "byte"}, {"c_double", "short"},
                {"n_float", "byte"}, {"n_double", "short"},
        }));
    }

    @Test
    public void testBooleanColumnTopReadsFalseAndConvertsToZero() throws Exception {
        // QuestDB 9.4.2 wrote BOOLEAN as parquet Required, which has no def level to carry a
        // column-top NULL, so n_bool's column-top rows (it was added after the first 100 rows) were
        // materialised as false rather than NULL. Reading them yields false, and converting
        // boolean->int maps false->0 / true->1 with NO NULLs -- unlike a freshly written Optional
        // file, the legacy file cannot recover the column-top NULL for a no-sentinel type. c_bool
        // (data for every row) likewise has no nulls.
        assertMemoryLeak(() -> {
            loadFixture();

            // All 100 column-top rows of n_bool read false.
            assertQuery("select n_bool, count() c from legacy_all where n_int = null")
                    .expectSize().returns("n_bool\tc\nfalse\t100\n");

            execute("alter table legacy_all alter column c_bool type int");
            execute("alter table legacy_all alter column n_bool type int");
            drainWalQueue();

            // boolean->int yields no NULLs for either column (false->0, true->1).
            assertQuery("select count() from legacy_all where c_bool = null")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("select count() from legacy_all where n_bool = null")
                    .noRandomAccess().expectSize().returns("count\n0\n");
            // The 100 column-top rows of n_bool converted to 0.
            assertQuery("select count() from legacy_all where n_int = null and n_bool = 0")
                    .noRandomAccess().expectSize().returns("count\n100\n");
        });
    }

    @Test
    public void testRewriteAfterTypeConversion() throws Exception {
        // An O3 insert into a parquet partition that has pending type conversions routes through
        // rewriteParquetRowGroupWithConversions with REAL conversions: c_byte->string is a
        // fixed->var conversion whose output buffer is malloc'd into tmpBufs and handed to the
        // descriptor. Validates that the (non-owning) descriptor does not double-free it.
        assertMemoryLeak(() -> {
            loadFixture();
            final StringSink sink = new StringSink();
            execute("alter table legacy_all alter column c_byte type string");
            execute("alter table legacy_all alter column c_int type long");
            drainWalQueue();
            Assert.assertEquals(150, readAll(sink));

            execute("insert into legacy_all (c_int, ts) values (999, '2024-01-01T00:00:00.500000Z')");
            drainWalQueue();
            Assert.assertEquals(151, readAll(sink));
        });
    }

    @Test
    public void testRewriteOfLegacyRequiredParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            loadFixture();

            // Decoding the legacy Required parquet must work. Capture a golden native copy of the
            // 150 rows before any rewrite touches the file.
            execute("create table golden as (select * from legacy_all)");
            assertQuery("select count() from legacy_all").noRandomAccess().expectSize().returns("count\n150\n");
            // The second column set was added after the first 100 rows -> column top 100, read NULL
            // for the nullable members.
            assertQuery("select count() from legacy_all where n_int = null").noRandomAccess().expectSize().returns("count\n100\n");
            assertQuery("select count() from legacy_all where n_int != null").noRandomAccess().expectSize().returns("count\n50\n");

            // Out-of-order insert into the parquet partition forces a rewrite. The first row group
            // is merged (re-encoded) while the other 14 would normally be raw-copied; the legacy
            // Required columns make forceFullReencode re-encode them all to Optional instead.
            execute("insert into legacy_all (c_byte, c_int, ts) values (99, 12345, '2024-01-01T00:00:00.500000Z')");
            drainWalQueue();

            assertQuery("select count() from legacy_all").noRandomAccess().expectSize().returns("count\n151\n");
            // The original 150 rows survive the rewrite byte-for-byte (no corruption from
            // Required pages under an Optional footer).
            assertSqlCursors(
                    "select * from golden order by ts",
                    "select * from legacy_all where ts != '2024-01-01T00:00:00.500000Z' order by ts"
            );
        });
    }
}
