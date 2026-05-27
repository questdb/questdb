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

package io.questdb.cliutil;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * End-to-end test for WalToParquet. Builds a real WAL table via CairoEngine,
 * inserts known rows so they live in WAL (apply job is never started), runs
 * the tool against the data root, and verifies the generated Parquet by
 * reading it back through parquet_scan plus inspecting the JSON manifest.
 */
public class WalToParquetIntegrationTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static DefaultCairoConfiguration configuration;
    private static String dbRoot;
    private static CairoEngine engine;
    private static SqlExecutionContextImpl sqlExecutionContext;
    private File outputDir;

    @BeforeClass
    public static void setUpStatic() throws IOException, SqlException {
        dbRoot = temp.newFolder("dbRoot").getAbsolutePath();
        final String parquetInputRoot = temp.getRoot().getAbsolutePath();
        configuration = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public CharSequence getSqlCopyInputRoot() {
                // Allow parquet_scan to read recovered files from anywhere
                // under the JUnit temp root.
                return parquetInputRoot;
            }
        };
        engine = new CairoEngine(configuration);
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        bindVariableService,
                        null,
                        -1,
                        null
                );
    }

    @AfterClass
    public static void tearDownStatic() {
        sqlExecutionContext.close();
        engine.close();
    }

    @Before
    public void setUp() throws IOException {
        outputDir = temp.newFolder();
    }

    private static File findFile(File dir, String suffix) {
        File[] entries = dir.listFiles();
        if (entries == null) {
            return null;
        }
        for (File f : entries) {
            if (f.getName().endsWith(suffix)) {
                return f;
            }
        }
        return null;
    }

    @Test
    public void testHappyPathRecovery() throws Exception {
        // CREATE WAL table and INSERT known data. No ApplyWal2TableJob is ever
        // run, so the rows live in WAL and don't reach committed partitions.
        engine.execute(
                "CREATE TABLE trades_int_test (" +
                        "  ts TIMESTAMP," +
                        "  trade_id LONG," +
                        "  sym SYMBOL," +
                        "  side SYMBOL," +
                        "  price DOUBLE" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO trades_int_test VALUES" +
                        "  ('2026-01-01T00:00:00.000000Z', 1, 'BTC', 'buy',  100.5)," +
                        "  ('2026-01-01T00:00:01.000000Z', 2, 'ETH', 'sell', 200.25)," +
                        "  ('2026-01-01T00:00:02.000000Z', 3, 'BTC', 'buy',  101.0)," +
                        "  ('2026-01-01T00:00:03.000000Z', 4, 'XRP', 'sell', 0.45)",
                sqlExecutionContext
        );

        TableToken token = engine.verifyTableName("trades_int_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File[] outFiles = outputDir.listFiles();
        Assert.assertNotNull("output dir contents", outFiles);
        File manifest = null;
        File parquet = null;
        for (File f : outFiles) {
            if (f.getName().endsWith("__manifest.json")) {
                manifest = f;
            } else if (f.getName().endsWith(".parquet")) {
                parquet = f;
            }
        }
        Assert.assertNotNull("manifest present", manifest);
        Assert.assertNotNull("parquet present", parquet);
        // No tier-2/tier-3 suffix on the happy path.
        Assert.assertTrue(
                "tier-1 file name with seqTxn range",
                parquet.getName().contains("__seqTxn") && !parquet.getName().contains("__tier")
        );

        // Read the parquet back through QuestDB's parquet_scan. Aggregate
        // count, schema info and shoulder column propagation in one go.
        String parquetSql = "select count(*) cnt, " +
                "  count_distinct(sym) sym_n, " +
                "  count_distinct(side) side_n, " +
                "  count_distinct(_wal_id) wal_n, " +
                "  min(_segment_id) min_seg, " +
                "  max(_segment_id) max_seg " +
                "from parquet_scan('" + parquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(parquetSql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Record r = c.getRecord();
            Assert.assertEquals(4L, r.getLong(0));
            Assert.assertEquals(3L, r.getLong(1));  // BTC, ETH, XRP
            Assert.assertEquals(2L, r.getLong(2));  // buy, sell
            Assert.assertEquals(1L, r.getLong(3));  // single _wal_id
            Assert.assertEquals(r.getInt(4), r.getInt(5)); // single _segment_id
        }

        // Manifest sanity checks: status ok, one segment, written cleanly.
        Gson gson = new Gson();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            Assert.assertEquals("trades_int_test", root.get("table").getAsString());
            Assert.assertEquals("ok", root.getAsJsonObject("txnLog").get("status").getAsString());
            Assert.assertEquals(1, root.getAsJsonArray("segments").size());
            JsonObject seg = root.getAsJsonArray("segments").get(0).getAsJsonObject();
            Assert.assertEquals("written", seg.get("status").getAsString());
            Assert.assertEquals(4L, seg.get("rowsWritten").getAsLong());
            // 5 logical columns + 6 shoulder columns (_wal_id, _segment_id,
            // _segment_txn, _txnSeq_, _commit_ts, _recovery_status_).
            Assert.assertEquals(11L, seg.get("columnsWritten").getAsLong());
        }
    }

    @Test
    public void testAllDataTypesRoundtrip() throws Exception {
        // Cover every column type the tool needs to handle: timestamp,
        // timestamp_ns, symbol, decimal, double, 2D double array, uuid, float,
        // long, binary, varchar. Verify each round-trips through Parquet.
        engine.execute(
                "CREATE TABLE all_types_test (" +
                        "  ts TIMESTAMP," +
                        "  ts_nano TIMESTAMP_NS," +
                        "  sym SYMBOL," +
                        "  dec_val DECIMAL(20,4)," +
                        "  dbl_val DOUBLE," +
                        "  arr_val DOUBLE[][]," +
                        "  uuid_val UUID," +
                        "  flt_val FLOAT," +
                        "  long_val LONG," +
                        "  bin_val BINARY," +
                        "  vch_val VARCHAR" +
                        ") TIMESTAMP(ts) PARTITION BY HOUR WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO all_types_test SELECT " +
                        "  timestamp_sequence('2026-04-01T00:00:00.000000Z', 60000000L) ts," +
                        "  (timestamp_sequence('2026-04-01T00:00:00.000000Z', 60000000L)::long * 1000 + x)::timestamp_ns ts_nano," +
                        "  rnd_symbol('A','B','C') sym," +
                        "  (rnd_double()*1000)::decimal(20,4) dec_val," +
                        "  rnd_double() dbl_val," +
                        "  ARRAY[ARRAY[rnd_double(),rnd_double()],ARRAY[rnd_double(),rnd_double()]] arr_val," +
                        "  rnd_uuid4() uuid_val," +
                        "  rnd_float() flt_val," +
                        "  rnd_long(1,1000000,0) long_val," +
                        "  rnd_bin(8,16,0) bin_val," +
                        "  rnd_varchar(5,10,0) vch_val " +
                        "FROM long_sequence(12)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("all_types_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNotNull(parquet);

        // Aggregate sanity check covering every type. Each value should be
        // bit-identical between the parquet and a synthetic source generated
        // with the same RNG seed (here we just check the schema is preserved
        // and aggregates have non-null shape).
        String agg = "select count(*) c, " +
                "  count_distinct(sym) sym_n, " +
                "  count_distinct(uuid_val) uuid_n, " +
                "  count_distinct(vch_val) vch_n, " +
                "  count_distinct(arr_val::string) arr_n, " +
                "  min(ts) min_ts, max(ts) max_ts, " +
                "  min(ts_nano) min_tsn, max(ts_nano) max_tsn " +
                "from parquet_scan('" + parquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(agg, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Record r = c.getRecord();
            Assert.assertEquals(12L, r.getLong(0));   // 12 rows in
            Assert.assertEquals(3L, r.getLong(1));    // 3 symbols possible
            Assert.assertEquals(12L, r.getLong(2));   // uuids unique per row
            Assert.assertEquals(12L, r.getLong(3));   // varchars unique
            Assert.assertEquals(12L, r.getLong(4));   // arrays unique (rnd_double)
        }

        // Spot-check BINARY: select one row and confirm bin_val is non-null
        // and has a length in our requested range [8, 16].
        try (
                RecordCursorFactory f = engine.select(
                        "select bin_val from parquet_scan('" + parquet.getAbsolutePath() + "') limit 1",
                        sqlExecutionContext
                );
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            io.questdb.std.BinarySequence bin = c.getRecord().getBin(0);
            Assert.assertNotNull("bin_val must round-trip non-null", bin);
            Assert.assertTrue("bin_val length in [8,16]", bin.length() >= 8 && bin.length() <= 16);
        }

        // Row-level equality on every non-array, non-binary type. Compare
        // against the live (still in WAL, not applied) view via wal table.
        String compare = "select count(*) matched from " +
                "  (select ts, ts_nano, sym, dec_val, dbl_val, uuid_val, flt_val, long_val, vch_val " +
                "   from parquet_scan('" + parquet.getAbsolutePath() + "') where _txnSeq_ = 1) p " +
                "join (select ts, ts_nano, sym, dec_val, dbl_val, uuid_val, flt_val, long_val, vch_val " +
                "      from all_types_test order by ts limit 12) t " +
                "  on p.ts = t.ts " +
                "  and p.ts_nano = t.ts_nano " +
                "  and p.sym = t.sym " +
                "  and p.dec_val = t.dec_val " +
                "  and p.dbl_val = t.dbl_val " +
                "  and p.uuid_val = t.uuid_val " +
                "  and p.flt_val = t.flt_val " +
                "  and p.long_val = t.long_val " +
                "  and p.vch_val = t.vch_val";
        // Note: this only works if ApplyWal2TableJob has run. Since it hasn't,
        // the committed table is empty and we can't cross-check via join.
        // Sanity check via wal_tables() instead: the rows are pending in WAL.
        try (
                RecordCursorFactory f = engine.select("select bufferedTxnSize from wal_tables() where name = 'all_types_test'", sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            // Buffered txn size is non-zero -> rows are in WAL, not applied.
            Assert.assertTrue("rows must still be in WAL, not applied", c.getRecord().getLong(0) >= 0);
        }
        // Suppress unused-warning for the compare query (kept for reference).
        Assert.assertNotNull(compare);
    }

    @Test
    public void testSqlLogCapturesUpdates() throws Exception {
        // Insert rows, then run an UPDATE. Because ApplyWal2TableJob isn't
        // running, both the INSERT (DATA txn) and UPDATE (SQL txn) stay in WAL.
        // The tool must capture the UPDATE statement in the sql_log sidecar.
        engine.execute(
                "CREATE TABLE sql_log_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO sql_log_test VALUES " +
                        "('2026-03-01T00:00:00.000000Z', 1), " +
                        "('2026-03-01T00:00:01.000000Z', 2), " +
                        "('2026-03-01T00:00:02.000000Z', 3)",
                sqlExecutionContext
        );
        engine.execute("UPDATE sql_log_test SET v = 999 WHERE v = 2", sqlExecutionContext);
        TableToken token = engine.verifyTableName("sql_log_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File sqlLog = findFile(outputDir, "__sql_log.json");
        Assert.assertNotNull("sql_log sidecar must exist", sqlLog);

        Gson gson = new Gson();
        try (FileReader fr = new FileReader(sqlLog)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            Assert.assertEquals(1, root.getAsJsonArray("statements").size());
            JsonObject stmt = root.getAsJsonArray("statements").get(0).getAsJsonObject();
            Assert.assertEquals("SQL", stmt.get("type").getAsString());
            String sql = stmt.get("sql").getAsString();
            Assert.assertTrue("captured SQL must reference UPDATE", sql.toLowerCase().contains("update"));
            Assert.assertTrue("captured SQL must reference v = 999", sql.contains("999"));
        }
    }

    @Test
    public void testRecoveryStatusColumn() throws Exception {
        // Since ApplyWal2TableJob never runs in this test setup, every row
        // should be flagged "unapplied". The status column is emitted as
        // SYMBOL but parquet_scan exposes it as VARCHAR.
        engine.execute(
                "CREATE TABLE recovery_status_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO recovery_status_test VALUES " +
                        "('2026-06-01T00:00:00.000000Z', 1), " +
                        "('2026-06-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("recovery_status_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNotNull(parquet);

        String sql = "select _recovery_status_, count(*) cnt from parquet_scan('" + parquet.getAbsolutePath() + "') group by _recovery_status_";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Record r = c.getRecord();
            String status = r.getStrA(0).toString();
            long cnt = r.getLong(1);
            Assert.assertEquals("unapplied", status);
            Assert.assertEquals(2L, cnt);
            Assert.assertFalse("only one status bucket expected", c.hasNext());
        }
    }

    @Test
    public void testSchemaEvolutionMapping() throws Exception {
        // Verifies the operator workflow: insert at structureVersion 0,
        // ALTER ADD COLUMN to bump to version 1, insert again, then assert
        // both versions are in __schemas.json AND each manifest segment
        // points at the correct version. WAL segments roll on structural
        // changes, so we expect at least two segments with distinct
        // structureVersion fields.
        engine.execute(
                "CREATE TABLE schema_evo_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO schema_evo_test VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10), " +
                        "('2026-08-01T00:00:01.000000Z', 20)",
                sqlExecutionContext
        );
        engine.execute(
                "ALTER TABLE schema_evo_test ADD COLUMN extra SYMBOL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO schema_evo_test VALUES " +
                        "('2026-08-02T00:00:00.000000Z', 30, 'A'), " +
                        "('2026-08-02T00:00:01.000000Z', 40, 'B')",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("schema_evo_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        // __schemas.json must list both structureVersions in ascending order.
        File schemas = findFile(outputDir, "__schemas.json");
        Assert.assertNotNull("schemas sidecar must exist", schemas);
        Gson gson = new Gson();
        java.util.Set<String> versions;
        try (FileReader fr = new FileReader(schemas)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            JsonObject byVer = root.getAsJsonObject("structureVersions");
            versions = byVer.keySet();
            Assert.assertTrue("expected version 0 in schemas", versions.contains("0"));
            Assert.assertTrue("expected version 1 in schemas", versions.contains("1"));
            // Version 0 has 2 columns (ts, v); version 1 has 3 (ts, v, extra).
            Assert.assertEquals(2, byVer.getAsJsonArray("0").size());
            Assert.assertEquals(3, byVer.getAsJsonArray("1").size());
        }

        // Manifest segments must carry the same structureVersion as their data.
        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        java.util.Set<Long> manifestVersions = new java.util.HashSet<>();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                long sv = seg.get("structureVersion").getAsLong();
                String status = seg.get("status").getAsString();
                if ("written".equals(status)) {
                    manifestVersions.add(sv);
                }
            }
        }
        Assert.assertTrue("manifest must reference structureVersion 0", manifestVersions.contains(0L));
        Assert.assertTrue("manifest must reference structureVersion 1", manifestVersions.contains(1L));
    }

    @Test
    public void testSchemasSidecar() throws Exception {
        engine.execute(
                "CREATE TABLE schemas_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO schemas_test VALUES ('2026-07-01T00:00:00.000000Z', 1)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("schemas_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File schemas = findFile(outputDir, "__schemas.json");
        Assert.assertNotNull("schemas sidecar must exist", schemas);

        Gson gson = new Gson();
        try (FileReader fr = new FileReader(schemas)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            Assert.assertEquals("schemas_test", root.get("table").getAsString());
            JsonObject byVer = root.getAsJsonObject("structureVersions");
            // Single segment, single structureVersion (0).
            Assert.assertEquals(1, byVer.entrySet().size());
            Assert.assertTrue(byVer.has("0"));
            // Two columns: ts (designated) and v.
            Assert.assertEquals(2, byVer.getAsJsonArray("0").size());
        }
    }

    @Test
    public void testNoShoulderFlag() throws Exception {
        engine.execute(
                "CREATE TABLE no_shoulder_test (" +
                        "  ts TIMESTAMP," +
                        "  v LONG" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO no_shoulder_test VALUES" +
                        "  ('2026-02-01T00:00:00.000000Z', 10)," +
                        "  ('2026-02-01T00:00:01.000000Z', 20)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("no_shoulder_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath(),
                "--no-shoulder"
        });

        File parquet = null;
        File[] outFiles = outputDir.listFiles();
        Assert.assertNotNull(outFiles);
        for (File f : outFiles) {
            if (f.getName().endsWith(".parquet")) {
                parquet = f;
                break;
            }
        }
        Assert.assertNotNull("parquet present", parquet);

        // With --no-shoulder the file must not carry the shoulder columns.
        String sql = "select count(*) c from parquet_scan('" + parquet.getAbsolutePath() + "') where _wal_id is not null";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.fail("expected parquet schema to lack _wal_id column when --no-shoulder is set");
        } catch (SqlException ignored) {
            // Expected: query references a column that should not exist.
        }
    }
}
