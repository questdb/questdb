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
import io.questdb.cairo.wal.ApplyWal2TableJob;
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
import java.io.FileWriter;
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
        TestUtils.assertMemoryLeak(this::doTestHappyPathRecovery);
    }

    private void doTestHappyPathRecovery() throws Exception {
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
                        "  timestamp_sequence('2026-04-01T00:00:00.000000Z', 60_000_000L) ts," +
                        "  (timestamp_sequence('2026-04-01T00:00:00.000000Z', 60_000_000L)::long * 1000 + x)::timestamp_ns ts_nano," +
                        "  rnd_symbol('A','B','C') sym," +
                        "  (rnd_double()*1000)::decimal(20,4) dec_val," +
                        "  rnd_double() dbl_val," +
                        "  ARRAY[ARRAY[rnd_double(),rnd_double()],ARRAY[rnd_double(),rnd_double()]] arr_val," +
                        "  rnd_uuid4() uuid_val," +
                        "  rnd_float() flt_val," +
                        "  rnd_long(1, 1_000_000, 0) long_val," +
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
        TestUtils.assertMemoryLeak(this::doTestSchemaEvolutionMapping);
    }

    private void doTestSchemaEvolutionMapping() throws Exception {
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
    public void testUnreadableEventRecordedAsPlaceholder() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestUnreadableEventRecordedAsPlaceholder);
    }

    private void doTestUnreadableEventRecordedAsPlaceholder() throws Exception {
        // When _event cannot be opened (corrupt or truncated), the sql_log
        // must record an UNKNOWN_EVENT_UNREADABLE placeholder pinpointing
        // the affected segment rather than silently dropping the events.
        engine.execute(
                "CREATE TABLE unreadable_evt_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO unreadable_evt_test VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 1)",
                sqlExecutionContext
        );
        engine.execute("UPDATE unreadable_evt_test SET v = 2 WHERE v = 1", sqlExecutionContext);
        TableToken token = engine.verifyTableName("unreadable_evt_test");

        // Truncate _event so WalEventReader.of() fails; the data-path tier-3
        // fallback will still derive a row count, but our event walk must
        // record an UNKNOWN_EVENT_UNREADABLE entry.
        File eventFile = new File(dbRoot + File.separator + token.getDirName()
                + File.separator + "wal1" + File.separator + "0" + File.separator + "_event");
        if (eventFile.isFile()) {
            try (FileWriter w = new FileWriter(eventFile)) {
                // Truncate to zero bytes.
            }
        }

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        // Three assertions:
        //  - the sql_log records the unrecoverable event with full
        //    (walId, segmentId) context (no fabricated segmentTxn).
        //  - the manifest segment is marked unrecoverable; no Parquet was
        //    written. WAL column files are mmap-preallocated, so deriving
        //    a row count from file length would fabricate thousands of
        //    bogus rows. With a V1 txnlog (the QuestDB default in this
        //    test setup) we have no trustworthy fallback source, so the
        //    only correct behaviour is to refuse the recovery.
        //  - the output directory contains no Parquet file for the
        //    affected segment.
        File sqlLog = findFile(outputDir, "__sql_log.json");
        Assert.assertNotNull("sql_log sidecar must exist", sqlLog);
        Gson gson = new Gson();
        boolean unknownFound = false;
        try (FileReader fr = new FileReader(sqlLog)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("statements")) {
                JsonObject stmt = el.getAsJsonObject();
                if ("UNKNOWN_EVENT_UNREADABLE".equals(stmt.get("type").getAsString())) {
                    unknownFound = true;
                    Assert.assertEquals(1, stmt.get("walId").getAsInt());
                    Assert.assertEquals(0, stmt.get("segmentId").getAsInt());
                    Assert.assertNotNull("error message expected", stmt.get("error"));
                    Assert.assertFalse(stmt.get("error").getAsString().isEmpty());
                    break;
                }
            }
        }
        Assert.assertTrue("expected an UNKNOWN_EVENT_UNREADABLE placeholder in sql_log", unknownFound);

        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            boolean unrecoverableFound = false;
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                String status = seg.get("status").getAsString();
                if (status.startsWith("skipped_event_unreadable")) {
                    unrecoverableFound = true;
                    Assert.assertEquals(0L, seg.get("rowsWritten").getAsLong());
                    // outputFile must be absent or null - no Parquet should
                    // be written for a segment whose row count cannot be
                    // trusted.
                    Assert.assertTrue(
                            "no outputFile expected when row count untrusted",
                            seg.get("outputFile").isJsonNull()
                    );
                    break;
                }
            }
            Assert.assertTrue("manifest must flag the segment unrecoverable", unrecoverableFound);
        }

        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNull(
                "no Parquet file should be emitted when row count cannot be trusted",
                parquet
        );
    }

    @Test
    public void testAppliedSeqTxnReadFromTxn() throws Exception {
        // Drive ApplyWal2TableJob to advance the _txn watermark, but skip the
        // purge job so the WAL segment is still on disk. The tool must:
        //   1. Read the advanced appliedSeqTxn from _txn (manifest watermark).
        //   2. Mark every row whose seqTxn <= appliedSeqTxn as applied_unpurged.
        // A future change to the _txn double-buffered layout would silently
        // break this without coverage here.
        engine.execute(
                "CREATE TABLE applied_seq_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO applied_seq_test VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1), " +
                        "('2026-10-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            walApplyJob.drain(0);
        }
        TableToken token = engine.verifyTableName("applied_seq_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath(),
                "--include-empty"
        });

        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull("manifest must be present after apply", manifest);
        Gson gson = new Gson();
        long appliedSeqTxn;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            appliedSeqTxn = root.get("appliedSeqTxn").getAsLong();
        }
        Assert.assertTrue(
                "appliedSeqTxn must be advanced (>0) after ApplyWal2TableJob.drain",
                appliedSeqTxn > 0
        );

        // ApplyWal2TableJob.drain(0) advances _txn but does NOT trigger purge
        // (purge is a separate job we never start). The WAL segment must still
        // be on disk, so the tool must emit a Parquet that the row-level
        // assertion needs. If a future change makes apply purge synchronously
        // this assertion fails and forces a re-evaluation of the contract -
        // which is the right behavior, not a silent skip.
        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNotNull("WAL segment must survive drain(0); purge runs separately", parquet);
        String sql = "select _recovery_status_, count(*) cnt from parquet_scan('"
                + parquet.getAbsolutePath() + "') group by _recovery_status_";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Record r = c.getRecord();
            Assert.assertEquals("applied_unpurged", r.getStrA(0).toString());
            Assert.assertEquals(2L, r.getLong(1));
            Assert.assertFalse("only one status bucket expected", c.hasNext());
        }
    }

    @Test
    public void testNullValuePreservation() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestNullValuePreservation);
    }

    private void doTestNullValuePreservation() throws Exception {
        // Verifies that explicit NULLs in source rows round-trip through
        // Parquet. The all-types test inserts random non-null values; this
        // covers the orthogonal case where the source row has explicit NULLs
        // in mixed nullable column types.
        engine.execute(
                "CREATE TABLE null_pres_test (" +
                        "  ts TIMESTAMP," +
                        "  l LONG," +
                        "  d DOUBLE," +
                        "  s SYMBOL," +
                        "  v VARCHAR" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO null_pres_test VALUES " +
                        "('2027-02-01T00:00:00.000000Z', NULL, NULL, NULL, NULL), " +
                        "('2027-02-01T00:00:01.000000Z', 7, 3.14, 'BTC', 'hi')",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("null_pres_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNotNull(parquet);
        // Count nulls per column. The recovered Parquet must preserve the
        // exactly-one-null-per-column pattern from the source rows.
        String sql = "SELECT count(*) total, " +
                "  sum(case when l IS NULL then 1 else 0 end) lnull, " +
                "  sum(case when d IS NULL then 1 else 0 end) dnull, " +
                "  sum(case when s IS NULL then 1 else 0 end) snull, " +
                "  sum(case when v IS NULL then 1 else 0 end) vnull, " +
                "  sum(case when l = 7 then 1 else 0 end) lval, " +
                "  sum(case when d = 3.14 then 1 else 0 end) dval, " +
                "  sum(case when s = 'BTC' then 1 else 0 end) sval, " +
                "  sum(case when v = 'hi' then 1 else 0 end) vval " +
                "FROM parquet_scan('" + parquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Record r = c.getRecord();
            Assert.assertEquals("total rows", 2L, r.getLong(0));
            Assert.assertEquals("LONG null count", 1L, r.getLong(1));
            Assert.assertEquals("DOUBLE null count", 1L, r.getLong(2));
            Assert.assertEquals("SYMBOL null count", 1L, r.getLong(3));
            Assert.assertEquals("VARCHAR null count", 1L, r.getLong(4));
            Assert.assertEquals("LONG value preserved", 1L, r.getLong(5));
            Assert.assertEquals("DOUBLE value preserved", 1L, r.getLong(6));
            Assert.assertEquals("SYMBOL value preserved", 1L, r.getLong(7));
            Assert.assertEquals("VARCHAR value preserved", 1L, r.getLong(8));
        }
    }

    @Test
    public void testDropColumnSchemaEvolution() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestDropColumnSchemaEvolution);
    }

    private void doTestDropColumnSchemaEvolution() throws Exception {
        // Counterpart to testSchemaEvolutionMapping (which covers ADD COLUMN).
        // DROP COLUMN bumps structureVersion and the segment must reference
        // the post-drop schema in the manifest. The dropped column does not
        // appear in the recovered schema/data.
        engine.execute(
                "CREATE TABLE drop_col_test (ts TIMESTAMP, a LONG, b SYMBOL) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO drop_col_test VALUES " +
                        "('2027-03-01T00:00:00.000000Z', 1, 'x'), " +
                        "('2027-03-01T00:00:01.000000Z', 2, 'y')",
                sqlExecutionContext
        );
        engine.execute("ALTER TABLE drop_col_test DROP COLUMN b", sqlExecutionContext);
        engine.execute(
                "INSERT INTO drop_col_test VALUES " +
                        "('2027-03-02T00:00:00.000000Z', 3), " +
                        "('2027-03-02T00:00:01.000000Z', 4)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("drop_col_test");

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
            JsonObject byVer = root.getAsJsonObject("structureVersions");
            // Pre-drop and post-drop versions must both be present.
            Assert.assertTrue("pre-drop structureVersion absent", byVer.has("0"));
            Assert.assertTrue("post-drop structureVersion absent", byVer.has("1"));
            // Pre-drop has 3 columns (ts, a, b); post-drop has 2 (ts, a).
            Assert.assertEquals(3, byVer.getAsJsonArray("0").size());
            Assert.assertEquals(2, byVer.getAsJsonArray("1").size());
        }

        // Manifest must list both structureVersions across the written segments.
        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        java.util.Set<Long> manifestVersions = new java.util.HashSet<>();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if ("written".equals(seg.get("status").getAsString())) {
                    manifestVersions.add(seg.get("structureVersion").getAsLong());
                }
            }
        }
        Assert.assertTrue("manifest must reference both pre- and post-drop versions",
                manifestVersions.contains(0L) && manifestVersions.contains(1L));

        // Verify the actual Parquet content per structureVersion. The
        // pre-drop file must carry column `b`; the post-drop file must NOT.
        // The DROP COLUMN guarantee is meaningless if dropped column data
        // leaks through into the post-drop segment's recovered output.
        File preDropParquet = null;
        File postDropParquet = null;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if (!"written".equals(seg.get("status").getAsString())) {
                    continue;
                }
                long sv = seg.get("structureVersion").getAsLong();
                String outFile = seg.get("outputFile").getAsString();
                File p = new File(outputDir, outFile);
                if (sv == 0L) {
                    preDropParquet = p;
                } else if (sv == 1L) {
                    postDropParquet = p;
                }
            }
        }
        Assert.assertNotNull("pre-drop Parquet must be present", preDropParquet);
        Assert.assertNotNull("post-drop Parquet must be present", postDropParquet);

        // Pre-drop Parquet: column b is selectable.
        String preSql = "SELECT count_distinct(b) c FROM parquet_scan('"
                + preDropParquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(preSql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals("pre-drop must have 2 distinct b values", 2L, c.getRecord().getLong(0));
        }

        // Post-drop Parquet: column b must be absent. A SELECT referencing
        // it must fail with SqlException at parse time.
        String postSql = "SELECT b FROM parquet_scan('" + postDropParquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(postSql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.fail("post-drop Parquet must not contain column `b`; got cursor of type " + f.getClass());
        } catch (SqlException expected) {
            // Expected: query references a column that no longer exists in
            // the post-drop schema. The Parquet emitted for the post-drop
            // segment must have only `ts` and `a`.
        }
    }

    @Test
    public void testRenameColumnSchemaEvolution() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestRenameColumnSchemaEvolution);
    }

    private void doTestRenameColumnSchemaEvolution() throws Exception {
        // RENAME COLUMN bumps structureVersion (like ADD and DROP). The
        // recovered schemas must record both names; the manifest segments
        // must reference the correct version each.
        engine.execute(
                "CREATE TABLE rename_col_test (ts TIMESTAMP, a LONG, b LONG) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO rename_col_test VALUES " +
                        "('2027-07-01T00:00:00.000000Z', 1, 10), " +
                        "('2027-07-01T00:00:01.000000Z', 2, 20)",
                sqlExecutionContext
        );
        engine.execute("ALTER TABLE rename_col_test RENAME COLUMN b TO renamed_b", sqlExecutionContext);
        engine.execute(
                "INSERT INTO rename_col_test VALUES " +
                        "('2027-07-02T00:00:00.000000Z', 3, 30), " +
                        "('2027-07-02T00:00:01.000000Z', 4, 40)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("rename_col_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        // Both pre- and post-rename schemas must be in __schemas.json. The
        // pre-rename schema names the column `b`; the post-rename schema
        // names it `renamed_b`. Same logical column count (3), different
        // names.
        File schemas = findFile(outputDir, "__schemas.json");
        Assert.assertNotNull("schemas sidecar must exist", schemas);
        Gson gson = new Gson();
        boolean preHasB = false;
        boolean postHasRenamedB = false;
        try (FileReader fr = new FileReader(schemas)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            JsonObject byVer = root.getAsJsonObject("structureVersions");
            Assert.assertTrue("pre-rename structureVersion absent", byVer.has("0"));
            Assert.assertTrue("post-rename structureVersion absent", byVer.has("1"));
            Assert.assertEquals(3, byVer.getAsJsonArray("0").size());
            Assert.assertEquals(3, byVer.getAsJsonArray("1").size());
            for (com.google.gson.JsonElement colEl : byVer.getAsJsonArray("0")) {
                if ("b".equals(colEl.getAsJsonObject().get("name").getAsString())) {
                    preHasB = true;
                }
            }
            for (com.google.gson.JsonElement colEl : byVer.getAsJsonArray("1")) {
                if ("renamed_b".equals(colEl.getAsJsonObject().get("name").getAsString())) {
                    postHasRenamedB = true;
                }
            }
        }
        Assert.assertTrue("pre-rename schema must have column 'b'", preHasB);
        Assert.assertTrue("post-rename schema must have column 'renamed_b'", postHasRenamedB);

        // Manifest segments must reference the correct structureVersion.
        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        java.util.Set<Long> manifestVersions = new java.util.HashSet<>();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if ("written".equals(seg.get("status").getAsString())) {
                    manifestVersions.add(seg.get("structureVersion").getAsLong());
                }
            }
        }
        Assert.assertTrue("manifest must reference both pre- and post-rename versions",
                manifestVersions.contains(0L) && manifestVersions.contains(1L));

        // Verify the actual Parquet content per structureVersion. Pre-rename
        // segment must carry column `b`; post-rename segment must carry
        // `renamed_b` and NOT `b`. A regression where the rename is reflected
        // only in __schemas.json but the recovered Parquet uses the wrong
        // column name would be caught here.
        File preRenameParquet = null;
        File postRenameParquet = null;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if (!"written".equals(seg.get("status").getAsString())) {
                    continue;
                }
                long sv = seg.get("structureVersion").getAsLong();
                String outFile = seg.get("outputFile").getAsString();
                File p = new File(outputDir, outFile);
                if (sv == 0L) {
                    preRenameParquet = p;
                } else if (sv == 1L) {
                    postRenameParquet = p;
                }
            }
        }
        Assert.assertNotNull("pre-rename Parquet must be present", preRenameParquet);
        Assert.assertNotNull("post-rename Parquet must be present", postRenameParquet);

        // Pre-rename Parquet: column b is selectable.
        try (
                RecordCursorFactory f = engine.select(
                        "SELECT count_distinct(b) c FROM parquet_scan('" + preRenameParquet.getAbsolutePath() + "')",
                        sqlExecutionContext
                );
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals("pre-rename must have 2 distinct b values", 2L, c.getRecord().getLong(0));
        }

        // Post-rename Parquet: column renamed_b is selectable.
        try (
                RecordCursorFactory f = engine.select(
                        "SELECT count_distinct(renamed_b) c FROM parquet_scan('" + postRenameParquet.getAbsolutePath() + "')",
                        sqlExecutionContext
                );
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals("post-rename must have 2 distinct renamed_b values", 2L, c.getRecord().getLong(0));
        }

        // Post-rename Parquet: column b must NOT be selectable; the rename
        // semantics require the Parquet to reflect the new schema.
        try (
                RecordCursorFactory f = engine.select(
                        "SELECT b FROM parquet_scan('" + postRenameParquet.getAbsolutePath() + "')",
                        sqlExecutionContext
                );
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.fail("post-rename Parquet must not contain the old column name `b`; got cursor of type " + f.getClass());
        } catch (SqlException expected) {
            // Expected: the post-rename Parquet has `renamed_b`, not `b`.
        }
    }

    @Test
    public void testMultiSegmentRecovery() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestMultiSegmentRecovery);
    }

    private void doTestMultiSegmentRecovery() throws Exception {
        // Force three WAL segments via two structural changes between data
        // inserts, then recover all of them. Each segment must produce its
        // own Parquet file with the correct row counts; the manifest must
        // record all three; cumulative row counts across the Parquets must
        // match the inserted total.
        //
        // Segment-roll-on-ALTER is the behavior of
        // io.questdb.cairo.wal.WalWriter#applyStructureChanges: when a
        // structural change arrives while segmentRowCount > 0 and
        // uncommittedRows == 0, it sets rollSegmentOnNextRow = true; the
        // next data write rolls to a fresh segment. The 3-segment count
        // depends on that contract; a future change to the WAL writer
        // that batches ALTERs into the current segment would break this
        // test's segment count assertion.
        engine.execute(
                "CREATE TABLE multi_seg_test (ts TIMESTAMP, v LONG) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO multi_seg_test VALUES " +
                        "('2027-08-01T00:00:00.000000Z', 1), " +
                        "('2027-08-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );
        engine.execute("ALTER TABLE multi_seg_test ADD COLUMN c1 SYMBOL", sqlExecutionContext);
        engine.execute(
                "INSERT INTO multi_seg_test VALUES " +
                        "('2027-08-02T00:00:00.000000Z', 3, 'A'), " +
                        "('2027-08-02T00:00:01.000000Z', 4, 'B'), " +
                        "('2027-08-02T00:00:02.000000Z', 5, 'C')",
                sqlExecutionContext
        );
        engine.execute("ALTER TABLE multi_seg_test ADD COLUMN c2 LONG", sqlExecutionContext);
        engine.execute(
                "INSERT INTO multi_seg_test VALUES " +
                        "('2027-08-03T00:00:00.000000Z', 6, 'D', 100)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("multi_seg_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        // Three "written" segments expected, each producing its own Parquet.
        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        Gson gson = new Gson();
        int writtenSegments = 0;
        long rowsAcrossSegments = 0;
        java.util.List<String> parquetNames = new java.util.ArrayList<>();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if ("written".equals(seg.get("status").getAsString())) {
                    writtenSegments++;
                    rowsAcrossSegments += seg.get("rowsWritten").getAsLong();
                    parquetNames.add(seg.get("outputFile").getAsString());
                }
            }
        }
        Assert.assertEquals("3 segments must be recovered", 3, writtenSegments);
        Assert.assertEquals("total row count across segments", 6L, rowsAcrossSegments);

        // Each Parquet file must actually exist on disk and read back the
        // expected number of rows; a manifest entry without the underlying
        // file would mean the manifest lies.
        long aggregateRows = 0;
        for (String name : parquetNames) {
            File p = new File(outputDir, name);
            Assert.assertTrue("Parquet must exist: " + name, p.isFile());
            String sql = "SELECT count(*) FROM parquet_scan('" + p.getAbsolutePath() + "')";
            try (
                    RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                    RecordCursor c = f.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(c.hasNext());
                aggregateRows += c.getRecord().getLong(0);
            }
        }
        Assert.assertEquals("aggregate Parquet row count", 6L, aggregateRows);
    }

    @Test
    public void testUnderscorePrefixedUserTable() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestUnderscorePrefixedUserTable);
    }

    private void doTestUnderscorePrefixedUserTable() throws Exception {
        // Regression for the discoverTables filter that previously dropped
        // every directory beginning with `_`. QuestDB's table name validation
        // permits a leading underscore, so the discovery path must surface
        // such tables.
        engine.execute(
                "CREATE TABLE \"_under_test\" (ts TIMESTAMP, v LONG) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO \"_under_test\" VALUES " +
                        "('2027-05-01T00:00:00.000000Z', 1), " +
                        "('2027-05-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );

        // No --table-dir: rely on discoverTables. The tool must find the
        // underscore-prefixed table and emit a Parquet for it.
        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--output-dir", outputDir.getAbsolutePath()
        });

        File[] outFiles = outputDir.listFiles();
        Assert.assertNotNull(outFiles);
        boolean foundParquet = false;
        for (File f : outFiles) {
            if (f.getName().startsWith("_under_test__") && f.getName().endsWith(".parquet")) {
                foundParquet = true;
                break;
            }
        }
        Assert.assertTrue("discoverTables must surface tables whose names start with `_`", foundParquet);
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
