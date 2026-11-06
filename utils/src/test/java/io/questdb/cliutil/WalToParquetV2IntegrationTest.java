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
 * V2 sequencer coverage for the WAL-to-Parquet tool. A separate engine is
 * required because the V2 vs V1 choice is made at table-creation time from
 * the configuration's default partTransactionCount, and the main integration
 * test class shares one engine across tests with the V1 default.
 *
 * <p>The interesting V2-specific path is tier-3 fallback: when {@code _event}
 * is corrupt, V2's per-txn row counts (carried in the txnlog) are the only
 * trustworthy source for the segment's committed row count.
 */
public class WalToParquetV2IntegrationTest {
    // Any positive value switches the sequencer format from V1 to V2.
    // The exact size is irrelevant for these tests; we just need a V2
    // txnlog so getTxnRowCount() is available for the tier-3 fallback.
    private static final int V2_PART_TXN_COUNT = 1024;
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
            public int getDefaultSeqPartTxnCount() {
                return V2_PART_TXN_COUNT;
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
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
    public void testV2TxnlogFormatVersionRecorded() throws Exception {
        // Sanity: the V2 engine config must produce a txnlog whose
        // manifest.txnLog.formatVersion is V2 (encoded as the int 1 in
        // WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2; V1 is encoded as 0).
        // Without this guard a future shift in the default sequencer config
        // could silently move these tests back to V1 and bypass the
        // V2-specific path.
        engine.execute(
                "CREATE TABLE v2_format_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO v2_format_test VALUES ('2026-12-01T00:00:00.000000Z', 1)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("v2_format_test");

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        Gson gson = new Gson();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            int formatVersion = root.getAsJsonObject("txnLog").get("formatVersion").getAsInt();
            // WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2 == 1 (V1 == 0).
            Assert.assertEquals("sanity: sequencer must be V2 for this test class", 1, formatVersion);
        }
    }

    @Test
    public void testV2Tier3RecoversFromCorruptEvent() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestV2Tier3RecoversFromCorruptEvent);
    }

    private void doTestV2Tier3RecoversFromCorruptEvent() throws Exception {
        // Insert known rows under V2 sequencer; truncate _event so the data-path
        // event walk fails. With V2, sumTxnRowCounts() can derive the segment's
        // committed row count from the txnlog (V2 carries getTxnRowCount() per
        // txn), so the tier-3 fallback must emit a __tier3.parquet with the
        // correct row count. This is the V2-specific recovery contract the
        // README documents.
        engine.execute(
                "CREATE TABLE v2_tier3_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO v2_tier3_test VALUES " +
                        "('2026-12-02T00:00:00.000000Z', 10), " +
                        "('2026-12-02T00:00:01.000000Z', 20), " +
                        "('2026-12-02T00:00:02.000000Z', 30), " +
                        "('2026-12-02T00:00:03.000000Z', 40), " +
                        "('2026-12-02T00:00:04.000000Z', 50)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("v2_tier3_test");

        File eventFile = new File(dbRoot + File.separator + token.getDirName()
                + File.separator + "wal1" + File.separator + "0" + File.separator + "_event");
        Assert.assertTrue("_event must exist before truncation", eventFile.isFile());
        try (FileWriter w = new FileWriter(eventFile)) {
            // Truncate to zero bytes.
        }

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File tier3Parquet = findFile(outputDir, "__tier3.parquet");
        Assert.assertNotNull(
                "tier-3 Parquet must be emitted on V2 when _event is unreadable",
                tier3Parquet
        );

        // Row count must match the txnlog-derived total, not a fabricated value
        // from preallocated column-file lengths.
        String sql = "select count(*) c from parquet_scan('" + tier3Parquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals("V2 tier-3 must recover exact row count from txnlog", 5L, c.getRecord().getLong(0));
        }

        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        Gson gson = new Gson();
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            boolean tier3StatusFound = false;
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                String status = seg.get("status").getAsString();
                if (status.contains("tier3")) {
                    tier3StatusFound = true;
                    Assert.assertEquals(5L, seg.get("rowsWritten").getAsLong());
                    break;
                }
            }
            Assert.assertTrue("manifest must mark the segment as tier-3 recovered", tier3StatusFound);
        }
    }

    @Test
    public void testV2Tier3PerRowRecoveryStatusAcrossTxns() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestV2Tier3PerRowRecoveryStatusAcrossTxns);
    }

    private void doTestV2Tier3PerRowRecoveryStatusAcrossTxns() throws Exception {
        // Regression for the tier-3 recovery_status uniformity bug. When a
        // segment contains multiple txns and the apply watermark falls inside
        // the segment (some txns applied, some not), every row's
        // _recovery_status_ must reflect its own txn's relation to the
        // watermark, NOT the segment's last txn's status. Before the fix the
        // tool stamped all rows with one value.
        engine.execute(
                "CREATE TABLE v2_tier3_mixed_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO v2_tier3_mixed_test VALUES " +
                        "('2027-01-01T00:00:00.000000Z', 1), " +
                        "('2027-01-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );
        // Apply ONLY the first txn so the watermark lands at seqTxn=1. Subsequent
        // INSERTs accumulate into the same segment (purge runs separately and we
        // never start it), so we end up with txn 1 applied and txns 2,3 unapplied
        // all sharing one WAL segment.
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            walApplyJob.drain(0);
        }
        engine.execute(
                "INSERT INTO v2_tier3_mixed_test VALUES " +
                        "('2027-01-01T00:00:02.000000Z', 3), " +
                        "('2027-01-01T00:00:03.000000Z', 4)",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO v2_tier3_mixed_test VALUES " +
                        "('2027-01-01T00:00:04.000000Z', 5), " +
                        "('2027-01-01T00:00:05.000000Z', 6)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("v2_tier3_mixed_test");

        // Truncate _event so the tool falls into the tier-3 path.
        File eventFile = new File(dbRoot + File.separator + token.getDirName()
                + File.separator + "wal1" + File.separator + "0" + File.separator + "_event");
        Assert.assertTrue("_event must exist before truncation", eventFile.isFile());
        try (FileWriter w = new FileWriter(eventFile)) {
            // Truncate to zero bytes.
        }

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        File tier3Parquet = findFile(outputDir, "__tier3.parquet");
        Assert.assertNotNull("tier-3 Parquet must be emitted", tier3Parquet);

        // Each row's seqTxn must reflect ITS OWN txn, not a single
        // segment-wide constant. With 3 txns of 2 rows each, we expect:
        //   - rows 0,1 → seqTxn = 1
        //   - rows 2,3 → seqTxn = 2
        //   - rows 4,5 → seqTxn = 3
        // The pre-fix code stamped a single value (lastSegmentTxn's seqTxn)
        // on every row; this test would have failed by asserting just one
        // distinct seqTxn value.
        // Each row's seqTxn must reflect ITS OWN txn. The pre-fix code
        // stamped a single value (lastSegmentTxn's seqTxn) on every row;
        // assert per-(seqTxn, status) row counts so a regression that
        // produces the right cardinality but wrong attribution still fails.
        // The 3 txns we inserted each carried 2 rows, so we expect exactly
        // 2 rows per seqTxn. The first txn was applied via drain(), the
        // remaining two were not, so seqTxn=1 must be applied_unpurged and
        // the others unapplied.
        String detailSql = "SELECT _txnSeq_, _recovery_status_, count(*) c " +
                "FROM parquet_scan('" + tier3Parquet.getAbsolutePath() + "') " +
                "GROUP BY _txnSeq_, _recovery_status_ ORDER BY _txnSeq_";
        java.util.Map<Long, String> seqToStatus = new java.util.HashMap<>();
        java.util.Map<Long, Long> seqToCount = new java.util.HashMap<>();
        try (
                RecordCursorFactory f = engine.select(detailSql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            while (c.hasNext()) {
                Record r = c.getRecord();
                long seqTxn = r.getLong(0);
                String status = r.getStrA(1).toString();
                long count = r.getLong(2);
                seqToStatus.put(seqTxn, status);
                seqToCount.put(seqTxn, count);
            }
        }
        Assert.assertEquals("must see exactly 3 distinct seqTxn buckets", 3, seqToStatus.size());
        Assert.assertEquals("seqTxn=1 must be applied_unpurged (applied via drain)",
                "applied_unpurged", seqToStatus.get(1L));
        Assert.assertEquals("seqTxn=2 must be unapplied (no drain after this insert)",
                "unapplied", seqToStatus.get(2L));
        Assert.assertEquals("seqTxn=3 must be unapplied",
                "unapplied", seqToStatus.get(3L));
        Assert.assertEquals("seqTxn=1 row count", Long.valueOf(2L), seqToCount.get(1L));
        Assert.assertEquals("seqTxn=2 row count", Long.valueOf(2L), seqToCount.get(2L));
        Assert.assertEquals("seqTxn=3 row count", Long.valueOf(2L), seqToCount.get(3L));
    }
}
