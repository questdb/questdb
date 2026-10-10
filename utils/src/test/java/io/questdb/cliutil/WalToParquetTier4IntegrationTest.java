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
 * Tier-4 fallback coverage. A segment's own {@code _meta} is corrupt; the
 * tool must fall back to a peer segment's {@code _meta} to recover a usable
 * schema and emit a manifest entry that flags the substitution. Isolated in
 * its own class because the test calls {@code engine.clear()} mid-run to
 * drop the engine's metadata cache before mutating {@code _meta} on disk.
 */
public class WalToParquetTier4IntegrationTest {
    // V2 sequencer is required: the tier-4 peer-meta fallback is reached
    // only via writeSegmentToParquetTier3, which itself only fires when
    // _event is unreadable AND the txnlog carries per-txn row counts (V2).
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
    public void testTier4PeerMetaFallback() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestTier4PeerMetaFallback);
    }

    private void doTestTier4PeerMetaFallback() throws Exception {
        // Create a table, insert rows, then ALTER to force a segment roll.
        // After the roll, segment 0 holds the original rows under the old
        // schema; segment 1 holds the post-ALTER rows under the new schema.
        // We corrupt BOTH segment 0's _event and _meta. The _event
        // corruption forces the tool into tier-3 (writeSegmentToParquetTier3,
        // possible under V2 because the txnlog carries per-txn row counts);
        // the _meta corruption then triggers the tier-4 peer-fallback inside
        // that path, which must discover segment 1's _meta and continue.
        engine.execute(
                "CREATE TABLE tier4_test (ts TIMESTAMP, v LONG) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO tier4_test VALUES " +
                        "('2027-06-01T00:00:00.000000Z', 1), " +
                        "('2027-06-01T00:00:01.000000Z', 2)",
                sqlExecutionContext
        );
        // ALTER forces the WAL writer to roll to a new segment.
        engine.execute("ALTER TABLE tier4_test ADD COLUMN extra SYMBOL", sqlExecutionContext);
        engine.execute(
                "INSERT INTO tier4_test VALUES " +
                        "('2027-06-02T00:00:00.000000Z', 3, 'A'), " +
                        "('2027-06-02T00:00:01.000000Z', 4, 'B')",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("tier4_test");

        // Drop engine handles before mutating segment 0 on disk.
        engine.clear();

        String seg0Dir = dbRoot + File.separator + token.getDirName()
                + File.separator + "wal1" + File.separator + "0";

        // Truncate segment 0's _event to force tier-3.
        File seg0Event = new File(seg0Dir, "_event");
        Assert.assertTrue("segment 0 _event must exist before truncation", seg0Event.isFile());
        try (FileWriter w = new FileWriter(seg0Event)) {
            // Truncate to zero bytes.
        }

        // Truncate segment 0's _meta to force the peer-meta fallback inside
        // tier-3. Segment 1's _meta is intact and serves as the peer schema.
        File seg0Meta = new File(seg0Dir, "_meta");
        Assert.assertTrue("segment 0 _meta must exist before truncation", seg0Meta.isFile());
        try (FileWriter w = new FileWriter(seg0Meta)) {
            // Truncate to zero bytes.
        }

        WalToParquet.main(new String[]{
                "--db-root", dbRoot,
                "--table-dir", token.getDirName(),
                "--table-name", token.getTableName(),
                "--table-id", String.valueOf(token.getTableId()),
                "--output-dir", outputDir.getAbsolutePath()
        });

        // Manifest must mention the tier-4 fallback for segment 0.
        File manifest = findFile(outputDir, "__manifest.json");
        Assert.assertNotNull(manifest);
        Gson gson = new Gson();
        boolean foundTier4Note = false;
        boolean seg0Written = false;
        boolean seg1Written = false;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                int segId = seg.get("segmentId").getAsInt();
                String status = seg.get("status").getAsString();
                if (segId == 0 && (status.contains("tier3") || status.equals("written"))) {
                    seg0Written = true;
                }
                if (segId == 1 && "written".equals(status)) {
                    seg1Written = true;
                }
                for (com.google.gson.JsonElement skEl : seg.getAsJsonArray("skippedColumns")) {
                    String skipped = skEl.getAsString();
                    if (skipped.contains("tier-4") && skipped.contains("peer")) {
                        foundTier4Note = true;
                    }
                }
            }
        }
        // The peer-fallback path emits a "tier-4" skippedColumns note. The
        // segment may be marked as a tier-3 fallback (the column reader
        // failed because _meta was corrupt, then tier-3 mmap path engaged).
        Assert.assertTrue("manifest must surface tier-4 peer-meta fallback note", foundTier4Note);
        Assert.assertTrue("segment 0 must produce some output via the fallback", seg0Written);
        Assert.assertTrue("segment 1 (intact) must be written", seg1Written);

        // Verify segment 0's recovered Parquet actually uses the peer schema.
        // The peer (segment 1) has the post-ALTER schema with the `extra`
        // SYMBOL column. A regression that picks a peer with a different
        // schema or fails to use it correctly would emit a Parquet that
        // either lacks the peer column or refers to absent columns.
        File seg0Parquet = findFile(outputDir, "__seg0__seqTxn1-1__tier3.parquet");
        Assert.assertNotNull("segment 0 tier-3 Parquet must exist on disk", seg0Parquet);
        try (
                io.questdb.cairo.sql.RecordCursorFactory f = engine.select(
                        "SELECT count(*) c FROM parquet_scan('" + seg0Parquet.getAbsolutePath() + "')",
                        sqlExecutionContext
                );
                io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals("segment 0 row count must match txnlog (2 rows)", 2L, c.getRecord().getLong(0));
        }

        // Verify the peer-borrowed schema actually drove the column
        // iteration: the peer (segment 1) has `extra SYMBOL` from the
        // ALTER, but segment 0 predates that ALTER so `extra.d` does not
        // exist under segment 0's dir. The tier-4 fallback iterates the
        // peer's column set and skips columns whose .d files are missing
        // - so `extra` must appear in manifest.skippedColumns for
        // segment 0 (proving the peer schema was used), AND must NOT
        // appear in the emitted Parquet (since the data was never
        // written). A regression that picked the wrong peer or used the
        // segment's own corrupt _meta would not list `extra` at all.
        boolean extraSkippedForSeg0 = false;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root2 = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement el : root2.getAsJsonArray("segments")) {
                JsonObject seg = el.getAsJsonObject();
                if (seg.get("segmentId").getAsInt() != 0) {
                    continue;
                }
                for (com.google.gson.JsonElement skEl : seg.getAsJsonArray("skippedColumns")) {
                    String skipped = skEl.getAsString();
                    if (skipped.startsWith("extra ")) {
                        extraSkippedForSeg0 = true;
                        break;
                    }
                }
            }
        }
        Assert.assertTrue("peer schema's `extra` column must appear in segment 0's skippedColumns (proves peer schema drove iteration)", extraSkippedForSeg0);

        // And confirm `extra` is genuinely absent from segment 0's
        // emitted Parquet - parquet_scan must reject the column.
        try (
                io.questdb.cairo.sql.RecordCursorFactory f = engine.select(
                        "SELECT extra FROM parquet_scan('" + seg0Parquet.getAbsolutePath() + "')",
                        sqlExecutionContext
                );
                io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.fail("segment 0's Parquet must NOT contain `extra` (it was skipped due to missing .d); got cursor " + f.getClass());
        } catch (SqlException expected) {
            // Expected: extra was not emitted into segment 0's Parquet.
        }
    }
}
