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
import java.io.FileWriter;
import java.io.IOException;

/**
 * Tier-2 fallback coverage. Isolated in its own class because the test
 * truncates a `_txnlog` file that the engine has memory-mapped via the
 * sequencer pool. Surviving the truncation requires `engine.clear()`,
 * which drops cached state across the entire engine. Other tests sharing
 * the same engine and dbRoot could see undefined behavior after the
 * truncation, so this class isolates the failure mode with its own
 * engine and dbRoot.
 */
public class WalToParquetTier2IntegrationTest {
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
    public void testTier2CorruptTxnlog() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestTier2CorruptTxnlog);
    }

    private void doTestTier2CorruptTxnlog() throws Exception {
        // Truncate _txnlog so openTxnLogStrictRO fails and processTable falls
        // back to the filesystem scan (tier 2). Data segments under wal*/N/
        // are intact, so we still recover the rows - just without cross-segment
        // seqTxn ordering and without sqlStatements/structuralChanges (both
        // come from the txnlog walk).
        engine.execute(
                "CREATE TABLE tier2_test (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO tier2_test VALUES " +
                        "('2026-11-01T00:00:00.000000Z', 1), " +
                        "('2026-11-01T00:00:01.000000Z', 2), " +
                        "('2026-11-01T00:00:02.000000Z', 3)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("tier2_test");

        // The engine has _txnlog memory-mapped via the sequencer pool. Truncate
        // it with the mapping still alive and the JVM segfaults inside the
        // unmap path on engine.close(). Drop all pools first so the file is no
        // longer mmap'd, then corrupt it on disk.
        engine.clear();

        File txnlogFile = new File(dbRoot + File.separator + token.getDirName()
                + File.separator + "txn_seq" + File.separator + "_txnlog");
        Assert.assertTrue("_txnlog must exist before truncation", txnlogFile.isFile());
        try (FileWriter w = new FileWriter(txnlogFile)) {
            // Truncate to zero bytes so the format-version read returns -1.
        }

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
            JsonObject txnLog = root.getAsJsonObject("txnLog");
            Assert.assertEquals("error", txnLog.get("status").getAsString());
            Assert.assertNotNull("error message must be recorded", txnLog.get("error"));
            // Filesystem scan must have found the segment despite txnlog loss.
            Assert.assertTrue(
                    "tier-2 scan must surface at least one segment",
                    root.getAsJsonArray("segments").size() > 0
            );
        }

        File parquet = findFile(outputDir, "__tier2.parquet");
        Assert.assertNotNull("tier-2 Parquet must be emitted from filesystem scan", parquet);

        String sql = "select count(*) c from parquet_scan('" + parquet.getAbsolutePath() + "')";
        try (
                RecordCursorFactory f = engine.select(sql, sqlExecutionContext);
                RecordCursor c = f.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(c.hasNext());
            Assert.assertEquals(3L, c.getRecord().getLong(0));
        }
    }
}
