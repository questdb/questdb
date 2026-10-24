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
import java.io.IOException;

/**
 * Partial column-file-loss coverage. Isolated in its own class because the
 * test calls {@code engine.clear()} mid-run to drop the WAL writer's mmap
 * handles before deleting a {@code .d} file on disk. Other tests sharing
 * the same engine and dbRoot would otherwise see stale pool state after
 * the clear; this class isolates the failure mode with its own engine
 * and dbRoot.
 */
public class WalToParquetPartialFileIntegrationTest {
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
    public void testPartialColumnFileLoss() throws Exception {
        TestUtils.assertMemoryLeak(this::doTestPartialColumnFileLoss);
    }

    private void doTestPartialColumnFileLoss() throws Exception {
        // When an individual column's .d file is missing, WalReader fails to
        // open the segment. The tool then must:
        //   (a) surface the specific missing column in manifest skippedColumns
        //       so the operator knows exactly what was lost;
        //   (b) mark the segment as skipped_reader_open_failed rather than
        //       silently emitting a Parquet that omits the column without a
        //       trace; and
        //   (c) not emit a Parquet file for this segment (since we cannot
        //       trust the surviving column data when the reader couldn't
        //       construct - this is the current production contract).
        engine.execute(
                "CREATE TABLE partial_loss_test (ts TIMESTAMP, a LONG, b DOUBLE) " +
                        "TIMESTAMP(ts) PARTITION BY DAY WAL",
                sqlExecutionContext
        );
        engine.execute(
                "INSERT INTO partial_loss_test VALUES " +
                        "('2027-04-01T00:00:00.000000Z', 1, 1.5), " +
                        "('2027-04-01T00:00:01.000000Z', 2, 2.5)",
                sqlExecutionContext
        );
        TableToken token = engine.verifyTableName("partial_loss_test");

        // The engine holds mmap handles on the WAL column files via the
        // WalWriter pool. Drop them before mutating on-disk state; otherwise
        // engine.close() at tearDown may segfault on unmapping a deleted
        // file.
        engine.clear();

        File bDataFile = new File(dbRoot + File.separator + token.getDirName()
                + File.separator + "wal1" + File.separator + "0" + File.separator + "b.d");
        Assert.assertTrue("b.d must exist before deletion", bDataFile.isFile());
        Assert.assertTrue("b.d delete must succeed", bDataFile.delete());

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
        boolean foundMissingB = false;
        boolean foundSkipReason = false;
        try (FileReader fr = new FileReader(manifest)) {
            JsonObject root = gson.fromJson(fr, JsonObject.class);
            for (com.google.gson.JsonElement segEl : root.getAsJsonArray("segments")) {
                JsonObject seg = segEl.getAsJsonObject();
                if ("skipped_reader_open_failed".equals(seg.get("status").getAsString())) {
                    foundSkipReason = true;
                }
                for (com.google.gson.JsonElement skEl : seg.getAsJsonArray("skippedColumns")) {
                    String skipped = skEl.getAsString();
                    if (skipped.startsWith("b ")) {
                        foundMissingB = true;
                        break;
                    }
                }
            }
        }
        Assert.assertTrue("manifest must surface b's missing .d file in skippedColumns", foundMissingB);
        Assert.assertTrue("segment must be marked skipped_reader_open_failed", foundSkipReason);

        // No Parquet should be emitted - the tool refuses to produce a
        // partially-recovered output when the canonical reader cannot
        // construct.
        File parquet = findFile(outputDir, ".parquet");
        Assert.assertNull("no Parquet should be emitted when a column file is missing", parquet);
    }
}
