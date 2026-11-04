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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.ParquetTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * A server-side prepared statement keeps its compiled COPY and runs it on each execution. Of the connection modes that
 * {@code assertWithPgServer(CONN_AWARE_ALL, ...)} runs, the extended ones with prepareThreshold -1 use such a statement
 * from the first execution, and the others compile the COPY again on each of the few executions these tests run.
 */
public class PGCopyExportTest extends BasePGTest {

    // the rows of pe, which createParquetSettingsTable() inserts
    private static final String PE_ROWS = """
            metric\tc\tts
            1\t1\t2024-01-01T00:00:00.000000Z
            2\t2\t2024-01-01T08:00:00.000000Z
            0\t3\t2024-01-01T16:00:00.000000Z
            1\t4\t2024-01-02T00:00:00.000000Z
            2\t5\t2024-01-02T08:00:00.000000Z
            0\t6\t2024-01-02T16:00:00.000000Z
            1\t7\t2024-01-03T00:00:00.000000Z
            2\t8\t2024-01-03T08:00:00.000000Z
            0\t9\t2024-01-03T16:00:00.000000Z
            """;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        // read_parquet() reads the exported files from the input root
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        // assertWithPgServer() runs setUp() before each connection mode, so no mode reads the files of another
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
        }
    }

    @Test
    public void testPreparedCopySelectFollowsParquetSettingOnEveryExecution() throws Exception {
        // The view reads a retyped column of a Parquet partition, so its export goes through a temp table, which
        // takes the PARQUET(...) setting of metric that holds at each execution.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> assertPreparedCopyFollowsParquetSetting(
                connection,
                "COPY (SELECT * FROM pe_v) TO 'pg' WITH FORMAT PARQUET",
                "pg"
        ));
    }

    @Test
    public void testPreparedCopySelectTakesBindValueOfEachExecution() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            final String parquetPath = exportRoot + File.separator + "pgb.parquet";
            try (PreparedStatement statement = connection.prepareStatement("COPY (SELECT * FROM t WHERE x <= ?) TO 'pgb' WITH FORMAT PARQUET")) {
                statement.setInt(1, 1);
                final String firstId = executeCopy(statement);
                assertParquetRows(parquetPath, """
                        x\tts
                        1\t2024-01-01T00:00:00.000000Z
                        """);

                statement.setInt(1, 2);
                final String secondId = executeCopy(statement);
                Assert.assertNotEquals(firstId, secondId);
                assertParquetRows(parquetPath, """
                        x\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-01T01:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testPreparedCopyTableRepartitionFollowsParquetSettingOnEveryExecution() throws Exception {
        // PARTITION_BY MONTH repartitions the table through a temp table, which takes the PARQUET(...) setting of
        // metric that holds at each execution. The table holds one month, so the export writes one file.
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> assertPreparedCopyFollowsParquetSetting(
                connection,
                "COPY pe TO 'pgm' WITH FORMAT PARQUET PARTITION_BY MONTH",
                "pgm"
        ));
    }

    // Sets the PARQUET(...) setting of metric for the next export.
    private static void alterMetricParquetSetting(String setting) throws Exception {
        execute("ALTER TABLE pe ALTER COLUMN metric SET PARQUET(" + setting + ")");
        drainWalAndViewQueues();
        // A pooled reader keeps the PARQUET(...) setting it loaded before the ALTER (#8191), so the test releases the
        // readers for the export to compile its select over the new setting.
        engine.releaseAllReaders();
    }

    /**
     * Creates the WAL table pe, whose metric column has a PARQUET(RLE_DICTIONARY) setting, converts its first day to
     * Parquet and retypes c from INT to LONG after that, and creates the view pe_v over pe.
     */
    private static void createParquetSettingsTable() throws Exception {
        execute("CREATE TABLE pe (metric INT PARQUET(RLE_DICTIONARY), c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO pe VALUES
                (1, 1, '2024-01-01T00:00:00.000000Z'),
                (2, 2, '2024-01-01T08:00:00.000000Z'),
                (0, 3, '2024-01-01T16:00:00.000000Z'),
                (1, 4, '2024-01-02T00:00:00.000000Z'),
                (2, 5, '2024-01-02T08:00:00.000000Z'),
                (0, 6, '2024-01-02T16:00:00.000000Z'),
                (1, 7, '2024-01-03T00:00:00.000000Z'),
                (2, 8, '2024-01-03T08:00:00.000000Z'),
                (0, 9, '2024-01-03T16:00:00.000000Z')
                """);
        drainWalQueue();
        execute("ALTER TABLE pe CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE pe ALTER COLUMN c TYPE LONG");
        drainWalAndViewQueues();
        execute("CREATE VIEW pe_v AS (SELECT * FROM pe)");
        drainWalAndViewQueues();
    }

    private void assertParquetRows(String parquetPath, String expectedRows) throws Exception {
        assertQuery("SELECT * FROM read_parquet('" + parquetPath + "')")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expectedRows);
    }

    private void assertPreparedCopyFollowsParquetSetting(Connection connection, String copySql, String fileName) throws Exception {
        createParquetSettingsTable();
        final String parquetPath = exportRoot + File.separator + fileName + ".parquet";
        final FilesFacade ff = configuration.getFilesFacade();
        try (PreparedStatement statement = connection.prepareStatement(copySql)) {
            final String firstId = executeCopy(statement);
            assertParquetRows(parquetPath, PE_ROWS);
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, ff, 0);

            alterMetricParquetSetting("DELTA_BINARY_PACKED");
            final String secondId = executeCopy(statement);
            Assert.assertNotEquals(firstId, secondId);
            assertParquetRows(parquetPath, PE_ROWS);
            ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0);
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath, ff, 0);

            alterMetricParquetSetting("default");
            final String thirdId = executeCopy(statement);
            Assert.assertNotEquals(secondId, thirdId);
            assertParquetRows(parquetPath, PE_ROWS);
            // metric has no setting now, so it takes the INT default, PLAIN
            ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_PLAIN, 0);
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath, ff, 0);
            ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0);
        }
    }

    // Executes the COPY, waits for its export to end and returns the export id.
    private String executeCopy(PreparedStatement statement) throws Exception {
        final String exportId;
        try (ResultSet resultSet = statement.executeQuery()) {
            Assert.assertTrue(resultSet.next());
            exportId = resultSet.getString(1);
            Assert.assertFalse(resultSet.next());
        }
        // The export job releases the export after it logs the result and drops the temp table, so the next COPY
        // starts after this export ended.
        TestUtils.assertEventually(() -> Assert.assertEquals(
                CopyExportContext.INACTIVE_COPY_ID,
                engine.getCopyExportContext().getActiveExportId()
        ));
        assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "' AND phase = 'success'")
                .noLeakCheck()
                .returns("status\nfinished\n");
        return exportId;
    }
}
