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

package io.questdb.test.griffin;

import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExportModel;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class CopyImportTest extends AbstractCairoTest {
    private final boolean walEnabled;

    public CopyImportTest() {
        this.walEnabled = TestUtils.isWal();
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
    }

    @Test
    public void testCopyCancelExtras() throws Exception {
        assertException(
                "copy 'foobar' cancel aw beans;",
                21,
                "unexpected token [aw]"
        );
    }

    @Test
    public void testCopyCancelThrowsExceptionOnNoActiveImport() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy 'foobar' cancel;", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "copy cancel ID format is invalid: 'foobar'");
            }
        });
    }

    @Test
    public void testCopyEmptyFileName() throws Exception {
        assertException(
                "copy x from ''",
                12,
                "file name expected"
        );
    }

    @Test
    public void testCopyFullHack() throws Exception {
        assertException(
                "copy x from '../../../../../'",
                12,
                "'.' is not allowed"
        );
    }

    @Test
    public void testCopyFullHack2() throws Exception {
        assertException(
                "copy x from '\\..\\..\\'",
                13,
                "'.' is not allowed"
        );
    }

    @Test
    public void testCopyNonExistingFile() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'does-not-exist.csv'", sqlExecutionContext);

        CopyRunnable assertion = () -> {
            String query = "select status from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1";
            assertSql("status\nfailed\n", query);
        };
        testCopy(insert, assertion);
    }

    @Test
    public void testCopyThrowsExceptionOnEmptyDelimiter() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with delimiter '';", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "delimiter is empty or contains more than 1 character");
            }
        });
    }

    @Test
    public void testCopyThrowsExceptionOnMultiCharDelimiter() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with delimiter '____';", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "delimiter is empty or contains more than 1 character");
            }
        });
    }

    @Test
    public void testCopyThrowsExceptionOnNonAsciiDelimiter() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with delimiter 'ą';", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "delimiter is not an ascii character");
            }
        });
    }

    @Test
    public void testCopyThrowsExceptionOnUnexpectedOption() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with YadaYadaYada;", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "unexpected option");
            }
        });
    }

    @Test
    public void testDefaultCopyOptions() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                ExportModel model = (ExportModel) compiler.testCompileModel("copy y from 'somefile.csv';", sqlExecutionContext);

                TestUtils.assertEquals("y", model.getTableName());
                TestUtils.assertEquals("'somefile.csv'", model.getFileName().token);
                assertFalse(model.isHeader());
                assertEquals(-1, model.getPartitionBy());
                assertNull(model.getTimestampColumnName());
                assertNull(model.getTimestampFormat());
                assertEquals(-1, model.getDelimiter());
            }
        });
    }

    @Test
    public void testIPv4Copy() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-ipv4.csv'", sqlExecutionContext);

        final String expected = """
                ip\tbytes\tts
                73.10.164.77\t-459221900\t1970-01-01T00:00:00.000000Z
                243.52.105.199\t-1918606750\t1970-01-01T00:00:00.010000Z
                244.205.216.75\t-1815360184\t1970-01-01T00:00:00.020000Z
                245.187.102.39\t1166550061\t1970-01-01T00:00:00.030000Z
                19.63.194.60\t84191888\t1970-01-01T00:00:00.040000Z
                170.231.28.110\t5909999\t1970-01-01T00:00:00.050000Z
                209.254.136.47\t-619137281\t1970-01-01T00:00:00.060000Z
                54.214.228.250\t-842604752\t1970-01-01T00:00:00.070000Z
                118.81.117.131\t-1360454642\t1970-01-01T00:00:00.080000Z
                12.26.233.25\t1602418024\t1970-01-01T00:00:00.090000Z
                91.117.188.200\t903672290\t1970-01-01T00:00:00.100000Z
                159.218.151.146\t866293058\t1970-01-01T00:00:00.110000Z
                153.86.216.128\t217373889\t1970-01-01T00:00:00.120000Z
                25.3.8.75\t-1208159854\t1970-01-01T00:00:00.130000Z
                21.228.187.97\t-1739179160\t1970-01-01T00:00:00.140000Z
                160.105.93.63\t-468408436\t1970-01-01T00:00:00.150000Z
                16.114.232.93\t1697384367\t1970-01-01T00:00:00.160000Z
                105.226.159.168\t209532415\t1970-01-01T00:00:00.170000Z
                176.235.98.227\t-209002510\t1970-01-01T00:00:00.180000Z
                16.102.255.244\t1199682643\t1970-01-01T00:00:00.190000Z
                136.128.196.197\t-1377407717\t1970-01-01T00:00:00.200000Z
                199.7.225.184\t-363280566\t1970-01-01T00:00:00.210000Z
                208.59.5.246\t987322039\t1970-01-01T00:00:00.220000Z
                80.92.251.160\t1627236742\t1970-01-01T00:00:00.230000Z
                73.99.165.49\t243664816\t1970-01-01T00:00:00.240000Z
                251.72.70.137\t-1249493437\t1970-01-01T00:00:00.250000Z
                15.119.158.129\t1650367385\t1970-01-01T00:00:00.260000Z
                237.18.105.225\t2020932751\t1970-01-01T00:00:00.270000Z
                44.41.69.33\t-1345116775\t1970-01-01T00:00:00.280000Z
                241.187.96.19\t-1353619359\t1970-01-01T00:00:00.290000Z
                31.79.232.218\t-1556885244\t1970-01-01T00:00:00.300000Z
                33.49.233.127\t1986265379\t1970-01-01T00:00:00.310000Z
                18.41.137.230\t-1864115267\t1970-01-01T00:00:00.320000Z
                233.227.190.16\t498715877\t1970-01-01T00:00:00.330000Z
                170.71.175.90\t-421397013\t1970-01-01T00:00:00.340000Z
                229.239.7.33\t86741976\t1970-01-01T00:00:00.350000Z
                164.251.178.217\t-789248170\t1970-01-01T00:00:00.360000Z
                74.210.31.253\t897072512\t1970-01-01T00:00:00.370000Z
                184.242.47.14\t-1821348329\t1970-01-01T00:00:00.380000Z
                45.75.194.187\t-900583071\t1970-01-01T00:00:00.390000Z
                203.162.226.25\t1571597743\t1970-01-01T00:00:00.400000Z
                209.146.233.89\t-2023914745\t1970-01-01T00:00:00.410000Z
                54.39.194.213\t521346438\t1970-01-01T00:00:00.420000Z
                124.226.114.109\t1218458875\t1970-01-01T00:00:00.430000Z
                161.68.18.43\t1405147489\t1970-01-01T00:00:00.440000Z
                89.99.13.57\t-1372174387\t1970-01-01T00:00:00.450000Z
                100.34.99.167\t251843878\t1970-01-01T00:00:00.460000Z
                133.87.72.116\t-1165828359\t1970-01-01T00:00:00.470000Z
                109.179.0.231\t820374002\t1970-01-01T00:00:00.480000Z
                108.23.255.136\t888374254\t1970-01-01T00:00:00.490000Z
                161.147.117.95\t-1944364429\t1970-01-01T00:00:00.500000Z
                230.45.134.155\t95450506\t1970-01-01T00:00:00.510000Z
                97.40.47.244\t1931978700\t1970-01-01T00:00:00.520000Z
                146.128.231.18\t1528918796\t1970-01-01T00:00:00.530000Z
                113.50.152.108\t103010556\t1970-01-01T00:00:00.540000Z
                113.37.113.189\t-464980243\t1970-01-01T00:00:00.550000Z
                85.99.107.96\t1600962385\t1970-01-01T00:00:00.560000Z
                21.123.238.92\t-1208481482\t1970-01-01T00:00:00.570000Z
                214.183.239.158\t2069457974\t1970-01-01T00:00:00.580000Z
                106.60.144.127\t427623953\t1970-01-01T00:00:00.590000Z
                129.164.56.195\t-768093703\t1970-01-01T00:00:00.600000Z
                58.117.46.253\t1371024627\t1970-01-01T00:00:00.610000Z
                65.70.41.178\t533740112\t1970-01-01T00:00:00.620000Z
                34.92.111.77\t-553786759\t1970-01-01T00:00:00.630000Z
                188.191.73.29\t-143470402\t1970-01-01T00:00:00.640000Z
                138.16.230.42\t-505052221\t1970-01-01T00:00:00.650000Z
                60.255.187.246\t796746871\t1970-01-01T00:00:00.660000Z
                70.21.112.53\t319455353\t1970-01-01T00:00:00.670000Z
                247.231.246.64\t-100221109\t1970-01-01T00:00:00.680000Z
                192.150.142.71\t-2105059411\t1970-01-01T00:00:00.690000Z
                160.220.153.90\t-891461738\t1970-01-01T00:00:00.700000Z
                165.50.29.236\t-65880808\t1970-01-01T00:00:00.710000Z
                235.252.137.82\t816292060\t1970-01-01T00:00:00.720000Z
                103.12.210.208\t1047309903\t1970-01-01T00:00:00.730000Z
                177.26.112.229\t-1385669311\t1970-01-01T00:00:00.740000Z
                239.186.91.217\t-1679664901\t1970-01-01T00:00:00.750000Z
                132.158.126.25\t2118725967\t1970-01-01T00:00:00.760000Z
                128.97.6.48\t-2114951002\t1970-01-01T00:00:00.770000Z
                231.229.248.123\t480942864\t1970-01-01T00:00:00.780000Z
                79.145.145.17\t-24921522\t1970-01-01T00:00:00.790000Z
                25.249.214.114\t-145767854\t1970-01-01T00:00:00.800000Z
                234.116.65.252\t-1081368842\t1970-01-01T00:00:00.810000Z
                4.11.124.202\t1750759839\t1970-01-01T00:00:00.820000Z
                34.68.207.127\t1162724194\t1970-01-01T00:00:00.830000Z
                192.178.145.180\t1729590565\t1970-01-01T00:00:00.840000Z
                4.99.85.67\t361217113\t1970-01-01T00:00:00.850000Z
                148.43.68.75\t-629516167\t1970-01-01T00:00:00.860000Z
                99.206.221.210\t2106668841\t1970-01-01T00:00:00.870000Z
                120.24.237.20\t-1134229692\t1970-01-01T00:00:00.880000Z
                28.115.175.214\t1167061546\t1970-01-01T00:00:00.890000Z
                23.27.209.206\t1562152960\t1970-01-01T00:00:00.900000Z
                3.120.34.215\t140281782\t1970-01-01T00:00:00.910000Z
                61.199.10.111\t-880708773\t1970-01-01T00:00:00.920000Z
                104.210.105.81\t814549647\t1970-01-01T00:00:00.930000Z
                227.251.209.109\t266567587\t1970-01-01T00:00:00.940000Z
                206.231.182.96\t-431208915\t1970-01-01T00:00:00.950000Z
                123.230.215.199\t-1105635385\t1970-01-01T00:00:00.960000Z
                36.167.171.18\t-632487017\t1970-01-01T00:00:00.970000Z
                179.250.189.96\t406558832\t1970-01-01T00:00:00.980000Z
                237.235.146.199\t1690052673\t1970-01-01T00:00:00.990000Z
                """;
        CopyRunnable assertion = () -> assertQueryNoLeakCheck(
                expected,
                "x",
                null,
                true,
                true
        );
        testCopy(insert, assertion);
    }

    @Test
    public void testParallelCopyCancelChecksImportId() throws Exception {
        assertMemoryLeak(() -> {
            try (CopyImportRequestJob copyRequestJob = new CopyImportRequestJob(engine, 1)) {
                String importId = runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;", sqlExecutionContext);

                try {
                    // selects nothing because ID is invalid
                    assertSql(
                            """
                                    id\tstatus
                                    ffffffffffffffff\tunknown
                                    """,
                            "copy 'ffffffffffffffff' cancel"
                    );

                    // this one should succeed
                    assertSql(
                            "id\tstatus\n" +
                                    importId + "\tcancelled\n", "copy '" + importId + "' cancel"
                    );
                } finally {
                    copyRequestJob.drain(0);
                }

                String query = "select status from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1";
                assertSql("status\ncancelled\n", query);
            }
        });
    }

    @Test
    public void testParallelCopyCancelRejectsSecondReq() throws Exception {
        assertMemoryLeak(() -> {
            try (CopyImportRequestJob copyRequestJob = new CopyImportRequestJob(engine, 1)) {
                String copyID = runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;", sqlExecutionContext);

                try {
                    // this import should be rejected
                    try {
                        runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                                "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "another import may be in progress");
                    }

                    // cancel request should succeed
                    assertSql(
                            "id\tstatus\n" +
                                    copyID + "\tcancelled\n", "copy '" + copyID + "' cancel"
                    );
                } finally {
                    copyRequestJob.drain(0);
                }
                String query = "select status from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1";
                assertSql("status\ncancelled\n", query);
            }
        });
    }

    @Test
    public void testParallelCopyCancelThrowsExceptionOnInvalidImportId() throws Exception {
        assertMemoryLeak(() -> {
            // we need to have an active import in place before the cancellation attempt
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;", sqlExecutionContext);

            try {
                execute("copy 'foobar' cancel;");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "copy cancel ID format is invalid: 'foobar'");
            }

            TestUtils.drainCopyImportJobQueue(engine);
        });
    }

    @Test
    public void testParallelCopyFileWithRawLongTsIntoExistingTable() throws Exception {
        CopyRunnable stmt = () -> {
            execute("""
                    CREATE TABLE reading (
                      readingTypeId SYMBOL,
                      value FLOAT,
                      readingDate TIMESTAMP
                    ) timestamp (readingDate) PARTITION BY DAY;""");
            runAndFetchCopyID("copy reading from 'test-quotes-rawts.csv';", sqlExecutionContext);
        };

        CopyRunnable test = () -> assertSql("cnt\n3\n", "select count(*) cnt from reading");

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoExistingTable() throws Exception {
        CopyRunnable stmt = () -> {
            execute("create table x ( ts timestamp, line symbol, description symbol, d double ) timestamp(ts) partition by MONTH;");
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error SKIP_ROW;", sqlExecutionContext);
        };

        CopyRunnable test = this::assertQuotesTableContentExisting;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoExistingTableWithDefaultWorkDir() throws Exception {
        String inputWorkRootTmp = inputWorkRoot;
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(PropServerConfiguration.TMP_DIRECTORY).$();
            inputWorkRoot = path.toString();
        }

        CopyRunnable stmt = () -> {
            execute("create table x ( ts timestamp, line symbol, description symbol, d double ) timestamp(ts) partition by MONTH;");
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error SKIP_ROW;", sqlExecutionContext);
        };

        CopyRunnable test = this::assertQuotesTableContentExisting;

        testCopy(stmt, test);

        inputWorkRoot = inputWorkRootTmp;
    }

    @Test
    public void testParallelCopyIntoExistingTableWithoutExplicitTimestampAndFormatInCOPY() throws Exception {
        CopyRunnable stmt = () -> {
            execute("create table x ( ts timestamp, line symbol, description symbol, d double ) timestamp(ts) partition by MONTH;");
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true delimiter ',' " +
                    "on error SKIP_ROW; ", sqlExecutionContext);
        };

        CopyRunnable test = this::assertQuotesTableContentExisting;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoExistingTableWithoutExplicitTimestampInCOPY() throws Exception {
        CopyRunnable stmt = () -> {
            execute("create table x ( ts timestamp, line symbol, description symbol, d double ) timestamp(ts) partition by MONTH;");
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error SKIP_ROW; ", sqlExecutionContext);
        };

        CopyRunnable test = this::assertQuotesTableContentExisting;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoNewTable() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID(
                "copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;",
                sqlExecutionContext
        );

        CopyRunnable test = this::assertQuotesTableContent;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoNewTableNoTsFormat() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID(
                "copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "partition by MONTH on error ABORT;",
                sqlExecutionContext
        );

        CopyRunnable test = this::assertQuotesTableContent;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyIntoNewTableWithDefaultWorkDir() throws Exception {
        String inputWorkRootTmp = inputWorkRoot;
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(PropServerConfiguration.TMP_DIRECTORY).$();
            inputWorkRoot = path.toString();
        }

        CopyRunnable stmt = () -> runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT; ", sqlExecutionContext);

        CopyRunnable test = this::assertQuotesTableContent;

        testCopy(stmt, test);

        inputWorkRoot = inputWorkRootTmp;
    }

    @Test
    public void testParallelCopyIntoNewTableWithUringDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_IO_URING_ENABLED, false);

        CopyRunnable stmt = () -> runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT; ", sqlExecutionContext);

        CopyRunnable test = this::assertQuotesTableContent;

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyLogTableStats() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID(
                "copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                        "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' partition by MONTH on error ABORT;",
                sqlExecutionContext
        );

        CopyRunnable test = () -> {
            String query = "select phase, status, rows_handled, rows_imported, errors from " + configuration.getSystemTableNamePrefix() + "text_import_log";
            assertSql("""
                    phase\tstatus\trows_handled\trows_imported\terrors
                    \tstarted\tnull\tnull\t0
                    analyze_file_structure\tstarted\tnull\tnull\t0
                    analyze_file_structure\tfinished\tnull\tnull\t0
                    boundary_check\tstarted\tnull\tnull\t0
                    boundary_check\tfinished\tnull\tnull\t0
                    indexing\tstarted\tnull\tnull\t0
                    indexing\tfinished\tnull\tnull\t0
                    partition_import\tstarted\tnull\tnull\t0
                    partition_import\tfinished\tnull\tnull\t0
                    symbol_table_merge\tstarted\tnull\tnull\t0
                    symbol_table_merge\tfinished\tnull\tnull\t0
                    update_symbol_keys\tstarted\tnull\tnull\t0
                    update_symbol_keys\tfinished\tnull\tnull\t0
                    build_symbol_index\tstarted\tnull\tnull\t0
                    build_symbol_index\tfinished\tnull\tnull\t0
                    move_partitions\tstarted\tnull\tnull\t0
                    move_partitions\tfinished\tnull\tnull\t0
                    attach_partitions\tstarted\tnull\tnull\t0
                    attach_partitions\tfinished\tnull\tnull\t0
                    \tfinished\t1000\t1000\t0
                    """, query);
        };

        testCopy(stmt, test);
    }

    @Test
    public void testParallelCopyRequiresWithBeforeOptions() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("copy x from 'somefile.csv' partition by HOUR;");
                Assert.fail();
            } catch (SqlException e) {
                assertEquals("[27] 'with' expected", e.getMessage());
            }
        });
    }

    @Test
    public void testParallelCopyThrowsExceptionOnBadOnErrorOption() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with on error EXPLODE;", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "invalid 'on error' copy option found");
            }
        });
    }

    @Test
    public void testParallelCopyThrowsExceptionOnBadPartitionByUnit() throws Exception {
        assertMemoryLeak(() -> {
            try {
                runAndFetchCopyID("copy dbRoot from 'test-quotes-big.csv' with partition by jiffy;", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "'NONE', 'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
        });
    }

    @Test
    public void testParallelCopyWithSkipAllAtomicityImportsNothing() throws Exception {
        testCopyWithAtomicity(true, "ABORT", 0);
    }

    @Test
    public void testParallelCopyWithSkipRowAtomicityImportsOnlyRowsWithNoParseErrors() throws Exception {
        // invalid geohash 'GEOHASH' in the CSV file errors out rather than storing NULL silently
        // therefore such row is skipped
        testCopyWithAtomicity(true, "SKIP_ROW", 5);
    }

    @Test
    public void testSerialCopy() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-import.csv'", sqlExecutionContext);

        final String expected = """
                StrSym\tIntSym\tInt_Col\tDoubleCol\tIsoDate\tFmt1Date\tFmt2Date\tPhone\tboolean\tlong
                CMP1\t1\t6992\t2.12060110410675\t2015-01-05T19:15:09.000Z\t2015-01-05T19:15:09.000Z\t2015-01-05T00:00:00.000Z\t6992\ttrue\t4952743
                CMP2\t2\t8014\t5.18098710570484\t2015-01-06T19:15:09.000Z\t2015-01-06T19:15:09.000Z\t2015-01-06T00:00:00.000Z\t8014\tfalse\t10918770
                CMP1\t4\t2599\t1.26877639908344\t2015-01-07T19:15:09.000Z\t2015-01-07T19:15:09.000Z\t2015-01-07T00:00:00.000Z\t2599\ttrue\t80790249
                CMP2\t2\t7610\t0.314211035147309\t2015-01-08T19:15:09.000Z\t2015-01-08T19:15:09.000Z\t2015-01-08T00:00:00.000Z\t7610\tfalse\t62209537
                CMP1\t5\t6608\t6.57507313182577\t2015-01-09T19:15:09.000Z\t2015-01-09T19:15:09.000Z\t2015-01-09T00:00:00.000Z\t6608\ttrue\t86456029
                CMP2\t6\t2699\t3.78073266241699\t2015-01-10T19:15:09.000Z\t2015-01-10T19:15:09.000Z\t2015-01-10T00:00:00.000Z\t2699\tfalse\t28805742
                CMP1\t1\t6902\t2.88266013609245\t2015-01-11T19:15:09.000Z\t2015-01-11T19:15:09.000Z\t2015-01-11T00:00:00.000Z\t6902\ttrue\t32945468
                CMP2\t6\t449\t8.2610409706831\t2015-01-12T19:15:09.000Z\t2015-01-12T19:15:09.000Z\t2015-01-12T00:00:00.000Z\t449\tfalse\t92310232
                CMP1\t7\t8284\t3.2045788760297\t2015-01-13T19:15:09.000Z\t2015-01-13T19:15:09.000Z\t2015-01-13T00:00:00.000Z\t8284\ttrue\t10239799
                CMP2\t3\t1066\t7.5186683377251\t2015-01-14T19:15:09.000Z\t2015-01-14T19:15:09.000Z\t2015-01-14T00:00:00.000Z\t1066\tfalse\t23331405
                CMP1\t4\t6938\t5.11407712241635\t2015-01-15T19:15:09.000Z\t2015-01-15T19:15:09.000Z\t2015-01-15T00:00:00.000Z\t(099)889-776\ttrue\t55296137
                \t6\t4527\t2.48986426275223\t2015-01-16T19:15:09.000Z\t2015-01-16T19:15:09.000Z\t2015-01-16T00:00:00.000Z\t2719\tfalse\t67489936
                CMP1\t7\t6460\t6.39910243218765\t2015-01-17T19:15:09.000Z\t2015-01-17T19:15:09.000Z\t2015-01-17T00:00:00.000Z\t5142\ttrue\t69744070
                CMP2\t1\t7335\t1.07411710545421\t2015-01-18T19:15:09.000Z\t2015-01-18T19:15:09.000Z\t2015-01-18T00:00:00.000Z\t2443\tfalse\t8553585
                CMP1\t5\t1487\t7.40816951030865\t2015-01-19T19:15:09.000Z\t2015-01-19T19:15:09.000Z\t2015-01-19T00:00:00.000Z\t6705\ttrue\t91341680
                CMP2\t5\t8997\t2.71285555325449\t2015-01-20T19:15:09.000Z\t2015-01-20T19:15:09.000Z\t2015-01-20T00:00:00.000Z\t5401\tfalse\t86999930
                CMP1\t1\t7054\t8.12909856671467\t2015-01-21T19:15:09.000Z\t2015-01-21T19:15:09.000Z\t2015-01-21T00:00:00.000Z\t8487\ttrue\t32189412
                \t2\t393\t2.56299464497715\t2015-01-22T19:15:09.000Z\t2015-01-22T19:15:09.000Z\t2015-01-22T00:00:00.000Z\t6862\tfalse\t47274133
                CMP1\t1\t7580\t8.1683173822239\t2015-01-23T19:15:09.000Z\t2015-01-23T19:15:09.000Z\t2015-01-23T00:00:00.000Z\t4646\ttrue\t13982302
                CMP2\t7\t6103\t6.36347207706422\t2015-01-24T19:15:09.000Z\t2015-01-24T19:15:09.000Z\t2015-01-24T00:00:00.000Z\t6047\tfalse\t84767095
                CMP1\t7\t1313\t7.38160170149058\t2015-01-25T19:15:09.000Z\t2015-01-25T19:15:09.000Z\t2015-01-25T00:00:00.000Z\t3837\ttrue\t13178079
                CMP1\t1\t9952\t5.43148486176506\t2015-01-26T19:15:09.000Z\t2015-01-26T19:15:09.000Z\t2015-01-26T00:00:00.000Z\t5578\tfalse\t61000112
                CMP2\t2\t5589\t3.8917106972076\t2015-01-27T19:15:09.000Z\t\t2015-01-27T00:00:00.000Z\t4153\ttrue\t43900701
                CMP1\t3\t9438\t3.90446535777301\t2015-01-28T19:15:09.000Z\t2015-01-28T19:15:09.000Z\t2015-01-28T00:00:00.000Z\t6363\tfalse\t88289909
                CMP2\t8\t8000\t2.27636352181435\t2015-01-29T19:15:09.000Z\t2015-01-29T19:15:09.000Z\t2015-01-29T00:00:00.000Z\t323\ttrue\t14925407
                CMP1\t2\t1581\t9.01423481060192\t2015-01-30T19:15:09.000Z\t2015-01-30T19:15:09.000Z\t2015-01-30T00:00:00.000Z\t9138\tfalse\t68225213
                CMP2\t8\t7067\t9.6284336107783\t2015-01-31T19:15:09.000Z\t2015-01-31T19:15:09.000Z\t2015-01-31T00:00:00.000Z\t8197\ttrue\t58403960
                CMP1\t8\t5313\t8.87764661805704\t2015-02-01T19:15:09.000Z\t2015-02-01T19:15:09.000Z\t2015-02-01T00:00:00.000Z\t2733\tfalse\t69698373
                \t4\t3883\t7.96873019309714\t2015-02-02T19:15:09.000Z\t2015-02-02T19:15:09.000Z\t2015-02-02T00:00:00.000Z\t6912\ttrue\t91147394
                CMP1\t7\t4256\t2.46553522534668\t2015-02-03T19:15:09.000Z\t2015-02-03T19:15:09.000Z\t2015-02-03T00:00:00.000Z\t9453\tfalse\t50278940
                CMP2\t4\t155\t5.08547495584935\t2015-02-04T19:15:09.000Z\t2015-02-04T19:15:09.000Z\t2015-02-04T00:00:00.000Z\t8919\ttrue\t8671995
                CMP1\t7\t4486\tnull\t2015-02-05T19:15:09.000Z\t2015-02-05T19:15:09.000Z\t2015-02-05T00:00:00.000Z\t8670\tfalse\t751877
                CMP2\t2\t6641\t0.0381825352087617\t2015-02-06T19:15:09.000Z\t2015-02-06T19:15:09.000Z\t2015-02-06T00:00:00.000Z\t8331\ttrue\t40909232527
                CMP1\t1\t3579\t0.849663221742958\t2015-02-07T19:15:09.000Z\t2015-02-07T19:15:09.000Z\t2015-02-07T00:00:00.000Z\t9592\tfalse\t11490662
                CMP2\t2\t4770\t2.85092033445835\t2015-02-08T19:15:09.000Z\t2015-02-08T19:15:09.000Z\t2015-02-08T00:00:00.000Z\t253\ttrue\t33766814
                CMP1\t5\t4938\t4.42754498450086\t2015-02-09T19:15:09.000Z\t2015-02-09T19:15:09.000Z\t2015-02-09T00:00:00.000Z\t7817\tfalse\t61983099
                CMP2\t6\t5939\t5.26230568997562\t2015-02-10T19:15:09.000Z\t2015-02-10T19:15:09.000Z\t2015-02-10T00:00:00.000Z\t7857\ttrue\t83851352
                CMP1\t6\t2830\t1.92678665509447\t2015-02-11T19:15:09.000Z\t2015-02-11T19:15:09.000Z\t2015-02-11T00:00:00.000Z\t9647\tfalse\t47528916
                CMP2\t3\t3776\t5.4143834207207\t2015-02-12T19:15:09.000Z\t2015-02-12T19:15:09.000Z\t2015-02-12T00:00:00.000Z\t5368\ttrue\t59341626
                CMP1\t8\t1444\t5.33778431359679\t2015-02-13T19:15:09.000Z\t2015-02-13T19:15:09.000Z\t2015-02-13T00:00:00.000Z\t7425\tfalse\t61302397
                CMP2\t2\t2321\t3.65820386214182\t2015-02-14T19:15:09.000Z\t2015-02-14T19:15:09.000Z\t2015-02-14T00:00:00.000Z\t679\ttrue\t90665386
                CMP1\t7\t3870\t3.42176506761461\t2015-02-15T19:15:09.000Z\t2015-02-15T19:15:09.000Z\t2015-02-15T00:00:00.000Z\t5610\tfalse\t50649828
                CMP2\t4\t1253\t0.541768460534513\t2015-02-16T19:15:09.000Z\t2015-02-16T19:15:09.000Z\t2015-02-16T00:00:00.000Z\t4377\ttrue\t21383690
                CMP1\t4\t268\t3.09822975890711\t2015-02-17T19:15:09.000Z\t2015-02-17T19:15:09.000Z\t2015-02-17T00:00:00.000Z\t669\tfalse\t71326228
                CMP2\t8\t5548\t3.7650444637984\t2015-02-18T19:15:09.000Z\t2015-02-18T19:15:09.000Z\t2015-02-18T00:00:00.000Z\t7369\ttrue\t82105548
                CMP1\t4\tnull\t9.31892040651292\t2015-02-19T19:15:09.000Z\t2015-02-19T19:15:09.000Z\t2015-02-19T00:00:00.000Z\t2022\tfalse\t16097569
                CMP2\t1\t1670\t9.44043743424118\t2015-02-20T19:15:09.000Z\t2015-02-20T19:15:09.000Z\t2015-02-20T00:00:00.000Z\t3235\ttrue\t88917951
                CMP1\t7\t5534\t5.78428176697344\t2015-02-21T19:15:09.000Z\t2015-02-21T19:15:09.000Z\t2015-02-21T00:00:00.000Z\t9650\tfalse\t10261372
                CMP2\t5\t8085\t5.49041963648051\t2015-02-22T19:15:09.000Z\t2015-02-22T19:15:09.000Z\t2015-02-22T00:00:00.000Z\t2211\ttrue\t28722529
                CMP1\t1\t7916\t7.37360095838085\t2015-02-23T19:15:09.000Z\t2015-02-23T19:15:09.000Z\t2015-02-23T00:00:00.000Z\t1598\tfalse\t48269680
                CMP2\t3\t9117\t6.16650991374627\t2015-02-24T19:15:09.000Z\t2015-02-24T19:15:09.000Z\t2015-02-24T00:00:00.000Z\t3588\ttrue\t4354364
                CMP1\t6\t2745\t6.12624417291954\t2015-02-25T19:15:09.000Z\t2015-02-25T19:15:09.000Z\t2015-02-25T00:00:00.000Z\t6149\tfalse\t71925383
                CMP2\t2\t986\t4.00966874323785\t2015-02-26T19:15:09.000Z\t2015-02-26T19:15:09.000Z\t2015-02-26T00:00:00.000Z\t4099\ttrue\t53416732
                CMP1\t7\t8510\t0.829101242125034\t2015-02-27T19:15:09.000Z\t2015-02-27T19:15:09.000Z\t2015-02-27T00:00:00.000Z\t6459\tfalse\t17817647
                CMP2\t6\t2368\t4.37540231039748\t2015-02-28T19:15:09.000Z\t2015-02-28T19:15:09.000Z\t2015-02-28T00:00:00.000Z\t7812\ttrue\t99185079
                CMP1\t6\t1758\t8.40889546554536\t2015-03-01T19:15:09.000Z\t2015-03-01T19:15:09.000Z\t2015-03-01T00:00:00.000Z\t7485\tfalse\t46226610
                CMP2\t4\t4049\t1.08890570467338\t2015-03-02T19:15:09.000Z\t2015-03-02T19:15:09.000Z\t2015-03-02T00:00:00.000Z\t4412\ttrue\t54936589
                CMP1\t7\t7543\t0.195319654885679\t2015-03-03T19:15:09.000Z\t2015-03-03T19:15:09.000Z\t2015-03-03T00:00:00.000Z\t6599\tfalse\t15161204
                CMP2\t3\t4967\t6.85113925952464\t2015-03-04T19:15:09.000Z\t2015-03-04T19:15:09.000Z\t2015-03-04T00:00:00.000Z\t3854\ttrue\t65617919
                CMP1\t8\t5195\t7.67904466483742\t2015-03-05T19:15:09.000Z\t2015-03-05T19:15:09.000Z\t2015-03-05T00:00:00.000Z\t8790\tfalse\t46057340
                CMP2\t6\t6111\t2.53866507206112\t2015-03-06T19:15:09.000Z\t2015-03-06T19:15:09.000Z\t2015-03-06T00:00:00.000Z\t6644\ttrue\t15179632
                CMP1\t5\t3105\t4.80623316485435\t2015-03-07T19:15:09.000Z\t2015-03-07T19:15:09.000Z\t2015-03-07T00:00:00.000Z\t5801\tfalse\t77929708
                CMP2\t7\t6621\t2.95066241407767\t2015-03-08T19:15:09.000Z\t2015-03-08T19:15:09.000Z\t2015-03-08T00:00:00.000Z\t975\ttrue\t83047755
                CMP1\t7\t7327\t1.22000687522814\t2015-03-09T19:15:09.000Z\t2015-03-09T19:15:09.000Z\t2015-03-09T00:00:00.000Z\t7221\tfalse\t8838331
                CMP2\t2\t3972\t8.57570362277329\t2015-03-10T19:15:09.000Z\t2015-03-10T19:15:09.000Z\t2015-03-10T00:00:00.000Z\t5746\ttrue\t26586255
                CMP1\t5\t2969\t4.82038192916662\t2015-03-11T19:15:09.000Z\t2015-03-11T19:15:09.000Z\t2015-03-11T00:00:00.000Z\t1217\tfalse\t65398530
                CMP2\t1\t1731\t6.87037272611633\t2015-03-12T19:15:09.000Z\t2015-03-12T19:15:09.000Z\t2015-03-12T00:00:00.000Z\t7299\ttrue\t61351111
                CMP1\t7\t6530\t9.17741159442812\t2015-03-13T19:15:09.000Z\t2015-03-13T19:15:09.000Z\t2015-03-13T00:00:00.000Z\t4186\tfalse\t68200832
                CMP2\t6\t441\t9.87805142300203\t2015-03-14T19:15:09.000Z\t2015-03-14T19:15:09.000Z\t2015-03-14T00:00:00.000Z\t6256\ttrue\t25615453
                CMP1\t8\t6476\t0.623656720854342\t2015-03-15T19:15:09.000Z\t2015-03-15T19:15:09.000Z\t2015-03-15T00:00:00.000Z\t8916\tfalse\t11378657
                CMP2\t3\t9245\t4.85969736473635\t2015-03-16T19:15:09.000Z\t2015-03-16T19:15:09.000Z\t2015-03-16T00:00:00.000Z\t5364\ttrue\t72902099
                CMP1\t5\t135\t0.71932214545086\t2015-03-17T19:15:09.000Z\t2015-03-17T19:15:09.000Z\t2015-03-17T00:00:00.000Z\t6172\tfalse\t94911256
                CMP2\t6\t5662\t0.934403704013675\t2015-03-18T19:15:09.000Z\t2015-03-18T19:15:09.000Z\t2015-03-18T00:00:00.000Z\t3228\ttrue\t71957668
                CMP1\t7\t8820\t2.26465462474152\t2015-03-19T19:15:09.000Z\t2015-03-19T19:15:09.000Z\t2015-03-19T00:00:00.000Z\t5414\tfalse\t37676934
                CMP2\t1\t1673\t1.13900111755356\t2015-03-20T19:15:09.000Z\t2015-03-20T19:15:09.000Z\t2015-03-20T00:00:00.000Z\t792\ttrue\t45159973
                CMP1\t6\t8704\t7.43929118616506\t2015-03-21T19:15:09.000Z\t2015-03-21T19:15:09.000Z\t2015-03-21T00:00:00.000Z\t4887\tfalse\t27305661
                CMP2\t4\t5380\t8.10803734697402\t2015-03-22T19:15:09.000Z\t2015-03-22T19:15:09.000Z\t2015-03-22T00:00:00.000Z\t8639\ttrue\t90187192
                CMP1\t8\t4176\t8.37395713664591\t2015-03-23T19:15:09.000Z\t2015-03-23T19:15:09.000Z\t2015-03-23T00:00:00.000Z\t7967\tfalse\t32268172
                CMP2\t1\t3419\t3.00495174946263\t2015-03-24T19:15:09.000Z\t2015-03-24T19:15:09.000Z\t2015-03-24T00:00:00.000Z\t7135\ttrue\t42567759
                CMP1\t7\t6785\t3.8469483377412\t2015-03-25T19:15:09.000Z\t2015-03-25T19:15:09.000Z\t2015-03-25T00:00:00.000Z\t9863\tfalse\t154099
                CMP2\t1\t7543\t3.16159424139187\t2015-03-26T19:15:09.000Z\t2015-03-26T19:15:09.000Z\t2015-03-26T00:00:00.000Z\t471\ttrue\t35226692
                CMP1\t2\t178\t1.37678213883191\t2015-03-27T19:15:09.000Z\t2015-03-27T19:15:09.000Z\t2015-03-27T00:00:00.000Z\t1374\tfalse\t80079972
                CMP2\t1\t7256\t6.15871280198917\t2015-03-28T19:15:09.000Z\t2015-03-28T19:15:09.000Z\t2015-03-28T00:00:00.000Z\t7280\ttrue\t86481439
                CMP1\t3\t2116\t7.31438394868746\t2015-03-29T19:15:09.000Z\t2015-03-29T19:15:09.000Z\t2015-03-29T00:00:00.000Z\t6402\tfalse\t60017381
                CMP2\t8\t1606\t8.10372669482604\t2015-03-30T19:15:09.000Z\t2015-03-30T19:15:09.000Z\t2015-03-30T00:00:00.000Z\t4188\ttrue\t74923808
                CMP1\t2\t2361\t2.69874187419191\t2015-03-31T19:15:09.000Z\t2015-03-31T19:15:09.000Z\t2015-03-31T00:00:00.000Z\t5815\tfalse\t16564471
                CMP2\t3\t7280\t8.83913917001337\t2015-04-01T19:15:09.000Z\t2015-04-01T19:15:09.000Z\t2015-04-01T00:00:00.000Z\t9220\ttrue\t7221046
                CMP1\t5\t8158\t1.9249943154864\t2015-04-02T19:15:09.000Z\t2015-04-02T19:15:09.000Z\t2015-04-02T00:00:00.000Z\t3342\tfalse\t28531977
                CMP2\t4\t3006\t8.50523490458727\t2015-04-03T19:15:09.000Z\t2015-04-03T19:15:09.000Z\t2015-04-03T00:00:00.000Z\t7198\ttrue\t17639973
                CMP1\t2\t8058\t3.24236876098439\t2015-04-04T19:15:09.000Z\t2015-04-04T19:15:09.000Z\t2015-04-04T00:00:00.000Z\t890\tfalse\t16188457
                CMP2\t8\t4913\t4.31931799743325\t2015-04-05T19:15:09.000Z\t2015-04-05T19:15:09.000Z\t2015-04-05T00:00:00.000Z\t2151\ttrue\t66148054
                CMP1\t6\t6114\t1.60783329280093\t2015-04-06T19:15:09.000Z\t2015-04-06T19:15:09.000Z\t2015-04-06T00:00:00.000Z\t7156\tfalse\t21576214
                CMP2\t1\t3799\t4.94223219808191\t2015-04-07T19:15:09.000Z\t2015-04-07T19:15:09.000Z\t2015-04-07T00:00:00.000Z\t9016\ttrue\t96119371
                CMP1\t8\t3672\t6.49665022967383\t2015-04-08T19:15:09.000Z\t2015-04-08T19:15:09.000Z\t2015-04-08T00:00:00.000Z\t3467\tfalse\t76381922
                CMP2\t6\t2315\t5.62425469048321\t2015-04-09T19:15:09.000Z\t2015-04-09T19:15:09.000Z\t2015-04-09T00:00:00.000Z\t7586\ttrue\t81396580
                CMP1\t8\t230\t6.72886302694678\t2015-04-10T19:15:09.000Z\t2015-04-10T19:15:09.000Z\t2015-04-10T00:00:00.000Z\t7928\tfalse\t18286886
                CMP2\t2\t2722\t2.23382522119209\t2015-04-11T19:15:09.000Z\t2015-04-11T19:15:09.000Z\t2015-04-11T00:00:00.000Z\t2584\ttrue\t75440358
                CMP1\t7\t3225\t3.55993304867297\t2015-04-12T19:15:09.000Z\t2015-04-12T19:15:09.000Z\t2015-04-12T00:00:00.000Z\t177\tfalse\t87523552
                CMP2\t6\t4692\t2.76645212434232\t2015-04-13T19:15:09.000Z\t2015-04-13T19:15:09.000Z\t2015-04-13T00:00:00.000Z\t4201\ttrue\t28465709
                CMP1\t7\t7116\t6.58135131234303\t2015-04-14T19:15:09.000Z\t2015-04-14T19:15:09.000Z\t2015-04-14T00:00:00.000Z\t3892\tfalse\t48420564
                CMP2\t3\t2457\t5.60338953277096\t2015-04-15T19:15:09.000Z\t2015-04-15T19:15:09.000Z\t2015-04-15T00:00:00.000Z\t7053\ttrue\t33039439
                CMP1\t8\t9975\t0.169386363122612\t2015-04-16T19:15:09.000Z\t2015-04-16T19:15:09.000Z\t2015-04-16T00:00:00.000Z\t6874\tfalse\t6451182
                CMP2\t5\t4952\t0.968641364015639\t2015-04-17T19:15:09.000Z\t2015-04-17T19:15:09.000Z\t2015-04-17T00:00:00.000Z\t1680\ttrue\t77366482
                CMP1\t6\t2024\t1.11267756437883\t2015-04-18T19:15:09.000Z\t2015-04-18T19:15:09.000Z\t2015-04-18T00:00:00.000Z\t3883\tfalse\t65946538
                CMP2\t2\t7689\t6.29668754525483\t2015-04-19T19:15:09.000Z\t2015-04-19T19:15:09.000Z\t2015-04-19T00:00:00.000Z\t254\ttrue\t15272074
                CMP1\t1\t9916\t0.246034313458949\t2015-04-20T19:15:09.000Z\t2015-04-20T19:15:09.000Z\t2015-04-20T00:00:00.000Z\t7768\tfalse\t24934386
                CMP2\t8\t2034\t7.2211763379164\t2015-04-21T19:15:09.000Z\t2015-04-21T19:15:09.000Z\t2015-04-21T00:00:00.000Z\t8514\ttrue\t26112211
                CMP1\t8\t673\t4.48250063927844\t2015-04-22T19:15:09.000Z\t2015-04-22T19:15:09.000Z\t2015-04-22T00:00:00.000Z\t2455\tfalse\t51949360
                CMP2\t3\t6513\t4.39972517313436\t2015-04-23T19:15:09.000Z\t2015-04-23T19:15:09.000Z\t2015-04-23T00:00:00.000Z\t7307\ttrue\t74090772
                CMP1\t2\t8509\t7.21647302387282\t2015-04-24T19:15:09.000Z\t2015-04-24T19:15:09.000Z\t2015-04-24T00:00:00.000Z\t1784\tfalse\t43610015
                CMP2\t1\t9263\t9.72563182003796\t2015-04-25T19:15:09.000Z\t2015-04-25T19:15:09.000Z\t2015-04-25T00:00:00.000Z\t8811\ttrue\t27236992
                CMP1\t7\t9892\t1.50758364936337\t2015-04-26T19:15:09.000Z\t2015-04-26T19:15:09.000Z\t2015-04-26T00:00:00.000Z\t8011\tfalse\t16678001
                CMP2\t4\t4244\t3.88368266867474\t2015-04-27T19:15:09.000Z\t2015-04-27T19:15:09.000Z\t2015-04-27T00:00:00.000Z\t7431\ttrue\t19956646
                CMP1\t6\t9643\t3.09016502927989\t2015-04-28T19:15:09.000Z\t2015-04-28T19:15:09.000Z\t2015-04-28T00:00:00.000Z\t7144\tfalse\t40810637
                CMP2\t5\t3361\t5.21436133189127\t2015-04-29T19:15:09.000Z\t2015-04-29T19:15:09.000Z\t2015-04-29T00:00:00.000Z\t7217\ttrue\t35823849
                CMP1\t2\t5487\t3.5918223625049\t2015-04-30T19:15:09.000Z\t2015-04-30T19:15:09.000Z\t2015-04-30T00:00:00.000Z\t1421\tfalse\t60850489
                CMP2\t8\t4391\t2.72367869038135\t2015-05-01T19:15:09.000Z\t2015-05-01T19:15:09.000Z\t2015-05-01T00:00:00.000Z\t1296\ttrue\t80036797
                CMP1\t4\t2843\t5.22989432094619\t2015-05-02T19:15:09.000Z\t2015-05-02T19:15:09.000Z\t2015-05-02T00:00:00.000Z\t7773\tfalse\t88340142
                CMP2\tnull\t2848\t5.32819046406075\t2015-05-03T19:15:09.000Z\t2015-05-03T19:15:09.000Z\t2015-05-03T00:00:00.000Z\t7628\ttrue\t36732064
                CMP1\tnull\t2776\t5.30948682921007\t2015-05-04T19:15:09.000Z\t2015-05-04T19:15:09.000Z\t2015-05-04T00:00:00.000Z\t5917\tfalse\t59635623
                CMP2\t8\t5256\t8.02117716753855\t2015-05-05T19:15:09.000Z\t2015-05-05T19:15:09.000Z\t2015-05-05T00:00:00.000Z\t4088\ttrue\t50247928
                CMP1\t7\t9250\t0.850080533418804\t2015-05-06T19:15:09.000Z\t2015-05-06T19:15:09.000Z\t2015-05-06T00:00:00.000Z\t519\tfalse\t61373305
                CMP2\t2\t6675\t7.95846320921555\t2015-05-07T19:15:09.000Z\t2015-05-07T19:15:09.000Z\t2015-05-07T00:00:00.000Z\t7530\ttrue\t49634855
                CMP1\t5\t8367\t9.34185237856582\t2015-05-08T19:15:09.000Z\t2015-05-08T19:15:09.000Z\t2015-05-08T00:00:00.000Z\t9714\tfalse\t91106929
                CMP2\t4\t370\t7.84945336403325\t2015-05-09T19:15:09.000Z\t2015-05-09T19:15:09.000Z\t2015-05-09T00:00:00.000Z\t8590\ttrue\t89638043
                CMP1\t7\t4055\t6.49124878691509\t2015-05-10T19:15:09.000Z\t2015-05-10T19:15:09.000Z\t2015-05-10T00:00:00.000Z\t3484\tfalse\t58849380
                CMP2\tnull\t6132\t2.01015920145437\t2015-05-11T19:15:09.000Z\t2015-05-11T19:15:09.000Z\t2015-05-11T00:00:00.000Z\t8132\ttrue\t51493476
                CMP1\t6\t6607\t0.0829047034494579\t2015-05-12T19:15:09.000Z\t2015-05-12T19:15:09.000Z\t2015-05-12T00:00:00.000Z\t1685\tfalse\t88274174
                CMP2\t8\t1049\t9.39520388608798\t2015-05-13T19:15:09.000Z\t2015-05-13T19:15:09.000Z\t2015-05-13T00:00:00.000Z\t7164\ttrue\t49001539
                """;

        CopyRunnable assertion = () -> assertSql(expected, "x");
        testCopy(insert, assertion);
    }

    @Test
    public void testSerialCopyCancelChecksImportId() throws Exception {
        assertMemoryLeak(() -> {
            try (CopyImportRequestJob copyRequestJob = new CopyImportRequestJob(engine, 1)) {
                // decrease smaller buffer otherwise the whole file imported in one go without ever checking the circuit breaker
                setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 1024);
                String copyID = runAndFetchCopyID("copy x from 'test-import.csv' with header true delimiter ',' " +
                        "on error ABORT;", sqlExecutionContext);

                try {
                    // this one should be rejected
                    assertSql(
                            """
                                    id\tstatus
                                    ffffffffffffffff\tunknown
                                    """,
                            "copy 'ffffffffffffffff' cancel"
                    );

                    // this one should succeed
                    assertSql(
                            "id\tstatus\n" +
                                    copyID + "\tcancelled\n", "copy '" + copyID + "' cancel"
                    );
                } finally {
                    copyRequestJob.drain(0);
                }

                String query = "select status from " + configuration.getSystemTableNamePrefix() + "text_import_log limit -1";
                assertSql("status\ncancelled\n", query);
            }
        });
    }

    @Test
    public void testSerialCopyColumnDelimiter() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-numeric-headers.csv' with header true delimiter ','", sqlExecutionContext);

        final String expected = """
                type\tvalue\tactive\tdesc\t_1
                ABC\txy\ta\tbrown fox jumped over the fence\t10
                CDE\tbb\tb\tsentence 1
                sentence 2\t12
                """;

        CopyRunnable assertion = () -> assertSql(expected, "x");
        testCopy(insert, assertion);
    }

    @Test
    public void testSerialCopyForceHeader() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-numeric-headers.csv' with header true", sqlExecutionContext);

        final String expected = """
                type\tvalue\tactive\tdesc\t_1
                ABC\txy\ta\tbrown fox jumped over the fence\t10
                CDE\tbb\tb\tsentence 1
                sentence 2\t12
                """;

        CopyRunnable assertion = () -> assertSql(expected, "x");
        testCopy(insert, assertion);
    }

    @Test
    public void testSerialCopyForceHeader2() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-numeric-headers.csv' with header false", sqlExecutionContext);

        final String expected = """
                f0\tf1\tf2\tf3\tf4
                type\tvalue\tactive\tdesc\t1
                ABC\txy\ta\tbrown fox jumped over the fence\t10
                CDE\tbb\tb\tsentence 1
                sentence 2\t12
                """;

        CopyRunnable assertion = () -> assertSql(expected, "x");
        testCopy(insert, assertion);
    }

    @Test
    public void testSerialCopyIntoExistingTableWithoutExplicitTimestampInCOPY() throws Exception {
        CopyRunnable stmt = () -> {
            execute("create table x ( ts timestamp, line symbol, description symbol, d double ) timestamp(ts);");
            runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error SKIP_ROW; ", sqlExecutionContext);
        };

        CopyRunnable test = () -> {
            String query = "select phase, status, rows_handled, rows_imported, errors from " + configuration.getSystemTableNamePrefix() + "text_import_log";
            assertSql("""
                    phase\tstatus\trows_handled\trows_imported\terrors
                    \tstarted\tnull\tnull\t0
                    \tfinished\t1000\t1000\t0
                    """, query);
        };

        testCopy(stmt, test);
    }

    @Test
    public void testSerialCopyIntoNewNonPartitionedTable() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSUUUZ' on error ABORT;", sqlExecutionContext);

        CopyRunnable test = this::assertQuotesTableContent;

        testCopy(stmt, test);
    }

    @Test
    public void testSerialCopyIntoNewTable() throws Exception {
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from 'test-numeric-headers.csv' with header true partition by NONE", sqlExecutionContext);

        final String expected = """
                type\tvalue\tactive\tdesc\t_1
                ABC\txy\ta\tbrown fox jumped over the fence\t10
                CDE\tbb\tb\tsentence 1
                sentence 2\t12
                """;

        CopyRunnable assertion = () -> assertSql(expected, "x");
        testCopy(insert, assertion);
    }

    @Test
    public void testSerialCopyLogTableStats() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID(
                "copy x from 'test-quotes-big.csv' with header true delimiter ',';",
                sqlExecutionContext
        );

        CopyRunnable test = () -> {
            String query = "select phase, status, rows_handled, rows_imported, errors from " + configuration.getSystemTableNamePrefix() + "text_import_log";
            assertSql("""
                    phase\tstatus\trows_handled\trows_imported\terrors
                    \tstarted\tnull\tnull\t0
                    \tfinished\t1000\t1000\t0
                    """, query);
        };

        testCopy(stmt, test);
    }

    @Test
    public void testSerialCopyNonDefaultTimestampFormat() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID("copy x from 'test-quotes-small.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSZ' partition by NONE on error ABORT;", sqlExecutionContext);

        CopyRunnable test = () -> assertSql("cnt\n3\n", "select count(*) cnt from x");

        testCopy(stmt, test);
    }

    @Test
    public void testSerialCopySkipsAllRowsOnIncorrectTimestampFormat() throws Exception {
        CopyRunnable stmt = () -> runAndFetchCopyID("copy x from 'test-quotes-big.csv' with header true timestamp 'ts' delimiter ',' " +
                "format 'yyyy-MM-ddTHH:mm:ss.SSSZ' partition by NONE on error ABORT;", sqlExecutionContext);

        CopyRunnable test = () -> assertSql("cnt\n0\n", "select count(*) cnt from x");

        testCopy(stmt, test);
    }

    @Test
    public void testSerialCopyWithSkipAllAtomicityImportsNothing() throws Exception {
        testCopyWithAtomicity(false, "ABORT", 0);
    }

    @Test
    public void testSerialCopyWithSkipRowAtomicityImportsOnlyRowsWithNoParseErrors() throws Exception {
        // invalid geohash 'GEOHASH' in the CSV file errors out rather than storing NULL silently
        // therefore such row is skipped
        testCopyWithAtomicity(false, "SKIP_ROW", 5);
    }

    @Test
    public void testSerialCopyWithSmallBuffer() throws Exception {
        // Q: This test uses the same input file as testSerialCopy() yet the expected result has 2 rows missing. How is that possible?
        // A: It's due to the algorithm for table structure inference in the TextLoader.
        //    The inference algorithm uses only a subset of the input file which fits into a single buffer.
        //    It uses the subset to infer column types. With a large enough buffer all rows fit it and the TextLoader can infer
        //    column types to accommodate for all rows.
        //    When the buffer is smaller, then the TextLoader analyzer will see only a subset of the rows which fits into
        //    the buffer and will infer column types based on that subset.
        //    Later, the loading phase may encounter a row wasn't seen by the analyzer and which may have a column
        //    which cannot be stored in the inferred data type.

        //    Here is a content of test-import.csv:
        //StrSym,Int Sym,Int_Col,DoubleCol,IsoDate,Fmt1Date,Fmt2Date,Phone,boolean,long
        //CMP1,1,6992,2.12060110410675,2015-01-05T19:15:09.000Z,2015-01-05 19:15:09,01/05/2015,6992,TRUE,4952743
        //CMP2,2,8014,5.18098710570484,2015-01-06T19:15:09.000Z,2015-01-06 19:15:09,01/06/2015,8014,FALSE,10918770
        //CMP1,4,2599,1.26877639908344,2015-01-07T19:15:09.000Z,2015-01-07 19:15:09,01/07/2015,2599,TRUE,80790249
        //CMP2,2,7610,0.314211035147309,2015-01-08T19:15:09.000Z,2015-01-08 19:15:09,01/08/2015,7610,FALSE,62209537
        //CMP1,5,6608,6.57507313182577,2015-01-09T19:15:09.000Z,2015-01-09 19:15:09,01/09/2015,6608,TRUE,86456029
        //CMP2,6,2699,3.78073266241699,2015-01-10T19:15:09.000Z,2015-01-10 19:15:09,01/10/2015,2699,FALSE,28805742
        //CMP1,1,6902,2.88266013609245,2015-01-11T19:15:09.000Z,2015-01-11 19:15:09,01/11/2015,6902,TRUE,32945468
        //CMP2,6,449,8.2610409706831,2015-01-12T19:15:09.000Z,2015-01-12 19:15:09,01/12/2015,449,FALSE,92310232
        //CMP1,7,8284,3.2045788760297,2015-01-13T19:15:09.000Z,2015-01-13 19:15:09,01/13/2015,8284,TRUE,10239799
        //CMP2,3,1066,7.5186683377251,2015-01-14T19:15:09.000Z,2015-01-14 19:15:09,01/14/2015,1066,FALSE,23331405
        //CMP1,4,6938,5.11407712241635,2015-01-15T19:15:09.000Z,2015-01-15 19:15:09,01/15/2015,(099)889-776,TRUE,55296137
        //[...]
        // When the buffer is large enough to fit all rows then the analyzer will infer the Phone column as "string".
        // Because it'll see the last row where the Phone column is "(099)889-776" and this can only be stored in a string.
        // But when the buffer is smaller then chances are less rows fit into it. In that case the analyzer will see
        // only the rows where the content of the Phone column fits into integer. So it'll infer the column type as int.
        // And later when rows are being written to a table then it will throw an error and skip the row,
        // because "(099)889-776" cannot be stored in a string.

        setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE, 1024);
        CopyRunnable insert = () -> runAndFetchCopyID("copy x from '/test-import.csv'", sqlExecutionContext);
        final String expected = """
                StrSym\tIntSym\tInt_Col\tDoubleCol\tIsoDate\tFmt1Date\tFmt2Date\tPhone\tboolean\tlong
                CMP1\t1\t6992\t2.12060110410675\t2015-01-05T19:15:09.000Z\t2015-01-05T19:15:09.000Z\t2015-01-05T00:00:00.000Z\t6992\ttrue\t4952743
                CMP2\t2\t8014\t5.18098710570484\t2015-01-06T19:15:09.000Z\t2015-01-06T19:15:09.000Z\t2015-01-06T00:00:00.000Z\t8014\tfalse\t10918770
                CMP1\t4\t2599\t1.26877639908344\t2015-01-07T19:15:09.000Z\t2015-01-07T19:15:09.000Z\t2015-01-07T00:00:00.000Z\t2599\ttrue\t80790249
                CMP2\t2\t7610\t0.314211035147309\t2015-01-08T19:15:09.000Z\t2015-01-08T19:15:09.000Z\t2015-01-08T00:00:00.000Z\t7610\tfalse\t62209537
                CMP1\t5\t6608\t6.57507313182577\t2015-01-09T19:15:09.000Z\t2015-01-09T19:15:09.000Z\t2015-01-09T00:00:00.000Z\t6608\ttrue\t86456029
                CMP2\t6\t2699\t3.78073266241699\t2015-01-10T19:15:09.000Z\t2015-01-10T19:15:09.000Z\t2015-01-10T00:00:00.000Z\t2699\tfalse\t28805742
                CMP1\t1\t6902\t2.88266013609245\t2015-01-11T19:15:09.000Z\t2015-01-11T19:15:09.000Z\t2015-01-11T00:00:00.000Z\t6902\ttrue\t32945468
                CMP2\t6\t449\t8.2610409706831\t2015-01-12T19:15:09.000Z\t2015-01-12T19:15:09.000Z\t2015-01-12T00:00:00.000Z\t449\tfalse\t92310232
                CMP1\t7\t8284\t3.2045788760297\t2015-01-13T19:15:09.000Z\t2015-01-13T19:15:09.000Z\t2015-01-13T00:00:00.000Z\t8284\ttrue\t10239799
                CMP2\t3\t1066\t7.5186683377251\t2015-01-14T19:15:09.000Z\t2015-01-14T19:15:09.000Z\t2015-01-14T00:00:00.000Z\t1066\tfalse\t23331405
                \t6\t4527\t2.48986426275223\t2015-01-16T19:15:09.000Z\t2015-01-16T19:15:09.000Z\t2015-01-16T00:00:00.000Z\t2719\tfalse\t67489936
                CMP1\t7\t6460\t6.39910243218765\t2015-01-17T19:15:09.000Z\t2015-01-17T19:15:09.000Z\t2015-01-17T00:00:00.000Z\t5142\ttrue\t69744070
                CMP2\t1\t7335\t1.07411710545421\t2015-01-18T19:15:09.000Z\t2015-01-18T19:15:09.000Z\t2015-01-18T00:00:00.000Z\t2443\tfalse\t8553585
                CMP1\t5\t1487\t7.40816951030865\t2015-01-19T19:15:09.000Z\t2015-01-19T19:15:09.000Z\t2015-01-19T00:00:00.000Z\t6705\ttrue\t91341680
                CMP2\t5\t8997\t2.71285555325449\t2015-01-20T19:15:09.000Z\t2015-01-20T19:15:09.000Z\t2015-01-20T00:00:00.000Z\t5401\tfalse\t86999930
                CMP1\t1\t7054\t8.12909856671467\t2015-01-21T19:15:09.000Z\t2015-01-21T19:15:09.000Z\t2015-01-21T00:00:00.000Z\t8487\ttrue\t32189412
                \t2\t393\t2.56299464497715\t2015-01-22T19:15:09.000Z\t2015-01-22T19:15:09.000Z\t2015-01-22T00:00:00.000Z\t6862\tfalse\t47274133
                CMP1\t1\t7580\t8.1683173822239\t2015-01-23T19:15:09.000Z\t2015-01-23T19:15:09.000Z\t2015-01-23T00:00:00.000Z\t4646\ttrue\t13982302
                CMP2\t7\t6103\t6.36347207706422\t2015-01-24T19:15:09.000Z\t2015-01-24T19:15:09.000Z\t2015-01-24T00:00:00.000Z\t6047\tfalse\t84767095
                CMP1\t7\t1313\t7.38160170149058\t2015-01-25T19:15:09.000Z\t2015-01-25T19:15:09.000Z\t2015-01-25T00:00:00.000Z\t3837\ttrue\t13178079
                CMP1\t1\t9952\t5.43148486176506\t2015-01-26T19:15:09.000Z\t2015-01-26T19:15:09.000Z\t2015-01-26T00:00:00.000Z\t5578\tfalse\t61000112
                CMP2\t2\t5589\t3.8917106972076\t2015-01-27T19:15:09.000Z\t\t2015-01-27T00:00:00.000Z\t4153\ttrue\t43900701
                CMP1\t3\t9438\t3.90446535777301\t2015-01-28T19:15:09.000Z\t2015-01-28T19:15:09.000Z\t2015-01-28T00:00:00.000Z\t6363\tfalse\t88289909
                CMP2\t8\t8000\t2.27636352181435\t2015-01-29T19:15:09.000Z\t2015-01-29T19:15:09.000Z\t2015-01-29T00:00:00.000Z\t323\ttrue\t14925407
                CMP1\t2\t1581\t9.01423481060192\t2015-01-30T19:15:09.000Z\t2015-01-30T19:15:09.000Z\t2015-01-30T00:00:00.000Z\t9138\tfalse\t68225213
                CMP2\t8\t7067\t9.6284336107783\t2015-01-31T19:15:09.000Z\t2015-01-31T19:15:09.000Z\t2015-01-31T00:00:00.000Z\t8197\ttrue\t58403960
                CMP1\t8\t5313\t8.87764661805704\t2015-02-01T19:15:09.000Z\t2015-02-01T19:15:09.000Z\t2015-02-01T00:00:00.000Z\t2733\tfalse\t69698373
                \t4\t3883\t7.96873019309714\t2015-02-02T19:15:09.000Z\t2015-02-02T19:15:09.000Z\t2015-02-02T00:00:00.000Z\t6912\ttrue\t91147394
                CMP1\t7\t4256\t2.46553522534668\t2015-02-03T19:15:09.000Z\t2015-02-03T19:15:09.000Z\t2015-02-03T00:00:00.000Z\t9453\tfalse\t50278940
                CMP2\t4\t155\t5.08547495584935\t2015-02-04T19:15:09.000Z\t2015-02-04T19:15:09.000Z\t2015-02-04T00:00:00.000Z\t8919\ttrue\t8671995
                CMP1\t7\t4486\tnull\t2015-02-05T19:15:09.000Z\t2015-02-05T19:15:09.000Z\t2015-02-05T00:00:00.000Z\t8670\tfalse\t751877
                CMP1\t1\t3579\t0.849663221742958\t2015-02-07T19:15:09.000Z\t2015-02-07T19:15:09.000Z\t2015-02-07T00:00:00.000Z\t9592\tfalse\t11490662
                CMP2\t2\t4770\t2.85092033445835\t2015-02-08T19:15:09.000Z\t2015-02-08T19:15:09.000Z\t2015-02-08T00:00:00.000Z\t253\ttrue\t33766814
                CMP1\t5\t4938\t4.42754498450086\t2015-02-09T19:15:09.000Z\t2015-02-09T19:15:09.000Z\t2015-02-09T00:00:00.000Z\t7817\tfalse\t61983099
                CMP2\t6\t5939\t5.26230568997562\t2015-02-10T19:15:09.000Z\t2015-02-10T19:15:09.000Z\t2015-02-10T00:00:00.000Z\t7857\ttrue\t83851352
                CMP1\t6\t2830\t1.92678665509447\t2015-02-11T19:15:09.000Z\t2015-02-11T19:15:09.000Z\t2015-02-11T00:00:00.000Z\t9647\tfalse\t47528916
                CMP2\t3\t3776\t5.4143834207207\t2015-02-12T19:15:09.000Z\t2015-02-12T19:15:09.000Z\t2015-02-12T00:00:00.000Z\t5368\ttrue\t59341626
                CMP1\t8\t1444\t5.33778431359679\t2015-02-13T19:15:09.000Z\t2015-02-13T19:15:09.000Z\t2015-02-13T00:00:00.000Z\t7425\tfalse\t61302397
                CMP2\t2\t2321\t3.65820386214182\t2015-02-14T19:15:09.000Z\t2015-02-14T19:15:09.000Z\t2015-02-14T00:00:00.000Z\t679\ttrue\t90665386
                CMP1\t7\t3870\t3.42176506761461\t2015-02-15T19:15:09.000Z\t2015-02-15T19:15:09.000Z\t2015-02-15T00:00:00.000Z\t5610\tfalse\t50649828
                CMP2\t4\t1253\t0.541768460534513\t2015-02-16T19:15:09.000Z\t2015-02-16T19:15:09.000Z\t2015-02-16T00:00:00.000Z\t4377\ttrue\t21383690
                CMP1\t4\t268\t3.09822975890711\t2015-02-17T19:15:09.000Z\t2015-02-17T19:15:09.000Z\t2015-02-17T00:00:00.000Z\t669\tfalse\t71326228
                CMP2\t8\t5548\t3.7650444637984\t2015-02-18T19:15:09.000Z\t2015-02-18T19:15:09.000Z\t2015-02-18T00:00:00.000Z\t7369\ttrue\t82105548
                CMP1\t4\tnull\t9.31892040651292\t2015-02-19T19:15:09.000Z\t2015-02-19T19:15:09.000Z\t2015-02-19T00:00:00.000Z\t2022\tfalse\t16097569
                CMP2\t1\t1670\t9.44043743424118\t2015-02-20T19:15:09.000Z\t2015-02-20T19:15:09.000Z\t2015-02-20T00:00:00.000Z\t3235\ttrue\t88917951
                CMP1\t7\t5534\t5.78428176697344\t2015-02-21T19:15:09.000Z\t2015-02-21T19:15:09.000Z\t2015-02-21T00:00:00.000Z\t9650\tfalse\t10261372
                CMP2\t5\t8085\t5.49041963648051\t2015-02-22T19:15:09.000Z\t2015-02-22T19:15:09.000Z\t2015-02-22T00:00:00.000Z\t2211\ttrue\t28722529
                CMP1\t1\t7916\t7.37360095838085\t2015-02-23T19:15:09.000Z\t2015-02-23T19:15:09.000Z\t2015-02-23T00:00:00.000Z\t1598\tfalse\t48269680
                CMP2\t3\t9117\t6.16650991374627\t2015-02-24T19:15:09.000Z\t2015-02-24T19:15:09.000Z\t2015-02-24T00:00:00.000Z\t3588\ttrue\t4354364
                CMP1\t6\t2745\t6.12624417291954\t2015-02-25T19:15:09.000Z\t2015-02-25T19:15:09.000Z\t2015-02-25T00:00:00.000Z\t6149\tfalse\t71925383
                CMP2\t2\t986\t4.00966874323785\t2015-02-26T19:15:09.000Z\t2015-02-26T19:15:09.000Z\t2015-02-26T00:00:00.000Z\t4099\ttrue\t53416732
                CMP1\t7\t8510\t0.829101242125034\t2015-02-27T19:15:09.000Z\t2015-02-27T19:15:09.000Z\t2015-02-27T00:00:00.000Z\t6459\tfalse\t17817647
                CMP2\t6\t2368\t4.37540231039748\t2015-02-28T19:15:09.000Z\t2015-02-28T19:15:09.000Z\t2015-02-28T00:00:00.000Z\t7812\ttrue\t99185079
                CMP1\t6\t1758\t8.40889546554536\t2015-03-01T19:15:09.000Z\t2015-03-01T19:15:09.000Z\t2015-03-01T00:00:00.000Z\t7485\tfalse\t46226610
                CMP2\t4\t4049\t1.08890570467338\t2015-03-02T19:15:09.000Z\t2015-03-02T19:15:09.000Z\t2015-03-02T00:00:00.000Z\t4412\ttrue\t54936589
                CMP1\t7\t7543\t0.195319654885679\t2015-03-03T19:15:09.000Z\t2015-03-03T19:15:09.000Z\t2015-03-03T00:00:00.000Z\t6599\tfalse\t15161204
                CMP2\t3\t4967\t6.85113925952464\t2015-03-04T19:15:09.000Z\t2015-03-04T19:15:09.000Z\t2015-03-04T00:00:00.000Z\t3854\ttrue\t65617919
                CMP1\t8\t5195\t7.67904466483742\t2015-03-05T19:15:09.000Z\t2015-03-05T19:15:09.000Z\t2015-03-05T00:00:00.000Z\t8790\tfalse\t46057340
                CMP2\t6\t6111\t2.53866507206112\t2015-03-06T19:15:09.000Z\t2015-03-06T19:15:09.000Z\t2015-03-06T00:00:00.000Z\t6644\ttrue\t15179632
                CMP1\t5\t3105\t4.80623316485435\t2015-03-07T19:15:09.000Z\t2015-03-07T19:15:09.000Z\t2015-03-07T00:00:00.000Z\t5801\tfalse\t77929708
                CMP2\t7\t6621\t2.95066241407767\t2015-03-08T19:15:09.000Z\t2015-03-08T19:15:09.000Z\t2015-03-08T00:00:00.000Z\t975\ttrue\t83047755
                CMP1\t7\t7327\t1.22000687522814\t2015-03-09T19:15:09.000Z\t2015-03-09T19:15:09.000Z\t2015-03-09T00:00:00.000Z\t7221\tfalse\t8838331
                CMP2\t2\t3972\t8.57570362277329\t2015-03-10T19:15:09.000Z\t2015-03-10T19:15:09.000Z\t2015-03-10T00:00:00.000Z\t5746\ttrue\t26586255
                CMP1\t5\t2969\t4.82038192916662\t2015-03-11T19:15:09.000Z\t2015-03-11T19:15:09.000Z\t2015-03-11T00:00:00.000Z\t1217\tfalse\t65398530
                CMP2\t1\t1731\t6.87037272611633\t2015-03-12T19:15:09.000Z\t2015-03-12T19:15:09.000Z\t2015-03-12T00:00:00.000Z\t7299\ttrue\t61351111
                CMP1\t7\t6530\t9.17741159442812\t2015-03-13T19:15:09.000Z\t2015-03-13T19:15:09.000Z\t2015-03-13T00:00:00.000Z\t4186\tfalse\t68200832
                CMP2\t6\t441\t9.87805142300203\t2015-03-14T19:15:09.000Z\t2015-03-14T19:15:09.000Z\t2015-03-14T00:00:00.000Z\t6256\ttrue\t25615453
                CMP1\t8\t6476\t0.623656720854342\t2015-03-15T19:15:09.000Z\t2015-03-15T19:15:09.000Z\t2015-03-15T00:00:00.000Z\t8916\tfalse\t11378657
                CMP2\t3\t9245\t4.85969736473635\t2015-03-16T19:15:09.000Z\t2015-03-16T19:15:09.000Z\t2015-03-16T00:00:00.000Z\t5364\ttrue\t72902099
                CMP1\t5\t135\t0.71932214545086\t2015-03-17T19:15:09.000Z\t2015-03-17T19:15:09.000Z\t2015-03-17T00:00:00.000Z\t6172\tfalse\t94911256
                CMP2\t6\t5662\t0.934403704013675\t2015-03-18T19:15:09.000Z\t2015-03-18T19:15:09.000Z\t2015-03-18T00:00:00.000Z\t3228\ttrue\t71957668
                CMP1\t7\t8820\t2.26465462474152\t2015-03-19T19:15:09.000Z\t2015-03-19T19:15:09.000Z\t2015-03-19T00:00:00.000Z\t5414\tfalse\t37676934
                CMP2\t1\t1673\t1.13900111755356\t2015-03-20T19:15:09.000Z\t2015-03-20T19:15:09.000Z\t2015-03-20T00:00:00.000Z\t792\ttrue\t45159973
                CMP1\t6\t8704\t7.43929118616506\t2015-03-21T19:15:09.000Z\t2015-03-21T19:15:09.000Z\t2015-03-21T00:00:00.000Z\t4887\tfalse\t27305661
                CMP2\t4\t5380\t8.10803734697402\t2015-03-22T19:15:09.000Z\t2015-03-22T19:15:09.000Z\t2015-03-22T00:00:00.000Z\t8639\ttrue\t90187192
                CMP1\t8\t4176\t8.37395713664591\t2015-03-23T19:15:09.000Z\t2015-03-23T19:15:09.000Z\t2015-03-23T00:00:00.000Z\t7967\tfalse\t32268172
                CMP2\t1\t3419\t3.00495174946263\t2015-03-24T19:15:09.000Z\t2015-03-24T19:15:09.000Z\t2015-03-24T00:00:00.000Z\t7135\ttrue\t42567759
                CMP1\t7\t6785\t3.8469483377412\t2015-03-25T19:15:09.000Z\t2015-03-25T19:15:09.000Z\t2015-03-25T00:00:00.000Z\t9863\tfalse\t154099
                CMP2\t1\t7543\t3.16159424139187\t2015-03-26T19:15:09.000Z\t2015-03-26T19:15:09.000Z\t2015-03-26T00:00:00.000Z\t471\ttrue\t35226692
                CMP1\t2\t178\t1.37678213883191\t2015-03-27T19:15:09.000Z\t2015-03-27T19:15:09.000Z\t2015-03-27T00:00:00.000Z\t1374\tfalse\t80079972
                CMP2\t1\t7256\t6.15871280198917\t2015-03-28T19:15:09.000Z\t2015-03-28T19:15:09.000Z\t2015-03-28T00:00:00.000Z\t7280\ttrue\t86481439
                CMP1\t3\t2116\t7.31438394868746\t2015-03-29T19:15:09.000Z\t2015-03-29T19:15:09.000Z\t2015-03-29T00:00:00.000Z\t6402\tfalse\t60017381
                CMP2\t8\t1606\t8.10372669482604\t2015-03-30T19:15:09.000Z\t2015-03-30T19:15:09.000Z\t2015-03-30T00:00:00.000Z\t4188\ttrue\t74923808
                CMP1\t2\t2361\t2.69874187419191\t2015-03-31T19:15:09.000Z\t2015-03-31T19:15:09.000Z\t2015-03-31T00:00:00.000Z\t5815\tfalse\t16564471
                CMP2\t3\t7280\t8.83913917001337\t2015-04-01T19:15:09.000Z\t2015-04-01T19:15:09.000Z\t2015-04-01T00:00:00.000Z\t9220\ttrue\t7221046
                CMP1\t5\t8158\t1.9249943154864\t2015-04-02T19:15:09.000Z\t2015-04-02T19:15:09.000Z\t2015-04-02T00:00:00.000Z\t3342\tfalse\t28531977
                CMP2\t4\t3006\t8.50523490458727\t2015-04-03T19:15:09.000Z\t2015-04-03T19:15:09.000Z\t2015-04-03T00:00:00.000Z\t7198\ttrue\t17639973
                CMP1\t2\t8058\t3.24236876098439\t2015-04-04T19:15:09.000Z\t2015-04-04T19:15:09.000Z\t2015-04-04T00:00:00.000Z\t890\tfalse\t16188457
                CMP2\t8\t4913\t4.31931799743325\t2015-04-05T19:15:09.000Z\t2015-04-05T19:15:09.000Z\t2015-04-05T00:00:00.000Z\t2151\ttrue\t66148054
                CMP1\t6\t6114\t1.60783329280093\t2015-04-06T19:15:09.000Z\t2015-04-06T19:15:09.000Z\t2015-04-06T00:00:00.000Z\t7156\tfalse\t21576214
                CMP2\t1\t3799\t4.94223219808191\t2015-04-07T19:15:09.000Z\t2015-04-07T19:15:09.000Z\t2015-04-07T00:00:00.000Z\t9016\ttrue\t96119371
                CMP1\t8\t3672\t6.49665022967383\t2015-04-08T19:15:09.000Z\t2015-04-08T19:15:09.000Z\t2015-04-08T00:00:00.000Z\t3467\tfalse\t76381922
                CMP2\t6\t2315\t5.62425469048321\t2015-04-09T19:15:09.000Z\t2015-04-09T19:15:09.000Z\t2015-04-09T00:00:00.000Z\t7586\ttrue\t81396580
                CMP1\t8\t230\t6.72886302694678\t2015-04-10T19:15:09.000Z\t2015-04-10T19:15:09.000Z\t2015-04-10T00:00:00.000Z\t7928\tfalse\t18286886
                CMP2\t2\t2722\t2.23382522119209\t2015-04-11T19:15:09.000Z\t2015-04-11T19:15:09.000Z\t2015-04-11T00:00:00.000Z\t2584\ttrue\t75440358
                CMP1\t7\t3225\t3.55993304867297\t2015-04-12T19:15:09.000Z\t2015-04-12T19:15:09.000Z\t2015-04-12T00:00:00.000Z\t177\tfalse\t87523552
                CMP2\t6\t4692\t2.76645212434232\t2015-04-13T19:15:09.000Z\t2015-04-13T19:15:09.000Z\t2015-04-13T00:00:00.000Z\t4201\ttrue\t28465709
                CMP1\t7\t7116\t6.58135131234303\t2015-04-14T19:15:09.000Z\t2015-04-14T19:15:09.000Z\t2015-04-14T00:00:00.000Z\t3892\tfalse\t48420564
                CMP2\t3\t2457\t5.60338953277096\t2015-04-15T19:15:09.000Z\t2015-04-15T19:15:09.000Z\t2015-04-15T00:00:00.000Z\t7053\ttrue\t33039439
                CMP1\t8\t9975\t0.169386363122612\t2015-04-16T19:15:09.000Z\t2015-04-16T19:15:09.000Z\t2015-04-16T00:00:00.000Z\t6874\tfalse\t6451182
                CMP2\t5\t4952\t0.968641364015639\t2015-04-17T19:15:09.000Z\t2015-04-17T19:15:09.000Z\t2015-04-17T00:00:00.000Z\t1680\ttrue\t77366482
                CMP1\t6\t2024\t1.11267756437883\t2015-04-18T19:15:09.000Z\t2015-04-18T19:15:09.000Z\t2015-04-18T00:00:00.000Z\t3883\tfalse\t65946538
                CMP2\t2\t7689\t6.29668754525483\t2015-04-19T19:15:09.000Z\t2015-04-19T19:15:09.000Z\t2015-04-19T00:00:00.000Z\t254\ttrue\t15272074
                CMP1\t1\t9916\t0.246034313458949\t2015-04-20T19:15:09.000Z\t2015-04-20T19:15:09.000Z\t2015-04-20T00:00:00.000Z\t7768\tfalse\t24934386
                CMP2\t8\t2034\t7.2211763379164\t2015-04-21T19:15:09.000Z\t2015-04-21T19:15:09.000Z\t2015-04-21T00:00:00.000Z\t8514\ttrue\t26112211
                CMP1\t8\t673\t4.48250063927844\t2015-04-22T19:15:09.000Z\t2015-04-22T19:15:09.000Z\t2015-04-22T00:00:00.000Z\t2455\tfalse\t51949360
                CMP2\t3\t6513\t4.39972517313436\t2015-04-23T19:15:09.000Z\t2015-04-23T19:15:09.000Z\t2015-04-23T00:00:00.000Z\t7307\ttrue\t74090772
                CMP1\t2\t8509\t7.21647302387282\t2015-04-24T19:15:09.000Z\t2015-04-24T19:15:09.000Z\t2015-04-24T00:00:00.000Z\t1784\tfalse\t43610015
                CMP2\t1\t9263\t9.72563182003796\t2015-04-25T19:15:09.000Z\t2015-04-25T19:15:09.000Z\t2015-04-25T00:00:00.000Z\t8811\ttrue\t27236992
                CMP1\t7\t9892\t1.50758364936337\t2015-04-26T19:15:09.000Z\t2015-04-26T19:15:09.000Z\t2015-04-26T00:00:00.000Z\t8011\tfalse\t16678001
                CMP2\t4\t4244\t3.88368266867474\t2015-04-27T19:15:09.000Z\t2015-04-27T19:15:09.000Z\t2015-04-27T00:00:00.000Z\t7431\ttrue\t19956646
                CMP1\t6\t9643\t3.09016502927989\t2015-04-28T19:15:09.000Z\t2015-04-28T19:15:09.000Z\t2015-04-28T00:00:00.000Z\t7144\tfalse\t40810637
                CMP2\t5\t3361\t5.21436133189127\t2015-04-29T19:15:09.000Z\t2015-04-29T19:15:09.000Z\t2015-04-29T00:00:00.000Z\t7217\ttrue\t35823849
                CMP1\t2\t5487\t3.5918223625049\t2015-04-30T19:15:09.000Z\t2015-04-30T19:15:09.000Z\t2015-04-30T00:00:00.000Z\t1421\tfalse\t60850489
                CMP2\t8\t4391\t2.72367869038135\t2015-05-01T19:15:09.000Z\t2015-05-01T19:15:09.000Z\t2015-05-01T00:00:00.000Z\t1296\ttrue\t80036797
                CMP1\t4\t2843\t5.22989432094619\t2015-05-02T19:15:09.000Z\t2015-05-02T19:15:09.000Z\t2015-05-02T00:00:00.000Z\t7773\tfalse\t88340142
                CMP2\tnull\t2848\t5.32819046406075\t2015-05-03T19:15:09.000Z\t2015-05-03T19:15:09.000Z\t2015-05-03T00:00:00.000Z\t7628\ttrue\t36732064
                CMP1\tnull\t2776\t5.30948682921007\t2015-05-04T19:15:09.000Z\t2015-05-04T19:15:09.000Z\t2015-05-04T00:00:00.000Z\t5917\tfalse\t59635623
                CMP2\t8\t5256\t8.02117716753855\t2015-05-05T19:15:09.000Z\t2015-05-05T19:15:09.000Z\t2015-05-05T00:00:00.000Z\t4088\ttrue\t50247928
                CMP1\t7\t9250\t0.850080533418804\t2015-05-06T19:15:09.000Z\t2015-05-06T19:15:09.000Z\t2015-05-06T00:00:00.000Z\t519\tfalse\t61373305
                CMP2\t2\t6675\t7.95846320921555\t2015-05-07T19:15:09.000Z\t2015-05-07T19:15:09.000Z\t2015-05-07T00:00:00.000Z\t7530\ttrue\t49634855
                CMP1\t5\t8367\t9.34185237856582\t2015-05-08T19:15:09.000Z\t2015-05-08T19:15:09.000Z\t2015-05-08T00:00:00.000Z\t9714\tfalse\t91106929
                CMP2\t4\t370\t7.84945336403325\t2015-05-09T19:15:09.000Z\t2015-05-09T19:15:09.000Z\t2015-05-09T00:00:00.000Z\t8590\ttrue\t89638043
                CMP1\t7\t4055\t6.49124878691509\t2015-05-10T19:15:09.000Z\t2015-05-10T19:15:09.000Z\t2015-05-10T00:00:00.000Z\t3484\tfalse\t58849380
                CMP2\tnull\t6132\t2.01015920145437\t2015-05-11T19:15:09.000Z\t2015-05-11T19:15:09.000Z\t2015-05-11T00:00:00.000Z\t8132\ttrue\t51493476
                CMP1\t6\t6607\t0.0829047034494579\t2015-05-12T19:15:09.000Z\t2015-05-12T19:15:09.000Z\t2015-05-12T00:00:00.000Z\t1685\tfalse\t88274174
                CMP2\t8\t1049\t9.39520388608798\t2015-05-13T19:15:09.000Z\t2015-05-13T19:15:09.000Z\t2015-05-13T00:00:00.000Z\t7164\ttrue\t49001539
                """;

        CopyRunnable assertion = () -> {
            assertSql(expected, "x");
            String query = "select phase, status, rows_handled, rows_imported, errors from " + configuration.getSystemTableNamePrefix() + "text_import_log";
            assertSql("""
                    phase\tstatus\trows_handled\trows_imported\terrors
                    \tstarted\tnull\tnull\t0
                    \tfinished\t129\t127\t2
                    """, query);
        };
        testCopy(insert, assertion);
    }

    @Test
    public void testSetAllParallelCopyOptions() throws SqlException {
        boolean[] useUpperCase = new boolean[]{true, false};
        Object[] partitionBy = new Object[]{"HOUR", PartitionBy.HOUR, "DAY", PartitionBy.DAY, "MONTH", PartitionBy.MONTH, "WEEK", PartitionBy.WEEK, "YEAR", PartitionBy.YEAR};
        Object[] onError = new Object[]{"SKIP_COLUMN", Atomicity.SKIP_COL, "SKIP_ROW", Atomicity.SKIP_ROW, "ABORT", Atomicity.SKIP_ALL};

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (boolean upperCase : useUpperCase) {
                for (int p = 0; p < partitionBy.length / 2; p += 2) {
                    for (int o = 0; o < onError.length / 2; o += 2) {

                        ExportModel model;
                        if (upperCase) {
                            model = (ExportModel) compiler.testCompileModel("COPY x FROM 'somefile.csv' WITH HEADER TRUE " +
                                    "PARTITION BY " + partitionBy[p] + " TIMESTAMP 'ts1' FORMAT 'yyyy-MM-ddTHH:mm:ss' DELIMITER ';' ON ERROR " + onError[o] + ";'", sqlExecutionContext);
                        } else {
                            model = (ExportModel) compiler.testCompileModel("copy x from 'somefile.csv' with header true " +
                                    "partition by " + partitionBy[p] + " timestamp 'ts1' format 'yyyy-MM-ddTHH:mm:ss' delimiter ';' on error " + onError[o] + ";'", sqlExecutionContext);
                        }

                        TestUtils.assertEquals("x", model.getTableName());
                        TestUtils.assertEquals("'somefile.csv'", model.getFileName().token);
                        assertTrue(model.isHeader());
                        assertEquals(partitionBy[p + 1], model.getPartitionBy());
                        TestUtils.assertEquals("ts1", model.getTimestampColumnName());
                        TestUtils.assertEquals("yyyy-MM-ddTHH:mm:ss", model.getTimestampFormat());
                        assertEquals(';', model.getDelimiter());
                        assertEquals(onError[o + 1], model.getAtomicity());
                    }
                }
            }
        }
    }

    private static Thread createJobThread(SynchronizedJob job, CountDownLatch latch) {
        return new Thread(() -> {
            try {
                while (latch.getCount() > 0) {
                    if (job.run(0)) {
                        latch.countDown();
                    }
                    Os.sleep(1);
                }
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

    private void assertQuotesTableContent() throws SqlException {
        assertQuotesTableContent0(false);
    }

    private void assertQuotesTableContent0(boolean columnsReordered) throws SqlException {
        final String values = columnsReordered ?
                "('1972-09-28T00:00:00.000000Z','line1001','desc 1001',0.918270255022)" :
                "('line1001','1972-09-28T00:00:00.000000Z',0.918270255022,'desc 1001')";

        execute("insert into x values" + values);
        if (walEnabled) {
            drainWalQueue();
        }
        assertSql("""
                        line\tts\td\tdescription
                        line992\t1972-09-19T00:00:00.000000Z\t0.107142280151\tdesc 992
                        line993\t1972-09-20T00:00:00.000000Z\t0.0974353165713\tdesc 993
                        line994\t1972-09-21T00:00:00.000000Z\t0.81272025622\tdesc 994
                        line995\t1972-09-22T00:00:00.000000Z\t0.566736320714\tdesc 995
                        line996\t1972-09-23T00:00:00.000000Z\t0.415739766699\tdesc 996
                        line997\t1972-09-24T00:00:00.000000Z\t0.378956184893\tdesc 997
                        line998\t1972-09-25T00:00:00.000000Z\t0.736755687844\tdesc 998
                        line999\t1972-09-26T00:00:00.000000Z\t0.910141500002\tdesc 999
                        line1000\t1972-09-27T00:00:00.000000Z\t0.918270255022\tdesc 1000
                        line1001\t1972-09-28T00:00:00.000000Z\t0.918270255022\tdesc 1001
                        """,
                "select line,ts,d,description from x limit -10"
        );

        assertSql("cnt\n1001\n", "select count(*) cnt from x");
    }

    private void assertQuotesTableContentExisting() throws SqlException {
        assertQuotesTableContent0(true);
    }

    private void testCopyWithAtomicity(boolean parallel, String atomicity, int expectedCount) throws Exception {
        CopyRunnable stmt = () -> {
            execute("create table alltypes (\n" +
                    "  bo boolean,\n" +
                    "  by byte,\n" +
                    "  sh short,\n" +
                    "  ch char,\n" +
                    "  in_ int,\n" +
                    "  lo long,\n" +
                    "  dat date, \n" +
                    "  tstmp timestamp, \n" +
                    "  ft float,\n" +
                    "  db double,\n" +
                    "  str string,\n" +
                    "  sym symbol,\n" +
                    "  l256 long256," +
                    "  ge geohash(20b)" +
                    ") timestamp(tstmp) partition by " + (parallel ? "DAY" : "NONE") + ";");
            runAndFetchCopyID("copy alltypes from 'test-errors.csv' with header true timestamp 'tstmp' delimiter ',' " +
                    "format 'yyyy-MM-ddTHH:mm:ss.SSSSSSZ' on error " + atomicity + ";", sqlExecutionContext);
        };

        CopyRunnable test = () -> assertSql("cnt\n" + expectedCount + "\n", "select count(*) cnt from alltypes");

        testCopy(stmt, test);
    }

    protected static String runAndFetchCopyID(String copySql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (
                RecordCursorFactory factory = select(copySql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            CharSequence value = cursor.getRecord().getStrA(0);
            Assert.assertNotNull(value);
            return value.toString();
        }
    }

    protected static void testCopy(CopyRunnable statement, CopyRunnable test) throws Exception {
        assertMemoryLeak(() -> {
            CountDownLatch processed = new CountDownLatch(1);

            execute("drop table if exists \"" + configuration.getSystemTableNamePrefix() + "text_import_log\"");
            try (CopyImportRequestJob copyRequestJob = new CopyImportRequestJob(engine, 1)) {
                Thread processingThread = createJobThread(copyRequestJob, processed);
                processingThread.start();
                statement.run();
                processed.await();
                test.run();
                processingThread.join();
                copyRequestJob.drain(0);
            }
        });
    }

    @FunctionalInterface
    public interface CopyRunnable {
        void run() throws Exception;
    }
}
