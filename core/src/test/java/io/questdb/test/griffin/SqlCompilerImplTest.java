/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.griffin.CompiledQuery.SET;

public class SqlCompilerImplTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(SqlCompilerImplTest.class);
    private static Path path;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        path = new Path();
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        path = Misc.free(path);
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void assertCastString() throws SqlException {
        final String expectedData = "a\n" +
                "JWCPS\n" +
                "\n" +
                "RXPEHNRXG\n" +
                "\n" +
                "\n" +
                "XIBBT\n" +
                "GWFFY\n" +
                "EYYQEHBHFO\n" +
                "PDXYSBEOUO\n" +
                "HRUEDRQQUL\n" +
                "JGETJRSZS\n" +
                "RFBVTMHGO\n" +
                "ZVDZJMY\n" +
                "CXZOUICWEK\n" +
                "VUVSDOTS\n" +
                "YYCTG\n" +
                "LYXWCKYLSU\n" +
                "SWUGSHOLNV\n" +
                "\n" +
                "BZXIOVI\n";

        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"SYMBOL\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_str(5,10,2) a from long_sequence(20))" +
                "), cast(a as SYMBOL)";

        assertCast(expectedData, expectedMeta, sql);
    }

    @Test
    public void tesFailOnNonBooleanJoinCondition() throws Exception {
        ddl("create table a ( ts timestamp, i int) timestamp(ts) ");
        ddl("create table b ( ts timestamp, i int) timestamp(ts) ");

        String booleanError = "boolean expression expected";

        assertFailure(30, booleanError,
                "select * from a " +
                        "join b on a.i - b.i"
        );

        assertFailure(35, booleanError,
                "select * from a " +
                        "left join b on a.i - b.i"
        );

        assertFailure(46, booleanError,
                "select * from a " +
                        "join b on a.ts = b.ts and a.i - b.i"
        );

        assertFailure(51, booleanError,
                "select * from a " +
                        "left join b on a.ts = b.ts and a.i - b.i"
        );

        for (String join : Arrays.asList("ASOF  ", "LT    ", "SPLICE")) {
            assertFailure(37, "unsupported " + join.trim() + " join expression",
                    "select * " +
                            "from a " +
                            "#JOIN# join b on a.i ^ a.i".replace("#JOIN#", join)
            );
        }

        String unexpectedError = "unexpected argument for function: and. expected args: (BOOLEAN,BOOLEAN). actual args: (INT,INT)";
        assertFailure(44, unexpectedError,
                "select * from a " +
                        "join b on a.i + b.i and a.i - b.i"
        );

        assertFailure(49, unexpectedError,
                "select * from a " +
                        "left join b on a.i + b.i and a.i - b.i"
        );

        assertFailure(60, unexpectedError,
                "select * from a " +
                        "join b on a.ts = b.ts and a.i - b.i and b.i - a.i"
        );
    }

    @Test
    public void testCannotCreateTable() throws Exception {
        assertFailure(
                new TestFilesFacadeImpl() {

                    @Override
                    public int mkdirs(Path path, int mode) {
                        return -1;
                    }
                },
                "create table x (a int)",
                "Could not create table"
        );
    }

    @Test
    public void testCastByteDate() throws SqlException {
        assertCastByte(
                "a\n" +
                        "1970-01-01T00:00:00.119Z\n" +
                        "1970-01-01T00:00:00.052Z\n" +
                        "1970-01-01T00:00:00.091Z\n" +
                        "1970-01-01T00:00:00.097Z\n" +
                        "1970-01-01T00:00:00.119Z\n" +
                        "1970-01-01T00:00:00.107Z\n" +
                        "1970-01-01T00:00:00.039Z\n" +
                        "1970-01-01T00:00:00.081Z\n" +
                        "1970-01-01T00:00:00.046Z\n" +
                        "1970-01-01T00:00:00.041Z\n" +
                        "1970-01-01T00:00:00.061Z\n" +
                        "1970-01-01T00:00:00.082Z\n" +
                        "1970-01-01T00:00:00.075Z\n" +
                        "1970-01-01T00:00:00.095Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.116Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.040Z\n" +
                        "1970-01-01T00:00:00.116Z\n" +
                        "1970-01-01T00:00:00.117Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastByteDouble() throws SqlException {
        assertCastByte(
                "a\n" +
                        "119.0\n" +
                        "52.0\n" +
                        "91.0\n" +
                        "97.0\n" +
                        "119.0\n" +
                        "107.0\n" +
                        "39.0\n" +
                        "81.0\n" +
                        "46.0\n" +
                        "41.0\n" +
                        "61.0\n" +
                        "82.0\n" +
                        "75.0\n" +
                        "95.0\n" +
                        "87.0\n" +
                        "116.0\n" +
                        "87.0\n" +
                        "40.0\n" +
                        "116.0\n" +
                        "117.0\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastByteFloat() throws SqlException {
        assertCastByte(
                "a\n" +
                        "119.0000\n" +
                        "52.0000\n" +
                        "91.0000\n" +
                        "97.0000\n" +
                        "119.0000\n" +
                        "107.0000\n" +
                        "39.0000\n" +
                        "81.0000\n" +
                        "46.0000\n" +
                        "41.0000\n" +
                        "61.0000\n" +
                        "82.0000\n" +
                        "75.0000\n" +
                        "95.0000\n" +
                        "87.0000\n" +
                        "116.0000\n" +
                        "87.0000\n" +
                        "40.0000\n" +
                        "116.0000\n" +
                        "117.0000\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastByteInt() throws SqlException {
        assertCastByte(
                "a\n" +
                        "119\n" +
                        "52\n" +
                        "91\n" +
                        "97\n" +
                        "119\n" +
                        "107\n" +
                        "39\n" +
                        "81\n" +
                        "46\n" +
                        "41\n" +
                        "61\n" +
                        "82\n" +
                        "75\n" +
                        "95\n" +
                        "87\n" +
                        "116\n" +
                        "87\n" +
                        "40\n" +
                        "116\n" +
                        "117\n",
                ColumnType.INT
        );
    }

    @Test
    public void testCastByteLong() throws SqlException {
        assertCastByte(
                "a\n" +
                        "119\n" +
                        "52\n" +
                        "91\n" +
                        "97\n" +
                        "119\n" +
                        "107\n" +
                        "39\n" +
                        "81\n" +
                        "46\n" +
                        "41\n" +
                        "61\n" +
                        "82\n" +
                        "75\n" +
                        "95\n" +
                        "87\n" +
                        "116\n" +
                        "87\n" +
                        "40\n" +
                        "116\n" +
                        "117\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastByteShort() throws SqlException {
        assertCastByte(
                "a\n" +
                        "119\n" +
                        "52\n" +
                        "91\n" +
                        "97\n" +
                        "119\n" +
                        "107\n" +
                        "39\n" +
                        "81\n" +
                        "46\n" +
                        "41\n" +
                        "61\n" +
                        "82\n" +
                        "75\n" +
                        "95\n" +
                        "87\n" +
                        "116\n" +
                        "87\n" +
                        "40\n" +
                        "116\n" +
                        "117\n",
                ColumnType.SHORT
        );
    }

    @Test
    public void testCastByteTimestamp() throws SqlException {
        assertCastByte(
                "a\n" +
                        "1970-01-01T00:00:00.000119Z\n" +
                        "1970-01-01T00:00:00.000052Z\n" +
                        "1970-01-01T00:00:00.000091Z\n" +
                        "1970-01-01T00:00:00.000097Z\n" +
                        "1970-01-01T00:00:00.000119Z\n" +
                        "1970-01-01T00:00:00.000107Z\n" +
                        "1970-01-01T00:00:00.000039Z\n" +
                        "1970-01-01T00:00:00.000081Z\n" +
                        "1970-01-01T00:00:00.000046Z\n" +
                        "1970-01-01T00:00:00.000041Z\n" +
                        "1970-01-01T00:00:00.000061Z\n" +
                        "1970-01-01T00:00:00.000082Z\n" +
                        "1970-01-01T00:00:00.000075Z\n" +
                        "1970-01-01T00:00:00.000095Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000116Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000040Z\n" +
                        "1970-01-01T00:00:00.000116Z\n" +
                        "1970-01-01T00:00:00.000117Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastDateByte() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.BYTE) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select cast(rnd_byte() as date) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.BYTE) + ")";

        assertCast(
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n" +
                        "84\n" +
                        "84\n" +
                        "74\n" +
                        "55\n" +
                        "83\n" +
                        "88\n" +
                        "32\n" +
                        "21\n" +
                        "91\n" +
                        "74\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCastDateDouble() throws SqlException {
        assertCastDate(
                "a\n" +
                        "1.426297242379E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.446081058169E12\n" +
                        "1.434834113022E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.439739868373E12\n" +
                        "1.443957889668E12\n" +
                        "1.440280260964E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.44318380966E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.435298544851E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.447181628184E12\n" +
                        "1.4423615004E12\n" +
                        "1.428165287226E12\n" +
                        "-9.223372036854776E18\n" +
                        "1.434999533562E12\n" +
                        "1.423736755529E12\n" +
                        "1.426566352765E12\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastDateFloat() throws SqlException {
        assertCastDate(
                "a\n" +
                        "1.42629719E12\n" +
                        "-9.223372E18\n" +
                        "1.44608107E12\n" +
                        "1.43483417E12\n" +
                        "-9.223372E18\n" +
                        "1.43973981E12\n" +
                        "1.44395783E12\n" +
                        "1.44028022E12\n" +
                        "-9.223372E18\n" +
                        "1.44318385E12\n" +
                        "-9.223372E18\n" +
                        "1.43529856E12\n" +
                        "-9.223372E18\n" +
                        "1.44718168E12\n" +
                        "1.44236151E12\n" +
                        "1.42816523E12\n" +
                        "-9.223372E18\n" +
                        "1.43499959E12\n" +
                        "1.4237367E12\n" +
                        "1.42656641E12\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastDateInt() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.INT) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select cast(rnd_int() as date) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.INT) + ")";

        assertCast(
                "a\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n" +
                        "-948263339\n" +
                        "1326447242\n" +
                        "592859671\n" +
                        "1868723706\n" +
                        "-847531048\n" +
                        "-1191262516\n" +
                        "-2041844972\n" +
                        "-1436881714\n" +
                        "-1575378703\n" +
                        "806715481\n" +
                        "1545253512\n" +
                        "1569490116\n" +
                        "1573662097\n" +
                        "-409854405\n" +
                        "339631474\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCastDateLong() throws SqlException {
        assertCastDate(
                "a\n" +
                        "1426297242379\n" +
                        "NaN\n" +
                        "1446081058169\n" +
                        "1434834113022\n" +
                        "NaN\n" +
                        "1439739868373\n" +
                        "1443957889668\n" +
                        "1440280260964\n" +
                        "NaN\n" +
                        "1443183809660\n" +
                        "NaN\n" +
                        "1435298544851\n" +
                        "NaN\n" +
                        "1447181628184\n" +
                        "1442361500400\n" +
                        "1428165287226\n" +
                        "NaN\n" +
                        "1434999533562\n" +
                        "1423736755529\n" +
                        "1426566352765\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastDateShort() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.SHORT) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select cast(rnd_short() as date) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.SHORT) + ")";

        assertCast(
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "-14644\n" +
                        "-5356\n" +
                        "-4914\n" +
                        "-24335\n" +
                        "-32679\n" +
                        "-19832\n" +
                        "-31548\n" +
                        "11665\n" +
                        "7739\n" +
                        "23922\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCastDateTimestamp() throws SqlException {
        assertCastDate(
                "a\n" +
                        "2015-03-14T01:40:42.379000Z\n" +
                        "\n" +
                        "2015-10-29T01:10:58.169000Z\n" +
                        "2015-06-20T21:01:53.022000Z\n" +
                        "\n" +
                        "2015-08-16T15:44:28.373000Z\n" +
                        "2015-10-04T11:24:49.668000Z\n" +
                        "2015-08-22T21:51:00.964000Z\n" +
                        "\n" +
                        "2015-09-25T12:23:29.660000Z\n" +
                        "\n" +
                        "2015-06-26T06:02:24.851000Z\n" +
                        "\n" +
                        "2015-11-10T18:53:48.184000Z\n" +
                        "2015-09-15T23:58:20.400000Z\n" +
                        "2015-04-04T16:34:47.226000Z\n" +
                        "\n" +
                        "2015-06-22T18:58:53.562000Z\n" +
                        "2015-02-12T10:25:55.529000Z\n" +
                        "2015-03-17T04:25:52.765000Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastDoubleByte() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "80\n" +
                        "8\n" +
                        "8\n" +
                        "65\n" +
                        "79\n" +
                        "22\n" +
                        "34\n" +
                        "76\n" +
                        "42\n" +
                        "0\n" +
                        "72\n" +
                        "42\n" +
                        "70\n" +
                        "38\n" +
                        "0\n" +
                        "32\n" +
                        "0\n" +
                        "97\n" +
                        "24\n" +
                        "63\n",
                ColumnType.BYTE
        );
    }

    @Test
    public void testCastDoubleDate() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.065Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.042Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.042Z\n" +
                        "1970-01-01T00:00:00.070Z\n" +
                        "1970-01-01T00:00:00.038Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.032Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.097Z\n" +
                        "1970-01-01T00:00:00.024Z\n" +
                        "1970-01-01T00:00:00.063Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastDoubleFloat() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "80.4322\n" +
                        "8.4870\n" +
                        "8.4383\n" +
                        "65.0859\n" +
                        "79.0568\n" +
                        "22.4523\n" +
                        "34.9107\n" +
                        "76.1103\n" +
                        "42.1777\n" +
                        "NaN\n" +
                        "72.6114\n" +
                        "42.2436\n" +
                        "70.9436\n" +
                        "38.5399\n" +
                        "0.3598\n" +
                        "32.8818\n" +
                        "NaN\n" +
                        "97.7110\n" +
                        "24.8088\n" +
                        "63.8161\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastDoubleInt() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "80\n" +
                        "8\n" +
                        "8\n" +
                        "65\n" +
                        "79\n" +
                        "22\n" +
                        "34\n" +
                        "76\n" +
                        "42\n" +
                        "NaN\n" +
                        "72\n" +
                        "42\n" +
                        "70\n" +
                        "38\n" +
                        "0\n" +
                        "32\n" +
                        "NaN\n" +
                        "97\n" +
                        "24\n" +
                        "63\n",
                ColumnType.INT
        );
    }

    @Test
    public void testCastDoubleLong() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "80\n" +
                        "8\n" +
                        "8\n" +
                        "65\n" +
                        "79\n" +
                        "22\n" +
                        "34\n" +
                        "76\n" +
                        "42\n" +
                        "NaN\n" +
                        "72\n" +
                        "42\n" +
                        "70\n" +
                        "38\n" +
                        "0\n" +
                        "32\n" +
                        "NaN\n" +
                        "97\n" +
                        "24\n" +
                        "63\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastDoubleShort() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "80\n" +
                        "8\n" +
                        "8\n" +
                        "65\n" +
                        "79\n" +
                        "22\n" +
                        "34\n" +
                        "76\n" +
                        "42\n" +
                        "0\n" +
                        "72\n" +
                        "42\n" +
                        "70\n" +
                        "38\n" +
                        "0\n" +
                        "32\n" +
                        "0\n" +
                        "97\n" +
                        "24\n" +
                        "63\n",
                ColumnType.SHORT
        );
    }

    @Test
    public void testCastDoubleTimestamp() throws SqlException {
        assertCastDouble(
                "a\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000065Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000022Z\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000042Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000042Z\n" +
                        "1970-01-01T00:00:00.000070Z\n" +
                        "1970-01-01T00:00:00.000038Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000032Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000097Z\n" +
                        "1970-01-01T00:00:00.000024Z\n" +
                        "1970-01-01T00:00:00.000063Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastFloatByte() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "80\n" +
                        "0\n" +
                        "8\n" +
                        "29\n" +
                        "0\n" +
                        "93\n" +
                        "13\n" +
                        "79\n" +
                        "0\n" +
                        "22\n" +
                        "0\n" +
                        "34\n" +
                        "0\n" +
                        "76\n" +
                        "52\n" +
                        "55\n" +
                        "0\n" +
                        "72\n" +
                        "62\n" +
                        "66\n",
                ColumnType.BYTE
        );
    }

    @Test
    public void testCastFloatDate() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.029Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.093Z\n" +
                        "1970-01-01T00:00:00.013Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.052Z\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.062Z\n" +
                        "1970-01-01T00:00:00.066Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastFloatDouble() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "80.43223571777344\n" +
                        "NaN\n" +
                        "8.48696231842041\n" +
                        "29.919904708862305\n" +
                        "NaN\n" +
                        "93.446044921875\n" +
                        "13.123357772827148\n" +
                        "79.05675506591797\n" +
                        "NaN\n" +
                        "22.45233726501465\n" +
                        "NaN\n" +
                        "34.910701751708984\n" +
                        "NaN\n" +
                        "76.11029052734375\n" +
                        "52.43722915649414\n" +
                        "55.991615295410156\n" +
                        "NaN\n" +
                        "72.61135864257812\n" +
                        "62.76953887939453\n" +
                        "66.93836975097656\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastFloatInt() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "80\n" +
                        "NaN\n" +
                        "8\n" +
                        "29\n" +
                        "NaN\n" +
                        "93\n" +
                        "13\n" +
                        "79\n" +
                        "NaN\n" +
                        "22\n" +
                        "NaN\n" +
                        "34\n" +
                        "NaN\n" +
                        "76\n" +
                        "52\n" +
                        "55\n" +
                        "NaN\n" +
                        "72\n" +
                        "62\n" +
                        "66\n",
                ColumnType.INT
        );
    }

    @Test
    public void testCastFloatLong() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "80\n" +
                        "NaN\n" +
                        "8\n" +
                        "29\n" +
                        "NaN\n" +
                        "93\n" +
                        "13\n" +
                        "79\n" +
                        "NaN\n" +
                        "22\n" +
                        "NaN\n" +
                        "34\n" +
                        "NaN\n" +
                        "76\n" +
                        "52\n" +
                        "55\n" +
                        "NaN\n" +
                        "72\n" +
                        "62\n" +
                        "66\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastFloatShort() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "80\n" +
                        "0\n" +
                        "8\n" +
                        "29\n" +
                        "0\n" +
                        "93\n" +
                        "13\n" +
                        "79\n" +
                        "0\n" +
                        "22\n" +
                        "0\n" +
                        "34\n" +
                        "0\n" +
                        "76\n" +
                        "52\n" +
                        "55\n" +
                        "0\n" +
                        "72\n" +
                        "62\n" +
                        "66\n",
                ColumnType.SHORT
        );
    }

    @Test
    public void testCastFloatTimestamp() throws SqlException {
        assertCastFloat(
                "a\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000029Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000093Z\n" +
                        "1970-01-01T00:00:00.000013Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000022Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000052Z\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000062Z\n" +
                        "1970-01-01T00:00:00.000066Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastIntByte() throws SqlException {
        assertCastInt("a\n" +
                "1\n" +
                "19\n" +
                "30\n" +
                "16\n" +
                "7\n" +
                "26\n" +
                "26\n" +
                "15\n" +
                "14\n" +
                "0\n" +
                "21\n" +
                "15\n" +
                "3\n" +
                "4\n" +
                "6\n" +
                "19\n" +
                "7\n" +
                "13\n" +
                "17\n" +
                "25\n", ColumnType.BYTE, 0);
    }

    @Test
    public void testCastIntDate() throws SqlException {
        assertCastInt(
                "a\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.026Z\n" +
                        "1970-01-01T00:00:00.026Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.013Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.025Z\n" +
                        "1970-01-01T00:00:00.021Z\n" +
                        "1970-01-01T00:00:00.023Z\n" +
                        "1969-12-07T03:28:36.352Z\n" +
                        "1970-01-01T00:00:00.006Z\n" +
                        "1970-01-01T00:00:00.019Z\n" +
                        "1970-01-01T00:00:00.007Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastIntDouble() throws SqlException {
        assertCastInt(
                "a\n" +
                        "1.0\n" +
                        "-2.147483648E9\n" +
                        "22.0\n" +
                        "22.0\n" +
                        "-2.147483648E9\n" +
                        "7.0\n" +
                        "26.0\n" +
                        "26.0\n" +
                        "-2.147483648E9\n" +
                        "13.0\n" +
                        "-2.147483648E9\n" +
                        "0.0\n" +
                        "-2.147483648E9\n" +
                        "25.0\n" +
                        "21.0\n" +
                        "23.0\n" +
                        "-2.147483648E9\n" +
                        "6.0\n" +
                        "19.0\n" +
                        "7.0\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastIntFloat() throws SqlException {
        assertCastInt(
                "a\n" +
                        "1.0000\n" +
                        "-2.14748365E9\n" +
                        "22.0000\n" +
                        "22.0000\n" +
                        "-2.14748365E9\n" +
                        "7.0000\n" +
                        "26.0000\n" +
                        "26.0000\n" +
                        "-2.14748365E9\n" +
                        "13.0000\n" +
                        "-2.14748365E9\n" +
                        "0.0000\n" +
                        "-2.14748365E9\n" +
                        "25.0000\n" +
                        "21.0000\n" +
                        "23.0000\n" +
                        "-2.14748365E9\n" +
                        "6.0000\n" +
                        "19.0000\n" +
                        "7.0000\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastIntLong() throws SqlException {
        assertCastInt("a\n" +
                "1\n" +
                "-2147483648\n" +
                "22\n" +
                "22\n" +
                "-2147483648\n" +
                "7\n" +
                "26\n" +
                "26\n" +
                "-2147483648\n" +
                "13\n" +
                "-2147483648\n" +
                "0\n" +
                "-2147483648\n" +
                "25\n" +
                "21\n" +
                "23\n" +
                "-2147483648\n" +
                "6\n" +
                "19\n" +
                "7\n", ColumnType.LONG);
    }

    @Test
    public void testCastIntShort() throws SqlException {
        assertCastInt("a\n" +
                        "1\n" +
                        "19\n" +
                        "30\n" +
                        "16\n" +
                        "7\n" +
                        "26\n" +
                        "26\n" +
                        "15\n" +
                        "14\n" +
                        "0\n" +
                        "21\n" +
                        "15\n" +
                        "3\n" +
                        "4\n" +
                        "6\n" +
                        "19\n" +
                        "7\n" +
                        "13\n" +
                        "17\n" +
                        "25\n",
                ColumnType.SHORT, 0
        );
    }

    @Test
    public void testCastIntTimestamp() throws SqlException {
        String expectedData = "a\n" +
                "1970-01-01T00:00:00.000001Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000022Z\n" +
                "1970-01-01T00:00:00.000022Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000007Z\n" +
                "1970-01-01T00:00:00.000026Z\n" +
                "1970-01-01T00:00:00.000026Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000013Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000000Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000025Z\n" +
                "1970-01-01T00:00:00.000021Z\n" +
                "1970-01-01T00:00:00.000023Z\n" +
                "1969-12-31T23:24:12.516352Z\n" +
                "1970-01-01T00:00:00.000006Z\n" +
                "1970-01-01T00:00:00.000019Z\n" +
                "1970-01-01T00:00:00.000007Z\n";
        assertCastInt(expectedData, ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastLongByte() throws SqlException {
        assertCastLong("a\n" +
                        "22\n" +
                        "11\n" +
                        "6\n" +
                        "26\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "15\n" +
                        "9\n" +
                        "26\n" +
                        "30\n" +
                        "8\n" +
                        "0\n" +
                        "4\n" +
                        "16\n" +
                        "10\n" +
                        "6\n" +
                        "3\n" +
                        "8\n" +
                        "12\n",
                ColumnType.BYTE, 0
        );
    }

    @Test
    public void testCastLongDate() throws SqlException {
        assertCastLong(
                "a\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.017Z\n" +
                        "1970-01-01T00:00:00.002Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.021Z\n" +
                        "1970-01-01T00:00:00.001Z\n" +
                        "1970-01-01T00:00:00.020Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.014Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.026Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.023Z\n" +
                        "1970-01-01T00:00:00.002Z\n" +
                        "1970-01-01T00:00:00.024Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.016Z\n" +
                        "1970-01-01T00:00:00.010Z\n" +
                        "1970-01-01T00:00:00.006Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastLongDouble() throws SqlException {
        assertCastLong(
                "a\n" +
                        "22.0\n" +
                        "-9.223372036854776E18\n" +
                        "17.0\n" +
                        "2.0\n" +
                        "-9.223372036854776E18\n" +
                        "21.0\n" +
                        "1.0\n" +
                        "20.0\n" +
                        "-9.223372036854776E18\n" +
                        "14.0\n" +
                        "-9.223372036854776E18\n" +
                        "26.0\n" +
                        "-9.223372036854776E18\n" +
                        "23.0\n" +
                        "2.0\n" +
                        "24.0\n" +
                        "-9.223372036854776E18\n" +
                        "16.0\n" +
                        "10.0\n" +
                        "6.0\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastLongFloat() throws SqlException {
        assertCastLong(
                "a\n" +
                        "22.0000\n" +
                        "-9.223372E18\n" +
                        "17.0000\n" +
                        "2.0000\n" +
                        "-9.223372E18\n" +
                        "21.0000\n" +
                        "1.0000\n" +
                        "20.0000\n" +
                        "-9.223372E18\n" +
                        "14.0000\n" +
                        "-9.223372E18\n" +
                        "26.0000\n" +
                        "-9.223372E18\n" +
                        "23.0000\n" +
                        "2.0000\n" +
                        "24.0000\n" +
                        "-9.223372E18\n" +
                        "16.0000\n" +
                        "10.0000\n" +
                        "6.0000\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastLongInt() throws SqlException {
        assertCastLong("a\n" +
                        "22\n" +
                        "11\n" +
                        "6\n" +
                        "26\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "15\n" +
                        "9\n" +
                        "26\n" +
                        "30\n" +
                        "8\n" +
                        "0\n" +
                        "4\n" +
                        "16\n" +
                        "10\n" +
                        "6\n" +
                        "3\n" +
                        "8\n" +
                        "12\n",
                ColumnType.INT, 0
        );
    }

    @Test
    public void testCastLongShort() throws SqlException {
        assertCastLong("a\n" +
                        "22\n" +
                        "11\n" +
                        "6\n" +
                        "26\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "15\n" +
                        "9\n" +
                        "26\n" +
                        "30\n" +
                        "8\n" +
                        "0\n" +
                        "4\n" +
                        "16\n" +
                        "10\n" +
                        "6\n" +
                        "3\n" +
                        "8\n" +
                        "12\n",
                ColumnType.SHORT, 0
        );
    }

    @Test
    public void testCastLongTimestamp() throws SqlException {
        assertCastLong(
                "a\n" +
                        "1970-01-01T00:00:00.000022Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000017Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000021Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000020Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000014Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000026Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000023Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000024Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000016Z\n" +
                        "1970-01-01T00:00:00.000010Z\n" +
                        "1970-01-01T00:00:00.000006Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastNumberFail() throws Exception {
        assertCastIntFail(ColumnType.BOOLEAN);
        assertCastLongFail(ColumnType.BOOLEAN);
        assertCastByteFail(ColumnType.BOOLEAN);
        assertCastShortFail(ColumnType.BOOLEAN);
        assertCastFloatFail(ColumnType.BOOLEAN);
        assertCastDoubleFail(ColumnType.BOOLEAN);

        assertCastIntFail(ColumnType.STRING);
        assertCastLongFail(ColumnType.STRING);
        assertCastByteFail(ColumnType.STRING);
        assertCastShortFail(ColumnType.STRING);
        assertCastFloatFail(ColumnType.STRING);
        assertCastDoubleFail(ColumnType.STRING);

        assertCastIntFail(ColumnType.SYMBOL);
        assertCastLongFail(ColumnType.SYMBOL);
        assertCastByteFail(ColumnType.SYMBOL);
        assertCastShortFail(ColumnType.SYMBOL);
        assertCastFloatFail(ColumnType.SYMBOL);
        assertCastDoubleFail(ColumnType.SYMBOL);

        assertCastIntFail(ColumnType.BINARY);
        assertCastLongFail(ColumnType.BINARY);
        assertCastByteFail(ColumnType.BINARY);
        assertCastShortFail(ColumnType.BINARY);
        assertCastFloatFail(ColumnType.BINARY);
        assertCastDoubleFail(ColumnType.BINARY);

        assertCastStringFail(ColumnType.BYTE);
        assertCastStringFail(ColumnType.SHORT);
        assertCastStringFail(ColumnType.INT);
        assertCastStringFail(ColumnType.LONG);
        assertCastStringFail(ColumnType.FLOAT);
        assertCastStringFail(ColumnType.DOUBLE);
        assertCastStringFail(ColumnType.DATE);
        assertCastStringFail(ColumnType.TIMESTAMP);

        assertCastSymbolFail(ColumnType.BYTE);
        assertCastSymbolFail(ColumnType.SHORT);
        assertCastSymbolFail(ColumnType.INT);
        assertCastSymbolFail(ColumnType.LONG);
        assertCastSymbolFail(ColumnType.FLOAT);
        assertCastSymbolFail(ColumnType.DOUBLE);
        assertCastSymbolFail(ColumnType.DATE);
        assertCastSymbolFail(ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastShortByte() throws SqlException {
        assertCastShort("a\n" +
                        "48\n" +
                        "110\n" +
                        "63\n" +
                        "99\n" +
                        "107\n" +
                        "43\n" +
                        "-10\n" +
                        "-105\n" +
                        "122\n" +
                        "-88\n" +
                        "-76\n" +
                        "108\n" +
                        "-78\n" +
                        "-113\n" +
                        "39\n" +
                        "-8\n" +
                        "-68\n" +
                        "17\n" +
                        "-69\n" +
                        "-14\n",
                ColumnType.BYTE, -128, 127
        );
    }

    @Test
    public void testCastShortDate() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1970-01-01T00:00:01.430Z\n" +
                        "1970-01-01T00:00:01.238Z\n" +
                        "1970-01-01T00:00:01.204Z\n" +
                        "1970-01-01T00:00:01.751Z\n" +
                        "1970-01-01T00:00:01.751Z\n" +
                        "1970-01-01T00:00:01.429Z\n" +
                        "1970-01-01T00:00:01.397Z\n" +
                        "1970-01-01T00:00:01.539Z\n" +
                        "1970-01-01T00:00:01.501Z\n" +
                        "1970-01-01T00:00:01.045Z\n" +
                        "1970-01-01T00:00:01.318Z\n" +
                        "1970-01-01T00:00:01.255Z\n" +
                        "1970-01-01T00:00:01.838Z\n" +
                        "1970-01-01T00:00:01.784Z\n" +
                        "1970-01-01T00:00:01.928Z\n" +
                        "1970-01-01T00:00:01.381Z\n" +
                        "1970-01-01T00:00:01.822Z\n" +
                        "1970-01-01T00:00:01.414Z\n" +
                        "1970-01-01T00:00:01.588Z\n" +
                        "1970-01-01T00:00:01.371Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastShortDouble() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1430.0\n" +
                        "1238.0\n" +
                        "1204.0\n" +
                        "1751.0\n" +
                        "1751.0\n" +
                        "1429.0\n" +
                        "1397.0\n" +
                        "1539.0\n" +
                        "1501.0\n" +
                        "1045.0\n" +
                        "1318.0\n" +
                        "1255.0\n" +
                        "1838.0\n" +
                        "1784.0\n" +
                        "1928.0\n" +
                        "1381.0\n" +
                        "1822.0\n" +
                        "1414.0\n" +
                        "1588.0\n" +
                        "1371.0\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastShortFloat() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1430.0000\n" +
                        "1238.0000\n" +
                        "1204.0000\n" +
                        "1751.0000\n" +
                        "1751.0000\n" +
                        "1429.0000\n" +
                        "1397.0000\n" +
                        "1539.0000\n" +
                        "1501.0000\n" +
                        "1045.0000\n" +
                        "1318.0000\n" +
                        "1255.0000\n" +
                        "1838.0000\n" +
                        "1784.0000\n" +
                        "1928.0000\n" +
                        "1381.0000\n" +
                        "1822.0000\n" +
                        "1414.0000\n" +
                        "1588.0000\n" +
                        "1371.0000\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastShortInt() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1430\n" +
                        "1238\n" +
                        "1204\n" +
                        "1751\n" +
                        "1751\n" +
                        "1429\n" +
                        "1397\n" +
                        "1539\n" +
                        "1501\n" +
                        "1045\n" +
                        "1318\n" +
                        "1255\n" +
                        "1838\n" +
                        "1784\n" +
                        "1928\n" +
                        "1381\n" +
                        "1822\n" +
                        "1414\n" +
                        "1588\n" +
                        "1371\n",
                ColumnType.INT
        );
    }

    @Test
    public void testCastShortLong() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1430\n" +
                        "1238\n" +
                        "1204\n" +
                        "1751\n" +
                        "1751\n" +
                        "1429\n" +
                        "1397\n" +
                        "1539\n" +
                        "1501\n" +
                        "1045\n" +
                        "1318\n" +
                        "1255\n" +
                        "1838\n" +
                        "1784\n" +
                        "1928\n" +
                        "1381\n" +
                        "1822\n" +
                        "1414\n" +
                        "1588\n" +
                        "1371\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastShortTimestamp() throws SqlException {
        assertCastShort(
                "a\n" +
                        "1970-01-01T00:00:00.001430Z\n" +
                        "1970-01-01T00:00:00.001238Z\n" +
                        "1970-01-01T00:00:00.001204Z\n" +
                        "1970-01-01T00:00:00.001751Z\n" +
                        "1970-01-01T00:00:00.001751Z\n" +
                        "1970-01-01T00:00:00.001429Z\n" +
                        "1970-01-01T00:00:00.001397Z\n" +
                        "1970-01-01T00:00:00.001539Z\n" +
                        "1970-01-01T00:00:00.001501Z\n" +
                        "1970-01-01T00:00:00.001045Z\n" +
                        "1970-01-01T00:00:00.001318Z\n" +
                        "1970-01-01T00:00:00.001255Z\n" +
                        "1970-01-01T00:00:00.001838Z\n" +
                        "1970-01-01T00:00:00.001784Z\n" +
                        "1970-01-01T00:00:00.001928Z\n" +
                        "1970-01-01T00:00:00.001381Z\n" +
                        "1970-01-01T00:00:00.001822Z\n" +
                        "1970-01-01T00:00:00.001414Z\n" +
                        "1970-01-01T00:00:00.001588Z\n" +
                        "1970-01-01T00:00:00.001371Z\n",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testCastTimestampByte() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.BYTE) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_byte()::timestamp a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.BYTE) + ")";

        assertCast(
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n" +
                        "84\n" +
                        "84\n" +
                        "74\n" +
                        "55\n" +
                        "83\n" +
                        "88\n" +
                        "32\n" +
                        "21\n" +
                        "91\n" +
                        "74\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCastTimestampDate() throws SqlException {
        assertCastTimestamp(
                "a\n" +
                        "47956-10-13T01:43:12.217Z\n" +
                        "\n" +
                        "47830-07-03T01:52:05.101Z\n" +
                        "47946-01-25T16:00:41.629Z\n" +
                        "\n" +
                        "47133-10-04T06:29:09.402Z\n" +
                        "47578-08-06T19:13:17.654Z\n" +
                        "47813-04-30T15:45:24.307Z\n" +
                        "\n" +
                        "47370-09-18T01:29:39.870Z\n" +
                        "\n" +
                        "47817-03-02T05:38:00.192Z\n" +
                        "\n" +
                        "47502-10-03T01:46:21.965Z\n" +
                        "47627-07-07T11:02:25.686Z\n" +
                        "47630-01-25T00:47:44.820Z\n" +
                        "\n" +
                        "47620-04-14T19:43:29.561Z\n" +
                        "47725-11-11T00:22:36.062Z\n" +
                        "47867-11-08T16:30:43.643Z\n",
                ColumnType.DATE
        );
    }

    @Test
    public void testCastTimestampDouble() throws SqlException {
        assertCastTimestamp(
                "a\n" +
                        "1.451202658992217E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.447217632325101E15\n" +
                        "1.450864540841629E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.425230490549402E15\n" +
                        "1.439268289997654E15\n" +
                        "1.446675695124307E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.43270813337987E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.446796791480192E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.436874860781965E15\n" +
                        "1.440811969345686E15\n" +
                        "1.44089254366482E15\n" +
                        "-9.223372036854776E18\n" +
                        "1.440583904609561E15\n" +
                        "1.443915505356062E15\n" +
                        "1.448396353843643E15\n",
                ColumnType.DOUBLE
        );
    }

    @Test
    public void testCastTimestampFloat() throws SqlException {
        assertCastTimestamp(
                "a\n" +
                        "1.45120261E15\n" +
                        "-9.223372E18\n" +
                        "1.44721768E15\n" +
                        "1.45086451E15\n" +
                        "-9.223372E18\n" +
                        "1.42523054E15\n" +
                        "1.43926824E15\n" +
                        "1.44667571E15\n" +
                        "-9.223372E18\n" +
                        "1.43270808E15\n" +
                        "-9.223372E18\n" +
                        "1.44679678E15\n" +
                        "-9.223372E18\n" +
                        "1.43687487E15\n" +
                        "1.44081201E15\n" +
                        "1.44089254E15\n" +
                        "-9.223372E18\n" +
                        "1.44058384E15\n" +
                        "1.44391553E15\n" +
                        "1.44839638E15\n",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testCastTimestampInt() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.INT) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_int()::timestamp a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.INT) + ")";

        assertCast(
                "a\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n" +
                        "-948263339\n" +
                        "1326447242\n" +
                        "592859671\n" +
                        "1868723706\n" +
                        "-847531048\n" +
                        "-1191262516\n" +
                        "-2041844972\n" +
                        "-1436881714\n" +
                        "-1575378703\n" +
                        "806715481\n" +
                        "1545253512\n" +
                        "1569490116\n" +
                        "1573662097\n" +
                        "-409854405\n" +
                        "339631474\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCastTimestampLong() throws SqlException {
        assertCastTimestamp(
                "a\n" +
                        "1451202658992217\n" +
                        "NaN\n" +
                        "1447217632325101\n" +
                        "1450864540841629\n" +
                        "NaN\n" +
                        "1425230490549402\n" +
                        "1439268289997654\n" +
                        "1446675695124307\n" +
                        "NaN\n" +
                        "1432708133379870\n" +
                        "NaN\n" +
                        "1446796791480192\n" +
                        "NaN\n" +
                        "1436874860781965\n" +
                        "1440811969345686\n" +
                        "1440892543664820\n" +
                        "NaN\n" +
                        "1440583904609561\n" +
                        "1443915505356062\n" +
                        "1448396353843643\n",
                ColumnType.LONG
        );
    }

    @Test
    public void testCastTimestampShort() throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(ColumnType.SHORT) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_short()::timestamp a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(ColumnType.SHORT) + ")";

        assertCast(
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "-14644\n" +
                        "-5356\n" +
                        "-4914\n" +
                        "-24335\n" +
                        "-32679\n" +
                        "-19832\n" +
                        "-31548\n" +
                        "11665\n" +
                        "7739\n" +
                        "23922\n",
                expectedMeta,
                sql
        );
    }

    @Test
    public void testCloseFactoryWithoutUsingCursor() throws Exception {
        String query = "select * from y where j > :lim";
        TestUtils.assertMemoryLeak(() -> {
            try {
                ddl(
                        "create table y as (" +
                                "select" +
                                " cast(x as int) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym2," +
                                " round(rnd_double(0), 3) price," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                                " rnd_boolean() b," +
                                " rnd_str(1,1,2) c," +
                                " rnd_double(2) d," +
                                " rnd_float(2) e," +
                                " rnd_short(10,1024) f," +
                                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                " rnd_symbol(4,4,4,2) ik," +
                                " rnd_long() j," +
                                " timestamp_sequence(0, 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                );

                bindVariableService.setLong("lim", 4);
                final RecordCursorFactory factory = select(query);
                factory.close();
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testColumnNameWithDot() throws Exception {
        assertFailure(29, "new column name contains invalid characters",
                "create table x (" +
                        "t TIMESTAMP, " +
                        "`bool.flag` BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testCompareStringAndChar() throws Exception {
        assertMemoryLeak(() -> {
            // constant
            assertSql("column\ntrue\n", "select 'ab' > 'a'");
            assertSql("column\nfalse\n", "select 'ab' = 'a'");
            assertSql("column\ntrue\n", "select 'ab' != 'a'");
            assertSql("column\nfalse\n", "select 'ab' < 'a'");

            // non-constant
            assertSql("column\ntrue\ntrue\n", "select x < 'd' from (select 'a' x union all select 'cd')");
            assertSql("column\ntrue\n", "select rnd_str('be', 'cd') < 'd' ");
            assertSql("column\ntrue\n", "select rnd_str('ac', 'be', 'cd') != 'd'");
            assertSql(

                    "column\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "true\n" +
                            "false\n",
                    "select rnd_str('d', 'cd') != 'd' from long_sequence(5)"
            );

            assertSql("column\ntrue\n", "select cast('ab' as char) <= 'a'");
            assertSql("column\nfalse\n", "select cast('ab' as string) <= 'a'");
            assertSql("column\ntrue\n", "select cast('a' as string) <= 'a'");
            assertSql("column\ntrue\n", "select cast('a' as char) <= 'a'");
            assertSql("column\ntrue\n", "select cast('a' as char) <= 'a'::string");
            assertSql("column\ntrue\n", "select cast('' as char) = null");
            assertSql("column\nfalse\n", "select cast('' as char) < null");
            assertSql("column\nfalse\n", "select cast('' as char) > null");
            assertSql("column\nfalse\n", "select cast('' as char) <= null"); // inconsistent with = null
            assertSql("column\nfalse\n", "select cast('' as char) >= null"); // inconsistent with = null
            assertSql("column\nfalse\n", "select cast('' as string) = null");
            assertSql("column\nfalse\n", "select cast('' as string) <= null");
            assertSql("column\ntrue\n", "select cast(null as string) = null");
            assertSql("column\nfalse\n", "select cast(null as string) <= null");// inconsistent with = null
            assertSql("column\nfalse\n", "select cast(null as string) >= null");// inconsistent with = null


            assertFailure(7, "", "select datediff('ma', 0::timestamp, 1::timestamp) ");
        });
    }

    //close command is a no-op in qdb
    @Test
    public void testCompileCloseDoesNothing() throws Exception {
        String query = "CLOSE ALL;";
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(SET, compiler.compile(query, sqlExecutionContext).getType());
            }
        });
    }

    //reset command is a no-op in qdb
    @Test
    public void testCompileResetDoesNothing() throws Exception {
        String query = "RESET ALL;";
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(SET, compiler.compile(query, sqlExecutionContext).getType());
            }
        });
    }

    @Test
    public void testCompileSet() throws Exception {
        String query = "SET x = y";
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(SET, compiler.compile(query, sqlExecutionContext).getType());
            }
        });
    }

    @Test
    public void testCompileStatementsBatch() throws Exception {
        String query = "SELECT pg_advisory_unlock_all(); CLOSE ALL;";
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compileBatch(query, sqlExecutionContext, null);
            }
        });
    }

    //unlisten command is a no-op in qdb (it's a pg-specific notification mechanism)
    @Test
    public void testCompileUnlistenDoesNothing() throws Exception {
        String query = "UNLISTEN *;";
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(SET, compiler.compile(query, sqlExecutionContext).getType());
            }
        });
    }

    @Test
    public void testCreateAsSelect() throws SqlException {
        String expectedData = "a1\ta\tb\tc\td\te\tf\tf1\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.1911234617573182\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.04142812470232493\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.7588175403454873\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.14830552335848957\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.33747075654972813\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696942726644\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.11371841836123953\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.05514933756198426\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.18586435581637295\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.49199001716312474\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630095\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514582851\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.7039785408034679\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.6506604601705693\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.8034049105590781\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.2862717364877081\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.10820602386069589\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        String expectedMeta = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"a1\",\"type\":\"INT\"},{\"index\":1,\"name\":\"a\",\"type\":\"INT\"},{\"index\":2,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":3,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":4,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":5,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":6,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":7,\"name\":\"f1\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"h\",\"type\":\"TIMESTAMP\"},{\"index\":10,\"name\":\"i\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"j1\",\"type\":\"LONG\"},{\"index\":13,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":14,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":15,\"name\":\"m\",\"type\":\"BINARY\"}],\"timestampIndex\":13}";

        assertCast(expectedData, expectedMeta, "create table y as (" +
                "select" +
                " rnd_int() a1," +
                " rnd_int(0, 30, 2) a," +
                " rnd_boolean() b," +
                " rnd_str(3,3,2) c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_short() f1," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                " rnd_symbol(4,4,4,2) i," +
                " rnd_long(100,200,2) j," +
                " rnd_long() j1," +
                " timestamp_sequence(0, 1000000000) k," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m" +
                " from long_sequence(20)" +
                ")  timestamp(k) partition by DAY");
    }

    @Test
    public void testCreateAsSelectCastSymbol() throws SqlException {
        final String expectedData = "a\n" +
                "CPSW\n" +
                "HYRX\n" +
                "\n" +
                "VTJW\n" +
                "PEHN\n" +
                "\n" +
                "VTJW\n" +
                "\n" +
                "CPSW\n" +
                "\n" +
                "PEHN\n" +
                "CPSW\n" +
                "VTJW\n" +
                "\n" +
                "\n" +
                "CPSW\n" +
                "\n" +
                "\n" +
                "\n" +
                "PEHN\n";

        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"STRING\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select rnd_symbol(4,4,4,2) a from long_sequence(20)" +
                "), cast(a as STRING)";

        assertCast(expectedData, expectedMeta, sql);
    }

    @Test
    public void testCreateAsSelectCharToGeoHash() throws Exception {
        assertQuery("geohash\n", "select geohash from geohash", "create table geohash (geohash geohash(1c))", null, "insert into geohash " +
                "select cast(rnd_str('q','u','e') as char) from long_sequence(10)", "geohash\n" +
                "q\n" +
                "q\n" +
                "u\n" +
                "e\n" +
                "e\n" +
                "e\n" +
                "e\n" +
                "u\n" +
                "q\n" +
                "u\n", true, true, false);
    }

    @Test
    public void testCreateAsSelectCharToGeoShort() throws Exception {
        assertException(
                "insert into geohash " +
                        "select cast(rnd_str('q','u','e','o','l') as char) from long_sequence(10)",
                "create table geohash (geohash geohash(2c))",
                27,
                "inconvertible types: CHAR -> GEOHASH(2c) [from=cast, to=geohash]"
        );
    }

    @Test
    public void testCreateAsSelectCharToGeoWiderByte() throws Exception {
        assertException(
                "insert into geohash " +
                        "select cast(rnd_str('q','u','e','o','l') as char) from long_sequence(10)",
                "create table geohash (geohash geohash(6b))",
                27,
                "inconvertible types: CHAR -> GEOHASH(6b) [from=cast, to=geohash]"
        );
    }

    @Test
    public void testCreateAsSelectCharToNarrowGeoByte() throws Exception {
        assertQuery("geohash\n", "select geohash from geohash", "create table geohash (geohash geohash(4b))", null, "insert into geohash " +
                "select cast(rnd_str('q','u','e') as char) from long_sequence(10)", "geohash\n" +
                "1011\n" +
                "1011\n" +
                "1101\n" +
                "0110\n" +
                "0110\n" +
                "0110\n" +
                "0110\n" +
                "1101\n" +
                "1011\n" +
                "1101\n", true, true, false);
    }

    @Test
    public void testCreateAsSelectConstantColumnRename() {
        try {
            assertCreateTableAsSelect(
                    null,
                    "create table Y as (select * from X) timestamp(t)",
                    new Fiddler() {
                        int state = 0;

                        @Override
                        public boolean isHappy() {
                            return state > 1;
                        }

                        @Override
                        public void run(CairoEngine engine) {
                            if (state++ > 0) {
                                // remove column from table X
                                try (TableWriter writer = getWriter("X")) {
                                    if (state == 2) {
                                        writer.removeColumn("b");
                                    } else {
                                        writer.removeColumn("b" + (state - 1));
                                    }
                                    writer.addColumn("b" + state, ColumnType.INT);
                                }
                            }
                        }
                    }
            );
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "underlying cursor is extremely volatile");
        }
    }

    @Test
    public void testCreateAsSelectGeoHashBitsLiteralTooManyBits() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(6b))",
                    null,
                    true,
                    true
            );
            try {
                insert("insert into geohash values(##1000111000111000111000111000111000111000111000110000110100101)");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(27, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid constant: ##1000111000111000111000111000111000111000111000110000110100101");
            }
        });
    }

    @Test
    public void testCreateAsSelectGeoHashBitsLiteralTooManyChars() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(6b))",
                    null,
                    true,
                    true
            );
            try {
                insert("insert into geohash values(##sp052w92p1p82)");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(27, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid constant: ##sp052w92p1p8");
            }
        });
    }

    @Test
    public void testCreateAsSelectGeoHashBitsPrecision() throws Exception {
        final String expected = "a\tb\n" +
                "01001110110\t00100001101\n" +
                "10001101001\t11111011101\n" +
                "10000101010\t11100100000\n" +
                "11000000101\t00001010111\n" +
                "10011100111\t00111000010\n" +
                "01110110001\t10110001001\n" +
                "11010111111\t10001100010\n" +
                "10010110001\t01010110101\n";

        assertMemoryLeak(() -> {
            ddl("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(11) b" +
                    " from long_sequence(8)" +
                    ")");
            assertSql(
                    expected, "x"
            );
        });
    }

    @Test
    public void testCreateAsSelectGeoHashByteSizedStorage5() throws Exception {
        assertMemoryLeak(() -> assertQuery("geohash\n", "select geohash from geohash", "create table geohash (geohash geohash(1c))", null, "insert into geohash " +
                "select rnd_str('q','u','e') from long_sequence(10)", "geohash\n" +
                "q\n" +
                "q\n" +
                "u\n" +
                "e\n" +
                "e\n" +
                "e\n" +
                "e\n" +
                "u\n" +
                "q\n" +
                "u\n", true, true, false));
    }

    @Test
    public void testCreateAsSelectGeoHashCharsLiteralNotChars() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(5b))",
                    null,
                    true,
                    true
            );
            try {
                insert("insert into geohash values(#sp@in)");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(27, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid constant: #sp@in");
            }
        });
    }

    @Test
    public void testCreateAsSelectGeoHashCharsLiteralTooFewChars() throws Exception {
        assertQuery(
                "geohash\n",
                "select geohash from geohash",
                "create table geohash (geohash geohash(11b))",
                null,
                true,
                true
        );
        try {
            insert("insert into geohash values(#sp)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(27, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: GEOHASH(2c) -> GEOHASH(11b) [from=#sp, to=geohash]");
        }
    }

    @Test
    public void testCreateAsSelectGeoHashCharsLiteralTooManyChars() throws Exception {
        assertQuery(
                "geohash\n",
                "select geohash from geohash",
                "create table geohash (geohash geohash(12c))",
                null,
                true,
                true
        );
        try {
            insert("insert into geohash values(#sp052w92p1p8889)");
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(27, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid constant: #sp052w92p1p8889");
        }
    }

    @Test
    public void testCreateAsSelectGeoHashCharsLiteralTruncating() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(6c))",
                    null,
                    true,
                    true
            );
            insert("insert into geohash values(#sp052w92p18)");
            assertSql("geohash\n" +
                    "sp052w\n", "geohash");
        });
    }

    @Test
    public void testCreateAsSelectGeoHashCharsLiteralWithWrongBits() throws Exception {
        assertFailure(7, "invalid constant: #sp052w92p1p87", "select #sp052w92p1p87");
        assertFailure(20, "missing bits size for GEOHASH constant", "select #sp052w92p1p8/");
        assertFailure(22, "missing bits size for GEOHASH constant", "select #sp052w92p1p8/ R");
        assertFailure(21, "missing bits size for GEOHASH constant", "select #sp052w92p1p8/0R");
        assertFailure(21, "missing bits size for GEOHASH constant", "select #sp052w92p1p8/t");
        assertFailure(21, "missing bits size for GEOHASH constant", "select #sp052w92p1p8/-1");
        assertFailure(7, "invalid bits size for GEOHASH constant", "select #sp052w92p1p8/ 61");
        assertFailure(7, "invalid constant: #sp052w92p1p8/011", "select #sp052w92p1p8/ 011");
        assertFailure(7, "invalid constant: #sp052w92p1p8/045", "select #sp052w92p1p8/045");
        assertFailure(7, "invalid constant: #sp/15", "select #sp/15"); // lacks precision
        assertFailure(7, "invalid bits size for GEOHASH constant: #/0", "select #/0");
        assertFailure(7, "invalid bits size for GEOHASH constant", "select #sp052w92p18/0");
    }

    @Test
    public void testCreateAsSelectGeoHashCharsPrecision() throws Exception {
        final String expected = "a\tb\n" +
                "9v1\t46s\n" +
                "jnw\tzfu\n" +
                "hp4\twh4\n" +
                "s2z\t1cj\n" +
                "mmt\t71f\n" +
                "fsn\tq4s\n" +
                "uzr\tjj5\n" +
                "ksu\tbuy\n";

        assertMemoryLeak(() -> {
            ddl("create table x as (" +
                    " select" +
                    " rnd_geohash(15) a," +
                    " rnd_geohash(15) b" +
                    " from long_sequence(8)" +
                    ")");
            assertSql(
                    expected, "x"
            );
        });
    }

    @Test
    public void testCreateAsSelectIOError() throws Exception {
        String sql = "create table y as (" +
                "select rnd_symbol(4,4,4,2) a from long_sequence(10000)" +
                "), cast(a as STRING)";

        final FilesFacade ff = new TestFilesFacadeImpl() {
            int mapCount = 0;

            @Override
            public long getMapPageSize() {
                return getPageSize();
            }

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (mapCount++ == 6) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };
        assertFailure(
                ff,
                sql,
                "Could not create table. See log for details"
        );
    }

    @Test
    public void testCreateAsSelectIOError2() throws Exception {
        String sql = "create table y as (" +
                "select rnd_symbol(4,4,4,2) a from long_sequence(10000)" +
                "), cast(a as STRING)";

        final FilesFacade ff = new TestFilesFacadeImpl() {
            private long metaFd;
            private int metaMapCount;
            private long txnFd;

            @Override
            public boolean close(int fd) {
                if (fd == metaFd) {
                    metaFd = -1;
                }
                if (fd == txnFd) {
                    txnFd = -1;
                }
                return super.close(fd);
            }

            @Override
            public long getMapPageSize() {
                return getPageSize();
            }

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                // this is very specific failure
                // it fails to open table writer metadata
                // and then fails to close txMem
                if (fd == metaFd) {
                    metaMapCount++;
                    return -1;
                }
                if (metaMapCount > 0 && fd == txnFd) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public int openRO(LPSZ name) {
                int fd = super.openRO(name);
                if (Chars.endsWith(name, Files.SEPARATOR + TableUtils.META_FILE_NAME)) {
                    metaFd = fd;
                }
                return fd;
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                int fd = super.openRW(name, opts);
                if (Chars.endsWith(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)) {
                    txnFd = fd;
                }
                return fd;
            }
        };

        assertFailure(
                ff,
                sql,
                "Could not create table. See log for details"

        );
    }

    @Test
    public void testCreateAsSelectInVolumeFail() throws Exception {
        try {
            assertQuery("geohash\n", "select geohash from geohash", "create table geohash (geohash geohash(1c)) in volume 'niza'", null, "insert into geohash " +
                    "select cast(rnd_str('q','u','e') as char) from long_sequence(10)", "geohash\n" +
                    "q\n" +
                    "q\n" +
                    "u\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "u\n" +
                    "q\n" +
                    "u\n", true, true, false);
            Assert.fail();
        } catch (SqlException e) {
            if (Os.isWindows()) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'in volume' is not supported on Windows");
            } else {
                TestUtils.assertContains(e.getFlyweightMessage(), "volume alias is not allowed [alias=niza]");
            }
        }
    }

    @Test
    public void testCreateAsSelectInVolumeNotAllowedAsItExistsAndCannotSoftLinkAndRemoveDir() throws Exception {
        Assume.assumeFalse(Os.isWindows()); // soft links not supported in windows
        File volume = temp.newFolder("other_path");
        String volumeAlias = "pera";
        String volumePath = volume.getAbsolutePath();
        String tableName = "geohash";
        String dirName = TableUtils.getTableDir(configuration.mangleTableDirNames(), tableName, 1, false);
        String target = volumePath + Files.SEPARATOR + dirName;
        AbstractCairoTest.ff = new TestFilesFacadeImpl() {
            @Override
            public boolean isDirOrSoftLinkDir(LPSZ path) {
                if (Chars.equals(path, target)) {
                    return false;
                }
                return super.exists(path);
            }

            @Override
            public int rmdir(Path name) {
                Assert.assertEquals(target + Files.SEPARATOR, name.toString());
                return -1;
            }

            @Override
            public int softLink(LPSZ src, LPSZ softLink) {
                Assert.assertEquals(target, src.toString());
                Assert.assertEquals(root + Files.SEPARATOR + dirName, softLink.toString());
                return -1;
            }
        };
        try {
            configuration.getVolumeDefinitions().of(volumeAlias + "->" + volumePath, path, root);
            assertQuery("geohash\n", "select geohash from " + tableName, "create table " + tableName + " (geohash geohash(1c)) in volume '" + volumeAlias + "'", null, "insert into " + tableName +
                    " select cast(rnd_str('q','u','e') as char) from long_sequence(10)", "geohash\n" +
                    "q\n" +
                    "q\n" +
                    "u\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "u\n" +
                    "q\n" +
                    "u\n", true, true, false);
            Assert.fail();
        } catch (SqlException e) {
            if (Os.isWindows()) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'in volume' is not supported on Windows");
            } else {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not create table, could not create soft link [src=" + target + ", tableDir=" + dirName + ']');
            }
        } finally {
            File table = new File(target);
            Assert.assertTrue(table.exists());
            Assert.assertTrue(table.isDirectory());
            Assert.assertEquals(0, FilesFacadeImpl.INSTANCE.rmdir(path.of(target).slash$()));
            Assert.assertTrue(volume.delete());
        }
    }

    @Test
    public void testCreateAsSelectInVolumeNotAllowedAsItNoLongerExists0() throws Exception {
        File volume = temp.newFolder("other_folder");
        String volumeAlias = "manzana";
        String volumePath = volume.getAbsolutePath();
        try {
            configuration.getVolumeDefinitions().of(volumeAlias + "->" + volumePath, path, root);
            Assert.assertTrue(volume.delete());
            assertQuery("geohash\n", "select geohash from geohash", "create table geohash (geohash geohash(1c)) in volume '" + volumeAlias + "'", null, "insert into geohash " +
                    "select cast(rnd_str('q','u','e') as char) from long_sequence(10)", "geohash\n" +
                    "q\n" +
                    "q\n" +
                    "u\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "e\n" +
                    "u\n" +
                    "q\n" +
                    "u\n", true, true, false);
            Assert.fail();
        } catch (SqlException | CairoException e) {
            if (Os.isWindows()) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'in volume' is not supported on Windows");
            } else {
                TestUtils.assertContains(e.getFlyweightMessage(), "not a valid path for volume [alias=" + volumeAlias + ", path=" + volumePath + ']');
            }
        } finally {
            Assert.assertFalse(volume.delete());
        }
    }

    @Test
    public void testCreateAsSelectInvalidTimestamp() throws Exception {
        assertFailure(97, "TIMESTAMP column expected",
                "create table y as (" +
                        "select * from (select rnd_int(0, 30, 2) a from long_sequence(20))" +
                        ")  timestamp(a) partition by DAY"
        );
    }

    @Test
    public void testCreateAsSelectRemoveColumn() throws SqlException {
        assertCreateTableAsSelect(
                "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"t\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":1}",
                "create table Y as (select * from X) timestamp(t)",
                new Fiddler() {
                    int state = 0;

                    @Override
                    public boolean isHappy() {
                        return state > 1;
                    }

                    @Override
                    public void run(CairoEngine engine) {
                        if (state++ == 1) {
                            // remove column from table X
                            try (TableWriter writer = getWriter("X")) {
                                writer.removeColumn("b");
                            }
                        }
                    }
                }
        );
    }

    @Test
    public void testCreateAsSelectRemoveColumnFromCast() {
        // because the column we delete is used in "cast" expression this SQL must fail
        try {
            assertCreateTableAsSelect(
                    "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"t\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":1}",
                    "create table Y as (select * from X), cast (b as DOUBLE) timestamp(t)",
                    new Fiddler() {
                        int state = 0;

                        @Override
                        public boolean isHappy() {
                            return state > 1;
                        }

                        @Override
                        public void run(CairoEngine engine) {
                            if (state++ == 1) {
                                // remove column from table X
                                try (TableWriter writer = getWriter("X")) {
                                    writer.removeColumn("b");
                                }
                            }
                        }
                    }
            );
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(43, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: b");
        }
    }

    @Test
    public void testCreateAsSelectReplaceColumn() throws SqlException {
        assertCreateTableAsSelect(
                "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"b\",\"type\":\"INT\"},{\"index\":1,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":2,\"name\":\"c\",\"type\":\"FLOAT\"}],\"timestampIndex\":1}",
                "create table Y as (select * from X) timestamp(t)",
                new Fiddler() {
                    int state = 0;

                    @Override
                    public boolean isHappy() {
                        return state > 1;
                    }

                    @Override
                    public void run(CairoEngine engine) {
                        if (state++ == 1) {
                            // remove column from table X
                            try (TableWriter writer = getWriter("X")) {
                                writer.removeColumn("a");
                                writer.addColumn("c", ColumnType.FLOAT);
                            }
                        }
                    }
                }
        );
    }

    @Test
    public void testCreateAsSelectReplaceTimestamp() {
        try {
            assertCreateTableAsSelect(
                    "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"INT\"},{\"index\":2,\"name\":\"t\",\"type\":\"FLOAT\"}],\"timestampIndex\":-1}",
                    "create table Y as (select * from X) timestamp(t)",
                    new Fiddler() {
                        int state = 0;

                        @Override
                        public boolean isHappy() {
                            return state > 1;
                        }

                        @Override
                        public void run(CairoEngine engine) {
                            if (state++ == 1) {
                                // remove column from table X
                                try (TableWriter writer = getWriter("X")) {
                                    writer.removeColumn("t");
                                    writer.addColumn("t", ColumnType.FLOAT);
                                }
                            }
                        }
                    }
            );
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(46, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "TIMESTAMP column expected");
        }
    }

    @Test
    public void testCreateEmptyTableNoPartition() throws SqlException {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t)"
        );

        try (TableReader reader = getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink
            );

            Assert.assertEquals(PartitionBy.NONE, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableNoTimestamp() throws SqlException {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) "
        );

        try (TableReader reader = getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":-1}",
                    sink
            );

            Assert.assertEquals(PartitionBy.NONE, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolCache() throws SqlException {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );

        try (TableReader reader = getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink
            );

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolNoCache() throws SqlException {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 nocache, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );

        try (TableReader reader = getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink
            );

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertFalse(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableWithIndex() throws SqlException {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "c SHORT, " +
                        "d LONG, " +
                        "e FLOAT, " +
                        "f DOUBLE, " +
                        "g DATE, " +
                        "h BINARY, " +
                        "t TIMESTAMP, " +
                        "x SYMBOL capacity 16 cache index capacity 2048, " +
                        "z STRING, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );

        try (TableReader reader = getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2048},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink
            );

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateTableFail() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 8; // this count is very deliberately coincidental with

            // number of rows we are appending
            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (count-- != 0) {
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
                return -1;
            }
        };

        assertFailure(
                ff,
                "create table x as (select cast(x as int) c, abs(rnd_int() % 650) a from long_sequence(5000000))",
                "Could not create table. See log for details"
        );
    }

    @Test
    public void testCreateTableUtf8() throws SqlException {
        ddl("create table доходы(экспорт int)");

        try (TableWriter writer = getWriter("доходы")) {
            for (int i = 0; i < 20; i++) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, i);
                row.append();
            }
            writer.commit();
        }

        ddl("create table миллионы as (select * from доходы)");

        final String expected = "экспорт\n" +
                "0\n" +
                "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n" +
                "6\n" +
                "7\n" +
                "8\n" +
                "9\n" +
                "10\n" +
                "11\n" +
                "12\n" +
                "13\n" +
                "14\n" +
                "15\n" +
                "16\n" +
                "17\n" +
                "18\n" +
                "19\n";

        assertReader(
                expected,
                "миллионы"
        );
    }

    @Test
    public void testCreateTableWithO3() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl(
                            "create table x (" +
                                    "a INT, " +
                                    "t TIMESTAMP, " +
                                    "y BOOLEAN) " +
                                    "timestamp(t) " +
                                    "partition by DAY WITH maxUncommittedRows=10000, o3MaxLag=250ms;"
                    );

                    try (TableWriter writer = getWriter("x");
                         TableMetadata tableMetadata = engine.getMetadata(writer.getTableToken())) {
                        sink.clear();
                        tableMetadata.toJson(sink);
                        TestUtils.assertEquals(
                                "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":2,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":1}",
                                sink
                        );
                        Assert.assertEquals(10000, tableMetadata.getMaxUncommittedRows());
                        Assert.assertEquals(250000, tableMetadata.getO3MaxLag());
                    }
                }
        );
    }

    @Test
    public void testDeallocateMissingStatementName() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertException("DEALLOCATE");
            } catch (SqlException e) {
                Assert.assertEquals(10, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "statement name expected");
            }
        });
    }

    @Test
    public void testDeallocateMultipleStatementNames() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertException("deallocate foo bar");
            } catch (SqlException e) {
                Assert.assertEquals(15, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [bar]");
            }
        });
    }

    @Test
    public void testDuplicateTableName() throws Exception {
        ddl(
                "create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
        engine.releaseAllWriters();

        assertFailure(13, "table already exists",
                "create table x (" +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH"
        );
    }

    @Test
    public void testEmptyQuery() throws Exception {
        assertException(
                "                        ",
                0,
                "empty query"
        );
    }

    @Test
    public void testExecuteQuery() throws Exception {
        assertFailure(
                68,
                "not a TIMESTAMP",
                "select * from (select rnd_int() x from long_sequence(20)) timestamp(x)"
        );
    }

    @Test
    public void testExpectedKeyword() throws Exception {
        final GenericLexer lexer = new GenericLexer(configuration.getSqlLexerPoolCapacity());
        lexer.of("keyword1 keyword2\nkeyword3\tkeyword4");
        SqlCompilerImpl.expectKeyword(lexer, "keyword1");
        SqlCompilerImpl.expectKeyword(lexer, "keyword2");
        SqlCompilerImpl.expectKeyword(lexer, "keyword3");
        SqlCompilerImpl.expectKeyword(lexer, "keyword4");
    }

    @Test
    public void testFailOnBadFunctionCallInOrderBy() throws Exception {
        ddl("create table test(time TIMESTAMP, symbol STRING);");

        assertFailure(97, "unexpected argument for function: SUM. expected args: (DOUBLE). actual args: (INT constant,INT constant)",
                "SELECT test.time AS ref0, test.symbol AS ref1 FROM test GROUP BY test.time, test.symbol ORDER BY SUM(1, -1)"
        );
    }

    @Test
    public void testFailOnEmptyColumnName() throws Exception {
        ddl("create table tab ( ts timestamp)");

        assertFailure(28, "Invalid column: ",
                "SELECT * FROM tab WHERE SUM(\"\", \"\")"
        );
    }

    @Test
    public void testFailOnEmptyInClause() throws Exception {
        ddl("create table tab(event short);");

        assertFailure(54, "too few arguments for 'in' [found=1,expected=2]", "SELECT COUNT(*) FROM tab WHERE tab.event > (tab.event IN ) ");
        assertFailure(54, "too few arguments for 'in'", "SELECT COUNT(*) FROM tab WHERE tab.event > (tab.event IN ())");
        assertFailure(54, "too few arguments for 'in'", "SELECT COUNT(*) FROM tab WHERE tab.event > (tab.event IN ()");
        assertFailure(13, "too few arguments for 'in'", "SELECT event IN () FROM tab");
        assertFailure(60, "too few arguments for 'in'", "SELECT COUNT(*) FROM tab a join tab b on a.event > (b.event IN ())");
    }

    @Test
    public void testFreesMemoryOnClose() {
        // NATIVE_SQL_COMPILER is excluded from TestUtils#assertMemoryLeak(),
        // so here we make sure that SQL compiler releases its memory on close.

        Path.clearThreadLocals();
        long mem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SQL_COMPILER);

        new SqlCompilerImpl(engine).close();

        Path.clearThreadLocals();
        long memAfter = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_SQL_COMPILER);

        Assert.assertEquals(mem, memAfter);
    }

    @Test
    public void testGeoLiteralAsColName() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str('#1234', '#88484') as \"#0101a\" from long_sequence(5) )");
            assertSql("#0101a\n" +
                    "#1234\n" +
                    "#1234\n", "select * from x where \"#0101a\" = '#1234'");
        });
    }

    @Test
    public void testGeoLiteralAsColName2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_geohash(14) as \"#0101a\" from long_sequence(5) )");
            assertSql("#0101a\n", "select * from x where #1234 = \"#0101a\"");
        });
    }

    @Test
    public void testGeoLiteralBinLength() throws Exception {
        assertMemoryLeak(() -> {
            StringSink bitString = Misc.getThreadLocalBuilder();
            bitString.put(Chars.repeat("0", 59)).put('1');
            assertSql("geobits\n" +
                    "000000000001\n", "select ##" + bitString + " as geobits");
        });
    }

    @Test
    public void testGeoLiteralInvalid1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str('#1234', '#88484') as str from long_sequence(1000) )");
            try {
                assertException("select * from x where str = #1234 '"); // random char at the end
            } catch (SqlException ex) {
                // Add error test assertion
                Assert.assertEquals("[34] dangling expression", ex.getMessage());
            }
        });
    }

    @Test
    public void testGeoLiteralInvalid2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str('#1234', '#88484') as str from long_sequence(1000) )");
            try {
                assertException("select * from x where str = #1234'"); // random char at the end
            } catch (SqlException ex) {
                // Add error test assertion
                Assert.assertEquals("[33] dangling expression", ex.getMessage());
            }
        });
    }

    @Test
    public void testGetCurrentUser() throws SqlException {
        assertQuery("current_user\n" +
                "admin\n", "select current_user()", null, true, true);
    }

    @Test
    public void testInLongTypeMismatch() throws Exception {
        assertFailure(43, "cannot compare LONG with type DOUBLE", "select 1 from long_sequence(1) where x in (123.456)");
    }

    @Test
    public void testInnerJoinConditionPushdown() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab ( created timestamp, value long ) timestamp(created) ");
            compile("insert into tab values (0, 0), (1, 1), (2,2)");

            for (String join : new String[]{"", "LEFT", "LT", "ASOF",}) {
                assertSql(
                        "count\n3\n",
                        "SELECT count(T2.created) " +
                                "FROM tab as T1 " +
                                "JOIN (SELECT * FROM tab) as T2 ON T1.created < T2.created " +
                                join + " JOIN tab as T3 ON T2.value=T3.value"
                );
            }
            assertSql(
                    "count\n1\n",
                    "SELECT count(T2.created) " +
                            "FROM tab as T1 " +
                            "JOIN tab T2 ON T1.created < T2.created " +
                            "JOIN (SELECT * FROM tab) as T3 ON T2.value=T3.value " +
                            "JOIN tab T4 on T3.created < T4.created"
            );

            assertSql(
                    "count\n3\n",
                    "SELECT count(T2.created) " +
                            "FROM tab as T1 " +
                            "JOIN tab T2 ON T1.created < T2.created " +
                            "JOIN (SELECT * FROM tab) as T3 ON T2.value=T3.value " +
                            "LEFT JOIN tab T4 on T3.created < T4.created"
            );

            assertSql(
                    "count\n3\n",
                    "SELECT count(T2.created) " +
                            "FROM tab as T1 " +
                            "JOIN tab T2 ON T1.created < T2.created " +
                            "JOIN (SELECT * FROM tab) as T3 ON T2.value=T3.value " +
                            "LEFT JOIN tab T4 on T3.created-T4.created = 0 "
            );
        });
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.1911234617573182\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.04142812470232493\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.7588175403454873\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.14830552335848957\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.33747075654972813\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696942726644\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.11371841836123953\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.05514933756198426\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.18586435581637295\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.49199001716312474\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630095\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514582851\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.7039785408034679\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.6506604601705693\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.8034049105590781\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.2862717364877081\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.10820602386069589\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, h SHORT, i DATE, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_int(0, 30, 2)," +
                        " rnd_boolean()," +
                        " rnd_str(3,3,2)," +
                        " rnd_double(2)," +
                        " rnd_float(2)," +
                        " rnd_short(10,1024)," +
                        " rnd_short()," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " rnd_symbol(4,4,4,2)," +
                        " rnd_long(100,200,2)," +
                        " rnd_long()," +
                        " timestamp_sequence(0, 1000000000)," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20)",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnList() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.1911234617573182\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.04142812470232493\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.7588175403454873\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.14830552335848957\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.33747075654972813\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696942726644\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.11371841836123953\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.05514933756198426\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.18586435581637295\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.49199001716312474\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630095\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514582851\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.7039785408034679\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.6506604601705693\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.8034049105590781\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.2862717364877081\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.10820602386069589\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, h SHORT, i DATE, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY)",
                "insert into x (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p) " +
                        "select" +
                        " rnd_int()," +
                        " rnd_int(0, 30, 2)," +
                        " rnd_boolean()," +
                        " rnd_str(3,3,2)," +
                        " rnd_double(2)," +
                        " rnd_float(2)," +
                        " rnd_short(10,1024)," +
                        " rnd_short()," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " rnd_symbol(4,4,4,2)," +
                        " rnd_long(100,200,2)," +
                        " rnd_long()," +
                        " timestamp_sequence(0, 1000000000)," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20)",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnListAndNoTimestamp() throws Exception {
        try {
            testInsertAsSelect(
                    "",
                    "create table x (a INT, n TIMESTAMP, o BYTE, p BINARY) timestamp(n) partition by DAY",
                    "insert into x (a) " +
                            "select * from (select" +
                            " rnd_int()" +
                            " from long_sequence(5))",
                    "select * from x"
            );
            Assert.fail();
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "select clause must provide timestamp column");
        }
    }

    @Test
    public void testInsertAsSelectColumnListAndTimestamp() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.1911234617573182\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.04142812470232493\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.7588175403454873\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.14830552335848957\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.33747075654972813\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696942726644\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.11371841836123953\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.05514933756198426\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.18586435581637295\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.49199001716312474\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630095\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514582851\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.7039785408034679\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.6506604601705693\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.8034049105590781\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.2862717364877081\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.10820602386069589\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, h SHORT, i DATE, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY) timestamp(n)",
                "insert into x (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p) " +
                        "select * from (select" +
                        " rnd_int()," +
                        " rnd_int(0, 30, 2)," +
                        " rnd_boolean()," +
                        " rnd_str(3,3,2)," +
                        " rnd_double(2)," +
                        " rnd_float(2)," +
                        " rnd_short(10,1024)," +
                        " rnd_short()," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                        " rnd_symbol(4,4,4,2)," +
                        " rnd_long(100,200,2)," +
                        " rnd_long()," +
                        " timestamp_sequence(0, 1000000000) ts," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20)) timestamp(ts)",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnListAndTimestampO3() throws Exception {
        String expectedData = "a\tn\to\tp\n" +
                "73575701\t1970-01-01T04:26:40.000000Z\t0\t\n" +
                "-727724771\t1970-01-01T04:43:20.000000Z\t0\t\n" +
                "1548800833\t1970-01-01T05:00:00.000000Z\t0\t\n" +
                "315515118\t1970-01-01T05:16:40.000000Z\t0\t\n" +
                "-1148479920\t1970-01-01T05:33:20.000000Z\t0\t\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, n TIMESTAMP, o BYTE, p BINARY) timestamp(n) partition by DAY",
                "insert into x (a, n) " +
                        "select * from (select" +
                        " rnd_int()," +
                        " timestamp_sequence(20 * 1000000000L, -1000000000L) ts" +
                        " from long_sequence(5))",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnListAndTimestampOfWrongType() throws Exception {
        try {
            testInsertAsSelect(
                    "",
                    "create table x (a INT, n TIMESTAMP, o BYTE, p BINARY) timestamp(n)",
                    "insert into x (a, n) " +
                            "select * from (select" +
                            " rnd_int(), " +
                            "rnd_int() " +
                            " from long_sequence(5))",
                    "select * from x"
            );
            Assert.fail();
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "expected timestamp column but type is INT");
        }
    }

    @Test
    public void testInsertAsSelectColumnSubset() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\tj\tk\tl\tm\tn\to\tp\n" +
                "NaN\tNaN\tfalse\t\t0.8043224099968393\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:00:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.08486964232560668\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:16:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.0843832076262595\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:33:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.6508594025855301\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:50:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7905675319675964\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:06:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.22452340856088226\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:23:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.3491070363730514\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:40:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7611029514995744\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:56:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.4217768841969397\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:13:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:30:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7261136209823622\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:46:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.4224356661645131\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:03:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7094360487171202\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:20:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.38539947865244994\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:36:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.0035983672154330515\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:53:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.3288176907679504\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:10:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:26:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9771103146051203\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:43:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.24808812376657652\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:00:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.6381607531178513\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:16:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.12503042190293423\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:33:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9038068796506872\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:50:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.13450170570900255\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:06:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.8912587536603974\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:23:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9755263540567968\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:40:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.26922103479744897\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:56:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.4138164748227684\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:13:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.5522494170511608\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:30:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.2459345277606021\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:46:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T08:03:20.000000Z\t0\t\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY)",
                "insert into x (e,n)" +
                        "select" +
                        " rnd_double(2)," +
                        " timestamp_sequence(0, 1000000000)" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectColumnSubset2() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\tj\tk\tl\tm\tn\to\tp\n" +
                "NaN\tNaN\tfalse\t\t0.8043224099968393\tNaN\t-13027\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.2845577791213847\tNaN\t21015\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9344604857394011\tNaN\t-5356\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7905675319675964\tNaN\t-19832\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.8899286912289663\tNaN\t23922\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t31987\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.4621835429127854\tNaN\t-4472\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.8072372233384567\tNaN\t4924\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.6276954028373309\tNaN\t-11679\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7094360487171202\tNaN\t-12348\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.1985581797355932\tNaN\t-8877\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.5249321062686694\tNaN\t13182\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t2056\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.21583224269349388\tNaN\t12941\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.8146807944500559\tNaN\t-5176\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.12503042190293423\tNaN\t-7976\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9687423276940171\tNaN\t15926\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.6700476391801052\tNaN\t2276\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.9755263540567968\tNaN\t5639\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.810161274171258\tNaN\t-391\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.3762501709498378\tNaN\t-30933\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.2459345277606021\tNaN\t20366\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.975019885372507\tNaN\t-3567\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.4900510449885239\tNaN\t3428\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t29978\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.04142812470232493\tNaN\t-19136\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.7997733229967019\tNaN\t-21442\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.6590341607692226\tNaN\t-2018\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.17370570324289436\tNaN\t9478\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.04645849844580874\tNaN\t6093\t\t\tNaN\tNaN\t\t0\t\n";

        testInsertAsSelect(
                expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY)",
                "insert into x (e,g)" +
                        "select" +
                        " rnd_double(2)," +
                        " rnd_short()" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectConvertible1() throws Exception {
        testInsertAsSelect(
                "a\tb\n" +
                        "-1148479920\tJWCPS\n" +
                        "592859671\tYRXPE\n" +
                        "806715481\tRXGZS\n" +
                        "1904508147\tXIBBT\n" +
                        "-85170055\tGWFFY\n" +
                        "-1715058769\tEYYQE\n" +
                        "-2119387831\tHFOWL\n" +
                        "-938514914\tXYSBE\n" +
                        "-461611463\tOJSHR\n" +
                        "-1272693194\tDRQQU\n" +
                        "-2144581835\tFJGET\n" +
                        "-296610933\tSZSRY\n" +
                        "1637847416\tBVTMH\n" +
                        "1627393380\tOZZVD\n" +
                        "-372268574\tMYICC\n" +
                        "-661194722\tOUICW\n" +
                        "-1201923128\tGHVUV\n" +
                        "-1950552842\tOTSED\n" +
                        "-916132123\tCTGQO\n" +
                        "659736535\tXWCKY\n" +
                        "-2075675260\tUWDSW\n" +
                        "1060917944\tSHOLN\n" +
                        "-1966408995\tIQBZX\n" +
                        "2124174232\tVIKJS\n" +
                        "-2088317486\tSUQSR\n" +
                        "1245795385\tKVVSJ\n" +
                        "116799613\tIPHZE\n" +
                        "359345889\tHVLTO\n" +
                        "-640305320\tJUMLG\n" +
                        "2011884585\tMLLEO\n",
                "create table x (a INT, b SYMBOL)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(5,5,0)" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectConvertible2() throws Exception {
        testInsertAsSelect(
                "b\ta\n" +
                        "-2144581835\tSBEOUOJSH\n" +
                        "-1162267908\tUEDRQQU\n" +
                        "-1575135393\tHYRXPEH\n" +
                        "326010667\t\n" +
                        "-1870444467\tSBEOUOJSH\n" +
                        "1637847416\tUEDRQQU\n" +
                        "-1533414895\tTJWCPS\n" +
                        "-1515787781\tBBTGPGWF\n" +
                        "1920890138\tTJWCPS\n" +
                        "-1538602195\tSBEOUOJSH\n" +
                        "-235358133\tRXGZSXUX\n" +
                        "-10505757\tHYRXPEH\n" +
                        "-661194722\tYUDEYYQEHB\n" +
                        "1196016669\t\n" +
                        "-1566901076\t\n" +
                        "-1201923128\t\n" +
                        "1876812930\tFOWLPDX\n" +
                        "-1424048819\tRXGZSXUX\n" +
                        "1234796102\t\n" +
                        "-45567293\tUEDRQQU\n" +
                        "-89906802\t\n" +
                        "-998315423\tYUDEYYQEHB\n" +
                        "-1794809330\tHYRXPEH\n" +
                        "659736535\t\n" +
                        "852921272\tSBEOUOJSH\n",
                "create table x (b INT, a STRING)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_symbol(8,6,10,2)" +
                        " from long_sequence(25)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectConvertibleList1() throws Exception {
        testInsertAsSelect(
                "a\tb\tn\n" +
                        "JWCPS\t-1148479920\t\n" +
                        "YRXPE\t592859671\t\n" +
                        "RXGZS\t806715481\t\n" +
                        "XIBBT\t1904508147\t\n" +
                        "GWFFY\t-85170055\t\n" +
                        "EYYQE\t-1715058769\t\n" +
                        "HFOWL\t-2119387831\t\n" +
                        "XYSBE\t-938514914\t\n" +
                        "OJSHR\t-461611463\t\n" +
                        "DRQQU\t-1272693194\t\n" +
                        "FJGET\t-2144581835\t\n" +
                        "SZSRY\t-296610933\t\n" +
                        "BVTMH\t1637847416\t\n" +
                        "OZZVD\t1627393380\t\n" +
                        "MYICC\t-372268574\t\n" +
                        "OUICW\t-661194722\t\n" +
                        "GHVUV\t-1201923128\t\n" +
                        "OTSED\t-1950552842\t\n" +
                        "CTGQO\t-916132123\t\n" +
                        "XWCKY\t659736535\t\n" +
                        "UWDSW\t-2075675260\t\n" +
                        "SHOLN\t1060917944\t\n" +
                        "IQBZX\t-1966408995\t\n" +
                        "VIKJS\t2124174232\t\n" +
                        "SUQSR\t-2088317486\t\n" +
                        "KVVSJ\t1245795385\t\n" +
                        "IPHZE\t116799613\t\n" +
                        "HVLTO\t359345889\t\n" +
                        "JUMLG\t-640305320\t\n" +
                        "MLLEO\t2011884585\t\n",
                "create table x (a SYMBOL, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(5,5,0)" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectConvertibleList2() throws Exception {
        testInsertAsSelect(
                "a\tb\tn\n" +
                        "SBEOUOJSH\t-2144581835\t\n" +
                        "UEDRQQU\t-1162267908\t\n" +
                        "HYRXPEH\t-1575135393\t\n" +
                        "\t326010667\t\n" +
                        "SBEOUOJSH\t-1870444467\t\n" +
                        "UEDRQQU\t1637847416\t\n" +
                        "TJWCPS\t-1533414895\t\n" +
                        "BBTGPGWF\t-1515787781\t\n" +
                        "TJWCPS\t1920890138\t\n" +
                        "SBEOUOJSH\t-1538602195\t\n" +
                        "RXGZSXUX\t-235358133\t\n" +
                        "HYRXPEH\t-10505757\t\n" +
                        "YUDEYYQEHB\t-661194722\t\n" +
                        "\t1196016669\t\n" +
                        "\t-1566901076\t\n" +
                        "\t-1201923128\t\n" +
                        "FOWLPDX\t1876812930\t\n" +
                        "RXGZSXUX\t-1424048819\t\n" +
                        "\t1234796102\t\n" +
                        "UEDRQQU\t-45567293\t\n" +
                        "\t-89906802\t\n" +
                        "YUDEYYQEHB\t-998315423\t\n" +
                        "HYRXPEH\t-1794809330\t\n" +
                        "\t659736535\t\n" +
                        "SBEOUOJSH\t852921272\t\n" +
                        "YUDEYYQEHB\t-1172180184\t\n" +
                        "SBEOUOJSH\t1254404167\t\n" +
                        "FOWLPDX\t-1768335227\t\n" +
                        "\t1060917944\t\n" +
                        "\t2060263242\t\n",
                "create table x (a STRING, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_symbol(8,6,10,2)" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectDuplicateColumn() throws Exception {
        ddl(
                "CREATE TABLE tab (" +
                        "  ts TIMESTAMP, " +
                        "  x INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );

        engine.releaseAllWriters();

        assertFailure(21, "Duplicate column [name=X]",
                "insert into tab ( x, 'X', ts ) values ( 7, 10, 11 )"
        );
    }

    @Test
    public void testInsertAsSelectDuplicateColumnNonAscii() throws Exception {
        ddl(
                "CREATE TABLE tabula (" +
                        "  ts TIMESTAMP, " +
                        "  龜 INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );

        engine.releaseAllWriters();

        assertFailure(24, "Duplicate column [name=龜]",
                "insert into tabula ( 龜, '龜', ts ) values ( 7, 10, 11 )"
        );
    }

    public void testInsertAsSelectError(
            CharSequence ddl,
            CharSequence insert,
            int errorPosition,
            CharSequence errorMessage
    ) throws Exception {
        testInsertAsSelectError(ddl, insert, errorPosition, errorMessage, SqlException.class);
    }

    public void testInsertAsSelectError(
            CharSequence ddl,
            CharSequence insert,
            int errorPosition,
            CharSequence errorMessage,
            Class<?> exception
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                ddl(ddl);
            }
            assertFailure0(errorPosition, errorMessage, insert, exception);
        });
    }

    @Test
    public void testInsertAsSelectFewerSelectColumns() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table y as (select x, cast(2*((x-1)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(10))");
            try {
                assertException("insert into y select cast(2*((x-1+10)/2) as int)+2 m, abs(rnd_int() % 100) b from long_sequence(6)");
            } catch (SqlException e) {
                Assert.assertEquals(14, e.getPosition());
                Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "not enough"));
            }
        });
    }

    @Test
    public void testInsertAsSelectInconvertible1() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_date( to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0)" +
                        " from long_sequence(30)", 32, "inconvertible types"
        );
    }

    @Test
    public void testInsertAsSelectInconvertible2() throws Exception {
        testInsertAsSelectError(
                "create table x (a INT, b BYTE)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_char()" +
                        " from long_sequence(30)",
                -1,
                "inconvertible value: T [CHAR -> BYTE]",
                ImplicitCastException.class
        );
    }

    @Test
    public void testInsertAsSelectInconvertibleList1() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_date( to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0)" +
                        " from long_sequence(30)", 17, "inconvertible types"
        );
    }

    @Test
    public void testInsertAsSelectInconvertibleList2() throws Exception {
        testInsertAsSelectError(
                "create table x (a BYTE, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_char()" +
                        " from long_sequence(30)",
                -1,
                "inconvertible value: T [CHAR -> BYTE]",
                ImplicitCastException.class
        );
    }

    @Test
    public void testInsertAsSelectInconvertibleList3() throws Exception {
        testInsertAsSelectError(
                "create table x (a BYTE, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_char()" +
                        " from long_sequence(30)",
                -1,
                "inconvertible value: T [CHAR -> BYTE]",
                ImplicitCastException.class
        );
    }

    @Test
    public void testInsertAsSelectInconvertibleList4() throws Exception {
        testInsertAsSelectError("create table x (a DATE, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_double(2)" +
                        " from long_sequence(30)", 17, "inconvertible types"
        );
    }

    @Test
    public void testInsertAsSelectInconvertibleList5() throws Exception {
        testInsertAsSelectError(
                "create table x (a FLOAT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(5,5,0)" +
                        " from long_sequence(30)",
                -1,
                "inconvertible value: `JWCPS` [STRING -> FLOAT]",
                ImplicitCastException.class
        );
    }

    @Test
    public void testInsertAsSelectInvalidColumn() throws Exception {
        testInsertAsSelectError("create table x (aux1 INT, b INT)",
                "insert into x (aux1,blast)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 20, "Invalid column: blast"
        );
    }

    @Test
    public void testInsertAsSelectPersistentIOError() throws Exception {
        AtomicBoolean inError = new AtomicBoolean(true);

        FilesFacade ff = new TestFilesFacadeImpl() {
            int pageCount = 0;

            @Override
            public long getMapPageSize() {
                return getPageSize();
            }

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (inError.get() && pageCount++ > 12) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };

        assertInsertAsSelectIOError(inError, ff);
    }

    @Test
    public void testInsertAsSelectReplaceColumn() throws Exception {
        final String expected = "a\tb\n" +
                "315515118\tNaN\n" +
                "-727724771\tNaN\n" +
                "-948263339\tNaN\n" +
                "592859671\tNaN\n" +
                "-847531048\tNaN\n" +
                "-2041844972\tNaN\n" +
                "-1575378703\tNaN\n" +
                "1545253512\tNaN\n" +
                "1573662097\tNaN\n" +
                "339631474\tNaN\n";

        Fiddler fiddler = new Fiddler() {
            int state = 0;

            @Override
            public boolean isHappy() {
                return state > 1;
            }

            @Override
            public void run(CairoEngine engine) {
                if (state++ == 1) {
                    // remove column from table X
                    try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
                        writer.removeColumn("int1");
                        writer.addColumn("c", ColumnType.INT);
                    }
                }
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration) {
                @Override
                public TableReader getReader(TableToken tableToken, long metadataVersion) {
                    fiddler.run(this);
                    return super.getReader(tableToken, metadataVersion);
                }
            }) {
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
                ) {

                    compiler.compile("create table x (a INT, b INT)", sqlExecutionContext);
                    compiler.compile("create table y as (select rnd_int() int1, rnd_int() int2 from long_sequence(10))", sqlExecutionContext);
                    // we need to pass the engine here, so the global test context won't do
                    compiler.compile("insert into x select * from y", sqlExecutionContext);

                    TestUtils.assertSql(
                            compiler,
                            // we need to pass the engine here, so the global test context won't do
                            sqlExecutionContext,
                            "select * from x",
                            sink,
                            expected
                    );
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
            }
        });
    }

    @Test
    public void testInsertAsSelectTableNotFound() throws Exception {
        testInsertAsSelectError(null, "insert into x (e,n)" +
                "select" +
                " rnd_double(2)," +
                " timestamp_sequence(0, 1000000000)" +
                " from long_sequence(30)", 12, "table does not exist [table=x]");
    }

    @Test
    public void testInsertAsSelectTemporaryIOError() throws Exception {
        AtomicBoolean inError = new AtomicBoolean(true);

        FilesFacade ff = new TestFilesFacadeImpl() {
            int pageCount = 0;

            @Override
            public long getMapPageSize() {
                return getPageSize();
            }

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (inError.get() && pageCount++ == 13) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };

        assertInsertAsSelectIOError(inError, ff);
    }

    @Test
    public void testInsertAsSelectTimestampNotSelected() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT, n TIMESTAMP) timestamp(n)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_int()" +
                        " from long_sequence(30)", 12, "select clause must provide timestamp column"
        );
    }

    @Test
    public void testInsertFromStringToLong256() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t as (select rnd_long256 v from long_sequence(1000))", sqlExecutionContext);
            ddl("create table l256(v long256)", sqlExecutionContext);
            ddl("insert into l256 select * from t", sqlExecutionContext);
            if (configuration.getWalEnabledDefault()) {
                drainWalQueue();
            }
            String expected = "v\n" +
                    "0xd29b84cdf070d2247559d6d5f9ed17242a1c9ad2bbc87e8041738668eaea02fa\n" +
                    "0xc3fd21defa26f6555ab5573037d8a34872a8be1517a17fd4e43cb3b6894fc88c\n" +
                    "0xc78d67954cb7866695b5e08df69df8819fc909a43f149089c143a3bb982af031\n" +
                    "0x6ddedcf7415306f799ce31489578cac77b0ec57771d6e9f27c517f53d504487d\n" +
                    "0xa38b2ad7fbc79d366f9b5d1b162ba472613f1eb5f98a2df86a7f0ebbd1d28a95\n";
            printSqlResult(expected, "t limit -5", null, true, false);
            printSqlResult(expected, "l256 limit -5", null, true, false);
        });
    }

    @Test
    public void testInsertGeoHashBitsLiteralNotBits() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(5b))",
                    null,
                    true,
                    true
            );
            try {
                insert("insert into geohash values(##11211)");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(27, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid constant: ##11211");
            }
        });
    }

    @Test
    public void testInsertGeoHashBitsLiteralTooFewBits() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    "create table geohash (geohash geohash(6b))",
                    null,
                    true,
                    true
            );
            try {
                insert("insert into geohash values(##10001)");
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(27, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: GEOHASH(1c) -> GEOHASH(6b) [from=##10001, to=geohash]");
            }
        });
    }

    @Test
    public void testInsertGeoHashByteSizedStorage1() throws Exception {
        testGeoHashWithBits("1c", "'s'",
                "geohash\n" +
                        "s\n"
        );
    }

    @Test
    public void testInsertGeoHashByteSizedStorage2() throws Exception {
        testGeoHashWithBits("4b", "cast('s' as geohash(4b))",
                "geohash\n" +
                        "1100\n"
        );
    }

    @Test
    public void testInsertGeoHashByteSizedStorage3() throws Exception {
        testGeoHashWithBits("6b", "##100011",
                "geohash\n" +
                        "100011\n"
        );
    }

    @Test
    public void testInsertGeoHashByteSizedStorage4() throws Exception {
        testGeoHashWithBits("3b", "##100011",
                "geohash\n" +
                        "100\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteral() throws Exception {
        testGeoHashWithBits("8c", "#sp052w92p1p8",
                "geohash\n" +
                        "sp052w92\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits1() throws Exception {
        testGeoHashWithBits("8c", "#sp052w92p1p8/40",
                "geohash\n" +
                        "sp052w92\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits2() throws Exception {
        testGeoHashWithBits("2b", "#0/2",
                "geohash\n" +
                        "00\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits3() throws Exception {
        testGeoHashWithBits("9b", "#100/9",
                "geohash\n" +
                        "000010000\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits4() throws Exception {
        testGeoHashWithBits("5b", "#1",
                "geohash\n" +
                        "1\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits5() throws Exception {
        testGeoHashWithBits("4b", "#1/4",
                "geohash\n" +
                        "0000\n"
        );
    }

    @Test
    public void testInsertGeoHashCharsLiteralWithBits6() throws Exception {
        testGeoHashWithBits("20b", "#1110",
                "geohash\n" +
                        "1110\n"
        );
    }

    @Test
    public void testInsertLong256() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "long256\n",
                "long256",
                "create table long256 (long256 long256)",
                null,
                "insert into long256 values" +
                        "('0x6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3a')," +
                        "('0X6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3b')," +
                        "('6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3c')",
                "long256\n" +
                        "0x6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3a\n" +
                        "0x6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3b\n" +
                        "0x6bbf0c833a5448baa23a0366d85079afc390e9837e67ac3f653076982d02dd3c\n",
                true,
                true,
                false
        ));
    }

    @Test
    public void testInsertNullSymbol() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE symbolic_index (s SYMBOL INDEX)", sqlExecutionContext);
            insert("INSERT INTO symbolic_index VALUES ('123456')");
            insert("INSERT INTO symbolic_index VALUES ('1')");
            insert("INSERT INTO symbolic_index VALUES ('')"); // not null
            ddl("CREATE TABLE symbolic_index_other AS (SELECT * FROM symbolic_index)", sqlExecutionContext);

            assertSql("s\n123456\n1\n\n", "symbolic_index_other");
            assertSql("s\n\n", "symbolic_index_other WHERE s = ''");
            assertSql("s\n", "symbolic_index_other WHERE s = NULL");
            assertSql("s\n", "symbolic_index_other WHERE s IS NULL");
            assertSql("s\n123456\n1\n", "symbolic_index_other WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE '' = s");
            assertSql("s\n", "symbolic_index_other WHERE NULL = s");
            assertSql("s\n123456\n1\n", "symbolic_index_other WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE NULL != s");

            insert("INSERT INTO symbolic_index_other VALUES (NULL)"); // null
            assertSql("s\n123456\n1\n\n\n", "symbolic_index_other");
            assertSql("s\n\n", "symbolic_index_other WHERE s = ''");
            assertSql("s\n\n", "symbolic_index_other WHERE s = NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE s IS NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE '' = s");
            assertSql("s\n\n", "symbolic_index_other WHERE NULL = s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE NULL != s");
        });
    }

    @Test
    public void testInsertNullSymbolWithIndex() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE symbolic_index (s SYMBOL INDEX)", sqlExecutionContext);
            insert("INSERT INTO symbolic_index VALUES ('123456')");
            insert("INSERT INTO symbolic_index VALUES ('1')");
            insert("INSERT INTO symbolic_index VALUES ('')"); // not null
            insert("INSERT INTO symbolic_index VALUES (NULL)"); // null

            assertSql("s\n123456\n1\n\n\n", "symbolic_index");
            assertSql("s\n\n", "symbolic_index WHERE s = ''");
            assertSql("s\n\n", "symbolic_index WHERE s = NULL");
            assertSql("s\n\n", "symbolic_index WHERE s IS NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index WHERE '' = s");
            assertSql("s\n\n", "symbolic_index WHERE NULL = s");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE NULL != s");
        });
    }

    @Test
    public void testInsertNullSymbolWithIndexFromAnotherTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE symbolic_index (s SYMBOL INDEX)", sqlExecutionContext);
            insert("INSERT INTO symbolic_index VALUES ('123456')");
            insert("INSERT INTO symbolic_index VALUES ('1')");
            insert("INSERT INTO symbolic_index VALUES ('')"); // not null
            insert("INSERT INTO symbolic_index VALUES (NULL)"); // null
            ddl("CREATE TABLE symbolic_index_other AS (SELECT * FROM symbolic_index)", sqlExecutionContext);

            assertSql("s\n123456\n1\n\n\n", "symbolic_index_other");
            assertSql("s\n\n", "symbolic_index_other WHERE s = ''");
            assertSql("s\n\n", "symbolic_index_other WHERE s = NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE s IS NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE '' = s");
            assertSql("s\n\n", "symbolic_index_other WHERE NULL = s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE NULL != s");
        });
    }

    @Test
    public void testInsertNullSymbolWithoutIndex() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE symbolic_index (s SYMBOL)", sqlExecutionContext);
            insert("INSERT INTO symbolic_index VALUES ('123456')");
            insert("INSERT INTO symbolic_index VALUES ('1')");
            insert("INSERT INTO symbolic_index VALUES ('')"); // not null
            insert("INSERT INTO symbolic_index VALUES (NULL)"); // null

            assertSql("s\n123456\n1\n\n\n", "symbolic_index");
            assertSql("s\n\n", "symbolic_index WHERE s = ''");
            assertSql("s\n\n", "symbolic_index WHERE s = NULL");
            assertSql("s\n\n", "symbolic_index WHERE s IS NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index WHERE '' = s");
            assertSql("s\n\n", "symbolic_index WHERE NULL = s");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index WHERE NULL != s");
        });
    }

    @Test
    public void testInsertNullSymbolWithoutIndexFromAnotherTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE symbolic_index (s SYMBOL)", sqlExecutionContext);
            insert("INSERT INTO symbolic_index VALUES ('123456')");
            insert("INSERT INTO symbolic_index VALUES ('1')");
            insert("INSERT INTO symbolic_index VALUES ('')"); // not null
            insert("INSERT INTO symbolic_index VALUES (NULL)"); // null
            ddl("CREATE TABLE symbolic_index_other AS (SELECT * FROM symbolic_index)", sqlExecutionContext);

            assertSql("s\n123456\n1\n\n\n", "symbolic_index_other");
            assertSql("s\n\n", "symbolic_index_other WHERE s = ''");
            assertSql("s\n\n", "symbolic_index_other WHERE s = NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE s IS NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != ''");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s != NULL");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE s IS NOT NULL");
            assertSql("s\n\n", "symbolic_index_other WHERE '' = s");
            assertSql("s\n\n", "symbolic_index_other WHERE NULL = s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE '' != s");
            assertSql("s\n123456\n1\n\n", "symbolic_index_other WHERE NULL != s");
        });
    }

    @Test
    public void testInsertTimestampAsStr() throws Exception {
        final String expected = "ts\n" +
                "2020-01-10T12:00:01.111143Z\n" +
                "2020-01-10T15:00:01.000143Z\n" +
                "2020-01-10T18:00:01.800000Z\n" +
                "\n";

        assertMemoryLeak(() -> {
            ddl("create table xy (ts timestamp)");
            // execute insert with nanos - we expect the nanos to be truncated
            insert("insert into xy(ts) values ('2020-01-10T12:00:01.111143123Z')");

            // execute insert with micros
            insert("insert into xy(ts) values ('2020-01-10T15:00:01.000143Z')");

            // execute insert with millis
            insert("insert into xy(ts) values ('2020-01-10T18:00:01.800Z')");

            // insert null
            insert("insert into xy(ts) values (null)");

            // test bad format
            try {
                insert("insert into xy(ts) values ('2020-01-10T18:00:01.800Zz')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `2020-01-10T18:00:01.800Zz` [STRING -> TIMESTAMP]");
            }

            assertSql(expected, "xy");
        });
    }

    @Test
    public void testJoinWithDuplicateColumns() throws Exception {
        ddl(
                "CREATE TABLE t1 (" +
                        "  ts TIMESTAMP, " +
                        "  x INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );
        ddl(
                "CREATE TABLE t2 (" +
                        "  ts TIMESTAMP, " +
                        "  x INT" +
                        ") TIMESTAMP(ts) PARTITION BY DAY"
        );
        insert("INSERT INTO t1(ts, x) VALUES (1, 1)");
        insert("INSERT INTO t2(ts, x) VALUES (1, 2)");
        engine.releaseInactive();

        // wildcard aliases are created after all other aliases
        // a duplicate column may be produced while optimiser does not have info on other aliases
        // if this occurs, the column is renamed once we have full alias info for all columns and this error is avoided

        assertSql("TS\tts2\tts1\n" +
                "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\n", "select t2.ts as \"TS\", t1.ts, t1.ts as ts1 from t1 asof join (select * from t2) t2;");

        assertSql("TS\tts1\tts2\tx\tts3\tx1\n" +
                "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\t2\n", "select t2.ts as \"TS\", t2.ts as \"ts1\", * from t1 asof join (select * from t2) t2;");

        assertSql("TS\tts1\tx\tts2\n" +
                "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n", "select t2.ts as \"TS\", t1.*, t2.ts \"ts1\" from t1 asof join (select * from t2) t2;");

        assertSql("TS\tts1\tx\tts2\n" +
                "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n", "select t2.ts as \"TS\", t1.*, t2.ts ts1 from t1 asof join (select * from t2) t2;");
    }

    @Test
    public void testLeftJoinPostMetadata() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab ( created timestamp, value long ) timestamp(created) ");
            compile("insert into tab values (0, 0), (1, 1)");

            String query = "SELECT count(1) FROM " +
                    "( SELECT * " +
                    "  FROM tab " +
                    "  LIMIT 0) as T1 " +
                    "LEFT OUTER JOIN tab as T2 ON T1.created<T2.created " +
                    "LEFT OUTER JOIN tab as T3 ON T2.created=T3.created " +
                    "WHERE T2.created IN (NOW(),NOW()) ";

            assertPlan(
                    query,
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: T3.created=T2.created\n" +
                            "        Filter filter: T2.created in [now(),now()]\n" +
                            "            Nested Loop Left Join\n" +
                            "              filter: T1.created<T2.created\n" +
                            "                Limit lo: 0\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertQuery("count\n" +
                            "0\n",
                    query,
                    null, false, true
            );
        });
    }

    @Test
    public void testLeftJoinReorder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab ( created timestamp, value long ) timestamp(created) ");
            insert("insert into tab values (0, 0), (1, 1), (2,2)");

            String query1 = "SELECT T1.created FROM " +
                    "( SELECT * " +
                    "  FROM tab " +
                    "  LIMIT -1) as T1 " +
                    "LEFT OUTER JOIN tab as T2 ON T1.created<T2.created " +
                    "WHERE T2.created is null or T2.created::long > 0";

            assertPlan(
                    query1,
                    "SelectedRecord\n" +
                            "    Filter filter: (T2.created=null or 0<T2.created::long)\n" +
                            "        Nested Loop Left Join\n" +
                            "          filter: T1.created<T2.created\n" +
                            "            Limit lo: 1\n" +
                            "                DataFrame\n" +
                            "                    Row backward scan\n" +
                            "                    Frame backward scan on: tab\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertQuery("created\n" +
                            "1970-01-01T00:00:00.000002Z\n",
                    query1,
                    "created", false, false
            );

            assertQuery("created\tvalue\tcreated1\tvalue1\n" +
                            "1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000001Z\t1\n" +
                            "1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\t2\n",
                    "SELECT * FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT -1) as T1 " +
                            "LEFT OUTER JOIN tab as T2 ON T1.value::string ~ '[0-9]'  " +
                            "WHERE T2.created is null or T2.created::long > 0",
                    "created", false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t2\n" +
                            "0\t2\tNaN\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created<T3.created " +
                            "WHERE T2.created::long > 0",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t1\n" +
                            "0\t2\t2\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and T2.value - T3.value = 0",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\tNaN\n" +
                            "0\t2\t2\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and T2.value = 2 ",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t1\n" +
                            "0\t2\tNaN\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and T3.value = 1 ",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t1\n" +
                            "0\t2\t2\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and T1.value = 0 ",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t1\n" +
                            "0\t2\t2\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and 1=1 ",
                    null, false, false
            );

            assertQuery("value\tvalue1\tvalue2\n" +
                            "0\t1\t1\n" +
                            "0\t2\t2\n",
                    "SELECT T1.value, T2.value, T3.value FROM " +
                            "( SELECT * " +
                            "  FROM tab " +
                            "  LIMIT 1) as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                            "LEFT JOIN tab as T3 ON T2.created=T3.created and T1.created = T1.created ",
                    null, false, false
            );

            String query3 = "SELECT T1.value, T2.value, T3.value, T4.value " +
                    "FROM (SELECT *  FROM tab limit 2) as T1 " +
                    "LEFT JOIN tab as T2 ON T1.created<T2.created " +
                    "LEFT JOIN (select * from tab limit 3) as T3 ON T2.created=T3.created " +
                    "LEFT JOIN (select * from tab limit 4) as T4 ON T3.created<T4.created " +
                    "WHERE T4.created is null";

            assertPlan(
                    query3,
                    "SelectedRecord\n" +
                            "    Filter filter: T4.created=null\n" +
                            "        Nested Loop Left Join\n" +
                            "          filter: T3.created<T4.created\n" +
                            "            Hash Outer Join Light\n" +
                            "              condition: T3.created=T2.created\n" +
                            "                Nested Loop Left Join\n" +
                            "                  filter: T1.created<T2.created\n" +
                            "                    Limit lo: 2\n" +
                            "                        DataFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: tab\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab\n" +
                            "                Hash\n" +
                            "                    Limit lo: 3\n" +
                            "                        DataFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: tab\n" +
                            "            Limit lo: 4\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n"
            );

            assertQuery("value\tvalue1\tvalue2\tvalue3\n" +
                            "0\t2\t2\tNaN\n" +
                            "1\t2\t2\tNaN\n",
                    query3, null, false, false
            );
        });
    }

    @Test
    public void testNonEqualityJoinCondition() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab ( created timestamp, value long ) timestamp(created) ");
            compile("insert into tab values (0, 0), (1, 1)");

            assertQuery("count\n" +
                            "1\n",
                    "SELECT " +
                            "  count(*) " +
                            "FROM " +
                            "  tab as T1 " +
                            "  JOIN tab as T2 ON T1.created < T2.created " +
                            "  JOIN tab as T3 ON T2.created = T3.created",
                    null, false, true
            );

            assertQuery("count\n" +
                            "1\n",
                    "SELECT " +
                            "  count(*) " +
                            "FROM " +
                            "  tab as T1 " +
                            "  JOIN tab as T2 ON T1.created < T2.created " +
                            "  JOIN tab as T3 ON T2.value = T3.value",
                    null, false, true
            );
        });
    }

    @Test
    public void testOrderByDouble() throws Exception {
        assertQuery("d\n" +
                        "NaN\n" +
                        "5.0\n" +
                        "4.0\n" +
                        "3.0\n" +
                        "2.0\n" +
                        "1.0\n",
                "select * from x order by d desc",
                "create table x as (select (6-x)::double d from long_sequence(5) union all select null)",
                null, true, true
        );
    }

    @Test
    public void testOrderByEmptyIdentifier() throws Exception {
        assertMemoryLeak(() -> {
            assertFailure(40, "non-empty literal or expression expected", "select 1 from long_sequence(1) order by ''");
            assertFailure(40, "non-empty literal or expression expected", "select 1 from long_sequence(1) order by \"\"");
        });
    }

    @Test
    public void testOrderByFloat() throws Exception {
        assertQuery("f\n" +
                        "NaN\n" +
                        "5.0\n" +
                        "4.0\n" +
                        "3.0\n" +
                        "2.0\n" +
                        "1.0\n",
                "select * from x order by f desc",
                "create table x as (select (6-x)::float f from long_sequence(5) union all select null::float)",
                null, true, true
        );
    }

    @Test
    public void testOrderGroupByTokensCanBeQuoted1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE trigonometry AS " +
                    "(SELECT" +
                    "     rnd_int(-180, 180, 1) * 1.0 angle_rad," +
                    "     rnd_symbol('A', 'B', 'C') sym," +
                    "     rnd_double() sine" +
                    " FROM long_sequence(1000)" +
                    ")", sqlExecutionContext);
            assertQuery(
                    "sym\tavg_angle_rad\tSUM(sine)\n" +
                            "A\t-1.95703125\t168.46508050039918\n" +
                            "B\t11.255060728744938\t183.76121842808922\n" +
                            "C\t-0.888030888030888\t164.8875613340687\n",
                    "SELECT" +
                            "    sym AS 'sym'," +
                            "    avg(angle_rad) AS avg_angle_rad," +
                            "    sum(sine) AS 'SUM(sine)' " +
                            "FROM trigonometry " +
                            "GROUP BY sym " +
                            "ORDER BY 'prefix' || sym, \"SUM(sine)\" DESC " +
                            "LIMIT 1000",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderGroupByTokensCanBeQuoted2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE trigonometry AS " +
                    "(SELECT" +
                    "     rnd_int(-180, 180, 1) * 1.0 angle_rad," +
                    "     rnd_symbol('A', 'B', 'C') sym," +
                    "     rnd_double() sine" +
                    " FROM long_sequence(1000)" +
                    ")", sqlExecutionContext);
            assertQuery(
                    "sym\tavg_angle_rad\tSUM(sine)\n" +
                            "A\t-1.95703125\t168.46508050039918\n" +
                            "B\t11.255060728744938\t183.76121842808922\n" +
                            "C\t-0.888030888030888\t164.8875613340687\n",
                    "SELECT" +
                            "    sym AS 'sym'," +
                            "    avg(angle_rad) AS avg_angle_rad," +
                            "    sum(sine) AS \"SUM(sine)\" " +
                            "FROM trigonometry " +
                            "GROUP BY sym " +
                            "ORDER BY 'prefix' || sym, \"SUM(sine)\" DESC " +
                            "LIMIT 1000",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testRaceToCreateEmptyTable() throws InterruptedException {
        AtomicInteger index = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();

        for (int i = 0; i < 50; i++) {
            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch haltLatch = new CountDownLatch(2);

            index.set(-1);
            success.set(0);

            LOG.info().$("create race [i=").$(i).$(']').$();

            new Thread(() -> {
                try {
                    barrier.await();
                    ddl("create table x (a INT, b FLOAT)");
                    index.set(0);
                    success.incrementAndGet();
                } catch (Exception ignore) {
//                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();
                    ddl("create table x (a STRING, b DOUBLE)");
                    index.set(1);
                    success.incrementAndGet();
                } catch (Exception ignore) {
//                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();

            Assert.assertTrue(haltLatch.await(30, TimeUnit.SECONDS));

            Assert.assertEquals(1, success.get());
            Assert.assertNotEquals(-1, index.get());

            TableToken tt;
            try (TableReader reader = getReader("x")) {
                tt = reader.getTableToken();
                sink.clear();
                reader.getMetadata().toJson(sink);
                if (index.get() == 0) {
                    TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"FLOAT\"}],\"timestampIndex\":-1}", sink);
                } else {
                    TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"STRING\"},{\"index\":1,\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestampIndex\":-1}", sink);
                }
            }
            engine.drop(path, tt);
        }
    }

    @Test
    public void testRebuildIndex() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table rebuild_index as (select rnd_symbol('1', '2', '33', '44') sym, x from long_sequence(15)), index(sym)");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            compile("reindex table rebuild_index column sym lock exclusive");
            assertSql("sym\tx\n" +
                    "1\t1\n" +
                    "1\t10\n" +
                    "1\t11\n" +
                    "1\t12\n", "select * from rebuild_index where sym = '1'");
        });
    }

    @Test
    public void testRebuildIndexInPartition() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table rebuild_index as (" +
                    "select rnd_symbol('1', '2', '33', '44') sym, x, timestamp_sequence(0, 12*60*60*1000000L) ts " +
                    "from long_sequence(15)" +
                    "), index(sym) timestamp(ts)");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            compile("reindex table rebuild_index column sym partition '1970-01-02' lock exclusive");
            assertSql("sym\tx\tts\n" +
                    "1\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "1\t10\t1970-01-05T12:00:00.000000Z\n" +
                    "1\t11\t1970-01-06T00:00:00.000000Z\n" +
                    "1\t12\t1970-01-06T12:00:00.000000Z\n", "select * from rebuild_index where sym = '1'"
            );
        });
    }

    @Test
    public void testRebuildIndexWritersLock() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table rebuild_index as (select rnd_symbol('1', '2', '33', '44') sym, x from long_sequence(15)), index(sym)");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            try (TableWriter ignore = getWriter("rebuild_index")) {
                compile("reindex table rebuild_index column sym lock exclusive");
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Cannot lock table");
            }
        });
    }

    @Test
    public void testReindexSyntaxCheckSemicolon() throws Exception {
        assertMemoryLeak(() -> {
                    compile(
                            "create table xxx as (" +
                                    "select " +
                                    "rnd_symbol('A', 'B', 'C') as sym1," +
                                    "rnd_symbol(4,4,4,2) as sym2," +
                                    "x," +
                                    "timestamp_sequence(0, 100000000) ts " +
                                    "from long_sequence(10000)" +
                                    "), index(sym1), index(sym2)");

                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    compile("REINDEX TABLE \"xxx\" Lock exclusive;");
                }
        );
    }

    @Test
    public void testReindexSyntaxError() throws Exception {
        assertException(
                "REINDEX TABLE xxx",
                "create table xxx as (" +
                        "select " +
                        "rnd_symbol('A', 'B', 'C') as sym1," +
                        "rnd_symbol(4,4,4,2) as sym2," +
                        "x," +
                        "timestamp_sequence(0, 100000000) ts " +
                        "from long_sequence(10000)" +
                        "), index(sym1), index(sym2)",
                "REINDEX TABLE xxx".length(),
                "LOCK EXCLUSIVE expected"
        );

        assertException(
                "REINDEX TABLE xxx COLUMN sym2",
                "REINDEX TABLE xxx COLUMN sym2".length(),
                "LOCK EXCLUSIVE expected"
        );

        assertException(
                "REINDEX TABLE xxx LOCK",
                "REINDEX TABLE xxx LOCK".length(),
                "LOCK EXCLUSIVE expected"
        );

        assertException(
                "REINDEX TABLE xxx PARTITION '1234''",
                "REINDEX TABLE xxx PARTITION '1234''".length(),
                "LOCK EXCLUSIVE expected"
        );

        assertException(
                "REINDEX xxx PARTITION '1234''",
                "REINDEX ".length(),
                "TABLE expected"
        );

        assertException(
                "REINDEX TABLE ",
                "REINDEX TABLE ".length(),
                "table name expected"
        );

        assertException(
                "REINDEX TABLE xxx COLUMN \"sym1\" lock exclusive twice",
                "REINDEX TABLE xxx COLUMN \"sym1\" lock exclusive twice".length(),
                "EOF expecte"
        );
    }

    @Test
    public void testRemoveColumnShiftTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x1 (a int, b double, t timestamp) timestamp(t)");

            try (TableReader reader = getReader("x1")) {
                Assert.assertEquals(2, reader.getMetadata().getTimestampIndex());

                try (TableWriter writer = getWriter("x1")) {
                    Assert.assertEquals(2, writer.getMetadata().getTimestampIndex());
                    writer.removeColumn("b");
                    Assert.assertEquals(2, writer.getMetadata().getTimestampIndex()); // Writer timestamp index doesn't change
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(1, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    @Test
    public void testRemoveTimestampAndReplace() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x1 (a int, b double, t timestamp) timestamp(t)");

            try (TableReader reader = getReader("x1")) {
                Assert.assertEquals(2, reader.getMetadata().getTimestampIndex());

                try (TableWriter writer = getWriter("x1")) {
                    Assert.assertEquals(2, writer.getMetadata().getTimestampIndex());
                    writer.removeColumn("t");
                    Assert.assertEquals(-1, writer.getMetadata().getTimestampIndex());
                    writer.addColumn("t", ColumnType.TIMESTAMP);
                    Assert.assertEquals(-1, writer.getMetadata().getTimestampIndex());
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(-1, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    @Test
    public void testRemoveTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x1 (a int, b double, t timestamp) timestamp(t)");

            try (TableReader reader = getReader("x1")) {
                Assert.assertEquals(2, reader.getMetadata().getTimestampIndex());

                try (TableWriter writer = getWriter("x1")) {
                    Assert.assertEquals(2, writer.getMetadata().getTimestampIndex());
                    writer.removeColumn("t");
                    Assert.assertEquals(-1, writer.getMetadata().getTimestampIndex());
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(-1, reader.getMetadata().getTimestampIndex());
            }
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table table_old_name (a int, b double, t timestamp) timestamp(t)");
            ddl("rename table table_old_name to table_new_name");
            try (TableReader reader = getReader("table_new_name")) {
                Assert.assertEquals(2, reader.getMetadata().getTimestampIndex());
                try (TableWriter writer = getWriter("table_new_name")) {
                    Assert.assertEquals(2, writer.getMetadata().getTimestampIndex());
                }
            }
        });
    }

    @Test
    public void testRndSymbolEmptyArgs() throws Exception {
        assertFailure(7, "function rnd_symbol expects arguments but has none",
                "select rnd_symbol() from long_sequence(1)"
        );
    }

    @Test
    public void testSelectCharInListContainingNull() throws Exception {
        assertQuery("c\n1\n\n",
                "select * from xCHAR where c in ('1', null)",
                "create table xCHAR as (select '1'::char c union all select null::char );",
                null, true, false
        );
    }

    @Test
    public void testSelectCharInNull() throws Exception {
        assertQuery("c\n\n",
                "select * from xCHAR where c in null",
                "create table xCHAR as (select '1'::char c union all select null::char )",
                null, true, false
        );
    }

    @Test
    public void testSelectDateInListContainingNull() throws Exception {
        assertQuery("c\n1970-01-01T00:00:00.001Z\n\n",
                "select * from x where c in (cast(1 as date), cast(null as date))",
                "create table x as (select cast(1 as date) c union all select null::date )",
                null, true, false
        );
    }

    @Test
    public void testSelectDateInNullString() throws Exception {
        assertQuery("c\n\n",
                "select * from x where c in null::string",
                "create table x as (select cast(1 as date) c union all select null::date )",
                null, true, false
        );
    }

    @Test
    public void testSelectDoubleInListContainingNull() throws Exception {
        assertQuery("c\n1.0\nNaN\n",
                "select * from x where c in (1.0, null, 1::byte, 1::short, 1::int, 1::long)",
                "create table x as (select 1d c union all select null::double )",
                null, true, false
        );
    }

    @Test
    public void testSelectDoubleInListWithBindVariable() throws Exception {
        ddl("create table x as (select 1D c union all select null::double )");

        bindVariableService.clear();
        bindVariableService.setStr("val", "1");
        selectDoubleInListWithBindVariable();

        bindVariableService.setLong("val", 1L);
        selectDoubleInListWithBindVariable();

        bindVariableService.setInt("val", 1);
        selectDoubleInListWithBindVariable();

        bindVariableService.setShort("val", (short) 1);
        selectDoubleInListWithBindVariable();

        bindVariableService.setByte("val", (byte) 1);
        selectDoubleInListWithBindVariable();

        bindVariableService.setFloat("val", 1f);
        selectDoubleInListWithBindVariable();

        bindVariableService.setDouble("val", 1d);
        selectDoubleInListWithBindVariable();
    }

    @Test
    public void testSelectDoubleInNull() throws Exception {
        assertQuery("c\nNaN\n",
                "select * from x where c in null",
                "create table x as (select 1.0 c union all select null::double )",
                null, true, false
        );
    }

    @Test
    public void testSelectDoubleNotInListContainingNull() throws Exception {
        assertQuery("c\n1.0\n",
                "select * from x where c not in (2.0,null)",
                "create table x as (select 1.0d c union all select null::double )",
                null, true, false
        );
    }

    @Test
    public void testSelectFloatInListContainingNull() throws Exception {
        assertQuery("c\n1.0\nNaN\n",
                "select * from x where c in (1.0f, null, 1::byte, 1::short, 1::int, 1::long)",
                "create table x as (select 1f c union all select null::float )",
                null, true, false
        );
    }

    @Test
    public void testSelectFloatInNull() throws Exception {
        assertQuery("c\nNaN\n",
                "select * from x where c in null",
                "create table x as (select 1.0f c union all select null::float )",
                null, true, false
        );
    }

    @Test
    public void testSelectFloatNotInListContainingNull() throws Exception {
        assertQuery("c\n1.0\n",
                "select * from x where c not in (2.0f,null)",
                "create table x as (select 1.0f c union all select null::float )",
                null, true, false
        );
    }

    @Test
    public void testSelectInListWithBadCastClosesFactories() throws Exception {
        String query = "select * from ( select x, '1' from long_sequence(1000000) order by 2 desc limit 999999 ) #SETOP#  " +
                "select * from ( select x, '2' from long_sequence(1000000) order by 2 desc limit 999999 ) #SETOP# " +
                "select * from ( select x, x::float from long_sequence(1000000) order by 2 desc limit 999999 ) ";

        assertFailure(96, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "UNION"));
        assertFailure(99, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "UNION ALL"));
        assertFailure(97, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "EXCEPT"));
        assertFailure(100, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "EXCEPT ALL"));
        assertFailure(100, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "INTERSECT"));
        assertFailure(103, "unsupported cast [column=1, from=CHAR, to=DOUBLE]", query.replace("#SETOP#", "INTERSECT ALL"));
    }

    @Test
    public void testSelectIntInListContainingNull() throws Exception {
        assertQuery("c\n1\nNaN\n",
                "select * from x where c in (1,null)",
                "create table x as (select 1 c union all select null::int )",
                null, true, false
        );
    }

    @Test
    public void testSelectIntInNull() throws Exception {
        assertQuery("c\nNaN\n",
                "select * from x where c in null",
                "create table x as (select 1 c union all select null::int )",
                null, true, false
        );
    }

    @Test
    public void testSelectIntNotInListContainingNull() throws Exception {
        assertQuery("c\n1\n",
                "select * from x where c not in (2,null)",
                "create table x as (select 1 c union all select null::int )",
                null, true, false
        );
    }

    @Test
    public void testSelectInvalidGeoHashLiteralBits() throws Exception {
        assertException(
                "select ##k from long_sequence(10)",
                7,
                "invalid constant: ##k"
        );
    }

    @Test
    public void testSelectLongInListContainingNull() throws Exception {
        assertQuery("c\n1\nNaN\n",
                "select * from x where c in (1,null, 1::byte, 1::short, 1::int, 1::long)",
                "create table x as (select 1L c union all select null::long )",
                null, true, false
        );
    }

    @Test
    public void testSelectLongInListWithBindVariable() throws Exception {
        ddl("create table x as (select 1L c union all select null::long )");

        bindVariableService.clear();
        bindVariableService.setStr("val", "1");
        selectLongInListWithBindVariable();

        bindVariableService.setLong("val", 1L);
        selectLongInListWithBindVariable();

        bindVariableService.setInt("val", 1);
        selectLongInListWithBindVariable();

        bindVariableService.setShort("val", (short) 1);
        selectLongInListWithBindVariable();

        bindVariableService.setByte("val", (byte) 1);
        selectLongInListWithBindVariable();
    }

    @Test
    public void testSelectLongInNull() throws Exception {
        assertQuery("c\nNaN\n",
                "select * from x where c in null",
                "create table x as (select 1L c union all select null::long )",
                null, true, false
        );
    }

    @Test
    public void testSelectLongNotInListContainingNull() throws Exception {
        assertQuery("c\n1\n",
                "select * from x where c not in (2,null)",
                "create table x as (select 1L c union all select null::long )",
                null, true, false
        );
    }

    @Test
    public void testSelectStrInListContainingNull() throws Exception {
        assertQuery(
                "l\tstr\n" +
                        "2\t2\n" +
                        "3\t\n",
                "select * from x where str in (null, '2', '2'::symbol)",
                "create table x as " +
                        "(" +
                        "select 1L as l, '1' as str  union all " +
                        "select 2L, '2' union all " +
                        "select 3L,  null " +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testSelectStrInNull() throws Exception {
        assertQuery(
                "l\tstr\n" +
                        "3\t\n",
                "select * from x where str in null",
                "create table x as " +
                        "(" +
                        "select 1L as l, '1' as str  union all " +
                        "select 2L, '2' union all " +
                        "select 3L, null " +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testSelectTimestampInListContainingNull() throws Exception {
        assertQuery("c\n1970-01-01T00:00:00.000001Z\n\n",
                "select * from x where c in (1,null)",
                "create table x as (select 1::timestamp c union all select null::timestamp )",
                null, true, false
        );
    }

    @Test
    public void testSelectTimestampInNull() throws Exception {
        assertQuery("c\n\n",
                "select * from x where c in null",
                "create table x as (select 1::timestamp c union all select null::timestamp )",
                null, true, false
        );
    }

    @Test
    public void testSelectTimestampInNullString() throws Exception {
        assertQuery("c\n\n",
                "select * from x where c in null::string",
                "create table x as (select 1::timestamp c union all select null::timestamp )",
                null, true, false
        );
    }

    @Test
    public void testSelectWithEmptySubSelectInWhereClause() throws Exception {
        assertException("select 1 from tab where (\"\")",
                "create table tab (i int)",
                25, "Invalid column"
        );

        assertException("select 1 from tab where (\"a\")",
                25, "Invalid column"
        );

        assertException("select 1 from tab where ('')",
                25, "boolean expression expected"
        );

        assertException("select 1 from tab where ('a')",
                25, "boolean expression expected"
        );
    }

    @Test
    public void testSymbolToStringAutoCast() throws Exception {
        final String expected = "cc\tk\n" +
                "PEHN_\t1970-01-01T00:00:00.000000Z\n" +
                "CPSW_ffyu\t1970-01-01T00:00:00.010000Z\n" +
                "VTJW_gpgw\t1970-01-01T00:00:00.020000Z\n" +
                "_\t1970-01-01T00:00:00.030000Z\n" +
                "VTJW_ffyu\t1970-01-01T00:00:00.040000Z\n";

        assertQuery(
                expected,
                "select concat(a, '_', to_lowercase(b)) cc, k from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testSymbolToStringAutoCastJoin() throws Exception {
        assertMemoryLeak(() -> {
            final String xx = "create table xx as " +
                    "(" +
                    "select" +
                    " rnd_str('IBM', 'APPL', 'SPY', 'FB') a," +
                    " timestamp_sequence(0, 10000) k" +
                    " from" +
                    " long_sequence(5)" +
                    ") timestamp(k)";

            final String yy = "create table yy as " +
                    "(" +
                    "select" +
                    " rnd_symbol('IBM', 'APPL', 'SPY') b," +
                    " timestamp_sequence(0, 10000) k" +
                    " from" +
                    " long_sequence(5)" +
                    ") timestamp(k)";

            ddl(xx);
            ddl(yy);

            final String expected = "a\tb\tc\n" +
                    "IBM\tIBM\tIBM_IBM\n" +
                    "SPY\tSPY\tSPY_SPY\n" +
                    "SPY\tSPY\tSPY_SPY\n" +
                    "APPL\tAPPL\tAPPL_APPL\n" +
                    "APPL\tAPPL\tAPPL_APPL\n" +
                    "APPL\tAPPL\tAPPL_APPL\n" +
                    "APPL\tAPPL\tAPPL_APPL\n";
            assertQuery(expected, "select xx.a, yy.b, concat(xx.a, '_', yy.b) c from xx join yy on xx.a = yy.b", null, false, true);
        });
    }

    @Test
    public void testSymbolToStringAutoCastWhere() throws Exception {
        final String expected = "a\tb\tk\n" +
                "IBM\tIBM\t1970-01-01T00:00:00.000000Z\n";
        assertQuery(
                expected,
                "select a, b, k from x where a=b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_str('IBM', 'APPL', 'SPY', 'FB') a," +
                        " rnd_symbol('IBM', 'APPL', 'SPY') b," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "k",
                true,
                false
        );
    }

    @Test
    public void testTimestampWithNanosInWhereClause() throws Exception {
        assertQuery("x\tts\n" +
                        "2\t2019-10-17T00:00:00.200000Z\n" +
                        "3\t2019-10-17T00:00:00.700000Z\n" +
                        "4\t2019-10-17T00:00:00.800000Z\n",
                "select * from x where ts between '2019-10-17T00:00:00.200000123Z' and '2019-10-17T00:00:00.800000123Z'",
                "create table x as " +
                        "(SELECT x, timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), rnd_short(1,5) * 100000L) as ts FROM long_sequence(5)" +
                        ")",
                null, true, false
        );
    }

    @Test
    public void testUseExtensionPoints() {
        try (SqlCompilerWrapper compiler = new SqlCompilerWrapper(engine)) {

            try {
                compiler.compile("alter altar", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(compiler.unknownAlterStatementCalled);
            }

            try {
                compiler.compile("show something", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(compiler.unknownShowStatementCalled);
            }

            try {
                compiler.compile("drop table ka boom zoom", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(compiler.unknownDropTableSuffixCalled);
            }

            try {
                compiler.compile("drop something", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(compiler.unknownDropStatementCalled);
            }

            try {
                compiler.compile("create table tab ( i int)", sqlExecutionContext);
                compiler.compile("alter table tab drop column i boom zoom", sqlExecutionContext);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(compiler.unknownDropColumnSuffixCalled);
            }
        }
    }

    private void assertCast(String expectedData, String expectedMeta, String ddl) throws SqlException {
        ddl(ddl);
        try (TableReader reader = getReader("y")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(expectedMeta, sink);
            TestUtils.assertReader(expectedData, reader, sink);
        }
    }

    private void assertCastByte(String expectedData, int castTo) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_byte(33, 119) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastByteFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_byte(2,50) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                94,
                "unsupported cast"
        );
    }

    private void assertCastDate(String expectedData, int castTo) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastDouble(String expectedData, int castTo) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select 100 * rnd_double(2) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastDoubleFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_double(2) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                93,
                "unsupported cast"
        );
    }

    private void assertCastFloat(String expectedData, int castTo) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select 100 * rnd_float(2) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastFloatFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_float(2) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                92,
                "unsupported cast"
        );
    }

    private void assertCastInt(String expectedData, int castTo) throws SqlException {
        assertCastInt(expectedData, castTo, 2);
    }

    private void assertCastInt(String expectedData, int castTo, int nanRate) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_int(0, 30, " + nanRate + ") a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastIntFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_int(0, 30, 2) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                97,
                "unsupported cast"
        );
    }

    private void assertCastLong(String expectedData, int castTo) throws SqlException {
        assertCastLong(expectedData, castTo, 2);
    }

    private void assertCastLong(String expectedData, int castTo, int nanRate) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_long(0, 30, " + nanRate + ") a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastLongFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_long(0, 30, 2) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                98,
                "unsupported cast"
        );
    }

    private void assertCastShort(String expectedData, int castTo) throws SqlException {
        assertCastShort(expectedData, castTo, 1024, 2048);
    }

    private void assertCastShort(String expectedData, int castTo, int min, int max) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_short(" + min + ", " + max + ") a from long_sequence(20))), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastShortFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_short(2,10) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                95,
                "unsupported cast"
        );
    }

    private void assertCastStringFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select * from (select rnd_str(5,10,2) a from long_sequence(20))" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                95,
                "unsupported cast"
        );
    }

    private void assertCastSymbolFail(int castTo) throws Exception {
        assertException(
                "create table y as (" +
                        "select rnd_symbol(4,6,10,2) a from long_sequence(20)" +
                        "), cast(a as " + ColumnType.nameOf(castTo) + ")",
                84,
                "unsupported cast"
        );
    }

    private void assertCastTimestamp(String expectedData, int castTo) throws SqlException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from (select rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) a from long_sequence(20))" +
                "), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCreateTableAsSelect(CharSequence expectedMetadata, CharSequence sql, Fiddler fiddler) throws SqlException {
        // create source table
        ddl("create table X (a int, b int, t timestamp) timestamp(t)");
        engine.releaseAllWriters();

        try (CairoEngine engine = new CairoEngine(configuration) {
            @Override
            public TableReader getReader(TableToken tableToken, long metadataVersion) {
                fiddler.run(this);
                return super.getReader(tableToken, metadataVersion);
            }
        }) {
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                compiler.compile(sql, sqlExecutionContext);
                Assert.assertTrue(fiddler.isHappy());
                try (TableReader reader = engine.getReader("Y")) {
                    sink.clear();
                    reader.getMetadata().toJson(sink);
                    TestUtils.assertEquals(expectedMetadata, sink);
                }

                Assert.assertEquals(0, engine.getBusyReaderCount());
            }
        }
    }

    private void assertFailure(FilesFacade ff, CharSequence sql, CharSequence message) throws Exception {
        assertMemoryLeak(
                ff,
                () -> {
                    try {
                        assertException(sql);
                    } catch (SqlException e) {
                        Assert.assertEquals(13, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), message);
                    }
                }
        );
    }

    private void assertFailure0(int position, CharSequence expectedMessage, CharSequence sql, Class<?> exception) {
        try {
            assertException(sql);
        } catch (Throwable e) {
            Assert.assertSame(exception, e.getClass());
            if (e instanceof FlyweightMessageContainer) {
                TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), expectedMessage);
                if (position != -1) {
                    Assert.assertSame(SqlException.class, e.getClass());
                    Assert.assertEquals(position, ((FlyweightMessageContainer) e).getPosition());
                }
            } else {
                Assert.fail();
            }
        }
    }

    private void assertInsertAsSelectIOError(AtomicBoolean inError, FilesFacade ff) throws Exception {
        assertMemoryLeak(
                ff,
                () -> {
                    ddl("create table x (a INT, b INT)");
                    try {
                        insert("insert into x select rnd_int() int1, rnd_int() int2 from long_sequence(1000000)");
                        Assert.fail();
                    } catch (CairoException ignore) {
                    }

                    inError.set(false);

                    try (TableWriter w = getWriter("x")) {
                        Assert.assertEquals(0, w.size());
                    }

                    insert("insert into x select rnd_int() int1, rnd_int() int2 from long_sequence(1000000)");
                    try (TableWriter w = getWriter("x")) {
                        Assert.assertEquals(1000000, w.size());
                    }

                    try (TableReader reader = getReader("x")) {
                        Assert.assertEquals(1000000, reader.size());
                    }
                }
        );
    }

    private void selectDoubleInListWithBindVariable() throws Exception {
        assertQuery("c\n1.0\n",
                "select * from x where c in (:val)",
                null, true, false
        );

        bindVariableService.clear();
    }

    private void selectLongInListWithBindVariable() throws Exception {
        assertQuery("c\n1\n",
                "select * from x where c in (:val)",
                null, true, false
        );

        bindVariableService.clear();
    }

    private void testGeoHashWithBits(String columnSize, String geoHash, String expected) throws Exception {
        assertMemoryLeak(() -> {
            assertQuery(
                    "geohash\n",
                    "select geohash from geohash",
                    String.format("create table geohash (geohash geohash(%s))", columnSize),
                    null,
                    true,
                    true
            );
            insert(String.format("insert into geohash values(%s)", geoHash));
            assertSql(expected, "geohash");
        });
    }

    private void testInsertAsSelect(CharSequence expectedData, CharSequence ddl, CharSequence insert, CharSequence select) throws Exception {
        assertMemoryLeak(() -> {
            ddl(ddl);
            insert(insert);
            assertSql(expectedData, select);
        });
    }

    protected void assertFailure(int position, CharSequence expectedMessage, CharSequence sql) throws Exception {
        assertMemoryLeak(() -> assertFailure0(position, expectedMessage, sql, SqlException.class));
    }

    private interface Fiddler {
        boolean isHappy();

        void run(CairoEngine engine);
    }

    static class SqlCompilerWrapper extends SqlCompilerImpl {
        boolean unknownAlterStatementCalled;
        boolean unknownDropColumnSuffixCalled;
        boolean unknownDropStatementCalled;
        boolean unknownDropTableSuffixCalled;
        boolean unknownShowStatementCalled;

        SqlCompilerWrapper(CairoEngine engine) {
            super(engine);
        }

        @Override
        protected void unknownAlterStatement(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
            unknownAlterStatementCalled = true;
            super.unknownAlterStatement(executionContext, tok);
        }

        @Override
        protected void unknownDropColumnSuffix(SecurityContext securityContext, CharSequence tok, TableToken tableToken, AlterOperationBuilder dropColumnStatement) throws SqlException {
            unknownDropColumnSuffixCalled = true;
            super.unknownDropColumnSuffix(securityContext, tok, tableToken, dropColumnStatement);
        }

        @Override
        protected void unknownDropStatement(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
            unknownDropStatementCalled = true;
            super.unknownDropStatement(executionContext, tok);
        }

        @Override
        protected void unknownDropTableSuffix(SqlExecutionContext executionContext, CharSequence tok, CharSequence tableName, int tableNamePosition, boolean hasIfExists) throws SqlException {
            unknownDropTableSuffixCalled = true;
            super.unknownDropTableSuffix(executionContext, tok, tableName, tableNamePosition, hasIfExists);
        }

        @Override
        protected RecordCursorFactory unknownShowStatement(SqlExecutionContext executionContext, CharSequence tok) throws SqlException {
            unknownShowStatementCalled = true;
            return super.unknownShowStatement(executionContext, tok);
        }
    }

}
