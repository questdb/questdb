/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.*;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlCompilerTest extends AbstractCairoTest {
    private final static Engine engine = new Engine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();

    @Test
    public void assertCastString() throws SqlException, IOException {
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
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_str(5,10,2)" +
                ")), cast(a as SYMBOL)";

        assertCast(expectedData, expectedMeta, sql);
    }

    @Before
    public void setUp2() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @After
    public void tearDown() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
    }

    @Test
    public void testCannotCreateTable() {

        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public int mkdir(LPSZ path, int mode) {
                return -1;
            }
        };

        CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        CairoEngine engine = new Engine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine, configuration);

        try {
            compiler.compile("create table x (a int)", bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(13, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Cannot create table");
        }
    }

    @Test
    public void testCastByteDate() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.DATE);
    }

    @Test
    public void testCastByteDouble() throws SqlException, IOException {
        assertCastByte("a\n" +
                        "119.000000000000\n" +
                        "52.000000000000\n" +
                        "91.000000000000\n" +
                        "97.000000000000\n" +
                        "119.000000000000\n" +
                        "107.000000000000\n" +
                        "39.000000000000\n" +
                        "81.000000000000\n" +
                        "46.000000000000\n" +
                        "41.000000000000\n" +
                        "61.000000000000\n" +
                        "82.000000000000\n" +
                        "75.000000000000\n" +
                        "95.000000000000\n" +
                        "87.000000000000\n" +
                        "116.000000000000\n" +
                        "87.000000000000\n" +
                        "40.000000000000\n" +
                        "116.000000000000\n" +
                        "117.000000000000\n",
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastByteFloat() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastByteInt() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.INT);
    }

    @Test
    public void testCastByteLong() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastByteShort() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.SHORT);
    }

    @Test
    public void testCastByteTimestamp() throws SqlException, IOException {
        assertCastByte("a\n" +
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
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastDateByte() throws SqlException, IOException {
        assertCastDate("a\n" +
                        "11\n" +
                        "0\n" +
                        "121\n" +
                        "-2\n" +
                        "0\n" +
                        "-43\n" +
                        "-124\n" +
                        "100\n" +
                        "0\n" +
                        "124\n" +
                        "0\n" +
                        "-45\n" +
                        "0\n" +
                        "24\n" +
                        "-16\n" +
                        "58\n" +
                        "0\n" +
                        "-6\n" +
                        "73\n" +
                        "125\n",
                ColumnType.BYTE);
    }

    @Test
    public void testCastDateDouble() throws SqlException, IOException {
        assertCastDate("a\n" +
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
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastDateFloat() throws SqlException, IOException {
        assertCastDate("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastDateInt() throws SqlException, IOException {
        assertCastDate("a\n" +
                        "368100107\n" +
                        "0\n" +
                        "-1322920583\n" +
                        "315036158\n" +
                        "0\n" +
                        "925824213\n" +
                        "848878212\n" +
                        "1466216804\n" +
                        "0\n" +
                        "74798204\n" +
                        "0\n" +
                        "779467987\n" +
                        "0\n" +
                        "-222350568\n" +
                        "-747511056\n" +
                        "-2058822342\n" +
                        "0\n" +
                        "480456698\n" +
                        "2102580553\n" +
                        "637210493\n",
                ColumnType.INT);
    }

    @Test
    public void testCastDateLong() throws SqlException, IOException {
        assertCastDate("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastDateShort() throws SqlException, IOException {
        assertCastDate("a\n" +
                        "-15605\n" +
                        "0\n" +
                        "-10887\n" +
                        "4606\n" +
                        "0\n" +
                        "-2859\n" +
                        "-9596\n" +
                        "-20124\n" +
                        "0\n" +
                        "21628\n" +
                        "0\n" +
                        "-17197\n" +
                        "0\n" +
                        "13080\n" +
                        "-7440\n" +
                        "-8902\n" +
                        "0\n" +
                        "12282\n" +
                        "-10935\n" +
                        "3965\n",
                ColumnType.SHORT);
    }

    @Test
    public void testCastDateTimestamp() throws SqlException, IOException {
        assertCastDate("a\n" +
                        "1970-01-17T12:11:37.242379Z\n" +
                        "\n" +
                        "1970-01-17T17:41:21.058169Z\n" +
                        "1970-01-17T14:33:54.113022Z\n" +
                        "\n" +
                        "1970-01-17T15:55:39.868373Z\n" +
                        "1970-01-17T17:05:57.889668Z\n" +
                        "1970-01-17T16:04:40.260964Z\n" +
                        "\n" +
                        "1970-01-17T16:53:03.809660Z\n" +
                        "\n" +
                        "1970-01-17T14:41:38.544851Z\n" +
                        "\n" +
                        "1970-01-17T17:59:41.628184Z\n" +
                        "1970-01-17T16:39:21.500400Z\n" +
                        "1970-01-17T12:42:45.287226Z\n" +
                        "\n" +
                        "1970-01-17T14:36:39.533562Z\n" +
                        "1970-01-17T11:28:56.755529Z\n" +
                        "1970-01-17T12:16:06.352765Z\n",
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastDoubleByte() throws SqlException, IOException {
        assertCastDouble("a\n" +
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
                ColumnType.BYTE);
    }

    @Test
    public void testCastDoubleDate() throws SqlException, IOException {
        assertCastDouble("a\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.065Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.042Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.042Z\n" +
                        "1970-01-01T00:00:00.070Z\n" +
                        "1970-01-01T00:00:00.038Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.032Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.097Z\n" +
                        "1970-01-01T00:00:00.024Z\n" +
                        "1970-01-01T00:00:00.063Z\n",
                ColumnType.DATE);
    }

    @Test
    public void testCastDoubleFloat() throws SqlException, IOException {
        assertCastDouble("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastDoubleInt() throws SqlException, IOException {
        assertCastDouble("a\n" +
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
                ColumnType.INT);
    }

    @Test
    public void testCastDoubleLong() throws SqlException, IOException {
        assertCastDouble("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastDoubleShort() throws SqlException, IOException {
        assertCastDouble("a\n" +
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
                ColumnType.SHORT);
    }

    @Test
    public void testCastDoubleTimestamp() throws SqlException, IOException {
        assertCastDouble("a\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000065Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000022Z\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000042Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000042Z\n" +
                        "1970-01-01T00:00:00.000070Z\n" +
                        "1970-01-01T00:00:00.000038Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000032Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000097Z\n" +
                        "1970-01-01T00:00:00.000024Z\n" +
                        "1970-01-01T00:00:00.000063Z\n",
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastFloatByte() throws SqlException, IOException {
        assertCastFloat("a\n" +
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
                ColumnType.BYTE);
    }

    @Test
    public void testCastFloatDate() throws SqlException, IOException {
        assertCastFloat("a\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.008Z\n" +
                        "1970-01-01T00:00:00.029Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.093Z\n" +
                        "1970-01-01T00:00:00.013Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.022Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.034Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.052Z\n" +
                        "1970-01-01T00:00:00.055Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.062Z\n" +
                        "1970-01-01T00:00:00.066Z\n",
                ColumnType.DATE);
    }

    @Test
    public void testCastFloatDouble() throws SqlException, IOException {
        assertCastFloat("a\n" +
                        "80.432235717773\n" +
                        "NaN\n" +
                        "8.486962318420\n" +
                        "29.919904708862\n" +
                        "NaN\n" +
                        "93.446044921875\n" +
                        "13.123357772827\n" +
                        "79.056755065918\n" +
                        "NaN\n" +
                        "22.452337265015\n" +
                        "NaN\n" +
                        "34.910701751709\n" +
                        "NaN\n" +
                        "76.110290527344\n" +
                        "52.437229156494\n" +
                        "55.991615295410\n" +
                        "NaN\n" +
                        "72.611358642578\n" +
                        "62.769538879395\n" +
                        "66.938369750977\n",
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastFloatInt() throws SqlException, IOException {
        assertCastFloat("a\n" +
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
                ColumnType.INT);
    }

    @Test
    public void testCastFloatLong() throws SqlException, IOException {
        assertCastFloat("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastFloatShort() throws SqlException, IOException {
        assertCastFloat("a\n" +
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
                ColumnType.SHORT);
    }

    @Test
    public void testCastFloatTimestamp() throws SqlException, IOException {
        assertCastFloat("a\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "1970-01-01T00:00:00.000029Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000093Z\n" +
                        "1970-01-01T00:00:00.000013Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000022Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000034Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000052Z\n" +
                        "1970-01-01T00:00:00.000055Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000062Z\n" +
                        "1970-01-01T00:00:00.000066Z\n",
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastIntByte() throws SqlException, IOException {
        assertCastInt("a\n" +
                "1\n" +
                "0\n" +
                "22\n" +
                "22\n" +
                "0\n" +
                "7\n" +
                "26\n" +
                "26\n" +
                "0\n" +
                "13\n" +
                "0\n" +
                "0\n" +
                "0\n" +
                "25\n" +
                "21\n" +
                "23\n" +
                "0\n" +
                "6\n" +
                "19\n" +
                "7\n", ColumnType.BYTE);
    }

    @Test
    public void testCastIntDate() throws SqlException, IOException {
        assertCastInt("a\n" +
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
                ColumnType.DATE);
    }

    @Test
    public void testCastIntDouble() throws SqlException, IOException {
        assertCastInt("a\n" +
                        "1.000000000000\n" +
                        "-2.147483648E9\n" +
                        "22.000000000000\n" +
                        "22.000000000000\n" +
                        "-2.147483648E9\n" +
                        "7.000000000000\n" +
                        "26.000000000000\n" +
                        "26.000000000000\n" +
                        "-2.147483648E9\n" +
                        "13.000000000000\n" +
                        "-2.147483648E9\n" +
                        "0.000000000000\n" +
                        "-2.147483648E9\n" +
                        "25.000000000000\n" +
                        "21.000000000000\n" +
                        "23.000000000000\n" +
                        "-2.147483648E9\n" +
                        "6.000000000000\n" +
                        "19.000000000000\n" +
                        "7.000000000000\n",
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastIntFloat() throws SqlException, IOException {
        assertCastInt("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastIntLong() throws SqlException, IOException {
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
    public void testCastIntShort() throws SqlException, IOException {
        assertCastInt("a\n" +
                        "1\n" +
                        "0\n" +
                        "22\n" +
                        "22\n" +
                        "0\n" +
                        "7\n" +
                        "26\n" +
                        "26\n" +
                        "0\n" +
                        "13\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "25\n" +
                        "21\n" +
                        "23\n" +
                        "0\n" +
                        "6\n" +
                        "19\n" +
                        "7\n",
                ColumnType.SHORT);
    }

    @Test
    public void testCastIntTimestamp() throws SqlException, IOException {
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
    public void testCastLongByte() throws SqlException, IOException {
        assertCastLong("a\n" +
                        "22\n" +
                        "0\n" +
                        "17\n" +
                        "2\n" +
                        "0\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "0\n" +
                        "14\n" +
                        "0\n" +
                        "26\n" +
                        "0\n" +
                        "23\n" +
                        "2\n" +
                        "24\n" +
                        "0\n" +
                        "16\n" +
                        "10\n" +
                        "6\n",
                ColumnType.BYTE);
    }

    @Test
    public void testCastLongDate() throws SqlException, IOException {
        assertCastLong("a\n" +
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
                ColumnType.DATE);
    }

    @Test
    public void testCastLongDouble() throws SqlException, IOException {
        assertCastLong("a\n" +
                        "22.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "17.000000000000\n" +
                        "2.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "21.000000000000\n" +
                        "1.000000000000\n" +
                        "20.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "14.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "26.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "23.000000000000\n" +
                        "2.000000000000\n" +
                        "24.000000000000\n" +
                        "-9.223372036854776E18\n" +
                        "16.000000000000\n" +
                        "10.000000000000\n" +
                        "6.000000000000\n",
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastLongFloat() throws SqlException, IOException {
        assertCastLong("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastLongInt() throws SqlException, IOException {
        assertCastLong("a\n" +
                        "22\n" +
                        "0\n" +
                        "17\n" +
                        "2\n" +
                        "0\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "0\n" +
                        "14\n" +
                        "0\n" +
                        "26\n" +
                        "0\n" +
                        "23\n" +
                        "2\n" +
                        "24\n" +
                        "0\n" +
                        "16\n" +
                        "10\n" +
                        "6\n",
                ColumnType.INT);
    }

    @Test
    public void testCastLongShort() throws SqlException, IOException {
        assertCastLong("a\n" +
                        "22\n" +
                        "0\n" +
                        "17\n" +
                        "2\n" +
                        "0\n" +
                        "21\n" +
                        "1\n" +
                        "20\n" +
                        "0\n" +
                        "14\n" +
                        "0\n" +
                        "26\n" +
                        "0\n" +
                        "23\n" +
                        "2\n" +
                        "24\n" +
                        "0\n" +
                        "16\n" +
                        "10\n" +
                        "6\n",
                ColumnType.SHORT);
    }

    @Test
    public void testCastLongTimestamp() throws SqlException, IOException {
        assertCastLong("a\n" +
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
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastNumberFail() {
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
    public void testCastShortByte() throws SqlException, IOException {
        assertCastShort("a\n" +
                        "-106\n" +
                        "-42\n" +
                        "-76\n" +
                        "-41\n" +
                        "-41\n" +
                        "-107\n" +
                        "117\n" +
                        "3\n" +
                        "-35\n" +
                        "21\n" +
                        "38\n" +
                        "-25\n" +
                        "46\n" +
                        "-8\n" +
                        "-120\n" +
                        "101\n" +
                        "30\n" +
                        "-122\n" +
                        "52\n" +
                        "91\n",
                ColumnType.BYTE);
    }

    @Test
    public void testCastShortDate() throws SqlException, IOException {
        assertCastShort("a\n" +
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
                ColumnType.DATE);
    }

    @Test
    public void testCastShortDouble() throws SqlException, IOException {
        assertCastShort("a\n" +
                        "1430.000000000000\n" +
                        "1238.000000000000\n" +
                        "1204.000000000000\n" +
                        "1751.000000000000\n" +
                        "1751.000000000000\n" +
                        "1429.000000000000\n" +
                        "1397.000000000000\n" +
                        "1539.000000000000\n" +
                        "1501.000000000000\n" +
                        "1045.000000000000\n" +
                        "1318.000000000000\n" +
                        "1255.000000000000\n" +
                        "1838.000000000000\n" +
                        "1784.000000000000\n" +
                        "1928.000000000000\n" +
                        "1381.000000000000\n" +
                        "1822.000000000000\n" +
                        "1414.000000000000\n" +
                        "1588.000000000000\n" +
                        "1371.000000000000\n",
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastShortFloat() throws SqlException, IOException {
        assertCastShort("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastShortInt() throws SqlException, IOException {
        assertCastShort("a\n" +
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
                ColumnType.INT);
    }

    @Test
    public void testCastShortLong() throws SqlException, IOException {
        assertCastShort("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastShortTimestamp() throws SqlException, IOException {
        assertCastShort("a\n" +
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
                ColumnType.TIMESTAMP);
    }

    @Test
    public void testCastTimestampByte() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
                        "89\n" +
                        "0\n" +
                        "-19\n" +
                        "-99\n" +
                        "0\n" +
                        "-102\n" +
                        "86\n" +
                        "83\n" +
                        "0\n" +
                        "30\n" +
                        "0\n" +
                        "-128\n" +
                        "0\n" +
                        "-115\n" +
                        "-106\n" +
                        "-76\n" +
                        "0\n" +
                        "25\n" +
                        "30\n" +
                        "-69\n",
                ColumnType.BYTE);
    }

    @Test
    public void testCastTimestampDate() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
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
                ColumnType.DATE);
    }

    @Test
    public void testCastTimestampDouble() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
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
                ColumnType.DOUBLE);
    }

    @Test
    public void testCastTimestampFloat() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
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
                ColumnType.FLOAT);
    }

    @Test
    public void testCastTimestampInt() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
                        "1929150553\n" +
                        "0\n" +
                        "-1662833171\n" +
                        "-1181550947\n" +
                        "0\n" +
                        "1427946650\n" +
                        "-1020695722\n" +
                        "1860812627\n" +
                        "0\n" +
                        "1532714782\n" +
                        "0\n" +
                        "-1596883072\n" +
                        "0\n" +
                        "2141839757\n" +
                        "765393046\n" +
                        "-264666444\n" +
                        "0\n" +
                        "333923609\n" +
                        "-959951586\n" +
                        "237646267\n",
                ColumnType.INT);
    }

    @Test
    public void testCastTimestampLong() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
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
                ColumnType.LONG);
    }

    @Test
    public void testCastTimestampShort() throws SqlException, IOException {
        assertCastTimestamp("a\n" +
                        "-32679\n" +
                        "0\n" +
                        "11757\n" +
                        "-2403\n" +
                        "0\n" +
                        "-17254\n" +
                        "27478\n" +
                        "-16557\n" +
                        "0\n" +
                        "24350\n" +
                        "0\n" +
                        "32640\n" +
                        "0\n" +
                        "-7795\n" +
                        "-1898\n" +
                        "-32076\n" +
                        "0\n" +
                        "17689\n" +
                        "19742\n" +
                        "12731\n",
                ColumnType.SHORT);
    }

    @Test
    public void testCreateAsSelect() throws SqlException, IOException {
        String expectedData = "a1\ta\tb\tc\td\te\tf\tf1\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.191123461757\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.041428124702\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.758817540345\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.148305523358\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.337470756550\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696943\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.113718418361\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.055149337562\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.185864355816\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.491990017163\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514583\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.703978540803\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.650660460171\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.803404910559\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.286271736488\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.108206023861\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        String expectedMeta = "{\"columnCount\":16,\"columns\":[{\"index\":0,\"name\":\"a1\",\"type\":\"INT\"},{\"index\":1,\"name\":\"a\",\"type\":\"INT\"},{\"index\":2,\"name\":\"b\",\"type\":\"BOOLEAN\"},{\"index\":3,\"name\":\"c\",\"type\":\"STRING\"},{\"index\":4,\"name\":\"d\",\"type\":\"DOUBLE\"},{\"index\":5,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":6,\"name\":\"f\",\"type\":\"SHORT\"},{\"index\":7,\"name\":\"f1\",\"type\":\"SHORT\"},{\"index\":8,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":9,\"name\":\"h\",\"type\":\"TIMESTAMP\"},{\"index\":10,\"name\":\"i\",\"type\":\"SYMBOL\"},{\"index\":11,\"name\":\"j\",\"type\":\"LONG\"},{\"index\":12,\"name\":\"j1\",\"type\":\"LONG\"},{\"index\":13,\"name\":\"k\",\"type\":\"TIMESTAMP\"},{\"index\":14,\"name\":\"l\",\"type\":\"BYTE\"},{\"index\":15,\"name\":\"m\",\"type\":\"BINARY\"}],\"timestampIndex\":13}";

        assertCast(expectedData, expectedMeta, "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a1', rnd_int()," +
                " 'a', rnd_int(0, 30, 2)," +
                " 'b', rnd_boolean()," +
                " 'c', rnd_str(3,3,2)," +
                " 'd', rnd_double(2)," +
                " 'e', rnd_float(2)," +
                " 'f', rnd_short(10,1024)," +
                " 'f1', rnd_short()," +
                " 'g', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                " 'h', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)," +
                " 'i', rnd_symbol(4,4,4,2)," +
                " 'j', rnd_long(100,200,2)," +
                " 'j1', rnd_long()," +
                " 'k', timestamp_sequence(to_timestamp(0), 1000000000)," +
                " 'l', rnd_byte(2,50)," +
                " 'm', rnd_bin(10, 20, 2)" +
                "))  timestamp(k) partition by DAY");
    }

    @Test
    public void testCreateAsSelectCastSymbol() throws SqlException, IOException {
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
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_symbol(4,4,4,2)" +
                ")), cast(a as STRING)";

        assertCast(expectedData, expectedMeta, sql);
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
                                try (TableWriter writer = engine.getWriter("X")) {
                                    if (state == 2) {
                                        writer.removeColumn("b");
                                    } else {
                                        writer.removeColumn("b" + (state - 1));
                                    }
                                    writer.addColumn("b" + state, ColumnType.INT);
                                }
                            }
                        }
                    });
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "underlying cursor is extremely volatile");
        }
    }

    @Test
    public void testCreateAsSelectIOError() throws Exception {

        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() throws Exception {
                String sql = "create table y as (" +
                        "select * from random_cursor(" +
                        " 10000," + // record count
                        " 'a', rnd_symbol(4,4,4,2)" +
                        ")), cast(a as STRING)";

                final FilesFacade ff = new FilesFacadeImpl() {
                    int mapCount = 0;

                    @Override
                    public long getMapPageSize() {
                        return getPageSize();
                    }

                    @Override
                    public long mmap(long fd, long len, long offset, int mode) {
                        if (mapCount++ > 5) {
                            return -1;
                        }
                        return super.mmap(fd, len, offset, mode);
                    }
                };

                final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                };

                try (Engine engine = new Engine(configuration); SqlCompiler compiler = new SqlCompiler(engine, configuration)) {
                    try {
                        compiler.compile(sql, bindVariableService);
                        Assert.fail();
                    } catch (CairoException ignore) {
                    }

                    engine.releaseAllReaders();
                    engine.releaseAllWriters();

                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
            }
        });
    }

    @Test
    public void testCreateAsSelectIOError2() throws Exception {

        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() throws Exception {
                String sql = "create table y as (" +
                        "select * from random_cursor(" +
                        " 10000," + // record count
                        " 'a', rnd_symbol(4,4,4,2)" +
                        ")), cast(a as STRING)";

                final FilesFacade ff = new FilesFacadeImpl() {
                    int mapCount = 0;

                    @Override
                    public long getMapPageSize() {
                        return getPageSize();
                    }

                    @Override
                    public long mmap(long fd, long len, long offset, int mode) {
                        // this is very specific failure
                        // it fails to open table writer metadata
                        // and then fails to close txMem
                        if (mapCount++ > 2) {
                            return -1;
                        }
                        return super.mmap(fd, len, offset, mode);
                    }
                };

                final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                };

                try (Engine engine = new Engine(configuration); SqlCompiler compiler = new SqlCompiler(engine, configuration)) {
                    try {
                        compiler.compile(sql, bindVariableService);
                        Assert.fail();
                    } catch (CairoException ignore) {
                    }

                    engine.releaseAllReaders();
                    engine.releaseAllWriters();

                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
            }
        });
    }

    @Test
    public void testCreateAsSelectInvalidTimestamp() {
        assertFailure(88, "TIMESTAMP column expected",
                "create table y as (" +
                        "select * from random_cursor(" +
                        " 20," + // record count
                        " 'a', rnd_int(0, 30, 2)" +
                        "))  timestamp(a) partition by DAY");
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
                            try (TableWriter writer = engine.getWriter("X")) {
                                writer.removeColumn("b");
                            }
                        }
                    }
                });
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
                                try (TableWriter writer = engine.getWriter("X")) {
                                    writer.removeColumn("b");
                                }
                            }
                        }
                    });
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(43, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "Invalid column: b");
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
                            try (TableWriter writer = engine.getWriter("X")) {
                                writer.removeColumn("a");
                                writer.addColumn("c", ColumnType.FLOAT);
                            }
                        }
                    }
                });
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
                                try (TableWriter writer = engine.getWriter("X")) {
                                    writer.removeColumn("t");
                                    writer.addColumn("t", ColumnType.FLOAT);
                                }
                            }
                        }
                    });
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(46, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "TIMESTAMP column expected");
        }
    }

    @Test
    public void testCreateCleanUpFailure() throws SqlException {
        // remove column from table X
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
                    try (TableWriter writer = engine.getWriter("X")) {
                        writer.removeColumn("a");
                        writer.addColumn("c", ColumnType.FLOAT);
                    }
                }
            }
        };

        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean rmdir(Path name) {
                if (Chars.endsWith(name, "Y" + Files.SEPARATOR)) {
                    return false;
                }
                return super.rmdir(name);
            }
        };

        final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        try (Engine engine = new Engine(configuration) {
            @Override
            public TableReader getReader(CharSequence tableName, long tableVersion) {
                fiddler.run(this);
                return super.getReader(tableName, tableVersion);
            }
        }) {

            SqlCompiler compiler = new SqlCompiler(engine, configuration);

            // create source table
            SqlCompilerTest.compiler.compile("create table X (a int, b int, t timestamp) timestamp(t)", bindVariableService);

            try {
                compiler.compile("create table Y as (select * from X) timestamp(t)", bindVariableService);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(0, e.getPosition());
                TestUtils.assertContains(e.getMessage(), "Concurrent modification cannot be handled");
            }

            Assert.assertEquals(0, engine.getBusyReaderCount());
        }
    }

    @Test
    public void testCreateEmptyTableNoPartition() throws SqlException {
        compiler.compile("create table x (" +
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
                        "timestamp(t)",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

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
        compiler.compile("create table x (" +
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
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":-1}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolCache() throws SqlException {
        compiler.compile("create table x (" +
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
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

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
        compiler.compile("create table x (" +
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
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

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
        compiler.compile("create table x (" +
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
                        "partition by MONTH",
                bindVariableService
        );

        try (TableReader reader = engine.getReader("x", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2048},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(reader.getMetadata().getColumnIndexQuiet("x"));
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateTableUtf8() throws SqlException, IOException {
        compiler.compile("create table доходы(экспорт int)", bindVariableService);

        try (TableWriter writer = engine.getWriter("доходы")) {
            for (int i = 0; i < 20; i++) {
                TableWriter.Row row = writer.newRow(0);
                row.putInt(0, i);
                row.append();
            }
            writer.commit();
        }

        compiler.compile("create table миллионы as (select * from доходы)", bindVariableService);

        try (TableReader reader = engine.getReader("миллионы", -1)) {
            sink.clear();
            printer.print(reader.getCursor(), reader.getMetadata(), true);
        }

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

        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testDuplicateTableName() throws SqlException {
        compiler.compile("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        assertFailure(13, "table already exists",
                "create table x (" +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH");
    }

    @Test
    public void testExecuteQuery() {
        assertFailure(
                58,
                "not a TIMESTAMP",
                "select * from random_cursor(20, 'x', rnd_int()) timestamp(x)"

        );
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.191123461757\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.041428124702\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.758817540345\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.148305523358\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.337470756550\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696943\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.113718418361\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.055149337562\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.185864355816\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.491990017163\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514583\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.703978540803\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.650660460171\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.803404910559\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.286271736488\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.108206023861\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(expectedData,
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
                        " timestamp_sequence(to_timestamp(0), 1000000000)," +
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
                "1253890363\t10\tfalse\tXYS\t0.191123461757\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.041428124702\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.758817540345\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.148305523358\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.337470756550\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696943\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.113718418361\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.055149337562\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.185864355816\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.491990017163\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514583\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.703978540803\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.650660460171\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.803404910559\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.286271736488\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.108206023861\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(expectedData,
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
                        " timestamp_sequence(to_timestamp(0), 1000000000)," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20)",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnListAndTimestamp() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                "1253890363\t10\tfalse\tXYS\t0.191123461757\t0.5793\t881\t-1379\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                "-1819240775\t27\ttrue\tGOO\t0.041428124702\t0.9205\t97\t-9039\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                "-1201923128\t18\ttrue\tUVS\t0.758817540345\t0.5779\t480\t-4379\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                "865832060\tNaN\ttrue\t\t0.148305523358\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                "00000010 38 e1\n" +
                "1100812407\t22\tfalse\tOVL\tNaN\t0.7633\t698\t-17778\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                "1677463366\t18\tfalse\tMNZ\t0.337470756550\t0.1179\t533\t18904\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                "00000010 28 60\n" +
                "39497392\t4\tfalse\tUOH\t0.029227696943\t0.1718\t652\t14242\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                "00000010 ea 4e ea 8b\n" +
                "1545963509\t10\tfalse\tNWI\t0.113718418361\t0.0620\t356\t-29980\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                "53462821\t4\tfalse\tGOO\t0.055149337562\t0.1195\t115\t-6087\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                "-2139296159\t30\tfalse\t\t0.185864355816\t0.5638\t299\t21020\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                "-406528351\t21\tfalse\tNLE\tNaN\tNaN\t968\t21057\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                "415709351\t17\tfalse\tGQZ\t0.491990017163\t0.6292\t581\t18605\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                "00000010 44 a8 0d fe\n" +
                "-1387693529\t19\ttrue\tMCG\t0.848083900630\t0.4699\t119\t24206\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                "00000010 20 53 3b 51\n" +
                "346891421\t21\tfalse\t\t0.933609514583\t0.6380\t405\t15084\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                "263487884\t27\ttrue\tHZQ\t0.703978540803\t0.8461\t834\t31562\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                "-1034870849\t9\tfalse\tLSV\t0.650660460171\t0.7020\t110\t-838\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                "1848218326\t26\ttrue\tSUW\t0.803404910559\t0.0440\t854\t-3502\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                "-1496904948\t5\ttrue\tDBZ\t0.286271736488\tNaN\t764\t5698\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                "856634079\t20\ttrue\tRJU\t0.108206023861\t0.4565\t669\t13505\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                "00000010 ab 3f a1 f5\n";

        testInsertAsSelect(expectedData,
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
                        " timestamp_sequence(to_timestamp(0), 1000000000) ts," +
                        " rnd_byte(2,50)," +
                        " rnd_bin(10, 20, 2)" +
                        " from long_sequence(20)) timestamp(ts)",
                "select * from x"
        );
    }

    @Test
    public void testInsertAsSelectColumnSubset() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\tj\tk\tl\tm\tn\to\tp\n" +
                "NaN\tNaN\tfalse\t\t0.804322409997\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:00:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.084869642326\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:16:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.084383207626\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:33:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.650859402586\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T00:50:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.790567531968\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:06:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.224523408561\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:23:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.349107036373\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:40:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.761102951500\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T01:56:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.421776884197\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:13:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:30:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.726113620982\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T02:46:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.422435666165\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:03:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.709436048717\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:20:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.385399478652\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:36:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.003598367215\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T03:53:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.328817690768\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:10:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:26:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.977110314605\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T04:43:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.248088123767\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:00:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.638160753118\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:16:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.125030421903\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:33:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.903806879651\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T05:50:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.134501705709\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:06:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.891258753660\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:23:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.975526354057\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:40:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.269221034797\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T06:56:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.413816474823\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:13:20.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.552249417051\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:30:00.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.245934527761\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T07:46:40.000000Z\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t0\t\t\tNaN\tNaN\t1970-01-01T08:03:20.000000Z\t0\t\n";

        testInsertAsSelect(expectedData,
                "create table x (a INT, b INT, c BOOLEAN, d STRING, e DOUBLE, f FLOAT, g SHORT, j TIMESTAMP, k SYMBOL, l LONG, m LONG, n TIMESTAMP, o BYTE, p BINARY)",
                "insert into x (e,n)" +
                        "select" +
                        " rnd_double(2)," +
                        " timestamp_sequence(to_timestamp(0), 1000000000)" +
                        " from long_sequence(30)",
                "x"
        );
    }

    @Test
    public void testInsertAsSelectColumnSubset2() throws Exception {
        String expectedData = "a\tb\tc\td\te\tf\tg\tj\tk\tl\tm\tn\to\tp\n" +
                "NaN\tNaN\tfalse\t\t0.804322409997\tNaN\t-13027\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.284557779121\tNaN\t21015\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.934460485739\tNaN\t-5356\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.790567531968\tNaN\t-19832\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.889928691229\tNaN\t23922\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t31987\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.462183542913\tNaN\t-4472\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.807237223338\tNaN\t4924\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.627695402837\tNaN\t-11679\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.709436048717\tNaN\t-12348\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.198558179736\tNaN\t-8877\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.524932106269\tNaN\t13182\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t2056\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.215832242693\tNaN\t12941\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.814680794450\tNaN\t-5176\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.125030421903\tNaN\t-7976\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.968742327694\tNaN\t15926\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.670047639180\tNaN\t2276\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.975526354057\tNaN\t5639\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.810161274171\tNaN\t-391\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.376250170950\tNaN\t-30933\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.245934527761\tNaN\t20366\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.975019885373\tNaN\t-3567\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.490051044989\tNaN\t3428\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\tNaN\tNaN\t29978\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.041428124702\tNaN\t-19136\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.799773322997\tNaN\t-21442\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.659034160769\tNaN\t-2018\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.173705703243\tNaN\t9478\t\t\tNaN\tNaN\t\t0\t\n" +
                "NaN\tNaN\tfalse\t\t0.046458498446\tNaN\t6093\t\t\tNaN\tNaN\t\t0\t\n";

        testInsertAsSelect(expectedData,
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
        testInsertAsSelect("a\tb\n" +
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
                "x");
    }

    @Test
    public void testInsertAsSelectConvertible2() throws Exception {
        testInsertAsSelect("b\ta\n" +
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
                "x");
    }

    @Test
    public void testInsertAsSelectConvertibleList1() throws Exception {
        testInsertAsSelect("a\tb\tn\n" +
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
                "x");
    }

    @Test
    public void testInsertAsSelectConvertibleList2() throws Exception {
        testInsertAsSelect("a\tb\tn\n" +
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
                "x");
    }

    public void testInsertAsSelectError(CharSequence ddl, CharSequence insert, int errorPosition, CharSequence errorMessage) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                if (ddl != null) {
                    compiler.compile(ddl, bindVariableService);
                }
                try {
                    compiler.compile(insert, bindVariableService);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(errorPosition, e.getPosition());
                    TestUtils.assertContains(e.getMessage(), errorMessage);
                }

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testInsertAsSelectInconvertible1() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 32, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertible2() throws Exception {
        testInsertAsSelectError("create table x (a INT, b BYTE)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 32, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertibleList1() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 17, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertibleList2() throws Exception {
        testInsertAsSelectError("create table x (a BYTE, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 17, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertibleList3() throws Exception {
        testInsertAsSelectError("create table x (a SHORT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 17, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertibleList4() throws Exception {
        testInsertAsSelectError("create table x (a FLOAT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_double(2)" +
                        " from long_sequence(30)", 17, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInconvertibleList5() throws Exception {
        testInsertAsSelectError("create table x (a FLOAT, b INT, n TIMESTAMP)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(5,5,0)" +
                        " from long_sequence(30)", 17, "inconvertible types");
    }

    @Test
    public void testInsertAsSelectInvalidColumn() throws Exception {
        testInsertAsSelectError("create table x (aux1 INT, b INT)",
                "insert into x (aux1,blast)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_long()" +
                        " from long_sequence(30)", 20, "Invalid column: blast");
    }

    @Test
    public void testInsertAsSelectPersistentIOError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            AtomicBoolean inError = new AtomicBoolean(true);

            FilesFacade ff = new FilesFacadeImpl() {
                int pageCount = 0;

                @Override
                public long getMapPageSize() {
                    return getPageSize();
                }

                @Override
                public long mmap(long fd, long len, long offset, int mode) {
                    if (inError.get() && pageCount++ > 9) {
                        return -1;
                    }
                    return super.mmap(fd, len, offset, mode);
                }


            };

            assertInsertAsSelectIOError(inError, ff);
        });
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
                    try (TableWriter writer = engine.getWriter("y")) {
                        writer.removeColumn("int1");
                        writer.addColumn("c", ColumnType.INT);
                    }
                }
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            try (Engine engine = new Engine(configuration) {
                @Override
                public TableReader getReader(CharSequence tableName, long version) {
                    fiddler.run(this);
                    return super.getReader(tableName, version);
                }
            }) {
                try (SqlCompiler compiler = new SqlCompiler(engine, configuration)) {

                    compiler.compile("create table x (a INT, b INT)", bindVariableService);
                    compiler.compile("create table y as (select rnd_int() int1, rnd_int() int2 from long_sequence(10))", bindVariableService);
                    compiler.compile("insert into x select * from y", bindVariableService);

                    try (RecordCursorFactory factory = compiler.compile("select * from x", bindVariableService)) {
                        sink.clear();
                        try (RecordCursor cursor = factory.getCursor(bindVariableService)) {
                            printer.print(cursor, factory.getMetadata(), true);
                        }
                        TestUtils.assertEquals(expected, sink);
                    }

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
                " timestamp_sequence(to_timestamp(0), 1000000000)" +
                " from long_sequence(30)", 12, "table does not exist");
    }

    @Test
    public void testInsertAsSelectTemporaryIOError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            AtomicBoolean inError = new AtomicBoolean(true);

            FilesFacade ff = new FilesFacadeImpl() {
                int pageCount = 0;

                @Override
                public long mmap(long fd, long len, long offset, int mode) {
                    if (inError.get() && pageCount++ == 9) {
                        return -1;
                    }
                    return super.mmap(fd, len, offset, mode);
                }

                @Override
                public long getMapPageSize() {
                    return getPageSize();
                }
            };

            assertInsertAsSelectIOError(inError, ff);
        });
    }

    @Test
    public void testInsertAsSelectTimestampNotInList() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT, n TIMESTAMP) timestamp(n)",
                "insert into x (b,a)" +
                        " select * from (" +
                        " select" +
                        " rnd_int()," +
                        " rnd_int() x" +
                        " from long_sequence(30)" +
                        ") timestamp(x)", 15, "column list must include timestamp");
    }

    @Test
    public void testInsertAsSelectTimestampNotSelected() throws Exception {
        testInsertAsSelectError("create table x (a INT, b INT, n TIMESTAMP) timestamp(n)",
                "insert into x (b,a)" +
                        "select" +
                        " rnd_int()," +
                        " rnd_int()" +
                        " from long_sequence(30)", 12, "select clause must provide timestamp column");
    }

    @Test
    public void testRaceToCreateEmptyTable() throws InterruptedException {
        SqlCompiler compiler2 = new SqlCompiler(engine, configuration);
        AtomicInteger index = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();

        for (int i = 0; i < 50; i++) {
            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch haltLatch = new CountDownLatch(2);

            index.set(-1);
            success.set(0);

            new Thread(() -> {
                try {
                    barrier.await();
                    compiler.compile("create table x (a INT, b FLOAT)", bindVariableService);
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
                    compiler2.compile("create table x (a STRING, b DOUBLE)", bindVariableService);
                    index.set(1);
                    success.incrementAndGet();
                } catch (Exception ignore) {
//                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();

            Assert.assertTrue(haltLatch.await(5, TimeUnit.SECONDS));

            Assert.assertEquals(1, success.get());
            Assert.assertNotEquals(-1, index.get());

            try (TableReader reader = engine.getReader("x", -1)) {
                sink.clear();
                reader.getMetadata().toJson(sink);
                if (index.get() == 0) {
                    TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"FLOAT\"}],\"timestampIndex\":-1}", sink);
                } else {
                    TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"STRING\"},{\"index\":1,\"name\":\"b\",\"type\":\"DOUBLE\"}],\"timestampIndex\":-1}", sink);
                }
            }
            engine.remove("x");
        }
    }

    @Test
    public void testSqlCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String ddl = "create table x (a STRING, b INT, n TIMESTAMP)";
            String insert = "insert into x (b,a)" +
                    "select" +
                    " rnd_int()," +
                    " rnd_symbol(8,6,10,2)" +
                    " from long_sequence(30)";
            String query = "x where a = :sym";

            compiler.compile(ddl, bindVariableService);
            compiler.compile(insert, bindVariableService);


            bindVariableService.setStr("sym", null);
            RecordCursorFactory factory = compiler.compile(query, bindVariableService);
            sink.clear();
            bindVariableService.setStr("sym", "RXGZSXUX");
            try (RecordCursor cursor = factory.getCursor(bindVariableService)) {
                printer.print(cursor, factory.getMetadata(), true);
            }

            TestUtils.assertEquals(
                    "a\tb\tn\n" +
                            "RXGZSXUX\t-235358133\t\n" +
                            "RXGZSXUX\t-1424048819\t\n", sink);

            compiler.cache(query, factory);

            sink.clear();

            try (RecordCursorFactory f = compiler.compile(query, bindVariableService)) {

                bindVariableService.setStr("sym", null);
                try (RecordCursor cursor = f.getCursor(bindVariableService)) {
                    printer.print(cursor, f.getMetadata(), true);
                }

                TestUtils.assertEquals("a\tb\tn\n" +
                                "\t326010667\t\n" +
                                "\t1196016669\t\n" +
                                "\t-1566901076\t\n" +
                                "\t-1201923128\t\n" +
                                "\t1234796102\t\n" +
                                "\t-89906802\t\n" +
                                "\t659736535\t\n" +
                                "\t1060917944\t\n" +
                                "\t2060263242\t\n",
                        sink);
            }

            // attempt to compile factory again after it is closed

            bindVariableService.setStr("sym", null);
            try (RecordCursorFactory f2 = compiler.compile(query, bindVariableService)) {
                sink.clear();
                bindVariableService.setStr("sym", "RXGZSXUX");
                try (RecordCursor cursor = f2.getCursor(bindVariableService)) {
                    printer.print(cursor, f2.getMetadata(), true);
                }

                TestUtils.assertEquals(
                        "a\tb\tn\n" +
                                "RXGZSXUX\t-235358133\t\n" +
                                "RXGZSXUX\t-1424048819\t\n", sink);
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

    private void assertCast(String expectedData, String expectedMeta, String sql) throws SqlException, IOException {
        compiler.compile(sql, bindVariableService);
        try (TableReader reader = engine.getReader("y", -1)) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(expectedMeta, sink);

            sink.clear();
            printer.print(reader.getCursor(), reader.getMetadata(), true);
            TestUtils.assertEquals(expectedData, sink);
        }
    }

    private void assertCastByte(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_byte(33, 119)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastByteFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_byte(2,50)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(85, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastDate(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastDouble(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', 100 * rnd_double(2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastDoubleFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_double(2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(84, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastFloat(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', 100 * rnd_float(2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastFloatFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_float(2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(83, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastInt(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_int(0, 30, 2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastIntFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_int(0, 30, 2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(88, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastLong(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_long(0, 30, 2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastLongFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_long(0, 30, 2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(89, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastShort(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_short(1024, 2048)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCastShortFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_short(2,10)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(86, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastStringFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_str(5,10,2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(86, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastSymbolFail(int castTo) {
        try {
            compiler.compile("create table y as (" +
                            "select * from random_cursor(" +
                            " 20," + // record count
                            " 'a', rnd_symbol(4,6,10,2)" +
                            ")), cast(a as " + ColumnType.nameOf(castTo) + ")",
                    bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(91, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "unsupported cast");
        }
    }

    private void assertCastTimestamp(String expectedData, int castTo) throws SqlException, IOException {
        String expectedMeta = "{\"columnCount\":1,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"" + ColumnType.nameOf(castTo) + "\"}],\"timestampIndex\":-1}";

        String sql = "create table y as (" +
                "select * from random_cursor(" +
                " 20," + // record count
                " 'a', rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2)" +
                ")), cast(a as " + ColumnType.nameOf(castTo) + ")";

        assertCast(expectedData, expectedMeta, sql);
    }

    private void assertCreateTableAsSelect(CharSequence expectedMetadata, CharSequence sql, Fiddler fiddler) throws SqlException {

        // create source table
        compiler.compile("create table X (a int, b int, t timestamp) timestamp(t)", bindVariableService);

        try (Engine engine = new Engine(configuration) {
            @Override
            public TableReader getReader(CharSequence tableName, long tableVersion) {
                fiddler.run(this);
                return super.getReader(tableName, tableVersion);
            }
        }) {

            SqlCompiler compiler = new SqlCompiler(engine, configuration);
            compiler.compile(sql, bindVariableService);

            Assert.assertTrue(fiddler.isHappy());

            try (TableReader reader = engine.getReader("Y", -1)) {
                sink.clear();
                reader.getMetadata().toJson(sink);
                TestUtils.assertEquals(expectedMetadata, sink);
            }

            Assert.assertEquals(0, engine.getBusyReaderCount());
        }
    }

    private void assertFailure(int position, CharSequence expectedMessage, CharSequence sql) {
        try {
            compiler.compile(sql, bindVariableService);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }

    private void assertInsertAsSelectIOError(AtomicBoolean inError, FilesFacade ff) throws SqlException {
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        try (Engine engine = new Engine(configuration)) {
            try (SqlCompiler compiler = new SqlCompiler(engine, configuration)) {

                compiler.compile("create table x (a INT, b INT)", bindVariableService);
                try {
                    compiler.compile("insert into x select rnd_int() int1, rnd_int() int2 from long_sequence(1000000)", bindVariableService);
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                try (TableWriter w = engine.getWriter("x")) {
                    Assert.assertEquals(0, w.size());
                }

                inError.set(false);

                compiler.compile("insert into x select rnd_int() int1, rnd_int() int2 from long_sequence(1000000)", bindVariableService);
                try (TableWriter w = engine.getWriter("x")) {
                    Assert.assertEquals(1000000, w.size());
                }

                try (TableReader reader = engine.getReader("x", 0)) {
                    Assert.assertEquals(1000000, reader.size());
                }

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            }
        }
    }

    private void testInsertAsSelect(CharSequence expectedData, CharSequence ddl, CharSequence insert, CharSequence select) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile(ddl, bindVariableService);
                compiler.compile(insert, bindVariableService);

                try (RecordCursorFactory factory = compiler.compile(select, bindVariableService)) {
                    sink.clear();
                    try (RecordCursor cursor = factory.getCursor(bindVariableService)) {
                        printer.print(cursor, factory.getMetadata(), true);
                    }
                    TestUtils.assertEquals(expectedData, sink);
                }

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    private interface Fiddler {
        boolean isHappy();

        void run(CairoEngine engine);
    }
}