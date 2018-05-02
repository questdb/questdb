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

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.Engine;
import com.questdb.cairo.SymbolMapReader;
import com.questdb.cairo.TableReader;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SqlCompilerTest extends AbstractCairoTest {
    private final static Engine engine = new Engine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();

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
    public void testCastDoubleTimestamp() throws SqlException, IOException {
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
    public void testCreateEmptyTableNoPartition() throws SqlException {
        compiler.execute("create table x (" +
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

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.NONE, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableNoTimestamp() throws SqlException {
        compiler.execute("create table x (" +
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

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":-1}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolCache() throws SqlException {
        compiler.execute("create table x (" +
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

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableSymbolNoCache() throws SqlException {
        compiler.execute("create table x (" +
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

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertFalse(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateEmptyTableWithIndex() throws SqlException {
        compiler.execute("create table x (" +
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

        try (TableReader reader = engine.getReader("x")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(
                    "{\"columnCount\":12,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"INT\"},{\"index\":1,\"name\":\"b\",\"type\":\"BYTE\"},{\"index\":2,\"name\":\"c\",\"type\":\"SHORT\"},{\"index\":3,\"name\":\"d\",\"type\":\"LONG\"},{\"index\":4,\"name\":\"e\",\"type\":\"FLOAT\"},{\"index\":5,\"name\":\"f\",\"type\":\"DOUBLE\"},{\"index\":6,\"name\":\"g\",\"type\":\"DATE\"},{\"index\":7,\"name\":\"h\",\"type\":\"BINARY\"},{\"index\":8,\"name\":\"t\",\"type\":\"TIMESTAMP\"},{\"index\":9,\"name\":\"x\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2048},{\"index\":10,\"name\":\"z\",\"type\":\"STRING\"},{\"index\":11,\"name\":\"y\",\"type\":\"BOOLEAN\"}],\"timestampIndex\":8}",
                    sink);

            Assert.assertEquals(PartitionBy.MONTH, reader.getPartitionedBy());
            Assert.assertEquals(0L, reader.size());

            int symbolIndex = reader.getMetadata().getColumnIndex("x");
            SymbolMapReader symbolMapReader = reader.getSymbolMapReader(symbolIndex);
            Assert.assertNotNull(symbolMapReader);
            Assert.assertEquals(16, symbolMapReader.getSymbolCapacity());
            Assert.assertTrue(symbolMapReader.isCached());
        }
    }

    @Test
    public void testCreateTableAsSelect() throws SqlException, IOException {
        String expectedData = "a1\ta\tb\tc\td\te\tf\tf1\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                "-1101822104\t22\tfalse\tEYY\t0.003598367215\t0.3288\t1012\t-9925\t2015-10-10T02:10:45.262Z\t2015-11-07T23:57:18.762143Z\tSXUX\t182\t-4842723177835140152\t1970-01-01T00:00:00.000000Z\t15\t\n" +
                "-27395319\t17\tfalse\t\t0.134501705709\t0.8913\t256\t1404\t2015-07-06T12:38:05.676Z\t2015-07-05T05:27:40.318830Z\tSXUX\t112\t6179044593759294347\t1970-01-01T00:16:40.000000Z\t31\t\n" +
                "326010667\t9\tfalse\t\t0.975019885373\t0.0011\t946\t-5637\t2015-07-26T19:40:33.330Z\t2015-02-12T10:08:48.404057Z\tGPGW\t129\t-5233802075754153909\t1970-01-01T00:33:20.000000Z\t25\t\n" +
                "-1212175298\t25\ttrue\tZOU\t0.565942913986\t0.8828\t958\t10633\t2015-10-26T01:51:34.298Z\t2015-04-29T03:13:50.122536Z\tRXGZ\t134\t-8081265393416742311\t1970-01-01T00:50:00.000000Z\t17\t\n" +
                "-916132123\t25\ttrue\tQOL\t0.038317858637\tNaN\t253\t-8761\t2015-09-19T19:55:53.176Z\t2015-08-16T07:46:57.313650Z\tGPGW\t190\t8611582118025429627\t1970-01-01T01:06:40.000000Z\t25\t\n" +
                "-876466531\t14\ttrue\tTIQ\tNaN\t0.9442\t95\t2508\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:23:20.000000Z\t40\t\n" +
                "602835017\t26\tfalse\tLTK\t0.240791559814\tNaN\t261\t-15573\t2015-03-03T13:26:27.587Z\t2015-07-06T20:28:39.110999Z\tSXUX\t153\t8889492928577876455\t1970-01-01T01:40:00.000000Z\t27\t\n" +
                "-805434743\t10\ttrue\t\t0.763261500432\t0.8817\t944\t-32151\t2015-03-18T09:57:14.898Z\t2015-06-24T05:57:55.642438Z\tGPGW\t173\t-6071768268784020226\t1970-01-01T01:56:40.000000Z\t6\t\n" +
                "133913299\t28\ttrue\tZRM\t0.117853162127\t0.7446\t175\t-5240\t2015-12-13T05:57:28.108Z\t2015-05-20T07:10:25.828661Z\t\t164\t7759595275644638709\t1970-01-01T02:13:20.000000Z\t39\t\n" +
                "-2108151088\t28\tfalse\tGII\t0.985907032220\t0.9884\t926\t17250\t2015-09-27T21:45:00.056Z\t\t\t122\t1488156692375549016\t1970-01-01T02:30:00.000000Z\t21\t\n" +
                "-1726426588\t30\tfalse\tZSQ\t0.779222297767\t0.8584\t1008\t11796\t2015-07-31T07:46:53.973Z\t2015-10-06T02:03:42.485301Z\t\t115\t-6782883555378798844\t1970-01-01T02:46:40.000000Z\t22\t\n" +
                "503883303\t29\ttrue\tQCE\t0.760625263412\t0.0658\t1018\t30442\t2015-11-09T04:12:58.038Z\t2015-11-14T01:00:25.951588Z\tRXGZ\t144\t4579251508938058953\t1970-01-01T03:03:20.000000Z\t15\t\n" +
                "-1504180829\t27\ttrue\tOMN\t0.758625411859\t0.7705\t241\t26471\t2015-11-23T04:33:14.998Z\t\tRXGZ\t163\t5953039264407551685\t1970-01-01T03:20:00.000000Z\t24\t\n" +
                "178157274\t20\ttrue\tXHF\t0.729248236745\t0.6108\t405\t13114\t2015-04-13T18:21:35.304Z\t2015-02-09T14:14:51.624879Z\tGPGW\tNaN\t-7274175842748412916\t1970-01-01T03:36:40.000000Z\t13\t\n" +
                "-715453934\t15\tfalse\tTOG\t0.585933238860\t0.3350\t639\t-3573\t\t2015-11-15T11:50:57.929220Z\tGPGW\t149\t-2471456524133707236\t1970-01-01T03:53:20.000000Z\t41\t\n" +
                "1561652006\t20\ttrue\tFFD\t0.515022928022\t0.1816\t439\t-11470\t2015-07-14T14:04:41.916Z\t2015-10-12T03:09:35.903970Z\tGPGW\t178\t7046578844650327247\t1970-01-01T04:10:00.000000Z\t47\t\n" +
                "267011905\t13\ttrue\tGPU\t0.753049452785\t0.4915\t598\t-24503\t2015-04-30T08:18:10.453Z\t2015-01-05T16:05:43.310197Z\t\t147\t6153381060986313135\t1970-01-01T04:26:40.000000Z\t40\t\n" +
                "-1091570282\t2\tfalse\tKHT\t0.187466319954\t0.2915\t348\t-7455\t2015-11-24T08:52:41.325Z\t2015-01-14T12:20:51.177806Z\tSXUX\t193\t1737550138998374432\t1970-01-01T04:43:20.000000Z\t21\t\n" +
                "1328749623\tNaN\tfalse\tYMI\t0.174984257225\t0.9798\t145\t22350\t2015-09-08T04:40:10.437Z\t2015-12-16T02:05:46.885318Z\tRXGZ\t173\t-3578120825657825955\t1970-01-01T05:00:00.000000Z\t42\t\n" +
                "1722682901\t24\ttrue\tGRM\t0.891161563102\t0.3209\t551\t17678\t2015-09-01T17:07:49.293Z\t2015-10-24T02:47:57.414139Z\tSXUX\t114\t-4979067588174058421\t1970-01-01T05:16:40.000000Z\t43\t\n";

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
    public void testDuplicateTableName() throws SqlException {
        compiler.execute("create table x (" +
                        "a INT, " +
                        "b BYTE, " +
                        "t TIMESTAMP, " +
                        "y BOOLEAN) " +
                        "timestamp(t) " +
                        "partition by MONTH",
                bindVariableService
        );

        try {
            compiler.execute("create table x (" +
                            "t TIMESTAMP, " +
                            "y BOOLEAN) " +
                            "timestamp(t) " +
                            "partition by MONTH",
                    bindVariableService
            );
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(13, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "table already exists");
        }
    }

    private void assertCast(String expectedData, String expectedMeta, String sql) throws SqlException, IOException {
        compiler.execute(sql, bindVariableService);
        try (TableReader reader = engine.getReader("y")) {
            sink.clear();
            reader.getMetadata().toJson(sink);
            TestUtils.assertEquals(expectedMeta, sink);

            sink.clear();
            printer.print(reader.getCursor(), true);
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
            compiler.execute("create table y as (" +
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
            compiler.execute("create table y as (" +
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
            compiler.execute("create table y as (" +
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
            compiler.execute("create table y as (" +
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
            compiler.execute("create table y as (" +
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
            compiler.execute("create table y as (" +
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

}