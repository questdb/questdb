/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpTypeConversionTest extends BaseLineTcpContextTest {

    private long time;

    private void resetTime() {
        time = 1465839830100400200L;
    }

    private long nextTime() {
        return time += 1000;
    }

    @Test
    public void testConversionToSymbol() throws Exception {
        testConversionToType("SYMBOL", "testCol\ttime\n" +
                        "questdb\t2016-06-13T17:43:50.100401Z\n" +
                        "\"questdbb\"\t2016-06-13T17:43:50.100402Z\n" +
                        "100i\t2016-06-13T17:43:50.100403Z\n" +
                        "-100i\t2016-06-13T17:43:50.100404Z\n" +
                        "0x100i\t2016-06-13T17:43:50.100405Z\n" +
                        "123\t2016-06-13T17:43:50.100406Z\n" +
                        "-54\t2016-06-13T17:43:50.100407Z\n" +
                        "23.3\t2016-06-13T17:43:50.100408Z\n" +
                        "T\t2016-06-13T17:43:50.100409Z\n" +
                        "false\t2016-06-13T17:43:50.100410Z\n" +
                        "1465839830101500200t\t2016-06-13T17:43:50.100411Z\n" +
                        "questdb\t2016-06-13T17:43:50.100412Z\n"
        );
    }

    @Test
    public void testConversionToString() throws Exception {
        testConversionToType("STRING", "testCol\ttime\n" +
                        "questdbb\t2016-06-13T17:43:50.100413Z\n"
        );
    }

    @Test
    public void testConversionToChar() throws Exception {
        testConversionToType("CHAR", "testCol\ttime\n" +
                "q\t2016-06-13T17:43:50.100413Z\n"
        );
    }

    @Test
    public void testConversionToLong256() throws Exception {
        testConversionToType("LONG256", "testCol\ttime\n" +
                "0x0150\t2016-06-13T17:43:50.100416Z\n"
        );
    }

    @Test
    public void testConversionToLong() throws Exception {
        testConversionToType("LONG", "testCol\ttime\n" +
                        "100\t2016-06-13T17:43:50.100414Z\n" +
                        "-100\t2016-06-13T17:43:50.100415Z\n" +
                        "1\t2016-06-13T17:43:50.100420Z\n" +
                        "0\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToInt() throws Exception {
        testConversionToType("INT", "testCol\ttime\n" +
                        "100\t2016-06-13T17:43:50.100414Z\n" +
                        "-100\t2016-06-13T17:43:50.100415Z\n" +
                        "1\t2016-06-13T17:43:50.100420Z\n" +
                        "0\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToShort() throws Exception {
        testConversionToType("SHORT", "testCol\ttime\n" +
                        "100\t2016-06-13T17:43:50.100414Z\n" +
                        "-100\t2016-06-13T17:43:50.100415Z\n" +
                        "1\t2016-06-13T17:43:50.100420Z\n" +
                        "0\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToByte() throws Exception {
        testConversionToType("BYTE", "testCol\ttime\n" +
                        "100\t2016-06-13T17:43:50.100414Z\n" +
                        "-100\t2016-06-13T17:43:50.100415Z\n" +
                        "1\t2016-06-13T17:43:50.100420Z\n" +
                        "0\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToBoolean() throws Exception {
        testConversionToType("BOOLEAN", "testCol\ttime\n" +
                "true\t2016-06-13T17:43:50.100420Z\n" +
                "false\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToFloat() throws Exception {
        testConversionToType("FLOAT", "testCol\ttime\n" +
                "123.0000\t2016-06-13T17:43:50.100417Z\n" +
                "-54.0000\t2016-06-13T17:43:50.100418Z\n" +
                "23.3000\t2016-06-13T17:43:50.100419Z\n" +
                "1.0000\t2016-06-13T17:43:50.100420Z\n" +
                "0.0000\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToDouble() throws Exception {
        testConversionToType("DOUBLE", "testCol\ttime\n" +
                "123.0\t2016-06-13T17:43:50.100417Z\n" +
                "-54.0\t2016-06-13T17:43:50.100418Z\n" +
                "23.3\t2016-06-13T17:43:50.100419Z\n" +
                "1.0\t2016-06-13T17:43:50.100420Z\n" +
                "0.0\t2016-06-13T17:43:50.100421Z\n"
        );
    }

    @Test
    public void testConversionToTimestamp() throws Exception {
        testConversionToType("TIMESTAMP", "testCol\ttime\n" +
                "1970-01-01T00:00:00.000100Z\t2016-06-13T17:43:50.100414Z\n" +
                "1969-12-31T23:59:59.999900Z\t2016-06-13T17:43:50.100415Z\n" +
                "2016-06-13T17:43:50.101500Z\t2016-06-13T17:43:50.100422Z\n"
        );
    }

    @Test
    public void testConversionToDate() throws Exception {
        testConversionToType("DATE", "testCol\ttime\n" +
                "1970-01-01T00:00:00.100Z\t2016-06-13T17:43:50.100414Z\n" +
                "1969-12-31T23:59:59.900Z\t2016-06-13T17:43:50.100415Z\n"
        );
    }

    @Test
    public void testConversionToBinary() throws Exception {
        testConversionToType("BINARY", "testCol\ttime\n");
    }

    @Test
    public void testConversionToGeoHash() throws Exception {
        testConversionToType("GEOHASH(7c)", "testCol\ttime\n" +
                "questdb\t2016-06-13T17:43:50.100412Z\n" +
                "questdb\t2016-06-13T17:43:50.100413Z\n" +
                "\t2016-06-13T17:43:50.100416Z\n");
    }

    private void testConversionToType(String type, String expected) throws Exception {
        resetTime();
        String table = "convTest";
        testConversion(table,
                "create table " + table + " (testCol " + type + ", time TIMESTAMP) timestamp(time);",
                table + ",testCol=questdb " + nextTime() + "\n" +
                        table + ",testCol=\"questdbb\" " + nextTime() + "\n" +
                        table + ",testCol=100i " + nextTime() + "\n" +
                        table + ",testCol=-100i " + nextTime() + "\n" +
                        table + ",testCol=0x100i " + nextTime() + "\n" +
                        table + ",testCol=123 " + nextTime() + "\n" +
                        table + ",testCol=-54 " + nextTime() + "\n" +
                        table + ",testCol=23.3 " + nextTime() + "\n" +
                        table + ",testCol=T " + nextTime() + "\n" +
                        table + ",testCol=false " + nextTime() + "\n" +
                        table + ",testCol=1465839830101500200t " + nextTime() + "\n" +
                        table + " testCol=questdb " + nextTime() + "\n" +
                        table + " testCol=\"questdbb\" " + nextTime() + "\n" +
                        table + " testCol=100i " + nextTime() + "\n" +
                        table + " testCol=-100i " + nextTime() + "\n" +
                        table + " testCol=0x0150i " + nextTime() + "\n" +
                        table + " testCol=123 " + nextTime() + "\n" +
                        table + " testCol=-54 " + nextTime() + "\n" +
                        table + " testCol=23.3 " + nextTime() + "\n" +
                        table + " testCol=T " + nextTime() + "\n" +
                        table + " testCol=false " + nextTime() + "\n" +
                        table + " testCol=1465839830101500t " + nextTime() + "\n",
                expected
        );
    }

    private void testConversion(String table, String createTableCmd, String input, String expected) throws Exception {
        runInContext(() -> {
            try (
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                compiler.compile(createTableCmd, sqlExecutionContext);
            } catch (SqlException ex) {
                throw new RuntimeException(ex);
            }

            recvBuffer = input;
            do {
                handleContextIO();
                Assert.assertFalse(disconnected);
            } while (recvBuffer.length() > 0);
            closeContext();
            assertTable(expected, table);
        });
    }
}
