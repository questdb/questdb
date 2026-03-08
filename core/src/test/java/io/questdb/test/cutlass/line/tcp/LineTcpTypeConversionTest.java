/*******************************************************************************
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

package io.questdb.test.cutlass.line.tcp;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LineTcpTypeConversionTest extends BaseLineTcpContextTest {
    private final boolean stringAsTagSupported;
    private final boolean walEnabled;
    private long time;

    public LineTcpTypeConversionTest(WalMode walMode) {
        walEnabled = (walMode == WalMode.WITH_WAL);
        symbolAsFieldSupported = true;
        stringAsTagSupported = true;
    }

    @Parameterized.Parameters(name = "{0}, {1}, {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL},
                {WalMode.NO_WAL}
        });
    }

    @Test
    public void testConversionToBinary() throws Exception {
        testConversionToType("BINARY", "testCol\ttime\n");
    }

    @Test
    public void testConversionToBoolean() throws Exception {
        testConversionToType("BOOLEAN", "testCol\ttime\n" +
                "true\t2016-06-13T17:43:50.100424Z\n" +
                "false\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToByte() throws Exception {
        testConversionToType("BYTE", "testCol\ttime\n" +
                "100\t2016-06-13T17:43:50.100418Z\n" +
                "-100\t2016-06-13T17:43:50.100419Z\n" +
                "1\t2016-06-13T17:43:50.100424Z\n" +
                "0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToChar() throws Exception {
        testConversionToType("CHAR", "testCol\ttime\n" +
                "q\t2016-06-13T17:43:50.100417Z\n"
        );
    }

    @Test
    public void testConversionToCharStringToCharCastAllowed() throws Exception {
        stringToCharCastAllowed = true;
        testConversionToType("CHAR", "testCol\ttime\n" +
                "q\t2016-06-13T17:43:50.100416Z\n" +
                "q\t2016-06-13T17:43:50.100417Z\n" +
                "1\t2016-06-13T17:43:50.100427Z\n" +
                "1\t2016-06-13T17:43:50.100429Z\n"
        );
    }

    @Test
    public void testConversionToDate() throws Exception {
        testConversionToType("DATE", "testCol\ttime\n" +
                "1970-01-01T00:00:00.100Z\t2016-06-13T17:43:50.100418Z\n" +
                "1969-12-31T23:59:59.900Z\t2016-06-13T17:43:50.100419Z\n" +
                "2016-06-13T17:43:50.101Z\t2016-06-13T17:43:50.100426Z\n"
        );
    }

    @Test
    public void testConversionToDouble() throws Exception {
        testConversionToType("DOUBLE", "testCol\ttime\n" +
                "100.0\t2016-06-13T17:43:50.100418Z\n" +
                "-100.0\t2016-06-13T17:43:50.100419Z\n" +
                "123.0\t2016-06-13T17:43:50.100421Z\n" +
                "-54.0\t2016-06-13T17:43:50.100422Z\n" +
                "23.3\t2016-06-13T17:43:50.100423Z\n" +
                "1.0\t2016-06-13T17:43:50.100424Z\n" +
                "0.0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToFloat() throws Exception {
        testConversionToType("FLOAT", "testCol\ttime\n" +
                "100.0\t2016-06-13T17:43:50.100418Z\n" +
                "-100.0\t2016-06-13T17:43:50.100419Z\n" +
                "123.0\t2016-06-13T17:43:50.100421Z\n" +
                "-54.0\t2016-06-13T17:43:50.100422Z\n" +
                "23.3\t2016-06-13T17:43:50.100423Z\n" +
                "1.0\t2016-06-13T17:43:50.100424Z\n" +
                "0.0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToGeoHash() throws Exception {
        testConversionToType("GEOHASH(7c)", "testCol\ttime\n" +
                "questdb\t2016-06-13T17:43:50.100416Z\n" +
                "\t2016-06-13T17:43:50.100417Z\n" +
                "\t2016-06-13T17:43:50.100427Z\n" +
                "\t2016-06-13T17:43:50.100429Z\n");
    }

    @Test
    public void testConversionToInt() throws Exception {
        testConversionToType("INT", "testCol\ttime\n" +
                "100\t2016-06-13T17:43:50.100418Z\n" +
                "-100\t2016-06-13T17:43:50.100419Z\n" +
                "1\t2016-06-13T17:43:50.100424Z\n" +
                "0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToLong() throws Exception {
        testConversionToType("LONG", "testCol\ttime\n" +
                "100\t2016-06-13T17:43:50.100418Z\n" +
                "-100\t2016-06-13T17:43:50.100419Z\n" +
                "1\t2016-06-13T17:43:50.100424Z\n" +
                "0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToLong256() throws Exception {
        testConversionToType("LONG256", "testCol\ttime\n" +
                "0x0150\t2016-06-13T17:43:50.100420Z\n"
        );
    }

    @Test
    public void testConversionToShort() throws Exception {
        testConversionToType("SHORT", "testCol\ttime\n" +
                "100\t2016-06-13T17:43:50.100418Z\n" +
                "-100\t2016-06-13T17:43:50.100419Z\n" +
                "1\t2016-06-13T17:43:50.100424Z\n" +
                "0\t2016-06-13T17:43:50.100425Z\n"
        );
    }

    @Test
    public void testConversionToString() throws Exception {
        testConversionToType("STRING", "testCol\ttime\n" +
                "questdb\t2016-06-13T17:43:50.100401Z\n" +
                "q\t2016-06-13T17:43:50.100402Z\n" +
                "\"questdbb\"\t2016-06-13T17:43:50.100403Z\n" +
                "\"q\"\t2016-06-13T17:43:50.100404Z\n" +
                "100i\t2016-06-13T17:43:50.100405Z\n" +
                "-100i\t2016-06-13T17:43:50.100406Z\n" +
                "0x100i\t2016-06-13T17:43:50.100407Z\n" +
                "123\t2016-06-13T17:43:50.100408Z\n" +
                "-54\t2016-06-13T17:43:50.100409Z\n" +
                "23.3\t2016-06-13T17:43:50.100410Z\n" +
                "T\t2016-06-13T17:43:50.100411Z\n" +
                "false\t2016-06-13T17:43:50.100412Z\n" +
                "1465839830101500200t\t2016-06-13T17:43:50.100413Z\n" +
                "questdbb\t2016-06-13T17:43:50.100416Z\n" +
                "q\t2016-06-13T17:43:50.100417Z\n" +
                "11111111-1111-1111-1111-111111111111\t2016-06-13T17:43:50.100427Z\n" +
                "22.6.4.2\t2016-06-13T17:43:50.100428Z\n" +
                "1.1.1.1\t2016-06-13T17:43:50.100429Z\n"
        );
    }

    @Test
    public void testConversionToSymbol() throws Exception {
        testConversionToType("SYMBOL", "testCol\ttime\n" +
                "questdb\t2016-06-13T17:43:50.100401Z\n" +
                "q\t2016-06-13T17:43:50.100402Z\n" +
                (stringAsTagSupported
                        ? "\"questdbb\"\t2016-06-13T17:43:50.100403Z\n" +
                        "\"q\"\t2016-06-13T17:43:50.100404Z\n"
                        : "") +
                "100i\t2016-06-13T17:43:50.100405Z\n" +
                "-100i\t2016-06-13T17:43:50.100406Z\n" +
                "0x100i\t2016-06-13T17:43:50.100407Z\n" +
                "123\t2016-06-13T17:43:50.100408Z\n" +
                "-54\t2016-06-13T17:43:50.100409Z\n" +
                "23.3\t2016-06-13T17:43:50.100410Z\n" +
                "T\t2016-06-13T17:43:50.100411Z\n" +
                "false\t2016-06-13T17:43:50.100412Z\n" +
                "1465839830101500200t\t2016-06-13T17:43:50.100413Z\n" +
                "questdbb\t2016-06-13T17:43:50.100416Z\n" +
                "q\t2016-06-13T17:43:50.100417Z\n" +
                "100\t2016-06-13T17:43:50.100418Z\n" +
                "-100\t2016-06-13T17:43:50.100419Z\n" +
                "0x0150\t2016-06-13T17:43:50.100420Z\n" +
                "123\t2016-06-13T17:43:50.100421Z\n" +
                "-54\t2016-06-13T17:43:50.100422Z\n" +
                "23.3\t2016-06-13T17:43:50.100423Z\n" +
                "T\t2016-06-13T17:43:50.100424Z\n" +
                "false\t2016-06-13T17:43:50.100425Z\n" +
                "1465839830101500\t2016-06-13T17:43:50.100426Z\n" +
                "11111111-1111-1111-1111-111111111111\t2016-06-13T17:43:50.100427Z\n" +
                "22.6.4.2\t2016-06-13T17:43:50.100428Z\n" +
                "1.1.1.1\t2016-06-13T17:43:50.100429Z\n"
        );
    }

    @Test
    public void testConversionToTimestamp() throws Exception {
        testConversionToType("TIMESTAMP", "testCol\ttime\n" +
                "1970-01-01T00:00:00.000100Z\t2016-06-13T17:43:50.100418Z\n" +
                "1969-12-31T23:59:59.999900Z\t2016-06-13T17:43:50.100419Z\n" +
                "2016-06-13T17:43:50.101500Z\t2016-06-13T17:43:50.100426Z\n"
        );
    }

    @Test
    public void testConversionToUuid() throws Exception {
        testConversionToType("UUID", "testCol\ttime\n" +
                "11111111-1111-1111-1111-111111111111\t2016-06-13T17:43:50.100427Z\n"
        );
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private long nextTime() {
        return time += 1000;
    }

    private void resetTime() {
        time = 1465839830100400200L;
    }

    private void testConversion(String table, String createTableCmd, String input, String expected) throws Exception {
        runInContext(() -> {
            execute(createTableCmd);
            recvBuffer = input;
            do {
                handleContextIO0();
                Assert.assertFalse(disconnected);
            } while (!recvBuffer.isEmpty());
            closeContext();
            mayDrainWalQueue();
            assertTable(expected, table);
            if (walEnabled) {
                Assert.assertTrue(isWalTable(table));
            }
        });
    }

    private void testConversionToType(String type, String expected) throws Exception {
        resetTime();
        String table = "convTest";
        testConversion(
                table,
                "create table " + table + " (testCol " + type + ", time TIMESTAMP) timestamp(time) partition by day" + (walEnabled ? " WAL;" : ";"),
                table + ",testCol=questdb " + nextTime() + "\n" +
                        table + ",testCol=q " + nextTime() + "\n" +
                        table + ",testCol=\"questdbb\" " + nextTime() + "\n" +
                        table + ",testCol=\"q\" " + nextTime() + "\n" +
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
                        table + " testCol=q " + nextTime() + "\n" +
                        table + " testCol=\"questdbb\" " + nextTime() + "\n" +
                        table + " testCol=\"q\" " + nextTime() + "\n" +
                        table + " testCol=100i " + nextTime() + "\n" +
                        table + " testCol=-100i " + nextTime() + "\n" +
                        table + " testCol=0x0150i " + nextTime() + "\n" +
                        table + " testCol=123 " + nextTime() + "\n" +
                        table + " testCol=-54 " + nextTime() + "\n" +
                        table + " testCol=23.3 " + nextTime() + "\n" +
                        table + " testCol=T " + nextTime() + "\n" +
                        table + " testCol=false " + nextTime() + "\n" +
                        table + " testCol=1465839830101500t " + nextTime() + "\n" +
                        table + " testCol=\"11111111-1111-1111-1111-111111111111\" " + nextTime() + "\n" +
                        table + ",testCol=22.6.4.2 " + nextTime() + "\n" +
                        table + " testCol=\"1.1.1.1\" " + nextTime() + "\n",
                expected
        );
    }
}
