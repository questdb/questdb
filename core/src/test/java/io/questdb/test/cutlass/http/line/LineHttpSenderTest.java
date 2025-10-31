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

package io.questdb.test.cutlass.http.line;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import io.questdb.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.cutlass.line.http.LineHttpSenderV2;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Array;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.PropertyKey.LINE_HTTP_ENABLED;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.test.AbstractCairoTest.parseFloorPartialTimestamp;
import static org.junit.Assert.assertEquals;

public class LineHttpSenderTest extends AbstractBootstrapTest {

    public static <T> T createDoubleArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.DOUBLE, shape, 0, indices);
    }

    public static <T> T createLongArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.LONG, shape, 0, indices);
    }

    public void assertSql(CairoEngine engine, CharSequence sql, CharSequence expectedResult) throws SqlException {
        StringSink sink = Misc.getThreadLocalSink();
        engine.print(sql, sink);
        if (!Chars.equals(sink, expectedResult)) {
            Assert.assertEquals(expectedResult, sink);
        }
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAddressWithNoPort() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation),
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "0.0.0.0:9000"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                Assert.assertEquals(9000, httpPort); // sanity check

                int totalCount = 100;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost").build()) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("tab")
                                .symbol("tag1", "value" + i % 10)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    sender.flush();
                }
                serverMain.awaitTable("tab");
                serverMain.assertSql("select count() from tab", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testAppendErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.execute("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, u uuid, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .stringColumn("u", "foo")
                            .at(1233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: u; cast error from protocol type: STRING to column type: UUID"
                    );
                    sender.reset();

                    sender.table("ex_tbl")
                            .doubleColumn("b", 1234)
                            .at(1233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; cast error from protocol type: FLOAT to column type: BYTE"
                    );
                    sender.reset();

                    sender.table("ex_tbl")
                            .longColumn("b", 1024)
                            .at(1233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; line protocol value: 1024 is out bounds of column type: BYTE"
                    );
                    sender.reset();

                    sender.table("ex_tbl")
                            .doubleColumn("i", 1024.2)
                            .at(1233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: i; cast error from protocol type: FLOAT to column type: INT"
                    );
                    sender.reset();

                    sender.table("ex_tbl")
                            .doubleColumn("str", 1024.2)
                            .at(1233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: str; cast error from protocol type: FLOAT to column type: STRING"
                    );
                }
            }
        });
    }

    @Test
    public void testArrayClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(2, 2)) {
                    // Partially fill array
                    array.append(1.0).append(2.0);

                    // Clear should reset append position
                    array.clear();

                    // Now fill from beginning
                    array.append(10.0).append(20.0).append(30.0).append(40.0);

                    String tableName = "clear_test";
                    int port = serverMain.getHttpServerPort();
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    // Verify clear() worked - should contain [10,20,30,40] not [1,2,30,40]
                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [[10.0,20.0],[30.0,40.0]]
                                    """);
                }
            }
        });
    }

    @Test
    public void testArrayConstructorValidation() {
        // Test negative dimensions in constructor
        try {
            new DoubleArray(-1, 5).close();
            Assert.fail("Should throw LineSenderException for negative dimension");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("dimension length must not be negative"));
        }

        // Test empty shape
        try {
            new DoubleArray().close();
            Assert.fail("Should throw LineSenderException for empty shape");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Shape must have at least one dimension"));
        }

        // Test dimension exceeding DIM_MAX_LEN
        int maxDim = (1 << 28) - 1; // ArrayView.DIM_MAX_LEN
        try {
            new DoubleArray(maxDim + 1).close();
            Assert.fail("Should throw LineSenderException for dimension exceeding max length");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("dimension length out of range"));
        }

        // Test too many dimensions (more than 32)
        int[] tooManyDims = new int[33];
        for (int i = 0; i < 33; i++) {
            tooManyDims[i] = 2;
        }
        try {
            new DoubleArray(tooManyDims).close();
            Assert.fail("Should throw LineSenderException for too many dimensions");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Maximum supported dimensionality is 32D"));
        }
    }

    @Test
    public void testArrayReshape2D() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(1, 12)) {
                    // fill with initial data
                    for (int i = 0; i < 12; i++) {
                        array.append(i + 1.0);
                    }

                    // reshape using the 2D method overload
                    array.reshape(3, 4);

                    String tableName = "reshape_2d_test";
                    int port = serverMain.getHttpServerPort();
                    try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                            .address("localhost:" + port)
                            .build()) {

                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [[1.0,2.0,3.0,4.0],[5.0,6.0,7.0,8.0],[9.0,10.0,11.0,12.0]]
                                    """);
                }
            }
        });
    }

    @Test
    public void testArrayReshape2DErrors() {
        try (DoubleArray array = new DoubleArray(2, 2)) {
            array.close();
            try {
                array.reshape(2, 3);
                Assert.fail("Should throw LineSenderException when reshaping closed array");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Cannot reshape a closed array"));
            }
        }

        try (DoubleArray array = new DoubleArray(2, 2)) {
            try {
                array.reshape(-1, 3);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions must not be negative"));
            }

            try {
                array.reshape(3, -2);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions must not be negative"));
            }
        }
    }

    @Test
    public void testArrayReshape3D() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(24)) {
                    for (int i = 0; i < 24; i++) {
                        array.append(i + 1.0);
                    }

                    // reshape using the 3D method overload
                    array.reshape(2, 3, 4);

                    String tableName = "reshape_3d_test";
                    int port = serverMain.getHttpServerPort();
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [[[1.0,2.0,3.0,4.0],[5.0,6.0,7.0,8.0],[9.0,10.0,11.0,12.0]],[[13.0,14.0,15.0,16.0],[17.0,18.0,19.0,20.0],[21.0,22.0,23.0,24.0]]]
                                    """);
                }
            }
        });
    }

    @Test
    public void testArrayReshape3DErrors() {
        try (DoubleArray array = new DoubleArray(2, 2, 2)) {
            array.close();
            try {
                array.reshape(2, 3, 4);
                Assert.fail("Should throw LineSenderException when reshaping closed array");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Cannot reshape a closed array"));
            }
        }

        try (DoubleArray array = new DoubleArray(2, 2, 2)) {
            try {
                array.reshape(-1, 3, 4);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions must not be negative"));
            }

            try {
                array.reshape(2, -3, 4);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions must not be negative"));
            }

            try {
                array.reshape(2, 3, -4);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions must not be negative"));
            }
        }
    }

    @Test
    public void testArrayReshapeDimensionLimits() {
        try (DoubleArray array = new DoubleArray(2, 2)) {
            // Test dimension exceeding DIM_MAX_LEN for reshape(int)
            int maxDim = (1 << 28) - 1; // ArrayView.DIM_MAX_LEN
            try {
                array.reshape(maxDim + 1);
                Assert.fail("Should throw LineSenderException for dimension exceeding max length");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array size out of range"));
            }

            // Test dimension exceeding DIM_MAX_LEN for reshape(int, int)
            try {
                array.reshape(100, maxDim + 1);
                Assert.fail("Should throw LineSenderException for dimension exceeding max length");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions out of range"));
            }

            // Test dimension exceeding DIM_MAX_LEN for reshape(int, int, int)
            try {
                array.reshape(10, 10, maxDim + 1);
                Assert.fail("Should throw LineSenderException for dimension exceeding max length");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array dimensions out of range"));
            }

            // Test empty shape for varargs reshape
            try {
                //noinspection RedundantArrayCreation
                array.reshape(new int[0]);
                Assert.fail("Should throw LineSenderException for empty shape");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Shape must have at least one dimension"));
            }

            // Test too many dimensions
            int[] tooManyDims = new int[33];
            for (int i = 0; i < 33; i++) {
                tooManyDims[i] = 2;
            }
            try {
                array.reshape(tooManyDims);
                Assert.fail("Should throw LineSenderException for too many dimensions");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Maximum supported dimensionality is 32D"));
            }
        }
    }

    @Test
    public void testArrayReshapeErrors() {
        try (DoubleArray array = new DoubleArray(2, 2)) {
            array.close();
            try {
                array.reshape(4);
                Assert.fail("Should throw LineSenderException when reshaping closed array");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Cannot reshape a closed array"));
            }
        }

        try (DoubleArray array = new DoubleArray(2, 2)) {
            try {
                array.reshape(-1);
                Assert.fail("Should throw LineSenderException for negative dimension");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Array size must not be negative"));
            }
        }
    }

    @Test
    public void testArrayReshapeSingleDimension() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(3, 3, 3)) {
                    // Fill with initial data
                    for (int i = 0; i < 27; i++) {
                        array.append(i + 1.0);
                    }

                    // Reshape to single dimension
                    array.reshape(27);

                    String tableName = "reshape_1d_test";
                    int port = serverMain.getHttpServerPort();
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    // Verify 1D array data (27 elements)
                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0]
                                    """);
                }
            }
        });
    }

    @Test
    public void testArrayReshapeVarargs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(2, 3)) {
                    array.append(1.0).append(2.0).append(3.0)
                            .append(4.0).append(5.0).append(6.0);

                    // reshape to 1D
                    array.reshape(6);

                    String tableName = "reshape_test";
                    int port = serverMain.getHttpServerPort();
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [1.0,2.0,3.0,4.0,5.0,6.0]
                                    """);

                    // Reshape to 3D and refill
                    array.reshape(1, 2, 3);
                    array.clear(); // Reset position
                    array.append(7.0).append(8.0).append(9.0)
                            .append(10.0).append(11.0).append(12.0);

                    String tableName2 = "reshape_test2";
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName2)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-23"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName2, 1);

                    // Verify 3D array data
                    serverMain.assertSql("SELECT arr FROM " + tableName2,
                            """
                                    arr
                                    [[[7.0,8.0,9.0],[10.0,11.0,12.0]]]
                                    """);
                }
            }
        });
    }

    @Test
    public void testArrayReshapeWithNDim() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (DoubleArray array = new DoubleArray(4, 4)) {
                    // Fill with initial data
                    for (int i = 0; i < 16; i++) {
                        array.append(i + 1.0);
                    }

                    int[] shape = {2, 8};
                    array.reshape(shape);

                    String tableName = "reshape_ndim_test";
                    int port = serverMain.getHttpServerPort();
                    try (
                            Sender sender = Sender.builder(Sender.Transport.HTTP)
                                    .address("localhost:" + port)
                                    .build()
                    ) {
                        sender.table(tableName)
                                .doubleArray("arr", array)
                                .at(parseFloorPartialTimestamp("2023-02-22"), ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTxn(tableName, 1);

                    // Verify 2D array data (2x8 shape)
                    serverMain.assertSql("SELECT arr FROM " + tableName,
                            """
                                    arr
                                    [[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0],[9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0]]
                                    """);
                }
            }
        });
    }

    @Test
    public void testAutoCreationArrayType() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_auto_creation_test";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we'll flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    double[] arr = createDoubleArray(1);
                    sender.table(tableName)
                            .doubleArray("arr", arr)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTxn(tableName, 1);
                    serverMain.assertSql(
                            "show create table " + tableName,
                            """
                                    ddl
                                    CREATE TABLE 'arr_auto_creation_test' (\s
                                    \tarr DOUBLE[],
                                    \ttimestamp TIMESTAMP
                                    ) timestamp(timestamp) PARTITION BY DAY WAL
                                    WITH maxUncommittedRows=500000, o3MaxLag=600000000us;
                                    """);
                }
            }
        });
    }

    @Test
    public void testAutoDetectMaxNameLen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .maxNameLength(20)
                        .build()
                ) {
                    sender.table("table_with_long________________________________name")
                            .longColumn("column_with_long________________________________name", 1)
                            .at(100000L, ChronoUnit.MICROS);
                }
                serverMain.awaitTable("table_with_long________________________________name");
                serverMain.assertSql("select * from 'table_with_long________________________________name'",
                        """
                                column_with_long________________________________name\ttimestamp
                                1\t1970-01-01T00:00:00.100000Z
                                """);
            }
        });
    }

    @Test
    public void testAutoFlush() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 50_000;
                int autoFlushRows = 1000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 127, 0, 1_000, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        if (i != 0 && i % autoFlushRows == 0) {
                            serverMain.awaitTable("table with space");
                            serverMain.assertSql("select count() from 'table with space'", "count\n" +
                                    i + "\n");
                        }
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    serverMain.awaitTable("table with space");
                    serverMain.assertSql("select count() from 'table with space'", "count\n" +
                            totalCount + "\n");
                }
            }
        });
    }

    @Test
    public void testCanSendLong256ViaString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.execute("create table foo (ts TIMESTAMP, d LONG256) timestamp(ts) partition by day wal;");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("foo")
                            .stringColumn("d", "0xAB")
                            .at(1233456L, ChronoUnit.NANOS);
                    sender.flush();

                    serverMain.awaitTxn("foo", 1);
                    serverMain.assertSql("foo;",
                            """
                                    ts\td
                                    1970-01-01T00:00:00.001233Z\t0xab
                                    """);

                    sender.table("foo")
                            .stringColumn("d", "0xABC")
                            .at(1233456L, ChronoUnit.NANOS);

                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: foo, column: d; cast error from protocol type: STRING to column type: LONG256"
                    );
                }
            }
        });
    }

    @Test
    public void testCancelRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                        .protocolVersion(PROTOCOL_VERSION_V1)
                        .build()) {

                    sender.cancelRow(); // this should be no-op

                    // this row should be inserted
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 3)
                            .at(Instant.parse("2024-09-09T14:38:26.361110Z"));

                    // this one is cancelled
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 1);
                    sender.cancelRow();

                    // and this one should be inserted again
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 2)
                            .at(Instant.parse("2024-09-09T14:28:26.361110Z"));

                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT * FROM h2o_feet",
                        """
                                async\twater_level\ttimestamp
                                true\t2.0\t2024-09-09T14:28:26.361110Z
                                true\t3.0\t2024-09-09T14:38:26.361110Z
                                """);
            }
        });
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV1() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV2() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123123123Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV1() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z"), ChronoUnit.MICROS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV2() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z"), ChronoUnit.MICROS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV1() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z") / 1000, ChronoUnit.MILLIS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV2() throws Exception {
        testCreateTimestampColumns(MicrosTimestampDriver.floor("2025-11-20T10:55:24.123456Z") / 1000, ChronoUnit.MILLIS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV1() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V1,
                new int[]{ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP},
                "1.111\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.123456Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV2() throws Exception {
        testCreateTimestampColumns(NanosTimestampDriver.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V2,
                new int[]{ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO},
                "1.111\t2025-11-19T10:55:24.123456789Z\t2025-11-19T10:55:24.123456Z\t2025-11-19T10:55:24.123000Z\t2025-11-19T10:55:24.123456799Z\t2025-11-20T10:55:24.123456789Z");
    }

    @Test
    public void testDdlListenerOnTableCreated() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain questdb = startWithEnvVariables()) {
                final AtomicBoolean failed = new AtomicBoolean();
                final CairoEngine engine = questdb.getEngine();
                engine.setDdlListener(new DefaultDdlListener() {
                    @Override
                    public void onTableOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind) {
                        try {
                            // assert that this is the expected table name and id
                            final int tableId = (int) engine.getTableIdGenerator().getCurrentId();
                            Assert.assertEquals("tab", tableToken.getTableName());
                            Assert.assertEquals(tableId, tableToken.getTableId());

                            // assert that the table is not available to others for reading yet
                            Assert.assertNull(engine.getTableTokenIfExists("tab"));

                            // assert that others competing to create the same table, cannot lock the table name 
                            Assert.assertNull(engine.lockTableName("tab", tableId, false, true));
                        } catch (Throwable th) {
                            th.printStackTrace(System.out);
                            failed.set(true);
                        }
                    }
                });

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + questdb.getHttpServerPort())
                        .build()
                ) {
                    sender.table("tab").longColumn("col", 1).atNow();
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    Assert.fail("Failed to ingest data, check log for exception");
                }

                if (failed.get()) {
                    Assert.fail("Failed in DDL listener, check log for exception");
                }
            }
        });
    }

    @Test
    public void testEmptyArraysMultiDimensional() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1024"
            )) {
                String tableName = "empty_arrays_test";
                serverMain.execute("CREATE TABLE " + tableName +
                        " (a1 double[], a2 double[][], a3 double[][][], ts TIMESTAMP)" +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    // Test various empty array shapes
                    double[] empty1D = createDoubleArray(0);
                    double[][] empty2D = createDoubleArray(0, 0);
                    double[][][] empty3D = createDoubleArray(0, 0, 0);

                    sender.table(tableName)
                            .doubleArray("a1", empty1D)
                            .doubleArray("a2", empty2D)
                            .doubleArray("a3", empty3D)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Test 2D array with first dimension non-zero but second dimension zero
                    double[][] partial2D = createDoubleArray(3, 0);
                    sender.table(tableName)
                            .doubleArray("a1", empty1D)
                            .doubleArray("a2", partial2D)
                            .doubleArray("a3", empty3D)
                            .at(200000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Test 3D array with various zero dimensions
                    double[][][] partial3D_1 = createDoubleArray(2, 0, 0);
                    sender.table(tableName)
                            .doubleArray("a1", empty1D)
                            .doubleArray("a2", empty2D)
                            .doubleArray("a3", partial3D_1)
                            .at(300000000000L, ChronoUnit.MICROS);
                    double[][][] partial3D_2 = createDoubleArray(2, 3, 0);
                    sender.table(tableName)
                            .doubleArray("a1", empty1D)
                            .doubleArray("a2", empty2D)
                            .doubleArray("a3", partial3D_2)
                            .at(400000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 3);
                serverMain.assertSql("select * from " + tableName,
                        """
                                a1\ta2\ta3\tts
                                []\t[]\t[]\t1970-01-02T03:46:40.000000Z
                                []\t[]\t[]\t1970-01-03T07:33:20.000000Z
                                []\t[]\t[]\t1970-01-04T11:20:00.000000Z
                                []\t[]\t[]\t1970-01-05T15:06:40.000000Z
                                """);
            }
        });
    }

    @Test
    public void testEmptyDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "arr_double_test";
                serverMain.execute("CREATE TABLE " + tableName + " (a1 double[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    double[] arr1d = createDoubleArray(0);

                    sender.table(tableName)
                            .doubleArray("a1", arr1d)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("select * from " + tableName,
                        """
                                a1\tts
                                []\t1970-01-02T03:46:40.000000Z
                                """);
            }
        });
    }

    @Test
    public void testEsotericArrayShapes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1024"
            )) {
                String tableName = "esoteric_arrays_test";
                serverMain.execute("CREATE TABLE " + tableName + " (a1 double[][][], a2 double[][][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    // Test esoteric shape [1][0][1] - should be treated as [1][0][0]
                    double[][][] esoteric1 = new double[1][0][1];

                    // Test shape [2][0][5] - should be treated as [2][0][0]
                    double[][][] esoteric2 = new double[2][0][5];

                    sender.table(tableName)
                            .doubleArray("a1", esoteric1)
                            .doubleArray("a2", esoteric2)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Test mixed zero/non-zero dimensions
                    double[][][] mixed1 = createDoubleArray(1, 0, 0);
                    double[][][] mixed2 = createDoubleArray(3, 0, 0);

                    sender.table(tableName)
                            .doubleArray("a1", mixed1)
                            .doubleArray("a2", mixed2)
                            .at(200000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("select * from " + tableName,
                        """
                                a1\ta2\tts
                                []\t[]\t1970-01-02T03:46:40.000000Z
                                []\t[]\t1970-01-03T07:33:20.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFlushAfterTimeout() throws Exception {
        // this is a regression test
        // there was a bug that flushes due to interval did not increase row count
        // bug scenario:
        // 1. flush to empty the local buffer
        // 2. period of inactivity
        // 3. insert a new row. this should trigger a flush due to interval flush. but it did not increase the row count so it stayed 0
        // 4. the flush() saw rowCount = 0 so it acted as noop
        // 5. every subsequent insert would trigger a flush due to interval (since the previous flush() was a noop and did not reset the timer)  but rowCount would stay 0
        // 6. nothing would be flushed and rows would keep accumulating in the buffer
        // 6. eventually the HTTP buffer would grow up to the limit and throw an exception

        // this test is to make sure that the scenario above does not happen
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                String confString = "http::addr=localhost:" + httpPort + ";auto_flush_rows=1;auto_flush_interval=1;";
                try (Sender sender = Sender.fromConfig(confString)) {

                    // insert a row to trigger row-based flush and reset the interval timer
                    sender.table("table")
                            .symbol("tag1", "value")
                            .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                            .atNow();

                    // wait a bit so the next insert triggers interval flush
                    Os.sleep(100);

                    // insert more rows
                    for (int i = 0; i < 9; i++) {
                        sender.table("table")
                                .symbol("tag1", "value")
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }

                    serverMain.awaitTable("table");
                    serverMain.assertSql("select count() from 'table'", """
                            count
                            10
                            """);
                }
            }
        });
    }

    @Test
    public void testHttpWithDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100;
                int autoFlushRows = 1000;
                String tableName = "accounts";

                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 127, 0, 1_000, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        // Add new symbol column with each second row
                        sender.table(tableName)
                                .symbol("balance" + i / 2, String.valueOf(i))
                                .atNow();

                        sender.flush();
                    }
                }

                for (int i = 0; i < 10; i++) {
                    serverMain.execute("drop table " + tableName);
                    assertSql(serverMain.getEngine(), "SELECT count() from tables() where table_name='" + tableName + "'", "count\n0\n");
                    serverMain.execute("create table " + tableName + " (" +
                            "balance1 symbol capacity 16, " +
                            "balance10 symbol capacity 16, " +
                            "timestamp timestamp)" +
                            " timestamp(timestamp) partition by DAY WAL " +
                            " dedup upsert keys (balance1, balance10, timestamp)");
                    assertSql(serverMain.getEngine(), "SELECT count() FROM (table_columns('Accounts')) WHERE upsertKey=true AND ( column = 'timestamp' )", """
                            count
                            1
                            """);
                    Os.sleep(10);
                }
            }
        });
    }

    @Test
    public void testIlpWithHttpContextPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), "context1"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertBinaryToOtherColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "binary_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, y varchar, a1 DOUBLE," +
                        " ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    // insert binary double to symbol column
                    sender.table(tableName)
                            .stringColumn("y", "ystr")
                            .doubleColumn("x", 9991.0)
                            .doubleColumn("a1", 1)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTxn(tableName, 1);
                    serverMain.assertSql("select * from " + tableName,
                            """
                                    x\ty\ta1\tts
                                    9991.0\tystr\t1.0\t1970-01-02T03:46:40.000000Z
                                    """);

                    // insert binary double to string column
                    try {
                        sender.table(tableName)
                                .symbol("x", "x1")
                                .doubleColumn("y", 9999.0)
                                .doubleColumn("a1", 1)
                                .at(100000000000L, ChronoUnit.MICROS);
                        sender.flush();
                        Assert.fail();
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), " cast error from protocol type: FLOAT to column type: VARCHAR");
                    }
                    sender.reset();

                    // insert string column to double
                    try {
                        sender.table(tableName)
                                .symbol("x", "x1")
                                .stringColumn("y", "ystr")
                                .stringColumn("a1", "11.u")
                                .at(100000000000L, ChronoUnit.MICROS);
                        sender.flush();
                        Assert.fail();
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), " cast error from protocol type: STRING to column type: DOUBLE");
                    }
                    sender.reset();

                    // insert array column to double
                    try {
                        sender.table(tableName)
                                .symbol("x", "x1")
                                .stringColumn("y", "ystr")
                                .doubleArray("a1", new double[]{1.0, 2.0})
                                .at(100000000000L, ChronoUnit.MICROS);
                        sender.flush();
                        Assert.fail();
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), " cast error from protocol type: ARRAY to column type: DOUBLE");
                    }
                }

                // send text double to symbol column
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V1)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .stringColumn("y", "ystr")
                            .doubleColumn("x", 9999.0)
                            .doubleColumn("a1", 1)
                            .at(100000000001L, ChronoUnit.MICROS);

                    try (DirectUtf8Sink sink = new DirectUtf8Sink(128)) {
                        for (int i = 0; i < 10; i++) {
                            sink.clear();
                            double v = 10000 + i;
                            sink.put(tableName).put(' ').put("x=");
                            if (i % 2 == 0) {
                                Numbers.append(sink, v);
                            } else {
                                sink.put('=');
                                sink.putAny((byte) 16);
                                sink.putDouble(v);
                            }
                            double a2 = 2 + i;
                            sink.put(",y=\"ystr\",a1=");
                            if (i % 2 != 0) {
                                Numbers.append(sink, a2);
                            } else {
                                sink.put('=');
                                sink.putAny((byte) 16);
                                sink.putDouble(a2);
                            }
                            long ts = 100000000002000L + i * 1000;
                            sink.put(" ").put(ts).put('\n');
                            ((AbstractLineHttpSender) sender).putRawMessage(sink);
                        }
                    }

                    sender.flush();
                    serverMain.awaitTxn(tableName, 2);

                    serverMain.assertSql("select * from " + tableName,
                            """
                                    x\ty\ta1\tts
                                    9991.0\tystr\t1.0\t1970-01-02T03:46:40.000000Z
                                    9999.0\tystr\t1.0\t1970-01-02T03:46:40.000001Z
                                    10000.0\tystr\t2.0\t1970-01-02T03:46:40.000002Z
                                    10001.0\tystr\t3.0\t1970-01-02T03:46:40.000003Z
                                    10002.0\tystr\t4.0\t1970-01-02T03:46:40.000004Z
                                    10003.0\tystr\t5.0\t1970-01-02T03:46:40.000005Z
                                    10004.0\tystr\t6.0\t1970-01-02T03:46:40.000006Z
                                    10005.0\tystr\t7.0\t1970-01-02T03:46:40.000007Z
                                    10006.0\tystr\t8.0\t1970-01-02T03:46:40.000008Z
                                    10007.0\tystr\t9.0\t1970-01-02T03:46:40.000009Z
                                    10008.0\tystr\t10.0\t1970-01-02T03:46:40.000010Z
                                    10009.0\tystr\t11.0\t1970-01-02T03:46:40.000011Z
                                    """);
                }
            }
        });
    }

    @Test
    public void testInsertDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "arr_double_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a1 DOUBLE[][][], a2 DOUBLE[][][], a3 DOUBLE[][][], a4 DOUBLE[][][], " +
                        "b1 DOUBLE[], b2 DOUBLE[][], b3 DOUBLE[][][], " +
                        "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build();
                     DoubleArray a1 = new DoubleArray(2, 2, 2);
                     DoubleArray a2 = new DoubleArray(2, 2, 2).set(99.0, 0, 1, 0).set(100.0, 1, 1, 1);
                     DoubleArray a3 = new DoubleArray(2, 2, 2).setAll(101);
                     DoubleArray a4 = new DoubleArray(100_000_000, 100_000_000, 0).setAll(0)
                ) {
                    // array.append() appends in a circular fashion, wrapping around to start from the end.
                    // We deliberately append two more than the length of the array, to test this behavior.
                    // The intended use is to fill it up exactly, then for the next row just continue
                    // filling up with new data.
                    for (int i = 0; i < 10; i++) {
                        a1.append(i);
                    }
                    double[] arr1d = createDoubleArray(5);
                    double[][] arr2d = createDoubleArray(2, 3);
                    double[][][] arr3d = createDoubleArray(1, 2, 3);

                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                            .longColumn("l1", 23452345)
                            .doubleArray("a1", a1)
                            .doubleArray("a2", a2)
                            .doubleArray("a3", a3)
                            .doubleArray("a4", a4)
                            .doubleArray("b1", arr1d)
                            .doubleArray("b2", arr2d)
                            .doubleArray("b3", arr3d)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("select * from " + tableName, """
                        x\ty\tl1\ta1\ta2\ta3\ta4\tb1\tb2\tb3\tts
                        42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t\
                        [[[8.0,9.0],[2.0,3.0]],[[4.0,5.0],[6.0,7.0]]]\t\
                        [[[0.0,0.0],[99.0,0.0]],[[0.0,0.0],[0.0,100.0]]]\t\
                        [[[101.0,101.0],[101.0,101.0]],[[101.0,101.0],[101.0,101.0]]]\t\
                        []\t\
                        [1.0,2.0,3.0,4.0,5.0]\t\
                        [[1.0,2.0,3.0],[2.0,4.0,6.0]]\t\
                        [[[1.0,2.0,3.0],[2.0,4.0,6.0]]]\t\
                        1970-01-02T03:46:40.000000Z
                        """);
            }
        });
    }

    @Test
    public void testInsertInvalidArrayDims() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_exception_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, a1 DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .autoFlushRows(Integer.MAX_VALUE)
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .doubleArray("a1", (double[][]) createDoubleArray(2, 2))
                            .at(100000000000L, ChronoUnit.MICROS);
                    flushAndAssertError(
                            sender,
                            "ast error from protocol type: DOUBLE[][] to column type: DOUBLE[]");
                }

            }
        });
    }

    @Test
    public void testInsertLargeArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_large_test";
                serverMain.execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, arr DOUBLE[])" +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we'll flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    double[] arr = createDoubleArray(10_000_000);
                    sender.table(tableName)
                            .doubleArray("arr", arr)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql(
                        "SELECT ts, arr[1] arr0, arr[10_000] arr4, arr[1_000_000] arr6, arr[10_000_000] arr7" +
                                " FROM " + tableName,
                        """
                                ts\tarr0\tarr4\tarr6\tarr7
                                1970-01-02T03:46:40.000000Z\t1.0\t10000.0\t1000000.0\t1.0E7
                                """);
            }
        });
    }

    @Test
    @Ignore("as of now only double arrays are supported")
    public void testInsertLongArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "arr_long_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a1 LONG[][][], a2 LONG[][][], a3 LONG[][][], a4 LONG[][][], " +
                        "b1 LONG[], b2 LONG[][], b3 LONG[][][], " +
                        "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build();
                     LongArray a1 = new LongArray(2, 2, 2);
                     LongArray a2 = new LongArray(2, 2, 2).set(99L, 0, 1, 0).set(100L, 1, 1, 1);
                     LongArray a3 = new LongArray(2, 2, 2).setAll(101);
                     LongArray a4 = new LongArray(100_000_000, 100_000_000, 0)
                ) {
                    // array.append() appends in a circular fashion, wrapping around to start from the end.
                    // We deliberately append two more than the length of the array, to test this behavior.
                    // The intended use is to fill it up exactly, then for the next row just continue
                    // filling up with new data.
                    for (int i = 0; i < 10; i++) {
                        a1.append(i);
                    }

                    long[] arr1d = createLongArray(5);
                    long[][] arr2d = createLongArray(2, 3);
                    long[][][] arr3d = createLongArray(1, 2, 3);
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")  // ensuring no array parsing for symbol
                            .longColumn("l1", 23452345)
                            .longArray("a1", a1)
                            .longArray("a2", a2)
                            .longArray("a3", a3)
                            .longArray("a4", a4)
                            .longArray("b1", arr1d)
                            .longArray("b2", arr2d)
                            .longArray("b3", arr3d)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql("select * from " + tableName, """
                        x\ty\tl1\ta1\ta2\ta3\ta4\tb1\tb2\tb3\tts
                        42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t\
                        [[[8,9],[2,3]],[[4,5],[6,7]]]\t\
                        [[[0,0],[99,0]],[[0,0],[0,100]]]\t\
                        [[[101,101],[101,101]],[[101,101],[101,101]]]\t\
                        []\t\
                        [1,2,3,4,5]\t\
                        [[1,2,3],[2,4,6]]\t\
                        [[[1,2,3],[2,4,6]]]\t\
                        1970-01-02T03:46:40.000000Z
                        """);
            }
        });
    }

    @Test
    public void testInsertNullArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "arr_nullable_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, l1 LONG, a1 DOUBLE[], " +
                        "a2 DOUBLE[][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .protocolVersion(PROTOCOL_VERSION_V2)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .longColumn("l1", 123098948)
                            .doubleArray("a1", (double[]) null)
                            .doubleArray("a2", (double[][]) null)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);

                serverMain.assertSql("select * from " + tableName,
                        """
                                x\tl1\ta1\ta2\tts
                                42i\t123098948\tnull\tnull\t1970-01-02T03:46:40.000000Z
                                """);
            }
        });
    }

    @Test
    public void testInsertSimpleDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                String tableName = "simple_double_test";
                serverMain.execute("CREATE TABLE " + tableName + " (x SYMBOL, y SYMBOL, l1 LONG, " +
                        "a double, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.awaitTxn(tableName, 0);

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .retryTimeoutMillis(0)
                        .build()
                ) {
                    sender.table(tableName)
                            .symbol("x", "42i")
                            .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]")
                            .longColumn("l1", 23452345)
                            .doubleColumn("a", 1.0)
                            .at(100000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("select * from " + tableName, """
                        x\ty\tl1\ta\tts
                        42i\t[6f1.0,2.5,3.0,4.5,5.0]\t23452345\t1.0\t1970-01-02T03:46:40.000000Z
                        """);
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpServerKeepAliveOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            )) {
                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp_varcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                String tableName = "h2o_feet";
                serverMain.execute("create table " + tableName + " (async symbol, location symbol, level varchar, water_level long, ts timestamp) timestamp(ts) partition by DAY WAL");

                int count = 10;

                String fullString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .build()
                ) {
                    for (int i = 0; i < count; i++) {
                        sender.table(tableName)
                                .symbol("async", "true")
                                .symbol("location", "santa_monica")
                                .stringColumn("level", fullString.substring(0, i % 10 + 10))
                                .longColumn("water_level", i)
                                .atNow();
                    }
                    sender.flush();
                }
                StringBuilder expectedString = new StringBuilder("level\n");
                for (int i = 0; i < count; i++) {
                    expectedString.append(fullString, 0, i % 10 + 10).append('\n');
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT level FROM h2o_feet", expectedString.toString());
            }
        });
    }

    @Test
    public void testLineHttpDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    LINE_HTTP_ENABLED.getEnvVarName(), "false"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 1_000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 127, 0, 1_000, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table")
                                .longColumn("lcol1", i)
                                .atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("Expected exception");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "http-status=405");
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: HTTP endpoint does not support ILP.");
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeDesignatedTimestampDoesNotRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .retryTimeoutMillis(Integer.MAX_VALUE) // high-enoung value so the test times out if retry is attempted
                        .build()
                ) {
                    sender.table("tab")
                            .longColumn("l", 1) // filler
                            .at(-1, ChronoUnit.MICROS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "error in line 1: table: tab, timestamp: -1; designated timestamp before 1970-01-01 is not allowed"
                    );
                }
            }
        });
    }

    @Test
    public void testRestrictedCreateColumnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false"
            )) {
                serverMain.execute("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "3")
                            .at(1222233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );
                    sender.reset();

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, cannot create table, creating new columns is disabled"
                    );
                }
            }
        });
    }

    @Test
    public void testRestrictedCreateTableError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false",
                    PropertyKey.LINE_AUTO_CREATE_NEW_TABLES.getEnvVarName(), "false"
            )) {
                serverMain.execute("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "2")
                            .at(1222233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );
                    sender.reset();

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456L, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, creating new tables is disabled"
                    );
                }
            }
        });
    }

    @Test
    public void testSmallMaxNameLen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost:1123")
                    .retryTimeoutMillis(Integer.MAX_VALUE)
                    .maxNameLength(20)
                    .protocolVersion(PROTOCOL_VERSION_V2)
                    .build()
            ) {
                try {
                    sender.table("table_with_long______________________name");
                    Assert.fail("Expected exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "table name is too long: [name = table_with_long______________________name, maxNameLength=20]");
                }

                try {
                    sender.table("table")
                            .doubleColumn("column_with_long______________________name", 1.0);
                    Assert.fail("Expected exception");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "column name is too long: [name = column_with_long______________________name, maxNameLength=20]");
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 300 + rnd.nextInt(100);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100_000;
                try (AbstractLineHttpSender sender = new LineHttpSenderV2("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 127, 0, 1_000, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .symbol("tag2", "value " + i % 10)
                                .stringColumn("scol1", "value" + i)
                                .stringColumn("scol2", "value" + i)
                                .longColumn("lcol 1", i)
                                .longColumn("lcol2", i)
                                .doubleColumn("dcol1", i)
                                .doubleColumn("dcol2", i)
                                .boolColumn("bcol1", i % 2 == 0)
                                .boolColumn("bcol2", i % 2 == 0)
                                .timestampColumn("tcol1", Instant.now())
                                .timestampColumn("tcol2", Instant.now())
                                .timestampColumn("tcol3", 1, ChronoUnit.HOURS)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    sender.flush();
                }
                serverMain.awaitTable("table with space");
                serverMain.assertSql("select count() from 'table with space'", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testTimestampIngestMicrosV1() throws Exception {
        testTimestampIngest("TIMESTAMP", PROTOCOL_VERSION_V1, """
                        ts\tdts
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                        2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                        """,
                null
        );
    }

    @Test
    public void testTimestampIngestMicrosV2() throws Exception {
        testTimestampIngest("TIMESTAMP", PROTOCOL_VERSION_V2, """
                ts\tdts
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                """, """
                ts\tdts
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834000Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123000Z\t2025-11-20T10:55:24.834129Z
                2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.834129Z
                2300-11-19T10:55:24.123456Z\t2300-11-20T10:55:24.834129Z
                2797-10-26T19:46:29.123456Z\t2797-10-26T19:46:30.123457Z
                """
        );
    }

    @Test
    public void testTimestampIngestNanosV1() throws Exception {
        testTimestampIngest("TIMESTAMP_NS", PROTOCOL_VERSION_V1, """
                        ts\tdts
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129092Z
                        """,
                null
        );
    }

    @Test
    public void testTimestampIngestNanosV2() throws Exception {
        testTimestampIngest("TIMESTAMP_NS", PROTOCOL_VERSION_V2, """
                        ts\tdts
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834000000Z
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129000Z
                        2025-11-19T10:55:24.123456789Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123000000Z	2025-11-20T10:55:24.834129082Z
                        2025-11-19T10:55:24.123456799Z	2025-11-20T10:55:24.834129092Z
                        """,
                null
        );
    }

    @Test
    public void testTimestampNSAutoCreateTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final String tab_ns = "tab_ns";
                final String tab_us = "tab_us";
                final int port = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    long ts_ns = NanosTimestampDriver.floor("2025-11-19T10:55:24.834129081Z");
                    long dts_ns = NanosTimestampDriver.floor("2025-11-20T10:55:24.834129082Z");
                    sender.table(tab_ns)
                            .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                            .at(dts_ns, ChronoUnit.NANOS);
                    sender.flush();
                    serverMain.awaitTable(tab_ns);
                    serverMain.assertSql(tab_ns, """
                            ts\ttimestamp
                            2025-11-19T10:55:24.834129081Z\t2025-11-20T10:55:24.834129082Z
                            """);

                    long ts_us = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123456Z");
                    long dts_us = MicrosTimestampDriver.floor("2025-11-20T10:55:24.234567Z");
                    sender.table(tab_us)
                            .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                            .at(dts_us, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTable(tab_us);
                    serverMain.assertSql(tab_us, """
                            ts\ttimestamp
                            2025-11-19T10:55:24.123456Z\t2025-11-20T10:55:24.234567Z
                            """);
                }

                final CairoEngine engine = serverMain.getEngine();

                final TableToken tt_ns = engine.verifyTableName(tab_ns);
                try (TableMetadata metadata = engine.getTableMetadata(tt_ns)) {
                    Assert.assertEquals(ColumnType.TIMESTAMP_NANO, metadata.getColumnType("ts"));
                    Assert.assertEquals(ColumnType.TIMESTAMP_NANO, metadata.getColumnType("timestamp"));
                }

                final TableToken tt_us = engine.verifyTableName(tab_us);
                try (TableMetadata metadata = engine.getTableMetadata(tt_us)) {
                    Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType("ts"));
                    Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType("timestamp"));
                }
            }
        });
    }

    @Test
    public void testTimestampNSOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.execute("create table tab (ts timestamp_ns, ts2 timestamp_ns) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    long max = NanosTimestampDriver.floor("2262-01-31 23:59:59.999999999");
                    sender.table("tab")
                            .timestampColumn("ts2", max, ChronoUnit.NANOS)
                            .at(max, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "designated timestamp overflow, max[9214646399999999999]"
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampNSUpperBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.execute("create table tab (ts timestamp_ns, ts2 timestamp_ns) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    long max = CommonUtils.MAX_TIMESTAMP;
                    sender.table("tab")
                            .timestampColumn("ts2", max, ChronoUnit.NANOS)
                            .at(max, ChronoUnit.NANOS);
                    sender.flush();
                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", """
                            ts\tts2
                            2261-12-31T23:59:59.999999999Z\t2261-12-31T23:59:59.999999999Z
                            """);

                    Instant nonDsInstant = Instant.ofEpochSecond(max / 1_000_000_000, max % 1_000_000_000);
                    Instant dsInstant = Instant.ofEpochSecond(max / 1_000_000_000, max % 1_000_000_000);
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsInstant)
                            .at(dsInstant);
                    sender.flush();

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", """
                            ts\tts2
                            2261-12-31T23:59:59.999999999Z\t2261-12-31T23:59:59.999999999Z
                            2261-12-31T23:59:59.999999999Z\t2261-12-31T23:59:59.999999999Z
                            """);
                }
            }
        });
    }

    @Test
    public void testTimestampUpperBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            MicrosFormatCompiler timestampFormatCompiler = new MicrosFormatCompiler();

            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.execute("create table tab (ts timestamp, ts2 timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {

                    DateFormat format = timestampFormatCompiler.compile("yyyy-MM-dd HH:mm:ss.SSSUUU");
                    // technically, we the storage layer supports dates up to 294247-01-10T04:00:54.775807Z
                    // but DateFormat does reliably support only 4 digit years. thus we use 9999-12-31T23:59:59.999Z
                    // is the maximum date that can be reliably worked with.
                    long nonDsTs = format.parse("9999-12-31 23:59:59.999999", EN_LOCALE);
                    long dsTs = format.parse("9999-12-31 23:59:59.999999", EN_LOCALE);

                    // first try with ChronoUnit
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsTs, ChronoUnit.MICROS)
                            .at(dsTs, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", """
                            ts\tts2
                            9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z
                            """);


                    // now try with the Instant overloads of `at()` and `timestampColumn()`
                    Instant nonDsInstant = Instant.ofEpochSecond(nonDsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    Instant dsInstant = Instant.ofEpochSecond(dsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsInstant)
                            .at(dsInstant);
                    sender.flush();

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", """
                            ts\tts2
                            9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z
                            9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z
                            """);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T buildNestedArray(ArrayDataType dataType, int[] shape, int currentDim, int[] indices) {
        if (currentDim == shape.length - 1) {
            Object arr = dataType.createArray(shape[currentDim]);
            for (int i = 0; i < Array.getLength(arr); i++) {
                indices[currentDim] = i;
                dataType.setElement(arr, i, indices);
            }
            return (T) arr;
        } else {
            Class<?> componentType = dataType.getComponentType(shape.length - currentDim - 1);
            Object arr = Array.newInstance(componentType, shape[currentDim]);
            for (int i = 0; i < shape[currentDim]; i++) {
                indices[currentDim] = i;
                Object subArr = buildNestedArray(dataType, shape, currentDim + 1, indices);
                Array.set(arr, i, subArr);
            }
            return (T) arr;
        }
    }

    private static void flushAndAssertError(Sender sender, String... errors) {
        try {
            sender.flush();
            Assert.fail("Expected exception");
        } catch (LineSenderException e) {
            for (String error : errors) {
                TestUtils.assertContains(e.getMessage(), error);
            }
        }
    }

    private static void sendIlp(String tableName, int count, ServerMain serverMain) throws NumericException {
        long timestamp = MicrosTimestampDriver.floor("2023-11-27T18:53:24.834Z");
        int i = 0;

        int port = serverMain.getHttpServerPort();
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("localhost:" + port)
                .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                .build()
        ) {
            if (count / 2 > 0) {
                String tableNameUpper = tableName.toUpperCase();
                for (; i < count / 2; i++) {
                    String tn = i % 2 == 0 ? tableName : tableNameUpper;
                    sender.table(tn)
                            .symbol("async", "true")
                            .symbol("location", "santa_monica")
                            .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                            .longColumn("water_level", i)
                            .at(timestamp, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            for (; i < count; i++) {
                String tableNameUpper = tableName.toUpperCase();
                String tn = i % 2 == 0 ? tableName : tableNameUpper;
                sender.table(tn)
                        .symbol("async", "true")
                        .symbol("location", "santa_monica")
                        .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .longColumn("water_level", i)
                        .at(timestamp, ChronoUnit.MICROS);
            }
            sender.flush();
        }
    }

    private void testCreateTimestampColumns(long timestamp, ChronoUnit unit, int protocolVersion, int[] expectedColumnTypes, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .retryTimeoutMillis(0)
                        .protocolVersion(protocolVersion)
                        .build()
                ) {
                    long ts_ns = NanosTimestampDriver.floor("2025-11-19T10:55:24.123456789Z");
                    long ts_us = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123456Z");
                    long ts_ms = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123Z") / 1000;
                    Instant ts_instant = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);

                    if (unit != null) {
                        sender.table("tab1")
                                .doubleColumn("col1", 1.111)
                                .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                                .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                                .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                                .timestampColumn("ts_instant", ts_instant)
                                .at(timestamp, unit);
                    } else {
                        sender.table("tab1")
                                .doubleColumn("col1", 1.111)
                                .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                                .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                                .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                                .timestampColumn("ts_instant", ts_instant)
                                .at(Instant.ofEpochSecond(timestamp / 1_000_000_000, timestamp % 1_000_000_000));
                    }

                    sender.flush();

                    serverMain.awaitTable("tab1");
                    serverMain.assertSql("tab1", "col1\tts_ns\tts_us\tts_ms\tts_instant\ttimestamp\n" + expected + "\n");

                    final CairoEngine engine = serverMain.getEngine();
                    final TableToken tt = engine.verifyTableName("tab1");
                    try (TableMetadata metadata = serverMain.getEngine().getTableMetadata(tt)) {
                        assertEquals(6, metadata.getColumnCount());
                        assertEquals(ColumnType.DOUBLE, metadata.getColumnType(0));
                        assertEquals(expectedColumnTypes[0], metadata.getColumnType(1));
                        assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(2));
                        assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(3));
                        assertEquals(expectedColumnTypes[1], metadata.getColumnType(4));
                        assertEquals(expectedColumnTypes[2], metadata.getColumnType(5));
                    }
                }
            }
        });
    }

    private void testTimestampIngest(String timestampType, int protocolVersion, String expected1, String expected2) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.execute("create table tab (ts " + timestampType + ", dts " + timestampType + ") timestamp(dts) partition by DAY WAL");

                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .retryTimeoutMillis(0)
                        .protocolVersion(protocolVersion)
                        .build()
                ) {
                    long ts_ns = NanosTimestampDriver.floor("2025-11-19T10:55:24.123456789Z");
                    long dts_ns = NanosTimestampDriver.floor("2025-11-20T10:55:24.834129082Z");
                    long ts_us = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123456Z");
                    long dts_us = MicrosTimestampDriver.floor("2025-11-20T10:55:24.834129Z");
                    long ts_ms = MicrosTimestampDriver.floor("2025-11-19T10:55:24.123Z") / 1000;
                    long dts_ms = MicrosTimestampDriver.floor("2025-11-20T10:55:24.834Z") / 1000;
                    Instant tsInstant_ns = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);
                    Instant dtsInstant_ns = Instant.ofEpochSecond(dts_ns / 1_000_000_000, dts_ns % 1_000_000_000 + 10);

                    sender.table("tab")
                            .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                            .at(dts_ns, ChronoUnit.NANOS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                            .at(dts_ns, ChronoUnit.NANOS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                            .at(dts_ns, ChronoUnit.NANOS);

                    sender.table("tab")
                            .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                            .at(dts_us, ChronoUnit.MICROS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                            .at(dts_us, ChronoUnit.MICROS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                            .at(dts_us, ChronoUnit.MICROS);

                    sender.table("tab")
                            .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                            .at(dts_ms, ChronoUnit.MILLIS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                            .at(dts_ms, ChronoUnit.MILLIS);
                    sender.table("tab")
                            .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                            .at(dts_ms, ChronoUnit.MILLIS);

                    sender.table("tab")
                            .timestampColumn("ts", tsInstant_ns)
                            .at(dtsInstant_ns);

                    sender.flush();

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("tab", expected1);

                    try {
                        long ts_tooLargeForNanos_us = MicrosTimestampDriver.floor("2300-11-19T10:55:24.123456Z");
                        long dts_tooLargeForNanos_us = MicrosTimestampDriver.floor("2300-11-20T10:55:24.834129Z");

                        sender.table("tab")
                                .timestampColumn("ts", ts_tooLargeForNanos_us, ChronoUnit.MICROS)
                                .at(dts_tooLargeForNanos_us, ChronoUnit.MICROS);

                        sender.flush();
                        if (expected2 == null) {
                            Assert.fail("Exception expected");
                        }
                    } catch (LineSenderException | ArithmeticException e) {
                        if (expected2 == null) {
                            TestUtils.assertContains(e.getMessage(), "long overflow");
                        } else {
                            throw e;
                        }
                    }

                    sender.reset();
                    try {
                        Instant tsInstant_tooLargeForNanos_us = Instant.ofEpochSecond(26123456789L, 123456789L);
                        Instant dtsInstant_tooLargeForNanos_us = Instant.ofEpochSecond(26123456790L, 123457890L);

                        sender.table("tab")
                                .timestampColumn("ts", tsInstant_tooLargeForNanos_us)
                                .at(dtsInstant_tooLargeForNanos_us);

                        sender.flush();
                        if (expected2 == null) {
                            Assert.fail("Exception expected");
                        }
                    } catch (LineSenderException | ArithmeticException e) {
                        if (expected2 == null) {
                            TestUtils.assertContains(e.getMessage(), "long overflow");
                        } else {
                            throw e;
                        }
                    }

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("tab", expected2 == null ? expected1 : expected2);
                }
            }
        });
    }

    private enum ArrayDataType {
        DOUBLE(double.class) {
            @Override
            public Object createArray(int length) {
                return new double[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                double[] arr = (double[]) array;
                double product = 1.0;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        },
        LONG(long.class) {
            @Override
            public Object createArray(int length) {
                return new long[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                long[] arr = (long[]) array;
                long product = 1L;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        };

        private final Class<?> baseType;
        private final Class<?>[] componentTypes = new Class<?>[17]; // 支持最多16维

        ArrayDataType(Class<?> baseType) {
            this.baseType = baseType;
            initComponentTypes();
        }

        public abstract Object createArray(int length);

        public Class<?> getComponentType(int dimsRemaining) {
            if (dimsRemaining < 0 || dimsRemaining > 16) {
                throw new RuntimeException("Array dimension too large");
            }
            return componentTypes[dimsRemaining];
        }

        public abstract void setElement(Object array, int index, int[] indices);

        private void initComponentTypes() {
            componentTypes[0] = baseType;
            for (int dim = 1; dim <= 16; dim++) {
                componentTypes[dim] = Array.newInstance(componentTypes[dim - 1], 0).getClass();
            }
        }
    }
}
