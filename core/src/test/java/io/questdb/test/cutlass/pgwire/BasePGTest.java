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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cutlass.pgwire.CircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.cutlass.text.CopyRequestJob;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

import static io.questdb.std.Numbers.hexDigits;

public abstract class BasePGTest extends AbstractCairoTest {

    public static final int CONN_AWARE_EXTENDED_BINARY = 4;
    public static final int CONN_AWARE_EXTENDED_CACHED_BINARY = 64;
    public static final int CONN_AWARE_EXTENDED_CACHED_TEXT = 128;
    public static final int CONN_AWARE_EXTENDED_PREPARED_BINARY = 16;
    public static final int CONN_AWARE_EXTENDED_PREPARED_TEXT = 32;
    public static final int CONN_AWARE_EXTENDED_TEXT = 8;
    public static final int CONN_AWARE_EXTENDED_ALL = CONN_AWARE_EXTENDED_BINARY
            | CONN_AWARE_EXTENDED_TEXT
            | CONN_AWARE_EXTENDED_PREPARED_BINARY
            | CONN_AWARE_EXTENDED_PREPARED_TEXT
            | CONN_AWARE_EXTENDED_CACHED_BINARY
            | CONN_AWARE_EXTENDED_CACHED_TEXT;
    public static final int CONN_AWARE_SIMPLE_BINARY = 1;
    public static final int CONN_AWARE_SIMPLE_TEXT = 2;
    public static final int CONN_AWARE_ALL =
            CONN_AWARE_SIMPLE_BINARY
                    | CONN_AWARE_SIMPLE_TEXT
                    | CONN_AWARE_EXTENDED_BINARY
                    | CONN_AWARE_EXTENDED_TEXT
                    | CONN_AWARE_EXTENDED_PREPARED_BINARY
                    | CONN_AWARE_EXTENDED_PREPARED_TEXT
                    | CONN_AWARE_EXTENDED_CACHED_BINARY
                    | CONN_AWARE_EXTENDED_CACHED_TEXT;
    protected CopyRequestJob copyRequestJob = null;
    protected int forceRecvFragmentationChunkSize = 1024 * 1024;
    protected int forceSendFragmentationChunkSize = 1024 * 1024;
    protected int recvBufferSize = 1024 * 1024;
    protected int sendBufferSize = 1024 * 1024;
    protected int selectCacheBlockCount = -1;

    public static void assertResultSet(CharSequence expected, StringSink sink, ResultSet rs) throws SQLException, IOException {
        assertResultSet(null, expected, sink, rs);
    }

    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            CircuitBreakerRegistry registry,
            ObjectFactory<SqlExecutionContextImpl> executionContextObjectFactory
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }
        return new PGWireServer(configuration, cairoEngine, workerPool, registry, executionContextObjectFactory);
    }

    public static PGWireServer createPGWireServer(
            PGWireConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        CircuitBreakerRegistry registry = new CircuitBreakerRegistry(configuration, cairoEngine.getConfiguration());

        return new PGWireServer(
                configuration,
                cairoEngine,
                workerPool,
                registry,
                () -> new SqlExecutionContextImpl(cairoEngine, workerPool.getWorkerCount(), workerPool.getWorkerCount())
        );
    }

    public static long printToSink(StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException, IOException {
        // dump metadata
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sink.put(',');
            }

            sink.put(metaData.getColumnName(i + 1));
            if (JDBCType.valueOf(metaData.getColumnType(i + 1)) == JDBCType.VARCHAR) {
                if (map != null) {
                    if (map.get(i + 1) == ColumnType.IPv4) {
                        sink.put('[').put("IPv4").put(']');
                    } else {
                        sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
                    }
                } else {
                    sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
                }
            } else {
                sink.put('[').put(JDBCType.valueOf(metaData.getColumnType(i + 1)).name()).put(']');
            }
        }
        sink.put('\n');

        Timestamp timestamp;
        long rows = 0;
        while (rs.next()) {
            rows++;
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sink.put(',');
                }
                switch (JDBCType.valueOf(metaData.getColumnType(i))) {
                    case VARCHAR:
                    case NUMERIC:
                        String stringValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(stringValue);
                        }
                        break;
                    case INTEGER:
                        int intValue = rs.getInt(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(intValue);
                        }
                        break;
                    case DOUBLE:
                        double doubleValue = rs.getDouble(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(doubleValue);
                        }
                        break;
                    case TIMESTAMP:
                        timestamp = rs.getTimestamp(i);
                        if (timestamp == null) {
                            sink.put("null");
                        } else {
                            sink.put(timestamp.toString());
                        }
                        break;
                    case REAL:
                        float floatValue = rs.getFloat(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(floatValue, 3);
                        }
                        break;
                    case SMALLINT:
                        sink.put(rs.getShort(i));
                        break;
                    case BIGINT:
                        long longValue = rs.getLong(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(longValue);
                        }
                        break;
                    case CHAR:
                        String strValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(strValue.charAt(0));
                        }
                        break;
                    case BIT:
                        sink.put(rs.getBoolean(i));
                        break;
                    case TIME:
                    case DATE:
                        timestamp = rs.getTimestamp(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(timestamp.toString());
                        }
                        break;
                    case BINARY:
                        InputStream stream = rs.getBinaryStream(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            toSink(stream, sink);
                        }
                        break;
                    case OTHER:
                        Object object = rs.getObject(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(object.toString());
                        }
                        break;
                    default:
                        assert false;
                }
            }
            sink.put('\n');
        }
        return rows;
    }

    private static void toSink(InputStream is, Utf16Sink sink) throws IOException {
        // limit what we print
        byte[] bb = new byte[1];
        int i = 0;
        while (is.read(bb) > 0) {
            byte b = bb[0];
            if (i > 0) {
                if ((i % 16) == 0) {
                    sink.put('\n');
                    Numbers.appendHexPadded(sink, i);
                }
            } else {
                Numbers.appendHexPadded(sink, i);
            }
            sink.putAscii(' ');

            final int v;
            if (b < 0) {
                v = 256 + b;
            } else {
                v = b;
            }

            if (v < 0x10) {
                sink.putAscii('0');
                sink.putAscii(hexDigits[b]);
            } else {
                sink.putAscii(hexDigits[v / 0x10]);
                sink.putAscii(hexDigits[v % 0x10]);
            }

            i++;
        }
    }

    protected static void assertResultSet(CharSequence expected, StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException, IOException {
        assertResultSet(null, expected, sink, rs, map);
    }

    protected static void assertResultSet(String message, CharSequence expected, StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException, IOException {
        printToSink(sink, rs, map);
        TestUtils.assertEquals(message, expected, sink);
    }

    protected static void assertResultSet(String message, CharSequence expected, StringSink sink, ResultSet rs) throws SQLException, IOException {
        sink.clear();
        printToSink(sink, rs, null);
        TestUtils.assertEquals(message, expected, sink);
    }

    protected static Connection getConnection(Mode mode, int port, boolean binary, int prepareThreshold) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(binary));
        properties.setProperty("preferQueryMode", mode.value);
        if (prepareThreshold > -2) { // -1 has special meaning in pg jdbc ...
            properties.setProperty("prepareThreshold", String.valueOf(prepareThreshold));
        }

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        // use this line to switch to local postgres
        // return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/qdb", properties);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    protected void assertWithPgServer(
            Mode mode,
            boolean binary,
            PGJobContextTest.ConnectionAwareRunnable runnable,
            int prepareThreshold,
            long queryTimeout
    ) throws Exception {
        LOG.info().$("asserting PG Wire server [mode=").$(mode)
                .$(", binary=").$(binary)
                .$(", prepareThreshold=").$(prepareThreshold)
                .I$();
        super.setUp();
        try {
            assertMemoryLeak(() -> {
                try (
                        final PGWireServer server = createPGServer(2, queryTimeout);
                        WorkerPool workerPool = server.getWorkerPool()
                ) {
                    workerPool.start(LOG);
                    try (final Connection connection = getConnection(mode, server.getPort(), binary, prepareThreshold)) {
                        runnable.run(connection, binary, mode, server.getPort());
                    }
                }
            });
        } finally {
            super.tearDown();
        }
    }

    protected void assertWithPgServer(long bits, long queryTimeout, PGJobContextTest.ConnectionAwareRunnable runnable) throws Exception {
        if ((bits & BasePGTest.CONN_AWARE_SIMPLE_BINARY) == BasePGTest.CONN_AWARE_SIMPLE_BINARY) {
            LOG.info().$("Mode: asserting simple binary").$();
            assertWithPgServer(Mode.SIMPLE, true, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.SIMPLE, true, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_SIMPLE_TEXT) == BasePGTest.CONN_AWARE_SIMPLE_TEXT) {
            LOG.info().$("Mode: asserting simple text").$();
            assertWithPgServer(Mode.SIMPLE, false, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.SIMPLE, false, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_BINARY) == BasePGTest.CONN_AWARE_EXTENDED_BINARY) {
            LOG.info().$("Mode: asserting extended binary").$();
            assertWithPgServer(Mode.EXTENDED, true, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED, true, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_TEXT) == BasePGTest.CONN_AWARE_EXTENDED_TEXT) {
            LOG.info().$("Mode: asserting extended text").$();
            assertWithPgServer(Mode.EXTENDED, false, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED, false, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_PREPARED_BINARY) == BasePGTest.CONN_AWARE_EXTENDED_PREPARED_BINARY) {
            LOG.info().$("Mode: asserting extended prepared binary").$();
            assertWithPgServer(Mode.EXTENDED_FOR_PREPARED, true, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED_FOR_PREPARED, true, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_PREPARED_TEXT) == BasePGTest.CONN_AWARE_EXTENDED_PREPARED_TEXT) {
            LOG.info().$("Mode: asserting extended prepared text").$();
            assertWithPgServer(Mode.EXTENDED_FOR_PREPARED, false, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED_FOR_PREPARED, false, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_CACHED_BINARY) == BasePGTest.CONN_AWARE_EXTENDED_CACHED_BINARY) {
            LOG.info().$("Mode: asserting extended cached binary").$();
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, true, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, true, runnable, -1, queryTimeout);
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_CACHED_TEXT) == BasePGTest.CONN_AWARE_EXTENDED_CACHED_TEXT) {
            LOG.info().$("Mode: asserting extended cached text").$();
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, false, runnable, -2, queryTimeout);
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, false, runnable, -1, queryTimeout);
        }
    }

    protected void assertWithPgServer(long bits, PGJobContextTest.ConnectionAwareRunnable runnable) throws Exception {
        assertWithPgServer(bits, Long.MAX_VALUE, runnable);
    }

    protected PGWireServer createPGServer(PGWireConfiguration configuration) throws SqlException {
        TestWorkerPool workerPool = new TestWorkerPool(configuration.getWorkerCount(), metrics);
        copyRequestJob = new CopyRequestJob(engine, configuration.getWorkerCount());

        workerPool.assign(copyRequestJob);
        workerPool.freeOnExit(copyRequestJob);

        return createPGWireServer(
                configuration,
                engine,
                workerPool
        );
    }

    protected PGWireServer createPGServer(int workerCount) throws SqlException {
        return createPGServer(workerCount, Long.MAX_VALUE);
    }

    protected PGWireServer createPGServer(int workerCount, long maxQueryTime) throws SqlException {
        return createPGServer(workerCount, maxQueryTime, -1);
    }

    protected PGWireServer createPGServer(int workerCount, long maxQueryTime, int connectionLimit) throws SqlException {

        final SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return (maxQueryTime == SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK)
                        ? 0 // fail on first check
                        : super.getCircuitBreakerThrottle();
            }

            // should be consistent with clock used in AbstractCairoTest, otherwise timeout tests become unreliable because
            // Os.currentTimeMillis() could be a couple ms in the future compare to System.currentTimeMillis(), at least on Windows 10
            @Override
            public @NotNull MillisecondClock getClock() {
                return () -> testMicrosClock.getTicks() / 1000L;
            }

            @Override
            public long getQueryTimeout() {
                return maxQueryTime;
            }
        };

        final PGWireConfiguration conf = new Port0PGWireConfiguration(connectionLimit) {

            @Override
            public int getSelectCacheBlockCount() {
                return selectCacheBlockCount == -1 ? super.getSelectCacheBlockCount() : selectCacheBlockCount;
            }

            @Override
            public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                return circuitBreakerConfiguration;
            }

            @Override
            public int getForceRecvFragmentationChunkSize() {
                return forceRecvFragmentationChunkSize;
            }

            @Override
            public int getForceSendFragmentationChunkSize() {
                return forceSendFragmentationChunkSize;
            }

            @Override
            public int getRecvBufferSize() {
                return recvBufferSize;
            }

            @Override
            public int getSendBufferSize() {
                return sendBufferSize;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };

        return createPGServer(conf);
    }

    protected void execSelectWithParam(PreparedStatement select, int value) throws SQLException {
        sink.clear();
        select.setInt(1, value);
        try (ResultSet resultSet = select.executeQuery()) {
            sink.clear();
            while (resultSet.next()) {
                sink.put(resultSet.getInt(1));
                sink.put('\n');
            }
        }
    }

    protected Connection getConnection(int port, boolean simple, boolean binary) throws SQLException {
        if (simple) {
            return getConnection(Mode.SIMPLE, port, binary, -2);
        } else {
            return getConnection(Mode.EXTENDED, port, binary, -2);
        }
    }

    protected Connection getConnection(int port, boolean simple, boolean binary, long statementTimeoutMs) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(binary));
        properties.setProperty("preferQueryMode", simple ? Mode.SIMPLE.value : Mode.EXTENDED.value);
        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        properties.setProperty("options", "-c statement_timeout=" + statementTimeoutMs);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    protected Connection getConnectionWitSslInitRequest(Mode mode, int port, boolean binary, int prepareThreshold) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("binaryTransfer", Boolean.toString(binary));
        properties.setProperty("preferQueryMode", mode.value);
        if (prepareThreshold > -2) { // -1 has special meaning in pg jdbc ...
            properties.setProperty("prepareThreshold", String.valueOf(prepareThreshold));
        }

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        // use this line to switch to local postgres
        // return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/qdb", properties);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    @NotNull
    protected NetworkFacade getFragmentedSendFacade() {
        return new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                int total = 0;
                for (int i = 0; i < bufferLen; i++) {
                    int n = super.sendRaw(fd, buffer + i, 1);
                    if (n < 0) {
                        return n;
                    }
                    total += n;
                }
                return total;
            }
        };
    }

    @NotNull
    protected DefaultPGWireConfiguration getHexPgWireConfig() {
        return new DefaultPGWireConfiguration() {
            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }

            @Override
            public IODispatcherConfiguration getDispatcherConfiguration() {
                return new DefaultIODispatcherConfiguration() {
                    @Override
                    public int getBindPort() {
                        return 0;  // Bind to ANY port.
                    }
                };
            }

            @Override
            public Rnd getRandom() {
                return new Rnd();
            }
        };
    }

    @NotNull
    protected DefaultPGWireConfiguration getStdPgWireConfig() {
        return new DefaultPGWireConfiguration() {
            @Override
            public IODispatcherConfiguration getDispatcherConfiguration() {
                return new DefaultIODispatcherConfiguration() {
                    @Override
                    public int getBindPort() {
                        return 0;  // Bind to ANY port.
                    }
                };
            }

            @Override
            public Rnd getRandom() {
                return new Rnd();
            }
        };
    }

    public enum Mode {
        SIMPLE("simple"),
        EXTENDED("extended"),
        EXTENDED_FOR_PREPARED("extendedForPrepared"),
        EXTENDED_CACHE_EVERYTHING("extendedCacheEverything");

        public final String value;

        Mode(String value) {
            this.value = value;
        }
    }
}
