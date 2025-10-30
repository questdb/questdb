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
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGHexTestsCircuitBreakRegistry;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.text.CopyImportRequestJob;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.ConcurrentCacheConfiguration;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeZone;

import static io.questdb.std.Numbers.hexDigits;
import static io.questdb.test.cutlass.pgwire.Port0PGConfiguration.getPGWirePort;

public abstract class BasePGTest extends AbstractCairoTest {

    public static final long CONN_AWARE_EXTENDED_LIMITED = 1;
    // QUIRKS Mode is where PostgresJDBC driver might send incorrect
    // message to the server. This breaks tests not just with QuestDB
    // but also with PostgreSQL actual.
    public static final long CONN_AWARE_QUIRKS = 4;
    public static final long CONN_AWARE_EXTENDED = CONN_AWARE_EXTENDED_LIMITED | CONN_AWARE_QUIRKS;
    public static final long CONN_AWARE_SIMPLE = 2;
    public static final long CONN_AWARE_ALL = CONN_AWARE_SIMPLE | CONN_AWARE_EXTENDED;
    protected static int sharedQueryWorkerCount = 0;
    protected CopyExportRequestJob copyExportRequestJob = null;
    protected CopyImportRequestJob copyImportRequestJob = null;
    protected int forceRecvFragmentationChunkSize = 1024 * 1024;
    protected int acceptLoopTimeout = 500;
    protected int forceSendFragmentationChunkSize = 1024 * 1024;
    protected long maxQueryTime = Long.MAX_VALUE;
    protected int recvBufferSize = 1024 * 1024;
    protected int selectCacheBlockCount = -1;
    protected int sendBufferSize = 1024 * 1024;

    public static void assertResultSet(CharSequence expected, StringSink sink, ResultSet rs) throws SQLException {
        assertResultSet(null, expected, sink, rs);
    }

    public static PGServer createPGWireServer(
            PGConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            PGCircuitBreakerRegistry registry,
            ObjectFactory<SqlExecutionContextImpl> executionContextObjectFactory
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }
        return new PGServer(configuration, cairoEngine, workerPool, registry, executionContextObjectFactory);
    }

    public static PGServer createPGWireServer(
            PGConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool,
            boolean fixedClientIdAndSecret
    ) {
        if (!configuration.isEnabled()) {
            return null;
        }

        PGCircuitBreakerRegistry registry = fixedClientIdAndSecret
                ? PGHexTestsCircuitBreakRegistry.INSTANCE
                : new DefaultPGCircuitBreakerRegistry(configuration, cairoEngine.getConfiguration());

        return new PGServer(configuration, cairoEngine, workerPool, registry, () -> new SqlExecutionContextImpl(cairoEngine, sharedQueryWorkerCount));
    }

    public static PGServer createPGWireServer(
            PGConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        return createPGWireServer(configuration, cairoEngine, workerPool, false);
    }

    public static long printToSink(StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException {
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
                            sink.put(floatValue);
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
                    case ARRAY:
                        Array array = rs.getArray(i);
                        if (array == null) {
                            sink.put("null");
                        } else {
                            writeArrayContent(sink, array.getArray());
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

    private static void toSink(InputStream is, Utf16Sink sink) {
        // limit what we print
        byte[] bb = new byte[1];
        int i = 0;
        try {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeArrayContent(StringSink sink, Object array) {
        if (array == null) {
            sink.put("null");
            return;
        }
        if (!array.getClass().isArray()) {
            if (array instanceof Number) {
                if (array instanceof Double) {
                    double d = ((Number) array).doubleValue();
                    if (Numbers.isNull(d)) {
                        sink.put("null");
                    } else {
                        sink.put(d);
                    }
                }
                if (array instanceof Float) {
                    float f = ((Number) array).floatValue();
                    if (Numbers.isNull(f)) {
                        sink.put("null");
                    } else {
                        sink.put(f);
                    }
                }
                if (array instanceof Long) {
                    long l = ((Number) array).longValue();
                    if (l == Numbers.LONG_NULL) {
                        sink.put("null");
                    } else {
                        sink.put(l);
                    }
                }
            } else if (array instanceof Boolean) {
                sink.put((Boolean) array);
            } else {
                sink.put(array.toString());
            }
            return;
        }

        sink.put('{');
        int length = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < length; i++) {
            Object element = java.lang.reflect.Array.get(array, i);
            writeArrayContent(sink, element);

            if (i < length - 1) {
                sink.put(',');
            }
        }
        sink.put('}');
    }

    protected static void assertResultSet(CharSequence expected, StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException, IOException {
        assertResultSet(null, expected, sink, rs, map);
    }

    protected static void assertResultSet(String message, CharSequence expected, StringSink sink, ResultSet rs, @Nullable IntIntHashMap map) throws SQLException, IOException {
        printToSink(sink, rs, map);
        TestUtils.assertEquals(message, expected, sink);
    }

    protected static void assertResultSet(String message, CharSequence expected, StringSink sink, ResultSet rs) throws SQLException {
        sink.clear();
        printToSink(sink, rs, null);
        TestUtils.assertEquals(message, expected, sink);
    }

    protected static Connection getConnection(Mode mode, int port, boolean binary) throws SQLException {
        return getConnection(mode, port, binary, -2);
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

        // a hack to deal with "Address already in use" error:
        // Some tests open and close connections in a quick succession.
        // This can lead to the above error since it might exhaust available ephemeral ports on the host machine
        int maxAttempts = 100;
        for (int attempt = 1; ; attempt++) {
            try {
                return DriverManager.getConnection(url, properties);
            } catch (PSQLException ex) {
                Throwable cause = ex.getCause();
                if (!(cause instanceof BindException)) {
                    // not a "Address already in use" error
                    throw ex;
                }
                String causeMessage = cause.getMessage();
                if (causeMessage == null || !causeMessage.contains("Address already in use")) {
                    // not a "Address already in use" error
                    throw ex;
                }
                if (attempt >= maxAttempts) {
                    // too many attempts
                    throw ex;
                }
                LOG.info().$("Failed to open a connection due to 'Address already in use'. Retrying... [attempt=").$(attempt).I$();
                Os.sleep(100);
            }
        }
    }

    protected void assertWithPgServer(
            Mode mode,
            boolean binary,
            int prepareThreshold,
            @NotNull PGJobContextTest.ConnectionAwareRunnable runnable
    ) throws Exception {
        assertWithPgServer(mode, binary, prepareThreshold, runnable, null);
    }

    protected void assertWithPgServer(
            Mode mode,
            boolean binary,
            int prepareThreshold,
            @NotNull PGJobContextTest.ConnectionAwareRunnable runnable,
            @Nullable Runnable setUpRunnable
    ) throws Exception {
        LOG.info().$("asserting PG Wire server [mode=").$(mode)
                .$(", binary=").$(binary)
                .$(", prepareThreshold=").$(prepareThreshold)
                .I$();
        setUp();
        if (setUpRunnable != null) {
            setUpRunnable.run();
        }
        try {
            assertMemoryLeak(() -> {
                try (
                        final PGServer server = createPGServer(2);
                        WorkerPool workerPool = server.getWorkerPool()
                ) {
                    workerPool.start(LOG);
                    for (int i = 0; i < 60000 && !server.isListening(); i++) {
                        Os.sleep(1);
                    }
                    try (final Connection connection = getConnection(mode, server.getPort(), binary, prepareThreshold)) {
                        runnable.run(connection, binary, mode, server.getPort());
                    }
                }
            });
        } finally {
            tearDown();
        }
    }

    protected void assertWithPgServer(long bits, @NotNull PGJobContextTest.ConnectionAwareRunnable runnable) throws Exception {
        assertWithPgServer(bits, runnable, null);
    }

    protected void assertWithPgServer(
            long bits,
            @NotNull PGJobContextTest.ConnectionAwareRunnable runnable,
            @Nullable Runnable setUpRunnable
    ) throws Exception {
        // SIMPLE + TEXT
        if (((bits & BasePGTest.CONN_AWARE_SIMPLE) == BasePGTest.CONN_AWARE_SIMPLE)) {
            LOG.info().$("Mode: asserting simple text").$();
            assertWithPgServer(Mode.SIMPLE, false, -1, runnable, setUpRunnable); // SIMPLE + TEXT
        }

        // EXTENDED + TEXT PARAMS + TEXT RESULT + P/B/D/E/S
        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_LIMITED) == BasePGTest.CONN_AWARE_EXTENDED_LIMITED) {
            assertWithPgServer(Mode.EXTENDED, true, -2, runnable, setUpRunnable); // EXTENDED + BINARY PARAMS + TEXT RESULT + P/B/D/E/S
            assertWithPgServer(Mode.EXTENDED, false, -2, runnable, setUpRunnable); // EXTENDED + TEXT PARAMS + TEXT RESULT + P/B/D/E/S
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, false, -2, runnable, setUpRunnable); // EXTENDED + TEXT PARAMS + TEXT RESULT + P/B/D/E/S
        }

        if ((bits & BasePGTest.CONN_AWARE_EXTENDED_LIMITED) == BasePGTest.CONN_AWARE_EXTENDED_LIMITED && (bits & CONN_AWARE_QUIRKS) == CONN_AWARE_QUIRKS) {
            assertWithPgServer(Mode.EXTENDED, true, -1, runnable, setUpRunnable); // EXTENDED + BINARY PARAMS + BINARY RESULT (P/D/S and B/E/S)
            assertWithPgServer(Mode.EXTENDED, false, -1, runnable, setUpRunnable); // EXTENDED + TEXT PARAMS + TEXT RESULT (P/D/S and B/E/S)
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, true, -1, runnable, setUpRunnable);  // EXTENDED + BINARY PARAMS + TEXT RESULT (P/D/S and B/E/S)
            assertWithPgServer(Mode.EXTENDED_CACHE_EVERYTHING, false, -1, runnable, setUpRunnable);   // EXTENDED + TEXT PARAMS + TEXT RESULT (P/D/S and B/E/S)
        }
    }

    protected void assertWithPgServerExtendedBinaryOnly(@NotNull PGJobContextTest.ConnectionAwareRunnable runnable) throws Exception {
        assertWithPgServerExtendedBinaryOnly(runnable, null);
    }

    protected void assertWithPgServerExtendedBinaryOnly(
            @NotNull PGJobContextTest.ConnectionAwareRunnable runnable,
            @Nullable Runnable setUpRunnable
    ) throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, runnable, setUpRunnable);
    }

    protected PGServer createPGServer(PGConfiguration configuration) throws SqlException {
        return createPGServer(configuration, false);
    }

    protected PGServer createPGServer(
            PGConfiguration configuration,
            boolean fixedClientIdAndSecret
    ) throws SqlException {
        TestWorkerPool workerPool = new TestWorkerPool(configuration);
        copyImportRequestJob = new CopyImportRequestJob(engine, configuration.getWorkerCount());
        workerPool.assign(copyImportRequestJob);
        workerPool.freeOnExit(copyImportRequestJob);

        copyExportRequestJob = new CopyExportRequestJob(engine);
        workerPool.assign(copyExportRequestJob);
        workerPool.freeOnExit(copyExportRequestJob);

        return createPGWireServer(
                configuration,
                engine,
                workerPool,
                fixedClientIdAndSecret
        );
    }

    protected PGServer createPGServer(int workerCount) throws SqlException {

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

        final ConcurrentCacheConfiguration concurrentCacheConfiguration = new DefaultConcurrentCacheConfiguration() {
            @Override
            public int getBlocks() {
                return selectCacheBlockCount == -1 ? super.getBlocks() : selectCacheBlockCount;
            }
        };

        final PGConfiguration conf = new Port0PGConfiguration(-1) {

            @Override
            public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
                return circuitBreakerConfiguration;
            }

            @Override
            public ConcurrentCacheConfiguration getConcurrentCacheConfiguration() {
                return concurrentCacheConfiguration;
            }

            @Override
            public long getAcceptLoopTimeout() {
                return acceptLoopTimeout;
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
            return getConnection(Mode.SIMPLE, port, binary);
        } else {
            return getConnection(Mode.EXTENDED, port, binary);
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
    protected DefaultPGConfiguration getStdPgWireConfig() {
        return new DefaultPGConfiguration() {
            @Override
            public int getBindPort() {
                return getPGWirePort();
            }

            @Override
            public Rnd getRandom() {
                return new Rnd();
            }
        };
    }

    @NotNull
    protected DefaultPGConfiguration getStdPgWireConfigAltCreds() {
        return new DefaultPGConfiguration() {
            @Override
            public int getBindPort() {
                return getPGWirePort();
            }

            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
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
