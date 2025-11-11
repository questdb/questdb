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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestDataUnavailableFunctionFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SuspendEvent;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.NetUtils;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGResultSetMetaData;
import org.postgresql.core.Tuple;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PgResultSet;
import org.postgresql.util.PGTimestamp;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT;
import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT;
import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;
import static io.questdb.test.tools.TestUtils.*;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * This class contains tests which replay PGWIRE traffic.
 * It is possible to simulate any PG client in our tests, such as different kind of Python, Rust or Go postgres libraries.
 * <p>
 * How to create these tests:
 * 1. Enable dumping of PG messages
 * Change PGWireConfiguration.getDumpNetworkTraffic() to return 'true'.
 * 2. Start ServerMain
 * 3. Run the PG client
 * This could be any PG client or library. The client connects to QuestDB, and runs SQL commands.
 * Since dumping of messages are enabled, PG traffic should be written to the log.
 * 4. Collect PG messages from the log
 * In the log you should see incoming and outgoing messages, such as ">0000006e00030" and "<520000000800000003".
 * Collect all sent and received messages.
 * 5. Create a test to replay the messages
 * Now we can create a test to simulate the client running the SQL commands.
 * Example:
 * <pre>
 * public void testExample() throws Exception {
 *      String script = ">0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000\n" +
 *          "<520000000800000003\n" +
 *          ">70000000076f6800\n" +
 *          "<520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549\n" +
 *          ">50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000\n" +
 *          ">4200000021000000010000000200000001330000000a35303030303030303030000044000000065000450000000900000000004800000004\n" +
 *          "<31000000043200000004540000004400037800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000024320000000000000300000413ffffffffffff0000440000001e0003000000013100000001330000000a35303030303030303030440000001e0003000000013200000001330000000a35303030303030303030430000000d53454c454354203200\n";
 *      assertHexScript(NetworkFacadeImpl.INSTANCE, script, getHexPgWireConfig());
 * }
 * </pre>
 * 6. Disable dumping of PG messages
 * Change PGWireConfiguration.getDumpNetworkTraffic() to return 'false' again.
 * 7. Run the test
 */
@SuppressWarnings("SqlNoDataSourceInspection")
public class PGJobContextTest extends BasePGTest {

    /**
     * When set to true, tests or sections of tests that are don't work with the WAL are skipped.
     */
    private static final long DAY_MICROS = Micros.HOUR_MICROS * 24L;
    private static final Log LOG = LogFactory.getLog(PGJobContextTest.class);
    private static final int count = 200;
    private static final String createDatesTblStmt = "create table xts as (select timestamp_sequence(0, 3600L * 1000 * 1000) ts from long_sequence(" + count + ")) timestamp(ts) partition by DAY";
    private static List<Object[]> datesArr;
    private static String stringTypeName;
    private final Rnd bufferSizeRnd = TestUtils.generateRandom(LOG);
    private final boolean walEnabled;

    public PGJobContextTest() {
        this.walEnabled = TestUtils.isWal();
    }

    public static void drainWalAndAssertTableExists(CharSequence tableName) {
        try (Path path = new Path()) {
            assertEquals(TableUtils.TABLE_EXISTS, engine.getTableStatus(path, engine.getTableTokenIfExists(tableName)));
        }
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss'.0'");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        final Stream<Object[]> dates = LongStream.rangeClosed(0, count - 1)
                .map(i -> i * Micros.HOUR_MICROS / 1000L)
                .mapToObj(ts -> new Object[]{ts * 1000L, formatter.format(new java.util.Date(ts))});
        datesArr = dates.collect(Collectors.toList());
        stringTypeName = ColumnType.nameOf(ColumnType.STRING);
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        selectCacheBlockCount = -1;
        sendBufferSize = 512 * (1 + bufferSizeRnd.nextInt(15));
        forceSendFragmentationChunkSize = (int) (10 + bufferSizeRnd.nextInt(Math.min(512, sendBufferSize) - 10) * bufferSizeRnd.nextDouble() * 1.2);

        recvBufferSize = 512 * (1 + bufferSizeRnd.nextInt(15));
        forceRecvFragmentationChunkSize = (int) (10 + bufferSizeRnd.nextInt(Math.min(512, recvBufferSize) - 10) * bufferSizeRnd.nextDouble() * 1.2);

        acceptLoopTimeout = bufferSizeRnd.nextInt(500) + 10;

        LOG.info().$("fragmentation params [sendBufferSize=").$(sendBufferSize)
                .$(", forceSendFragmentationChunkSize=").$(forceSendFragmentationChunkSize)
                .$(", recvBufferSize=").$(recvBufferSize)
                .$(", forceRecvFragmentationChunkSize=").$(forceRecvFragmentationChunkSize)
                .$(", acceptLoopTimeout=").$(acceptLoopTimeout)
                .I$();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testAllParamsHex() throws Exception {
        skipOnWalRun();
        final String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5000
                >00
                >002200
                >5345
                >5420
                >65
                >7874
                >7261
                >5f
                >66
                >6c6f
                >6174
                >5f
                >64
                >69676974
                >73
                >20
                >3d20
                >33
                >0000
                >0042
                >0000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >50
                >0000
                >00
                >3700
                >5345
                >54
                >2061
                >70
                >706c
                >69
                >6361
                >7469
                >6f
                >6e5f
                >6e
                >616d6520
                >3d20
                >2750
                >6f
                >7374
                >67
                >726553514c20
                >4a
                >44
                >42
                >4320
                >44
                >726976
                >657227
                >00
                >0000
                >420000000c00000000
                >0000000045
                >00
                >000009000000
                >00
                >0153
                >0000
                >0004
                <310000000432000000044300000008534554005a0000000549
                >50
                >00
                >0000
                >2a00
                >73
                >656c
                >65
                >6374
                >2031
                >2c322c
                >33
                >2066
                >72
                >6f6d
                >20
                >6c6f
                >6e
                >67
                >5f7365
                >7175
                >65
                >6e63
                >6528
                >31
                >2900
                >00
                >0042
                >0000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >000000260073656c6563
                >74
                >2031
                >2066
                >72
                >6f
                >6d
                >206c
                >6f6e
                >675f73
                >65
                >7175656e63
                >65
                >2832
                >290000
                >00
                >420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >5000
                >0000
                >2a00
                >7365
                >6c
                >65637420
                >31
                >2c32
                >2c33
                >2066
                >726f
                >6d
                >206c
                >6f6e
                >675f
                >7365
                >7175
                >65
                >6e63
                >65
                >28
                >31
                >2900
                >0000
                >420000000c00
                >00
                >000000
                >00
                >0000
                >44
                >0000
                >0006
                >5000
                >45
                >0000
                >0009
                >0000
                >0000
                >00
                >530000
                >00
                >04
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >0000
                >00
                >26
                >0073
                >656c
                >65637420
                >3120
                >6672
                >6f6d
                >20
                >6c6f
                >6e67
                >5f7365
                >7175
                >656e
                >6365
                >2832
                >2900
                >0000
                >42000000
                >0c
                >0000
                >0000
                >0000
                >00
                >00
                >44
                >0000
                >0006
                >50
                >0045
                >0000
                >0009
                >00
                >00
                >00
                >0000
                >530000
                >0004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >5000
                >00
                >00
                >2a00
                >73
                >65
                >6c65
                >6374
                >2031
                >2c32
                >2c
                >33
                >20
                >6672
                >6f6d
                >20
                >6c6f
                >6e67
                >5f
                >73657175656e6365283129
                >0000
                >0042000000
                >0c000000000000
                >00
                >00
                >44
                >00
                >0000
                >06
                >5000
                >4500
                >00
                >00
                >0900
                >00
                >00
                >0000
                >53000000
                >04
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >0000
                >0026
                >0073
                >65
                >6c
                >6563
                >7420
                >312066
                >726f
                >6d
                >206c
                >6f
                >6e67
                >5f73
                >65
                >7175
                >656e
                >6365
                >283229
                >00
                >0000
                >420000000c
                >000000
                >00
                >0000
                >00
                >00
                >44
                >0000
                >000650
                >0045
                >00
                >00
                >0009
                >0000
                >00
                >00
                >00
                >5300
                >00
                >0004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >50
                >00
                >00
                >00
                >2a
                >0073
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >2c
                >32
                >2c
                >3320
                >6672
                >6f
                >6d
                >20
                >6c6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >31
                >29
                >000000420000000c
                >0000000000
                >00
                >000044
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >0900
                >00
                >00
                >00
                >00
                >530000
                >00
                >04
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >00
                >00
                >00
                >2600
                >7365
                >6c
                >6563
                >74
                >20
                >3120
                >66
                >72
                >6f
                >6d
                >20
                >6c
                >6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >0000
                >00
                >00
                >00
                >00
                >4400
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >0000
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >50
                >0000
                >00
                >2d
                >53
                >5f
                >31
                >0073
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >2c
                >32
                >2c
                >33
                >2066
                >72
                >6f
                >6d
                >206c
                >6f
                >6e
                >675f
                >7365
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >31
                >2900
                >00
                >00
                >420000000f0053
                >5f
                >31
                >0000
                >00
                >00
                >00
                >00
                >00
                >44
                >0000
                >0006
                >50
                >00
                >4500
                >00
                >00
                >09
                >0000
                >00
                >00
                >00
                >530000
                >0004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >5000
                >00
                >00
                >26
                >00
                >73
                >656c
                >65
                >63
                >74
                >20
                >31
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c
                >6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >6365
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >44
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >0000
                >0009
                >00
                >00
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >4200
                >00
                >00
                >0f
                >00
                >53
                >5f
                >31
                >0000
                >00
                >00
                >00
                >00
                >00
                >45000000
                >0900
                >00
                >00
                >00
                >0053
                >00000004
                <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >5000
                >00
                >00
                >26
                >00
                >73
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c6f6e675f73657175656e63
                >65283229000000
                >420000000c00
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >44
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >53000000
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >42
                >00
                >00
                >00
                >0f
                >00
                >53
                >5f
                >31
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >000000
                >5300
                >0000
                >04
                <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >5000
                >00
                >00
                >26
                >00
                >73
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c
                >6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >44
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >42
                >00
                >00
                >00
                >0f
                >00
                >53
                >5f
                >31
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >53
                >00
                >00
                >00
                >04
                <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >00
                >00
                >00
                >2600
                >73
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c
                >6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >00
                >0000
                >0000
                >00
                >4400
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >42
                >00
                >00
                >00
                >0f
                >00
                >53
                >5f
                >31
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >45
                >00
                >00
                >00
                >09
                >0000
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >00
                >00
                >00
                >26
                >00
                >73
                >65
                >6c
                >65
                >63
                >74
                >2031
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c6f6e675f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >44
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >42
                >00
                >00
                >00
                >0f
                >00
                >53
                >5f
                >31
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >53
                >00
                >00
                >00
                >04
                <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                >50
                >00
                >00
                >00
                >26
                >00
                >73
                >65
                >6c
                >65
                >63
                >74
                >20
                >31
                >20
                >66
                >72
                >6f
                >6d
                >20
                >6c
                >6f
                >6e
                >67
                >5f
                >73
                >65
                >71
                >75
                >65
                >6e
                >63
                >65
                >28
                >32
                >29
                >00
                >00
                >00
                >420000
                >00
                >0c
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >00
                >44
                >00
                >00
                >00
                >06
                >50
                >00
                >45
                >00
                >00
                >00
                >09
                >00
                >00
                >00
                >00
                >00
                >5300
                >00
                >00
                >04
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >5800000004
                """;
        assertHexScript(
                getFragmentedSendFacade(),
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testAllTypesSelectExtended() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            CallableStatement stmt = connection.prepareCall(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," + // str
                            " rnd_str(1,1,2) c," + // str
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," + // str
                            " rnd_long() j," +
                            " timestamp_sequence(889001, 8890012) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 16, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_char() cc," + // str
                            " rnd_long256() l2," + // str
                            " rnd_varchar(3,16,2) v" + // str
                            " from long_sequence(15)) timestamp(k) partition by DAY"
            );

            stmt.execute();
            mayDrainWalQueue();

            try (PreparedStatement statement = connection.prepareStatement("x")) {
                for (int i = 0; i < 50; i++) {
                    sink.clear();
                    try (ResultSet rs = statement.executeQuery()) {
                        // dump metadata
                        assertResultSet(
                                """
                                        kk[INTEGER],a[INTEGER],b[BIT],c[VARCHAR],d[DOUBLE],e[REAL],f[SMALLINT],g[TIMESTAMP],i[VARCHAR],j[BIGINT],k[TIMESTAMP],l[SMALLINT],m[BINARY],n[VARCHAR],cc[CHAR],l2[VARCHAR],v[VARCHAR]
                                        1,1569490116,false,Z,null,0.7611029,428,2015-05-16 20:27:48.158,VTJW,-8671107786057422727,1970-01-01 00:00:00.889001,26,00000000 68 61 26 af 19 c4 95 94 36 53 49 b4 59 7e,null,W,0xc2593f82b430328d84a09f29df637e3863eb3740c80f661e9c8afa23e6ca6ca1,鉾檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}
                                        2,-10505757,true,null,0.40455469747939254,0.88374215,862,2015-09-17 20:47:08.536,null,-4608960730952244094,1970-01-01 00:00:09.779013,2,00000000 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e,LYXWCKYLSU,W,0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885,null
                                        3,2060263242,false,L,null,0.34947264,869,2015-05-15 18:43:06.827,CPSW,-5439556746612026472,1970-01-01 00:00:18.669025,11,null,JSMSSUQSRLTKVVSJ,O,0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b,"+zMKZ 4xL?49Mq
                                        4,923501161,true,E,0.8595900073631431,0.6583311,870,2015-06-28 03:15:43.251,PEHN,-1798101751056570485,1970-01-01 00:00:27.559037,50,00000000 34 04 23 8d d8 57 91 88 28 a5 18 93 bd,PJOXPKRG,I,0x62a2f11e8510a3e99cb8fc6467028eb0a07934b2a15de8e0550988dbaca49734,-Ь\uDA23\uDF64m\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66
                                        5,-1594425659,false,L,0.20727557301543031,0.08675945,871,2015-01-03 14:55:27.713,VTJW,3820631780839257855,1970-01-01 00:00:36.449049,12,00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4,GMXUKLGMXSLUQDYO,P,0x9bae41871fd934427cbab83425e7712e47bb6f42d5f825fb52319f105d14eb26,mPO=I~9)
                                        6,-255808425,true,G,0.28964821678040487,0.47705013,706,null,null,9029088579359707814,1970-01-01 00:00:45.339061,31,00000000 48 d4 41 9d fb 49 40 44 49 96 cf 2b b3 71 a7,MIGQZVKHTLQZSLQV,F,0xb21ebf27d20c0c5ba58b9151b05d33577d4456fa92fbc5b266e891b7af142fd6,l⤃堝ᢣ΄BǬ\uDB37\uDC95Qǜbȶ\u05EC˟
                                        7,890407955,false,E,0.0031075670450616544,0.12384617,366,null,HYRX,-5602486220193156045,1970-01-01 00:00:54.229073,20,null,QQUWQ,O,0xdc9aef010871b1fedfd79391d4cc2a2e7dcd37091f5dac023cc96390430d88ac,A'ò墠
                                        8,-795877457,false,Y,null,null,232,2015-01-25 14:54:10.798,null,-5783981180087707423,1970-01-01 00:01:03.119085,47,null,KJSMKIXEYVTUPDHH,G,0x2f872563b25ea913455b1f46fe7f40cd2337f7e6b82ebc2405c5c1b231cffa45,null
                                        9,-1060590724,true,H,null,0.76208127,268,2015-09-22 07:01:04.042,PEHN,3292156476231287573,1970-01-01 00:01:12.009097,27,00000000 c3 2f ed b0 ba 08 e0 2c ee 41 de,null,E,0x8627da676e13e34b7893e5d5f1ce706c3d5e569a2485dfb77c868671877614df,nM_a~hgMw1$c~{
                                        10,-116429939,false,H,null,0.40425098,81,null,CPSW,-8557532716763860362,1970-01-01 00:01:20.899109,22,null,YXPVKNCBWLNLRH,W,0x7959b9679660fde98cb91e95a6c70b0c689799a1912d7c5a74edb5a7633f86d1,haB]6O}(2D
                                        11,1926049591,true,null,null,0.73383796,827,2015-12-10 01:09:10.433,null,7133205196190817214,1970-01-01 00:01:29.789121,35,00000000 ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6,BWVLOM,P,0x5086c740f96dbb943c3a3b7947ce8369926cbcb16e9a2f11cfab70f2d175d0d9,YC_IBut$
                                        12,-1108612462,false,null,0.4421551587238961,null,25,null,null,-8769824513195810344,1970-01-01 00:01:38.679133,44,null,EPGIUQ,Z,0x78cba77d5205a7df8bb0645af60f7a1fb166288cc3685d60a15d60f88f9d6d92,_-"k[JYtuW/t]}%
                                        13,878060915,true,O,0.38881940598288367,0.44441247,628,2015-01-15 10:30:41.186,PEHN,8732050720474492412,1970-01-01 00:01:47.569145,41,00000000 88 f3 32 27 70 c8 01 b0 dc c9 3a 5b 7e,UXCDK,D,0x793ab30b4920c6775f52a6e644f22fb36dd16cf2d32f169e45f3be35eaae37a2,읈ҽ\uDA01\uDE60
                                        14,1510122165,false,G,null,0.3595577,918,2015-05-31 19:50:48.744,null,4768494777702471066,1970-01-01 00:01:56.459157,11,null,CWLFORGF,I,0xa62eec8815a4ecb2cba1f946c8072d3fbe016461a36853338c0e98b4ae768477,9y_G3>XzlGEYD
                                        15,-2038288432,true,N,0.06052105248562101,0.18684262,196,null,null,-1429876300179126818,1970-01-01 00:02:05.349169,15,00000000 81 e7 a2 16 22 35 3b 1c 9c 1d 5c,DYRODIPUNR,P,0xcfbc17960ded8623a35a624468b2006e75f5d434af616ffc9929b6a51b9efb10,Ƨ阇1(rոҊG\uD9A6\uDD42
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }

            // run some random SQLs
            final String header = "kk[INTEGER],a[INTEGER],b[BIT],c[VARCHAR],d[DOUBLE],e[REAL],f[SMALLINT],g[TIMESTAMP],i[VARCHAR],j[BIGINT],k[TIMESTAMP],l[SMALLINT],m[BINARY],n[VARCHAR],cc[CHAR],l2[VARCHAR],v[VARCHAR]\n";

            final String[] results = {
                    "1,1569490116,false,Z,null,0.7611029,428,2015-05-16 20:27:48.158,VTJW,-8671107786057422727,1970-01-01 00:00:00.889001,26,00000000 68 61 26 af 19 c4 95 94 36 53 49 b4 59 7e,null,W,0xc2593f82b430328d84a09f29df637e3863eb3740c80f661e9c8afa23e6ca6ca1,鉾檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}\n",
                    "2,-10505757,true,null,0.40455469747939254,0.88374215,862,2015-09-17 20:47:08.536,null,-4608960730952244094,1970-01-01 00:00:09.779013,2,00000000 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e,LYXWCKYLSU,W,0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885,null\n",
                    "3,2060263242,false,L,null,0.34947264,869,2015-05-15 18:43:06.827,CPSW,-5439556746612026472,1970-01-01 00:00:18.669025,11,null,JSMSSUQSRLTKVVSJ,O,0x9502128cda0887fe3cdb8640c107a6927eb6d80649d1dfe38e4a7f661df6c32b,\"+zMKZ 4xL?49Mq\n",
                    "4,923501161,true,E,0.8595900073631431,0.6583311,870,2015-06-28 03:15:43.251,PEHN,-1798101751056570485,1970-01-01 00:00:27.559037,50,00000000 34 04 23 8d d8 57 91 88 28 a5 18 93 bd,PJOXPKRG,I,0x62a2f11e8510a3e99cb8fc6467028eb0a07934b2a15de8e0550988dbaca49734,-Ь\uDA23\uDF64m\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\n",
                    "5,-1594425659,false,L,0.20727557301543031,0.08675945,871,2015-01-03 14:55:27.713,VTJW,3820631780839257855,1970-01-01 00:00:36.449049,12,00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4,GMXUKLGMXSLUQDYO,P,0x9bae41871fd934427cbab83425e7712e47bb6f42d5f825fb52319f105d14eb26,mPO=I~9)\n",
                    "6,-255808425,true,G,0.28964821678040487,0.47705013,706,null,null,9029088579359707814,1970-01-01 00:00:45.339061,31,00000000 48 d4 41 9d fb 49 40 44 49 96 cf 2b b3 71 a7,MIGQZVKHTLQZSLQV,F,0xb21ebf27d20c0c5ba58b9151b05d33577d4456fa92fbc5b266e891b7af142fd6,l⤃堝ᢣ΄BǬ\uDB37\uDC95Qǜbȶ\u05EC˟\n",
                    "7,890407955,false,E,0.0031075670450616544,0.12384617,366,null,HYRX,-5602486220193156045,1970-01-01 00:00:54.229073,20,null,QQUWQ,O,0xdc9aef010871b1fedfd79391d4cc2a2e7dcd37091f5dac023cc96390430d88ac,A'ò墠\n",
                    "8,-795877457,false,Y,null,null,232,2015-01-25 14:54:10.798,null,-5783981180087707423,1970-01-01 00:01:03.119085,47,null,KJSMKIXEYVTUPDHH,G,0x2f872563b25ea913455b1f46fe7f40cd2337f7e6b82ebc2405c5c1b231cffa45,null\n",
                    "9,-1060590724,true,H,null,0.76208127,268,2015-09-22 07:01:04.042,PEHN,3292156476231287573,1970-01-01 00:01:12.009097,27,00000000 c3 2f ed b0 ba 08 e0 2c ee 41 de,null,E,0x8627da676e13e34b7893e5d5f1ce706c3d5e569a2485dfb77c868671877614df,nM_a~hgMw1$c~{\n",
                    "10,-116429939,false,H,null,0.40425098,81,null,CPSW,-8557532716763860362,1970-01-01 00:01:20.899109,22,null,YXPVKNCBWLNLRH,W,0x7959b9679660fde98cb91e95a6c70b0c689799a1912d7c5a74edb5a7633f86d1,haB]6O}(2D\n",
                    "11,1926049591,true,null,null,0.73383796,827,2015-12-10 01:09:10.433,null,7133205196190817214,1970-01-01 00:01:29.789121,35,00000000 ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6,BWVLOM,P,0x5086c740f96dbb943c3a3b7947ce8369926cbcb16e9a2f11cfab70f2d175d0d9,YC_IBut$\n",
                    "12,-1108612462,false,null,0.4421551587238961,null,25,null,null,-8769824513195810344,1970-01-01 00:01:38.679133,44,null,EPGIUQ,Z,0x78cba77d5205a7df8bb0645af60f7a1fb166288cc3685d60a15d60f88f9d6d92,_-\"k[JYtuW/t]}%\n",
                    "13,878060915,true,O,0.38881940598288367,0.44441247,628,2015-01-15 10:30:41.186,PEHN,8732050720474492412,1970-01-01 00:01:47.569145,41,00000000 88 f3 32 27 70 c8 01 b0 dc c9 3a 5b 7e,UXCDK,D,0x793ab30b4920c6775f52a6e644f22fb36dd16cf2d32f169e45f3be35eaae37a2,읈ҽ\uDA01\uDE60\n",
                    "14,1510122165,false,G,null,0.3595577,918,2015-05-31 19:50:48.744,null,4768494777702471066,1970-01-01 00:01:56.459157,11,null,CWLFORGF,I,0xa62eec8815a4ecb2cba1f946c8072d3fbe016461a36853338c0e98b4ae768477,9y_G3>XzlGEYD\n",
                    "15,-2038288432,true,N,0.06052105248562101,0.18684262,196,null,null,-1429876300179126818,1970-01-01 00:02:05.349169,15,00000000 81 e7 a2 16 22 35 3b 1c 9c 1d 5c,DYRODIPUNR,P,0xcfbc17960ded8623a35a624468b2006e75f5d434af616ffc9929b6a51b9efb10,Ƨ阇1(rոҊG\uD9A6\uDD42\n"
            };

            for (int i = 0; i < 100; i++) {
                sink.clear();
                try (PreparedStatement statement = connection.prepareStatement("x where kk = " + (i + 1))) {
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(header + (i < results.length ? results[i] : ""), sink, rs);
                    }
                }
            }
        });
    }

    @Test
    public void testArrayBindingVars() throws Exception {
        skipOnWalRun();
        // In simple mode bind vars are interpolated into the query text,
        // and we don't have implicit cast from string to array, so test extended mode only.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            final Array arr1 = connection.createArrayOf("float8", new Double[][]{{2d, 4d, 6d}, {8d, 10d, 12d}});
            final Array arr2 = connection.createArrayOf("float8", new Double[][]{{1d, 2d, 3d}, {4d, 5d, 6d}});

            final String[] inserts = new String[]{
                    "insert into x values (? + ?)",
                    "insert into x values (? - ?)",
                    "insert into x values (? * ?)",
                    "insert into x values (? / ?)",
            };
            for (String insert : inserts) {
                // array + array
                try (PreparedStatement stmt = connection.prepareStatement(insert)) {
                    stmt.setArray(1, arr1);
                    stmt.setArray(2, arr2);
                    stmt.execute();
                }
                // array + scalar
                try (PreparedStatement stmt = connection.prepareStatement(insert)) {
                    stmt.setArray(1, arr1);
                    stmt.setLong(2, 1);
                    stmt.execute();
                }
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet(
                            """
                                    al[ARRAY]
                                    {{3.0,6.0,9.0},{12.0,15.0,18.0}}
                                    {{3.0,5.0,7.0},{9.0,11.0,13.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,3.0,5.0},{7.0,9.0,11.0}}
                                    {{2.0,8.0,18.0},{32.0,50.0,72.0}}
                                    {{2.0,4.0,6.0},{8.0,10.0,12.0}}
                                    {{2.0,2.0,2.0},{2.0,2.0,2.0}}
                                    {{2.0,4.0,6.0},{8.0,10.0,12.0}}
                                    """,
                            sink,
                            rs
                    );
                }
            }

            final String[] updates = new String[]{
                    "update x set al = ? + ?",
                    "update x set al = ? - ?",
                    "update x set al = ? * ?",
                    "update x set al = ? / ?",
            };
            for (String update : updates) {
                // array + array
                try (PreparedStatement stmt = connection.prepareStatement(update)) {
                    stmt.setArray(1, arr1);
                    stmt.setArray(2, arr2);
                    stmt.execute();
                }
                // array + scalar
                try (PreparedStatement stmt = connection.prepareStatement(update)) {
                    stmt.setArray(1, arr1);
                    stmt.setLong(2, 2);
                    stmt.execute();
                }
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet(
                            """
                                    al[ARRAY]
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testArrayBindingVarsWrongDimensions() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            final Array arr1 = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d, 4d, 5d, 6d});
            final Array arr2 = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d, 4d, 5d, 6d});

            final String[] inserts = new String[]{
                    "insert into x values (? + ?)",
                    "insert into x values (? - ?)",
                    "insert into x values (? * ?)",
                    "insert into x values (? / ?)",
            };
            for (String insert : inserts) {
                // array + array
                try (PreparedStatement stmt = connection.prepareStatement(insert)) {
                    stmt.setArray(1, arr1);
                    stmt.setArray(2, arr2);
                    try {
                        stmt.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertContains(e.getMessage(), "inconvertible types: DOUBLE[] -> DOUBLE[][]");
                    }
                }
                // array + scalar
                try (PreparedStatement stmt = connection.prepareStatement(insert)) {
                    stmt.setArray(1, arr1);
                    stmt.setLong(2, 42);
                    try {
                        stmt.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertContains(e.getMessage(), "inconvertible types: DOUBLE[] -> DOUBLE[][]");
                    }
                }
            }

            final String[] updates = new String[]{
                    "update x set al = ? + ?",
                    "update x set al = ? - ?",
                    "update x set al = ? * ?",
                    "update x set al = ? / ?",
            };
            for (String update : updates) {
                // array + array
                try (PreparedStatement stmt = connection.prepareStatement(update)) {
                    stmt.setArray(1, arr1);
                    stmt.setArray(2, arr2);
                    try {
                        stmt.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertContains(e.getMessage(), "inconvertible types: DOUBLE[] -> DOUBLE[][]");
                    }
                }
                // array + scalar
                try (PreparedStatement stmt = connection.prepareStatement(update)) {
                    stmt.setArray(1, arr1);
                    stmt.setLong(2, 42);
                    try {
                        stmt.execute();
                        Assert.fail();
                    } catch (SQLException e) {
                        TestUtils.assertContains(e.getMessage(), "inconvertible types: DOUBLE[] -> DOUBLE[][]");
                    }
                }
            }
        });
    }

    @Test
    /*
import asyncio
import asyncpg


async def main():
    pool = await asyncpg.create_pool(
        host="127.0.0.1",
        port="5432",
        database="qdb",
        user="admin",
        password="quest",
        min_size=1,
        max_size=1,
    )
    async with pool.acquire() as connection:
        await connection.fetch("SELECT * FROM thistabledoesnotexist;")


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
     */
    public void testAsyncPgExecutesTableDoesNotExists() throws Exception {
        // Python sends P/D/H
        // Old PGWire responeded with E/Z - Z is incorrect
        // Python then send S
        // Old PGWire did not respond with Z (because it recorded in its state that Z was send with "flush")
        // The 2.0 PGWire does responds to P/D/H with E only, then responds to S with Z. Python is ok with that and
        // this is the correct sequence.
        skipOnWalRun();
        assertHexScript("""
                >0000000804d2162f
                <4e
                >0000003900030000636c69656e745f656e636f64696e6700277574662d382700757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >500000003e5f5f6173796e6370675f73746d745f315f5f0053454c454354202a2046524f4d20746869737461626c65646f65736e6f7465786973743b0000004400000018535f5f6173796e6370675f73746d745f315f5f004800000004
                <450000004b433030303030004d7461626c6520646f6573206e6f74206578697374205b7461626c653d746869737461626c65646f65736e6f7465786973745d00534552524f52005031350000
                >5300000004
                <5a0000000549
                >510000004753454c4543542070675f61647669736f72795f756e6c6f636b5f616c6c28293b0a434c4f534520414c4c3b0a554e4c495354454e202a3b0a524553455420414c4c3b00
                <540000002f000170675f61647669736f72795f756e6c6f636b5f616c6c0000000000000100000413ffffffffffff0000440000000a0001ffffffff430000000d53454c4543542031004300000008534554004300000008534554004300000008534554005a0000000549
                """);
    }

    @Test
    public void testBadMessageLength() throws Exception {
        skipOnWalRun();
        final String script =
                """
                        >0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000006f
                        <!!""";
        assertHexScript(
                getFragmentedSendFacade(),
                script,
                getStdPgWireConfig()
        );
    }

    @Test
    public void testBadPasswordLength() throws Exception {
        skipOnWalRun();
        assertHexScript(
                """
                        >0000000804d2162f
                        <4e
                        >0000007500030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000464756e6e6f00
                        <!!"""
        );
    }

    @Test
    public void testBasicFetch() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;

            PreparedStatement tbl = connection.prepareStatement("create table x (a int)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a) values(?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setInt(1, i);
                insert.execute();
            }
            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
            for (int testSize : testSizes) {
                stmt.setFetchSize(testSize);
                assertEquals(testSize, stmt.getFetchSize());

                ResultSet rs = stmt.executeQuery();
                assertEquals(testSize, rs.getFetchSize());

                int count = 0;
                while (rs.next()) {
                    assertEquals(count, rs.getInt(1));
                    ++count;
                }

                assertEquals(totalRows, count);
            }
        });
    }

    @Test
    public void testBasicFetchIPv4() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;
            IntIntHashMap map = new IntIntHashMap();
            map.put(1, ColumnType.IPv4);

            PreparedStatement tbl = connection.prepareStatement("create table x (a ipv4)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a) values(?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setString(1, "1.1.1.1");
                insert.execute();
            }
            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
            for (int testSize : testSizes) {
                stmt.setFetchSize(testSize);
                assertEquals(testSize, stmt.getFetchSize());

                ResultSet rs = stmt.executeQuery();
                assertEquals(testSize, rs.getFetchSize());
                sink.clear();
                assertResultSet(
                        """
                                a[IPv4]
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                1.1.1.1
                                """,
                        sink,
                        rs,
                        map
                );
            }
        });
    }

    @Test
    public void testBasicFetchIPv4MultiCol() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;
            IntIntHashMap map = new IntIntHashMap();
            map.put(1, ColumnType.IPv4);
            map.put(3, ColumnType.IPv4);

            PreparedStatement tbl = connection.prepareStatement("create table x (a ipv4, b int, c ipv4, d int)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a, b, c, d) values(?, ?, ?, ?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setString(1, "12.2.65.90");
                insert.setInt(2, 5);
                insert.setString(3, "65.34.123.99");
                insert.setInt(4, 22);
                insert.execute();
            }
            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            int[] testSizes = {1, 49, 50, 51, 99, 100, 101};
            for (int testSize : testSizes) {
                System.out.println("testsize: " + testSize);
                stmt.setFetchSize(testSize);
                assertEquals(testSize, stmt.getFetchSize());

                ResultSet rs = stmt.executeQuery();
                assertEquals(testSize, rs.getFetchSize());
                sink.clear();
                assertResultSet(
                        """
                                a[IPv4],b[INTEGER],c[IPv4],d[INTEGER]
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                12.2.65.90,5,65.34.123.99,22
                                """,
                        sink,
                        rs,
                        map
                );
            }
        });
    }

    @Test
    public void testBasicFetchIPv4Null() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;
            IntIntHashMap map = new IntIntHashMap();
            map.put(1, ColumnType.IPv4);

            PreparedStatement tbl = connection.prepareStatement("create table x (a ipv4)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a) values(?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setString(1, null);
                insert.execute();
            }
            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            int[] testSizes = {0, 1, 49, 50, 51, 99, 100, 101};
            for (int testSize : testSizes) {
                stmt.setFetchSize(testSize);
                assertEquals(testSize, stmt.getFetchSize());

                ResultSet rs = stmt.executeQuery();
                assertEquals(testSize, rs.getFetchSize());
                sink.clear();
                assertResultSet(
                        """
                                a[IPv4]
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                null
                                """,
                        sink,
                        rs,
                        map
                );
            }
        });
    }

    @Test
    public void testBatchInsertWithTransaction() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table test (id long,val int)");
                statement.executeUpdate("create table test2(id long,val int)");
            }

            connection.setAutoCommit(false);
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test(id,val) values(?,?)")) {
                batchInsert.setLong(1, 0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();
                batchInsert.setLong(1, 1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();
                batchInsert.setLong(1, 2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                batchInsert.executeLargeBatch();
            }

            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test2(id,val) values(?,?)")) {
                batchInsert.setLong(1, 0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();
                batchInsert.setLong(1, 1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();
                batchInsert.setLong(1, 2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                batchInsert.executeLargeBatch();
            }

            connection.commit();

            connection.setAutoCommit(true);
            StringSink sink = new StringSink();
            String expected = """
                    id[BIGINT],val[INTEGER]
                    0,1
                    1,2
                    2,3
                    """;
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from test");
            assertResultSet(expected, sink, rs);

            sink.clear();
            Statement statement2 = connection.createStatement();
            ResultSet rs2 = statement2.executeQuery("select * from test2");
            assertResultSet(expected, sink, rs2);

            //now switch on autocommit and check that data is inserted without explicitly calling commit()
            connection.setAutoCommit(true);
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test(id,val) values(?,?)")) {
                batchInsert.setLong(1, 3L);
                batchInsert.setInt(2, 4);
                batchInsert.addBatch();
                batchInsert.setLong(1, 4L);
                batchInsert.setInt(2, 5);
                batchInsert.addBatch();
                batchInsert.setLong(1, 5L);
                batchInsert.setInt(2, 6);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                batchInsert.executeLargeBatch();
            }

            sink.clear();
            expected = """
                    id[BIGINT],val[INTEGER]
                    0,1
                    1,2
                    2,3
                    3,4
                    4,5
                    5,6
                    """;
            Statement statement3 = connection.createStatement();
            ResultSet rs3 = statement3.executeQuery("select * from test");
            assertResultSet(expected, sink, rs3);

            //now fail insertion during transaction
            try (Statement statement4 = connection.createStatement()) {
                statement4.executeUpdate("create table anothertab(id long, val int, k timestamp) timestamp(k) ");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into anothertab(id, val, k) values(?,?,?)")) {
                batchInsert.setLong(1, 3L);
                batchInsert.setInt(2, 4);
                batchInsert.setLong(3, 1_000L);
                batchInsert.addBatch();
                batchInsert.setLong(1, 4L);
                batchInsert.setInt(2, 5);
                batchInsert.setLong(3, 0L);
                batchInsert.addBatch();
                batchInsert.setLong(1, 5L);
                batchInsert.setInt(2, 6);
                batchInsert.setLong(3, 2_000L);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                batchInsert.executeLargeBatch();
                Assert.fail();
            } catch (Exception e) {
                LOG.error().$(e).$();
            }
            //now transaction fail, we should rollback transaction
            connection.rollback();
            connection.setAutoCommit(true);
            sink.clear();
            expected = "id[BIGINT],val[INTEGER],k[TIMESTAMP]\n";
            Statement statement4 = connection.createStatement();
            ResultSet rs4 = statement4.executeQuery("select * from anothertab");
            assertResultSet(expected, sink, rs4);
        });
    }

    @Test
    public void testBindVariableDropLastPartitionListByMonthHigherPrecision() throws Exception {
        testBindVariableDropLastPartitionListWithDatePrecision(PartitionBy.MONTH);
    }

    @Test
    public void testBindVariableDropLastPartitionListByNoneHigherPrecision() throws Exception {
        try {
            testBindVariableDropLastPartitionListWithDatePrecision(PartitionBy.NONE);
            Assert.fail();
        } catch (PSQLException e) {
            TestUtils.assertContains(e.getMessage(), "ERROR: table is not partitioned");
        }
    }

    @Test
    public void testBindVariableDropLastPartitionListByWeekHigherPrecision() throws Exception {
        testBindVariableDropLastPartitionListWithDatePrecision(PartitionBy.WEEK);
    }

    @Test
    public void testBindVariableDropLastPartitionListWithWeekPrecision() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE x (l LONG, ts TIMESTAMP, date DATE) TIMESTAMP(ts) PARTITION BY WEEK").execute();
            connection.prepareStatement("INSERT INTO x VALUES (12, '2023-02-11T11:12:22.116234Z', '2023-02-11'::date)").execute();
            connection.prepareStatement("INSERT INTO x VALUES (13, '2023-02-12T16:42:00.333999Z', '2023-02-12'::date)").execute();
            connection.prepareStatement("INSERT INTO x VALUES (14, '2023-03-21T03:52:00.999999Z', '2023-03-21'::date)").execute();
            connection.commit();
            mayDrainWalQueue();
            try (PreparedStatement dropPartition = connection.prepareStatement("ALTER TABLE x DROP PARTITION LIST '" + "2023-02-06T09" + "' ;")) {
                Assert.assertFalse(dropPartition.execute());
            }
            mayDrainWalQueue();
            try (
                    PreparedStatement select = connection.prepareStatement("x");
                    ResultSet rs = select.executeQuery()
            ) {
                sink.clear();
                assertResultSet(
                        """
                                l[BIGINT],ts[TIMESTAMP],date[TIMESTAMP]
                                14,2023-03-21 03:52:00.999999,2023-03-21 00:00:00.0
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testBindVariableInFilter() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table x (l long, ts timestamp) timestamp(ts) partition by YEAR").execute();
            connection.prepareStatement("insert into x values (100, 0)").execute();
            connection.prepareStatement("insert into x values (101, 1)").execute();
            connection.prepareStatement("insert into x values (102, 2)").execute();
            connection.prepareStatement("insert into x values (103, 3)").execute();
            connection.commit();

            mayDrainWalQueue();
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where l != ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    l[BIGINT],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    101,1970-01-01 00:00:00.000001
                                    102,1970-01-01 00:00:00.000002
                                    103,1970-01-01 00:00:00.000003
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInVarArg() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table x (s symbol, ts timestamp) timestamp(ts) partition by YEAR").execute();
            connection.prepareStatement("insert into x values ('a', 0)").execute();
            connection.prepareStatement("insert into x values ('b', 1)").execute();
            connection.prepareStatement("insert into x values ('a', 2)").execute();
            connection.prepareStatement("insert into x values ('c', 3)").execute();
            connection.prepareStatement("insert into x values (null, 4)").execute();
            connection.commit();

            mayDrainWalQueue();

            try (PreparedStatement ps = connection.prepareStatement("select * from x where s in (?, ?, ?)")) {
                sink.clear();
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.setString(3, null);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    s[VARCHAR],ts[TIMESTAMP]
                                    a,1970-01-01 00:00:00.0
                                    b,1970-01-01 00:00:00.000001
                                    a,1970-01-01 00:00:00.000002
                                    null,1970-01-01 00:00:00.000004
                                    """,
                            sink,
                            rs
                    );
                }

                sink.clear();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.setString(3, "a");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    s[VARCHAR],ts[TIMESTAMP]
                                    a,1970-01-01 00:00:00.0
                                    a,1970-01-01 00:00:00.000002
                                    c,1970-01-01 00:00:00.000003
                                    null,1970-01-01 00:00:00.000004
                                    """,
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("select * from x where ts in (?, ?)")) {
                sink.clear();
                ps.setString(1, "1970-01-01 00:00:00.000001");
                ps.setString(2, "1970-01-01 00:00:00.000004");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    s[VARCHAR],ts[TIMESTAMP]
                                    b,1970-01-01 00:00:00.000001
                                    null,1970-01-01 00:00:00.000004
                                    """,
                            sink,
                            rs
                    );
                }

                sink.clear();
                ps.setString(1, "1970-01-01 00:00:00.000002");
                ps.setString(2, "1970-01-01 00:00:00.000005");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    s[VARCHAR],ts[TIMESTAMP]
                                    a,1970-01-01 00:00:00.000002
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

//Testing through postgres - need to establish connection
//    @Test
//    public void testReadINet() throws SQLException, IOException {
//        Properties properties = new Properties();
//        properties.setProperty("user", "admin");
//        properties.setProperty("password", "postgres");
//        properties.setProperty("sslmode", "disable");
//        properties.setProperty("binaryTransfer", Boolean.toString(true));
//        properties.setProperty("preferQueryMode", Mode.EXTENDED.value);
//        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
//
//        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/postgres", 5432);
//
//        try (final Connection connection = DriverManager.getConnection(url, properties)) {
//            var stmt = connection.prepareStatement("select * from ipv4");
//            ResultSet rs = stmt.executeQuery();
//            assertResultSet("a[OTHER]\n" +
//                    "1.1.1.1\n" +
//                    "12.2.65.90\n", sink, rs);
//        }
//    }

    @Test
    public void testBindVariableIsNotNull() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table tab1 (value int, ts timestamp) timestamp(ts) partition by DAY").execute();
            connection.prepareStatement("insert into tab1 (value, ts) values (100, 0)").execute();
            connection.prepareStatement("insert into tab1 (value, ts) values (null, 1)").execute();
            connection.commit();
            connection.setAutoCommit(true);

            mayDrainWalQueue();
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where 3 is not null")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where coalesce(3.14, 12.37) is not null")) {
                // 'is not' is an alias for '!=', the matching type for this operator
                // (with null on the right) is DOUBLE
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is not null")) {
                // 'is not' is an alias for '!=', the matching type for this operator
                // (with null on the right) is DOUBLE
                ps.setDouble(1, 3.14);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is not null")) {
                ps.setDouble(1, Double.NaN);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "value[INTEGER],ts[TIMESTAMP]\n",
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is not null")) {
                ps.setInt(1, Numbers.INT_NULL);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "value[INTEGER],ts[TIMESTAMP]\n",
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is not null")) {
                ps.setInt(1, 12);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            // not an error in PGWire 2.0
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is not null")) {
                ps.setString(1, "");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
                ps.setString(1, "cah-cha-cha");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is not ?")) {
                ps.setString(1, "NULL");
                try (ResultSet ignore1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS NOT must be followed by NULL");
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is not ?")) {
                ps.setDouble(1, Double.NaN);
                try (ResultSet ignore1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS NOT must be followed by NULL");
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is not ?")) {
                ps.setNull(1, Types.NULL);
                try (ResultSet ignored1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS NOT must be followed by NULL");
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where value is not ?")) {
                ps.setString(1, "NULL");
                try (ResultSet ignored1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS NOT must be followed by NULL");
                }
            }
        });
    }

//Testing through postgres - need to establish connection
//    @Test
//    public void testReadINet() throws SQLException, IOException {
//        Properties properties = new Properties();
//        properties.setProperty("user", "admin");
//        properties.setProperty("password", "postgres");
//        properties.setProperty("sslmode", "disable");
//        properties.setProperty("binaryTransfer", Boolean.toString(true));
//        properties.setProperty("preferQueryMode", Mode.EXTENDED.value);
//        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
//
//        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/postgres", 5432);
//
//        try (final Connection connection = DriverManager.getConnection(url, properties)) {
//            var stmt = connection.prepareStatement("select * from ipv4");
//            ResultSet rs = stmt.executeQuery();
//            assertResultSet("a[OTHER]\n" +
//                    "1.1.1.1\n" +
//                    "12.2.65.90\n", sink, rs);
//        }
//    }

    @Test
    public void testBindVariableIsNull() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table tab1 (value int, ts timestamp) timestamp(ts) partition by YEAR").execute();
            connection.prepareStatement("insert into tab1 (value, ts) values (100, 0)").execute();
            connection.prepareStatement("insert into tab1 (value, ts) values (null, 1)").execute();
            connection.commit();
            connection.setAutoCommit(true);

            mayDrainWalQueue();
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is null")) {
                try (ResultSet rs = ps.executeQuery()) {
                    // all rows, null = null is always true
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where (? | null) is null")) {
                ps.setLong(1, 1066);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is null")) {
                // 'is' is an alias for '=', the matching type for this operator, with null
                // on the right, is DOUBLE (EqDoubleFunctionFactory)
                ps.setDouble(1, Double.NaN);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is null")) {
                // type information is lost in text mode; Numbers.INT_NaN is transmitted as "-2147483648" string
                // and bind variable type is set to BYTEA, despite us calling setInt()
                // server cannot assume that the client is sending null
                ps.setInt(1, Numbers.INT_NULL);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    value[INTEGER],ts[TIMESTAMP]
                                    100,1970-01-01 00:00:00.0
                                    null,1970-01-01 00:00:00.000001
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is null")) {
                // 'is' is an alias for '=', the matching type for this operator
                // (with null on the right) is DOUBLE, and thus INT is a valid
                // value type
                ps.setInt(1, 21);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "value[INTEGER],ts[TIMESTAMP]\n",
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where ? is null")) {
                ps.setString(1, "");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "value[INTEGER],ts[TIMESTAMP]\n",
                            sink,
                            rs
                    );
                }
                ps.setString(1, "cha-cha-cha");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "value[INTEGER],ts[TIMESTAMP]\n",
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where value is ?")) {
                ps.setString(1, "NULL");
                try (ResultSet ignore1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS must be followed by NULL");

                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is ?")) {
                ps.setDouble(1, Double.NaN);
                try (ResultSet ignore1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS must be followed by NULL");
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where null is ?")) {
                ps.setNull(1, Types.NULL);
                try (ResultSet ignored1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS must be followed by NULL");
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("tab1 where value is ?")) {
                ps.setString(1, "NULL");
                try (ResultSet ignored1 = ps.executeQuery()) {
                    Assert.fail();
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "IS must be followed by NULL");
                }
            }
        });
    }

    @Test
    public void testBindVariablesWithIndexedSymbolInFilter() throws Exception {
        testBindVariablesWithIndexedSymbolInFilter(true);
    }

    @Test
    public void testBindVariablesWithNonIndexedSymbolInFilter() throws Exception {
        testBindVariablesWithIndexedSymbolInFilter(false);
    }

    @Test
    public void testBlobOverLimit() throws Exception {
        skipOnWalRun(); // non-partitioned
        PGConfiguration configuration = new Port0PGConfiguration() {
            @Override
            public int getMaxBlobSizeOnQuery() {
                return 150;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(configuration);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try {
                    try (final Connection connection = getConnection(server.getPort(), false, true)) {
                        Statement statement = connection.createStatement();
                        statement.executeQuery(
                                "select " +
                                        "rnd_str(4,4,4) s, " +
                                        "rnd_int(0, 256, 4) i, " +
                                        "rnd_double(4) d, " +
                                        "timestamp_sequence(0,10000) t, " +
                                        "rnd_float(4) f, " +
                                        "rnd_short() _short, " +
                                        "rnd_long(0, 10000000, 5) l, " +
                                        "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                        "rnd_byte(0,127) bb, " +
                                        "rnd_boolean() b, " +
                                        "rnd_symbol(4,4,4,2), " +
                                        "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                        "rnd_bin(1024,2048,2) " +
                                        "from long_sequence(50)");

                        Assert.fail();
                    }
                } catch (PSQLException e) {
                    Assert.assertNotNull(e.getServerErrorMessage());
                    TestUtils.assertContains(e.getServerErrorMessage().getMessage(), "blob is too large");
                }
            }
        });
    }

    @Test
    public void testBrokenUtf8QueryInParseMessage() throws Exception {
        skipOnWalRun(); // non-partitioned
        // the modern server will return actionable error message to the client
        assertHexScript(
                """
                        >0000000804d2162f
                        <4e
                        >0000007500030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >50000000220053ac542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <4500000039433030303030004d696e76616c6964205554463820627974657320696e20706172736520717565727900534552524f5200503100005a0000000549
                        """
        );
    }

    @Test
    public void testByteBindingVariable() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (a byte)");
            statement.execute();

            try (final PreparedStatement insert = connection.prepareStatement("insert into x values (?)")) {
                // the parameter must be null so client does not know the type and parse message won't have types specified
                // this makes the compiler to derive the type from the column type as BYTE
                insert.setObject(1, null);
                insert.execute();
            }

            try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                sink.clear();
                assertResultSet("""
                        a[SMALLINT]
                        0
                        """, sink, resultSet);
            }
        });
    }

    @Test
    public void testCairoException() throws Exception {
        skipOnWalRun(); // non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("create table xyz(a int)").execute();
            try (TableWriter ignored1 = getWriter("xyz")) {
                connection.prepareStatement("drop table xyz").execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "could not lock 'xyz~'");
                Assert.assertEquals("00000", e.getSQLState());
            }
        });
    }

    @Test
    public void testCancelOneQueryOutOfMultipleRunningOnes() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table if not exists tab as " +
                    "(select x::timestamp ts, x, rnd_double() d " +
                    "from long_sequence(1)) " +
                    "timestamp(ts) " +
                    "partition by day");
            mayDrainWalQueue();

            final int THREADS = 5;
            final int BLOCKED_THREAD = 3;

            ObjList<Connection> conns = new ObjList<>();
            final long[] results = new long[THREADS];
            final CountDownLatch startLatch = new CountDownLatch(THREADS);
            final CountDownLatch endLatch = new CountDownLatch(THREADS);
            final AtomicInteger errorCount = new AtomicInteger();

            try {
                for (int i = 0; i < THREADS; i++) {
                    conns.add(getConnection(mode, port, binary));
                }

                for (int i = 0; i < THREADS; i++) {
                    final int j = i;
                    new Thread(() -> {
                        final String query = (j == BLOCKED_THREAD) ?
                                "select count(*) from tab t1 join tab t2 on t1.x = t2.x where sleep(12000)" :
                                "select count(*) from tab where sleep(100)";
                        try (PreparedStatement stmt = conns.getQuick(j).prepareStatement(query)) {
                            startLatch.countDown();
                            startLatch.await();
                            try (ResultSet rs = stmt.executeQuery()) {
                                rs.next();
                                results[j] = rs.getLong(1);
                            }
                        } catch (Throwable e) {
                            if (!Chars.containsLowerCase(e.getMessage(), "cancelled by user")) {
                                LOG.error().$(e).$();
                                errorCount.incrementAndGet();
                            }
                        } finally {
                            endLatch.countDown();
                        }
                    }).start();
                }

                while (endLatch.getCount() > 0) {
                    Os.sleep(1);
                    ((PgConnection) conns.getQuick(BLOCKED_THREAD)).cancelQuery();
                }

                for (int i = 0; i < THREADS; i++) {
                    Assert.assertEquals(i != BLOCKED_THREAD ? 1 : 0, results[i]);
                }

                Assert.assertEquals(0, errorCount.get());
            } finally {
                for (int i = 0, n = conns.size(); i < n; i++) {
                    conns.getQuick(i).close();
                }
            }
        });
    }

    @Test
    public void testCancelQueryThatReusesCircuitBreakerFromPreviousConnection() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table if not exists tab as (select x::timestamp ts, x, rnd_double() d from long_sequence(1)) timestamp(ts) partition by day");
            mayDrainWalQueue();

            try (
                    final PGServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                int backendPid;

                //first connection
                try (final PgConnection connection = (PgConnection) getConnection(server.getPort(), false, true)) {
                    backendPid = executeAndCancelQuery(connection);
                }

                PgConnection sameConn;

                while (true) {
                    try {
                        final PgConnection conn = (PgConnection) getConnection(server.getPort(), false, true);
                        if (backendPid == conn.getQueryExecutor().getBackendPID()) {
                            sameConn = conn;
                            break;
                        } else {
                            conn.close();
                        }
                    } catch (PSQLException e) {
                        // ignore the error and retry
                    }
                }

                //first run query and complete
                try (final PreparedStatement stmt = sameConn.prepareStatement("select count(*) from tab where x > 0")) {
                    ResultSet result = stmt.executeQuery();
                    sink.clear();
                    assertResultSet("count[BIGINT]\n1\n", sink, result);

                    //then run query and cancel
                    executeAndCancelQuery(sameConn);
                } finally {
                    sameConn.close();
                }
            }
        });
    }

    @Test
    public void testCancelRunningQuery() throws Exception {
        String[] queries = {
                "create table new_tab as (select count(*) from tab t1 join tab t2 on t1.x = t2.x where sleep(120000))",
                "select count(*) from tab t1 join tab t2 on t1.x = t2.x where sleep(120000)",
                "insert into dest select count(*)::timestamp, 0, 0.0 from tab t1 join tab t2 on t1.x = t2.x where sleep(120000)",
                "update dest set l = t1.x from (tab where d > 0 limit 1, -1 ) t1 where sleep(120000)"
        };
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table if not exists tab as " +
                    "(select x::timestamp ts, x, rnd_double() d from long_sequence(5)) timestamp(ts) partition by day");
            execute("create table if not exists dest as (select x l from long_sequence(10))");
            mayDrainWalQueue();

            for (String query : queries) {
                AtomicBoolean isCancelled = new AtomicBoolean(false);
                CountDownLatch finished = new CountDownLatch(1);

                try (final PreparedStatement stmt = connection.prepareStatement(query)) {
                    new Thread(() -> {
                        PGConnection pgCon = (PGConnection) connection;
                        try {
                            while (!isCancelled.get()) {
                                Os.sleep(1);
                                pgCon.cancelQuery();
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        } finally {
                            finished.countDown();
                        }
                    }, "cancellation thread").start();
                    try {
                        stmt.execute();
                        Assert.fail("expected PSQLException with cancel message for query: " + query);
                    } catch (PSQLException e) {
                        isCancelled.set(true);
                        finished.await();
                        assertContains(e.getMessage(), "cancelled by user");
                    }
                }
            }
        });
    }

    @Test
    public void testCharIntLongDoubleBooleanParametersWithoutExplicitParameterTypeHex() throws Exception {
        skipOnWalRun(); // non-partitioned
        String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000
                >4200000021000000010000000200000001330000000a35303030303030303030000044000000065000450000000900000000004800000004
                <31000000043200000004540000004400037800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000024320000000000000300000413ffffffffffff0000440000001e0003000000013100000001330000000a35303030303030303030440000001e0003000000013200000001330000000a35303030303030303030430000000d53454c454354203200
                """;

        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testCloseMessageFollowedByNewQueryHex() throws Exception {
        skipOnWalRun(); // non-partitioned
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >430000000953535f310050000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <330000000431000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >5800000004
                        """
        );
    }

    @Test
    public void testCloseMessageForPortalHex() throws Exception {
        skipOnWalRun(); // non-partitioned
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >430000000950535f31005300000004
                        """
        );
    }

    @Test
    public void testCloseMessageForSelectWithParamsHex() throws Exception {
        skipOnWalRun();
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                        <31000000043200000004540000005900047800000000000001000000140008ffffffff0000243100000000000002000000170004ffffffff0000243200000000000003000000140008ffffffff0000243300000000000004000002bd0008ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                        <31000000043200000004540000005900047800000000000001000000140008ffffffff0000243100000000000002000000170004ffffffff0000243200000000000003000000140008ffffffff0000243300000000000004000002bd0008ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                        <31000000043200000004540000005900047800000000000001000000140008ffffffff0000243100000000000002000000170004ffffffff0000243200000000000003000000140008ffffffff0000243300000000000004000002bd0008ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >500000003b0073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002600000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                        <31000000043200000004540000005900047800000000000001000000140008ffffffff0000243100000000000002000000170004ffffffff0000243200000000000003000000140008ffffffff0000243300000000000004000002bd0008ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >500000003e535f310073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e63652832290000030000001700000014000002bd420000002900535f31000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                        <31000000043200000004540000005900047800000000000001000000140008ffffffff0000243100000000000002000000170004ffffffff0000243200000000000003000000140008ffffffff0000243300000000000004000002bd0008ffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >420000002900535f31000003000000000000000300000001340000000331323300000004352e34330000450000000900000000005300000004
                        <3200000004440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                        >430000000953535f31005300000004
                        <33000000045a0000000549
                        >5800000004
                        """
        );
    }

    @Test
    public void testCloseMessageHex() throws Exception {
        skipOnWalRun(); // select only
        //hex for close message 43 00000009 53 535f31 00
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >430000000953535f31005300000004
                        <33000000045a0000000549
                        >5800000004
                        """
        );
    }

    @Test
    public void testContextClearsTransactionFlag() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(true);
            try (PreparedStatement pstmt = connection.prepareStatement("create table t as " +
                    "(select cast(x + 1 as long) a, cast(x as timestamp) b from long_sequence(0))")) {
                pstmt.execute();
            }
            connection.prepareStatement("BEGIN").execute();
            connection.close();

            for (int i = 0; i < 100; i++) {
                try (final Connection connection2 = getConnection(mode, port, false)) {
                    connection2.prepareStatement("insert into t values (1, 1)").execute();
                }
            }
            assertSql("""
                    count
                    100
                    """, "select count(*) from t");
        });
    }

    @Test
    public void testCreateDropCreateTable() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("create table x as (select x::timestamp as ts from long_sequence(100)) timestamp (ts)");
                try (ResultSet rs = stmt.executeQuery("tables();")) {
                    assertResultSet("""
                                    id[INTEGER],table_name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],o3MaxLag[BIGINT],walEnabled[BIT],directoryName[VARCHAR],dedup[BIT],ttlValue[INTEGER],ttlUnit[VARCHAR],matView[BIT]
                                    2,x,ts,NONE,1000,300000000,false,x~,false,0,HOUR,false
                                    """,
                            sink, rs
                    );
                }

                stmt.execute("drop table x");
                drainWalQueue();
                stmt.execute("create table x as (select x::timestamp as ts from long_sequence(100)) timestamp (ts)");

                try (ResultSet rs = stmt.executeQuery("tables();")) {
                    assertResultSet("""
                                    id[INTEGER],table_name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],o3MaxLag[BIGINT],walEnabled[BIT],directoryName[VARCHAR],dedup[BIT],ttlValue[INTEGER],ttlUnit[VARCHAR],matView[BIT]
                                    3,x,ts,NONE,1000,300000000,false,x~,false,0,HOUR,false
                                    """,
                            sink, rs
                    );
                }
            }
        });
    }

    @Test
    public void testCreateMatView() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            // Validate that a prepared create mat view statement validates base table name
            // at execution stage, not when parsing.
            final String createViewSql = "create materialized view price_1h as (" +
                    "  select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by week";
            final PreparedStatement createViewStmt = connection.prepareStatement(createViewSql);
            try {
                createViewStmt.execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "table does not exist");
            }

            connection.prepareStatement(
                    "create table base_price (" +
                            "  sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            ).execute();
            connection.prepareStatement(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            ).execute();

            createViewStmt.execute();
            mayDrainWalAndMatViewQueues();

            sink.clear();
            try (
                    PreparedStatement statement = connection.prepareStatement("price_1h");
                    ResultSet rs = statement.executeQuery()
            ) {
                assertResultSet(
                        """
                                sym[VARCHAR],price[DOUBLE],ts[TIMESTAMP]
                                gbpusd,1.323,2024-09-10 12:00:00.0
                                jpyusd,103.21,2024-09-10 12:00:00.0
                                gbpusd,1.321,2024-09-10 13:00:00.0
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testCreateTableAsSelectExtendedPrepared() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement pstmt = connection.prepareStatement("create table t as " +
                    "(select cast(x + 1 as long) a, cast(x as timestamp) b from long_sequence(10))")) {
                pstmt.execute();
            }

            assertSql(
                    """
                            a\tb
                            2\t1970-01-01T00:00:00.000001Z
                            3\t1970-01-01T00:00:00.000002Z
                            4\t1970-01-01T00:00:00.000003Z
                            5\t1970-01-01T00:00:00.000004Z
                            6\t1970-01-01T00:00:00.000005Z
                            7\t1970-01-01T00:00:00.000006Z
                            8\t1970-01-01T00:00:00.000007Z
                            9\t1970-01-01T00:00:00.000008Z
                            10\t1970-01-01T00:00:00.000009Z
                            11\t1970-01-01T00:00:00.000010Z
                            """, "t"
            );

            // Drop the table and create it once again with the same contents to verify
            // that the named statement gets executed.
            try (PreparedStatement pstmt = connection.prepareStatement("drop table t")) {
                pstmt.execute();
            }
            try (PreparedStatement pstmt = connection.prepareStatement("create table t as " +
                    "(select cast(x + 1 as long) a, cast(x as timestamp) b from long_sequence(10))")) {
                pstmt.execute();
            }
            assertSql(
                    """
                            a\tb
                            2\t1970-01-01T00:00:00.000001Z
                            3\t1970-01-01T00:00:00.000002Z
                            4\t1970-01-01T00:00:00.000003Z
                            5\t1970-01-01T00:00:00.000004Z
                            6\t1970-01-01T00:00:00.000005Z
                            7\t1970-01-01T00:00:00.000006Z
                            8\t1970-01-01T00:00:00.000007Z
                            9\t1970-01-01T00:00:00.000008Z
                            10\t1970-01-01T00:00:00.000009Z
                            11\t1970-01-01T00:00:00.000010Z
                            """, "t"
            );
        });
    }

    @Test
    public void testCreateTableDuplicateColumnName() throws Exception {
        skipOnWalRun(); // non-partitioned table

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try {
                connection.prepareStatement(
                        """
                                create table tab as (
                                            select
                                                rnd_byte() b,
                                                rnd_boolean() B
                                            from long_sequence(1)
                                        )""").execute();
                Assert.fail();
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "ERROR: Duplicate column [name=B]\n" +
                        "  Position: 102");
            }
        });
    }

    @Test
    public void testCreateTableDuplicateColumnNameNonAscii() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try {
                connection.prepareStatement("""
                        create table tab as (
                                    select
                                        rnd_byte() 侘寂,
                                        rnd_boolean() 侘寂
                                    from long_sequence(1)
                                )""").execute();
                Assert.fail();
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "Duplicate column [name=侘寂]");
            }
        });
    }

    @Test
    public void testCreateTableExtendedPrepared() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement pstmt = connection.prepareStatement("""
                    create table t (
                      a SYMBOL,
                      b TIMESTAMP)
                        timestamp(b)""")) {
                pstmt.execute();
            }
            assertSql(
                    "a\tb\n", "t"
            );
        });
    }

    @Test
    public void testCursorFetch() throws Exception {
        skipOnWalRun(); // the test doesn't use partitioned tables

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 10000;
            int fetchSize = 993;

            CallableStatement stmt = connection.prepareCall(
                    "create table x as (select" +
                            " cast(x as int) kk, " +
                            " rnd_int() a," +
                            " rnd_boolean() b," + // str
                            " rnd_str(1,1,2) c," + // str
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," + // str
                            " rnd_long() j," +
                            " timestamp_sequence(889001, 8890012) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_char() cc," + // str
                            " rnd_long256() l2" + // str
                            " from long_sequence(" + totalRows + ")) timestamp(k) partition by YEAR" // str
            );
            stmt.execute();
            mayDrainWalQueue();

            try (PreparedStatement statement = connection.prepareStatement("x")) {
                statement.setFetchSize(fetchSize);
                int count = 0;
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        count++;
                        assertEquals(count, rs.getInt(1));
                    }
                }
                Assert.assertEquals(totalRows, count);
            }
        });
    }

    @Test
    public void testDDL() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (a int)")) {
                statement.execute();
                try (
                        PreparedStatement select = connection.prepareStatement("x");
                        ResultSet rs = select.executeQuery()
                ) {
                    sink.clear();
                    assertResultSet("a[INTEGER]\n", sink, rs);
                }
            }
        });
    }

    @Test
    public void testDecimalType_insertIntoDecimalColumns() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            if (!binary) {
                return;
            }
            try (final PreparedStatement statement = connection.prepareStatement("create table x (d8 decimal(2, 0), d16 decimal(4, 1), d32 decimal(9, 0), d64 decimal(18, 6), d128 decimal(38, 6), d256 decimal(76, 0))")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?, ?, ?, ?, ?)")) {
                    insert.setString(1, "12");
                    insert.setString(2, "123.4");
                    insert.setString(3, "12345678");
                    insert.setString(4, "1234567890.123456");
                    insert.setString(5, "1234567890123455678901234.56789");
                    insert.setString(6, "123456789012345567890123456789012345678901234567890");
                    insert.executeUpdate();
                }
                try (ResultSet resultSet = connection.prepareStatement("select * from x").executeQuery()) {
                    sink.clear();
                    String expected = "d8[NUMERIC],d16[NUMERIC],d32[NUMERIC],d64[NUMERIC],d128[NUMERIC],d256[NUMERIC]\n" +
                            "12,123.4,12345678,1234567890.123456,1234567890123455678901234.567890,123456789012345567890123456789012345678901234567890\n";
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testDecimalType_update_nonPartitionedTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (d1 decimal(16, 5))")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?)")) {
                    insert.setString(1, "123456.78901");
                    insert.executeUpdate();
                }
                try (PreparedStatement update = connection.prepareStatement("update x set d1 = ?")) {
                    update.setString(1, "9876543.21098");
                    update.executeUpdate();
                }
                try (ResultSet resultSet = connection.prepareStatement("select *  from x").executeQuery()) {
                    sink.clear();
                    String expected = "d1[NUMERIC]\n" +
                            "9876543.21098\n";
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testDecimalType_update_partitionedTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (ts timestamp, d1 decimal(16, 5)) timestamp(ts) partition by DAY")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?)")) {
                    insert.setTimestamp(1, new Timestamp(0));
                    insert.setString(2, "123456.78901");
                    insert.executeUpdate();
                }
                try (PreparedStatement update = connection.prepareStatement("update x set d1 = ?")) {
                    update.setString(1, "9876543.21098");
                    update.executeUpdate();
                }
                if (walEnabled) {
                    drainWalQueue();
                }
                try (ResultSet resultSet = connection.prepareStatement("select d1 from x").executeQuery()) {
                    sink.clear();
                    String expected = "d1[NUMERIC]\n" +
                            "9876543.21098\n";
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testDiscardClearsTransactionFlag() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement pstmt = connection.prepareStatement("create table t as " +
                    "(select cast(x + 1 as long) a, cast(x as timestamp) b from long_sequence(0))")) {
                pstmt.execute();
            }
            connection.prepareStatement("insert into t values (1, 1)").execute();
            connection.prepareStatement("COMMIT").execute();
            connection.prepareStatement("DISCARD ALL").execute();

            try (final Connection conn2 = getConnection(Mode.SIMPLE, port, binary)) {
                for (int i = 0; i < 100; i++) {
                    conn2.prepareStatement("insert into t values (1, 1)").execute();
                }
            }
            assertSql("""
                    count
                    101
                    """, "select count(*) from t");
        });
    }

    @Test
    public void testDisconnectDuringAuth() throws Exception {
        skipOnWalRun(); // we are not touching tables at all, no reason to run the same test twice.
        for (int i = 0; i < 3; i++) {
            testDisconnectDuringAuth0(i);
        }
    }

    @Test
    public void testDotNetHex() throws Exception {
        // DotNet code sends the following:
        //   SELECT version()
        // The issue that was here is STRING is required to be sent as "binary" type
        // it is the same as non-binary, but DotNet puts strict criteria on field format. It has to be 1.
        // Other drivers are less sensitive, perhaps they just do non-zero check
        // Here we assert that 1 is correctly derived from column type

        skipOnWalRun(); // select only
        String script = """
                >0000003b00030000757365720061646d696e00636c69656e745f656e636f64696e67005554463800646174616261736500706f7374677265730000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000180053454c4543542076657273696f6e2829000000420000000e0000000000000001000144000000065000450000000900000000005300000004
                <310000000432000000045400000020000176657273696f6e0000000000000100000413ffffffffffff0001440000004d000100000043506f737467726553514c2031322e332c20636f6d70696c65642062792056697375616c20432b2b206275696c6420313931342c2036342d6269742c2051756573744442430000000d53454c4543542031005a0000000549
                >51000000104449534341524420414c4c005800000004
                <4300000008534554005a0000000549""";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testDropTable() throws Exception {
        String[][] sqlExpectedErrMsg = {
                {"drop table doesnt", "ERROR: table does not exist [table=doesnt]"},
                {"drop table", "ERROR: expected IF EXISTS table-name"},
                {"drop doesnt", "ERROR: 'table' or 'materialized view' or 'all' expected"},
                {"drop", "ERROR: 'table' or 'materialized view' or 'all' expected"},
                {"drop table if doesnt", "ERROR: expected EXISTS"},
                {"drop table exists doesnt", "ERROR: table and column names that are SQL keywords have to be enclosed in double quotes, such as \"exists\""},
                {"drop table if exists", "ERROR: table name expected"},
                {"drop table if exists;", "ERROR: table name expected"},
                {"drop all table if exists;", "ERROR: ';' or 'tables' expected"},
                {"drop all tables if exists;", "ERROR: ';' or 'tables' expected"},
                {"drop database ;", "ERROR: 'table' or 'materialized view' or 'all' expected"}
        };
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            for (int i = 0, n = sqlExpectedErrMsg.length; i < n; i++) {
                String[] testData = sqlExpectedErrMsg[i];
                try (PreparedStatement statement = connection.prepareStatement(testData[0])) {
                    statement.execute();
                    Assert.fail(testData[0]);
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), testData[1]);
                }
            }
        });
    }

    @Test
    public void testDropTableIfExistsDoesNotFailWhenTableDoesNotExist() throws Exception {
        skipOnWalRun(); // table not created
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement("drop table if exists doesnt")) {
                statement.execute();
            }
        });
    }

    @Test
    public void testEmptySql() throws Exception {
        skipOnWalRun(); // table not created
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement("")) {
                statement.execute();
            }
        });
    }

    @Test
    public void testErrnoInErrorMessage() throws Exception {
        skipOnWalRun(); // non-partitioned table
        ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return 4; // Too many open files
            }

            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertWithPgServerExtendedBinaryOnly((connection, binary, mode, port) -> {
            try (
                    PreparedStatement stmt = connection.prepareStatement(
                            "create table x as (" +
                                    " select x, timestamp_sequence(0, 1000) ts" +
                                    " from long_sequence(1)" +
                                    ") timestamp (ts)"
                    )
            ) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("x")) {
                stmt.executeQuery();
                Assert.fail();
            } catch (PSQLException ex) {
                assertContains(ex.getMessage(), "[4]");
            }
        });
    }

    @Test
    public void testExecuteAndCancelSqlCommands() throws Exception {
        Assume.assumeTrue(walEnabled);

        final long TIMEOUT = 240_000;

        String baseTable = "create table tab (b boolean, ts timestamp, sym symbol)";
        String walTable = baseTable + " timestamp(ts) partition by DAY WAL";
        ObjList<String> ddls = new ObjList<>(
                baseTable,
                baseTable + " timestamp(ts)",
                baseTable + " timestamp(ts) partition by DAY BYPASS WAL"
                // walTable //TODO: ban WAL update cancellation
        );

        String createAsSelect = "create table new_tab as (select * from tab where sleep(120000))";
        String select1 = "select 1 from long_sequence(1) where sleep(120000)";
        String select2 = "select sleep(120000) from long_sequence(1)";
        String selectWithJoin = "select 1 from long_sequence(1) ls1 join long_sequence(1) on sleep(120000)";
        String insertAsSelect1 = "insert into tab select true, 100000000000000L::timestamp, 'B' from long_sequence(1) where sleep(120000)";
        String insertAsSelect2 = "insert into tab select sleep(120000), 100000000000000L::timestamp, 'B' from long_sequence(1)";
        String insertAsSelectBatched = "insert batch 100 into tab select true, 100000000000000L::timestamp, 'B' from long_sequence(1) where sleep(120000)";
        String insertAsSelectWithJoin1 = "insert into tab select ls1.x = ls2.x, 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x where sleep(120000)";
        String insertAsSelectWithJoin2 = "insert into tab select sleep(120000), 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x";
        String insertAsSelectWithJoin3 = "insert into tab select ls1.x = ls2.x, 100000000000000L::timestamp, 'B' from long_sequence(1) ls1 left join (select * from long_sequence(1)) ls2 on ls1.x = ls2.x and sleep(120000)";
        String update1 = "update tab set b=true where sleep(120000)";
        String update2 = "update tab set b=sleep(120000)";
        String updateWithJoin1 = "update tab t1 set b=true from tab t2 where sleep(120000) and t1.b = t2.b";
        String updateWithJoin2 = "update tab t1 set b=sleep(120000) from tab t2 where t1.b = t2.b";
        String addColumns = "alter table tab add column s1 symbol index";

        ObjList<String> commands = new ObjList<>(
                createAsSelect,
                select1,
                select2,
                selectWithJoin,
                insertAsSelect1,
                insertAsSelect2,
                insertAsSelectBatched,
                insertAsSelectWithJoin1,
                insertAsSelectWithJoin2,
                insertAsSelectWithJoin3,
                update1,
                update2,
                updateWithJoin1,
                updateWithJoin2,
                addColumns
        );

        try {
            DelayedListener registryListener = new DelayedListener();
            engine.getQueryRegistry().setListener(registryListener);

            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                SOCountDownLatch started = new SOCountDownLatch(1);
                SOCountDownLatch stopped = new SOCountDownLatch(1);
                AtomicReference<Exception> queryError = new AtomicReference<>();

                for (int i = 0, n = ddls.size(); i < n; i++) {
                    final String ddl = ddls.getQuick(i);
                    boolean isWal = ddl.equals(walTable);

                    execute("drop table if exists tab");
                    execute(ddl);
                    execute("insert into tab select true, (86400000000*x)::timestamp, null from long_sequence(1000)");
                    execute("drop table if exists new_tab");
                    if (isWal) {
                        drainWalQueue();
                    }

                    for (int j = 0, k = commands.size(); j < k; j++) {
                        final String command = commands.getQuick(j);

                        // skip pending wal changes in case wal table has been suspended by earlier command
                        if (isWal) {
                            try (RecordCursorFactory factory = select("select suspended, writerTxn, sequencerTxn from wal_tables() where name = 'tab'")) {
                                boolean suspended;
                                long sequencerTxn;

                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    cursor.hasNext();
                                    Record record = cursor.getRecord();
                                    suspended = record.getBool(0);
                                    sequencerTxn = record.getLong(2) + 1;
                                }

                                if (suspended) {
                                    execute("alter table tab resume wal from txn " + sequencerTxn);

                                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                        cursor.hasNext();
                                        Record record = cursor.getRecord();
                                        suspended = record.getBool(1);
                                        Assert.assertFalse(suspended);
                                    }
                                }
                            }
                            drainWalQueue();
                        }

                        // statements containing multiple transactions, such as 'alter table add column col1, col2' are currently not supported for WAL tables
                        // UPDATE statements with join are not supported yet for WAL tables
                        if ((isWal && (command.equals(updateWithJoin1) || command.equals(updateWithJoin2) || command.equals(addColumns)))) {
                            continue;
                        }

                        try {
                            if (isWal) {
                                drainWalQueue();
                            }

                            started.setCount(isWal ? 2 : 1);
                            stopped.setCount(isWal ? 2 : 1);
                            queryError.set(null);
                            registryListener.queryFound.setCount(1);
                            registryListener.queryText = command;

                            new Thread(() -> {
                                try {
                                    try (Connection conn = getConnection(mode, port, binary, 5)) {
                                        started.countDown();

                                        try (PreparedStatement stmt = conn.prepareStatement(command)) {
                                            if (command.startsWith("select")) {
                                                try (ResultSet result = stmt.executeQuery()) {
                                                    while (result.next()) {
                                                        // ignore
                                                    }
                                                }
                                            } else {
                                                stmt.executeUpdate();
                                            }
                                        }
                                    }
                                } catch (SQLException e) {
                                    // ignore errors showing that statement has been cancelled
                                    if (!Chars.contains(e.getMessage(), "Could not create table")
                                            && !Chars.contains(e.getMessage(), "cancelled by user")) {
                                        queryError.set(e);
                                    }
                                } catch (Exception e) {
                                    queryError.set(e);
                                } finally {
                                    stopped.countDown();
                                }
                            }, "command_thread").start();

                            if (isWal) {
                                Thread walJob = new Thread(() -> {
                                    started.countDown();

                                    try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                        while (queryError.get() == null) {
                                            drainWalQueue(walApplyJob);
                                        }
                                    } finally {
                                        // release native path memory used by the job
                                        Path.clearThreadLocals();
                                        stopped.countDown();
                                    }
                                }, "wal_job");
                                walJob.start();
                            }

                            started.await();

                            long queryId;
                            long start = System.currentTimeMillis();

                            try (PreparedStatement stmt = connection.prepareStatement("select query_id from query_activity() where query = ?")) {
                                stmt.setString(1, command);
                                while (true) {
                                    Os.sleep(1);
                                    try (ResultSet result = stmt.executeQuery()) {
                                        if (result.next()) {
                                            queryId = result.getLong(1);
                                            break;
                                        }
                                    }

                                    if (System.currentTimeMillis() - start > TIMEOUT) {
                                        throw new RuntimeException("Timed out waiting for command to appear in registry: " + command);
                                    }
                                    if (queryError.get() != null) {
                                        throw new RuntimeException("Query to cancel failed!", queryError.get());
                                    }
                                }
                            }

                            try (PreparedStatement stmt = connection.prepareStatement("cancel query " + queryId)) {
                                stmt.executeUpdate();
                            } finally {
                                registryListener.queryFound.countDown();
                            }
                            start = System.currentTimeMillis();

                            try (PreparedStatement stmt = connection.prepareStatement("select * from query_activity() where query_id = ?")) {
                                stmt.setLong(1, queryId);
                                while (true) {
                                    Os.sleep(1);
                                    try (ResultSet result = stmt.executeQuery()) {
                                        if (!result.next()) {
                                            break;
                                        }
                                    }
                                    if (System.currentTimeMillis() - start > TIMEOUT) {
                                        throw new RuntimeException("Timed out waiting for command to stop: " + command);
                                    }
                                }
                            }

                            // run simple query to test that previous query cancellation doesn't 'spill into' other queries
                            try (PreparedStatement stmt = connection.prepareStatement("select sleep(1)")) {
                                try (ResultSet result = stmt.executeQuery()) {
                                    result.next();
                                    Assert.assertTrue(result.getBoolean(1));
                                }
                            }
                        } catch (Throwable t) {
                            throw new RuntimeException("Failed on\n ddl: " + ddl +
                                    "\n query: " + command +
                                    "\n exception: ", t);
                        } finally {
                            queryError.set(new Exception()); // stop wal thread
                            stopped.await();
                        }
                    }
                }
            });
        } finally {
            engine.getQueryRegistry().setListener(null);
        }
    }

    @Test
    public void testExecuteAndFailedQueryDoesntLeaveItInRegistry() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table trades as (select 'a'::symbol symbol, -1 price from long_sequence(10))");

            try (PreparedStatement pstmt = connection.prepareStatement("SELECT symbol,approx_percentile(price, 50, 2) from trades")) {
                pstmt.executeQuery();
                Assert.fail();
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "ERROR: percentile must be between 0.0 and 1.0");
            }

            try (PreparedStatement pstmt = connection.prepareStatement("select * from query_activity() where query = ?")) {
                pstmt.setString(1, "SELECT symbol,approx_percentile(price, 50, 2) from trades");
                ResultSet rs = pstmt.executeQuery();
                sink.clear();
                assertResultSet("query_id[BIGINT],worker_id[BIGINT],worker_pool[VARCHAR],username[VARCHAR],query_start[TIMESTAMP],state_change[TIMESTAMP],state[VARCHAR],is_wal[BIT],query[VARCHAR]\n",
                        sink, rs
                );
            }
        });
    }

    @Test
    public void testExecuteSameQueryManyTimesWithMaxRowsReturnsCorrectResult() throws Exception {
        skipOnWalRun();
        // this test exercises maxRows feature of the protocol, which is not supported (not sent to the server)
        // in "simple" mode.
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("create table if not exists tab ( a int, b long, ts timestamp)");
            }

            //max rows bigger than result set sie (empty result set)
            assertResultTenTimes(
                    connection,
                    "select * from tab",
                    "a[INTEGER],b[BIGINT],ts[TIMESTAMP]\n",
                    5
            );

            //max rows bigger than result set sie (non-empty result set)
            assertResultTenTimes(
                    connection,
                    "select 1 as x",
                    """
                            x[INTEGER]
                            1
                            """,
                    5
            );

            //max rows smaller than result set size
            assertResultTenTimes(
                    connection,
                    "select x from long_sequence(5)",
                    """
                            x[BIGINT]
                            1
                            2
                            3
                            """,
                    3
            );

            // max rows smaller than cursor size, cursor does not return size
            assertResultTenTimes(
                    connection,
                    "show columns from tab",
                    """
                            column[VARCHAR],type[VARCHAR],indexed[BIT],indexBlockCapacity[INTEGER],symbolCached[BIT],symbolCapacity[INTEGER],symbolTableSize[INTEGER],designated[BIT],upsertKey[BIT]
                            a,INT,false,0,false,0,0,false,false
                            b,LONG,false,0,false,0,0,false,false
                            """,
                    2
            );

            // max rows bigger than cursor size, cursor does not return size
            assertResultTenTimes(
                    connection,
                    "show columns from tab",
                    """
                            column[VARCHAR],type[VARCHAR],indexed[BIT],indexBlockCapacity[INTEGER],symbolCached[BIT],symbolCapacity[INTEGER],symbolTableSize[INTEGER],designated[BIT],upsertKey[BIT]
                            a,INT,false,0,false,0,0,false,false
                            b,LONG,false,0,false,0,0,false,false
                            ts,TIMESTAMP,false,0,false,0,0,false,false
                            """,
                    6
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement pstmt = connection.prepareStatement("create table xx as (" +
                    "select x," +
                    " timestamp_sequence(0, 1000) ts" +
                    " from long_sequence(100000)) timestamp (ts)")) {
                pstmt.execute();
            }

            try (PreparedStatement statement = connection.prepareStatement("explain select * from xx limit 10")) {
                statement.execute();
                try (ResultSet rs = statement.getResultSet()) {
                    assertResultSet(
                            """
                                    QUERY PLAN[VARCHAR]
                                    Limit lo: 10 skip-over-rows: 0 limit: 10
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: xx
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testExplainPlanWithBindVariables() throws Exception {
        sharedQueryWorkerCount = 2; // Set to 1 to enable parallel query plans
        try {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                try (PreparedStatement pstmt = connection.prepareStatement("create table xx as (" +
                        "select x," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(1000)) timestamp (ts)")) {
                    pstmt.execute();
                }

                String query;
                if (mode == Mode.SIMPLE && !binary) {
                    // In the simple text mode we have to explicitly cast the first variable to long
                    // otherwise JDBC driver sends just text 'explain select * from xx where x > ('0') and x < ('10.0')::double limit 10'
                    // QuestDB complains with 'there is no matching operator `>` with the argument types: LONG > CHAR'
                    query = "explain select * from xx where x > ?::long and x < ?::double limit 10";
                } else {
                    // in other modes we can keep things simple
                    // we still cast the 2nd variable, to test it actually works
                    query = "explain select * from xx where x > ? and x < ?::double limit 10";
                }
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    final StringSink expectedResult = new StringSink();
                    for (int i = 0; i < 3; i++) {
                        System.out.println(i);
                        statement.setLong(1, i);
                        statement.setDouble(2, (i + 1) * 10);
                        statement.execute();
                        sink.clear();
                        expectedResult.clear();
                        try (ResultSet rs = statement.getResultSet()) {
                            if (mode == Mode.SIMPLE) {
                                // simple mode inlines variables in the sql text
                                expectedResult.put("QUERY PLAN[VARCHAR]\n" +
                                        "Async Filter workers: 2\n" +
                                        "  limit: 10\n" +
                                        "  filter: ('" + i + "'::long<x and x<'" + (i + 1) * 10 + ".0'::double)\n" +
                                        "    PageFrame\n" +
                                        "        Row forward scan\n" +
                                        "        Frame forward scan on: xx\n");
                            } else {
                                // extended mode actually uses binding vars
                                expectedResult.put("""
                                        QUERY PLAN[VARCHAR]
                                        Async Filter workers: 2
                                          limit: 10
                                          filter: ($0::long<x and x<$1::double)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: xx
                                        """);
                            }

                            assertResultSet(
                                    expectedResult,
                                    sink,
                                    rs
                            );
                        }
                    }
                }
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }

    @Test
    public void testExplainPlanWithBindVariablesFailsIfAllValuesArentSet() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement("explain select * from long_sequence(1) where x > ? and x < ? limit 10")) {
                statement.setLong(1, 0);
                try {
                    statement.execute();
                } catch (PSQLException e) {
                    Assert.assertEquals("No value specified for parameter 2.", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testExplainPlanWithWhitespaces() throws Exception {
        sharedQueryWorkerCount = 2; // Set to 1 to enable parallel query plans
        try {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                try (PreparedStatement pstmt = connection.prepareStatement("create table xx as (" +
                        "select x as x," +
                        " 's' || x as str" +
                        " from long_sequence(100000))")) {
                    pstmt.execute();
                }

                try (PreparedStatement statement = connection.prepareStatement("explain select * from xx where str = '\b\f\n\r\t\u0005' order by str,x limit 10")) {
                    statement.execute();
                    try (ResultSet rs = statement.getResultSet()) {
                        assertResultSet(
                                """
                                        QUERY PLAN[VARCHAR]
                                        Async Top K lo: 10 workers: 2
                                          filter: str='\\b\\f\\n\\r\\t\\u0005'
                                          keys: [str, x]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: xx
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            });
        } finally {
            sharedQueryWorkerCount = 0; // reset to default
        }
    }

    @Test
    public void testExtendedQueryTimeout() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final PreparedStatement statement = conn.prepareStatement("select s, count(*) from t1 group by s ")) {
                statement.execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
            }
        });
    }

    @Test
    public void testFetch10RowsAtaTime() throws Exception {
        // fetch works only in extended query mode
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "create table xx as (" +
                            "select x " +
                            " from long_sequence(100000) )")) {
                pstmt.execute();
            }
            int count = 0;
            try (PreparedStatement statement = connection.prepareStatement("select * from xx limit 100")) {
                statement.setFetchSize(10);
                try (ResultSet rs = statement.executeQuery()) {
                    List<Tuple> rows = getRows(rs);
                    Assert.assertEquals(10, rows.size());
                    while (rs.next()) {
                        Assert.assertEquals(++count, rs.getRow());
                    }
                }
            }

            Assert.assertEquals(100, count);
        });
    }

    /*
    Tests simple query fetched 1 row at a time, with flush commands in between as done by following node.js code:
    "use strict"

    const { Client, types } = require("pg")
    const QueryStream = require('pg-query-stream')
    const JSONStream = require('JSONStream')

    const start = async () => {
        const client = new Client({
            database: "qdb",
            host: "127.0.0.1",
            password: "quest",
            port: 8812,
            user: "admin",
        })
        await client.connect()

        const res = await client.query('SELECT * FROM long_sequence(5)')
        console.log(res.rows)

        const query = new QueryStream('SELECT * FROM long_sequence(5)',[],
            {   batchSize: 1,
                types: {
                    getTypeParser: (dataTypeID, format) => {
                        return types.getTypeParser(dataTypeID, format)
                    },
                }
            }
        )
        const stream = client.query(query)
        stream.pipe(JSONStream.stringify()).pipe(process.stdout)

        // release the client when the stream is finished
        const streamEndPromise = deferredPromise()
        stream.on('end', streamEndPromise.resolve)
        stream.on('error', streamEndPromise.reject)

        await streamEndPromise
        client.end()
    }

    function deferredPromise() {
        let resolve, reject
        const p = new Promise((_resolve, _reject) => {
            resolve = _resolve
            reject = _reject
        })
        p.resolve = resolve
        p.reject = reject
        return p
    }

    start()
        .then(() => console.log('Done'))
        .catch(console.error)
    */
    @Test
    public void testFetch1RowAtaTimeWithFlushInBetween() throws Exception {
        assertHexScript("""
                >0000003600030000757365720061646d696e0064617461626173650071646200636c69656e745f656e636f64696e6700555446380000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >510000002353454c454354202a2046524f4d206c6f6e675f73657175656e636528352900
                <540000001a00017800000000000001000000140008ffffffff0000440000000b00010000000131440000000b00010000000132440000000b00010000000133440000000b00010000000134440000000b00010000000135430000000d53454c4543542035005a0000000549
                >50000000260053454c454354202a2046524f4d206c6f6e675f73657175656e6365283529000000
                >420000000f435f310000000000000000
                >440000000950435f3100
                >4800000004
                <31000000043200000004540000001a00017800000000000001000000140008ffffffff0000
                >450000000c435f310000000001
                >4800000004
                <440000000b000100000001317300000004
                >450000000c435f310000000001
                >4800000004
                <440000000b000100000001327300000004
                >450000000c435f3100000000014800000004
                <440000000b000100000001337300000004
                >450000000c435f310000000001
                >4800000004
                <440000000b000100000001347300000004
                >450000000c435f310000000001
                >4800000004
                <440000000b000100000001357300000004
                >450000000c435f310000000001
                >4800000004
                <430000000d53454c454354203000
                >430000000950435f3100
                >5300000004
                <33000000045a0000000549
                >5800000004
                """);
    }

    @Test
    public void testFetchDisconnectReleasesReaderCrossJoin() throws Exception {
        final String query = "with crj as (select first(x) as p0 from xx) select x / p0 from xx cross join crj";

        testFetchDisconnectReleasesReader(query);
    }

    @Test
    public void testFetchDisconnectReleasesReaderHashJoin() throws Exception {
        final String query = "with crj as (select first(x) as p0 from xx) select x / p0 from crj join xx on x = p0 ";

        testFetchDisconnectReleasesReader(query);
    }

    @Test
    public void testFetchDisconnectReleasesReaderLeftHashJoin() throws Exception {//slave - cross join
        final String query = "with crj as (select first(x) as p0 from xx)  select x / p0 from crj left join (select * from xx x1 cross join xx x2) on x = p0 and x <= 1";

        testFetchDisconnectReleasesReader(query);
    }

    @Test
    public void testFetchDisconnectReleasesReaderLeftHashJoinLight() throws Exception {
        final String query = "with crj as (select first(x) as p0 from xx)  select x / p0 from crj left join xx on x = p0 and x <= 1";

        testFetchDisconnectReleasesReader(query);
    }

    @Test
    public void testFetchDisconnectReleasesReaderLeftNLJoin() throws Exception {
        final String query = "with crj as (select first(x) as p0 from xx) select x / p0 from xx left join crj on x <= p0";

        testFetchDisconnectReleasesReader(query);
    }

    @Test
    public void testFetchTablePartitions() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table if not exists t1 as " +
                    "(" +
                    "select dateadd('h', x::int, '2023-03-23T00:00:00.000000Z') as ts  " +
                    "from long_sequence(30)" +
                    ") " +
                    "timestamp(ts) partition by day")) {
                stmt.execute();
                mayDrainWalQueue();
            }

            try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM table_partitions('t1')")) {
                ResultSet resultSet = stmt.executeQuery();

                resultSet.next();
                assertEquals(0, resultSet.getLong(1));
                assertEquals("DAY", resultSet.getString(2));
                assertEquals("2023-03-23", resultSet.getString(3));
                assertTrue(resultSet.getString(4).startsWith("2023-03-23 01:00:00"));
                assertTrue(resultSet.getString(5).startsWith("2023-03-23 23:00:00"));
                assertEquals(23L, resultSet.getLong(6));
                // skip disk sizes as there's a race
                assertFalse(resultSet.getBoolean(9));
                assertFalse(resultSet.getBoolean(10));
                assertTrue(resultSet.getBoolean(11));
                assertFalse(resultSet.getBoolean(12));
                assertFalse(resultSet.getBoolean(13));

                resultSet.next();
                assertEquals(1, resultSet.getLong(1));
                assertEquals("DAY", resultSet.getString(2));
                assertEquals("2023-03-24", resultSet.getString(3));
                assertTrue(resultSet.getString(4).startsWith("2023-03-24 00:00:00"));
                assertTrue(resultSet.getString(5).startsWith("2023-03-24 06:00:00"));
                assertEquals(7L, resultSet.getLong(6));
                // skip disk sizes as there's a race
                assertFalse(resultSet.getBoolean(9));
                assertTrue(resultSet.getBoolean(10));
                assertTrue(resultSet.getBoolean(11));
                assertFalse(resultSet.getBoolean(12));
                assertFalse(resultSet.getBoolean(13));
            }
        });
    }

    @Test
    public void testFetchWithBindVariables() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 10;

            PreparedStatement tbl = connection.prepareStatement("create table x (a int)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a) values(?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setInt(1, i);
                insert.execute();
            }
            connection.commit();

            // first execute a query with the same text, but different bind var types
            try (PreparedStatement stmt = connection.prepareStatement("x where a != ?")) {
                stmt.setString(1, "-1");
                ResultSet rs = stmt.executeQuery();
                assertResultSet(
                        """
                                a[INTEGER]
                                0
                                1
                                2
                                3
                                4
                                5
                                6
                                7
                                8
                                9
                                """,
                        sink,
                        rs
                );
            }

            PreparedStatement stmt = connection.prepareStatement("x where a != ?");
            stmt.setInt(1, -1);
            int[] testSizes = {0, 1, 3, 10, 12};
            for (int testSize : testSizes) {
                stmt.setFetchSize(testSize);
                assertEquals(testSize, stmt.getFetchSize());

                ResultSet rs = stmt.executeQuery();
                assertEquals(testSize, rs.getFetchSize());

                int count = 0;
                while (rs.next()) {
                    assertEquals(count, rs.getInt(1));
                    ++count;
                }

                assertEquals(totalRows, count);
            }
        });
    }

    @Test
    public void testGORMConnect() throws Exception {
        skipOnWalRun(); // table not created
        // GORM is a Golang ORM tool
        assertHexScript(
                """
                        >0000005e0003000064617461626173650071646200646174657374796c650049534f2c204d44590065787472615f666c6f61745f646967697473003200757365720061646d696e00636c69656e745f656e636f64696e6700555446380000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >51000000063b00
                        """
        );
    }

    @Test
    public void testGeoHashInsert() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table xyz (" +
                    "a geohash(1b)," +
                    "b geohash(2b)," +
                    "c geohash(3b)," +
                    "d geohash(1c)," +
                    "e geohash(2c)," +
                    "f geohash(4c)," +
                    "g geohash(8c)" +
                    ")"
            );
            connection.setAutoCommit(false);
            try (
                    final PreparedStatement insert = connection.prepareStatement(
                            "insert into xyz values (" +
                                    "cast(? as geohash(1b))," +
                                    "cast(? as geohash(2b))," +
                                    "cast(? as geohash(3b))," +
                                    "cast(? as geohash(1c))," +
                                    "cast(? as geohash(2c))," +
                                    "cast(? as geohash(4c))," +
                                    "cast(? as geohash(8c)))"
                    )) {
                for (int i = 0; i < 100; i++) {
                    insert.setString(1, "0b");
                    insert.setString(2, "10b");
                    insert.setString(3, "010b");
                    insert.setString(4, "x");
                    insert.setString(5, "xy");
                    insert.setString(6, "xyzw");
                    insert.setString(7, "xyzwzvxq");
                    insert.execute();
                    Assert.assertEquals(1, insert.getUpdateCount());
                }
                connection.commit();

                try (
                        RecordCursorFactory factory = select("xyz");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    int count1 = 0;
                    while (cursor.hasNext()) {
                        //TODO: bits GeoHash literal
//                            Assert.assertEquals((byte)GeoHashes.fromBitString("0", 0), record.getGeoByte(0));
//                            Assert.assertEquals((byte)GeoHashes.fromBitString("01", 0), record.getGeoByte(1));
//                            Assert.assertEquals((byte)GeoHashes.fromBitString("010", 0), record.getGeoByte(2));
                        Assert.assertEquals(GeoHashes.fromString("x", 0, 1), record.getGeoByte(3));
                        Assert.assertEquals(GeoHashes.fromString("xy", 0, 2), record.getGeoShort(4));
                        Assert.assertEquals(GeoHashes.fromString("xyzw", 0, 4), record.getGeoInt(5));
                        Assert.assertEquals(GeoHashes.fromString("xyzwzvxq", 0, 8), record.getGeoLong(6));
                        count1++;
                    }

                    Assert.assertEquals(100, count1);
                }
            }
        });
    }

    @Test
    public void testGeoHashSelect() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Statement statement = connection.createStatement();

            // Create table with random values. Selecting it without materializing
            // will result in unstable select due to network fragmentation and line re-sending
            statement.execute(
                    "create table x as (select " +
                            "rnd_geohash(1) hash1b, " +
                            "rnd_geohash(2) hash2b, " +
                            "rnd_geohash(3) hash3b, " +
                            "rnd_geohash(5) hash1c, " +
                            "rnd_geohash(10) hash2c, " +
                            "rnd_geohash(20) hash4c, " +
                            "rnd_geohash(40) hash8c " +
                            "from long_sequence(10))");

            ResultSet rs = statement.executeQuery("select * from x");

            final String expected = """
                    hash1b[VARCHAR],hash2b[VARCHAR],hash3b[VARCHAR],hash1c[VARCHAR],hash2c[VARCHAR],hash4c[VARCHAR],hash8c[VARCHAR]
                    0,00,100,z,hp,wh4b,s2z2fyds
                    0,10,001,f,q4,uzr0,jj53eufn
                    1,01,111,7,q0,s2vq,y5nbb1qj
                    1,10,111,r,5t,g5xx,kt2bujns
                    1,11,010,w,u7,qjuz,gyye1jqc
                    1,01,101,2,cs,vqnq,9yvqyf2r
                    1,10,001,0,be,4bw1,v676yupj
                    0,11,010,q,vg,g6mm,4tyruscu
                    1,01,011,u,wt,jgke,pw94gc64
                    0,01,101,8,y0,b2vj,b8182chp
                    """;
            StringSink sink1 = new StringSink();
            // dump metadata
            assertResultSet(expected, sink1, rs);
        });
    }

    @Test
    public void testGetRow() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement();
            stmt.setFetchSize(1);
            int totalRows = 10;
            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();
            ResultSet rs = stmt.executeQuery("x");
            int count = 0;
            while (rs.next()) {
                count++;
                assertEquals(count, rs.getInt(1));
                assertEquals(count, rs.getRow());
            }
            assertEquals(totalRows, count);
        });
    }

    @Test
    /*
     * Tests the following golang code (PGX):
     * <pre>
     * package main
     *
     * import (
     * 	"context"
     * 	"fmt"
     * 	"github.com/jackc/pgx/v4/pgxpool"
     * 	"log"
     *
     * 	_ "github.com/jackc/pgx/v4"
     * )
     *
     * const (
     * 	host     = "localhost"
     * 	port     = 5432
     * 	user     = "admin"
     * 	password = "quest"
     * 	dbname   = "qdb"
     * )
     *
     * func main() {
     *
     * 	//urlExample := "postgres://postgres:pwd@localhost:5432/postgres"
     * 	urlExample := "postgres://admin:quest@localhost:5432/postgres"
     *
     * 	ctx0 := context.Background()
     * 	dbpool, err := pgxpool.Connect(ctx0, urlExample)
     *
     * 	if err != nil {
     * 		log.Fatalln("Unable to connect: %v\n", err)
     *        }
     * 	defer dbpool.Close()
     *
     * 	query := fmt.Sprintf("SELECT true, false")
     * 	rows, err := dbpool.Query(ctx0, query)
     * 	if (err != nil) {
     * 		log.Fatalln("Query failed")
     *    }
     *
     * 	for rows.Next() {
     * 		var side bool
     * 		var side2 bool
     * 		err = rows.Scan(&side, &side2)
     * 		if (err != nil) {
     * 			log.Fatalln("err scan row")
     *        } else {
     * 			log.Println(side, side2)
     *        }
     *    }
     *
     * }
     * </pre>
     */
    public void testGolangBoolean() throws Exception {
        skipOnWalRun(); // table not created
        assertHexScriptAltCreds(
                """
                        >0000000804d2162f
                        <4e
                        >0000002400030000757365720078797a00646174616261736500706f7374677265730000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >50000000246c72757073635f315f300053454c45435420747275652c2066616c73650000004400000010536c72757073635f315f30005300000004
                        <310000000474000000060000540000003500027472756500000000000001000000100001ffffffff000066616c736500000000000002000000100001ffffffff00005a0000000549
                        >420000001a006c72757073635f315f30000000000000020001000144000000065000450000000900000000005300000004
                        <3200000004540000003500027472756500000000000001000000100001ffffffff000166616c736500000000000002000000100001ffffffff00014400000010000200000001010000000100430000000d53454c4543542031005a0000000549
                        >5800000004
                        """
        );
    }

    @Test
    public void testGroupByExpressionNotAppearingInSelectClause() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final PreparedStatement statement = conn.prepareStatement("select count(*) from t1 group by 1+2")) {
                try (ResultSet rs = statement.executeQuery()) {
                    sink.clear();
                    assertResultSet("count[BIGINT]\n1000\n", sink, rs);
                }
            }
        });
    }

    @Test
    public void testGroupByExpressionNotAppearingInSelectClauseWhenTableIsEmpty() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 ( s string );");
            try (final PreparedStatement statement = conn.prepareStatement("select count(*) from t1 group by 1+2")) {
                try (ResultSet rs = statement.executeQuery()) {
                    sink.clear();
                    assertResultSet("count[BIGINT]\n", sink, rs);
                }
            }
        });
    }

    @Test
    public void testGroupByExpressionWithBindVariableNotAppearingInSelectClause() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final PreparedStatement statement = conn.prepareStatement("select count(*) from t1 group by 1+?")) {
                statement.setLong(1, 1);
                try (ResultSet rs = statement.executeQuery()) {
                    sink.clear();
                    assertResultSet("count[BIGINT]\n1000\n", sink, rs);
                }
            }
        });
    }

    @Test
    public void testGssApiRequestClosedGracefully() throws Exception {
        assertHexScriptAltCreds(
                """
                        >0000000804d21630
                        <4e
                        """
        );
    }

    @Test
    public void testHappyPathForIntParameterWithoutExplicitParameterTypeHex() throws Exception {
        skipOnWalRun(); // table not created
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000
                        >42000000110000000000010000000133000044000000065000450000000900000000004800000004
                        <31000000043200000004540000002f00027800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200
                        """
        );
    }

    @Test
    public void testHexFragmentedSend() throws Exception {
        skipOnWalRun(); // table not created
        // This is a simple select from long_sequence. A selection of column types.
        // Legacy code behaviour is that for "FLOAT" column it uses DOUBLE formatting. E.g. column
        // is specified as OID 700 on the Row Description, but is sent as 0.46218354.
        // The modern code sends FLOAT as FLOAT, e.g. 0.462 instead.
        assertHexScript(
                """
                        >0000000804d2162f
                        <4e
                        >000000700003000075736572
                        >0061646d696e0064617461
                        >626173650071
                        >646200636c
                        >69656e74
                        >5f656e636f
                        >64696e6700
                        >5554463800446174
                        >655374796c650049
                        >534f0054696d655a
                        >6f6e65004575
                        >726f70652f
                        >4c6f6e64
                        >6f6e006578
                        >7472615f666c6f
                        >61745f64
                        >696769747300320000
                        <520000000800000003
                        >700000
                        >000a7175
                        >65737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >50000000220053455420
                        >65787472615f666c6f61745f64
                        >69
                        >67697473203d2033000000420000
                        >000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700
                        >53
                        >45542061
                        >70
                        >70
                        >6c6963
                        >61
                        >74
                        >696f6e5f6e616d6520
                        >3d
                        >2027506f737467726553514c204a44
                        >424320
                        >44
                        >7269766572
                        >2700000042
                        >0000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >50
                        >000001
                        >94
                        >0073656c
                        >65
                        >637420726e
                        >64
                        >5f737472
                        >28
                        >342c342c34292073
                        >2c20
                        >726e64
                        >5f
                        >69
                        >6e7428302c203235362c20342920692c20726e
                        >645f64
                        >6f75626c65
                        >28
                        >34
                        >2920642c
                        >20
                        >74
                        >696d6573
                        >74
                        >616d705f
                        >73
                        >65717565
                        >6e63
                        >6528302c
                        >31
                        >30
                        >303030
                        >29
                        >20
                        >742c20726e
                        >64
                        >5f
                        >666c6f61
                        >74
                        >28
                        >34292066
                        >2c20726e
                        >64
                        >5f73686f
                        >72
                        >74282920
                        >5f
                        >73
                        >686f72742c
                        >20
                        >726e645f
                        >6c
                        >6f6e67
                        >2830
                        >2c20313030
                        >30
                        >30
                        >303030
                        >2c20
                        >3529206c
                        >2c
                        >20
                        >726e645f
                        >74
                        >696d6573
                        >74
                        >61
                        >6d7028
                        >74
                        >6f
                        >5f74696d
                        >65
                        >7374616d
                        >70
                        >28
                        >27323031
                        >35
                        >272c27797979
                        >79
                        >27292c746f
                        >5f
                        >74696d6573
                        >74
                        >616d702827
                        >32
                        >303136
                        >272c
                        >277979
                        >79
                        >79
                        >27292c
                        >32
                        >29
                        >207473
                        >32
                        >2c
                        >20726e
                        >64
                        >5f
                        >627974
                        >6528
                        >302c31
                        >32
                        >37
                        >29206262
                        >2c
                        >20726e
                        >64
                        >5f626f
                        >6f
                        >6c
                        >65616e
                        >28
                        >29
                        >20622c
                        >20
                        >72
                        >6e645f
                        >73
                        >79
                        >6d626f
                        >6c
                        >28342c34
                        >2c
                        >34
                        >2c3229
                        >2c
                        >20
                        >726e64
                        >5f
                        >646174
                        >65
                        >28
                        >746f5f6461
                        >74
                        >652827
                        >32
                        >303135
                        >27
                        >2c
                        >20
                        >277979
                        >7979
                        >27292c20
                        >74
                        >6f
                        >5f6461
                        >74
                        >652827
                        >32
                        >30
                        >3136272c
                        >2027
                        >797979
                        >79
                        >27292c20
                        >32
                        >29
                        >2c726e
                        >64
                        >5f62696e
                        >28
                        >31302c32
                        >30
                        >2c3229
                        >20
                        >66726f
                        >6d
                        >20
                        >6c6f6e
                        >67
                        >5f
                        >736571
                        >75
                        >656e63
                        >65
                        >28
                        >35302900
                        >00
                        >004200
                        """);
    }

    @Test
    public void testIPv4Operations() throws Exception {
        skipOnWalRun(); // Non-partitioned
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);

            connection.prepareStatement("create table x (a ipv4)").execute();

            connection.prepareStatement("insert into x values('1.1.1.1')").execute();
            connection.prepareStatement("insert into x values('2.2.2.2')").execute();
            connection.prepareStatement("insert into x values('3.3.3.3')").execute();
            connection.commit();

            // netmask
            try (PreparedStatement stmt = connection.prepareStatement("select netmask(?)")) {
                stmt.setString(1, null);
                ResultSet rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                netmask[VARCHAR]
                                null
                                """,
                        sink,
                        rs
                );

                stmt.setString(1, "256.256.256.256/20");
                rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                netmask[VARCHAR]
                                255.255.240.0
                                """,
                        sink,
                        rs
                );
            }

            // <<
            try (PreparedStatement stmt = connection.prepareStatement("x where a << ?")) {
                stmt.setString(1, null);
                ResultSet rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        "a[VARCHAR]\n",
                        sink,
                        rs
                );

                stmt.setString(1, "1.1.1.1/30");
                rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                a[VARCHAR]
                                1.1.1.1
                                """,
                        sink,
                        rs
                );
            }

            // <<=
            try (PreparedStatement stmt = connection.prepareStatement("x where a <<= ?")) {
                stmt.setString(1, null);
                ResultSet rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        "a[VARCHAR]\n",
                        sink,
                        rs
                );

                stmt.setString(1, "2.2.2.2/30");
                rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                a[VARCHAR]
                                2.2.2.2
                                """,
                        sink,
                        rs
                );
            }

            // >>
            try (PreparedStatement stmt = connection.prepareStatement("x where ? >> a")) {
                stmt.setString(1, null);
                ResultSet rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        "a[VARCHAR]\n",
                        sink,
                        rs
                );

                stmt.setString(1, "3.3.3.3/31");
                rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                a[VARCHAR]
                                3.3.3.3
                                """,
                        sink,
                        rs
                );
            }

            // >>=
            try (PreparedStatement stmt = connection.prepareStatement("x where ? >>= a")) {
                stmt.setString(1, null);
                ResultSet rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        "a[VARCHAR]\n",
                        sink,
                        rs
                );

                stmt.setString(1, "1.1.1.1/32");
                rs = stmt.executeQuery();
                sink.clear();
                assertResultSet(
                        """
                                a[VARCHAR]
                                1.1.1.1
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testImplicitCastExceptionInWindowFunction() throws Exception {
        skipOnWalRun(); // Non-WAL
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final String ddl = "CREATE TABLE 'trades' ( " +
                    " symbol SYMBOL, " +
                    " side SYMBOL, " +
                    " price DOUBLE, " +
                    " amount DOUBLE, " +
                    " timestamp TIMESTAMP " +
                    ") timestamp(timestamp) PARTITION BY DAY;";
            try (PreparedStatement stmt = connection.prepareStatement(ddl)) {
                stmt.execute();
            }

            final String insert = "INSERT INTO trades VALUES ('ETH-USD', 'sell', 2615.54, 0.00044, '2022-03-08T18:03:57.609765Z');";
            try (PreparedStatement stmt = connection.prepareStatement(insert)) {
                stmt.execute();
            }

            final String query = "SELECT " +
                    "    timestamp, " +
                    "    price, " +
                    "    lag('timestamp') OVER (ORDER BY timestamp) AS previous_price " +
                    "FROM trades " +
                    "LIMIT 10;";
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.executeQuery();
                Assert.fail();
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "ERROR: inconvertible value: `timestamp` [STRING -> DOUBLE]");
            }
        });
    }

    @Test
    public void testImplicitStringAndCharConversions() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);

            try (PreparedStatement stmt = connection.prepareStatement("select ? > 'a'")) {
                stmt.setString(1, "ab");
                ResultSet resultSet = stmt.executeQuery();

                sink.clear();
                assertResultSet("""
                        column[BIT]
                        true
                        """, sink, resultSet);

                stmt.setString(1, "a");
                resultSet = stmt.executeQuery();

                sink.clear();
                assertResultSet("""
                        column[BIT]
                        false
                        """, sink, resultSet);

                stmt.setString(1, "");
                resultSet = stmt.executeQuery();
                sink.clear();
                assertResultSet("""
                        column[BIT]
                        false
                        """, sink, resultSet);


                stmt.setString(1, null);
                resultSet = stmt.executeQuery();
                sink.clear();
                assertResultSet("""
                        column[BIT]
                        false
                        """, sink, resultSet);
            }
        });
    }

    @Test
    public void testIndexedSymbolBindVariableNotEqualsSingleValueMultipleExecutions() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,3) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(4)" +
                    "), index(b) timestamp(k) partition by DAY").execute();

            sink.clear();

            mayDrainWalQueue();

            try (PreparedStatement ps = connection.prepareStatement("select * from x where b != ?")) {
                ps.setString(1, "VTJW");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    a[DOUBLE],b[VARCHAR],k[TIMESTAMP]
                                    11.427984775756228,null,1970-01-01 00:00:00.0
                                    23.90529010846525,RXGZ,1970-01-03 07:33:20.0
                                    70.94360487171201,PEHN,1970-01-04 11:20:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }

            // Verify that the underlying factory correctly re-calculates
            // the excluded set when the bind variable value changes.
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where b != ?")) {
                ps.setString(1, "RXGZ");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    a[DOUBLE],b[VARCHAR],k[TIMESTAMP]
                                    11.427984775756228,null,1970-01-01 00:00:00.0
                                    42.17768841969397,VTJW,1970-01-02 03:46:40.0
                                    70.94360487171201,PEHN,1970-01-04 11:20:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }

            // The factory should correctly recognize NULL as the excluded value.
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where b != ?")) {
                ps.setString(1, null);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    a[DOUBLE],b[VARCHAR],k[TIMESTAMP]
                                    42.17768841969397,VTJW,1970-01-02 03:46:40.0
                                    23.90529010846525,RXGZ,1970-01-03 07:33:20.0
                                    70.94360487171201,PEHN,1970-01-04 11:20:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testIndexedSymbolBindVariableNotMultipleValuesMultipleExecutions() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,0) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(1)" +
                    "), index(b) timestamp(k) partition by DAY").execute();

            mayDrainWalQueue();

            // First we try to filter out not yet existing keys.
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where b != ? and b != ?")) {
                ps.setString(1, "EHBH");
                ps.setString(2, "BBTG");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    a[DOUBLE],b[VARCHAR],k[TIMESTAMP]
                                    11.427984775756228,HYRX,1970-01-01 00:00:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }

            // Insert new rows including the keys of interest.
            connection.prepareStatement("insert into x " +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,0) b," +
                    " timestamp_sequence(100000000000, 100000000000) k" +
                    " from" +
                    " long_sequence(3)").execute();

            mayDrainWalQueue();

            // The query should filter the keys out.
            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where b != ? and b != ?")) {
                ps.setString(1, "EHBH");
                ps.setString(2, "BBTG");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    a[DOUBLE],b[VARCHAR],k[TIMESTAMP]
                                    11.427984775756228,HYRX,1970-01-01 00:00:00.0
                                    40.22810626779558,EYYQ,1970-01-04 11:20:00.0
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    // Test odd queries that should not be transformed into cursor-based fetches.
    @Test
    public void testInsert() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            int totalRows = 1;
            PreparedStatement tbl = connection.prepareStatement("create table x (a int)");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(a) values(?)");
            for (int i = 0; i < totalRows; i++) {
                insert.setInt(1, i);
                insert.setFetchSize(100); // Should be meaningless.
                insert.execute();
            }
        });
    }

    @Test
    public void testInsert2() throws Exception {
        String expectedAll = """
                a[INTEGER],d[TIMESTAMP],t[TIMESTAMP],d1[TIMESTAMP],t1[TIMESTAMP],t2[TIMESTAMP]
                0,2011-04-11 00:00:00.0,2011-04-11 14:40:54.998821,2011-04-11 14:40:54.998,2011-04-11 00:00:00.0,2011-04-11 14:40:54.998821
                1,2011-04-11 00:00:00.0,2011-04-11 14:40:54.999821,2011-04-11 14:40:54.999,2011-04-11 00:00:00.0,2011-04-11 14:40:54.999821
                2,2011-04-11 00:00:00.0,2011-04-11 14:40:55.000821,2011-04-11 14:40:55.0,2011-04-11 00:00:00.0,2011-04-11 14:40:55.000821
                3,2011-04-11 00:00:00.0,2011-04-11 14:40:55.001821,2011-04-11 14:40:55.001,2011-04-11 00:00:00.0,2011-04-11 14:40:55.001821
                4,2011-04-11 00:00:00.0,2011-04-11 14:40:55.002821,2011-04-11 14:40:55.002,2011-04-11 00:00:00.0,2011-04-11 14:40:55.002821
                5,2011-04-11 00:00:00.0,2011-04-11 14:40:55.003821,2011-04-11 14:40:55.003,2011-04-11 00:00:00.0,2011-04-11 14:40:55.003821
                6,2011-04-11 00:00:00.0,2011-04-11 14:40:55.004821,2011-04-11 14:40:55.004,2011-04-11 00:00:00.0,2011-04-11 14:40:55.004821
                7,2011-04-11 00:00:00.0,2011-04-11 14:40:55.005821,2011-04-11 14:40:55.005,2011-04-11 00:00:00.0,2011-04-11 14:40:55.005821
                8,2011-04-11 00:00:00.0,2011-04-11 14:40:55.006821,2011-04-11 14:40:55.006,2011-04-11 00:00:00.0,2011-04-11 14:40:55.006821
                9,2011-04-11 00:00:00.0,2011-04-11 14:40:55.007821,2011-04-11 14:40:55.007,2011-04-11 00:00:00.0,2011-04-11 14:40:55.007821
                10,2011-04-11 00:00:00.0,2011-04-11 14:40:55.008821,2011-04-11 14:40:55.008,2011-04-11 00:00:00.0,2011-04-11 14:40:55.008821
                11,2011-04-11 00:00:00.0,2011-04-11 14:40:55.009821,2011-04-11 14:40:55.009,2011-04-11 00:00:00.0,2011-04-11 14:40:55.009821
                12,2011-04-11 00:00:00.0,2011-04-11 14:40:55.010821,2011-04-11 14:40:55.01,2011-04-11 00:00:00.0,2011-04-11 14:40:55.010821
                13,2011-04-11 00:00:00.0,2011-04-11 14:40:55.011821,2011-04-11 14:40:55.011,2011-04-11 00:00:00.0,2011-04-11 14:40:55.011821
                14,2011-04-11 00:00:00.0,2011-04-11 14:40:55.012821,2011-04-11 14:40:55.012,2011-04-11 00:00:00.0,2011-04-11 14:40:55.012821
                15,2011-04-11 00:00:00.0,2011-04-11 14:40:55.013821,2011-04-11 14:40:55.013,2011-04-11 00:00:00.0,2011-04-11 14:40:55.013821
                16,2011-04-11 00:00:00.0,2011-04-11 14:40:55.014821,2011-04-11 14:40:55.014,2011-04-11 00:00:00.0,2011-04-11 14:40:55.014821
                17,2011-04-11 00:00:00.0,2011-04-11 14:40:55.015821,2011-04-11 14:40:55.015,2011-04-11 00:00:00.0,2011-04-11 14:40:55.015821
                18,2011-04-11 00:00:00.0,2011-04-11 14:40:55.016821,2011-04-11 14:40:55.016,2011-04-11 00:00:00.0,2011-04-11 14:40:55.016821
                19,2011-04-11 00:00:00.0,2011-04-11 14:40:55.017821,2011-04-11 14:40:55.017,2011-04-11 00:00:00.0,2011-04-11 14:40:55.017821
                20,2011-04-11 00:00:00.0,2011-04-11 14:40:55.018821,2011-04-11 14:40:55.018,2011-04-11 00:00:00.0,2011-04-11 14:40:55.018821
                21,2011-04-11 00:00:00.0,2011-04-11 14:40:55.019821,2011-04-11 14:40:55.019,2011-04-11 00:00:00.0,2011-04-11 14:40:55.019821
                22,2011-04-11 00:00:00.0,2011-04-11 14:40:55.020821,2011-04-11 14:40:55.02,2011-04-11 00:00:00.0,2011-04-11 14:40:55.020821
                23,2011-04-11 00:00:00.0,2011-04-11 14:40:55.021821,2011-04-11 14:40:55.021,2011-04-11 00:00:00.0,2011-04-11 14:40:55.021821
                24,2011-04-11 00:00:00.0,2011-04-11 14:40:55.022821,2011-04-11 14:40:55.022,2011-04-11 00:00:00.0,2011-04-11 14:40:55.022821
                25,2011-04-11 00:00:00.0,2011-04-11 14:40:55.023821,2011-04-11 14:40:55.023,2011-04-11 00:00:00.0,2011-04-11 14:40:55.023821
                26,2011-04-11 00:00:00.0,2011-04-11 14:40:55.024821,2011-04-11 14:40:55.024,2011-04-11 00:00:00.0,2011-04-11 14:40:55.024821
                27,2011-04-11 00:00:00.0,2011-04-11 14:40:55.025821,2011-04-11 14:40:55.025,2011-04-11 00:00:00.0,2011-04-11 14:40:55.025821
                28,2011-04-11 00:00:00.0,2011-04-11 14:40:55.026821,2011-04-11 14:40:55.026,2011-04-11 00:00:00.0,2011-04-11 14:40:55.026821
                29,2011-04-11 00:00:00.0,2011-04-11 14:40:55.027821,2011-04-11 14:40:55.027,2011-04-11 00:00:00.0,2011-04-11 14:40:55.027821
                30,2011-04-11 00:00:00.0,2011-04-11 14:40:55.028821,2011-04-11 14:40:55.028,2011-04-11 00:00:00.0,2011-04-11 14:40:55.028821
                31,2011-04-11 00:00:00.0,2011-04-11 14:40:55.029821,2011-04-11 14:40:55.029,2011-04-11 00:00:00.0,2011-04-11 14:40:55.029821
                32,2011-04-11 00:00:00.0,2011-04-11 14:40:55.030821,2011-04-11 14:40:55.03,2011-04-11 00:00:00.0,2011-04-11 14:40:55.030821
                33,2011-04-11 00:00:00.0,2011-04-11 14:40:55.031821,2011-04-11 14:40:55.031,2011-04-11 00:00:00.0,2011-04-11 14:40:55.031821
                34,2011-04-11 00:00:00.0,2011-04-11 14:40:55.032821,2011-04-11 14:40:55.032,2011-04-11 00:00:00.0,2011-04-11 14:40:55.032821
                35,2011-04-11 00:00:00.0,2011-04-11 14:40:55.033821,2011-04-11 14:40:55.033,2011-04-11 00:00:00.0,2011-04-11 14:40:55.033821
                36,2011-04-11 00:00:00.0,2011-04-11 14:40:55.034821,2011-04-11 14:40:55.034,2011-04-11 00:00:00.0,2011-04-11 14:40:55.034821
                37,2011-04-11 00:00:00.0,2011-04-11 14:40:55.035821,2011-04-11 14:40:55.035,2011-04-11 00:00:00.0,2011-04-11 14:40:55.035821
                38,2011-04-11 00:00:00.0,2011-04-11 14:40:55.036821,2011-04-11 14:40:55.036,2011-04-11 00:00:00.0,2011-04-11 14:40:55.036821
                39,2011-04-11 00:00:00.0,2011-04-11 14:40:55.037821,2011-04-11 14:40:55.037,2011-04-11 00:00:00.0,2011-04-11 14:40:55.037821
                40,2011-04-11 00:00:00.0,2011-04-11 14:40:55.038821,2011-04-11 14:40:55.038,2011-04-11 00:00:00.0,2011-04-11 14:40:55.038821
                41,2011-04-11 00:00:00.0,2011-04-11 14:40:55.039821,2011-04-11 14:40:55.039,2011-04-11 00:00:00.0,2011-04-11 14:40:55.039821
                42,2011-04-11 00:00:00.0,2011-04-11 14:40:55.040821,2011-04-11 14:40:55.04,2011-04-11 00:00:00.0,2011-04-11 14:40:55.040821
                43,2011-04-11 00:00:00.0,2011-04-11 14:40:55.041821,2011-04-11 14:40:55.041,2011-04-11 00:00:00.0,2011-04-11 14:40:55.041821
                44,2011-04-11 00:00:00.0,2011-04-11 14:40:55.042821,2011-04-11 14:40:55.042,2011-04-11 00:00:00.0,2011-04-11 14:40:55.042821
                45,2011-04-11 00:00:00.0,2011-04-11 14:40:55.043821,2011-04-11 14:40:55.043,2011-04-11 00:00:00.0,2011-04-11 14:40:55.043821
                46,2011-04-11 00:00:00.0,2011-04-11 14:40:55.044821,2011-04-11 14:40:55.044,2011-04-11 00:00:00.0,2011-04-11 14:40:55.044821
                47,2011-04-11 00:00:00.0,2011-04-11 14:40:55.045821,2011-04-11 14:40:55.045,2011-04-11 00:00:00.0,2011-04-11 14:40:55.045821
                48,2011-04-11 00:00:00.0,2011-04-11 14:40:55.046821,2011-04-11 14:40:55.046,2011-04-11 00:00:00.0,2011-04-11 14:40:55.046821
                49,2011-04-11 00:00:00.0,2011-04-11 14:40:55.047821,2011-04-11 14:40:55.047,2011-04-11 00:00:00.0,2011-04-11 14:40:55.047821
                50,2011-04-11 00:00:00.0,2011-04-11 14:40:55.048821,2011-04-11 14:40:55.048,2011-04-11 00:00:00.0,2011-04-11 14:40:55.048821
                51,2011-04-11 00:00:00.0,2011-04-11 14:40:55.049821,2011-04-11 14:40:55.049,2011-04-11 00:00:00.0,2011-04-11 14:40:55.049821
                52,2011-04-11 00:00:00.0,2011-04-11 14:40:55.050821,2011-04-11 14:40:55.05,2011-04-11 00:00:00.0,2011-04-11 14:40:55.050821
                53,2011-04-11 00:00:00.0,2011-04-11 14:40:55.051821,2011-04-11 14:40:55.051,2011-04-11 00:00:00.0,2011-04-11 14:40:55.051821
                54,2011-04-11 00:00:00.0,2011-04-11 14:40:55.052821,2011-04-11 14:40:55.052,2011-04-11 00:00:00.0,2011-04-11 14:40:55.052821
                55,2011-04-11 00:00:00.0,2011-04-11 14:40:55.053821,2011-04-11 14:40:55.053,2011-04-11 00:00:00.0,2011-04-11 14:40:55.053821
                56,2011-04-11 00:00:00.0,2011-04-11 14:40:55.054821,2011-04-11 14:40:55.054,2011-04-11 00:00:00.0,2011-04-11 14:40:55.054821
                57,2011-04-11 00:00:00.0,2011-04-11 14:40:55.055821,2011-04-11 14:40:55.055,2011-04-11 00:00:00.0,2011-04-11 14:40:55.055821
                58,2011-04-11 00:00:00.0,2011-04-11 14:40:55.056821,2011-04-11 14:40:55.056,2011-04-11 00:00:00.0,2011-04-11 14:40:55.056821
                59,2011-04-11 00:00:00.0,2011-04-11 14:40:55.057821,2011-04-11 14:40:55.057,2011-04-11 00:00:00.0,2011-04-11 14:40:55.057821
                60,2011-04-11 00:00:00.0,2011-04-11 14:40:55.058821,2011-04-11 14:40:55.058,2011-04-11 00:00:00.0,2011-04-11 14:40:55.058821
                61,2011-04-11 00:00:00.0,2011-04-11 14:40:55.059821,2011-04-11 14:40:55.059,2011-04-11 00:00:00.0,2011-04-11 14:40:55.059821
                62,2011-04-11 00:00:00.0,2011-04-11 14:40:55.060821,2011-04-11 14:40:55.06,2011-04-11 00:00:00.0,2011-04-11 14:40:55.060821
                63,2011-04-11 00:00:00.0,2011-04-11 14:40:55.061821,2011-04-11 14:40:55.061,2011-04-11 00:00:00.0,2011-04-11 14:40:55.061821
                64,2011-04-11 00:00:00.0,2011-04-11 14:40:55.062821,2011-04-11 14:40:55.062,2011-04-11 00:00:00.0,2011-04-11 14:40:55.062821
                65,2011-04-11 00:00:00.0,2011-04-11 14:40:55.063821,2011-04-11 14:40:55.063,2011-04-11 00:00:00.0,2011-04-11 14:40:55.063821
                66,2011-04-11 00:00:00.0,2011-04-11 14:40:55.064821,2011-04-11 14:40:55.064,2011-04-11 00:00:00.0,2011-04-11 14:40:55.064821
                67,2011-04-11 00:00:00.0,2011-04-11 14:40:55.065821,2011-04-11 14:40:55.065,2011-04-11 00:00:00.0,2011-04-11 14:40:55.065821
                68,2011-04-11 00:00:00.0,2011-04-11 14:40:55.066821,2011-04-11 14:40:55.066,2011-04-11 00:00:00.0,2011-04-11 14:40:55.066821
                69,2011-04-11 00:00:00.0,2011-04-11 14:40:55.067821,2011-04-11 14:40:55.067,2011-04-11 00:00:00.0,2011-04-11 14:40:55.067821
                70,2011-04-11 00:00:00.0,2011-04-11 14:40:55.068821,2011-04-11 14:40:55.068,2011-04-11 00:00:00.0,2011-04-11 14:40:55.068821
                71,2011-04-11 00:00:00.0,2011-04-11 14:40:55.069821,2011-04-11 14:40:55.069,2011-04-11 00:00:00.0,2011-04-11 14:40:55.069821
                72,2011-04-11 00:00:00.0,2011-04-11 14:40:55.070821,2011-04-11 14:40:55.07,2011-04-11 00:00:00.0,2011-04-11 14:40:55.070821
                73,2011-04-11 00:00:00.0,2011-04-11 14:40:55.071821,2011-04-11 14:40:55.071,2011-04-11 00:00:00.0,2011-04-11 14:40:55.071821
                74,2011-04-11 00:00:00.0,2011-04-11 14:40:55.072821,2011-04-11 14:40:55.072,2011-04-11 00:00:00.0,2011-04-11 14:40:55.072821
                75,2011-04-11 00:00:00.0,2011-04-11 14:40:55.073821,2011-04-11 14:40:55.073,2011-04-11 00:00:00.0,2011-04-11 14:40:55.073821
                76,2011-04-11 00:00:00.0,2011-04-11 14:40:55.074821,2011-04-11 14:40:55.074,2011-04-11 00:00:00.0,2011-04-11 14:40:55.074821
                77,2011-04-11 00:00:00.0,2011-04-11 14:40:55.075821,2011-04-11 14:40:55.075,2011-04-11 00:00:00.0,2011-04-11 14:40:55.075821
                78,2011-04-11 00:00:00.0,2011-04-11 14:40:55.076821,2011-04-11 14:40:55.076,2011-04-11 00:00:00.0,2011-04-11 14:40:55.076821
                79,2011-04-11 00:00:00.0,2011-04-11 14:40:55.077821,2011-04-11 14:40:55.077,2011-04-11 00:00:00.0,2011-04-11 14:40:55.077821
                80,2011-04-11 00:00:00.0,2011-04-11 14:40:55.078821,2011-04-11 14:40:55.078,2011-04-11 00:00:00.0,2011-04-11 14:40:55.078821
                81,2011-04-11 00:00:00.0,2011-04-11 14:40:55.079821,2011-04-11 14:40:55.079,2011-04-11 00:00:00.0,2011-04-11 14:40:55.079821
                82,2011-04-11 00:00:00.0,2011-04-11 14:40:55.080821,2011-04-11 14:40:55.08,2011-04-11 00:00:00.0,2011-04-11 14:40:55.080821
                83,2011-04-11 00:00:00.0,2011-04-11 14:40:55.081821,2011-04-11 14:40:55.081,2011-04-11 00:00:00.0,2011-04-11 14:40:55.081821
                84,2011-04-11 00:00:00.0,2011-04-11 14:40:55.082821,2011-04-11 14:40:55.082,2011-04-11 00:00:00.0,2011-04-11 14:40:55.082821
                85,2011-04-11 00:00:00.0,2011-04-11 14:40:55.083821,2011-04-11 14:40:55.083,2011-04-11 00:00:00.0,2011-04-11 14:40:55.083821
                86,2011-04-11 00:00:00.0,2011-04-11 14:40:55.084821,2011-04-11 14:40:55.084,2011-04-11 00:00:00.0,2011-04-11 14:40:55.084821
                87,2011-04-11 00:00:00.0,2011-04-11 14:40:55.085821,2011-04-11 14:40:55.085,2011-04-11 00:00:00.0,2011-04-11 14:40:55.085821
                88,2011-04-11 00:00:00.0,2011-04-11 14:40:55.086821,2011-04-11 14:40:55.086,2011-04-11 00:00:00.0,2011-04-11 14:40:55.086821
                89,2011-04-11 00:00:00.0,2011-04-11 14:40:55.087821,2011-04-11 14:40:55.087,2011-04-11 00:00:00.0,2011-04-11 14:40:55.087821
                """;

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            //
            // test methods of inserting QuestDB's DATA and TIMESTAMP values
            //
            final PreparedStatement statement = connection.prepareStatement("create table x (a int, d date, t timestamp, d1 date, t1 timestamp, t2 timestamp) timestamp(t) partition by DAY");
            statement.execute();

            // exercise parameters on select statement
            PreparedStatement select = connection.prepareStatement("x where a = ?");
            execSelectWithParam(select, 9);


            try (final PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?, ?, ?, ?, ?)")) {
                long micros = MicrosFormatUtils.parseTimestamp("2011-04-11T14:40:54.998821Z");
                for (int i = 0; i < 90; i++) {
                    insert.setInt(1, i);
                    // DATE as jdbc's DATE
                    // jdbc's DATE takes millis from epoch and i think it removes time element from it, leaving
                    // just date
                    insert.setDate(2, new Date(micros / 1000));

                    // TIMESTAMP as jdbc's TIMESTAMP, this should keep the micros
                    Timestamp ts;

                    ts = new Timestamp(micros / 1000L);
                    ts.setNanos((int) ((micros % 1_000_000L) * 1000L));
                    insert.setTimestamp(3, ts);

                    // DATE as jdbc's TIMESTAMP, both millis
                    ts = new Timestamp(micros / 1000L);
                    insert.setTimestamp(4, ts);

                    // TIMESTAMP as jdbc's DATE, DATE takes millis keep only date part
                    insert.setDate(5, new Date(micros / 1000L));

                    // TIMESTAMP as PG specific TIMESTAMP type
                    PGTimestamp pgTs = new PGTimestamp(micros / 1000L);
                    pgTs.setNanos((int) ((micros % 1_000_000L) * 1000));
                    insert.setTimestamp(6, pgTs);

                    insert.execute();
                    Assert.assertEquals(1, insert.getUpdateCount());
                    micros += 1000;
                }
            }

            mayDrainWalQueue();

            try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                sink.clear();
                assertResultSet(expectedAll, sink, resultSet);
            }

            TestUtils.assertEquals(expectedAll, sink);

            // exercise parameters on select statement
            execSelectWithParam(select, 9);
            TestUtils.assertEquals("9\n", sink);

            execSelectWithParam(select, 11);
            TestUtils.assertEquals("11\n", sink);
        });
    }

    @Test
    public void testInsert2dArrayBindingVars() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                Array arr = connection.createArrayOf("float8", new Double[][]{{1d, 2d, 3d}, {4d, 5d, 6d}});
                stmt.setArray(1, arr);
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet(
                            """
                                    al[ARRAY]
                                    {{1.0,2.0,3.0},{4.0,5.0,6.0}}
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testInsert2dArrayBindingVarsWrongDimensions() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                final Array arr = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d, 4d, 5d});
                stmt.setArray(1, arr);
                try {
                    stmt.execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContainsEither(
                            e.getMessage(),
                            "array type mismatch [expected=DOUBLE[][], actual=DOUBLE[]]",
                            "inconvertible value: `{\"1.0\",\"2.0\",\"3.0\",\"4.0\",\"5.0\"}` [STRING -> DOUBLE[][]]"
                    );
                }
            }
        });
    }

    @Test
    public void testInsertAllTypes() throws Exception {
        skipOnWalRun(); // non-partitioned table
        // heavy bind variable use
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            execute("create table xyz (" +
                    "a byte," +
                    "b char," +
                    "c short," +
                    "d int," +
                    "e long," +
                    "f float," +
                    "g double," +
                    "h string," +
                    "i symbol," +
                    "j boolean," +
                    "k long256" +
                    ")"
            );
            try (
                    final PreparedStatement insert = connection.prepareStatement(
                            "insert into xyz values (?,?,?,?,?,?,?,?,?,?,?)"
                    )
            ) {
                final Rnd rnd = new Rnd();
                connection.setAutoCommit(false);
                for (int i = 0; i < 1_000; i++) {
                    if (rnd.nextInt() % 4 > 0) {
                        insert.setByte(1, rnd.nextByte());
                    } else {
                        insert.setNull(1, Types.SMALLINT);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setString(2, rnd.nextString(1));
                    } else {
                        insert.setNull(2, Types.VARCHAR);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setShort(3, rnd.nextShort());
                    } else {
                        insert.setNull(3, Types.SMALLINT);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setInt(4, rnd.nextInt());
                    } else {
                        insert.setNull(4, Types.INTEGER);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setLong(5, rnd.nextLong());
                    } else {
                        insert.setNull(5, Types.BIGINT);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setFloat(6, rnd.nextFloat());
                    } else {
                        insert.setNull(6, Types.REAL);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setDouble(7, rnd.nextDouble());
                    } else {
                        insert.setNull(7, Types.FLOAT);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setString(8, "hello21");
                    } else {
                        insert.setNull(8, Types.VARCHAR);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setString(9, "bus");
                    } else {
                        insert.setNull(9, Types.VARCHAR);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setBoolean(10, true);
                    } else {
                        insert.setNull(10, Types.BOOLEAN);
                    }

                    if (rnd.nextInt() % 4 > 0) {
                        insert.setString(11, "05a9796963abad00001e5f6bbdb38");
                    } else {
                        insert.setNull(11, Types.VARCHAR);
                    }
                    insert.execute();
                    Assert.assertEquals(1, insert.getUpdateCount());
                }
                connection.commit();

                rnd.reset();
                try (RecordCursorFactory factory = select("xyz")) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        int count1 = 0;
                        while (cursor.hasNext()) {

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextByte(), record.getByte(0));
                            } else {
                                Assert.assertEquals(0, record.getByte(0));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextChar(), record.getChar(1));
                            } else {
                                Assert.assertEquals(0, record.getChar(1));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextShort(), record.getShort(2));
                            } else {
                                Assert.assertEquals(0, record.getShort(2));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextInt(), record.getInt(3));
                            } else {
                                Assert.assertEquals(Numbers.INT_NULL, record.getInt(3));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextLong(), record.getLong(4));
                            } else {
                                Assert.assertEquals(Numbers.LONG_NULL, record.getLong(4));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextFloat(), record.getFloat(5), 0.0001f);
                            } else {
                                Assert.assertTrue(Float.isNaN(record.getFloat(5)));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertEquals(rnd.nextDouble(), record.getDouble(6), 0.000001);
                            } else {
                                Assert.assertTrue(Double.isNaN(record.getDouble(6)));
                            }

                            final int strType = ColumnType.typeOf("STRING");
                            if (rnd.nextInt() % 4 > 0) {
                                if (strType == ColumnType.VARCHAR) {
                                    sink.clear();
                                    record.getVarchar(7, sink);
                                    TestUtils.assertEquals("hello21", sink);
                                } else {
                                    TestUtils.assertEquals("hello21", record.getStrA(7));
                                }
                            } else {
                                if (strType == ColumnType.VARCHAR) {
                                    Assert.assertNull(record.getVarcharA(7));
                                } else {
                                    Assert.assertNull(record.getStrA(7));
                                }
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                TestUtils.assertEquals("bus", record.getSymA(8));
                            } else {
                                Assert.assertNull(record.getSymA(8));
                            }

                            if (rnd.nextInt() % 4 > 0) {
                                Assert.assertTrue(record.getBool(9));
                            } else {
                                Assert.assertFalse(record.getBool(9));
                            }

                            sink.clear();
                            record.getLong256(10, sink);
                            if (rnd.nextInt() % 4 > 0) {
                                TestUtils.assertEquals("0x5a9796963abad00001e5f6bbdb38", sink);
                            } else {

                                Assert.assertEquals(0, sink.length());
                            }
                            count1++;
                        }

                        Assert.assertEquals(1_000, count1);
                    }
                }
            }
        });
    }

    @Test
    public void testInsertAsSelect() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            conn.createStatement().execute("create table x(ts timestamp, col1 long) timestamp(ts) partition by day wal");
            conn.createStatement().execute("create table y(ts timestamp, col1 long) timestamp(ts) partition by day wal");

            assertEquals(10, conn.createStatement().executeUpdate("insert into x select timestamp_sequence(1677628800000000, 10000000), x from long_sequence(10)"));
            drainWalQueue();
            assertSql(
                    conn,
                    "x",
                    """
                            ts[TIMESTAMP],col1[BIGINT]
                            2023-03-01 00:00:00.0,1
                            2023-03-01 00:00:10.0,2
                            2023-03-01 00:00:20.0,3
                            2023-03-01 00:00:30.0,4
                            2023-03-01 00:00:40.0,5
                            2023-03-01 00:00:50.0,6
                            2023-03-01 00:01:00.0,7
                            2023-03-01 00:01:10.0,8
                            2023-03-01 00:01:20.0,9
                            2023-03-01 00:01:30.0,10
                            """
            );
            assertSql(
                    conn,
                    "y",
                    "ts[TIMESTAMP],col1[BIGINT]\n"
            );

            assertEquals(2, conn.createStatement().executeUpdate("insert into y select * from x where col1 > 8"));
            drainWalQueue();
            assertSql(
                    conn,
                    "y",
                    """
                            ts[TIMESTAMP],col1[BIGINT]
                            2023-03-01 00:01:20.0,9
                            2023-03-01 00:01:30.0,10
                            """
            );
        });
    }

    @Test
    public void testInsertBinaryBindVariable() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (final PreparedStatement insert = connection.prepareStatement("insert into xyz values (?)")) {
                execute("create table xyz (" +
                        "a binary" +
                        ")"
                );
                connection.setAutoCommit(false);
                int totalCount = 10;
                for (int i = 0; i < totalCount; i++) {
                    insert.setBytes(1, new byte[]{1, 2, 3, 4});
                    insert.execute();
                }
                connection.commit();

                try (RecordCursorFactory factory = select("xyz")) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        int count1 = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(4, record.getBinLen(0));
                            count1++;
                        }

                        Assert.assertEquals(totalCount, count1);
                    }
                }
            }
        });
    }

    @Test
    public void testInsertBinaryOver200KbBinaryProtocol() throws Exception {
        final int maxLength = 200 * 1024;
        testBinaryInsert(maxLength, Math.max(recvBufferSize, maxLength + 100), Math.max(sendBufferSize, maxLength + 100));
    }

    @Test
    public void testInsertBinaryOverHalfMb() throws Exception {
        final int maxLength = 524287;
        testBinaryInsert(maxLength, Math.max(recvBufferSize, maxLength + 100), Math.max(sendBufferSize, maxLength + 100));
    }

    @Test
    public void testInsertBinaryOverRecvOverflow() throws Exception {
        final int maxLength = 524287;
        try {
            testBinaryInsert(maxLength, 2048, 1024 * 1024 + 100);
            Assert.fail();
        } catch (PSQLException e) {
            TestUtils.assertContains(e.getMessage(), "An I/O error occurred while sending to the backend");
        }
    }

    @Test
    public void testInsertBinarySendBufOverflow() throws Exception {
        final int maxLength = 524287;
        try {
            testBinaryInsert(maxLength, 1024 * 1024 + 100, 2048);
            Assert.fail();
        } catch (PSQLException e) {
            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
        }
    }

    @Test
    public void testInsertBooleans() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement(
                    "create table booleans (value boolean, ts timestamp) timestamp(ts) partition by YEAR"
            ).execute();

            Rnd rand = new Rnd();
            String[] values = {"TrUE", null, "", "false", "true", "banana", "22"};

            try (PreparedStatement insert = connection.prepareStatement("insert into booleans values (cast(? as boolean), ?)")) {
                long micros = MicrosFormatUtils.parseTimestamp("2022-04-19T18:50:00.998666Z");
                for (int i = 0; i < 30; i++) {
                    insert.setString(1, values[rand.nextInt(values.length)]);
                    insert.setTimestamp(2, new Timestamp(micros / 1000L));
                    insert.execute();
                    Assert.assertEquals(1, insert.getUpdateCount());
                    micros += 1_000_000L;
                }
            }

            mayDrainWalQueue();
            try (ResultSet resultSet = connection.prepareStatement("booleans").executeQuery()) {
                sink.clear();
                assertResultSet(
                        """
                                value[BIT],ts[TIMESTAMP]
                                true,2022-04-19 18:50:00.998
                                false,2022-04-19 18:50:01.998
                                false,2022-04-19 18:50:02.998
                                true,2022-04-19 18:50:03.998
                                false,2022-04-19 18:50:04.998
                                false,2022-04-19 18:50:05.998
                                false,2022-04-19 18:50:06.998
                                false,2022-04-19 18:50:07.998
                                false,2022-04-19 18:50:08.998
                                true,2022-04-19 18:50:09.998
                                false,2022-04-19 18:50:10.998
                                false,2022-04-19 18:50:11.998
                                false,2022-04-19 18:50:12.998
                                false,2022-04-19 18:50:13.998
                                false,2022-04-19 18:50:14.998
                                false,2022-04-19 18:50:15.998
                                false,2022-04-19 18:50:16.998
                                true,2022-04-19 18:50:17.998
                                false,2022-04-19 18:50:18.998
                                true,2022-04-19 18:50:19.998
                                false,2022-04-19 18:50:20.998
                                false,2022-04-19 18:50:21.998
                                false,2022-04-19 18:50:22.998
                                true,2022-04-19 18:50:23.998
                                true,2022-04-19 18:50:24.998
                                true,2022-04-19 18:50:25.998
                                true,2022-04-19 18:50:26.998
                                false,2022-04-19 18:50:27.998
                                false,2022-04-19 18:50:28.998
                                false,2022-04-19 18:50:29.998
                                """,
                        sink,
                        resultSet
                );
            }
        });
    }

    @Test
    public void testInsertDateAndTimestampFromRustHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000004300030000636c69656e745f656e636f64696e6700555446380074696d657a6f6e650055544300757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5100000063435245415445205441424c45204946204e4f54204558495354532072757374202874732054494d455354414d502c20647420444154452c206e616d6520535452494e472c2076616c756520494e54292074696d657374616d70287473293b00
                <43000000074f4b005a0000000549
                >500000002e733000494e5345525420494e544f20727573742056414c5545532824312c24322c24332c2434290000004400000008537330005300000004
                <3100000004740000001600040000045a0000045a00000413000000176e000000045a0000000549
                >4200000042007330000001000100040000000800025c7a454d92ad0000000800025c7a454d92ad0000000c72757374206578616d706c65000000040000007b00010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000549
                >4300000008537330005300000004
                <33000000045a0000000549
                >5800000004""";
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testInsertDoubleTableWithTypeSuffix() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (val double)");
            statement.execute();

            // mimics the behavior of Python drivers
            // which will set NaN and Inf into string with ::float suffix
            final PreparedStatement insert = connection.prepareStatement("insert into x values " +
                    "('NaN'::float)," +
                    "('Infinity'::float)," +
                    "('-Infinity'::float)," +
                    "('1.234567890123'::float)");
            insert.execute();

            final String expectedAbleToInsertToDoubleTable = """
                    val[DOUBLE]
                    null
                    null
                    null
                    1.234567890123
                    """;
            try (ResultSet resultSet = connection.prepareStatement("select * from x").executeQuery()) {
                sink.clear();
                assertResultSet(expectedAbleToInsertToDoubleTable, sink, resultSet);
            }

            final String expectedInsertWithoutLosingPrecision = """
                    val[DOUBLE]
                    1.234567890123
                    """;
            try (ResultSet resultSet = connection.prepareStatement("select * from x where val = cast('1.234567890123' as double)").executeQuery()) {
                sink.clear();
                assertResultSet(expectedInsertWithoutLosingPrecision, sink, resultSet);
            }
        });
    }

    @Test
    public void testInsertExtendedAndCommit() throws Exception {
        String expectedAll = "count[BIGINT]\n10000\n";
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            //
            // test methods of inserting QuestDB's DATA and TIMESTAMP values
            //
            final PreparedStatement statement = connection.prepareStatement(
                    "CREATE TABLE x " +
                            "(a INT, d DATE, t TIMESTAMP, d1 DATE, t1 TIMESTAMP, t3 TIMESTAMP, b1 SHORT, t4 TIMESTAMP) " +
                            "TIMESTAMP(t) PARTITION BY YEAR");
            statement.execute();

            // exercise parameters on select statement
            PreparedStatement select = connection.prepareStatement("x where a = ?");
            execSelectWithParam(select, 9);


            final PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO x VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            String date = "2011-04-11";
            String time = "14:40:54.998821";
            long micros = MicrosFormatUtils.parseTimestamp(date + "T" + time + "Z");
            for (int i = 0; i < 10_000; i++) {
                insert.setInt(1, i);
                // DATE as jdbc's DATE
                // jdbc's DATE takes millis from epoch and i think it removes time element from it, leaving
                // just date
                insert.setDate(2, new Date(micros / 1000));

                // TIMESTAMP as jdbc's TIMESTAMP, this should keep the micros
                insert.setTimestamp(3, Timestamp.valueOf(date + " " + time));

                // DATE as jdbc's TIMESTAMP, this should keep millis and we need to supply millis
                insert.setTimestamp(4, new Timestamp(micros / 1000L));

                // TIMESTAMP as jdbc's DATE, DATE takes millis and throws them away
                insert.setDate(5, new Date(micros));

                // TIMESTAMP as PG specific TIMESTAMP type
                insert.setTimestamp(6, new PGTimestamp(micros));

                insert.setByte(7, (byte) 'A');

                // TIMESTAMP as long
                insert.setLong(8, micros);

                insert.execute();
                Assert.assertEquals(1, insert.getUpdateCount());
                micros += 1;

                if (i % 128 == 0) {
                    connection.commit();
                }
            }
            connection.commit();
            mayDrainWalQueue();

            try (ResultSet resultSet = connection.prepareStatement("select count() from x").executeQuery()) {
                sink.clear();
                assertResultSet(expectedAll, sink, resultSet);
            }

            TestUtils.assertEquals(expectedAll, sink);

            // exercise parameters on select statement
            execSelectWithParam(select, 9);
            TestUtils.assertEquals("9\n", sink);

            execSelectWithParam(select, 11);
            TestUtils.assertEquals("11\n", sink);
        });
    }

    @Test
    public void testInsertFloatTableWithTypeSuffix() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (val float)");
            statement.execute();

            // mimics the behavior of Python drivers
            // which will set NaN and Inf into string with ::float suffix
            final PreparedStatement insert = connection.prepareStatement("insert into x values " +
                    "('null'::float)," +
                    "('Infinity'::float)," +
                    "('-Infinity'::float)," +
                    "('1.234567890123'::float)");  // should be first cast info double, then cast to float on insert
            insert.execute();

            final String expectedAbleToInsertToFloatTable = """
                    val[REAL]
                    null
                    null
                    null
                    1.2345679
                    """;
            try (ResultSet resultSet = connection.prepareStatement("select * from x").executeQuery()) {
                sink.clear();
                assertResultSet(expectedAbleToInsertToFloatTable, sink, resultSet);
            }

            final String expectedInsertWithLosingPrecision = """
                    val[REAL]
                    1.2345679
                    """;
            try (ResultSet resultSet = connection.prepareStatement("select * from x where val = 1.23456788063").executeQuery()) {
                sink.clear();
                assertResultSet(expectedInsertWithLosingPrecision, sink, resultSet);
            }
        });
    }

    /*
    nodejs code:
    ------------------
            const { Client } = require("pg")

            const start = async () => {
              try {
                const client = new Client({
                  database: "qdb",
                  host: "127.0.0.1",
                  password: "quest",
                  port: 8812,
                  user: "admin",
                })
                await client.connect()

                const res = await client.query("INSERT INTO test VALUES($1, $2);", [
                  "abc",
                  "123"
                ])

                console.log(res)

                await client.end()
              } catch (e) {
                console.log(e)
              }
            }

            start()
    ------------------
     */
    @Test
    public void testInsertFomNodeJsWith2Parameters_TableDoesNotExist() throws Exception {
        skipOnWalRun(); // non-partitioned table
        final String script = """
                >0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                """;
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, getStdPgWireConfigAltCreds());
    }

    /*
nodejs code:
------------------
        const { Client } = require("pg")

        const start = async () => {
          try {
            const client = new Client({
              database: "qdb",
              host: "127.0.0.1",
              password: "quest",
              port: 8812,
              user: "admin",
            })
            await client.connect()

            const res = await client.query("CREATE TABLE test (id string, number int);")

            const res = await client.query("INSERT INTO test VALUES($1, $2);", [
              "abc",
              "123"
            ])

            console.log(res)

            await client.end()
          } catch (e) {
            console.log(e)
          }
        }

        start()
------------------
 */
    @Test
    public void testInsertFomNodeJsWith2Parameters_WithTableCreation() throws Exception {
        skipOnWalRun(); // non-partitioned table
        final String script = """
                >0000006900030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >510000002f435245415445205441424c4520746573742028696420737472696e672c206e756d62657220696e74293b00
                <43000000074f4b005a0000000549
                >500000002800494e5345525420494e544f20746573742056414c5545532824312c202432293b000000420000001a00000000000200000003616263000000033132330000
                >44000000065000450000000900000000004800000004
                <310000000432000000046e00000004430000000f494e534552542030203100
                >4800000004
                >5300000004
                <5a0000000549
                >5800000004
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testInsertNoMemLeak() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE test(id LONG);");
            }

            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO test values(alloc(42));")) {
                insert.execute();
                // execute insert multiple times to verify cache interaction
                insert.execute();
            }

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("test;");
                assertResultSet(
                        """
                                id[BIGINT]
                                42
                                42
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testInsertPreparedRenameInsert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE ts (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ts VALUES(?, ?)")) {
                insert.setInt(1, 0);
                insert.setTimestamp(2, new Timestamp(1632761103202L));
                insert.execute();
                connection.commit();

                insert.setInt(1, 1);
                insert.setTimestamp(2, new Timestamp(1632761103203L));
                insert.execute();
                connection.commit();

                connection.prepareStatement("rename table ts to ts2").execute();
                try {
                    insert.execute();
                    fail();
                } catch (PSQLException ex) {
                    TestUtils.assertContains(ex.getMessage(), "table does not exist [table=ts]");
                }
                connection.commit();
            }

            mayDrainWalQueue();

            sink.clear();
            try (
                    PreparedStatement ps = connection.prepareStatement("ts2");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertResultSet(
                        """
                                id[INTEGER],ts[TIMESTAMP]
                                0,2021-09-27 16:45:03.202
                                1,2021-09-27 16:45:03.203
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testInsertSelectRenameException() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE ts (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
            connection.prepareStatement("CREATE TABLE ts1 (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ts VALUES(?, ?)");
                 PreparedStatement insert2 = connection.prepareStatement("INSERT INTO ts1 SELECT id, ts from ts where ts = ?")
            ) {
                insert.setInt(1, 0);
                insert.setTimestamp(2, new Timestamp(1632761103202L));
                insert2.setTimestamp(1, new Timestamp(1632761103202L));
                insert.execute();
                connection.commit();
                mayDrainWalQueue();
                insert2.execute();
                connection.commit();

                connection.prepareStatement("rename table ts to ts_bak").execute();
                try {
                    insert2.execute();
                    fail();
                } catch (PSQLException ex) {
                    TestUtils.assertContains(ex.getMessage(), "table does not exist [table=ts]");
                }
                connection.commit();

                mayDrainWalQueue();

                sink.clear();
                try (
                        PreparedStatement ps = connection.prepareStatement("ts1");
                        ResultSet rs = ps.executeQuery()
                ) {
                    assertResultSet(
                            """
                                    id[INTEGER],ts[TIMESTAMP]
                                    0,2021-09-27 16:45:03.202
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testInsertSelectWithParameterBindings() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (
                    final PreparedStatement insertSrc = connection.prepareStatement(
                            "INSERT INTO source_table(id, name, value, ts) VALUES (?, ?, ?, ?)"
                    );
                    final PreparedStatement insertSelect = connection.prepareStatement(
                            "INSERT INTO target_table(id, name, value, ts) " +
                                    "SELECT id, name, value, ts FROM source_table s WHERE s.ts > ?"
                    );
                    final Statement queryStmt = connection.createStatement()
            ) {
                connection.prepareStatement("CREATE TABLE source_table (id INT, name symbol, value double, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
                connection.prepareStatement("CREATE TABLE target_table (id INT, name symbol, value double, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();

                insertSrc.setInt(1, 1);
                insertSrc.setString(2, "AAA");
                insertSrc.setDouble(3, 123.45);
                insertSrc.setString(4, "2025-04-22 12:00:00");
                insertSrc.execute();
                mayDrainWalQueue();
                insertSrc.setInt(1, 2);
                insertSrc.setString(2, "BBB");
                insertSrc.setDouble(3, 123.45);
                insertSrc.setString(4, "2025-04-23 12:00:00");
                insertSrc.execute();
                mayDrainWalQueue();
                insertSelect.setString(1, "2025-04-23 08:00:00");
                int affectedRows = insertSelect.executeUpdate();
                Assert.assertEquals(1, affectedRows);
                mayDrainWalQueue();
                try (ResultSet rs = queryStmt.executeQuery(
                        "SELECT id, name, value, FROM target_table")) {
                    String expected =
                            """
                                    id[INTEGER],name[VARCHAR],value[DOUBLE]
                                    2,BBB,123.45
                                    """;
                    assertResultSet(expected, sink, rs);
                }
            }
        });
    }

    @Test
    public void testInsertStringWithEscapedQuote() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement s = connection.createStatement()) {
                s.execute("create table t ( s string)");
                s.executeUpdate("insert into t values ('o''brien ''''')");

                sink.clear();
                try (ResultSet resultSet = s.executeQuery("select * from t")) {
                    assertResultSet("s[VARCHAR]\no'brien ''\n", sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testInsertTableDoesNotExist() throws Exception {
        skipOnWalRun(); // non-partitioned table
        // we are going to:
        // 1. create a table
        // 2. insert a record
        // 3. drop table
        // 4. attempt to insert a record (should fail)
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement statement = connection.prepareStatement("create table x (a int)");
            statement.execute();

            // exercise parameters on select statement
            PreparedStatement select = connection.prepareStatement("x where a = ?");
            execSelectWithParam(select, 9);

            PreparedStatement insert = connection.prepareStatement("insert into x (a) values (?)");
            insert.setInt(1, 1);
            insert.execute();

            PreparedStatement drop = connection.prepareStatement("drop table x");
            drop.execute();

            try {
                insert.setInt(1, 10);
                insert.execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "table does not exist [table=x]");
            }
        });
    }

    @Test
    public void testInsertTimestampAsString() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            String expectedAll = """
                    count[BIGINT]
                    10
                    """;
            connection.setAutoCommit(false);
            //
            // test methods of inserting QuestDB's DATA and TIMESTAMP values
            //
            final PreparedStatement statement = connection.prepareStatement("create table x (a int, t timestamp, t1 timestamp) timestamp(t) partition by YEAR");
            statement.execute();

            // exercise parameters on select statement
            PreparedStatement select = connection.prepareStatement("x where a = ?");
            execSelectWithParam(select, 9);


            final PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?, ?)");
            for (int i = 0; i < 10; i++) {
                insert.setInt(1, i);
                // TIMESTAMP as ISO string to designated and non-designated timestamp
                insert.setString(2, "2011-04-1" + i + "T14:40:54.998821Z");
                insert.setString(3, "2011-04-11T1" + i + ":40:54.998821Z");

                insert.execute();
                Assert.assertEquals(1, insert.getUpdateCount());
            }
            connection.commit();
            mayDrainWalQueue();

            try (ResultSet resultSet = connection.prepareStatement("select count() from x").executeQuery()) {
                sink.clear();
                assertResultSet(expectedAll, sink, resultSet);
            }

            TestUtils.assertEquals(expectedAll, sink);

            // exercise parameters on select statement
            execSelectWithParam(select, 9);
            TestUtils.assertEquals("9\n", sink);
        });
    }

    @Test
    public void testInsertTimestampWithTypeSuffix() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (ts timestamp) timestamp(ts) partition by YEAR");
            statement.execute();

            // the below timestamp formats are used by Python drivers
            final PreparedStatement insert = connection.prepareStatement("insert into x values " +
                    "('2020-06-01T00:00:02'::timestamp)," +
                    "('2020-06-01T00:00:02.000009'::timestamp)");
            insert.execute();
            mayDrainWalQueue();

            final String expected = """
                    ts[TIMESTAMP]
                    2020-06-01 00:00:02.0
                    2020-06-01 00:00:02.000009
                    """;
            try (ResultSet resultSet = connection.prepareStatement("select * from x").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testIntAndLongParametersWithoutExplicitParameterTypeButOneExplicitTextFormatHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000300073656c65637420782c202024312c2024322066726f6d206c6f6e675f73657175656e63652832293b000000
                >4200000021000000010000000200000001330000000a35303030303030303030000044000000065000450000000900000000004800000004
                <31000000043200000004540000004400037800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000024320000000000000300000413ffffffffffff0000440000001e0003000000013100000001330000000a35303030303030303030440000001e0003000000013200000001330000000a35303030303030303030430000000d53454c454354203200
                """;

        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testIntParameterWithoutExplicitParameterTypeButExplicitTextFormatHex() throws Exception {
        skipOnWalRun(); // select only
        String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000
                >420000001300000001000000010000000133000044000000065000450000000900000000004800000004
                <31000000043200000004540000002f00027800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200
                """;

        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testInvalidateWriterBetweenInserts() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table test_batch(id long,val int)");
            }
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_batch(id,val) values(?,?)")) {
                batchInsert.setLong(1, 0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();

                int[] a = batchInsert.executeBatch();
                Assert.assertEquals(3, a.length);
                Assert.assertEquals(1, a[0]);
                Assert.assertEquals(1, a[1]);
                Assert.assertEquals(1, a[2]);


                execute("create table spot1 as (select * from test_batch)");
                execute("drop table test_batch");
                execute("rename table spot1 to test_batch");
                mayDrainWalQueue();

                batchInsert.setLong(1, 0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();

                a = batchInsert.executeBatch();
                Assert.assertEquals(3, a.length);
                Assert.assertEquals(1, a[0]);
                Assert.assertEquals(1, a[1]);
                Assert.assertEquals(1, a[2]);
            }
            mayDrainWalQueue();

            StringSink sink = new StringSink();
            String expected = """
                    id[BIGINT],val[INTEGER]
                    0,1
                    1,2
                    2,3
                    0,1
                    1,2
                    2,3
                    """;
            try (
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("select * from test_batch")
            ) {
                assertResultSet(expected, sink, rs);
            }
        });
    }

    @Test
    public void testJdbcIsValid() throws Exception {
        skipOnWalRun(); // non-wal specific
        AtomicReference<Connection> connectionRef = new AtomicReference<>();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Assert.assertTrue(connection.isValid(5));
            final Connection connection2 = getConnection(mode, port, binary, 1);
            connectionRef.set(connection2);
            Assert.assertTrue(connection.isValid(5));
            Assert.assertTrue(connection2.isValid(5));
        });

        Assert.assertFalse(connectionRef.get().isValid(5));
        connectionRef.get().close();
    }

    @Test
    public void testJsonExtractBindVariable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("""
                    create table json_example as  (
                      select '{
                        "hello": "world",
                        "list": [
                            1,
                            2,
                            3
                         ],
                         "list.of.dicts": [
                             {"hello": "world"},
                             {"hello": "bob"}
                         ]
                    }'::varchar text, timestamp_sequence(0, 100000) ts from long_sequence(10)
                    ) timestamp(ts)\s
                    partition by day
                    ;
                    """).execute();
            mayDrainWalQueue();
            sink.clear();

            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, '.list[1]', 5)) from json_example;")) {
                try (ResultSet rs = ps.executeQuery()) {
                    // all rows, null = null is always true
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, '.list[1]')::varchar::int) from json_example;")) {
                try (ResultSet rs = ps.executeQuery()) {
                    // all rows, null = null is always true
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, '.list[1]')::int) from json_example;")) {
                try (ResultSet rs = ps.executeQuery()) {
                    // all rows, null = null is always true
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, ?, 5)) from json_example;")) {
                ps.setString(1, ".list[1]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }
                ps.setString(1, ".list[2]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    30
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, ?)::varchar::int) from json_example;")) {
                ps.setString(1, ".list[1]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }

                ps.setString(1, ".list[2]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    30
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select sum(json_extract(text, ?)::int) from json_example;")) {
                ps.setString(1, ".list[1]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    20
                                    """,
                            sink,
                            rs
                    );
                }

                ps.setString(1, ".list[2]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[BIGINT]
                                    30
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select json_extract(?, ?, 5) p")) {
                ps.setString(1, """
                        {
                            "hello": "world",
                            "list": [
                                1,
                                2,
                                3
                             ],
                             "list.of.dicts": [
                                 {"hello": "world"},
                                 {"hello": "bob"}
                             ]
                        }""");
                ps.setString(2, ".list[1]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    p[INTEGER]
                                    2
                                    """,
                            sink,
                            rs
                    );
                }

                // set json to null
                ps.setString(1, null);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    p[INTEGER]
                                    null
                                    """,
                            sink,
                            rs
                    );
                }
            }

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select json_extract(?, ?)::int p")) {
                ps.setString(1, """
                        {
                            "hello": "world",
                            "list": [
                                1,
                                2,
                                3
                             ],
                             "list.of.dicts": [
                                 {"hello": "world"},
                                 {"hello": "bob"}
                             ]
                        }""");
                ps.setString(2, ".list[1]");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    p[INTEGER]
                                    2
                                    """,
                            sink,
                            rs
                    );
                }

                // set json to null
                ps.setString(1, null);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    p[INTEGER]
                                    null
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testLargeBatchCairoExceptionResume() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table test_large_batch(id long, val int, ts timestamp) timestamp(ts)");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_large_batch(id,val,ts) values(?,?,?)")) {
                for (int i = 0; i < 2; i++) {
                    batchInsert.clearParameters();
                    batchInsert.setLong(1, 0L);
                    batchInsert.setInt(2, 1);
                    batchInsert.setLong(3, i);
                    batchInsert.addBatch();
                }

                try {
                    // insert out of order
                    batchInsert.setLong(1, 0L);
                    batchInsert.setInt(2, 1);
                    batchInsert.setLong(3, -100);
                    batchInsert.addBatch();
                    batchInsert.executeBatch();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "timestamp before 1970-01-01 is not allowed");
                    connection.rollback();
                }

                // try again
                for (int i = 0; i < 30; i++) {
                    batchInsert.clearParameters();
                    batchInsert.setLong(1, 0L);
                    batchInsert.setInt(2, 1);
                    batchInsert.setLong(3, i);
                    batchInsert.addBatch();
                }
                batchInsert.executeBatch();
                connection.commit();
            }

            StringSink sink = new StringSink();
            String expected = """
                    count[BIGINT]
                    30
                    """;
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select count(*) from test_large_batch");
            assertResultSet(expected, sink, rs);
        });
    }

    @Test
    public void testLargeBatchInsertMethod() throws Exception {
        skipOnWalRun(); // non-partitioned table

        assertWithPgServer(
                CONN_AWARE_EXTENDED,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table test_large_batch(id long,val int)");
                    }
                    connection.setAutoCommit(false);
                    try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_large_batch(id,val) values(?,?)")) {
                        for (int i = 0; i < 10_000; i++) {
                            batchInsert.clearParameters();
                            batchInsert.setLong(1, 0L);
                            batchInsert.setInt(2, 1);
                            batchInsert.addBatch();

                            batchInsert.clearParameters();
                            batchInsert.setLong(1, 1L);
                            batchInsert.setInt(2, 2);
                            batchInsert.addBatch();

                            batchInsert.clearParameters();
                            batchInsert.setLong(1, 2L);
                            batchInsert.setInt(2, 3);
                            batchInsert.addBatch();
                        }
                        batchInsert.executeBatch();
                        connection.commit();
                    }

                    StringSink sink = new StringSink();
                    String expected = """
                            count[BIGINT]
                            30000
                            """;
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("select count(*) from test_large_batch");
                    assertResultSet(expected, sink, rs);
                },
                () -> {
                    // Small fragmentation chunk makes this test very slow. Set the fragmentation to be near the send buffer size.
                    forceSendFragmentationChunkSize = Math.max(1024, forceSendFragmentationChunkSize);
                }
        );
    }

    @Test
    public void testLargeOutput() throws Exception {
        skipOnWalRun(); // non-partitioned table
        final String expected = """
                1[INTEGER],2[INTEGER],3[INTEGER]
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                1,2,3
                """;

        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(50)");
                    Statement statement1 = connection.createStatement();

                    StringSink sink = new StringSink();
                    for (int i = 0; i < 10; i++) {
                        sink.clear();
                        ResultSet rs = statement.executeQuery();

                        statement1.executeQuery("select 1 from long_sequence(2)");
                        assertResultSet(expected, sink, rs);
                        rs.close();
                    }
                },
                () -> sendBufferSize = 512
        );
    }

    @Test
    public void testLargeOutputHex() throws Exception {
        skipOnWalRun(); // select only
        String script = """
                >0000007300030000757365720078797a006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >500000002b0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >500000002e535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e636528353029000000420000000f00535f310000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >420000000f00535f3100000000000000450000000900000000005300000004
                <32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >420000000f00535f3100000000000000450000000900000000005300000004
                <32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >420000000f00535f3100000000000000450000000900000000005300000004
                <32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >420000000f00535f3100000000000000450000000900000000005300000004
                <32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >420000000f00535f3100000000000000450000000900000000005300000004
                <32000000044400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <4400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133
                <44000000150003000000013100000001320000000133440000001500030000000131000000013200000001334400000015000300000001310000000132000000013344000000150003000000013100000001320000000133430000000e53454c454354203530005a0000000549
                >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                >5800000004
                """;
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, new Port0PGConfiguration() {
            @Override
            public String getDefaultPassword() {
                return "oh";
            }

            @Override
            public String getDefaultUsername() {
                return "xyz";
            }

            @Override
            public int getSendBufferSize() {
                return 512;
            }
        });
    }

    @Test
    public void testLargeSelect() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("""
                                CREATE TABLE IF NOT EXISTS recorded_l1_data (
                                 HighLimitPrice double,
                                 LastAuctionImbalanceSide string,
                                 LastAuctionImbalanceVolume double,
                                 LastAuctionPrice double,
                                 LastAuctionVolume double,
                                 LastPrice double,
                                 LastTradePrice double,
                                 LastTradeQty double,
                                 LowLimitPrice double,
                                 MARKET_EURONEXT_PhaseQualifier long,
                                 MARKET_EURONEXT_StatusReason long,
                                 MARKET_EURONEXT_TradingPeriod long,
                                 MARKET_GroupTradingStatus long,
                                 MARKET_JSE_MIT_TradingStatusDetails string,
                                 MARKET_LSE_SuspendedIndicator string,
                                 MARKET_OMX_NORDIC_NoteCodes1 long,
                                 MARKET_OMX_NORDIC_NoteCodes2 long,
                                 MARKET_SWX_BookCondition long,
                                 MARKET_SWX_SecurityTradingStatus long,
                                 MARKET_SWX_TradingPhase string,
                                 MARKET_SWX_TradingSessionSubID string,
                                 MARKET_TradingStatus long,
                                 askPx double,
                                 askQty double,
                                 bidPx double,
                                 bidQty double,
                                 glid symbol,
                                 TradingStatus long,
                                 serverTimestamp long,
                                 marketTimestamp long,
                                 timestamp timestamp
                                 ) timestamp(timestamp) partition by DAY;"""
                        );
                    }

                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("""
                                insert into recorded_l1_data\s
                                 select\s
                                     rnd_double(),\s
                                     rnd_str(),\s
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_str(),
                                     rnd_str(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_str(),
                                     rnd_str(),
                                     rnd_long(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_double(),
                                     rnd_symbol('a','b','c'),
                                     rnd_long(),
                                     rnd_long(),
                                     rnd_long(),
                                     timestamp_sequence(0, 100000)
                                     from long_sequence(5000);"""
                        );
                    }

                    mayDrainWalQueue();

                    double sum = 0;
                    long count = 0;
                    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM recorded_l1_data;")) {
                        try (ResultSet rs = preparedStatement.executeQuery()) {
                            while (rs.next()) {
                                sum += rs.getDouble(1);
                                count++;
                            }
                        }
                    }
                    Assert.assertEquals(5_000, count);
                    Assert.assertEquals(2489.1431526879937, sum, 0.00000001);
                },
                () -> {
                    staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false");
                    sendBufferSize = Math.max(sendBufferSize, 2048);
                    recvBufferSize = Math.max(recvBufferSize, 5000);
                }
        );
    }

    @Test
    public void testLatestByDeferredValueFactoriesWithBindVariable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {

            try (PreparedStatement pstmt = connection.prepareStatement("CREATE TABLE IF NOT EXISTS tab (" +
                    "ts timestamp, " +
                    "sym symbol, " +
                    "isym symbol," +
                    "h string," +
                    "type char," +
                    "stat char" +
                    "), index(isym) timestamp(ts) partition by MONTH")) {
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = connection.prepareStatement("insert into tab values (0, 'S1', 'S1', 'h1', 'X', 'Y' )," +
                    " (1, 'S2', 'S2', 'h2', 'X', 'Y' ), (3, null, null, 'h3', 'X', 'Y' )")) {
                pstmt.executeUpdate();
            }

            mayDrainWalQueue();

            testExecuteWithDifferentBindVariables(
                    connection,
                    "select h, isym from " +// LatestByValueFilteredRecordCursor
                            "(select h, stat, isym from tab " +
                            "where sym = ? and type = 'X' latest on ts partition by sym " +
                            ") where stat='Y'"
            );

            testExecuteWithDifferentBindVariables(
                    connection,
                    "select h, isym from " + // LatestByValueRecordCursor
                            "(select h, stat, isym from tab " +
                            "where sym = ? latest on ts partition by sym " +
                            ") where stat='Y'"
            );

            testExecuteWithDifferentBindVariables(
                    connection,
                    "select h, isym from " + // LatestByValueIndexedFilteredRecordCursor
                            "(select h, stat, isym from tab " +
                            "where isym = ? and type = 'X' latest on ts partition by isym" +
                            ") where stat='Y'"
            );
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testLimitWithBindVariable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "create table xx as ( select x from long_sequence(1000) )")) {
                pstmt.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement("select * from xx limit ?")) {
                statement.setLong(1, 5);
                try (ResultSet rs = statement.executeQuery()) {
                    sink.clear();
                    assertResultSet("x[BIGINT]\n1\n2\n3\n4\n5\n", sink, rs);
                }
            }
        });
    }

    @Test
    public void testLocalCopyFrom() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (
                    final PreparedStatement copy = connection.prepareStatement("copy x from '/test-numeric-headers.csv' with header true");
                    final ResultSet ignore = copy.executeQuery()
            ) {
                assertEventually(() -> {
                    try (
                            final PreparedStatement select = connection.prepareStatement("select * from x");
                            final ResultSet rs = select.executeQuery()
                    ) {
                        sink.clear();
                        assertResultSet("""
                                type[VARCHAR],value[VARCHAR],active[VARCHAR],desc[VARCHAR],_1[INTEGER]
                                ABC,xy,a,brown fox jumped over the fence,10
                                CDE,bb,b,sentence 1
                                sentence 2,12
                                """, sink, rs);
                    } catch (SQLException e) {
                        throw new AssertionError(e);
                    }
                });

            }
        });
    }

    @Test
    public void testLocalCopyFromCancellation() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement copyStatement = connection.prepareStatement("copy x from '/test-numeric-headers.csv' with header true")) {
                String copyID;
                try (ResultSet rs = copyStatement.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    copyID = rs.getString("id");
                }

                try (
                        PreparedStatement cancelStatement = connection.prepareStatement("copy '" + copyID + "' cancel");
                        ResultSet rs = cancelStatement.executeQuery()
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(copyID, rs.getString(1));
                    String status = rs.getString(2);
                    Assert.assertTrue("cancelled".equals(status) || "finished".equals(status));
                }

                try (
                        PreparedStatement incorrectCancelStatement = connection.prepareStatement("copy 'ffffffffffffffff' cancel");
                        ResultSet rs = incorrectCancelStatement.executeQuery()
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("unknown", rs.getString(2));
                }

                // Pretend that the copy was cancelled and try to cancel it one more time.
                engine.getCopyImportContext().clear();

                try (
                        PreparedStatement cancelStatement = connection.prepareStatement("copy '" + copyID + "' cancel");
                        ResultSet rs = cancelStatement.executeQuery()
                ) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(copyID, rs.getString(1));
                    Assert.assertNotEquals("cancelled", rs.getString(2));
                }
            } finally {
                copyImportRequestJob.drain(0);
            }
        });
    }

    @Test
    public void testLoginBadPassword() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Properties properties = new Properties();
            properties.setProperty("user", "admin");
            properties.setProperty("password", "dunno");
            try {
                final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
                DriverManager.getConnection(url, properties);
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "invalid username/password");
            }
        });
    }

    @Test
    public void testLoginBadUsername() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            Properties properties = new Properties();
            properties.setProperty("user", "joe");
            properties.setProperty("password", "quest");
            try {
                final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
                DriverManager.getConnection(url, properties);
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "invalid username/password");
            }
        });
    }

    @Test
    public void testLoginBadUsernameHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        // this test specifically assert that we do not send
        // "ready for next query" message back to client when they fail to log in
        String script = """
                >0000000804d2162f
                <4e
                >0000003c00030000636c69656e745f656e636f64696e6700277574662d382700757365720078797a00646174616261736500706f7374677265730000
                <520000000800000003
                >70000000076f6800
                <450000002e433030303030004d696e76616c696420757365726e616d652f70617373776f726400534552524f520000
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testMalformedInitPropertyName() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                ">0000004c00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<!!",
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testMalformedInitPropertyValue() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                ">0000001e00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000\n" +
                        "<!!",
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testMatchSymbolBindVariable() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table x as (select rnd_symbol('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(50))").execute();
            mayDrainWalQueue();

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where name ~ ?")) {
                ps.setString(1, "^jjk.*");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    name[VARCHAR]
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    jjke
                                    """,
                            sink,
                            rs
                    );
                }
                ps.setString(1, "^op.*");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    name[VARCHAR]
                                    ope
                                    ope
                                    ope
                                    ope
                                    ope
                                    ope
                                    ope
                                    ope
                                    ope
                                    """,
                            sink,
                            rs
                    );
                }
            }
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testMetadata() throws Exception {
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> connection.getMetaData().getColumns("dontcare", "whatever", "x", null).close(),
                () -> recvBufferSize = 2048
        );
    }

    @Test
    public void testMicroTimestamp() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareCall("create table x(t timestamp)").execute();

            PreparedStatement statement = connection.prepareStatement("insert into x values (?)");

            final String expected = """
                    t[TIMESTAMP]
                    2019-02-11 13:48:11.123998
                    2019-02-11 13:48:11.123999
                    2019-02-11 13:48:11.124
                    2019-02-11 13:48:11.124001
                    2019-02-11 13:48:11.124002
                    2019-02-11 13:48:11.124003
                    2019-02-11 13:48:11.124004
                    2019-02-11 13:48:11.124005
                    2019-02-11 13:48:11.124006
                    2019-02-11 13:48:11.124007
                    2019-02-11 13:48:11.124008
                    2019-02-11 13:48:11.124009
                    2019-02-11 13:48:11.12401
                    2019-02-11 13:48:11.124011
                    2019-02-11 13:48:11.124012
                    2019-02-11 13:48:11.124013
                    2019-02-11 13:48:11.124014
                    2019-02-11 13:48:11.124015
                    2019-02-11 13:48:11.124016
                    2019-02-11 13:48:11.124017
                    """;

            long ts = MicrosFormatUtils.parseUTCTimestamp("2019-02-11T13:48:11.123998Z");
            for (int i = 0; i < 20; i++) {
                statement.setLong(1, ts + i);
                statement.execute();
            }
            StringSink sink = new StringSink();
            PreparedStatement sel = connection.prepareStatement("x");
            ResultSet res = sel.executeQuery();
            assertResultSet(expected, sink, res);
        });
    }

    @Test
    public void testMiscExtendedPrepared() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            try (PreparedStatement pstmt = connection.prepareStatement("begin")) {
                pstmt.execute();
            }
            try (PreparedStatement pstmt = connection.prepareStatement("set")) {
                pstmt.execute();
            }
            try (PreparedStatement pstmt = connection.prepareStatement("commit")) {
                pstmt.execute();
            }
            try (PreparedStatement pstmt = connection.prepareStatement("rollback")) {
                pstmt.execute();
            }
        });
    }

    @Test
    public void testMultiplePreparedStatements() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement ps1 = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
            PreparedStatement ps2 = connection.prepareStatement("select 4,5,6 from long_sequence(1)");
            PreparedStatement ps3 = connection.prepareStatement("select 7,8,9 from long_sequence(2)");

            final String expected = """
                    1[INTEGER],2[INTEGER],3[INTEGER]
                    1,2,3
                    """;

            StringSink sink = new StringSink();
            for (int i = 0; i < 10; i++) {
                sink.clear();
                ResultSet rs1 = ps1.executeQuery();
                ResultSet rs2 = ps2.executeQuery();
                ResultSet rs3 = ps3.executeQuery();

                assertResultSet(expected, sink, rs1);
                rs1.close();
                rs2.close();
                rs3.close();
            }

            Statement statement1 = connection.createStatement();
            for (int i = 0; i < 10; i++) {
                PreparedStatement s = connection.prepareStatement("select 2,2,2,2 from long_sequence(1)");
                s.executeQuery();
                statement1.executeQuery("select 1 from long_sequence(2)");
            }
        });
    }

    @Test
    @Ignore("expected 101, actual 100. we do not support read-your-uncommitted-writes in a transaction")
    public void testMultistatement() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server.getPort(), false, true)) {
                    connection.setAutoCommit(false);
                    int totalRows = 100;

                    CallableStatement tbl = connection.prepareCall(
                            "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
                    tbl.execute();
                    connection.commit();
                    // Queries with multiple statements should not be transformed.
                    PreparedStatement stmt = connection.prepareStatement("insert into x(a) values(100); x");
                    stmt.setFetchSize(10);

                    assertFalse(stmt.execute()); // INSERT
                    assertTrue(stmt.getMoreResults()); // SELECT
                    ResultSet rs = stmt.getResultSet();
                    int count = 0;
                    while (rs.next()) {
                        assertEquals(count, rs.getInt(1));
                        ++count;
                    }
                    assertEquals(totalRows + 1, count);
                }
            }
        });
    }

    @Test
    public void testNamedPortalForInsert() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server.getPort(), false, true)) {
                    int totalRows = 100;

                    CallableStatement tbl = connection.prepareCall(
                            "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
                    tbl.execute();
                    PreparedStatement stmt = connection.prepareStatement("insert into x(a) values(100); x");
                    stmt.setFetchSize(10);

                    assertFalse(stmt.execute()); // INSERT
                    assertTrue(stmt.getMoreResults()); // SELECT
                    ResultSet rs = stmt.getResultSet();
                    int count = 0;
                    while (rs.next()) {
                        assertEquals(count, rs.getInt(1));
                        ++count;
                    }
                    assertEquals(totalRows + 1, count);
                }
            }
        });
    }

    @Test
    public void testNamedStatementLimit() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table x as (select rnd_str() s from long_sequence(10))");
            }

            ObjList<Statement> statements = new ObjList<>();
            try {
                for (int i = 0; i < 50_000; i++) {
                    Statement stmt = connection.createStatement();
                    // capturing statements instances to prevent them from being GCed
                    // since PG JDBC does use phantom references to track statement instances
                    // and close them when they are GCed
                    statements.add(stmt);
                    try (ResultSet ignore = stmt.executeQuery("select * from x")) {
                        while (ignore.next()) {
                            // ignore
                        }
                    }
                }
                Assert.fail("Expected exception");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "too many named statements");
                TestUtils.assertContains(e.getMessage(), "[limit=10000]");
            } finally {
                Misc.freeObjListIfCloseable(statements);
            }
        });
    }

    @Test
    public void testNamedStatementWithoutParameterTypeHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >5000000032535f310073656c65637420782c24312c24322c24332066726f6d206c6f6e675f73657175656e6365283229000000420000002900535f31000003000000000000000300000001340000000331323300000004352e3433000044000000065000450000000900000000005300000004
                <31000000043200000004540000005900047800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000024320000000000000300000413ffffffffffff000024330000000000000400000413ffffffffffff0000440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                >420000002900535f31000003000000000000000300000001340000000331323300000004352e34330000450000000900000000005300000004
                <3200000004440000001f0004000000013100000001340000000331323300000004352e3433440000001f0004000000013200000001340000000331323300000004352e3433430000000d53454c4543542032005a0000000549
                >430000000953535f31005300000004
                <33000000045a0000000549
                >5800000004
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    // if the driver tries to use a cursor with autocommit on
    // it will fail because the cursor will disappear partway
    // through execution
    @Test
    public void testNoCursorWithAutoCommit() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 10;

            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();

            connection.setAutoCommit(true);
            Statement stmt = connection.createStatement();
            stmt.setFetchSize(3);
            ResultSet rs = stmt.executeQuery("x");
            int count = 0;
            while (rs.next()) {
                assertEquals(count++, rs.getInt(1));
            }
            assertEquals(totalRows, count);
        });
    }

    @Test
    public void testNoDataAndEmptyQueryResponsesHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                <310000000432000000044300000008534554005a0000000549
                >500000000800000000420000000c000000000000000044000000065000450000000900000000005300000004
                <310000000432000000046e0000000449000000045a0000000549
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testNoDataAndEmptyQueryResponsesHex_simpleTextProtocol() throws Exception {
        /*
         * go.mod:
         * module testquestpg
         *
         * go 1.19
         *
         * require github.com/lib/pq v1.10.7 // indirect
         *
         * main.go:
         *package main
         *
         * import (
         * 	"database/sql"
         * 	"fmt"
         * 	_ "github.com/lib/pq"
         * )
         *
         * const (
         * 	host     = "localhost"
         * 	port     = 8812
         * 	user     = "xyz"
         * 	password = "oh"
         * 	dbname   = "qdb"
         * )
         *
         * func main() {
         * 	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
         *
         * 	db, err := sql.Open("postgres", psqlconn)
         * 	if err != nil {
         * 		panic(err)
         *        }
         *
         * 	err = db.Ping()
         * 	if err != nil {
         * 		panic(err)
         *    }
         *
         * 	db.Close()
         * }
         *
         *
         */

        // db.Ping() in the golang program above uses the simple text protocol to execute ";" as a query
        // we need to make sure that we respond with an empty query response
        String script = """
                >0000005c00030000636c69656e745f656e636f64696e6700555446380065787472615f666c6f61745f646967697473003200646174657374796c650049534f2c204d445900757365720078797a006461746162617365007164620000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >51000000063b00
                <49000000045a0000000549
                >5800000004""";

        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testNullTypeSerialization() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    sink.clear();
                    try (PreparedStatement ps = connection.prepareStatement("create table test as (select x from long_sequence(10))")) {
                        ps.execute();
                    }
                    try (
                            PreparedStatement ps = connection.prepareStatement("""
                                    SELECT * FROM (
                                      SELECT\s
                                        n.nspname
                                        ,c.relname
                                        ,a.attname
                                        ,a.atttypid
                                        ,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull
                                        ,a.atttypmod
                                        ,a.attlen
                                        ,t.typtypmod
                                        ,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum
                                        , nullif(a.attidentity, '') as attidentity
                                        ,null as attgenerated
                                        ,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc
                                        ,dsc.description
                                        ,t.typbasetype
                                        ,t.typtype \s
                                      FROM pg_catalog.pg_namespace n
                                      JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
                                      JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
                                      JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                                      LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
                                      LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
                                      LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
                                      LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
                                      WHERE\s
                                        c.relkind in ('r','p','v','f','m')
                                        and a.attnum > 0\s
                                        AND NOT a.attisdropped
                                        AND c.relname LIKE E'test'
                                      ) c WHERE true
                                      ORDER BY nspname,c.relname,attnum;
                                    """);
                            ResultSet rs = ps.executeQuery()
                    ) {
                        assertResultSet(
                                """
                                        nspname[VARCHAR],relname[VARCHAR],attname[VARCHAR],atttypid[INTEGER],attnotnull[BIT],atttypmod[INTEGER],attlen[SMALLINT],typtypmod[INTEGER],attnum[BIGINT],attidentity[VARCHAR],attgenerated[VARCHAR],adsrc[VARCHAR],description[VARCHAR],typbasetype[INTEGER],typtype[CHAR]
                                        public,test,x,20,false,-1,8,0,1,null,null,null,null,0,b
                                        """,
                                sink,
                                rs
                        );
                    }
                },
                () -> recvBufferSize = 2048
        );
    }

    @Test
    public void testPHPSelectHex() throws Exception {
        //         PHP client script to reproduce
        //        $dbName = 'qdb';
        //        $hostname = '127.0.0.1';
        //        $password = 'quest';
        //        $port = 8812;
        //        $username = 'admin';
        //
        //        $pdo = new PDO("pgsql:host=$hostname;dbname=$dbName;port=$port;options='--client_encoding=UTF8'", $username, $password);
        //        $stmt = $pdo->prepare("SELECT * FROM x00 limit 10");
        //        try {
        //            $stmt->execute(array());
        //            $res = $stmt->fetchAll();
        //            print_r($res);
        //        } catch(PDOException $e) {
        //            echo $e;
        //        }
        skipOnWalRun(); // non-partitioned table
        String scriptx00 = """
                >0000000804d2162f
                <4e
                >0000004000030000757365720061646d696e00646174616261736500716462006f7074696f6e73002d2d636c69656e745f656e636f64696e673d555446380000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >500000003370646f5f73746d745f30303030303030310053454c454354202a2046524f4d20783030206c696d69742031300000005300000004
                <31000000045a0000000549
                >420000001f0070646f5f73746d745f303030303030303100000000000001000044000000065000450000000900000000005300000004
                <3200000004540000008a00066900000000000001000000170004ffffffff000073796d0000000000000200000413ffffffffffff0000616d7400000000000003000002bd0008ffffffff000074696d657374616d70000000000000040000045a0008ffffffff0000630000000000000500000413ffffffffffff00006400000000000006000002bd0008ffffffff0000440000005700060000000131000000046d7366740000000632322e3436330000001a323031382d30312d30312030303a31323a30302e3030303030300000000343444500000011302e323939313939303435393631383435440000005500060000000132000000046d7366740000000636352e3038360000001a323031382d30312d30312030303a32343a30302e303030303030ffffffff00000012302e39383536323930383435383734323633440000005800060000000133000000046d7366740000000635302e3933380000001a323031382d30312d30312030303a33363a30302e3030303030300000000358595a00000012302e37363131303239353134393935373434440000006400060000000134000000046d7366740000001235352e3939323030303030303030303030340000001a323031382d30312d30312030303a34383a30302e3030303030300000000358595a00000012302e32333930353239303130383436353235440000005a0006000000013500000005676f6f676c0000000636372e3738360000001a323031382d30312d30312030313a30303a30302e3030303030300000000358595a00000013302e333835333939343738363532343439393444000000650006000000013600000005676f6f676c0000001233332e3630383030303030303030303030340000001a323031382d30312d30312030313a31323a30302e3030303030300000000343444500000012302e3736373536373330373037393631303444000000590006000000013700000005676f6f676c0000000636322e3137330000001a323031382d30312d30312030313a32343a30302e3030303030300000000343444500000012302e3633383136303735333131373835313344000000470006000000013800000005676f6f676c0000000635372e3933350000001a323031382d30312d30312030313a33363a30302e3030303030300000000358595affffffff440000004300060000000139000000046d7366740000000636372e3631390000001a323031382d30312d30312030313a34383a30302e303030303030ffffffffffffffff4400000057000600000002313000000005676f6f676c0000000634322e3238310000001a323031382d30312d30312030323a30303a30302e303030303030ffffffff00000012302e37363634323536373533353936313338430000000e53454c454354203130005a0000000549
                >51000000214445414c4c4f434154452070646f5f73746d745f303030303030303100
                <430000000f4445414c4c4f43415445005a0000000549
                >5800000004
                """;

        assertMemoryLeak(() -> {
            execute(
                    "create table x00 as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0)*100, 3) amt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                            " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                            " rnd_double(2) d" +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp)"
            );
            try (
                    final PGServer server = createPGServer(new Port0PGConfiguration(), true);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                NetUtils.playScript(NetworkFacadeImpl.INSTANCE, scriptx00, "127.0.0.1", server.getPort());
            }
        });
    }

    //checks that function parser error doesn't persist and affect later queries issued through the same connection
    @Test
    public void testParseErrorDoesNotCorruptConnection() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps1 = connection.prepareStatement("select * from " +
                    "(select cast(x as timestamp) ts, '0x05cb69971d94a00000192178ef80f0' as id, x from long_sequence(10) ) " +
                    "where ts between '2022-03-20' " +
                    "AND id <> '0x05ab6d9fabdabb00066a5db735d17a' " +
                    "AND id <> '0x05aba84839b9c7000006765675e630' " +
                    "AND id <> '0x05abc58d80ba1f000001ed05351873'")) {
                ps1.executeQuery();
                Assert.fail("PSQLException should be thrown");
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "there is no matching operator `!=` with the argument types: BOOLEAN != STRING");
            }

            try (PreparedStatement s = connection.prepareStatement("select 2 a,2 b from long_sequence(1) where x > 0 and x < 10")) {
                StringSink sink = new StringSink();
                ResultSet result = s.executeQuery();
                assertResultSet("a[INTEGER],b[INTEGER]\n2,2\n", sink, result);
            }
        });
    }

    @Test
    //checks that function parser error doesn't persist and affect later queries issued through the same connection
    public void testParseErrorDoesntCorruptConnection() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {

            try (PreparedStatement ps1 = connection.prepareStatement("select * from " +
                    "(select cast(x as timestamp) ts, '0x05cb69971d94a00000192178ef80f0' as id, x from long_sequence(10) ) " +
                    "where ts between '2022-03-20' " +
                    "AND id <> '0x05ab6d9fabdabb00066a5db735d17a' " +
                    "AND id <> '0x05aba84839b9c7000006765675e630' " +
                    "AND id <> '0x05abc58d80ba1f000001ed05351873'")) {
                ps1.executeQuery();
                Assert.fail("PSQLException should be thrown");
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "there is no matching operator `!=` with the argument types: BOOLEAN != STRING");
            }

            try (PreparedStatement s = connection.prepareStatement("select 2 a,2 b from long_sequence(1) where x > 0 and x < 10")) {
                StringSink sink = new StringSink();
                ResultSet result = s.executeQuery();
                assertResultSet("a[INTEGER],b[INTEGER]\n2,2\n", sink, result);
            }
        });
    }

    @Test
    public void testPlanWithIndexAndBindingVariables() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            // columns:
            // hc = high-cardinality column - must be preferred for index-scans
            // lc = low-cardinality column
            execute("CREATE TABLE 'idx' ( " +
                    "hc SYMBOL CAPACITY 256 INDEX," +
                    "lc SYMBOL CAPACITY 256 INDEX," +
                    "ts TIMESTAMP " +
                    ") timestamp(ts) PARTITION BY DAY BYPASS WAL");
            execute("insert into idx select concat('hc', x%10) as hc, concat('lc', x%2) as lc, x::timestamp from long_sequence(100);");

            try (PreparedStatement ps = connection.prepareStatement("""
                    explain
                    select * from idx
                    where hc in (?, ?) and lc in (?, ?)
                    and ts >= '1970-01-01T00:00:00.000077Z' and ts <= '1970-01-01T00:00:00.279828Z'
                    order by ts asc;
                    """)) {
                ps.setString(1, "hc_1");
                ps.setString(2, "hc_2");
                ps.setString(3, "lc_1");
                ps.setString(4, "lc_2");

                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet("""
                            QUERY PLAN[VARCHAR]
                            FilterOnValues
                                Table-order scan
                                    Index forward scan on: hc deferred: true
                                      symbolFilter: hc=$0::string
                                      filter: lc in [$2::string,$3::string]
                                    Index forward scan on: hc deferred: true
                                      symbolFilter: hc=$1::string
                                      filter: lc in [$2::string,$3::string]
                                Interval forward scan on: idx
                                  intervals: [("1970-01-01T00:00:00.000077Z","1970-01-01T00:00:00.279828Z")]
                            """, new StringSink(), rs);
                }
            }

            // swap the order of the predicates - the plan must still do index-scan on the high-cardinality column
            try (PreparedStatement ps = connection.prepareStatement("""
                    explain
                    select * from idx
                    where lc in (?, ?) and hc in (?, ?)
                    and ts >= '1970-01-01T00:00:00.000077Z' and ts <= '1970-01-01T00:00:00.279828Z'
                    order by ts asc;
                    """)) {
                ps.setString(1, "lc_1");
                ps.setString(2, "lc_2");
                ps.setString(3, "hc_1");
                ps.setString(4, "hc_2");


                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet("QUERY PLAN[VARCHAR]\n" +
                            "FilterOnValues\n" +
                            "    Table-order scan\n" +
                            "        Index forward scan on: hc deferred: true\n" + // still scanning on the high-cardinality column
                            "          symbolFilter: hc=$2::string\n" +
                            "          filter: lc in [$0::string,$1::string]\n" +
                            "        Index forward scan on: hc deferred: true\n" +
                            "          symbolFilter: hc=$3::string\n" +
                            "          filter: lc in [$0::string,$1::string]\n" +
                            "    Interval forward scan on: idx\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000077Z\",\"1970-01-01T00:00:00.279828Z\")]\n", new StringSink(), rs);
                }
            }
        });
    }

    @Test
    public void testPlanWithIndexAndSingleBindingVariable() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            execute(
                    "CREATE TABLE x ( " +
                            "hc SYMBOL INDEX," +
                            "lc SYMBOL INDEX," +
                            "ts TIMESTAMP " +
                            ") timestamp(ts) PARTITION BY DAY BYPASS WAL"
            );
            execute("insert into x select concat('sym', x%20), concat('sym', x%10), x::timestamp from long_sequence(100);");

            try (PreparedStatement ps = connection.prepareStatement(
                    """
                            explain
                            select * from x
                            where hc in (?) and lc in ('sym0')
                            and ts >= '1970-01-01T00:00:00.000077Z' and ts <= '1970-01-01T00:00:00.279828Z'
                            order by ts asc;
                            """
            )) {
                ps.setString(1, "sym0");
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(
                            "QUERY PLAN[VARCHAR]\n" +
                                    "PageFrame\n" +
                                    "    Index forward scan on: hc deferred: true\n" + // verify we are scanning on the high-cardinality column
                                    "      symbolFilter: hc=$0::string\n" +
                                    "      filter: lc in [sym0]\n" +
                                    "    Interval forward scan on: x\n" +
                                    "      intervals: [(\"1970-01-01T00:00:00.000077Z\",\"1970-01-01T00:00:00.279828Z\")]\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testPrepareInsertAsSelect() throws Exception {
        skipOnWalRun(); // the test uses non-partitioned tables

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            CallableStatement stmt = connection.prepareCall("drop table if exists mining_event;");
            stmt.execute();

            stmt = connection.prepareCall("drop table if exists mining_event_5m;");
            stmt.execute();

            stmt = connection.prepareCall(
                    """
                            create table mining_event (
                                        ts timestamp,
                                        user symbol capacity 12800 CACHE INDEX,
                                        worker symbol capacity 1280000 NOCACHE,
                                        shares long
                                    ) timestamp(ts) partition by day bypass wal"""
            );
            stmt.execute();
            stmt = connection.prepareCall(
                    """
                            create table mining_event_5m (
                                        ts timestamp,
                                        user symbol capacity 12800 CACHE INDEX,
                                        worker symbol capacity 1280000 NOCACHE,
                                        shares long
                                    )
                                    timestamp(ts)
                                    partition by day bypass wal"""
            );
            stmt.execute();
            try (PreparedStatement insert = connection.prepareStatement(
                    "insert into mining_event (ts, user, worker, shares) values ('2024-01-12 00:00:03', 'user_1', 'user_1.w1', 10);")) {
                insert.executeUpdate();
            }

            String insertAsSelect = """
                    insert into mining_event_5m
                            select ts, user, worker, sum(shares) as shares
                            from mining_event
                            where ts >= '2024-01-12 00:00:00' sample by 5m fill (none) align to calendar;""";
            try (PreparedStatement statement = connection.prepareStatement(insertAsSelect)) {
                statement.execute();
            }

            try (PreparedStatement statement = connection.prepareStatement("select count() from mining_event")) {
                try (ResultSet rs = statement.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getLong(1));
                }
            }

            try (PreparedStatement statement = connection.prepareStatement("select count() from mining_event_5m")) {
                try (ResultSet rs = statement.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getLong(1));
                }
            }
        });
    }

    @Test
    public void testPreparedStatement() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
            Statement statement1 = connection.createStatement();

            final String expected = """
                    1[INTEGER],2[INTEGER],3[INTEGER]
                    1,2,3
                    """;

            StringSink sink = new StringSink();
            for (int i = 0; i < 10; i++) {
                sink.clear();
                ResultSet rs = statement.executeQuery();

                statement1.executeQuery("select 1 from long_sequence(2)");
                assertResultSet(expected, sink, rs);
                rs.close();
            }
        });
    }

    @Test
    public void testPreparedStatementHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >500000002a0073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >500000002d535f310073656c65637420312c322c332066726f6d206c6f6e675f73657175656e6365283129000000420000000f00535f310000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000004200033100000000000001000000170004ffffffff00003200000000000002000000170004ffffffff00003300000000000003000000170004ffffffff000044000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >420000000f00535f3100000000000000450000000900000000005300000004
                        <320000000444000000150003000000013100000001320000000133430000000d53454c4543542031005a0000000549
                        >50000000260073656c65637420312066726f6d206c6f6e675f73657175656e6365283229000000420000000c000000000000000044000000065000450000000900000000005300000004
                        <31000000043200000004540000001a00013100000000000001000000170004ffffffff0000440000000b00010000000131440000000b00010000000131430000000d53454c4543542032005a0000000549
                        >5800000004
                        """,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testPreparedStatementInsertSelectNullDesignatedColumn() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            {
                try (
                        final Statement statement = connection.createStatement();
                        final PreparedStatement insert = connection.prepareStatement("insert into tab(ts, value) values(?, ?)")
                ) {
                    statement.execute("create table tab(ts timestamp, value double) timestamp(ts) partition by MONTH");
                    // Null is not allowed
                    insert.setNull(1, Types.NULL);
                    insert.setNull(2, Types.NULL);
                    try {
                        insert.executeUpdate();
                        fail("inserting NULL for designated timestamp should fail");
                    } catch (PSQLException expected) {
                        TestUtils.assertContains(expected.getMessage(), "ERROR: designated timestamp column cannot be NULL");
                        final ServerErrorMessage serverErrorMessage = expected.getServerErrorMessage();
                        Assert.assertNotNull(serverErrorMessage);
                        if (mode == Mode.SIMPLE) {
                            // in simple mode variables are substituted with "values ((NULL))", note the extra bracket
                            // the error position shifts by 1
                            Assert.assertEquals(36, serverErrorMessage.getPosition());
                        } else {
                            Assert.assertEquals(35, serverErrorMessage.getPosition());
                        }
                    }
                    // Insert a dud
                    insert.setString(1, "1970-01-01 00:11:22.334455");
                    insert.setNull(2, Types.NULL);
                    insert.executeUpdate();

                    mayDrainWalQueue();

                    sink.clear();
                    try (ResultSet rs = statement.executeQuery("select null, ts, value from tab where value is null")) {
                        String expected = """
                                null[VARCHAR],ts[TIMESTAMP],value[DOUBLE]
                                null,1970-01-01 00:11:22.334455,null
                                """;
                        assertResultSet(expected, sink, rs);
                    }
                    statement.execute("drop table tab");
                    mayDrainWalQueue();
                }
            }
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementInsertSelectNullNoDesignatedColumn() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final Statement statement = connection.createStatement()) {
                statement.execute("create table tab(ts timestamp, value double)");
                try (PreparedStatement insert = connection.prepareStatement("insert into tab(ts, value) values(?, ?)")) {
                    insert.setNull(1, Types.NULL);
                    insert.setNull(2, Types.NULL);
                    insert.executeUpdate();
                }
                try (ResultSet rs = statement.executeQuery("select null, ts, value from tab where value = null")) {
                    StringSink sink = new StringSink();
                    String expected = """
                            null[VARCHAR],ts[TIMESTAMP],value[DOUBLE]
                            null,null,null
                            """;
                    assertResultSet(expected, sink, rs);
                }
                statement.execute("drop table tab");
            }
        });
    }

    @Test
    public void testPreparedStatementParamBadByte() throws Exception {
        skipOnWalRun(); // non-partitioned table
        final String script =
                """
                        >0000006b00030000757365720061646d696e006461746162617365006e6162755f61707000636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e6500474d540065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5000000022005345542065787472615f666c6f61745f646967697473203d2033000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >500000003700534554206170706c69636174696f6e5f6e616d65203d2027506f737467726553514c204a4442432044726976657227000000420000000c0000000000000000450000000900000000015300000004
                        <310000000432000000044300000008534554005a0000000549
                        >50000000cd0073656c65637420782c24312c24322c24332c24342c24352c24362c24372c24382c24392c2431302c2431312c2431322c2431332c2431342c2431352c2431362c2431372c2431382c2431392c2432302c2432312c2432322066726f6d206c6f6e675f73657175656e63652835290000160000001700000014000002bd000002bd0000001500000010000004130000041300000000000000000000001700000014000002bc000002bd000000150000001000000413000004130000043a000000000000045a000004a04200000123000000160000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001600000001340000000331323300000004352e343300000007302e353637383900000002993100000004545255450000000568656c6c6f0000001dd0b3d180d183d0bfd0bfd0b020d182d183d180d0b8d181d182d0bed0b20000000e313937302d30312d3031202b30300000001a313937302d30382d32302031313a33333a32302e3033332b3030ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a313937302d30312d30312030303a30353a30302e3031312b30300000001a313937302d30312d30312030303a30383a32302e3032332b3030000044000000065000450000000900000000005300000004
                        <4500000050433030303030004d696e76616c6964205554463820656e636f64696e6720666f7220737472696e672076616c7565205b7661726961626c65496e6465783d345d00534552524f5200503100005a0000000549
                        """;
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, new Port0PGConfiguration());
    }

    @Test
    public void testPreparedStatementParams() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            final PGConfiguration conf = new Port0PGConfiguration() {
                @Override
                public int getWorkerCount() {
                    return 4;
                }
            };

            final WorkerPool workerPool = new TestWorkerPool(4, conf.getMetrics());
            try (final PGServer server = createPGWireServer(
                    conf,
                    engine,
                    workerPool
            )) {
                workerPool.start(LOG);
                try {
                    Assert.assertNotNull(server);
                    Properties properties = new Properties();
                    properties.setProperty("user", "admin");
                    properties.setProperty("password", "quest");
                    properties.setProperty("sslmode", "disable");
                    properties.setProperty("binaryTransfer", "true");
                    TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
                    final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", server.getPort());
                    final Connection connection = DriverManager.getConnection(url, properties);
                    PreparedStatement statement = connection.prepareStatement(
                            "select x,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? from long_sequence(5)");
                    statement.setInt(1, 4);
                    statement.setLong(2, 123L);
                    statement.setFloat(3, 5.43f);
                    statement.setDouble(4, 0.56789);
                    statement.setByte(5, (byte) 91);
                    statement.setBoolean(6, true);
                    statement.setString(7, "hello");
                    // this is to test UTF8 behaviour
                    statement.setString(8, "группа туристов");
                    statement.setDate(9, new Date(100L));
                    statement.setTimestamp(10, new Timestamp(20000000033L));

                    // nulls
                    statement.setNull(11, Types.INTEGER);
                    statement.setNull(12, Types.BIGINT);
                    statement.setNull(13, Types.REAL);
                    statement.setNull(14, Types.DOUBLE);
                    // SMALL_INT is not nullable and will become 0
                    statement.setNull(15, Types.SMALLINT);
                    statement.setNull(16, Types.BOOLEAN);
                    statement.setNull(17, Types.VARCHAR);
                    statement.setString(18, null);
                    statement.setNull(19, Types.DATE);

                    // when someone uses PostgreSQL's type extensions, which alter driver behaviour
                    // we should handle this gracefully
                    statement.setTimestamp(20, new PGTimestamp(300011));
                    statement.setTimestamp(21, new PGTimestamp(500023, new GregorianCalendar()));
                    statement.setTimestamp(22, null);

                    // modern uses metadata from the PARSE message
                    final String expected = """
                            x[BIGINT],$1[INTEGER],$2[BIGINT],$3[REAL],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[INTEGER],$12[BIGINT],$13[REAL],$14[DOUBLE],$15[SMALLINT],$16[BIT],$17[VARCHAR],$18[VARCHAR],$19[TIMESTAMP],$20[TIMESTAMP],$21[TIMESTAMP],$22[VARCHAR]
                            1,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023,null
                            2,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023,null
                            3,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023,null
                            4,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023,null
                            5,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023,null
                            """;

                    StringSink sink = new StringSink();
                    for (int i = 0; i < 10000; i++) {
                        sink.clear();
                        ResultSet rs = statement.executeQuery();
                        assertResultSet(expected, sink, rs);
                        rs.close();
                    }
                    connection.close();
                } finally {
                    workerPool.halt();
                }
            }
        });
    }

    @Test
    public void testPreparedStatementRenameTableReexecution() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareStatement("CREATE TABLE ts as" +
                    " (select x, timestamp_sequence('2022-02-24T04', 2) ts from long_sequence(2) )" +
                    " TIMESTAMP(ts) PARTITION BY MONTH").execute();
            drainWalAndAssertTableExists("ts");

            PreparedStatement tsToTs2 = connection.prepareStatement("rename table ts to ts2");
            tsToTs2.execute();
            drainWalAndAssertTableExists("ts2");

            PreparedStatement ts2ToTs3 = connection.prepareStatement("rename table ts2 to ts3");
            ts2ToTs3.execute();
            drainWalAndAssertTableExists("ts3");

            PreparedStatement ts3ToTs = connection.prepareStatement("rename table ts3 to ts");
            ts3ToTs.execute();
            drainWalAndAssertTableExists("ts");

            // now re-execute already parsed statements again
            tsToTs2.execute();
            drainWalAndAssertTableExists("ts2");

            ts2ToTs3.execute();
            drainWalAndAssertTableExists("ts3");

            ts3ToTs.execute();
            drainWalAndAssertTableExists("ts");
        });
    }

    @Test
    public void testPreparedStatementSelectNull() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("select ? from long_sequence(1)")) {
                sink.clear();
                statement.setNull(1, Types.NULL);
                // we are not naming the field explicitly, depending on the protocol (simple or extended)
                // the SQL text to the server will change. Impacting the column name
                if (mode == Mode.SIMPLE) {
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet("""
                                NULL[VARCHAR]
                                null
                                """, sink, rs);
                    }
                    statement.setNull(1, Types.VARCHAR);
                    try (ResultSet rs = statement.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                NULL[VARCHAR]
                                null
                                """, sink, rs);
                    }
                } else {
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet("""
                                $1[VARCHAR]
                                null
                                """, sink, rs);
                    }
                    statement.setNull(1, Types.VARCHAR);
                    try (ResultSet rs = statement.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                $1[VARCHAR]
                                null
                                """, sink, rs);
                    }
                }
            }
        });
    }

    @Test
    public void testPreparedStatementTextParams() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    PreparedStatement statement = connection.prepareStatement("select x, ? as \"$1\",? as \"$2\",? as \"$3\",? as \"$4\"," +
                            "? as \"$5\",? as \"$6\",? as \"$7\",? as \"$8\",? as \"$9\",? as \"$10\",? as \"$11\",? as \"$12\",? as \"$13\"," +
                            "? as \"$14\",? as \"$15\",? as \"$16\",? as \"$17\",? as \"$18\",? as \"$19\",? as \"$20\",? as \"$21\" from long_sequence(5)");
                    statement.setInt(1, 4);
                    statement.setLong(2, 123L);
                    statement.setFloat(3, 5.43f);
                    statement.setDouble(4, 0.56789);
                    statement.setByte(5, (byte) 91);
                    statement.setBoolean(6, true);
                    statement.setString(7, "hello");
                    // this is to test UTF8 behaviour
                    statement.setString(8, "группа туристов");
                    statement.setDate(9, new Date(100L));
                    statement.setTimestamp(10, new Timestamp(20000000033L));

                    // nulls
                    statement.setNull(11, Types.INTEGER);
                    statement.setNull(12, Types.BIGINT);
                    statement.setNull(13, Types.REAL);
                    statement.setNull(14, Types.DOUBLE);
                    statement.setNull(15, Types.SMALLINT);
                    statement.setNull(16, Types.BOOLEAN);
                    statement.setNull(17, Types.VARCHAR);
                    statement.setString(18, null);
                    statement.setNull(19, Types.DATE);
                    // statement.setNull(20, Types.TIMESTAMP);

                    // when someone uses PostgreSQL's type extensions, which alter driver behaviour
                    // we should handle this gracefully

                    statement.setTimestamp(20, new PGTimestamp(300011));
                    statement.setTimestamp(21, new PGTimestamp(500023, new GregorianCalendar()));

                    final String expected;
                    // JDBC driver is being evil: even though we defined parameter #3 explicitly as "float",
                    // the driver will correctly send OID 700 on the binary protocol for this paramter, but
                    // in "text" mode, it will send OID 701 making server believe it is "double". This is
                    // one of many JDBC driver bugs, that is liable to change in the future driver versions
                    if (binary) {
                        if (mode == Mode.SIMPLE) {
                            // simple mode
                            expected = """
                                    x[BIGINT],$1[INTEGER],$2[BIGINT],$3[REAL],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[VARCHAR],$12[VARCHAR],$13[VARCHAR],$14[VARCHAR],$15[VARCHAR],$16[VARCHAR],$17[VARCHAR],$18[VARCHAR],$19[VARCHAR],$20[TIMESTAMP],$21[TIMESTAMP]
                                    1,4,123,5.430,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    2,4,123,5.430,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    3,4,123,5.430,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    4,4,123,5.430,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    5,4,123,5.430,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    """;
                        } else {
                            expected = """
                                    x[BIGINT],$1[INTEGER],$2[BIGINT],$3[REAL],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[INTEGER],$12[BIGINT],$13[REAL],$14[DOUBLE],$15[SMALLINT],$16[BIT],$17[VARCHAR],$18[VARCHAR],$19[TIMESTAMP],$20[TIMESTAMP],$21[TIMESTAMP]
                                    1,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    2,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    3,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    4,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    5,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    """;
                        }
                    } else {
                        if (mode == Mode.SIMPLE) {
                            expected = """
                                    x[BIGINT],$1[INTEGER],$2[BIGINT],$3[DOUBLE],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[VARCHAR],$12[VARCHAR],$13[VARCHAR],$14[VARCHAR],$15[VARCHAR],$16[VARCHAR],$17[VARCHAR],$18[VARCHAR],$19[VARCHAR],$20[TIMESTAMP],$21[TIMESTAMP]
                                    1,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    2,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    3,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    4,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    5,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,null,null,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    """;
                        } else {
                            expected = """
                                    x[BIGINT],$1[INTEGER],$2[BIGINT],$3[DOUBLE],$4[DOUBLE],$5[SMALLINT],$6[BIT],$7[VARCHAR],$8[VARCHAR],$9[VARCHAR],$10[VARCHAR],$11[INTEGER],$12[BIGINT],$13[REAL],$14[DOUBLE],$15[SMALLINT],$16[BIT],$17[VARCHAR],$18[VARCHAR],$19[TIMESTAMP],$20[TIMESTAMP],$21[TIMESTAMP]
                                    1,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    2,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    3,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    4,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    5,4,123,5.43,0.56789,91,true,hello,группа туристов,1970-01-01 +00,1970-08-20 11:33:20.033+00,null,null,null,null,0,false,null,null,null,1970-01-01 00:05:00.011,1970-01-01 00:08:20.023
                                    """;
                        }
                    }

                    sink.clear();
                    try (ResultSet rs = statement.executeQuery()) {
                        assertResultSet(expected, sink, rs);
                    }
                },
                () -> {
                    sendBufferSize = 1024;
                    recvBufferSize = 1024;
                }
        );
    }

    @Test
    public void testPreparedStatementWithBindVariablesOnDifferentConnection() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(createDatesTblStmt)) {
                statement.execute();
            }
            mayDrainWalQueue();
            queryTimestampsInRange(connection);

            try (final Connection connection2 = getConnection(port, false, binary)) {
                queryTimestampsInRange(connection2);

                if (isEnabledForWalRun()) {
                    try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                        statement.execute();
                    }
                }
            }
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementWithBindVariablesSetWrongOnDifferentConnection() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(createDatesTblStmt)) {
                statement.execute();
            }
            mayDrainWalQueue();
            queryTimestampsInRange(connection);

            boolean caught = false;
            try (final Connection connection2 = getConnection(mode, port, binary)) {
                try (PreparedStatement statement = connection2.prepareStatement(
                        "select ts FROM xts WHERE ts <= dateadd('d', -1, ?) and ts >= dateadd('d', -2, ?)"
                )) {
                    sink.clear();
                    statement.setString(1, "2024-01-05");
                    statement.setString(2, "b2222");
                    statement.executeQuery();
                } catch (PSQLException ex) {
                    caught = true;
                    TestUtils.assertContains(ex.getMessage(), "ERROR: inconvertible value: `b2222` [" + stringTypeName + " -> TIMESTAMP_NS");
                }
            }

            if (isEnabledForWalRun()) {
                try (final Connection connection2 = getConnection(mode, port, binary);
                     PreparedStatement statement = connection2.prepareStatement("drop table xts")) {
                    statement.execute();
                }
            }
            Assert.assertTrue("Exception is not thrown", caught);
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementWithBindVariablesSetWrongOnSameConnection() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(createDatesTblStmt)) {
                statement.execute();
            }
            mayDrainWalQueue();
            queryTimestampsInRange(connection);

            boolean caught = false;
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "select ts FROM xts WHERE ts <= dateadd('d', -1, ?) and ts >= dateadd('d', -2, ?)"
                    )
            ) {
                sink.clear();
                statement.setString(1, "2024-01-05");
                statement.setString(2, "b2222");
                statement.executeQuery();
            } catch (PSQLException ex) {
                caught = true;
                TestUtils.assertContains(ex.getMessage(), "ERROR: inconvertible value: `b2222` [" + stringTypeName + " -> TIMESTAMP_NS]");
            }

            if (isEnabledForWalRun()) {
                try (
                        PreparedStatement statement = connection.prepareStatement("drop table xts")
                ) {
                    statement.execute();
                }
            }
            Assert.assertTrue("Exception is not thrown", caught);
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementWithBindVariablesTimestampRange() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(createDatesTblStmt)) {
                statement.execute();
            }
            mayDrainWalQueue();
            queryTimestampsInRange(connection);
            if (isEnabledForWalRun()) {
                try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                    statement.execute();
                }
            }
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementWithNowFunction() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "create table xts (ts timestamp) timestamp(ts) partition by YEAR")) {
                statement.execute();
            }

            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO xts VALUES(now())")) {
                for (long micros = 0; micros < 200 * Micros.HOUR_MICROS; micros += Micros.HOUR_MICROS) {
                    setCurrentMicros(micros);
                    statement.execute();
                }
            }

            mayDrainWalQueue();
            queryTimestampsInRange(connection);

            try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                statement.execute();
            }
        }, () -> staticOverrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_ENABLED, "false"));
    }

    @Test
    public void testPreparedStatementWithSystimestampFunction() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "create table xts (ts timestamp) timestamp(ts)")) {
                statement.execute();
            }

            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO xts VALUES(systimestamp())")) {
                for (long micros = 0; micros < 200 * Micros.HOUR_MICROS; micros += Micros.HOUR_MICROS) {
                    setCurrentMicros(micros);
                    statement.execute();
                }
            }

            queryTimestampsInRange(connection);

            try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                statement.execute();
            }
        });
    }

    @Test
    public void testPythonInsertDateSelectHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000000804d2162f
                <4e
                >0000002100030000757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >510000001b53455420646174657374796c6520544f202749534f2700
                <4300000008534554005a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >5100000067435245415445205441424c45204946204e4f542045584953545320747261646573202874732054494d455354414d502c206461746520444154452c206e616d6520535452494e472c2076616c756520494e54292074696d657374616d70287473293b00
                <43000000074f4b005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323230303839273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2030293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323331303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2031293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323332303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2032293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323332303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2033293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323333303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2034293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323333303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2035293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323334303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2036293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323334303238273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2037293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323335303738273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2038293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000930a2020202020202020494e5345525420494e544f207472616465730a202020202020202056414c554553202827323032312d30312d32365431333a34333a34302e323335303738273a3a74696d657374616d702c2027323032312d30312d3236273a3a646174652c2027707974686f6e20707265702073746174656d656e74272c2039293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >510000000b434f4d4d495400
                <430000000b434f4d4d4954005a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >510000001a53454c454354202a2046524f4d207472616465733b00
                <540000006100047473000000000000010000045a0008ffffffff000064617465000000000000020000045a0008ffffffff00006e616d650000000000000300000413ffffffffffff000076616c756500000000000004000000170004ffffffff0000440000005d00040000001a323032312d30312d32362031333a34333a34302e32323030383900000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000130440000005d00040000001a323032312d30312d32362031333a34333a34302e32333130323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000131440000005d00040000001a323032312d30312d32362031333a34333a34302e32333230323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000132440000005d00040000001a323032312d30312d32362031333a34333a34302e32333230323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000133440000005d00040000001a323032312d30312d32362031333a34333a34302e32333330323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000134440000005d00040000001a323032312d30312d32362031333a34333a34302e32333330323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000135440000005d00040000001a323032312d30312d32362031333a34333a34302e32333430323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000136440000005d00040000001a323032312d30312d32362031333a34333a34302e32333430323800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000137440000005d00040000001a323032312d30312d32362031333a34333a34302e32333530373800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000138440000005d00040000001a323032312d30312d32362031333a34333a34302e32333530373800000017323032312d30312d32362030303a30303a30302e30303000000015707974686f6e20707265702073746174656d656e740000000139430000000e53454c454354203130005a0000000554
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testPythonInsertSelectHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        String script = """
                >0000000804d2162f
                <4e
                >0000002100030000757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >510000001b53455420646174657374796c6520544f202749534f2700
                <4300000008534554005a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >510000005c435245415445205441424c45204946204e4f542045584953545320747261646573202874732054494d455354414d502c206e616d6520535452494e472c2076616c756520494e54292074696d657374616d70287473293b00
                <43000000074f4b005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383335383439273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383431343837273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383432313035273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383432353134273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383432393439273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383433333739273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383433383237273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383434333138273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383434373833273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >51000000840a2020202020202020494e5345525420494e544f20747261646573202874732c206e616d652c2076616c7565290a202020202020202056414c554553202827323032312d30312d32345430353a30313a31312e383435323833273a3a74696d657374616d702c202770792d616263272c20313233293b0a202020202020202000
                <430000000f494e5345525420302031005a0000000554
                >510000000b434f4d4d495400
                <430000000b434f4d4d4954005a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >510000001a53454c454354202a2046524f4d207472616465733b00
                <540000004a00037473000000000000010000045a0008ffffffff00006e616d650000000000000200000413ffffffffffff000076616c756500000000000003000000170004ffffffff0000440000003500030000001a323032312d30312d32342030353a30313a31312e3833353834390000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834313438370000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834323130350000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834323531340000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834323934390000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834333337390000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834333832370000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834343331380000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834343738330000000670792d61626300000003313233440000003500030000001a323032312d30312d32342030353a30313a31312e3834353238330000000670792d61626300000003313233430000000e53454c454354203130005a0000000554
                >5800000004
                """;
        assertHexScript(
                NetworkFacadeImpl.INSTANCE,
                script,
                new Port0PGConfiguration()
        );
    }

    @Test
    public void testQueryAgainstIndexedSymbol() throws Exception {
        Assume.assumeTrue(walEnabled);
        final String[] values = {"'5'", "null", "'5' || ''", "replace(null, 'A', 'A')", "?5", "?null"};
        final CharSequenceObjHashMap<String> valMap = new CharSequenceObjHashMap<>();
        valMap.put("5", "5");
        valMap.put("'5'", "5");
        valMap.put("null", "null");
        valMap.put("'5' || ''", "5");
        valMap.put("replace(null, 'A', 'A')", "null");

        String no5 = "1\n2\n3\n4\n6\n7\n8\n9\nnull\n";
        String noNull = "1\n2\n3\n4\n5\n6\n7\n8\n9\n";
        String no5AndNull = "1\n2\n3\n4\n6\n7\n8\n9\n";

        final String[] tsOptions = {"", "timestamp(ts)", "timestamp(ts) partition by HOUR"};
        final String strType = ColumnType.nameOf(ColumnType.STRING).toLowerCase();
        for (String tsOption : tsOptions) {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                execute("drop table if exists tab");
                execute("create table tab (s symbol index, ts timestamp) " + tsOption);
                execute("insert into tab select case when x = 10 then null::" + strType + " else x::" + strType + " end, x::timestamp from long_sequence(10) ");
                drainWalQueue();

                ResultProducer sameVal =
                        (paramVals, isBindVals, bindVals, output) -> {
                            String value = isBindVals[0] ? bindVals[0] : paramVals[0];
                            output.put(valMap.get(value)).put('\n');
                        };

                assertQueryAgainstIndexedSymbol(values, "s = #X", new String[]{"#X"}, connection, tsOption, sameVal);
                assertQueryAgainstIndexedSymbol(values, "s in (#X)", new String[]{"#X"}, connection, tsOption, sameVal);
                assertQueryAgainstIndexedSymbol(values, "s in (#X, '10')", new String[]{"#X"}, connection, tsOption, sameVal);

                ResultProducer otherVals = (paramVals, isBindVals, bindVals, output) -> {
                    String value = isBindVals[0] ? bindVals[0] : paramVals[0];
                    if (valMap.get(value).equals("5")) {
                        output.put(no5);
                    } else {
                        output.put(noNull);
                    }
                };

                assertQueryAgainstIndexedSymbol(values, "s != #X", new String[]{"#X"}, connection, tsOption, otherVals);
                assertQueryAgainstIndexedSymbol(values, "s != #X and s != '10'", new String[]{"#X"}, connection, tsOption, otherVals);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X)", new String[]{"#X"}, connection, tsOption, otherVals);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X, '10')", new String[]{"#X"}, connection, tsOption, otherVals);

                ResultProducer sameValIfParamsTheSame = (paramVals, isBindVals, bindVals, output) -> {
                    String left = isBindVals[0] ? bindVals[0] : paramVals[0];
                    String right = isBindVals[1] ? bindVals[1] : paramVals[1];
                    boolean isSame = valMap.get(left).equals(valMap.get(right));
                    if (isSame) {
                        output.put(valMap.get(left)).put('\n');
                    }
                };

                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s = #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1) and s in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, 'S1') and s in (#X2, 'S2')", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s in (#X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1) and s = #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, 'S') and s = #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);

                ResultProducer otherVals2 = (paramVals, isBindVals, bindVals, output) -> {
                    String left = isBindVals[0] ? bindVals[0] : paramVals[0];
                    String right = isBindVals[1] ? bindVals[1] : paramVals[1];
                    boolean isSame = valMap.get(left).equals(valMap.get(right));
                    if (!isSame) {
                        output.put(no5AndNull);
                    } else {
                        if (valMap.get(left).equals("5")) {
                            output.put(no5);
                        } else {
                            output.put(noNull);
                        }
                    }
                };

                assertQueryAgainstIndexedSymbol(values, "s != #X1 and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s != #X1 and s != #X2 and s != 'S'", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s != #X1 and s not in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s != #X1 and s not in (#X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X1) and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X1, 'S') and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X1, 'S2') and s not in (#X2, 'S1')", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X1, #X2)", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);
                assertQueryAgainstIndexedSymbol(values, "s not in (#X1, #X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, otherVals2);

                ResultProducer sameValIfParmsDiffer = (paramVals, isBindVals, bindVals, output) -> {
                    String left = isBindVals[0] ? bindVals[0] : paramVals[0];
                    String right = isBindVals[1] ? bindVals[1] : paramVals[1];
                    boolean isSame = valMap.get(left).equals(valMap.get(right));
                    if (!isSame) {
                        output.put(valMap.get(left)).put('\n');
                    }
                };

                assertQueryAgainstIndexedSymbol(values, "s in (#X1) and s not in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, 'S') and s not in (#X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1) and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, 'S') and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s not in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s not in (#X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);
                assertQueryAgainstIndexedSymbol(values, "s = #X1 and s != #X2", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParmsDiffer);

                ResultProducer sameVal2 = (paramVals, isBindVals, bindVals, output) -> {
                    String left = isBindVals[0] ? bindVals[0] : paramVals[0];
                    String right = isBindVals[1] ? bindVals[1] : paramVals[1];
                    boolean isSame = valMap.get(left).equals(valMap.get(right));
                    if (isSame) {
                        output.put(valMap.get(right)).put('\n');
                    } else {
                        output.put("5\nnull\n");
                    }
                };

                assertQueryAgainstIndexedSymbol(values, "s in (#X1, #X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameVal2);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, #X2, 'S')", new String[]{"#X1", "#X2"}, connection, tsOption, sameVal2);
                assertQueryAgainstIndexedSymbol(values, "s in (#X1, #X2, 'S', 'S') and s not in ('S1', 'S1')", new String[]{"#X1", "#X2"}, connection, tsOption, sameVal2);
            });
        }
    }

    @Test
    public void testQueryCountMetrics() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table x as (select x id from long_sequence(10))");
            // table
            configuration.getMetrics().pgWireMetrics().resetQueryCounters();
            try (
                    PreparedStatement stmt = connection.prepareStatement("select count() from x;");
                    ResultSet rs = stmt.executeQuery()
            ) {
                rs.next();
                Assert.assertEquals(10, rs.getLong(1));
                Assert.assertEquals(1, configuration.getMetrics().pgWireMetrics().startedQueriesCount());
                Assert.assertEquals(1, configuration.getMetrics().pgWireMetrics().completedQueriesCount());
            }
            // virtual
            configuration.getMetrics().pgWireMetrics().resetQueryCounters();
            try (
                    PreparedStatement stmt = connection.prepareStatement("select 1;");
                    ResultSet rs = stmt.executeQuery()
            ) {
                rs.next();
                Assert.assertEquals(1, rs.getLong(1));
                Assert.assertEquals(1, configuration.getMetrics().pgWireMetrics().startedQueriesCount());
                Assert.assertEquals(1, configuration.getMetrics().pgWireMetrics().completedQueriesCount());
            }
        });
    }

    @Test
    public void testQueryCountWithTsSmallerThanMinTsInTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute(
                    "create table \"table\" (" +
                            "id symbol, " +
                            "timestamp timestamp) " +
                            "timestamp(timestamp) partition by day"
            );
            execute(
                    "insert into \"table\" " +
                            " select rnd_symbol(16, 10,10,0), dateadd('s', x::int, '2023-03-23T00:00:00.000000Z') " +
                            " from long_sequence(10000)"
            );

            conn.setAutoCommit(false);
            String queryBase = "select * from \"table\" "
                    + " WHERE timestamp >= '2023-03-23T00:00:00.000000Z'"
                    + " ORDER BY timestamp ";

            int countStar = getCountStar(conn);
            int ascCount = getRowCount(queryBase + "ASC", conn);
            int ascLimitCount = getRowCount(queryBase + "ASC LIMIT 100000", conn);
            int descCount = getRowCount(queryBase + "DESC", conn);
            int descLimitCount = getRowCount(queryBase + "DESC LIMIT 100000", conn);

            String message =
                    String.format("%n -- QUERY RESULTS -- %n"
                            + "  count(*)       = [%d]%n"
                            + "  descCount      = [%d]%n"
                            + "  descLimitCount = [%d]%n"
                            + "  ascCount       = [%d]%n"
                            + "  ascLimitCount  = [%d]%n"
                            + " -----------------%n", countStar, descCount, descLimitCount, ascCount, ascLimitCount);

            boolean allEqual = countStar == descCount
                    && descCount == descLimitCount && descCount == ascCount && descCount == ascLimitCount;

            Assert.assertTrue(message, allEqual);
        });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableEventNeverFired() throws Exception {
        Assume.assumeFalse(walEnabled);
        maxQueryTime = 100;
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
            TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;
            try {
                String query = "select * from test_data_unavailable(1, 10)";
                String expected = """
                        x[BIGINT],y[BIGINT],z[BIGINT]
                        1,1,1
                        """;
                try (ResultSet resultSet = connection.prepareStatement(query).executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, resultSet);
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "timeout, query aborted ");
                }
            } finally {
                // Make sure to close the event on the producer side.
                Misc.free(eventRef.get());
            }
        });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableEventTriggeredAfterDelay() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            int totalRows = 3;
            int backoffCount = 3;

            final AtomicInteger totalEvents = new AtomicInteger();
            final AtomicReference<SuspendEvent> eventRef = new AtomicReference<>();
            final AtomicBoolean stopDelayThread = new AtomicBoolean();
            final AtomicInteger errorCount = new AtomicInteger();

            final Thread delayThread = new Thread(() -> {
                while (!stopDelayThread.get()) {
                    SuspendEvent event = eventRef.getAndSet(null);
                    if (event != null) {
                        Os.sleep(1);
                        try {
                            event.trigger();
                            event.close();
                            totalEvents.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    } else {
                        Os.pause();
                    }
                }
            });
            delayThread.start();

            TestDataUnavailableFunctionFactory.eventCallback = eventRef::set;

            String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
            String expected = """
                    x[BIGINT],y[BIGINT],z[BIGINT]
                    1,1,1
                    2,2,2
                    3,3,3
                    """;
            try (ResultSet resultSet = connection.prepareStatement(query).executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
            stopDelayThread.set(true);

            delayThread.join();
            Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
            Assert.assertEquals(0, errorCount.get());
        });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableEventTriggeredImmediately() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            int totalRows = 3;
            int backoffCount = 10;

            final AtomicInteger totalEvents = new AtomicInteger();
            TestDataUnavailableFunctionFactory.eventCallback = event -> {
                event.trigger();
                event.close();
                totalEvents.incrementAndGet();
            };

            String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
            String expected = """
                    x[BIGINT],y[BIGINT],z[BIGINT]
                    1,1,1
                    2,2,2
                    3,3,3
                    """;
            try (ResultSet resultSet = connection.prepareStatement(query).executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }

            Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
        });
    }

    @Test
    public void testQueryEventuallySucceedsOnDataUnavailableSmallSendBuffer() throws Exception {
        skipOnWalRun(); // test doesn't use tables
        assertMemoryLeak(() -> {
            PGConfiguration configuration = new Port0PGConfiguration() {
                @Override
                public int getSendBufferSize() {
                    return 192;
                }
            };

            try (
                    PGServer server = createPGServer(configuration);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (Connection connection = getConnection(Mode.EXTENDED, port, true)) {
                    int totalRows = 16;
                    int backoffCount = 3;

                    final AtomicInteger totalEvents = new AtomicInteger();
                    TestDataUnavailableFunctionFactory.eventCallback = event -> {
                        event.trigger();
                        event.close();
                        totalEvents.incrementAndGet();
                    };

                    String query = "select * from test_data_unavailable(" + totalRows + ", " + backoffCount + ")";
                    String expected = """
                            x[BIGINT],y[BIGINT],z[BIGINT]
                            1,1,1
                            2,2,2
                            3,3,3
                            4,4,4
                            5,5,5
                            6,6,6
                            7,7,7
                            8,8,8
                            9,9,9
                            10,10,10
                            11,11,11
                            12,12,12
                            13,13,13
                            14,14,14
                            15,15,15
                            16,16,16
                            """;
                    try (ResultSet resultSet = connection.prepareStatement(query).executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, resultSet);
                    }

                    Assert.assertEquals(totalRows * backoffCount, totalEvents.get());
                }
            }
        });
    }

    @Test
    public void testQueryTimeout() throws Exception {
        skipOnWalRun(); // non-partitioned table
        maxQueryTime = 100;
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table tab as (select rnd_double() d from long_sequence(1000000))");
            try (final PreparedStatement statement = connection.prepareStatement("select * from tab order by d")) {
                try {
                    statement.execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                }
            }
        });
    }

    @Test
    public void testQueryTimeoutModern() throws Exception {
        skipOnWalRun(); // non-partitioned table
        maxQueryTime = 100;
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("select sleep(120000)")) {
                try {
                    statement.execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                }
            }
        });
    }

    @Test
    public void testQuestDBVersionIncludedInStatus() throws Exception {
        skipOnWalRun(); // no table at all
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PgConnection pgConnection = connection.unwrap(PgConnection.class);
            String actualVersion = pgConnection.getParameterStatus("questdb_version");
            String expectedVersion = configuration.getBuildInformation().getSwVersion();
            Assert.assertEquals(expectedVersion, actualVersion);
        });
    }

    // TODO(puzpuzpuz): fix schema changes handling in PGWire for extended protocol
    //                  https://github.com/questdb/questdb/issues/4971
    @Ignore
    @Test
    public void testReadParquetSchemaChangeExtended() throws Exception {
        testReadParquetSchemaChange(false);
    }

    @Test
    public void testReadParquetSchemaChangeSimple() throws Exception {
        testReadParquetSchemaChange(true);
    }

    @Test
    public void testRegProcedure() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    final CallableStatement stmt = connection.prepareCall("SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype " +
                            "FROM pg_type as t " +
                            "LEFT JOIN pg_range as r ON oid = rngtypid " +
                            "WHERE " +
                            "t.typname IN ('int2', 'int4', 'int8', 'oid', 'float4', 'float8', 'text', 'varchar', 'char', 'name', 'bpchar', 'bool', 'bit', 'varbit', 'timestamptz', 'date', 'money', 'bytea', 'point', 'hstore', 'json', 'jsonb', 'cidr', 'inet', 'uuid', 'xml', 'tsvector', 'macaddr', 'citext', 'ltree', 'line', 'lseg', 'box', 'path', 'polygon', 'circle', 'time', 'timestamp', 'numeric', 'interval') " +
                            "OR t.typtype IN ('r', 'e', 'd') " +
                            "OR t.typinput = 'array_in(cstring,oid,integer)'::regprocedure " +
                            "OR t.typelem != 0 ");
                    stmt.execute();
                },
                () -> recvBufferSize = Math.max(2048, recvBufferSize)
        );
    }

    @Test
    public void testRegularBatchInsertMethod() throws Exception {
        // bind variables do not work well over "simple" protocol
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL ^ CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("create table test_batch(id long,val int)");
            }
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_batch(id,val) values(?,?)")) {
                batchInsert.setLong(1, 0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();

                batchInsert.clearParameters();
                batchInsert.setLong(1, 2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();

                int[] a = batchInsert.executeBatch();
                Assert.assertEquals(3, a.length);
                Assert.assertEquals(1, a[0]);
                Assert.assertEquals(1, a[1]);
                Assert.assertEquals(1, a[2]);
            }

            StringSink sink = new StringSink();
            String expected = """
                    id[BIGINT],val[INTEGER]
                    0,1
                    1,2
                    2,3
                    """;
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from test_batch");
            assertResultSet(expected, sink, rs);
        });
    }

    // test four:
    // -set fetchsize = 50
    // -run query (50 rows fetched)
    // -set fetchsize = 25
    // -process results:
    // --process 50 rows.
    // --do a FETCH FORWARD 25
    // --process 25 rows
    // --do a FETCH FORWARD 25
    // --process 25 rows. end of results.
    @Test
    public void testResultSetFetchSizeFour() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;

            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();

            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            stmt.setFetchSize(50);
            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(25);

            int count = 0;
            while (rs.next()) {
                assertEquals(count, rs.getInt(1));
                ++count;
            }

            assertEquals(totalRows, count);
        });
    }

    // test one:
    // -set fetchsize = 0
    // -run query (all rows should be fetched)
    // -set fetchsize = 50 (should have no effect)
    // -process results
    @Test
    public void testResultSetFetchSizeOne() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;

            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();

            PreparedStatement stmt = connection.prepareStatement("x");
            stmt.setFetchSize(0);

            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(50); // Should have no effect.

            int count = 0;
            while (rs.next()) {
                assertEquals(count, rs.getInt(1));
                ++count;
            }

            assertEquals(totalRows, count);
        });
    }

    // test three:
    // -set fetchsize = 25
    // -run query (25 rows fetched)
    // -set fetchsize = 50
    // -process results:
    // --process 25 rows. should NOT hit end-of-results here.
    // --do a FETCH FORWARD 50
    // --process 50 rows
    // --do a FETCH FORWARD 50
    // --process 25 rows. end of results.
    @Test
    public void testResultSetFetchSizeThree() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;

            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();

            connection.commit();

            PreparedStatement stmt = connection.prepareStatement("x");
            stmt.setFetchSize(25);
            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(50);

            int count = 0;
            while (rs.next()) {
                assertEquals(count, rs.getInt(1));
                ++count;
            }

            assertEquals(totalRows, count);
        });
    }

    // test two:
    // -set fetchsize = 25
    // -run query (25 rows fetched)
    // -set fetchsize = 0
    // -process results:
    // --process 25 rows
    // --should do a FETCH ALL to get more data
    // --process 75 rows
    @Test
    public void testResultSetFetchSizeTwo() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            int totalRows = 100;

            CallableStatement tbl = connection.prepareCall(
                    "create table x as (select cast(x - 1 as int) a from long_sequence(" + totalRows + "))");
            tbl.execute();

            connection.commit();
            PreparedStatement stmt = connection.prepareStatement("x");
            stmt.setFetchSize(25);
            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(0);

            int count = 0;
            while (rs.next()) {
                assertEquals(count, rs.getInt(1));
                ++count;
            }

            assertEquals(totalRows, count);
        });
    }

    @Test
    public void testRollbackDataOnStaleTransaction() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table xyz(a int)").execute();
            connection.prepareStatement("insert into xyz values (100)").execute();
            connection.prepareStatement("insert into xyz values (101)").execute();
            connection.prepareStatement("insert into xyz values (102)").execute();
            connection.prepareStatement("insert into xyz values (103)").execute();

            sink.clear();
            try (
                    PreparedStatement ps = connection.prepareStatement("xyz");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertResultSet(
                        "a[INTEGER]\n",
                        sink,
                        rs
                );
            }

            try {
                final SOCountDownLatch latch = new SOCountDownLatch(1);
                engine.setPoolListener(createWriterReleaseListener("xyz", latch));
                connection.close();
                latch.await(TimeUnit.SECONDS.toNanos(10));
            } finally {
                engine.setPoolListener(null);
            }

            try (TableWriter w = getWriter("xyz")) {
                w.commit();
            }

            try (final Connection connection2 = getConnection(mode, port, binary)) {
                sink.clear();
                try (
                        PreparedStatement ps = connection2.prepareStatement("xyz");
                        ResultSet rs = ps.executeQuery()
                ) {
                    assertResultSet(
                            "a[INTEGER]\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testRowLimitNotResumed() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (final Connection connection = getConnection(Mode.EXTENDED, port, true)) {
                    try (CallableStatement st1 = connection.prepareCall("create table y as (" +
                            "select timestamp_sequence(0, 1000000000) timestamp," +
                            " rnd_symbol('a','b',null) symbol1 " +
                            " from long_sequence(10)" +
                            ") timestamp (timestamp) partition by YEAR")) {
                        st1.execute();
                    }
                }
            }
            mayDrainWalQueue();

            try (
                    final PGServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                for (int i = 0; i < 3; i++) {
                    int port = server.getPort();
                    try (final Connection connection = getConnection(Mode.EXTENDED, port, true)) {
                        try (PreparedStatement select1 = connection.prepareStatement("select version()")) {
                            ResultSet rs0 = select1.executeQuery();
                            sink.clear();
                            assertResultSet("""
                                    version[VARCHAR]
                                    PostgreSQL 12.3, compiled by Visual C++ build 1914, 64-bit, QuestDB
                                    """, sink, rs0);
                            rs0.close();
                        }
                        try (PreparedStatement select2 = connection.prepareStatement("select timestamp from y")) {
                            select2.setMaxRows(1);
                            ResultSet rs2 = select2.executeQuery();
                            rs2.next();
                            rs2.close();
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRunAlterWhenTableLockedAndAlterTakesTooLong() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            node1.setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 1000);
            node1.setProperty(CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, 30_000);
            SOCountDownLatch queryStartedCountDown = new SOCountDownLatch();
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "_meta.swp")) {
                        queryStartedCountDown.await();
                        Os.sleep(configuration.getWriterAsyncCommandBusyWaitTimeout() * 2);
                    }
                    return super.openRW(name, opts);
                }
            };
            testAddColumnBusyWriter(true, new SOCountDownLatch());
        });
    }

    @Test
    public void testRunAlterWhenTableLockedAndAlterTakesTooLongFailsToWait() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            skipOnWalRun(); // Alters do not wait for WAL tables
            node1.setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 1000);
            long writerAsyncCommandMaxTimeout = configuration.getWriterAsyncCommandBusyWaitTimeout();
            Assert.assertEquals(1000, writerAsyncCommandMaxTimeout);
            node1.setProperty(CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, writerAsyncCommandMaxTimeout);
            SOCountDownLatch queryStartedCountDown = new SOCountDownLatch();
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "_meta.swp")) {
                        queryStartedCountDown.await();
                        // wait for twice the time to allow busy wait to time out
                        Os.sleep(configuration.getWriterAsyncCommandBusyWaitTimeout() * 2);
                    }
                    return super.openRW(name, opts);
                }
            };
            testAddColumnBusyWriter(false, queryStartedCountDown);
        });
    }

    @Test
    public void testRunAlterWhenTableLockedAndAlterTimeoutsToStart() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            node1.setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 1);
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "_meta.swp")) {
                        Os.sleep(50);
                    }
                    return super.openRW(name, opts);
                }
            };
            testAddColumnBusyWriter(false, new SOCountDownLatch());
        });
    }

    @Test
    public void testRunAlterWhenTableLockedWithInserts() throws Exception {
        skipOnWalRun(); // non-partitioned table
        node1.setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 10_000);
        assertMemoryLeak(() -> testAddColumnBusyWriter(true, new SOCountDownLatch()));
    }

    @Test
    public void testRunQueryAfterCancellingPreviousInTheSameConnection() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table if not exists tab as " +
                    "(select x::timestamp ts, " +
                    "        x, " +
                    "        rnd_double() d " +
                    " from long_sequence(10)) " +
                    "timestamp(ts) partition by day");
            mayDrainWalQueue();
            executeAndCancelQuery((PgConnection) connection);

            try (final PreparedStatement stmt = connection.prepareStatement("select count(*) from tab where x > 0")) {
                ResultSet result = stmt.executeQuery();
                sink.clear();
                assertResultSet("count[BIGINT]\n10\n", sink, result);
            }
        });
    }

    @Test
    public void testRunSimpleQueryMultipleTimes() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                final String query = "select 42 as the_answer";
                final String expected = """
                        the_answer[INTEGER]
                        42
                        """;

                ResultSet rs = statement.executeQuery(query);
                assertResultSet(expected, sink, rs);

                sink.clear();
                rs = statement.executeQuery(query);
                assertResultSet(expected, sink, rs);
            }
        });
    }

    @Test
    public void testRustBindVariableHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        //hex for close message 43 00000009 53 535f31 00
        assertHexScript("""
                >0000003600030000636c69656e745f656e636f64696e67005554463800757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >5100000067435245415445205441424c45204946204e4f542045584953545320747261646573202874732054494d455354414d502c206461746520444154452c206e616d6520535452494e472c2076616c756520494e54292074696d657374616d70287473293b00
                <43000000074f4b005a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >5000000031733000696e7365727420696e746f207472616465732076616c756573202824312c24322c24332c2434290000004400000008537330005300000004
                <3100000004740000001600040000045a0000045a00000413000000176e000000045a0000000554
                >420000004200733000000100010004000000080002649689ed0814000000080002649689ed08170000000c72757374206578616d706c65000000040000000000010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >510000000b434f4d4d495400
                <430000000b434f4d4d4954005a0000000549
                >4300000008537330005300000004
                <33000000045a0000000549
                >5800000004
                """);
    }

    @Test
    public void testRustSelectHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertHexScript("""
                >0000004300030000636c69656e745f656e636f64696e6700555446380074696d657a6f6e650055544300757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >510000005c435245415445205441424c45204946204e4f542045584953545320747261646573202874732054494d455354414d502c206e616d6520535452494e472c2076616c756520494e54292074696d657374616d70287473293b00
                <43000000074f4b005a0000000549
                >5000000059733000494e5345525420494e544f207472616465732056414c55455328746f5f74696d657374616d702824312c2027797979792d4d4d2d64645448483a6d6d3a73732e53535355555527292c24322c2433290000004400000008537330005300000004
                <3100000004740000001200030000041300000413000000176e000000045a0000000549
                >4200000048007330000001000100030000001a323032312d30312d32305431343a30303a30362e3537323839370000000c72757374206578616d706c65000000040000007b00010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000549
                >4300000008537330005300000004
                <33000000045a0000000549
                >510000000a424547494e00
                <430000000a424547494e005a0000000554
                >500000005b733100696e7365727420696e746f207472616465732076616c7565732028746f5f74696d657374616d702824312c2027797979792d4d4d2d64645448483a6d6d3a73732e53535355555527292c24322c202433290000004400000008537331005300000004
                <3100000004740000001200030000041300000413000000176e000000045a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630323834330000000c72757374206578616d706c65000000040000000000010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630333430360000000c72757374206578616d706c65000000040000000100010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630333830350000000c72757374206578616d706c65000000040000000200010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630343139360000000c72757374206578616d706c65000000040000000300010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630343537370000000c72757374206578616d706c65000000040000000400010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630343938320000000c72757374206578616d706c65000000040000000500010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630353338350000000c72757374206578616d706c65000000040000000600010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630353738310000000c72757374206578616d706c65000000040000000700010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630363237380000000c72757374206578616d706c65000000040000000800010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >4200000048007331000001000100030000001a323032312d30312d32305431343a30303a30362e3630363636360000000c72757374206578616d706c65000000040000000900010001450000000900000000005300000004
                <3200000004430000000f494e5345525420302031005a0000000554
                >510000000b434f4d4d495400
                <430000000b434f4d4d4954005a0000000549
                >4300000008537331005300000004
                <33000000045a0000000549
                >5800000004
                """);
    }

    @Test
    public void testSchemasCall() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table test (id long,val int)");
                        statement.executeUpdate("create table test2(id long,val int)");
                    }

                    final DatabaseMetaData metaData = connection.getMetaData();
                    try (ResultSet rs = metaData.getCatalogs()) {
                        assertResultSet(
                                """
                                        TABLE_CAT[VARCHAR]
                                        qdb
                                        """,
                                sink,
                                rs
                        );
                    }

                    sink.clear();

                    try (ResultSet rs = metaData.getSchemas()) {
                        assertResultSet(
                                """
                                        TABLE_SCHEM[VARCHAR],TABLE_CATALOG[VARCHAR]
                                        pg_catalog,null
                                        public,null
                                        """,
                                sink,
                                rs
                        );
                    }

                    sink.clear();

                    try (ResultSet rs = metaData.getTables("qdb", null, null, null)) {
                        assertResultSet(
                                """
                                        TABLE_CAT[VARCHAR],TABLE_SCHEM[VARCHAR],TABLE_NAME[VARCHAR],TABLE_TYPE[VARCHAR],REMARKS[VARCHAR],TYPE_CAT[VARCHAR],TYPE_SCHEM[VARCHAR],TYPE_NAME[VARCHAR],SELF_REFERENCING_COL_NAME[VARCHAR],REF_GENERATION[VARCHAR]
                                        null,pg_catalog,pg_class,SYSTEM TABLE,null,,,,,
                                        null,public,sys.text_import_log,TABLE,null,,,,,
                                        null,public,test,TABLE,null,,,,,
                                        null,public,test2,TABLE,null,,,,,
                                        """,
                                sink,
                                rs
                        );
                    }

                    sink.clear();
                    try (ResultSet rs = metaData.getColumns("qdb", null, "test", null)) {
                        assertResultSet(
                                """
                                        TABLE_CAT[VARCHAR],TABLE_SCHEM[VARCHAR],TABLE_NAME[VARCHAR],COLUMN_NAME[VARCHAR],DATA_TYPE[SMALLINT],TYPE_NAME[VARCHAR],COLUMN_SIZE[INTEGER],BUFFER_LENGTH[VARCHAR],DECIMAL_DIGITS[INTEGER],NUM_PREC_RADIX[INTEGER],NULLABLE[INTEGER],REMARKS[VARCHAR],COLUMN_DEF[VARCHAR],SQL_DATA_TYPE[INTEGER],SQL_DATETIME_SUB[INTEGER],CHAR_OCTET_LENGTH[VARCHAR],ORDINAL_POSITION[INTEGER],IS_NULLABLE[VARCHAR],SCOPE_CATALOG[VARCHAR],SCOPE_SCHEMA[VARCHAR],SCOPE_TABLE[VARCHAR],SOURCE_DATA_TYPE[SMALLINT],IS_AUTOINCREMENT[VARCHAR],IS_GENERATEDCOLUMN[VARCHAR]
                                        null,public,test,id,-5,int8,19,null,0,10,1,null,null,null,null,19,1,YES,null,null,null,0,NO,NO
                                        null,public,test,val,4,int4,10,null,0,10,1,null,null,null,null,10,2,YES,null,null,null,0,NO,NO
                                        """,
                                sink,
                                rs
                        );
                    }
                },
                () -> recvBufferSize = 2048
        );
    }

    @Test
    /* asyncq.py (please pay attention to non-standard username/password)

--
    import asyncio
    import asyncpg

    async def run():
        conn = await asyncpg.connect(user='xyz', password='oh',
                                 database='postgres', host='127.0.0.1')
        s = """
            select * from 'tab'
            LIMIT 100
            """
        values = await conn.fetch(s)
        await conn.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
--

-- SQL to create table

create table tab as (
    select
        rnd_byte() b,
        rnd_short() sh,
        rnd_int() i,
        rnd_long() l,
        rnd_float() f,
        rnd_double() d,
        rnd_str() s,
        rnd_symbol('abc', 'cde') sym,
        rnd_boolean() bool,
        rnd_date() dt,
        rnd_long256() lt,
        rnd_char() ch,
        rnd_timestamp(0, 1000, 0) ts,
        rnd_bin() bin
    from long_sequence(10)
);

     */
    public void testSelectAllTypesFromAsyncPG() throws Exception {
        skipOnWalRun(); // non-partitioned table
        execute("""
                create table tab as (
                    select
                        rnd_byte() b,
                        rnd_short() sh,
                        rnd_int() i,
                        rnd_long() l,
                        rnd_float() f,
                        rnd_double() d,
                        rnd_str() s,
                        rnd_symbol('abc', 'cde') sym,
                        rnd_boolean() bool,
                        rnd_date() dt,
                        rnd_long256() lt,
                        rnd_char() ch,
                        rnd_timestamp(0, 1000, 0) ts,
                        rnd_bin() bin
                    from long_sequence(10)
                );
                """
        );

        engine.releaseAllWriters();

        final String script = """
                >0000000804d2162f
                <4e
                >0000003c00030000636c69656e745f656e636f64696e6700277574662d382700757365720078797a00646174616261736500706f7374677265730000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000515f5f6173796e6370675f73746d745f315f5f000a202020202020202073656c656374202a2066726f6d2027746162270a20202020202020204c494d4954203130300a20202020202020200000004400000018535f5f6173796e6370675f73746d745f315f5f004800000004
                <310000000474000000060000540000012a000e6200000000000001000000150002ffffffff0000736800000000000002000000150002ffffffff00006900000000000003000000170004ffffffff00006c00000000000004000000140008ffffffff00006600000000000005000002bc0004ffffffff00006400000000000006000002bd0008ffffffff0000730000000000000700000413ffffffffffff000073796d0000000000000800000413ffffffffffff0000626f6f6c00000000000009000000100001ffffffff000064740000000000000a0000045a0008ffffffff00006c740000000000000b00000413ffffffffffff000063680000000000000c00000412ffff00000005000074730000000000000d0000045a0008ffffffff000062696e0000000000000e00000011ffffffffffff0001
                >4200000022005f5f6173796e6370675f73746d745f315f5f0000010001000000010001450000000900000000005300000004
                <320000000444000000d6000e00000002004c0000000260ee000000045c50d341000000089f9b2131d49fcd1d000000043dadd020000000083fd23631d4c984f000000005595258504500000003636465000000010100000008fffca2ff9b5cae6000000042307836653630613031613562336561306462346230663539356631343365356437323266316138323636653739323165336237313664653364323564636332643931000000015800000008fffca2fec4c821d600000020c788dea0793c7715686126af19c49594365349b4597e3b08a11e388d1b9ef4c844000000d6000e00000002003900000002fb09000000040fbffdfe000000086afe61bd7c4ae0d8000000043f675fe3000000083feeffefe8f64b8500000005445251515500000003636465000000010100000008fffca2fee4ad31d000000042307836353566383761336132316435373566363130663639656665303633666537393333366463343334373930656433333132626266636636366261623933326663000000014a00000008fffca2fec4c8227400000020934d1a8e78b5b91153d0fb64bb1ad4f02d40e24bb13ee3f1f11eca9c1d06ac3744000000da000e0000000200700000000217cd000000046fdde48200000008997918f622d62989000000043f3916a1000000083fdd38eacf6e41fa000000094f545345445959435400000003616263000000010000000008fffca3000aa21be800000042307837656261663663613939336638666339386231333039636633326436386262386161376463346563636236383134366662333766316563383237353263376437000000014300000008fffca2fec4c8227a000000208447dcd2857fa5b87b4a9d467c8ddd93e6d0b32b0798cc7648a3bb64d2ad491c44000000d8000e00000002005600000002cc3c0000000424a116ed000000086ea837f54a415439000000043e9beabe000000083f9b7b1f63e262c0000000074a4f4a4950485a00000003616263000000010100000008fffca300ec9bd72800000042307862623536616237376366666530613839346165643131633732323536613830633762356464326238353133623331653762323065313930306361666638313961000000014f00000008fffca2fec4c8229d00000020b7c29f298e295e69c6ebeac3c9739346fec2d368798b431d573404238dd8579144000000d7000e00000002004c000000023a2800000004c43377a500000008fdb12ef0d2c74218000000043e8ad49a000000083fe4a8ba7fe3d5cd000000064a4f58504b5200000003616263000000010100000008fffca301160fd32000000042307838643563346265643834333264653938363261326631316538353130613365393963623866633634363730323865623061303739333462326131356465386530000000014f00000008fffca2fec4c820f8000000202860b0ec0b92587d24bc2e606a1c0b20a2868937112c140c2d208452d96f04ab44000000db000e00000002007d000000027e470000000455572a8f000000089c0a1370d099b723000000043f2c45d5000000083fdd63a4d105648a0000000a4e4f4d56454c4c4b4b4800000003636465000000010000000008fffca2ffe0fb34c800000042307834633037316431636136353830356133303565373337303063626562653565623366386363346663343736636163633937393834323036623434363761323830000000014c00000008fffca2fec4c823290000002079e435e43adc5c65ff276777125452d02926c5aada18ce5fb28b5c549025c22044000000db000e000000020039000000020d7000000004f85e333a000000087d85ee2916b209c7000000043e8c4988000000083fd61b4700e1e4460000000a544a434b464d514e544f00000003636465000000010100000008fffca2ffe5c7e3c000000042307833346130353839393038383036393862376362303535633534373235623935323761313931363464383037636565363133343537306132626565343436373335000000014d00000008fffca2fec4c821840000002057a5dba1761c1c26fb2e42faf56e8f80e354b807b13257ff9aef88cb4ba1cfcf44000000d5000e00000002007500000002947d000000048a4592a60000000886be020b55a15fd1000000043f4f90cc000000083fe28cacbc129a84000000044849554700000003616263000000010000000008fffca2ff170d01f000000042307837333762316461636436626535393731393233383461616264383838656362333461363533323836623031303931326237326631643638363735643836376366000000014300000008fffca2fec4c823d60000002011963708dd98ef54882aa2ade7d462e14ed6b2575be3713d20e237f26443845544000000d8000e0000000200240000000240dd00000004c493cf44000000089aadb86434093111000000043d5244c0000000083fef5a79f2bd966500000007575a4e464b504500000003636465000000010000000008fffca300421f7e0800000042307862663839323565316139336666613637396638376439316330366466383733353766623537626331366335313265623862353264336265616664376536306537000000014700000008fffca2fec4c8227e000000208e28b6a917ec0e01c4eb9f138fbb2a4baf8f89df358fdafe3398808520533b5144000000d9000e00000002006900000002655d000000044d4f2528000000083cc96390430d88ac000000043e5c247c000000083fe2723a9f780843000000085151454d58444b5800000003636465000000010100000008fffca2fef62d1d3000000042307837656635393366303066623438313863363466363836303336343261373136643734356430373932643038666466616638303530376365316434323238383630000000015900000008fffca2fec4c822af00000020463b473ce1723b9defc44ac9cffb9d63ca94006bdd18fe7176bc4524cd13007c430000000e53454c454354203130005a0000000549
                >5800000004""";
        assertHexScript(
                getFragmentedSendFacade(),
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    @Test
    public void testSelectArrayBindingVars() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            final Array array1d = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d});
            final Array array2d = connection.createArrayOf("float8", new Double[][]{{1d, 2d, 3d}, {4d, 5d, 6d}});

            final int nVars = 12;
            try (PreparedStatement stmt = connection.prepareStatement(
                    "select ? + ? sum, ? - ? sub, ? * ? mul, ? / ? div, " +
                            "? + 42 sum_scalar, ? - 42 sub_scalar, ? * 42 mul_scalar, ? / 42 div_scalar " +
                            "from long_sequence(1)"
            )) {
                for (int i = 0; i < nVars; i++) {
                    stmt.setArray(i + 1, array1d);
                }

                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[ARRAY],sub[ARRAY],mul[ARRAY],div[ARRAY],sum_scalar[ARRAY],sub_scalar[ARRAY],mul_scalar[ARRAY],div_scalar[ARRAY]
                                    {2.0,4.0,6.0},{0.0,0.0,0.0},{1.0,4.0,9.0},{1.0,1.0,1.0},{43.0,44.0,45.0},{-41.0,-40.0,-39.0},{42.0,84.0,126.0},{0.023809523809523808,0.047619047619047616,0.07142857142857142}
                                    """,
                            sink,
                            rs
                    );
                }

                // Now, let's change the bind var array dimensionality - the query should succeed
                for (int i = 0; i < nVars; i++) {
                    stmt.setArray(i + 1, array2d);
                }

                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet(
                            """
                                    sum[ARRAY],sub[ARRAY],mul[ARRAY],div[ARRAY],sum_scalar[ARRAY],sub_scalar[ARRAY],mul_scalar[ARRAY],div_scalar[ARRAY]
                                    {{2.0,4.0,6.0},{8.0,10.0,12.0}},{{0.0,0.0,0.0},{0.0,0.0,0.0}},{{1.0,4.0,9.0},{16.0,25.0,36.0}},{{1.0,1.0,1.0},{1.0,1.0,1.0}},{{43.0,44.0,45.0},{46.0,47.0,48.0}},{{-41.0,-40.0,-39.0},{-38.0,-37.0,-36.0}},{{42.0,84.0,126.0},{168.0,210.0,252.0}},{{0.023809523809523808,0.047619047619047616,0.07142857142857142},{0.09523809523809523,0.11904761904761904,0.14285714285714285}}
                                    """,
                            sink,
                            rs
                    );
                }
            }
        }, () -> recvBufferSize = 4096); // all bind vars need to fit the buffer
    }

    /* asyncqp.py - bind variable in where clause.
       Unlike jdbc driver, Asyncpg doesn't pass parameter types in Parse message and relies on types returned in ParameterDescription.
    import asyncio
    import asyncpg

    async def run():
        conn = await asyncpg.connect(user='xyz', password='oh', database='postgres', host='127.0.0.1')
        s = """
                select * from 'tab2' where a > $1
                LIMIT 100
            """
        values = await conn.fetch(s, 0.4)
        await conn.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
     */
    @Test
    public void testSelectBindVarsAsyncPG() throws Exception {
        skipOnWalRun(); // non-partitioned table

        execute("create table tab2 (a double);");
        execute("insert into 'tab2' values (0.7);");
        execute("insert into 'tab2' values (0.2);");
        engine.clear();

        final String script = """
                >0000000804d2162f
                <4e
                >0000003c00030000636c69656e745f656e636f64696e6700277574662d382700757365720078797a00646174616261736500706f7374677265730000
                <520000000800000003
                >70000000076f6800
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000675f5f6173796e6370675f73746d745f315f5f000a20202020202020202020202073656c656374202a2066726f6d202774616232272077686572652061203e2024310a2020202020202020202020204c494d4954203130300a20202020202020200000004400000018535f5f6173796e6370675f73746d745f315f5f004800000004
                <3100000004740000000a0001000002bd540000001a00016100000000000001000002bd0008ffffffff0000
                >420000002e005f5f6173796e6370675f73746d745f315f5f00000100010001000000083fd999999999999a00010001450000000900000000005300000004
                <320000000444000000120001000000083fe6666666666666430000000d53454c4543542031005a0000000549
                >5800000004
                """;
        assertHexScript(
                getFragmentedSendFacade(),
                script,
                getStdPgWireConfigAltCreds()
        );
    }

    /* asyncqp.py - bind variable appear both in select and where clause
       Unlike jdbc driver, Asyncpg doesn't pass parameter types in Parse message and relies on types returned in ParameterDescription.

    import asyncio
    import asyncpg

    async def run():
        conn = await asyncpg.connect(user='xyz', password='oh', database='postgres', host='127.0.0.1')
        s = """
                select $1, * from 'tab2' where a > $1 LIMIT 100
            """
        values = await conn.fetch(s, 'oh' 0.4)
        await conn.close()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
     */
    @Test//bind variables make sense in extended mode only
    public void testSelectBindVarsInSelectAndWhereAsyncPG() throws Exception {
        skipOnWalRun(); // non-partitioned table

        execute("create table tab2 (a double);");
        execute("insert into 'tab2' values (0.7);");
        execute("insert into 'tab2' values (0.2);");
        engine.clear();

        final String script = """
                >0000000804d2162f
                <4e
                >0000003900030000636c69656e745f656e636f64696e6700277574662d382700757365720061646d696e006461746162617365007164620000
                <520000000800000003
                >700000000a717565737400
                <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                >50000000665f5f6173796e6370675f73746d745f315f5f000a202020202020202020202073656c6563742024312c2a2066726f6d20746162322077686572652061203e2024320a20202020202020202020204c494d4954203130300a20202020202020200000004400000018535f5f6173796e6370675f73746d745f315f5f004800000004
                <3100000004740000000e000200000413000002bd540000002f000224310000000000000100000413ffffffffffff00006100000000000002000002bd0008ffffffff0000
                >4200000036005f5f6173796e6370675f73746d745f315f5f000002000100010002000000026f68000000083fd999999999999a00010001450000000900000000005300000004
                <320000000444000000180002000000026f68000000083fe6666666666666430000000d53454c4543542031005a0000000549
                >5800000004
                """;
        assertHexScript(script);
    }

    @Test
    public void testSelectStringInWithBindVariables() throws Exception {
        sharedQueryWorkerCount = 2; // Set to 1 to enable parallel query plans

        try {
            assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
                connection.setAutoCommit(false);
                connection.prepareStatement("CREATE TABLE tab (ts TIMESTAMP, s INT)").execute();
                connection.prepareStatement("INSERT INTO tab VALUES ('2023-06-05T11:12:22.116234Z', 1)").execute();//monday
                connection.prepareStatement("INSERT INTO tab VALUES ('2023-06-06T16:42:00.333999Z', 2)").execute();//tuesday
                connection.prepareStatement("INSERT INTO tab VALUES ('2023-06-07T03:52:00.999999Z', 3)").execute();//wednesday
                connection.prepareStatement("INSERT INTO tab VALUES (null, 4)").execute();
                connection.commit();
                mayDrainWalQueue();
                String query = "SELECT * FROM tab WHERE to_str(ts,'EE') in (?,'Wednesday',?)";
                try (PreparedStatement stmt = connection.prepareStatement("explain " + query)) {
                    stmt.setString(1, "Tuesday");
                    stmt.setString(2, "Friday");
                    try (ResultSet rs = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        QUERY PLAN[VARCHAR]
                                        Async Filter workers: 2
                                          filter: to_str(ts) in [$0::string,'Wednesday',$1::string]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                        """,
                                sink,
                                rs
                        );
                    }
                }

                try (PreparedStatement stmt = connection.prepareStatement(query)) {
                    stmt.setString(1, "Monday");
                    stmt.setString(2, null);
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                ts[TIMESTAMP],s[INTEGER]
                                2023-06-05 11:12:22.116234,1
                                2023-06-07 03:52:00.999999,3
                                null,4
                                """, sink, resultSet);
                    }

                    stmt.setString(1, "Tuesday");
                    stmt.setString(2, "Friday");
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                ts[TIMESTAMP],s[INTEGER]
                                2023-06-06 16:42:00.333999,2
                                2023-06-07 03:52:00.999999,3
                                """, sink, resultSet);
                    }

                    stmt.setString(1, "Saturday");
                    stmt.setString(2, "Sunday");
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                ts[TIMESTAMP],s[INTEGER]
                                2023-06-07 03:52:00.999999,3
                                """, sink, resultSet);
                    }
                }

                try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM tab WHERE to_str(ts,'EE') in (?)")) {
                    stmt.setString(1, "Monday");
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet("""
                                ts[TIMESTAMP],s[INTEGER]
                                2023-06-05 11:12:22.116234,1
                                """, sink, resultSet);
                    }

                    stmt.setString(1, "Saturday");
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        sink.clear();
                        assertResultSet("ts[TIMESTAMP],s[INTEGER]\n", sink, resultSet);
                    }
                }
            });
        } finally {
            sharedQueryWorkerCount = 0;
        }
    }

    @Test
    public void testSemicolon() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement(";;")) {
                statement.execute();
            }
        });
    }

    @Test
    public void testSendBufferFull() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    connection.setAutoCommit(false);
                    try (PreparedStatement pstmt = connection.prepareStatement("create table t as " +
                            "(select cast(x + 1 as long) a, cast(x as timestamp) b from long_sequence(10))")) {
                        pstmt.execute();
                    }

                    for (int i = 20; i < 100; i++) {
                        try (PreparedStatement select = connection.prepareStatement(
                                "select x from long_sequence(" + i + ")")) {

                            try (ResultSet resultSet = select.executeQuery()) {
                                int r = 1;
                                while (resultSet.next()) {
                                    Assert.assertEquals(r++, resultSet.getLong(1));
                                }
                            }
                        }

                        try (PreparedStatement pstmt = connection.prepareStatement("insert into t values (1, " + i + ")")) {
                            pstmt.execute();
                        }
                    }
                },
                () -> {
                    sendBufferSize = 512;
                    forceSendFragmentationChunkSize = 10;
                }
        );
    }

    @Test
    public void testSendingBufferWhenFlushMessageReceivedHex() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertHexScriptAltCreds(
                """
                        >0000006e00030000757365720078797a0064617461626173650071646200636c69656e745f656e636f64696e67005554463800446174655374796c650049534f0054696d655a6f6e65004575726f70652f4c6f6e646f6e0065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >70000000076f6800
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >500000002c0073656c65637420782c202024312066726f6d206c6f6e675f73657175656e63652832293b000000
                        >42000000110000000000010000000133000044000000065000450000000900000000004800000004
                        <31000000043200000004540000002f00027800000000000001000000140008ffffffff000024310000000000000200000413ffffffffffff000044000000100002000000013100000001334400000010000200000001320000000133430000000d53454c454354203200
                        >4800000004
                        >5300000004
                        <5a0000000549
                        >5800000004
                        """
        );
    }

    @Test
    public void testSimple() throws Exception {
        testQuery("rnd_double(4) d, ", "s[VARCHAR],i[INTEGER],d[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY],rnd_char[CHAR],rnd_long256[VARCHAR]\n");
    }

    @Test
    public void testSimpleAlterTable() throws Exception {
        // we are going to:
        // 1. create a table
        // 2. alter table
        // 3. check table column added
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement statement = connection.prepareStatement("create table x (a int)");
            statement.execute();

            PreparedStatement alter = connection.prepareStatement("alter table x add column b long");
            alter.executeUpdate();

            PreparedStatement select = connection.prepareStatement("x");
            try (ResultSet resultSet = select.executeQuery()) {
                Assert.assertEquals(1, resultSet.findColumn("a"));
                Assert.assertEquals(2, resultSet.findColumn("b"));
            }
        });
    }

    @Test
    public void testSimpleCountQueryTimeout() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final Statement statement = conn.createStatement()) {
                statement.execute("select count(*) from t1 where s = 's10'");
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
            }
        });
    }

    @Test
    public void testSimpleGroupByQueryTimeout() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final Statement statement = conn.createStatement()) {
                statement.execute("select s, count(*) from t1 group by s ");
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
            }
        });
    }

    @Test
    public void testSimpleModeNoCommit() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (ignore, binary, mode, port) -> {
            for (int i = 0; i < 50; i++) {
                try (final Connection connection = getConnection(mode, port, binary)) {

                    connection.setAutoCommit(false);
                    connection.prepareStatement("create table if not exists xyz(a int)").execute();
                    connection.prepareStatement("insert into xyz values (100)").execute();
                    connection.prepareStatement("insert into xyz values (101)").execute();
                    connection.prepareStatement("insert into xyz values (102)").execute();
                    connection.prepareStatement("insert into xyz values (103)").execute();
                    connection.rollback();

                    sink.clear();
                    try (
                            PreparedStatement ps = connection.prepareStatement("xyz");
                            ResultSet rs = ps.executeQuery()
                    ) {
                        assertResultSet(
                                "a[INTEGER]\n",
                                sink,
                                rs
                        );
                    }
                    // The next iteration of the loop will create a new connection which may be in a different thread than the current
                    // connection
                    // The new connection will execute a "create table if not exists " statement which requires a full table lock
                    // This connection has just execute a read query on the table and hence has a temporary read lock which will be
                    // released shortly after we receive the query response
                    // In order to guarantee that the temporary read lock is released before the next iteration of this loop we execute
                    // a new query, with this connection, which does not lock the table.
                    connection.prepareStatement("select 1").execute();
                }
            }
        });
    }

    @Test
    public void testSimpleQueryLoopThenSchemaChangeThenExtendedQuery() throws Exception {
        // This is a regression test. The bug scenario occurred as follows:
        // 1. A client using a simple protocol poisoned a query cache. This was due to a bug where a simple query would
        //    never poll() from the cache, but would populate it after completion. As a result, each query execution
        //    added a new entry to the cache.
        // 2. A schema change invalidated all cached queries. This is expected.
        // 3. A client using extended protocol then attempted to execute the same query. Upon receiving the PARSE message,
        //    it consulted the query cache and found a stale query plan. This triggered a retry, but subsequent retries
        //    also failed because they consulted the cache and found other stale plans.

        selectCacheBlockCount = 100; // large cache, must be larger than 'cairo.sql.max.recompile.attempts'
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                // first poison the cache using the SIMPLE protocol
                try (
                        final Connection connection = getConnection(server.getPort(), true, false);
                        final Statement statement = connection.createStatement()
                ) {
                    statement.execute("create table tab(ts timestamp, value double)");
                    for (int i = 0; i < selectCacheBlockCount; i++) {
                        statement.execute("select * from tab"); // an attempt to populate cache
                    }

                    // change the schema. if the previous SELECT queries are cached then they are all stale by now
                    statement.execute("alter table tab add column x int");
                }

                // now run a query with an extended protocol - this consults query cache
                try (
                        final Connection connection = getConnection(server.getPort(), false, false);
                        final Statement statement = connection.createStatement()
                ) {
                    statement.execute("select * from tab;");
                }
            }
        });
    }

    @Test
    public void testSimpleSimpleQuery() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "create table x as (select " +
                                "rnd_str(4,4,4) s, " +
                                "rnd_int(0, 256, 4) i, " +
                                "rnd_double(4) d, " +
                                "timestamp_sequence(0,10000) t, " +
                                "rnd_float(4) f, " +
                                "rnd_short() _short, " +
                                "rnd_long(0, 10000000, 5) l, " +
                                "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                                "rnd_byte(0,127) bb, " +
                                "rnd_boolean() b, " +
                                "rnd_symbol(4,4,4,2), " +
                                "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                                "rnd_bin(10,20,2) " +
                                "from long_sequence(50))");
                ResultSet rs = statement.executeQuery("select * from x");

                final String expected = """
                        s[VARCHAR],i[INTEGER],d[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY]
                        null,57,0.6254021542412018,1970-01-01 00:00:00.0,0.46218354,-1593,3425232,null,121,false,PEHN,2015-03-17 04:25:52.765,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e
                        XYSB,142,0.5793466326862211,1970-01-01 00:00:00.01,0.9687423,20088,1517490,2015-01-17 20:41:19.480685,100,true,PEHN,2015-06-20 01:10:58.599,00000000 79 5f 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb
                        00000010 64
                        OZZV,219,0.16381374773748514,1970-01-01 00:00:00.02,0.65903413,-12303,9489508,2015-08-13 17:10:19.752521,6,false,null,2015-05-20 01:48:37.418,00000000 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e
                        OLYX,30,0.7133910271555843,1970-01-01 00:00:00.03,0.65513355,6610,6504428,2015-08-08 00:42:24.545639,123,false,null,2015-01-03 13:53:03.165,null
                        TIQB,42,0.6806873134626418,1970-01-01 00:00:00.04,0.625966,-1605,8814086,2015-07-28 15:08:53.462495,28,true,CPSW,null,00000000 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                        LTOV,137,0.7632615004324503,1970-01-01 00:00:00.05,0.88169044,9054,null,2015-04-20 05:09:03.580574,106,false,PEHN,2015-01-09 06:57:17.512,null
                        ZIMN,125,null,1970-01-01 00:00:00.06,null,11524,8335261,2015-10-26 02:10:50.688394,111,true,PEHN,2015-08-21 15:46:32.624,null
                        OPJO,168,0.10459352312331183,1970-01-01 00:00:00.07,0.5346019,-5920,7080704,2015-07-11 09:15:38.342717,103,false,VTJW,null,null
                        GLUO,145,0.5391626621794673,1970-01-01 00:00:00.08,0.76681465,14242,2499922,2015-11-02 09:01:31.312804,84,false,PEHN,2015-11-14 17:37:36.043,null
                        ZVQE,103,0.6729405590773638,1970-01-01 00:00:00.09,null,13727,7875846,2015-12-12 13:16:26.134562,22,true,PEHN,2015-01-20 04:50:34.098,00000000 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c
                        00000010 65 ff
                        LIGY,199,0.2836347139481469,1970-01-01 00:00:00.1,null,30426,3215562,2015-08-21 14:55:07.055722,11,false,VTJW,null,00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12
                        MQNT,43,0.5859332388599638,1970-01-01 00:00:00.11,0.33504146,27019,null,null,27,true,PEHN,2015-07-12 12:59:47.665,00000000 26 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57
                        00000010 ff 9a ef
                        WWCC,213,0.7665029914376952,1970-01-01 00:00:00.12,0.57967216,13640,4121923,2015-08-06 02:27:30.469762,73,false,PEHN,2015-04-30 08:18:10.453,00000000 71 a7 d5 af 11 96 37 08 dd 98 ef 54 88 2a a2 ad
                        00000010 e7 d4
                        VFGP,120,0.8402964708129546,1970-01-01 00:00:00.13,0.773223,7223,7241423,2015-12-18 07:32:18.456025,43,false,VTJW,null,00000000 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15 e7 b8 35
                        00000010 67
                        RMDG,134,0.11047315214793696,1970-01-01 00:00:00.14,0.04321289,21227,7155708,2015-07-03 04:12:45.774281,42,true,CPSW,2015-02-24 12:10:43.199,null
                        WFOQ,255,null,1970-01-01 00:00:00.15,0.11624247,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,2015-12-09 09:57:17.078,null
                        MXDK,56,0.9997797234031688,1970-01-01 00:00:00.16,0.52348924,-32372,6884132,null,58,false,null,2015-01-20 06:18:18.583,null
                        XMKJ,139,0.8405815493567417,1970-01-01 00:00:00.17,0.3058008,25856,null,2015-05-18 03:50:22.731437,2,true,VTJW,2015-06-25 10:45:01.014,00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35
                        VIHD,null,null,1970-01-01 00:00:00.18,0.55011326,22280,9109842,2015-01-25 13:51:38.270583,94,false,CPSW,2015-10-27 02:52:19.935,00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9
                        WPNX,null,0.9469700813926907,1970-01-01 00:00:00.19,0.4149661,-17933,674261,2015-03-04 15:43:15.213686,43,true,HYRX,2015-12-18 21:28:25.325,00000000 b3 4c 0e 8f f1 0c c5 60 b7 d1
                        YPOV,36,0.6741248448728824,1970-01-01 00:00:00.2,0.030997396,-5888,1375423,2015-12-10 20:50:35.866614,3,true,null,2015-07-23 20:17:04.236,00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d
                        NUHN,null,0.6940917925148332,1970-01-01 00:00:00.21,0.33924818,-25226,3524748,2015-05-07 04:07:18.152968,39,true,VTJW,2015-04-04 15:23:34.13,00000000 b8 be f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0
                        00000010 35 d8
                        BOSE,240,0.06001827721556019,1970-01-01 00:00:00.22,0.3787927,23904,9069339,2015-03-21 03:42:42.643186,84,true,null,null,null
                        INKG,124,0.8615841627702753,1970-01-01 00:00:00.23,0.4041016,-30383,7233542,2015-07-21 16:42:47.012148,99,false,null,2015-08-27 17:25:35.308,00000000 87 fc 92 83 fc 88 f3 32 27 70 c8 01 b0 dc c9 3a
                        00000010 5b 7e
                        FUXC,52,0.7430101994511517,1970-01-01 00:00:00.24,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,2015-08-29 20:15:51.835,null
                        UNYQ,71,0.442095410281938,1970-01-01 00:00:00.25,0.53945625,-22611,null,2015-12-23 18:41:42.319859,98,true,PEHN,2015-01-26 00:55:50.202,00000000 28 ed 97 99 d8 77 33 3f b2 67 da 98 47 47 bf
                        KBMQ,null,0.28019218825051395,1970-01-01 00:00:00.26,null,12240,null,2015-08-16 01:02:55.766622,21,false,null,2015-05-19 00:47:18.698,00000000 6a de 46 04 d3 81 e7 a2 16 22 35 3b 1c
                        JSOL,243,null,1970-01-01 00:00:00.27,0.06820166,-17468,null,null,20,true,null,2015-06-19 10:38:54.483,00000000 3d e0 2d 04 86 e7 ca 29 98 07 69 ca 5b d6 cf 09
                        00000010 69
                        HNSS,150,null,1970-01-01 00:00:00.28,0.14756352,14841,5992443,null,25,false,PEHN,null,00000000 14 d6 fc ee 03 22 81 b8 06 c4 06 af
                        PZPB,101,0.061646717786158045,1970-01-01 00:00:00.29,null,12237,9878179,2015-09-03 22:13:18.852465,79,false,VTJW,2015-12-17 15:12:54.958,00000000 12 61 3a 9a ad 98 2e 75 52 ad 62 87 88 45 b9 9d
                        OYNN,25,0.3393509514000247,1970-01-01 00:00:00.3,0.62812525,22412,4736378,2015-10-10 12:19:42.528224,106,true,CPSW,2015-07-01 00:23:49.789,00000000 54 13 3f ff b6 7e cd 04 27 66 94 89 db
                        null,117,0.5638404775663161,1970-01-01 00:00:00.31,null,-5604,6353018,null,84,false,null,null,00000000 2b ad 25 07 db 62 44 33 6e 00 8e
                        HVRI,233,0.22407665790705777,1970-01-01 00:00:00.32,0.4246651,10469,1715213,null,86,false,null,2015-02-02 05:48:17.373,null
                        OYTO,96,0.7407581616916364,1970-01-01 00:00:00.33,0.52777666,-12239,3499620,2015-02-07 22:35:03.212268,17,false,PEHN,2015-03-29 12:55:11.682,null
                        LFCY,63,0.7217315729790722,1970-01-01 00:00:00.34,null,23344,9523982,null,123,false,CPSW,2015-05-18 04:35:27.228,00000000 05 e5 c0 4e cc d6 e3 7b 34 cd 15 35 bb a4
                        GHLX,148,0.3057937704964272,1970-01-01 00:00:00.35,0.635559,-31457,2322337,2015-10-22 12:06:05.544701,91,true,HYRX,2015-05-21 09:33:18.158,00000000 57 1d 91 72 30 04 b7 02 cb 03
                        YTSZ,123,null,1970-01-01 00:00:00.36,0.51918846,22534,4446236,2015-07-27 07:23:37.233711,53,false,CPSW,2015-01-13 04:37:10.036,null
                        SWLU,251,null,1970-01-01 00:00:00.37,0.17904758,7734,4082475,2015-10-21 18:24:34.400345,69,false,PEHN,2015-04-01 14:33:42.005,null
                        TQJL,245,null,1970-01-01 00:00:00.38,0.8645536,9516,929340,2015-05-28 04:18:18.640567,69,false,VTJW,2015-06-12 20:12:28.881,00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47
                        REIJ,94,null,1970-01-01 00:00:00.39,0.13027799,-29924,null,2015-03-20 22:14:46.204718,113,true,HYRX,2015-12-19 13:58:41.819,null
                        HDHQ,94,0.7234181773407536,1970-01-01 00:00:00.4,0.72973335,19970,654131,2015-01-10 22:56:08.48045,84,true,null,2015-03-05 17:14:48.275,00000000 4f 56 6b 65 a4 53 38 e9 cd c1 a7 ee 86 75 ad a5
                        00000010 2d 49
                        UMEU,40,0.008444033230580739,1970-01-01 00:00:00.41,0.80527276,-11623,4599862,2015-11-20 04:02:44.335947,76,false,PEHN,2015-05-17 17:33:20.922,null
                        YJIH,184,null,1970-01-01 00:00:00.42,0.38269663,17614,3101671,2015-01-28 12:05:46.683001,105,true,null,2015-12-07 19:24:36.838,00000000 ec 69 cd 73 bb 9b c5 95 db 61 91 ce
                        CYXG,27,0.2917796053045747,1970-01-01 00:00:00.43,0.9529176,3944,249165,null,67,true,null,2015-03-02 08:19:44.566,00000000 01 48 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29
                        MRTG,143,0.02632531361499113,1970-01-01 00:00:00.44,0.9425658,-27320,1667842,2015-01-24 19:56:15.973109,11,false,null,2015-01-24 07:15:02.772,null
                        DONP,246,0.654226248740447,1970-01-01 00:00:00.45,0.5557617,27477,4160018,2015-12-14 03:40:05.911839,20,true,PEHN,2015-10-29 14:35:10.167,00000000 07 92 01 f5 6a a1 31 cd cb c2 a2 b4 8e 99
                        IQXS,232,0.23075700218038853,1970-01-01 00:00:00.46,0.048540235,-18113,4005228,2015-06-11 13:00:07.248188,8,true,CPSW,2015-08-16 11:09:24.311,00000000 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00
                        null,178,null,1970-01-01 00:00:00.47,0.903052,-14626,2934570,2015-04-04 08:51:54.068154,88,true,null,2015-07-01 04:32:23.083,00000000 84 36 25 63 2b 63 61 43 1c 47 7d b6 46 ba bb 98
                        00000010 ca 08 be a4
                        HUWZ,94,0.110401374979613,1970-01-01 00:00:00.48,0.42038977,-3736,5687514,2015-01-02 17:18:05.627633,74,false,null,2015-03-29 06:39:11.642,null
                        SRED,66,0.11274667140915928,1970-01-01 00:00:00.49,0.059869826,-10543,3669377,2015-10-22 02:53:02.381351,77,true,PEHN,null,00000000 7c 3f d6 88 3a 93 ef 24 a5 e2 bc
                        """;

                // dump metadata
                assertResultSet(expected, sink, rs);
            }
        });
    }

    @Test
    public void testSimpleVarcharInsert() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement tbl = connection.prepareStatement("create table x as (" +
                    "select " +
                    "rnd_varchar('A','B','C') v, " +
                    "rnd_str('A','B','C') s, " +
                    "from long_sequence(5)" +
                    ")");
            tbl.execute();

            PreparedStatement insert2 = connection.prepareStatement("insert into x(v,s) values ('F','F'),('G','G'),('H','H')");
            insert2.execute();

            PreparedStatement stmnt = connection.prepareStatement("select * from x");
            ResultSet rs = stmnt.executeQuery();

            final String expected = """
                    v[VARCHAR],s[VARCHAR]
                    A,A
                    B,C
                    C,C
                    C,B
                    A,B
                    F,F
                    G,G
                    H,H
                    """;
            assertResultSet(expected, sink, rs);
        });
    }

    @Test
    public void testSingleInClause() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(createDatesTblStmt)) {
                statement.execute();
            }

            mayDrainWalQueue();

            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .filter(arr -> (long) arr[0] < Micros.HOUR_MICROS * 24)
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NOT IN
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts not in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .filter(arr -> (long) arr[0] >= Micros.HOUR_MICROS * 24)
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // IN NULL
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts in ?")) {
                sink.clear();
                statement.setString(1, null);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = "";
                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NOT IN NULL
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts not in ?")) {
                sink.clear();
                statement.setString(1, null);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NULL in not null
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE cast(NULL as TIMESTAMP) in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = "";
                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            if (isEnabledForWalRun()) {
                try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                    statement.execute();
                }
            }
        });
    }

    @Test
    public void testSingleInClauseNonDedicatedTimestamp() throws Exception {
        skipOnWalRun(); // non-partitioned table
        // this test fails in simple mode for the new PG driver
        // The driver use to send `timestamp in '2020'` and now it sends
        // `timestamp in ('2020')`. We interpret the latter as "points" rather than intervals.
        // The fix would be to treat `in ('2020') as interval list
        assertWithPgServer(CONN_AWARE_ALL ^ CONN_AWARE_SIMPLE, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "create table xts as (select timestamp_sequence(0, 3600L * 1000 * 1000) ts from long_sequence(" + count + "))")) {
                statement.execute();
            }

            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .filter(arr -> (long) arr[0] < Micros.HOUR_MICROS * 24)
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NOT IN
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts not in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .filter(arr -> (long) arr[0] >= Micros.HOUR_MICROS * 24)
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // IN NULL
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts in ?")) {
                sink.clear();
                statement.setString(1, null);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = "";
                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NOT IN NULL
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE ts not in ?")) {
                sink.clear();
                statement.setString(1, null);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = datesArr.stream()
                            .map(arr -> arr[1] + "\n")
                            .collect(Collectors.joining());

                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            // NULL in not null
            try (PreparedStatement statement = connection.prepareStatement("select ts FROM xts WHERE cast(NULL as TIMESTAMP) in ?")) {
                sink.clear();
                String date = "1970-01-01";
                statement.setString(1, date);
                statement.executeQuery();
                try (ResultSet rs = statement.executeQuery()) {
                    String expected = "";
                    assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
                }
            }

            try (PreparedStatement statement = connection.prepareStatement("drop table xts")) {
                statement.execute();
            }
        });

    }

    @Test
    public void testSlowClient() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            DelayingNetworkFacade nf = new DelayingNetworkFacade();
            PGConfiguration configuration = new Port0PGConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }

                @Override
                public int getSendBufferSize() {
                    return 1024;
                }
            };
            try (
                    final PGServer server = createPGServer(configuration);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true);
                        Statement statement = connection.createStatement()
                ) {
                    String sql = "SELECT * FROM long_sequence(100) x";

                    nf.startDelaying();

                    boolean hasResultSet = statement.execute(sql);
                    // Temporary log showing a value of hasResultSet, as it is currently impossible to stop the server and complete the test.
                    LOG.info().$("hasResultSet=").$(hasResultSet).$();
                    Assert.assertTrue(hasResultSet);
                }
            }
        });
    }

    @Test
    public void testSlowClient2() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            DelayingNetworkFacade nf = new DelayingNetworkFacade();
            PGConfiguration configuration = new Port0PGConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };
            try (
                    final PGServer server = createPGServer(configuration);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (
                        Connection connection = getConnection(Mode.EXTENDED, port, true);
                        Statement statement = connection.createStatement()
                ) {
                    statement.executeUpdate("CREATE TABLE sensors (ID LONG, make STRING, city STRING)");
                    statement.executeUpdate("""
                            INSERT INTO sensors
                                SELECT
                                    x ID,\s
                                    rnd_str('Eberle', 'Honeywell', 'Omron', 'United Automation', 'RS Pro') make,
                                    rnd_str('New York', 'Miami', 'Boston', 'Chicago', 'San Francisco') city
                                FROM long_sequence(10000) x""");
                    statement.executeUpdate("""
                            CREATE TABLE readings
                            AS(
                                SELECT
                                    x ID,
                                    timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), rnd_long(1,10,0) * 100000L) ts,
                                    rnd_double(0)*8 + 15 temp,
                                    rnd_long(0, 10000, 0) sensorId
                                FROM long_sequence(10000) x)
                            TIMESTAMP(ts)
                            PARTITION BY MONTH""");

                    String sql = """
                            SELECT *
                            FROM readings
                            JOIN(
                                SELECT ID sensId, make, city
                                FROM sensors)
                            ON readings.sensorId = sensId""";

                    nf.startDelaying();

                    boolean hasResultSet = statement.execute(sql);
                    // Temporary log showing a value of hasResultSet, as it is currently impossible to stop the server and complete the test.
                    LOG.info().$("hasResultSet=").$(hasResultSet).$();
                    Assert.assertTrue(hasResultSet);
                }
            }
        });
    }

    @Test
    public void testSmallSendBufferBigColumnValueNotEnoughSpace1() throws Exception {
        final int sndBufSize = 256 + bufferSizeRnd.nextInt(256);

        // varchar, string, binary
        int[] sizes = {sndBufSize / 4, sndBufSize / 2, sndBufSize - 4 - 1};
        sizes[bufferSizeRnd.nextInt(sizes.length)] = 2 * sndBufSize;

        final int varcharSize = sizes[0];
        final int stringSize = sizes[1];
        final int binarySize = sizes[2];

        assertWithPgServerExtendedBinaryOnly(
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table x as (" +
                                "select" +
                                " rnd_boolean() f1," +
                                " rnd_str(" + stringSize + "," + stringSize + ",2) s1," +
                                " rnd_varchar(" + varcharSize + "," + varcharSize + ",2) v1," +
                                " rnd_bin(" + binarySize + "," + binarySize + ",2) b1," +
                                " timestamp_sequence(500000000000L,100000000L) ts" +
                                " from long_sequence(10)" +
                                ") timestamp (ts) partition by DAY");

                        mayDrainWalQueue();

                        String sql = "SELECT * FROM x";
                        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                            try (ResultSet ignore = stmt.executeQuery()) {
                                Assert.fail("exception expected");
                            }
                        } catch (SQLException e) {
                            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
                        }
                    }
                },
                () -> sendBufferSize = sndBufSize
        );
    }

    @Test
    public void testSmallSendBufferBigColumnValueNotEnoughSpace2() throws Exception {
        Assume.assumeFalse(walEnabled);

        final int varcharSize = 600;

        final String ddl = "create table x as (" +
                "select " +
                "  rnd_boolean() f1," +
                "  rnd_byte(1,10) f2," +
                "  rnd_short(1,10) f3," +
                "  rnd_char() f4," +
                "  rnd_int(1,10,2) f5," +
                "  rnd_long(1,10,2) f6," +
                "  rnd_float(2) f7," +
                "  rnd_double(2) f8," +
                "  rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) f9," +
                "  to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 f10," +
                "  rnd_uuid4(2) f11," +
                "  rnd_geohash(4) f12," +
                "  rnd_geohash(8) f13," +
                "  rnd_geohash(16) f14," +
                "  rnd_geohash(32) f15," +
                "  rnd_ipv4() f16," +
                "  rnd_long256() f17," +
                "  rnd_symbol(4,4,4,2) f18," +
                "  rnd_str(10,10,0) f19," +
                "  rnd_bin(16,16,0) f20," +
                "  rnd_varchar(" + varcharSize + "," + varcharSize + ",2) f21," +
                "  timestamp_sequence(500000000000L,100000000L) ts " +
                "from long_sequence(1)" +
                ") timestamp (ts) partition by DAY";

        // We need to be in full control of binary/text format since the buffer size depends on that,
        // so we run just a few combinations.
        assertWithPgServer(
                Mode.SIMPLE,
                false,
                -1,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(ddl);

                        try (PreparedStatement stmt = connection.prepareStatement("x")) {
                            try (ResultSet ignore = stmt.executeQuery()) {
                                Assert.fail("exception expected");
                            }
                        } catch (SQLException e) {
                            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer [sendBufferSize=512, requiredSize=1788]");
                        }
                    }
                },
                () -> {
                    recvBufferSize = 1024;
                    sendBufferSize = 512;
                }
        );

        assertWithPgServerExtendedBinaryOnly(
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(ddl);

                        try (PreparedStatement stmt = connection.prepareStatement("x")) {
                            try (ResultSet ignore = stmt.executeQuery()) {
                                Assert.fail("exception expected");
                            }
                        } catch (SQLException e) {
                            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer [sendBufferSize=512, requiredSize=1629]");
                        }
                    }
                },
                () -> {
                    recvBufferSize = 1024;
                    sendBufferSize = 512;
                }
        );
    }

    @Test
    public void testSmallSendBufferForRowData() throws Exception {
        // WAL is irrelevant here, we are checking result sending code path
        skipOnWalRun();
        assertWithPgServer(
                CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate("create table x as (" +
                                "select" +
                                " rnd_str(5,16,2) i," +
                                " rnd_str(5,16,2) sym," +
                                " rnd_str(5,16,2) amt," +
                                " rnd_str(5,16,2) timestamp," +
                                " rnd_str(5,16,2) b," +
                                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                                " rnd_str(5,16,2) d," +
                                " rnd_str(5,16,2) e," +
                                " rnd_str(450,450,0) f," + // <-- really long string
                                " rnd_str(5,16,2) g," +
                                " rnd_str(5,16,2) ik," +
                                " rnd_str(5,16,2) j," +
                                " timestamp_sequence(500000000000L,100000000L) ts," +
                                " rnd_str(5,16,2) l," +
                                " rnd_str(5,16,2) m," +
                                " rnd_str(5,16,2) n," +
                                " rnd_str(5,16,2) t," +
                                " rnd_str(5,16,2) l256" +
                                " from long_sequence(10000)" +
                                ") timestamp (ts) partition by DAY");

                        mayDrainWalQueue();

                        String sql = "SELECT * FROM x";
                        try {
                            statement.execute(sql);
                            Assert.fail();
                        } catch (SQLException e) {
                            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
                        }
                    }
                },
                () -> sendBufferSize = 512
        );
    }

    @Test
    public void testSmallSendBufferForRowDescription() throws Exception {
        // WAL is irrelevant here, we are checking result sending code path
        skipOnWalRun();
        String sqlCreate = "create table x as (" +
                "select" +
                " rnd_str(5,16,2) i," +
                " rnd_str(5,16,2) sym," +
                " rnd_str(5,16,2) amt," +
                " rnd_str(5,16,2) timestamp," +
                " rnd_str(5,16,2) b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_str(5,16,2) d," +
                " rnd_str(5,16,2) e," +
                " rnd_str(5,16,2) f," +
                " rnd_str(5,16,2) g," +
                " rnd_str(5,16,2) ik," +
                " rnd_str(5,16,2) j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_str(5,16,2) l," +
                " rnd_str(5,16,2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_str(5,16,2) t," +
                " rnd_str(5,16,2) l256" +
                " from long_sequence(100)" +
                ") timestamp (ts) partition by DAY";
        String sql = "SELECT * FROM x";
        // binary encoding only. row description message and record should be able to send in chunks
        assertWithPgServerExtendedBinaryOnly(
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(sqlCreate);
                        try {
                            statement.execute(sql);
                        } catch (SQLException e) {
                            Assert.fail();
                        }
                    }
                },
                () -> sendBufferSize = 256
        );

        // row description message should be sent but record should not
        assertWithPgServer(
                CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS,
                (connection, binary, mode, port) -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeUpdate(sqlCreate);
                        try {
                            statement.execute(sql);
                            Assert.fail();
                        } catch (SQLException e) {
                            TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
                        }
                    }
                },
                () -> sendBufferSize = 256
        );
    }

    @Test
    public void testSmallSendBufferLargeErrorMessage() throws Exception {
        Assume.assumeFalse(walEnabled);

        final int sndBufSize = 256;
        final int errorLen = sndBufSize + bufferSizeRnd.nextInt(1000);

        // We need to be in full control of binary/text format since the buffer size depends on that,
        // so we run just a few combinations.
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    // error message overflows the buffer
                    try (Statement ignore1 = connection.createStatement()) {
                        try (PreparedStatement stmt = connection.prepareStatement("select large_error_message(" + errorLen + ") from long_sequence(1);")) {
                            try (ResultSet ignore2 = stmt.executeQuery()) {
                                Assert.fail("exception expected");
                            }
                        } catch (SQLException e) {
                            sink.clear();
                            sink.repeat("e", 216);
                            sink.put("...");
                            TestUtils.assertContains(e.getMessage(), sink);
                        }
                    }

                    // error message fits into the buffer
                    try (Statement ignore1 = connection.createStatement()) {
                        try (PreparedStatement stmt = connection.prepareStatement("select large_error_message(" + (sendBufferSize / 2) + ") from long_sequence(1);")) {
                            try (ResultSet ignore2 = stmt.executeQuery()) {
                                Assert.fail("exception expected");
                            }
                        } catch (SQLException e) {
                            sink.clear();
                            sink.repeat("e", sendBufferSize / 2);
                            TestUtils.assertContains(e.getMessage(), sink);
                        }
                    }
                },
                () -> sendBufferSize = sndBufSize
        );
    }

    @Test
    public void testSmallSendBufferWideRecordPermute() throws Exception {
        // WAL is irrelevant here, we are checking result sending code path
        skipOnWalRun();

        // 256 is not enough for the row description message
        final int[] bufferSizes = {512, 1024, 2048, 4096, 8192};
        final int numIteration = 10;
        final int numRow = 10;

        List<String> fixed = new ArrayList<>(Arrays.asList(
                "rnd_boolean() f1",
                "rnd_byte(1,10) f2",
                "rnd_short(1,10) f3",
                "rnd_char() f4",
                "rnd_int(1,10,2) f5",
                "rnd_long(1,10,2) f6",
                "rnd_float(2) f7",
                "rnd_double(2) f8",
                "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) f9",
                "to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 f10",
                "rnd_uuid4(2) f11",
                "rnd_geohash(4) f12",
                "rnd_geohash(8) f13",
                "rnd_geohash(16) f14",
                "rnd_geohash(32) f15",
                "rnd_ipv4() f16",
                "rnd_long256() f17",
                "timestamp_sequence(500000000000L,100000000L) ts"
        ));

        for (int i = 0; i < numIteration; i++) {
            final int sndBufSize = bufferSizes[bufferSizeRnd.nextInt(bufferSizes.length)];

            final int varcharSize = sndBufSize / 4;
            final int stringSize = sndBufSize / 2;
            final int binarySize = sndBufSize - 4 - 1;

            List<String> permutedColumns = new ArrayList<>(fixed);
            permutedColumns.add("rnd_symbol(4," + stringSize + "," + stringSize + ",2) e1");
            permutedColumns.add("rnd_str(" + stringSize + "," + stringSize + ",0) s1");
            permutedColumns.add("rnd_varchar(" + varcharSize + "," + varcharSize + ",2) v1");
            permutedColumns.add("rnd_bin(" + binarySize + "," + binarySize + ",0) b1");

            Collections.shuffle(permutedColumns);

            String columnsSql = String.join(",", permutedColumns);
            String createSql = "create table x as ( " +
                    "select " + columnsSql + " " +
                    "from long_sequence(" + numRow + ") " +
                    ") timestamp (ts) partition by DAY";

            String selectSql = "SELECT * FROM x";
            assertWithPgServerExtendedBinaryOnly(
                    (connection, binary, mode, port) -> {
                        try (Statement statement = connection.createStatement()) {
                            statement.execute(createSql);
                            mayDrainWalQueue();
                        }

                        try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
                            try (ResultSet rs = stmt.executeQuery()) {
                                long rows = 0;
                                while (rs.next()) {
                                    rows++;
                                }
                                // no need to assert generated random junk
                                Assert.assertEquals(numRow, rows);
                            }
                        }
                    },
                    () -> {
                        recvBufferSize = 1024; // big enough for a wide create table sql
                        sendBufferSize = sndBufSize; // enough space for describe message
                    }
            );
        }
    }

    @Test
    public void testSqlBatchTimeout() throws Exception {
        maxQueryTime = TIMEOUT_FAIL_ON_FIRST_CHECK;
        // Exclude quirks, because they send P(arse) message for all SQL statements in the script
        // and only then (E)xecute them. This means at the time when it's parsing 'select count(*) from tab;'
        // the 'tab' table does not exist yet. because the CREATE TABLE was not yet (E)xecuted.
        assertWithPgServer(CONN_AWARE_ALL & ~CONN_AWARE_QUIRKS, (connection, binary, mode, port) -> {
            try (final Statement statement = connection.createStatement()) {
                statement.execute("create table tab (d double);" +
                        "select count(*) from tab;" +
                        "select * from (select * from long_sequence(1000) order by x desc) ");
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "timeout, query aborted ");
            }
        });
    }

    @Test
    public void testSquashPartitionsReturnsOk() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                    connection.setAutoCommit(false);
                    connection.prepareStatement("CREATE TABLE x (l LONG, ts TIMESTAMP, date DATE) TIMESTAMP(ts) PARTITION BY WEEK").execute();
                    connection.prepareStatement("INSERT INTO x VALUES (12, '2023-02-11T11:12:22.116234Z', '2023-02-11'::date)").execute();
                    connection.prepareStatement("INSERT INTO x VALUES (13, '2023-02-12T16:42:00.333999Z', '2023-02-12'::date)").execute();
                    connection.prepareStatement("INSERT INTO x VALUES (14, '2023-03-21T03:52:00.999999Z', '2023-03-21'::date)").execute();
                    connection.commit();
                    mayDrainWalQueue();
                    try (PreparedStatement dropPartition = connection.prepareStatement("ALTER TABLE x SQUASH partitions;")) {
                        Assert.assertFalse(dropPartition.execute());
                    }
                }
        );
    }

    @Test
    public void testStringBindvarEqStringyCol() throws Exception {
        testVarcharBindVars("select v,s from x where ? != v and ? != s");
    }

    @Test
    public void testStringyColEqStringBindvar() throws Exception {
        testVarcharBindVars("select v,s from x where v != ? and s != ?");
    }

    @Test
    public void testStringyColEqVarcharBindvar() throws Exception {
        testVarcharBindVars(
                "select v,s from x where v != ?::varchar and s != ?::varchar");
    }

    @Test
    public void testSymbolBindVariableInFilter() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary1, mode, port) -> {
            // create and initialize table outside of PG wire
            // to ensure we do not collaterally initialize execution context on function parser
            execute("""
                    CREATE TABLE x (
                        ticker symbol index,
                        sample_time timestamp,
                        value int
                    ) timestamp (sample_time) partition by YEAR""");
            execute("INSERT INTO x VALUES ('ABC',0,0)");
            mayDrainWalQueue();

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("select * from x where ticker=?")) {
                ps.setString(1, "ABC");
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            """
                                    ticker[VARCHAR],sample_time[TIMESTAMP],value[INTEGER]
                                    ABC,1970-01-01 00:00:00.0,0
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    /*
        use sqlx::postgres::{PgPoolOptions};

        #[tokio::main]
        async fn main() -> anyhow::Result<()> {

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect("postgres://admin:quest@localhost:8812/qdb")
                .await?;

            let result = sqlx::query("SELECT $1 from long_sequence(2)")
                .bind(1)
                .execute(&pool).await?;


            assert_eq!(result.rows_affected(), 2);

            Ok(())
        }

        [dependencies]
        tokio = { version = "1", features = ["full"] }
        anyhow = "1.0.89"
        sqlx = { version = "0.7", features = [ "postgres", "runtime-tokio" ] }
     */
    public void testSyncAfterLoginSendsRNQ() throws Exception {
        skipOnWalRun();
        final String script =
                """
                        >0000006b00030000757365720061646d696e0064617461626173650071646200446174655374796c650049534f2c204d445900636c69656e745f656e636f64696e6700555446380054696d655a6f6e65005554430065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5300000004
                        <5a0000000549
                        >500000003373716c785f735f310053454c4543542024312066726f6d206c6f6e675f73657175656e636528322900000100000017440000000e5373716c785f735f31005300000004
                        <3100000004740000000a000100000017540000001b0001243100000000000001000000170004ffffffff00005a0000000549
                        >42000000200073716c785f735f310000010001000100000004000000010001000145000000090000000000430000000650005300000004
                        <3200000004440000000e00010000000400000001440000000e00010000000400000001430000000d53454c45435420320033000000045a0000000549
                        """;
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, new Port0PGConfiguration());
    }

    @Test
    public void testSyntaxErrorReporting() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try {
                connection.prepareCall("drop table xyz;").execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "table does not exist [table=xyz]");
                TestUtils.assertEquals("00000", e.getSQLState());
            }
        });
    }

    @Test
    public void testSyntaxErrorSimple() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try {
                // column does not exits
                connection.prepareStatement("select x2 from long_sequence(5)").execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "Invalid column: x2");
                TestUtils.assertEquals("00000", e.getSQLState());
            }
        });
    }

    @Test
    public void testTableReferenceOutOfDate() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final String query = "select * from test_table_reference_out_of_date();";
            try (
                    PreparedStatement stmt = connection.prepareStatement(query);
                    ResultSet ignore = stmt.executeQuery()
            ) {
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "cached query plan cannot be used because table schema has changed");
            }
        });
    }

    // TODO(puzpuzpuz): fix schema changes handling in PGWire for extended protocol
    //                  https://github.com/questdb/questdb/issues/4971
    @Ignore
    @Test
    public void testTableSchemaChangeExtended() throws Exception {
        testTableSchemaChange(false);
    }

    @Test
    public void testTableSchemaChangeSimple() throws Exception {
        testTableSchemaChange(true);
    }

    /*
    We want to ensure that tableoid is set to zero, otherwise squirrelSql will not display the result set.
     */
    @Test
    public void testThatTableOidIsSetToZero() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (
                    final PreparedStatement statement = connection.prepareStatement("select 1,2,3 from long_sequence(1)");
                    final ResultSet rs = statement.executeQuery()
            ) {
                assertTrue(((PGResultSetMetaData) rs.getMetaData()).getBaseColumnName(1).isEmpty()); // getBaseColumnName returns "" if tableOid is zero
            }
        });
    }

    @Test
    public void testTimeoutIsPerPreparedStatement() throws Exception {
        // what are we testing here? nothing is asserted
        maxQueryTime = 1000;
        assertWithPgServer(CONN_AWARE_ALL, (conn, binary, mode, port) -> {
            execute("create table t1 as (select 's' || x as s from long_sequence(1000));");
            try (final PreparedStatement statement = conn.prepareStatement("insert into t1 select 's' || x from long_sequence(100)")) {
                statement.execute();
            }
            try (final PreparedStatement statement = conn.prepareStatement("insert into t1 select 's' || x from long_sequence(100)")) {
                statement.execute();
            }
        });
    }

    @Test
    public void testTimestamp() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE ts (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
            connection.prepareStatement("INSERT INTO ts VALUES(0, '2021-09-27T16:45:03.202345Z')").execute();
            connection.commit();
            connection.setAutoCommit(true);

            mayDrainWalQueue();

            // select the timestamp that we just inserted
            Timestamp ts;
            try (PreparedStatement statement = connection.prepareStatement("SELECT ts FROM ts")) {
                try (ResultSet rs = statement.executeQuery()) {
                    assertTrue(rs.next());
                    ts = rs.getTimestamp("ts");
                }
            }

            // NOTE: java.sql.Timestamp takes milliseconds from epoch as constructor parameter,
            // which is processed and stored internally coupling ts.getTime() and ts.getNanos():
            //   - ts.getTime(): the last 3 digits account for millisecond precision, e.g. 1632761103202L -> 202 milliseconds.
            //   - ts.getNanos(): the first 3 digits match the last 3 digits from ts.getTime(), then
            //         3 more digits follow for micros, and 3 more for nanos,, e.g. 202345000 -> (202)milli(345)micro(000)nano
            assertEquals(1632761103202L, ts.getTime());
            assertEquals(202345000, ts.getNanos());
            assertEquals("2021-09-27 16:45:03.202345", ts.toString());

            sink.clear();
            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO ts VALUES (?, ?)")) {
                int rowId = 1;

                // Case 1: insert timestamp as we selected it, no modifications
                // -> microsecond precision is kept
                ps.setInt(1, rowId++);
                ps.setTimestamp(2, ts);
                ps.execute();

                // Case 2: we create a timestamp from another, but there is a catch, we must set the nanos too
                // -> microsecond precision is kept
                Timestamp aTs = new Timestamp(ts.getTime());
                aTs.setNanos(ts.getNanos());
                ps.setInt(1, rowId++);
                ps.setTimestamp(2, aTs);
                ps.execute();

                // Case 3: if we forget to setNanos, we get correct timestamp
                // -> this results in a broken timestamp 1970-...
                Timestamp bTs = new Timestamp(ts.getTime());
                ps.setInt(1, rowId++);
                ps.setTimestamp(2, bTs);
                ps.execute();

                // Case 6: where we take QuestDB
                // timestamp WITH microsecond precision, and we massage it to extract two
                // numbers that can be used to create a java.sql.Timestamp.
                // -> microsecond precision is kept
                long questdbTs = MicrosFormatUtils.parseTimestamp("2021-09-27T16:45:03.202345Z");
                long time = questdbTs / 1000;
                int nanos = (int) (questdbTs - (int) (questdbTs / 1e6) * 1e6) * 1000;
                assertEquals(1632761103202345L, questdbTs);
                assertEquals(1632761103202L, time);
                assertEquals(202345000, nanos);
                Timestamp eTs = new Timestamp(time);
                eTs.setNanos(nanos);
                ps.setInt(1, rowId);
                ps.setTimestamp(2, eTs);
                ps.execute();
            }

            mayDrainWalQueue();

            try (PreparedStatement statement = connection.prepareStatement("SELECT id, ts FROM ts ORDER BY id ASC")) {
                sink.clear();
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet(
                            """
                                    id[INTEGER],ts[TIMESTAMP]
                                    0,2021-09-27 16:45:03.202345
                                    1,2021-09-27 16:45:03.202345
                                    2,2021-09-27 16:45:03.202345
                                    3,2021-09-27 16:45:03.202
                                    4,2021-09-27 16:45:03.202345
                                    """,
                            sink,
                            rs
                    );
                }
            }

            if (isEnabledForWalRun()) {
                connection.prepareStatement("drop table ts").execute();
            }
        });
    }

    @Test
    public void testTimestampSentEqualsReceived() throws Exception {
        final Timestamp expectedTs = new Timestamp(1632761103202L); // '2021-09-27T16:45:03.202000Z'
        assertEquals(1632761103202L, expectedTs.getTime());
        assertEquals(202000000, expectedTs.getNanos());
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE ts (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH").execute();
            connection.commit();
            connection.setAutoCommit(true);

            // insert
            final Timestamp ts = Timestamp.valueOf("2021-09-27 16:45:03.202");
            assertEquals(expectedTs.getTime(), ts.getTime());
            assertEquals(expectedTs.getNanos(), ts.getNanos());
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO ts VALUES (?)")) {
                // QuestDB timestamps have MICROSECOND precision and require you to be aware
                // of it if you use java.sql.Timestamp's constructor
                insert.setTimestamp(1, ts);
                insert.execute();
            }

            mayDrainWalQueue();

            // select
            final Timestamp tsBack;
            try (ResultSet queryResult = connection.prepareStatement("SELECT * FROM ts").executeQuery()) {
                queryResult.next();
                tsBack = queryResult.getTimestamp("ts");
            }
            assertEquals(expectedTs.getTime(), tsBack.getTime());
            assertEquals(expectedTs.getNanos(), tsBack.getNanos());
            assertEquals(expectedTs, tsBack);

            // cleanup
            if (isEnabledForWalRun()) {
                connection.prepareStatement("drop table ts").execute();
            }
        });
    }

    @Test
    public void testTransaction() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table xyz(a int)").execute();
            connection.prepareStatement("insert into xyz values (100)").execute();
            connection.prepareStatement("insert into xyz values (101)").execute();
            connection.prepareStatement("insert into xyz values (102)").execute();
            connection.prepareStatement("insert into xyz values (103)").execute();
            connection.commit();

            sink.clear();
            try (
                    PreparedStatement ps = connection.prepareStatement("xyz");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertResultSet(
                        """
                                a[INTEGER]
                                100
                                101
                                102
                                103
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    @Test
    public void testTruncateAndUpdateOnNonPartitionedTableWithDesignatedTs() throws Exception {
        skipOnWalRun(); // non-partitioned table
        testTruncateAndUpdateOnTable("timestamp(ts)");
    }

    @Test
    public void testTruncateAndUpdateOnNonPartitionedTableWithoutDesignatedTs() throws Exception {
        skipOnWalRun(); // non-partitioned table
        testTruncateAndUpdateOnTable("");
    }

    @Test
    public void testTruncateAndUpdateOnPartitionedTable() throws Exception {
        testTruncateAndUpdateOnTable("timestamp(ts) partition by DAY");
    }

    public void testTruncateAndUpdateOnTable(String config) throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement stat = connection.createStatement()) {
                stat.execute("create table tb ( i int, b boolean, ts timestamp ) " + config + ";");
            }

            try (Statement stat = connection.createStatement()) {
                stat.execute("insert into tb values (1, true, now());");
                stat.execute("update tb set i = 10, b = true;");
                stat.execute("truncate table tb;");
                stat.execute("insert into tb values (2, true, cast(0 as timestamp));");
                stat.execute("insert into tb values (1, true, '2022-09-28T17:00:00.000000Z');");
                stat.execute("update tb set i = 12, b = true;");

                mayDrainWalQueue();

                try (ResultSet result = stat.executeQuery("tb")) {
                    sink.clear();
                    assertResultSet("""
                            i[INTEGER],b[BIT],ts[TIMESTAMP]
                            12,true,1970-01-01 00:00:00.0
                            12,true,2022-09-28 17:00:00.0
                            """, sink, result);
                }
            }
        });
    }

    @Test
    public void testUndefinedBindVariableInSymbol() throws Exception {
        final String[] values = {"'5'", "null", "'5' || ''", "replace(null, 'A', 'A')", "?5", "?null"};
        final CharSequenceObjHashMap<String> valMap = new CharSequenceObjHashMap<>();
        valMap.put("5", "5");
        valMap.put("'5'", "5");
        valMap.put("null", "null");
        valMap.put("'5' || ''", "5");
        valMap.put("replace(null, 'A', 'A')", "null");

        final String[] tsOptions = {"", "timestamp(ts)", "timestamp(ts) partition by HOUR"};
        final String strType = ColumnType.nameOf(ColumnType.STRING).toLowerCase();
        for (String tsOption : tsOptions) {
            assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
                execute("drop table if exists tab");
                execute("create table tab (s symbol index, ts timestamp) " + tsOption);
                execute("insert into tab select case when x = 10 then null::" + strType + " else x::" + strType + " end, x::timestamp from long_sequence(10) ");
                drainWalQueue();

                ResultProducer sameValIfParamsTheSame = (paramVals, isBindVals, bindVals, output) -> {
                    String left = isBindVals[0] ? bindVals[0] : paramVals[0];
                    String right = isBindVals[1] ? bindVals[1] : paramVals[1];
                    boolean isSame = valMap.get(left).equals(valMap.get(right));
                    if (isSame) {
                        output.put(valMap.get(left)).put('\n');
                    }
                };
                assertQueryAgainstIndexedSymbol(values, "s in (#X1) and s in (#X2)", new String[]{"#X1", "#X2"}, connection, tsOption, sameValIfParamsTheSame);
            });
        }
    }

    @Test
    public void testUnsupportedParameterType() throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("select x, ? y from long_sequence(5)")) {
                // TIME is passed over protocol as UNSPECIFIED type
                // it will rely on date parser to work out what it is
                // for now date parser does not parse just time, it could i guess if required.
                statement.setTime(1, new Time(100L));

                try (ResultSet rs = statement.executeQuery()) {
                    StringSink sink = new StringSink();
                    // dump metadata
                    assertResultSet(
                            """
                                    x[BIGINT],y[VARCHAR]
                                    1,00:00:00.1+00
                                    2,00:00:00.1+00
                                    3,00:00:00.1+00
                                    4,00:00:00.1+00
                                    5,00:00:00.1+00
                                    """,
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testUpdate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (a long, b double, ts timestamp) timestamp(ts) partition by YEAR");
            statement.execute();

            final PreparedStatement insert1 = connection.prepareStatement("insert into x values " +
                    "(1, 2.0, '2020-06-01T00:00:02'::timestamp)," +
                    "(2, 2.6, '2020-06-01T00:00:06'::timestamp)," +
                    "(5, 3.0, '2020-06-01T00:00:12'::timestamp)");
            insert1.execute();

            final PreparedStatement update1 = connection.prepareStatement("update x set a=9 where b>2.5");
            int numOfRowsUpdated1 = update1.executeUpdate();
            assertEquals(2, numOfRowsUpdated1);

            final PreparedStatement insert2 = connection.prepareStatement("insert into x values " +
                    "(8, 4.0, '2020-06-01T00:00:22'::timestamp)," +
                    "(10, 6.0, '2020-06-01T00:00:32'::timestamp)");
            insert2.execute();

            final PreparedStatement update2 = connection.prepareStatement("update x set a=7 where b>5.0");
            int numOfRowsUpdated2 = update2.executeUpdate();

            if (!walEnabled) {
                assertEquals(1, numOfRowsUpdated2);
            }

            mayDrainWalQueue();

            final String expected = """
                    a[BIGINT],b[DOUBLE],ts[TIMESTAMP]
                    1,2.0,2020-06-01 00:00:02.0
                    9,2.6,2020-06-01 00:00:06.0
                    9,3.0,2020-06-01 00:00:12.0
                    8,4.0,2020-06-01 00:00:22.0
                    7,6.0,2020-06-01 00:00:32.0
                    """;
            try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testUpdateAsync() throws Exception {
        testUpdateAsync(null, writer -> {
                },
                """
                        a[BIGINT],b[DOUBLE],ts[TIMESTAMP]
                        1,2.0,2020-06-01 00:00:02.0
                        9,2.6,2020-06-01 00:00:06.0
                        9,3.0,2020-06-01 00:00:12.0
                        """
        );
    }

    @Test
    public void testUpdateAsyncWithReaderOutOfDateException() throws Exception {
        skipOnWalRun();
        SOCountDownLatch queryScheduledCount = new SOCountDownLatch(1);
        testUpdateAsync(queryScheduledCount, new OnTickAction() {
                    private boolean first = true;

                    @Override
                    public void run(TableWriter writer) {
                        if (first) {
                            queryScheduledCount.await();
                            // adding a new column before calling writer.tick() will result in ReaderOutOfDateException
                            // thrown from UpdateOperator as this changes table structure
                            // recompile should be successful so the UPDATE completes
                            writer.addColumn("newCol", ColumnType.INT);
                            first = false;
                        }
                    }
                },
                """
                        a[BIGINT],b[DOUBLE],ts[TIMESTAMP],newCol[INTEGER]
                        1,2.0,2020-06-01 00:00:02.0,null
                        9,2.6,2020-06-01 00:00:06.0,null
                        9,3.0,2020-06-01 00:00:12.0,null
                        """
        );
    }

    @Test
    public void testUpdateBatch() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            final PreparedStatement statement = connection.prepareStatement("create table x (a long, b double, ts timestamp) timestamp(ts) partition by YEAR");
            statement.execute();

            final PreparedStatement insert1 = connection.prepareStatement("insert into x values " +
                    "(1, 2.0, '2020-06-01T00:00:02'::timestamp)," +
                    "(2, 2.6, '2020-06-01T00:00:06'::timestamp)," +
                    "(5, 3.0, '2020-06-01T00:00:12'::timestamp)");
            insert1.execute();

            final PreparedStatement update1 = connection.prepareStatement("update x set a=9 where b>2.5; update x set a=3 where b>2.7; update x set a=2 where b<2.2");
            int numOfRowsUpdated1 = update1.executeUpdate();

            drainWalQueue();
            assertEquals(2, numOfRowsUpdated1);

            final PreparedStatement insert2 = connection.prepareStatement("insert into x values " +
                    "(8, 4.0, '2020-06-01T00:00:22'::timestamp)," +
                    "(10, 6.0, '2020-06-01T00:00:32'::timestamp)");
            insert2.execute();

            final PreparedStatement update2 = connection.prepareStatement("update x set a=7 where b>5.0; update x set a=6 where a=2");
            int numOfRowsUpdated2 = update2.executeUpdate();
            if (!walEnabled) {
                assertEquals(1, numOfRowsUpdated2);
            }

            mayDrainWalQueue();
            final String expected = """
                    a[BIGINT],b[DOUBLE],ts[TIMESTAMP]
                    6,2.0,2020-06-01 00:00:02.0
                    9,2.6,2020-06-01 00:00:06.0
                    3,3.0,2020-06-01 00:00:12.0
                    8,4.0,2020-06-01 00:00:22.0
                    7,6.0,2020-06-01 00:00:32.0
                    """;
            try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testUpdateNoAutoCommit() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (
                        final Connection connection = getConnection(Mode.EXTENDED, port, true)
                ) {
                    connection.setAutoCommit(false);

                    PreparedStatement tbl = connection.prepareStatement("create table x (a int, b int, ts timestamp) timestamp(ts) partition by YEAR");
                    tbl.execute();

                    PreparedStatement insert = connection.prepareStatement("insert into x values(?, ?, '2022-03-17T00:00:00'::timestamp)");
                    for (int i = 0; i < 10; i++) {
                        insert.setInt(1, i);
                        insert.setInt(2, i + 100);
                        insert.execute();
                    }

//                    "a\tb\tts\n" +
//                    "0\t100\t2022-03-17T00:00:00.000000Z\n" +
//                    "1\t101\t2022-03-17T00:00:00.000000Z\n" +
//                    "2\t102\t2022-03-17T00:00:00.000000Z\n" +
//                    "3\t103\t2022-03-17T00:00:00.000000Z\n" +
//                    "4\t104\t2022-03-17T00:00:00.000000Z\n" +
//                    "5\t105\t2022-03-17T00:00:00.000000Z\n" +
//                    "6\t106\t2022-03-17T00:00:00.000000Z\n" +
//                    "7\t107\t2022-03-17T00:00:00.000000Z\n" +
//                    "8\t108\t2022-03-17T00:00:00.000000Z\n" +
//                    "9\t109\t2022-03-17T00:00:00.000000Z\n"

                    PreparedStatement updateB = connection.prepareStatement("update x set b=? where a=?");
                    for (int i = 0; i < 10; i++) {
                        updateB.setInt(1, i + 10);
                        updateB.setInt(2, i);
                        updateB.execute();
                    }

//                    "a\tb\tts\n" +
//                    "0\t10\t2022-03-17T00:00:00.000000Z\n" +
//                    "1\t11\t2022-03-17T00:00:00.000000Z\n" +
//                    "2\t12\t2022-03-17T00:00:00.000000Z\n" +
//                    "3\t13\t2022-03-17T00:00:00.000000Z\n" +
//                    "4\t14\t2022-03-17T00:00:00.000000Z\n" +
//                    "5\t15\t2022-03-17T00:00:00.000000Z\n" +
//                    "6\t16\t2022-03-17T00:00:00.000000Z\n" +
//                    "7\t17\t2022-03-17T00:00:00.000000Z\n" +
//                    "8\t18\t2022-03-17T00:00:00.000000Z\n" +
//                    "9\t19\t2022-03-17T00:00:00.000000Z\n"

                    for (int i = 10; i < 15; i++) {
                        insert.setInt(1, i);
                        insert.setInt(2, i + 100);
                        insert.execute();
                    }

//                    "a\tb\tts\n" +
//                    "0\t10\t2022-03-17T00:00:00.000000Z\n" +
//                    "1\t11\t2022-03-17T00:00:00.000000Z\n" +
//                    "2\t12\t2022-03-17T00:00:00.000000Z\n" +
//                    "3\t13\t2022-03-17T00:00:00.000000Z\n" +
//                    "4\t14\t2022-03-17T00:00:00.000000Z\n" +
//                    "5\t15\t2022-03-17T00:00:00.000000Z\n" +
//                    "6\t16\t2022-03-17T00:00:00.000000Z\n" +
//                    "7\t17\t2022-03-17T00:00:00.000000Z\n" +
//                    "8\t18\t2022-03-17T00:00:00.000000Z\n" +
//                    "9\t19\t2022-03-17T00:00:00.000000Z\n" +
//                    "10\t110\t2022-03-17T00:00:00.000000Z\n" +
//                    "11\t111\t2022-03-17T00:00:00.000000Z\n" +
//                    "12\t112\t2022-03-17T00:00:00.000000Z\n" +
//                    "13\t113\t2022-03-17T00:00:00.000000Z\n" +
//                    "14\t114\t2022-03-17T00:00:00.000000Z\n"

                    PreparedStatement updateA = connection.prepareStatement("update x set a=? where a=?");
                    for (int i = 10; i < 15; i++) {
                        updateA.setInt(1, i + 10);
                        updateA.setInt(2, i);
                        updateA.execute();
                    }

//                    "a\tb\tts\n" +
//                    "0\t10\t2022-03-17T00:00:00.000000Z\n" +
//                    "1\t11\t2022-03-17T00:00:00.000000Z\n" +
//                    "2\t12\t2022-03-17T00:00:00.000000Z\n" +
//                    "3\t13\t2022-03-17T00:00:00.000000Z\n" +
//                    "4\t14\t2022-03-17T00:00:00.000000Z\n" +
//                    "5\t15\t2022-03-17T00:00:00.000000Z\n" +
//                    "6\t16\t2022-03-17T00:00:00.000000Z\n" +
//                    "7\t17\t2022-03-17T00:00:00.000000Z\n" +
//                    "8\t18\t2022-03-17T00:00:00.000000Z\n" +
//                    "9\t19\t2022-03-17T00:00:00.000000Z\n" +
//                    "20\t110\t2022-03-17T00:00:00.000000Z\n" +
//                    "21\t111\t2022-03-17T00:00:00.000000Z\n" +
//                    "22\t112\t2022-03-17T00:00:00.000000Z\n" +
//                    "23\t113\t2022-03-17T00:00:00.000000Z\n" +
//                    "24\t114\t2022-03-17T00:00:00.000000Z\n"

                    for (int i = 0; i < 5; i++) {
                        updateA.setInt(1, i + 10);
                        updateA.setInt(2, i);
                        updateA.execute();
                    }

//                    "a\tb\tts\n" +
//                    "10\t10\t2022-03-17T00:00:00.000000Z\n" +
//                    "11\t11\t2022-03-17T00:00:00.000000Z\n" +
//                    "12\t12\t2022-03-17T00:00:00.000000Z\n" +
//                    "13\t13\t2022-03-17T00:00:00.000000Z\n" +
//                    "14\t14\t2022-03-17T00:00:00.000000Z\n" +
//                    "5\t15\t2022-03-17T00:00:00.000000Z\n" +
//                    "6\t16\t2022-03-17T00:00:00.000000Z\n" +
//                    "7\t17\t2022-03-17T00:00:00.000000Z\n" +
//                    "8\t18\t2022-03-17T00:00:00.000000Z\n" +
//                    "9\t19\t2022-03-17T00:00:00.000000Z\n" +
//                    "20\t110\t2022-03-17T00:00:00.000000Z\n" +
//                    "21\t111\t2022-03-17T00:00:00.000000Z\n" +
//                    "22\t112\t2022-03-17T00:00:00.000000Z\n" +
//                    "23\t113\t2022-03-17T00:00:00.000000Z\n" +
//                    "24\t114\t2022-03-17T00:00:00.000000Z\n"

                    for (int i = 0; i < 3; i++) {
                        updateB.setInt(1, i + 1000);
                        updateB.setInt(2, i + 10);
                        updateB.execute();
                    }

                    connection.commit();
                    mayDrainWalQueue();

                    final String expected = """
                            a[INTEGER],b[INTEGER],ts[TIMESTAMP]
                            5,15,2022-03-17 00:00:00.0
                            6,16,2022-03-17 00:00:00.0
                            7,17,2022-03-17 00:00:00.0
                            8,18,2022-03-17 00:00:00.0
                            9,19,2022-03-17 00:00:00.0
                            10,1000,2022-03-17 00:00:00.0
                            11,1001,2022-03-17 00:00:00.0
                            12,1002,2022-03-17 00:00:00.0
                            13,13,2022-03-17 00:00:00.0
                            14,14,2022-03-17 00:00:00.0
                            20,110,2022-03-17 00:00:00.0
                            21,111,2022-03-17 00:00:00.0
                            22,112,2022-03-17 00:00:00.0
                            23,113,2022-03-17 00:00:00.0
                            24,114,2022-03-17 00:00:00.0
                            """;
                    try (ResultSet resultSet = connection.prepareStatement("x order by a").executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, resultSet);
                    }
                }
            }
        });
    }

    @Test
    public void testUpdatePreparedRenameUpdate() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE ts as" +
                    " (select x, timestamp_sequence('2022-02-24T04', 1000000) ts from long_sequence(2) )" +
                    " TIMESTAMP(ts) PARTITION BY MONTH").execute();

            try (PreparedStatement update = connection.prepareStatement("UPDATE ts set x = x + 10 WHERE x = ?")) {
                update.setInt(1, 1);
                update.execute();
                connection.commit();

                connection.prepareStatement("rename table ts to ts2").execute();
                try {
                    update.execute();
                    if (isEnabledForWalRun()) {
                        Assert.fail("Exception expected");
                    }
                } catch (PSQLException ex) {
                    TestUtils.assertContains(ex.getMessage(), "table does not exist [table=ts]");
                }
                connection.commit();
            }

            mayDrainWalQueue();

            sink.clear();
            try (
                    PreparedStatement ps = connection.prepareStatement("ts2");
                    ResultSet rs = ps.executeQuery()
            ) {
                assertResultSet(
                        """
                                x[BIGINT],ts[TIMESTAMP]
                                11,2022-02-24 04:00:00.0
                                2,2022-02-24 04:00:01.0
                                """,
                        sink,
                        rs
                );
            }

        });
    }

    @Test
    public void testUpdateTwiceWithSamePreparedStatement() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(id LONG, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            try (PreparedStatement statement = connection.prepareStatement("UPDATE tango SET id = ?")) {
                for (int i = 0; i < 2; i++) {
                    statement.setLong(1, 42);
                    statement.executeUpdate();
                }
            }
        });
    }

    @Test
    public void testUpdateTwiceWithSameQueryText() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE tango(id LONG, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            }
            String updateText = "UPDATE tango SET id = ?";
            for (int i = 0; i < 2; i++) {
                try (PreparedStatement statement = connection.prepareStatement(updateText)) {
                    statement.setLong(1, 42);
                    statement.executeUpdate();
                }
            }
        });
    }

    @Test
    public void testUpdateWithNowAndSystimestamp() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            setCurrentMicros(123678000);
            final PreparedStatement statement = connection.prepareStatement("create table x (a timestamp, b double, ts timestamp) timestamp(ts)");
            statement.execute();

            final PreparedStatement insert1 = connection.prepareStatement("insert into x values " +
                    "('2020-06-01T00:00:02'::timestamp, 2.0, '2020-06-01T00:00:02'::timestamp)," +
                    "('2020-06-01T00:00:06'::timestamp, 2.6, '2020-06-01T00:00:06'::timestamp)," +
                    "('2020-06-01T00:00:12'::timestamp, 3.0, '2020-06-01T00:00:12'::timestamp)");
            insert1.execute();

            final PreparedStatement update1 = connection.prepareStatement("update x set a=now() where b>2.5");
            int numOfRowsUpdated1 = update1.executeUpdate();
            assertEquals(2, numOfRowsUpdated1);

            final PreparedStatement insert2 = connection.prepareStatement("insert into x values " +
                    "('2020-06-01T00:00:22'::timestamp, 4.0, '2020-06-01T00:00:22'::timestamp)," +
                    "('2020-06-01T00:00:32'::timestamp, 6.0, '2020-06-01T00:00:32'::timestamp)");
            insert2.execute();

            final PreparedStatement update2 = connection.prepareStatement("update x set a=systimestamp() where b>5.0");
            int numOfRowsUpdated2 = update2.executeUpdate();
            assertEquals(1, numOfRowsUpdated2);

            final String expected = """
                    a[TIMESTAMP],b[DOUBLE],ts[TIMESTAMP]
                    2020-06-01 00:00:02.0,2.0,2020-06-01 00:00:02.0
                    1970-01-01 00:02:03.678,2.6,2020-06-01 00:00:06.0
                    1970-01-01 00:02:03.678,3.0,2020-06-01 00:00:12.0
                    2020-06-01 00:00:22.0,4.0,2020-06-01 00:00:22.0
                    1970-01-01 00:02:03.678,6.0,2020-06-01 00:00:32.0
                    """;
            try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                sink.clear();
                assertResultSet(expected, sink, resultSet);
            }
        });
    }

    @Test
    public void testUtf8QueryText() throws Exception {
        testQuery(
                "rnd_double(4) расход, ",
                "s[VARCHAR],i[INTEGER],расход[DOUBLE],t[TIMESTAMP],f[REAL],_short[SMALLINT],l[BIGINT],ts2[TIMESTAMP],bb[SMALLINT],b[BIT],rnd_symbol[VARCHAR],rnd_date[TIMESTAMP],rnd_bin[BINARY],rnd_char[CHAR],rnd_long256[VARCHAR]\n"
        );
    }

    @Test
    public void testUuidType_insertIntoUUIDColumn() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (u1 uuid, u2 uuid, s1 string)")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?, ?)")) {
                    insert.setObject(1, UUID.fromString("12345678-1234-5678-9012-345678901234"));
                    insert.setString(2, "12345678-1234-5678-9012-345678901234");
                    insert.setObject(3, UUID.fromString("12345678-1234-5678-9012-345678901234"));
                    insert.executeUpdate();
                }
                try (ResultSet resultSet = connection.prepareStatement("select *  from x").executeQuery()) {
                    sink.clear();
                    String expected = """
                            u1[OTHER],u2[OTHER],s1[VARCHAR]
                            12345678-1234-5678-9012-345678901234,12345678-1234-5678-9012-345678901234,12345678-1234-5678-9012-345678901234
                            """;
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testUuidType_update_nonPartitionedTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (u1 uuid)")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?)")) {
                    insert.setObject(1, UUID.fromString("11111111-1111-1111-1111-111111111111"));
                    insert.executeUpdate();
                }
                try (PreparedStatement update = connection.prepareStatement("update x set u1 = ?")) {
                    update.setObject(1, UUID.fromString("22222222-2222-2222-2222-222222222222"));
                    update.executeUpdate();
                }
                try (ResultSet resultSet = connection.prepareStatement("select *  from x").executeQuery()) {
                    sink.clear();
                    String expected = """
                            u1[OTHER]
                            22222222-2222-2222-2222-222222222222
                            """;
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    @Test
    public void testUuidType_update_partitionedTable() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (final PreparedStatement statement = connection.prepareStatement("create table x (ts timestamp, u1 uuid) timestamp(ts) partition by DAY")) {
                statement.execute();
                try (PreparedStatement insert = connection.prepareStatement("insert into x values (?, ?)")) {
                    insert.setTimestamp(1, new Timestamp(0));
                    insert.setObject(2, UUID.fromString("11111111-1111-1111-1111-111111111111"));
                    insert.executeUpdate();
                }
                try (PreparedStatement update = connection.prepareStatement("update x set u1 = ?")) {
                    update.setObject(1, UUID.fromString("12345678-1234-5467-9876-123321001987"));
                    update.executeUpdate();
                }
                if (walEnabled) {
                    drainWalQueue();
                }
                try (ResultSet resultSet = connection.prepareStatement("select u1  from x").executeQuery()) {
                    sink.clear();
                    String expected = """
                            u1[OTHER]
                            12345678-1234-5467-9876-123321001987
                            """;
                    assertResultSet(expected, sink, resultSet);
                }
            }
        });
    }

    /*
        package main

        import (
            "database/sql"
            "fmt"
            "math"
            "time"

            _ "github.com/lib/pq"
        )

        const (
            host     = "localhost"
            port     = 8812
            user     = "admin"
            password = "quest"
            dbname   = "qdb"
        )

        func main() {
            connStr := fmt.Sprintf(
                "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
                host, port, user, password, dbname)
            db, err := sql.Open("postgres", connStr)
            checkErr(err)
            defer db.Close()

            date, err := time.ParseInLocation("2006-01-02T15:04:05.999", "2022-01-12T12:01:01.120", time.UTC)
            checkErr(err)
            timestamp, err := time.ParseInLocation("2006-01-02T15:04:05.999999", "2020-02-13T10:11:12.123450", time.UTC)
            checkErr(err)

            stmt, err := db.Prepare("INSERT INTO all_types values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, " +
                "cast($13 as geohash(1c)), cast($14 as geohash(2c)) , cast($15  as geohash(4c)), cast($16 as geohash(8c)), $17, $18, cast('' || $19 as long256), $20)")
            checkErr(err)
            defer stmt.Close()

            var data = [][]interface{}{
                {bool(false), int16(0), int16(0), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, timestamp},
                {bool(true), int16(1), int16(1), mkstring("a"), mkint32(4), mkint64(5), &date, &timestamp, mkfloat32(12.345), mkfloat64(1.0234567890123),
                    mkstring("string"), mkstring("symbol"), mkstring("r"), mkstring("rj"), mkstring("rjtw"), mkstring("rjtwedd0"), mkstring("1.2.3.4"),
                    mkstring("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), mkstring("0x5dd94b8492b4be20632d0236ddb8f47c91efc2568b4d452847b4a645dbe4871a"), &timestamp},
                {bool(true), int16(math.MaxInt8), int16(math.MaxInt16), mkstring("z"), mkint32(math.MaxInt32), mkint64(math.MaxInt64), &date, mktimestamp("1970-01-01T00:00:00.000000"),
                    mkfloat32(math.MaxFloat32), mkfloat64(math.MaxFloat64), mkstring("XXX"), mkstring(" "), mkstring("e"), mkstring("ee"), mkstring("eeee"), mkstring("eeeeeeee"),
                    mkstring("255.255.255.255"), mkstring("a0eebc99-ffff-ffff-ffff-ffffffffffff"), mkstring("0x5dd94b8492b4be20632d0236ddb8f47c91efc2568b4d452847b4a645dbefffff"), mktimestamp("2020-03-31T00:00:00.987654")}}

            for i := 0; i < len(data); i++ {
                _, err = stmt.Exec(data[i]...)
                checkErr(err)
            }
        }

        func checkErr(err error) {
            if err != nil {
                panic(err)
            }
        }

        func mktimestamp(s string) *time.Time {
            timestamp, err := time.ParseInLocation("2006-01-02T15:04:05.999999", s, time.UTC)
            checkErr(err)
            return &timestamp
        }

        func mkstring(s string) *string {
            return &s
        }
        func mkfloat32(f float32) *float32 {
            return &f
        }
        func mkfloat64(f float64) *float64 {
            return &f
        }
        func mkint32(i int32) *int32 {
            return &i
        }
        func mkint64(i int64) *int64 {
            return &i
        }

        ----------------------------
        require (
            github.com/lib/pq v1.10.9 // indirect
        )
     */
    @Test
    @Ignore("""
            Legacy server's error message: 'bind variable at 18 is defined as unknown and cannot accept STRING'
            Modern server's error message: 'Internal error. Exception type: UnsupportedOperationException'
            thrown from ShortFunction.getByte()""")
    public void testVarargBindVariables() throws Exception {
        skipOnWalRun();
        engine.execute("CREATE TABLE all_types (" +
                "bool boolean,  byte_ byte,  short_ short,  char_ char,  int_ int,  long_ long,  date_ date, " +
                "tstmp timestamp,   float_ float,  double_ double,  str string,  sym symbol, " +
                "ge1 geohash(1c),  ge2 geohash(2c),  ge4 geohash(4c),  ge8 geohash(8c)," +
                " ip ipv4,   uuid_ uuid,  l256 long256," +
                " ts timestamp) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL", sqlExecutionContext);

        final String script =
                """
                        >0000005e00030000757365720061646d696e00636c69656e745f656e636f64696e6700555446380064617461626173650071646200646174657374796c650049534f2c204d44590065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >50000000ee3100494e5345525420494e544f20616c6c5f74797065732076616c756573202824312c2024322c2024332c2024342c2024352c2024362c2024372c2024382c2024392c202431302c202431312c202431322c2063617374282431332061732067656f6861736828316329292c2063617374282431342061732067656f686173682832632929202c206361737428243135202061732067656f6861736828346329292c2063617374282431362061732067656f6861736828386329292c202431372c202431382c2063617374282727207c7c20243139206173206c6f6e67323536292c202432302900000044000000075331005300000004
                        <3100000004740000005600140000001000000015000000150000001200000017000000140000045a0000045a000002bc000002bd0000041300000413000004130000041300000413000004130000041300000b86000000000000045a6e000000045a0000000549
                        >420000007e003100000000140000000566616c736500000001300000000130ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000001a323032302d30322d31332031303a31313a31322e31323334355a0000450000000900000000005300000004
                        <3200000004430000000f494e5345525420302031005a0000000549
                        >420000015a0031000000001400000004747275650000000131000000013100000001610000000134000000013500000017323032322d30312d31322031323a30313a30312e31325a0000001a323032302d30322d31332031303a31313a31322e31323334355a0000001231322e3334353030303236373032383830390000000f312e3032333435363738393031323300000006737472696e670000000673796d626f6c000000017200000002726a00000004726a747700000008726a74776564643000000007312e322e332e340000002461306565626339392d396330622d346566382d626236642d366262396264333830613131000000423078356464393462383439326234626532303633326430323336646462386634376339316566633235363862346434353238343762346136343564626534383731610000001a323032302d30322d31332031303a31313a31322e31323334355a0000450000000900000000005300000004
                        <3200000004430000000f494e5345525420302031005a0000000549
                        >42000002b100310000000014000000047472756500000003313237000000053332373637000000017a0000000a32313437343833363437000000133932323333373230333638353437373538303700000017323032322d30312d31322031323a30313a30312e31325a00000014313937302d30312d30312030303a30303a30305a0000002733343032383233343636333835323838363030303030303030303030303030303030303030303000000135313739373639333133343836323331353730303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030000000035858580000000120000000016500000002656500000004656565650000000865656565656565650000000f3235352e3235352e3235352e3235350000002461306565626339392d666666662d666666662d666666662d666666666666666666666666000000423078356464393462383439326234626532303633326430323336646462386634376339316566633235363862346434353238343762346136343564626566666666660000001b323032302d30332d33312030303a30303a30302e3938373635345a0000450000000900000000005300000004
                        <3200000004430000000f494e5345525420302031005a0000000549
                        >43000000075331005300000004
                        <33000000045a0000000549
                        >5800000004
                        """;

        assertHexScript(
                NetworkFacadeImpl.INSTANCE, script, new Port0PGConfiguration()
        );
    }

    @Test
    public void testVarchar() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(
                        "create table varchars as (select rnd_int() varchar1 from long_sequence(1))");
                statement.execute("varchars");
                try (ResultSet rs = statement.getResultSet()) {
                    assertTrue(rs.next());
//                    assertEquals("\u1755\uDA1F\uDE98|\uD924\uDE04۲", rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        });
    }

    /*
        use sqlx::postgres::PgPoolOptions;

        #[async_std::main]
        async fn main() -> Result<(), sqlx::Error> {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect("postgresql://admin:quest@localhost:8812/qdb")
                .await?;

            let row: (String,) = sqlx::query_as("SELECT id from x").fetch_one(&pool).await?;
            assert_eq!(row.0, "D");
            Ok(())
        }

        --------------------------------------
        [dependencies]
        async-std = { version = "1.12.0", features = [ "attributes" ] }
        sqlx = { version = "0.7", features = [ "runtime-async-std", "postgres" ] }
    */
    @Test
    public void testVarcharBinaryType() throws Exception {
        skipOnWalRun();
        engine.execute("create table x (id varchar)", sqlExecutionContext);
        engine.execute("insert into x values ('entry')", sqlExecutionContext);

        final String script =
                """
                        >0000006b00030000757365720061646d696e0064617461626173650071646200446174655374796c650049534f2c204d445900636c69656e745f656e636f64696e6700555446380054696d655a6f6e65005554430065787472615f666c6f61745f64696769747300320000
                        <520000000800000003
                        >700000000a717565737400
                        <520000000800000000530000001154696d655a6f6e6500474d5400530000001d6170706c69636174696f6e5f6e616d6500517565737444420053000000187365727665725f76657273696f6e0031312e33005300000019696e74656765725f6461746574696d6573006f6e005300000019636c69656e745f656e636f64696e670055544638004b0000000c0000003fbb8b96505a0000000549
                        >5300000004
                        <5a0000000549
                        >500000002073716c785f735f310053454c4543542069642066726f6d2078000000440000000e5373716c785f735f31005300000004
                        <310000000474000000060000540000001b000169640000000000000100000413ffffffffffff00005a0000000549
                        >42000000180073716c785f735f31000001000100000001000145000000090000000001430000000650005300000004
                        <3200000004440000000f000100000005656e747279730000000433000000045a0000000549
                        >5300000004
                        <5a0000000549
                        """;

        assertHexScript(
                NetworkFacadeImpl.INSTANCE, script, new Port0PGConfiguration()
        );
    }

    @Test
    public void testVarcharBindVarMixedAscii() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table x (a varchar, ts timestamp) timestamp(ts) partition by day");
            }

            try (PreparedStatement ps = connection.prepareStatement("insert into x values (?, ?)")) {
                Timestamp bTs = new Timestamp(0);
                ps.setString(1, "a");
                ps.setTimestamp(2, bTs);
                ps.execute();

                ps.setString(1, "Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:");
                ps.setTimestamp(2, bTs);
                ps.execute();

                ps.setString(1, "d9INVpegZ\"N");
                ps.setTimestamp(2, bTs);
                ps.execute();

                ps.setString(1, "葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*");
                ps.setTimestamp(2, bTs);
                ps.execute();
            }
            drainWalQueue();

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("select * from x");
                assertResultSet(
                        """
                                a[VARCHAR],ts[TIMESTAMP]
                                a,1970-01-01 00:00:00.0
                                Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:,1970-01-01 00:00:00.0
                                d9INVpegZ"N,1970-01-01 00:00:00.0
                                葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*,1970-01-01 00:00:00.0
                                """,
                        sink,
                        rs
                );
            }

            sink.clear();

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "x",
                    sink,
                    """
                            a\tts
                            a\t1970-01-01T00:00:00.000000Z
                            Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:\t1970-01-01T00:00:00.000000Z
                            d9INVpegZ"N\t1970-01-01T00:00:00.000000Z
                            葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*\t1970-01-01T00:00:00.000000Z
                            """
            );
        });
    }

    @Test
    public void testVarcharBindvarEqStringyCol() throws Exception {
        testVarcharBindVars(
                "select v,s from x where ?::varchar != v and ?::varchar != s");
    }

    private static int executeAndCancelQuery(PgConnection connection) throws SQLException, InterruptedException {
        int backendPid;
        AtomicBoolean isCancelled = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);
        backendPid = connection.getQueryExecutor().getBackendPID();
        String query = "select count(*) from tab t1 join tab t2 on t1.x = t2.x where sleep(120000)";

        try (final PreparedStatement stmt = connection.prepareStatement(query)) {
            Thread thread2 = new Thread(() -> {
                try {
                    while (!isCancelled.get()) {
                        Os.sleep(1);
                        connection.cancelQuery();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    finished.countDown();
                }
            }, "cancellation thread");
            thread2.start();

            try {
                Os.sleep(1);
                stmt.execute();
                Assert.fail("expected PSQLException with cancel message");
            } catch (PSQLException e) {
                assertContains(e.getMessage(), "cancelled by user");
                isCancelled.set(true);
                finished.await();
            } finally {
                thread2.join();
            }
        }
        return backendPid;
    }

    private static int getCountStar(Connection conn) throws Exception {
        int count = -1;
        try (PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM \"table\"")) {
            try (ResultSet result = stmt.executeQuery()) {
                if (result.next()) {
                    count = result.getInt(1);
                }
            }
        }
        return count;
    }

    private static int getRowCount(String query, Connection conn) throws Exception {
        int count = 0;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setFetchSize(100_000);
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    count++;
                }
            }
        }
        return count;
    }

    private void assertHexScript(String script) throws Exception {
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, getStdPgWireConfig());
    }

    private void assertHexScript(
            NetworkFacade clientNf,
            String script,
            PGConfiguration configuration
    ) throws Exception {

        /*
            You can use Wireshark to capture and decode. You can also see executed statements in the logs.
            From a Wireshark capture you can right-click on a packet and follow conversation:

            ...n....user.xyz.database.qdb.client_encoding.UTF8.DateStyle.ISO.TimeZone.Europe/London.extra_float_digits.2..R........p....oh.R........S....TimeZone.GMT.S....application_name.QuestDB.S....server_version.11.3.S....integer_datetimes.on.S....client_encoding.UTF8.Z....IP...".SET extra_float_digits = 3...B............E...	.....S....1....2....C....SET.Z....IP...7.SET application_name = 'PostgreSQL JDBC Driver'...B............E...	.....S....1....2....C....SET.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...-S_1.select 1,2,3 from long_sequence(1)...B.....S_1.......D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IX....
        */

        assertMemoryLeak(() -> {
            try (
                    PGServer server = createPGServer(configuration, true);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                NetUtils.playScript(clientNf, script, "127.0.0.1", server.getPort());
            }
        });
    }

    private void assertHexScriptAltCreds(String script) throws Exception {
        skipOnWalRun();
        assertHexScript(NetworkFacadeImpl.INSTANCE, script, getStdPgWireConfigAltCreds());
    }

    private void assertQueryAgainstIndexedSymbol(
            String[] values,
            String whereClause,
            String[] params,
            Connection connection,
            String tsOption,
            ResultProducer expected
    ) throws Exception {
        StringSink expSink = new StringSink();
        StringSink metaSink = new StringSink();
        String[] paramValues = new String[params.length];
        boolean[] isBindParam = new boolean[params.length];
        String[] bindValues = new String[params.length];
        int nValues = values.length;

        int iterations = 1;
        for (int i = 0; i < params.length; i++) {
            iterations *= nValues;
        }

        for (int iter = 0; iter < iterations; iter++) {
            int tempIter = iter;

            for (int p = 0; p < params.length; p++) {
                paramValues[p] = values[tempIter % nValues];

                if (paramValues[p].startsWith("?")) {
                    isBindParam[p] = true;
                    bindValues[p] = paramValues[p].substring(1);
                    paramValues[p] = "?";
                } else {
                    isBindParam[p] = false;
                    bindValues[p] = null;
                }

                tempIter /= nValues;
            }

            String where = whereClause;
            for (int p = 0; p < params.length; p++) {
                assert paramValues[p] != null;
                where = where.replace(params[p], paramValues[p]);
            }

            String query = "select s from tab where " + where;
            LOG.info().$("iteration [iter=").$(iter).$(". sql=").$(query).I$();

            sink.clear();
            expSink.clear();
            expSink.put("s[VARCHAR]\n");
            expected.produce(paramValues, isBindParam, bindValues, expSink);
            metaSink.clear();
            metaSink.put("query: ").put(query).put("\nvalues: ");
            for (int p = 0; p < paramValues.length; p++) {
                metaSink.put(isBindParam[p] ? bindValues[p] : paramValues[p]).put(' ');
            }
            metaSink.put("\nts option: ").put(tsOption);

            try (PreparedStatement ps = connection.prepareStatement(query)) {
                int bindIdx = 1;
                for (int p = 0; p < paramValues.length; p++) {
                    if (isBindParam[p]) {
                        ps.setString(bindIdx++, "null".equals(bindValues[p]) ? null : bindValues[p]);
                    }
                }
                try (ResultSet result = ps.executeQuery()) {
                    assertResultSet(metaSink.toString(), expSink, sink, result);
                }
            }
        }
    }

    private void assertResultTenTimes(Connection connection, String sql, String expected, int maxRows) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setMaxRows(maxRows);

            for (int i = 0; i < 10; i++) {
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet(expected, sink, rs);
                }
            }
        }
    }

    private void assertSql(Connection conn, String sql, String expectedResult) throws SQLException {
        final StringSink sink = Misc.getThreadLocalSink();
        sink.clear();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            BasePGTest.assertResultSet(expectedResult, sink, rs);
        }
    }

    private PGServer createPGServer(SOCountDownLatch queryScheduledCount) {
        int workerCount = 2;

        final PGConfiguration conf = new Port0PGConfiguration() {

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };

        WorkerPool workerPool = new TestWorkerPool(2, conf.getMetrics());
        DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(conf, engine.getConfiguration());
        try {
            return createPGWireServer(
                    conf,
                    engine,
                    workerPool,
                    registry,
                    createPGSqlExecutionContextFactory(null, queryScheduledCount)
            );
        } catch (Throwable t) {
            Misc.free(registry);
            Misc.free(workerPool);
            throw t;
        }
    }

    private ObjectFactory<SqlExecutionContextImpl> createPGSqlExecutionContextFactory(
            SOCountDownLatch queryStartedCount,
            SOCountDownLatch queryScheduledCount
    ) {
        return () -> new SqlExecutionContextImpl(engine, 0) {
            @Override
            public QueryFutureUpdateListener getQueryFutureUpdateListener() {
                return new QueryFutureUpdateListener() {
                    @Override
                    public void reportProgress(long commandId, int status) {
                        if (status == OperationFuture.QUERY_STARTED && queryStartedCount != null) {
                            queryStartedCount.countDown();
                        }
                    }

                    @Override
                    public void reportStart(TableToken tableToken, long commandId) {
                        if (queryScheduledCount != null) {
                            queryScheduledCount.countDown();
                        }
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private List<Tuple> getRows(ResultSet rs) {
        return TestUtils.unchecked(() -> {
            Field field = PgResultSet.class.getDeclaredField("rows");
            field.setAccessible(true);
            return (List<Tuple>) field.get(rs);
        });
    }

    private boolean isEnabledForWalRun() {
        return true;
    }

    private void mayDrainWalAndMatViewQueues() {
        if (walEnabled) {
            drainWalAndMatViewQueues();
        }
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private void queryTimestampsInRange(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select ts FROM xts WHERE ts <= dateadd('d', -1, ?) and ts >= dateadd('d', -2, ?)")
        ) {
            ResultSet rs = null;
            for (long micros = 0; micros < count * Micros.HOUR_MICROS; micros += Micros.HOUR_MICROS * 7) {
                sink.clear();
                // constructor requires millis
                Timestamp ts = new Timestamp(micros / 1000L);
                ts.setNanos((int) ((micros % 1_000_000) * 1000));
                statement.setTimestamp(1, ts);
                statement.setTimestamp(2, ts);
                rs = statement.executeQuery();

                long finalMicros = micros;
                String expected = datesArr
                        .stream()
                        .filter(arr -> (long) arr[0] <= (finalMicros - DAY_MICROS) && (long) arr[0] >= (finalMicros - 2 * DAY_MICROS))
                        .map(arr -> arr[1] + "\n")
                        .collect(Collectors.joining());

                assertResultSet("ts[TIMESTAMP]\n" + expected, sink, rs);
            }
            rs.close();
        }
    }

    private void skipOnWalRun() {
        Assume.assumeTrue("Test disabled during WAL run.", !walEnabled);
    }

    private void testAddColumnBusyWriter(boolean alterRequestReturnSuccess, SOCountDownLatch queryStartedCountDownLatch) throws SQLException, InterruptedException, BrokenBarrierException, SqlException {
        AtomicLong errors = new AtomicLong();
        int workerCount = 2;

        final PGConfiguration conf = new Port0PGConfiguration() {

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };

        try (
                DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(conf, engine.getConfiguration());
                WorkerPool pool = new WorkerPool(conf)
        ) {
            pool.assign(new ServerMain.EngineMaintenanceJob(engine));
            try (
                    PGServer server = createPGWireServer(
                            conf,
                            engine,
                            pool,
                            registry,
                            createPGSqlExecutionContextFactory(queryStartedCountDownLatch, null)
                    )
            ) {
                Assert.assertNotNull(server);
                int iteration = 0;

                do {
                    pool.start(LOG);
                    final String tableName = "xyz" + iteration++;
                    execute("create table " + tableName + " (a int)");

                    try {
                        int port1 = server.getPort();
                        try (final Connection connection1 = getConnection(Mode.EXTENDED, port1, true)
                        ) {
                            int port = server.getPort();
                            try (final Connection connection2 = getConnection(Mode.EXTENDED, port, true);
                                 final PreparedStatement insert = connection1.prepareStatement(
                                         "insert into " + tableName + " values (?)"
                                 )
                            ) {
                                connection1.setAutoCommit(false);
                                int totalCount = 10;
                                for (int i = 0; i < totalCount; i++) {
                                    insert.setInt(1, i);
                                    insert.execute();
                                }
                                CyclicBarrier start = new CyclicBarrier(2);
                                CountDownLatch finished = new CountDownLatch(1);
                                errors.set(0);

                                new Thread(() -> {
                                    try {
                                        start.await();
                                        try (
                                                final PreparedStatement alter = connection2.prepareStatement(
                                                        "alter table " + tableName + " add column b long"
                                                )
                                        ) {
                                            alter.execute();
                                        }
                                    } catch (Throwable e) {
                                        e.printStackTrace();
                                        errors.incrementAndGet();
                                    } finally {
                                        finished.countDown();
                                    }
                                }).start();

                                start.await();
                                Os.sleep(100);
                                connection1.commit();
                                finished.await();

                                if (alterRequestReturnSuccess) {
                                    Assert.assertEquals(0, errors.get());
                                    try (TableReader rdr = getReader(tableName)) {
                                        int bIndex = rdr.getMetadata().getColumnIndex("b");
                                        Assert.assertEquals(1, bIndex);
                                        Assert.assertEquals(totalCount, rdr.size());
                                    }
                                }
                            }
                        }
                    } finally {
                        pool.halt();
                        engine.releaseAllWriters();
                    }
                    // Failure may not happen if we're lucky, even when they are expected
                    // When alterRequestReturnSuccess if false and errors are 0, repeat
                } while (!alterRequestReturnSuccess && errors.get() == 0);
            }
        }
    }

    private void testBinaryInsert(int maxLength, int recvBufferSize, int sendBufferSize) throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(
                CONN_AWARE_EXTENDED,
                (connection, binary, mode, port) -> {
                    execute("create table xyz (" +
                            "a binary" +
                            ")"
                    );
                    try (final PreparedStatement insert = connection.prepareStatement("insert into xyz values (?)")) {
                        connection.setAutoCommit(false);
                        try (InputStream str = new InputStream() {
                            int value = 0;

                            @Override
                            public int read() {
                                if (maxLength == value) return -1;
                                return value++ % 255;
                            }

                            @Override
                            public void reset() {
                                value = 0;
                            }
                        }) {
                            int totalCount = 10;
                            for (int r = 0; r < totalCount; r++) {
                                insert.setBinaryStream(1, str);
                                insert.execute();
                                str.reset();
                            }
                            connection.commit();

                            try (
                                    PreparedStatement select = connection.prepareStatement("select a from xyz");
                                    ResultSet rs = select.executeQuery()
                            ) {

                                int count = 0;
                                while (rs.next()) {
                                    InputStream bs = rs.getBinaryStream(1);
                                    int len = 0;
                                    int i = bs.read();
                                    while (i > -1) {
                                        Assert.assertEquals(
                                                len % 255,
                                                i & 0xff // Convert byte to unsigned int
                                        );
                                        len++;
                                        i = bs.read();
                                    }
                                    Assert.assertEquals(maxLength, len);
                                    count++;
                                }

                                Assert.assertEquals(totalCount, count);
                            }
                        }
                    }
                },
                () -> {
                    this.recvBufferSize = recvBufferSize;
                    this.sendBufferSize = sendBufferSize;
                }
        );
    }

    private void testBindVariableDropLastPartitionListWithDatePrecision(int partitionBy) throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("CREATE TABLE x (l LONG, ts TIMESTAMP, date DATE) TIMESTAMP(ts) PARTITION BY " + PartitionBy.toString(partitionBy)).execute();
            connection.prepareStatement("INSERT INTO x VALUES (12, '2023-02-11T11:12:22.116234Z', '2023-02-11'::date)").execute();
            connection.prepareStatement("INSERT INTO x VALUES (13, '2023-02-12T16:42:00.333999Z', '2023-02-12'::date)").execute();
            connection.prepareStatement("INSERT INTO x VALUES (14, '2023-03-21T03:52:00.999999Z', '2023-03-21'::date)").execute();
            connection.commit();
            mayDrainWalQueue();
            try (
                    PreparedStatement select = connection.prepareStatement("SELECT date FROM x WHERE ts = '2023-02-11T11:12:22.116234Z'");
                    ResultSet rs = select.executeQuery()
            ) {
                Assert.assertTrue(rs.next());
                try (PreparedStatement dropPartition = connection.prepareStatement("ALTER TABLE x DROP PARTITION LIST '" + rs.getDate("date") + "';")) {
                    Assert.assertFalse(dropPartition.execute());
                }
            }
            mayDrainWalQueue();
            try (
                    PreparedStatement select = connection.prepareStatement("x");
                    ResultSet rs = select.executeQuery()
            ) {
                sink.clear();
                assertResultSet(
                        """
                                l[BIGINT],ts[TIMESTAMP],date[TIMESTAMP]
                                14,2023-03-21 03:52:00.999999,2023-03-21 00:00:00.0
                                """,
                        sink,
                        rs
                );
            }
        });
    }

    private void testBindVariablesWithIndexedSymbolInFilter(boolean indexed) throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.setAutoCommit(false);
            connection.prepareStatement("create table x (device_id symbol" + (indexed ? " index," : ",") + " column_name symbol, value double, timestamp timestamp) timestamp(timestamp) partition by day").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d1', 'c1', 101.1, 0)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d1', 'c1', 101.2, 1)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d1', 'c1', 101.3, 2)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d2', 'c1', 201.1, 0)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d2', 'c1', 201.2, 1)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d2', 'c1', 201.3, 2)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d3', 'c1', 301.1, 0)").execute();
            connection.prepareStatement("insert into x (device_id, column_name, value, timestamp) values ('d3', 'c1', 301.2, 1)").execute();
            connection.commit();

            mayDrainWalQueue();

            // single key value in filter

            try (PreparedStatement ps = connection.prepareStatement("select * from x where device_id = ? and timestamp > ? order by timestamp, value desc")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, "d1");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // try querying non-existing symbol
                    ps.setString(1, "foobar");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                "device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]\n",
                                sink,
                                rs
                        );
                    }

                    // and then an existing one
                    ps.setString(1, "d2");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("select * from x where device_id != ? and timestamp > ? order by timestamp, value desc")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, "d1");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // try querying non-existing symbol
                    ps.setString(1, "foobar");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // and then an existing one
                    ps.setString(1, "d2");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }

            // multiple key values in filter

            try (PreparedStatement ps = connection.prepareStatement("select * from x where device_id in (?, ?) and timestamp > ? order by timestamp, value desc")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, "d1");
                    ps.setString(2, "d2");
                    ps.setTimestamp(3, createTimestamp(0));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.2,1970-01-01 00:00:00.000001
                                        d1,c1,101.2,1970-01-01 00:00:00.000001
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // try querying non-existing symbols
                    ps.setString(1, "foobar");
                    ps.setString(2, "barbaz");
                    ps.setTimestamp(3, createTimestamp(0));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                "device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]\n",
                                sink,
                                rs
                        );
                    }

                    // and then an existing duplicate one
                    ps.setString(1, "d2");
                    ps.setString(2, "d2");
                    ps.setTimestamp(3, createTimestamp(0));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.2,1970-01-01 00:00:00.000001
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }

            try (PreparedStatement ps = connection.prepareStatement("select * from x where device_id not in (?, ?) and timestamp > ? order by timestamp, value desc")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, "d2");
                    ps.setString(2, "d3");
                    ps.setTimestamp(3, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // try querying non-existing symbols
                    ps.setString(1, "foobar");
                    ps.setString(2, "barbaz");
                    ps.setTimestamp(3, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d2,c1,201.3,1970-01-01 00:00:00.000002
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }

                    // and then an existing duplicate one
                    ps.setString(1, "d2");
                    ps.setString(2, "d2");
                    ps.setTimestamp(2, createTimestamp(1));
                    try (ResultSet rs = ps.executeQuery()) {
                        sink.clear();
                        assertResultSet(
                                """
                                        device_id[VARCHAR],column_name[VARCHAR],value[DOUBLE],timestamp[TIMESTAMP]
                                        d1,c1,101.3,1970-01-01 00:00:00.000002
                                        """,
                                sink,
                                rs
                        );
                    }
                }
            }
        });
    }

    private void testDisconnectDuringAuth0(int allowedSendCount) throws Exception {
        DisconnectOnSendNetworkFacade nf = new DisconnectOnSendNetworkFacade(allowedSendCount);
        assertMemoryLeak(() -> {
            PGConfiguration configuration = new Port0PGConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };
            try (
                    final PGServer server = createPGServer(configuration);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                for (int i = 0; i < 10; i++) {
                    try (Connection ignored1 = getConnectionWitSslInitRequest(Mode.EXTENDED, server.getPort(), false, -2)) {
                        assertExceptionNoLeakCheck("Connection should not be established when server disconnects during authentication");
                    } catch (PSQLException ignored) {
                    }
                    Assert.assertEquals(0, nf.getAfterDisconnectInteractions());
                    TestUtils.assertEventually(() -> Assert.assertTrue(nf.isSocketClosed()));
                    nf.reset();
                }
            }
        });
    }

    private void testExecuteWithDifferentBindVariables(Connection connection, String query) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, "S1");
            sink.clear();
            try (ResultSet rs = stmt.executeQuery()) {
                assertResultSet("""
                        h[VARCHAR],isym[VARCHAR]
                        h1,S1
                        """, sink, rs);
            }

            stmt.setString(1, "S2");
            sink.clear();
            try (ResultSet rs = stmt.executeQuery()) {
                assertResultSet("""
                        h[VARCHAR],isym[VARCHAR]
                        h2,S2
                        """, sink, rs);
            }

            stmt.setString(1, null);
            sink.clear();
            try (ResultSet rs = stmt.executeQuery()) {
                assertResultSet("""
                        h[VARCHAR],isym[VARCHAR]
                        h3,null
                        """, sink, rs);
            }

            stmt.setString(1, "S0");
            sink.clear();
            try (ResultSet rs = stmt.executeQuery()) {
                assertResultSet("h[VARCHAR],isym[VARCHAR]\n", sink, rs);
            }
        }
    }

    private void testFetchDisconnectReleasesReader(String query) throws Exception {
        skipOnWalRun(); // non-partitioned table
        // Circuit breaker does not work with fragmented buffer
        // TODO: find a solution, Net.peek() always return a byte when the incoming buffer not read fully
        // when executing 'E' (execute) postgres protocol command
        assertWithPgServer(
                CONN_AWARE_ALL,
                (connection, binary, mode, port) -> {
                    connection.setAutoCommit(false);

                    PreparedStatement tbl = connection.prepareStatement("create table xx as (" +
                            "select x," +
                            " timestamp_sequence(0, 1000) ts" +
                            " from long_sequence(1000000)) timestamp (ts)");
                    tbl.execute();

                    PreparedStatement stmt = connection.prepareStatement(query);
                    connection.setNetworkTimeout(Runnable::run, 1);
                    int testSize = 1000;
                    stmt.setFetchSize(testSize);
                    assertEquals(testSize, stmt.getFetchSize());

                    try {
                        // wait for disconnect timer to trigger
                        while (connection.isValid(10)) {
                            // in theory, we do not need to execute the query here, but if the line is removed
                            // connection.isValid() does not always detect that the connection is closed (or it takes a very long time)
                            stmt.executeQuery();
                            Os.sleep(250);
                        }
                        stmt.executeQuery();
                        Assert.fail("Exception is not thrown");
                    } catch (PSQLException ex) {
                        ex.printStackTrace();
                        // expected
                        Assert.assertNotNull(ex);
                    }
                },
                () -> forceRecvFragmentationChunkSize = recvBufferSize
        );
    }

    private void testQuery(String s, String s2) throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table x as (" +
                        "select " +
                        "rnd_str(4,4,4) s, " +
                        "rnd_int(0, 256, 4) i, " +
                        s +
                        "timestamp_sequence(0,10000) t, " +
                        "rnd_float(4) f, " +
                        "rnd_short() _short, " +
                        "rnd_long(0, 10000000, 5) l, " +
                        "rnd_timestamp(to_timestamp('2015','yyyy'),to_timestamp('2016','yyyy'),2) ts2, " +
                        "rnd_byte(0,127) bb, " +
                        "rnd_boolean() b, " +
                        "rnd_symbol(4,4,4,2), " +
                        "rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2)," +
                        "rnd_bin(10,20,2), " +
                        "rnd_char(), " +
                        "rnd_long256() " +
                        "from long_sequence(50)" +
                        ")");

                ResultSet rs = statement.executeQuery("select * from x");

                final String expected = s2 +
                        "null,57,0.6254021542412018,1970-01-01 00:00:00.0,0.46218354,-1593,3425232,null,121,false,PEHN,2015-03-17 04:25:52.765,00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e,D,0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\n" +
                        "OUOJ,77,null,1970-01-01 00:00:00.01,0.6761935,-7374,7777791,2015-06-19 08:47:45.603182,53,true,null,2015-11-10 09:50:33.215,00000000 8b 81 2b 93 4d 1a 8e 78 b5 b9 11 53 d0 fb 64 bb\n" +
                        "00000010 1a d4 f0,V,0xbedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e29660300cea7db540\n" +
                        "ICCX,205,0.8837421918800907,1970-01-01 00:00:00.02,0.053843975,6093,4552960,2015-07-17 00:50:59.787742,33,false,VTJW,2015-07-15 01:06:11.226,00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47,U,0x8b4e4831499fc2a526567f4430b46b7f78c594c496995885aa1896d0ad3419d2\n" +
                        "GSHO,31,0.34947269997137365,1970-01-01 00:00:00.03,0.1975137,10795,6406207,2015-05-22 14:59:41.673422,56,false,null,null,00000000 49 1c f2 3c ed 39 ac a8 3b a6,S,0x7eb6d80649d1dfe38e4a7f661df6c32b2f171b3f06f6387d2fd2b4a60ba2ba3b\n" +
                        "HZEP,180,0.06944480046327317,1970-01-01 00:00:00.04,0.4295631,21347,null,2015-02-07 10:02:13.600956,41,false,HYRX,null,00000000 ea c3 c9 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34,F,0x38e4be9e19321b57832dd27952d949d8691dd4412a2d398d4fc01e2b9fd11623\n" +
                        "HWVD,38,0.48524046868499715,1970-01-01 00:00:00.05,0.6797563,25579,5575751,2015-10-19 12:38:49.360294,15,false,VTJW,2015-02-06 22:58:50.333,null,Q,0x85134468025aaeb0a2f8bbebb989ba609bb0f21ac9e427283eef3f158e084362\n" +
                        "PGLU,97,0.029227696942726644,1970-01-01 00:00:00.06,0.17180288,-18912,8340272,2015-05-24 22:09:55.175991,111,false,VTJW,2015-11-08 21:57:22.812,00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b,K,0x55d3686d5da27e14255a91b0e28abeb36c3493fcb2d0272d6046e5d137dd8f0f\n" +
                        "WIFF,104,0.892454783921197,1970-01-01 00:00:00.07,0.09303343,28218,4009057,2015-02-18 07:26:10.141055,89,false,HYRX,null,00000000 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90 25 c2 20\n" +
                        "00000010 ff,R,0x55b0586d1c02dfb399904624c49b6d8a7d85ee2916b209c779406ab1f85e333a\n" +
                        "CLTJ,115,0.2093569947644236,1970-01-01 00:00:00.08,0.54595995,-8207,2378718,2015-04-21 12:25:43.291916,31,false,PEHN,null,00000000 a5 db a1 76 1c 1c 26 fb 2e 42 fa,F,0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\n" +
                        "HFLP,79,0.9130151105125102,1970-01-01 00:00:00.09,null,14667,2513248,2015-08-31 13:16:12.318782,3,false,null,2015-02-08 12:28:36.066,null,U,0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\n" +
                        "GLNY,138,0.7165847318191405,1970-01-01 00:00:00.1,0.75304896,-2666,9337379,2015-03-25 09:21:52.776576,111,false,HYRX,2015-01-24 15:23:13.092,00000000 62 e1 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43,Y,0xaac42ccbc493cf44aa6a0a1d4cdf40dd6ae4fd257e4412a07f19777ec1368055\n" +
                        "VTNP,237,0.29242748475227853,1970-01-01 00:00:00.11,0.7527907,-26861,2354132,2015-02-10 18:27:11.140675,56,true,null,2015-02-25 00:45:15.363,00000000 28 b6 a9 17 ec 0e 01 c4 eb 9f 13 8f bb 2a 4b,O,0x926cdd99e63abb35650d1fb462d014df59070392ef6aa389932e4b508e35428f\n" +
                        "WFOQ,255,null,1970-01-01 00:00:00.12,0.11624247,31569,6688277,2015-05-19 03:30:45.779999,126,true,PEHN,2015-12-09 09:57:17.078,null,E,0x4f38804270a4a64349b5760a687d8cf838cbb9ae96e9ecdc745ed9faeb513ad3\n" +
                        "EJCT,195,0.13312214396754163,1970-01-01 00:00:00.13,0.94351375,-3013,null,2015-11-03 14:54:47.524015,114,true,PEHN,2015-08-28 07:41:29.952,00000000 fb 9d 63 ca 94 00 6b dd 18 fe 71 76 bc 45 24 cd\n" +
                        "00000010 13 00 7c,R,0x3cfe50b9cabaf1f29e0dcffb7520ebcac48ad6b8f6962219b27b0ac7fbdee201\n" +
                        "JYYF,249,0.2000682450929353,1970-01-01 00:00:00.14,0.6021005,5869,2079217,2015-07-10 18:16:38.882991,44,true,HYRX,null,00000000 b7 6c 4b fb 2d 16 f3 89 a3 83 64 de d6 fd c4 5b\n" +
                        "00000010 c4 e9 19 47,P,0x85e70b46349799fe49f783d5343dd7bc3d3fe1302cd3371137fccdabf181b5ad\n" +
                        "TZOD,null,0.36078878996232167,1970-01-01 00:00:00.15,0.60070705,-23125,5083310,null,11,false,VTJW,2015-09-19 18:14:57.59,00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                        "00000010 00,E,0xcff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d2375166223a6181642\n" +
                        "PBMB,76,0.23567419576658333,1970-01-01 00:00:00.16,0.5713685,26284,null,2015-05-21 13:14:56.349036,45,true,null,2015-09-11 09:34:39.05,00000000 97 cb f6 2c 23 45 a3 76 60 15,M,0x3c3a3b7947ce8369926cbcb16e9a2f11cfab70f2d175d0d9aeb989be79cd2b8c\n" +
                        "TKRI,201,0.2625424312419562,1970-01-01 00:00:00.17,0.9153045,-5486,9917162,2015-05-03 03:59:04.256719,66,false,VTJW,2015-01-15 03:22:01.033,00000000 a1 f5 4b ea 01 c9 63 b4 fc 92 60 1f df 41 ec 2c,O,0x4e3e15ad49e0a859312981a73c9dfce79022a75a739ee488eefa2920026dba88\n" +
                        "NKGQ,174,0.4039042639581232,1970-01-01 00:00:00.18,0.43757588,20687,7315329,2015-07-25 04:52:27.724869,20,false,PEHN,2015-06-10 22:28:57.01,00000000 92 83 fc 88 f3 32 27 70 c8 01 b0,T,0x579b14c2725d7a7e5dfbd8e23498715b8d9ee30e7bcbf83a6d1b1c80f012a4c9\n" +
                        "FUXC,52,0.7430101994511517,1970-01-01 00:00:00.19,null,-14729,1042064,2015-08-21 02:10:58.949674,28,true,CPSW,2015-08-29 20:15:51.835,null,X,0x41457ebc5a02a2b542cbd49414e022a06f4aa2dc48a9a4d99288224be334b250\n" +
                        "TGNJ,159,0.9562577128401444,1970-01-01 00:00:00.2,0.25131977,795,5069730,2015-07-01 01:36:57.101749,71,true,PEHN,2015-09-12 05:41:59.999,00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed,M,0x4ba20a8e0cf7c53c9f527485c4aac4a2826f47baacd58b28700a67f6119c63bb\n" +
                        "HCNP,173,0.18684267640195917,1970-01-01 00:00:00.21,0.6884149,-14882,8416858,2015-06-16 19:31:59.812848,25,false,HYRX,2015-09-30 17:28:24.113,00000000 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1 ec 74 f8 10\n" +
                        "00000010 fc 6e 23,D,0x3d64559865f84c86488be951819f43042f036147c78e0b2d127ca5db2f41c5e0\n" +
                        "EZBR,243,0.8203418140538824,1970-01-01 00:00:00.22,0.22122747,-8447,4677168,2015-03-24 03:32:39.832378,78,false,CPSW,2015-02-16 04:04:19.082,00000000 42 67 78 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8,Q,0x721304ffe1c934386466208d506905af40c7e3bce4b28406783a3945ab682cc4\n" +
                        "ZPBH,131,0.1999576586778039,1970-01-01 00:00:00.23,0.4793073,-18951,874555,2015-12-22 19:13:55.404123,52,false,null,2015-10-03 05:16:17.891,null,Z,0xa944baa809a3f2addd4121c47cb1139add4f1a5641c91e3ab81f4f0ca152ec61\n" +
                        "VLTP,196,0.4104855595304533,1970-01-01 00:00:00.24,0.91834927,-12269,142107,2015-10-10 18:27:43.423774,92,false,PEHN,2015-02-06 18:42:24.631,null,H,0x5293ce3394424e6a5ae63bdf09a84e32bac4484bdeec40e887ec84d015101766\n" +
                        "RUMM,185,null,1970-01-01 00:00:00.25,0.8377384,-27649,3639049,2015-05-06 00:51:57.375784,89,true,PEHN,null,null,W,0x3166ed3bbffb858312f19057d95341886360c99923d254f38f22547ae9661423\n" +
                        "null,71,0.7409092302023607,1970-01-01 00:00:00.26,0.7417434,-18837,4161180,2015-04-22 10:19:19.162814,37,true,HYRX,2015-09-23 03:14:56.664,00000000 8e 93 bd 27 42 f8 25 2a 42 71 a3 7a 58 e5,D,0x689a15d8906770fcaefe0266b9f63bd6698c574248e9011c6cc84d9a6d41e0b8\n" +
                        "NGZT,214,0.18170646835643245,1970-01-01 00:00:00.27,0.8413721,21764,3231872,null,79,false,HYRX,2015-05-20 07:51:29.675,00000000 ab ab ac 21 61 99 be 2d f5 30 78 6d 5a 3b,H,0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\n" +
                        "EYYP,13,null,1970-01-01 00:00:00.28,0.5341281,19136,4658108,2015-08-20 05:26:04.061614,5,false,CPSW,2015-03-23 23:43:37.634,00000000 c8 66 0c 40 71 ea 20 7e 43 97 27 1f 5c d9 ee 04\n" +
                        "00000010 5b 9c,C,0x6e6ed811e25486953f35987a50016bbf481e9f55c33ac48c6a22b0bd6f7b0bf2\n" +
                        "GMPL,50,0.7902682918274309,1970-01-01 00:00:00.29,0.8740701,-27807,5693029,2015-07-14 21:06:07.975747,37,true,CPSW,2015-09-01 04:00:29.049,00000000 3b 4b b7 e2 7f ab 6e 23 03 dd c7 d6,U,0x72c607b1992ff2f8802e839b77a4a2d34b8b967c412e7c895b509b55d1c38d29\n" +
                        "BCZI,207,0.10863061577000221,1970-01-01 00:00:00.3,0.12934059,3999,121232,null,88,true,CPSW,2015-05-10 21:10:20.041,00000000 97 0b f5 ef 3b be 85 7c 11 f7 34,K,0x33be4c04695f74d776ac6df71a221f518f3c64248fb5943ea55ab4e6916f3f6c\n" +
                        "DXUU,139,null,1970-01-01 00:00:00.31,0.2622214,-15289,341060,2015-01-06 07:48:24.624773,110,false,null,2015-07-08 18:37:16.872,00000000 71 cf 5a 8f 21 06 b2 3f 0e 41 93 89 27 ca 10 2f\n" +
                        "00000010 60 ce,N,0x1c05d81633694e02795ebacfceb0c7dd7ec9b7e9c634bc791283140ab775531c\n" +
                        "FMDV,197,0.2522102209201954,1970-01-01 00:00:00.32,0.9930633,-26026,5396438,null,83,true,CPSW,null,00000000 86 75 ad a5 2d 49 48 68 36 f0 35,K,0x308a7a4966e65a0160b00229634848957fa67d6a419e1721b1520f66caa74945\n" +
                        "SQCN,62,0.11500943478849246,1970-01-01 00:00:00.33,0.5945632,1011,4631412,null,56,false,VTJW,null,null,W,0x66906dc1f1adbc206a8bf627c859714a6b841d6c6c8e44ce147261f8689d9250\n" +
                        "QSCM,130,0.8671405978559277,1970-01-01 00:00:00.34,0.427746,22899,403193,null,21,true,PEHN,2015-11-30 21:04:32.865,00000000 a0 ba a5 d1 63 ca 32 e5 0d 68 52 c6 94 c3 18 c9\n" +
                        "00000010 7c,I,0x3dcc3621f3734c485bb81c28ec2ddb0163def06fb4e695dc2bfa47b82318ff9f\n" +
                        "UUZI,196,0.9277429447320458,1970-01-01 00:00:00.35,0.6252758,24355,5761736,null,116,false,null,2015-02-04 07:15:26.997,null,B,0xb0a5224248b093a067eee4529cce26c37429f999bffc9548aa3df14bfed42969\n" +
                        "DEQN,41,0.9028381160965113,1970-01-01 00:00:00.36,0.12049389,29066,2545404,2015-04-07 21:58:14.714791,125,false,PEHN,2015-02-06 23:29:49.836,00000000 ec 4b 97 27 df cd 7a 14 07 92 01,I,0x55016acb254b58cd3ce05caab6551831683728ff2f725aa1ba623366c2d08e6a\n" +
                        "null,164,0.7652775387729266,1970-01-01 00:00:00.37,0.31229204,-8563,7684501,2015-02-01 12:38:28.322282,0,true,HYRX,2015-07-16 20:11:51.34,null,F,0x97af9db84b80545ecdee65143cbc92f89efea4d0456d90f29dd9339572281042\n" +
                        "QJPL,160,0.1740035812230043,1970-01-01 00:00:00.38,0.76274085,5991,2099269,2015-02-25 15:49:06.472674,65,true,VTJW,2015-04-23 11:15:13.065,00000000 de 58 45 d0 1b 58 be 33 92 cd 5c 9d,E,0xa85a5fc20776e82b36c1cdbfe34eb2636eec4ffc0b44f925b09ac4f09cb27f36\n" +
                        "BKUN,208,0.4452148524967028,1970-01-01 00:00:00.39,0.5820424,17928,6383721,2015-10-23 07:12:20.730424,7,false,null,2015-01-02 17:04:58.959,00000000 5e 37 e4 68 2a 96 06 46 b6 aa,F,0xe1d2020be2cb7be9c5b68f9ea1bd30c789e6d0729d44b64390678b574ed0f592\n" +
                        "REDS,4,0.03804995327454719,1970-01-01 00:00:00.4,0.10288429,2358,1897491,2015-07-21 16:34:14.571565,75,false,CPSW,2015-07-30 16:04:46.726,00000000 d6 88 3a 93 ef 24 a5 e2 bc 86,P,0x892458b34e8769928647166465305ef1dd668040845a10a38ea5fba6cf9bfc92\n" +
                        "MPVR,null,null,1970-01-01 00:00:00.41,0.5917935,8754,5828044,2015-10-05 21:11:10.600851,116,false,CPSW,null,null,H,0x9d1e67c6be2f24b2a4e2cc6a628c94395924dadabaed7ee459b2a61b0fcb74c5\n" +
                        "KKNZ,186,0.8223388398922372,1970-01-01 00:00:00.42,0.7204948,-6179,8728907,null,80,true,VTJW,2015-09-11 03:49:12.244,00000000 16 b2 d8 83 f5 95 7c 95 fd 52 bb 50 c9,B,0x55724661cfcc811f4482e1a2ba8efaef6e4aef0394801c40941d89f24081f64d\n" +
                        "BICL,182,0.7215695095610233,1970-01-01 00:00:00.43,0.22679222,-22899,6401660,2015-08-23 18:31:29.931618,78,true,null,null,null,T,0xbbb751ee10f060d1c2fbeb73044504aea55a8e283bcf857b539d8cd889fa9c91\n" +
                        "SWPF,null,0.48770772310128674,1970-01-01 00:00:00.44,0.9136698,-17929,8377336,2015-12-13 23:04:20.465454,28,false,HYRX,2015-10-31 13:37:01.327,00000000 b2 31 9c 69 be 74 9a ad cc cf b8 e4 d1 7a 4f,I,0xbe91d734443388a2a631d716b575c819c9224a25e3f6e6fa6cd78093d5e7ea16\n" +
                        "BHEV,80,0.8917678500174907,1970-01-01 00:00:00.45,0.23679739,29284,9577513,2015-10-20 07:38:23.889249,27,false,HYRX,2015-12-15 13:32:56.797,00000000 92 83 24 53 60 4d 04 c2 f0 7a 07 d4 a3 d1 5f 0d\n" +
                        "00000010 fe 63 10 0d,V,0x225fddd0f4325a9d8634e1cb317338a0d3cb7f61737f167dc902b6f6d779c753\n" +
                        "DPCH,62,0.6684502332750604,1970-01-01 00:00:00.46,0.8791061,-22600,9266553,null,89,true,VTJW,2015-05-25 19:42:17.955,00000000 35 1b b9 0f 97 f5 77 7e a3 2d ce fe eb cd 47 06\n" +
                        "00000010 53 61 97,S,0x89d6a43b23f83695b236ae5ffab54622ce1f4dac846490a8b88f0468c0cbfa33\n" +
                        "MKNJ,61,0.2682009935575007,1970-01-01 00:00:00.47,0.81340104,-1322,null,2015-11-04 08:11:39.996132,4,false,CPSW,2015-07-29 22:51:03.349,00000000 82 08 fb e7 94 3a 32 5d 8a 66 0b e4 85 f1 13 06\n" +
                        "00000010 f2 27,V,0x9890d4aea149f0498bdef1c6ba16dd8cbd01cf83632884ae8b7083f888554b0c\n" +
                        "GSQI,158,0.8047954890194065,1970-01-01 00:00:00.48,0.34691578,23139,1252385,2015-04-22 00:10:12.067311,32,true,null,2015-01-09 06:06:32.213,00000000 38 a7 85 46 1a 27 5b 4d 0f 33 f4 70,V,0xc0e6e110b909e13a812425a38162be0bb65e29ed529d4dba868a7075f3b34357\n" +
                        "BPTU,205,0.430214712409255,1970-01-01 00:00:00.49,0.9052249,31266,8271557,2015-01-07 05:53:03.838005,14,true,VTJW,2015-10-30 05:33:15.819,00000000 24 0b c5 1a 5a 8d 85 50 39 42 9e 8a 86 17 89 6b,S,0x4e272e9dfde7bb12618178f7feba5021382a8c47a28fefa475d743cf0c2c4bcd\n";

                assertResultSet(expected, sink, rs);
            }
        });
    }

    private void testReadParquetSchemaChange(boolean simple) throws Exception {
        inputRoot = root; // the parquet files are exported into the root dir
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            try (
                    PGServer server = createPGServer(1);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (Connection connection = getConnection(server.getPort(), simple, true)) {
                    connection.prepareStatement("create table x as (select 1 id_x, timestamp_sequence(0,10000) as ts from long_sequence(1))").execute();
                    connection.prepareStatement("create table y as (select 2 id_y, 'foobar' str, timestamp_sequence(1,10000) as ts from long_sequence(1))").execute();

                    try (
                            Path path = new Path();
                            PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                            TableReader readerX = engine.getReader("x");
                            TableReader readerY = engine.getReader("y")
                    ) {
                        path.of(root).concat("table.parquet").$();
                        PartitionEncoder.populateFromTableReader(readerX, partitionDescriptor, 0);
                        PartitionEncoder.encode(partitionDescriptor, path);

                        try (PreparedStatement ps = connection.prepareStatement("select * from read_parquet('table.parquet')")) {
                            try (ResultSet resultSet = ps.executeQuery()) {
                                sink.clear();
                                assertResultSet(
                                        """
                                                id_x[INTEGER],ts[TIMESTAMP]
                                                1,1970-01-01 00:00:00.0
                                                """,
                                        sink,
                                        resultSet
                                );
                            }

                            // delete the file and populate from y table
                            engine.getConfiguration().getFilesFacade().remove(path.$());
                            PartitionEncoder.populateFromTableReader(readerY, partitionDescriptor, 0);
                            PartitionEncoder.encode(partitionDescriptor, path);

                            // Query the data once again - this time the Parquet schema is different,
                            // so the query should get recompiled.
                            try (ResultSet resultSet = ps.executeQuery()) {
                                sink.clear();
                                assertResultSet(
                                        """
                                                id_y[INTEGER],str[VARCHAR],ts[TIMESTAMP]
                                                2,foobar,1970-01-01 00:00:00.000001
                                                """,
                                        sink,
                                        resultSet
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    private void testTableSchemaChange(boolean simple) throws Exception {
        skipOnWalRun(); // non-partitioned table
        assertMemoryLeak(() -> {
            try (
                    PGServer server = createPGServer(1);
                    WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (Connection connection = getConnection(server.getPort(), simple, true)) {
                    connection.prepareStatement("create table x as (select 2 id, 'foobar' str, timestamp_sequence(1,10000) as ts from long_sequence(1))").execute();

                    try (PreparedStatement ps = connection.prepareStatement("x")) {
                        try (ResultSet resultSet = ps.executeQuery()) {
                            sink.clear();
                            assertResultSet(
                                    """
                                            id[INTEGER],str[VARCHAR],ts[TIMESTAMP]
                                            2,foobar,1970-01-01 00:00:00.000001
                                            """,
                                    sink,
                                    resultSet
                            );
                        }

                        connection.prepareStatement("alter table x drop column str;").execute();

                        // Query the data once again - this time the schema is different,
                        // so the query should get recompiled.
                        try (ResultSet resultSet = ps.executeQuery()) {
                            sink.clear();
                            assertResultSet(
                                    """
                                            id[INTEGER],ts[TIMESTAMP]
                                            2,1970-01-01 00:00:00.000001
                                            """,
                                    sink,
                                    resultSet
                            );
                        }
                    }
                }
            }
        });
    }

    private void testUpdateAsync(SOCountDownLatch queryScheduledCount, OnTickAction onTick, String expected) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(queryScheduledCount);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                int port = server.getPort();
                try (final Connection connection = getConnection(Mode.SIMPLE, port, false);
                     final PreparedStatement statement = connection.prepareStatement(
                             "create table x (a long, b double, ts timestamp) timestamp(ts) partition by YEAR")
                ) {
                    statement.execute();
                    try (final PreparedStatement insert1 = connection.prepareStatement("insert into x values " +
                            "(1, 2.0, '2020-06-01T00:00:02'::timestamp)," +
                            "(2, 2.6, '2020-06-01T00:00:06'::timestamp)," +
                            "(5, 3.0, '2020-06-01T00:00:12'::timestamp)")) {
                        insert1.execute();
                    }
                    mayDrainWalQueue();

                    try (TableWriter writer = getWriter("x")) {
                        SOCountDownLatch finished = new SOCountDownLatch(1);
                        new Thread(() -> {
                            try (
                                    final PreparedStatement update1 = connection.prepareStatement(
                                            "update x set a=9 where b>2.5"
                                    )
                            ) {
                                int numOfRowsUpdated1 = update1.executeUpdate();
                                assertEquals(2, numOfRowsUpdated1);
                            } catch (Throwable e) {
                                Assert.fail(e.getMessage());
                            } finally {
                                finished.countDown();
                            }
                        }).start();

                        Clock microsecondClock = engine.getConfiguration().getMicrosecondClock();
                        long startTimeMicro = microsecondClock.getTicks();
                        // Wait 1 min max for completion
                        while (microsecondClock.getTicks() - startTimeMicro < 60_000_000 && finished.getCount() > 0) {
                            onTick.run(writer);
                            writer.tick(true);
                            finished.await(500_000);
                        }
                    }

                    mayDrainWalQueue();

                    try (ResultSet resultSet = connection.prepareStatement("x").executeQuery()) {
                        sink.clear();
                        assertResultSet(expected, sink, resultSet);
                    }
                }
            }
        });
    }

    private void testVarcharBindVars(String query) throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            PreparedStatement tbl = connection.prepareStatement("create table x as (" +
                    "select " +
                    "rnd_varchar(null, 'A','ABCDEFGHI','abcdefghijk') v, " +
                    "rnd_str(null, 'A','ABCDEFGHI','abcdefghijk') s, " +
                    "from long_sequence(5)" +
                    ")");
            tbl.execute();

            PreparedStatement insert = connection.prepareStatement("insert into x(v,s) values (?,?)");
            for (int i = 0; i < 5; i++) {
                insert.setString(1, String.valueOf((char) ('D' + i)));
                insert.setString(2, String.valueOf((char) ('D' + i)));
                insert.execute();
            }

            try (PreparedStatement stmnt = connection.prepareStatement(query)) {
                stmnt.setString(1, "D");
                stmnt.setString(2, "D");
                try (ResultSet rs = stmnt.executeQuery()) {
                    final String expected = """
                            v[VARCHAR],s[VARCHAR]
                            null,ABCDEFGHI
                            A,abcdefghijk
                            A,abcdefghijk
                            ABCDEFGHI,abcdefghijk
                            ABCDEFGHI,null
                            E,E
                            F,F
                            G,G
                            H,H
                            """;
                    assertResultSet(expected, sink, rs);
                }
            }
            try (PreparedStatement stmnt = connection.prepareStatement(query)) {
                stmnt.setString(1, null);
                stmnt.setString(2, null);
                try (ResultSet rs = stmnt.executeQuery()) {
                    final String expected = """
                            v[VARCHAR],s[VARCHAR]
                            A,abcdefghijk
                            A,abcdefghijk
                            ABCDEFGHI,abcdefghijk
                            D,D
                            E,E
                            F,F
                            G,G
                            H,H
                            """;
                    assertResultSet(expected, sink, rs);
                }
            }
        });
    }

    @FunctionalInterface
    public interface ConnectionAwareRunnable {
        void run(Connection connection, boolean binary, Mode mode, int port) throws Exception;
    }

    @FunctionalInterface
    interface OnTickAction {
        void run(TableWriter writer);
    }

    @FunctionalInterface
    interface ResultProducer {
        void produce(String[] paramVals, boolean[] isBindVals, String[] bindVals, Utf16Sink output);
    }

    private static class DelayedListener implements QueryRegistry.Listener {
        private final SOCountDownLatch queryFound = new SOCountDownLatch(1);
        private volatile CharSequence queryText;

        @Override
        public void onRegister(CharSequence query, long queryId, SqlExecutionContext context) {
            if (queryText == null) {
                return;
            }

            if (Chars.equalsNc(queryText, query)) {
                queryFound.await();
                queryText = null;
            }
        }
    }

    private static class DelayingNetworkFacade extends NetworkFacadeImpl {
        private final AtomicInteger delayedAttemptsCounter = new AtomicInteger(0);
        private final AtomicBoolean delaying = new AtomicBoolean(false);

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            if (!delaying.get()) {
                return super.sendRaw(fd, buffer, bufferLen);
            }

            if (delayedAttemptsCounter.decrementAndGet() < 0) {
                delaying.set(false);
            }
            return 0;
        }

        void startDelaying() {
            delayedAttemptsCounter.set(1000);
            delaying.set(true);
        }
    }

    private static class DisconnectOnSendNetworkFacade extends NetworkFacadeImpl {
        private final int initialAllowedSendCalls;
        // the state is only mutated from QuestDB threads and QuestDB calls it from a single thread only -> no need for AtomicInt
        // we also *read* it from a test thread -> volatile is needed
        private final AtomicInteger remainingAllowedSendCalls = new AtomicInteger();
        private volatile boolean socketClosed = false;

        private DisconnectOnSendNetworkFacade(int allowedSendCount) {
            this.remainingAllowedSendCalls.set(allowedSendCount);
            this.initialAllowedSendCalls = allowedSendCount;
        }

        @Override
        public int close(long fd) {
            socketClosed = true;
            return super.close(fd);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (remainingAllowedSendCalls.get() < 0) {
                remainingAllowedSendCalls.decrementAndGet();
                return -1;
            }
            return super.recvRaw(fd, buffer, bufferLen);
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            if (remainingAllowedSendCalls.decrementAndGet() < 0) {
                return -1;
            }
            return super.sendRaw(fd, buffer, bufferLen);
        }

        int getAfterDisconnectInteractions() {
            if (remainingAllowedSendCalls.get() >= 0) {
                return 0;
            }
            return -(remainingAllowedSendCalls.incrementAndGet());
        }

        boolean isSocketClosed() {
            return socketClosed;
        }

        void reset() {
            remainingAllowedSendCalls.set(initialAllowedSendCalls);
            socketClosed = false;
        }
    }
}
