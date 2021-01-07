package io.questdb.cairo.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.replication.MasterReplicationService.MasterReplicationConfiguration;
import io.questdb.cairo.replication.SlaveReplicationService.SlaveReplicationConfiguration;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationTest.class);
    private static final String ZERO_COUNT = "count\n0\n";
    private final Path path = new Path(4096);
    private static NetworkFacade NF;
    private static CharSequence slaveRoot;

    @BeforeClass
    public static void setUp() throws IOException {
        AbstractCairoTest.setUp();
        NF = new NetworkFacadeImpl() {
            @Override
            public int recv(long fd, long buffer, int bufferLen) {
                int rc = super.recv(fd, buffer, bufferLen);
                // LOG.info().$("recv [fd=").$(fd).$(", bufferLen=").$(bufferLen).$(", rc=").$(rc).$();
                return rc;
            }

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                int rc = super.send(fd, buffer, bufferLen);
                // LOG.info().$("send [fd=").$(fd).$(", bufferLen=").$(bufferLen).$(", rc=").$(rc).$();
                return rc;
            }

            @Override
            public int close(long fd) {
                int rc = super.close(fd);
                // LOG.info().$("close [fd=").$(fd).$(", rc=").$(rc).$();
                return rc;
            }
        };
    }

    @BeforeClass
    public static void setUp2() {
        AbstractGriffinTest.setUp2();
        sqlExecutionContext.getRandom().reset(0, 1);
    }

    private static CairoConfiguration slaveConfiguration;
    private static CairoEngine slaveEngine;
    private static SqlCompiler slaveCompiler;
    private static SqlExecutionContext slaveSqlExecutionContext;

    @BeforeClass
    public static void startSlaveEngine() throws IOException {
        slaveRoot = temp.newFolder("dbSlaveRoot").getAbsolutePath();
        slaveConfiguration = new DefaultCairoConfiguration(slaveRoot);
        slaveEngine = new CairoEngine(slaveConfiguration);
        slaveCompiler = new SqlCompiler(slaveEngine);
        slaveSqlExecutionContext = new SqlExecutionContextImpl(slaveEngine, 1).with(AllowAllCairoSecurityContext.INSTANCE, new BindVariableServiceImpl(slaveConfiguration), null, -1, null);
    }

    @AfterClass
    public static void stopSlaveEngine() {
        slaveEngine.releaseInactive();
        Assert.assertEquals(0, slaveEngine.getBusyWriterCount());
        Assert.assertEquals(0, slaveEngine.getBusyReaderCount());
    }

    @After
    public void cleanupSlaveEngine() {
        slaveEngine.resetTableId();
        slaveEngine.releaseAllReaders();
        slaveEngine.releaseAllWriters();
        try (Path path = new Path().of(slaveRoot)) {
            Files.rmdir(path.$());
            Files.mkdirs(path.of(slaveRoot).put(Files.SEPARATOR).$(), slaveConfiguration.getMkDirMode());
        }
    }

    @Test
    public void testSimple1() throws Exception {
        runTest("testSimple1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(20)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            replicateTable("source", "(ts TIMESTAMP, l LONG) TIMESTAMP(ts)", 0, Long.MAX_VALUE);
        });
    }

    @Test
    public void testSimple2() throws Exception {
        runTest("testSimple2", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(20)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            replicateTable("source", null, 0, Long.MAX_VALUE);
        });
    }

    @Test
    public void testSimpleStringColumn() throws Exception {
        runTest("testSimpleStringColumn", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_str(3,10,2) s FROM long_sequence(20000)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            replicateTable("source", "(ts TIMESTAMP, s STRING) TIMESTAMP(ts)", 0, Long.MAX_VALUE);
        });
    }

    private void runTest(String name, LeakProneCode runnable) throws Exception {
        LOG.info().$("Starting test ").$(name).$();
        TestUtils.assertMemoryLeak(() -> {
            runnable.run();
            engine.releaseInactive();
            slaveEngine.releaseInactive();
        });
        LOG.info().$("Finished test ").$(name).$();
    }

    private String select(CharSequence selectSql, boolean slave) throws SqlException {
        SqlCompiler selectCompiler;
        SqlExecutionContext selectSqlExecutionContext;

        if (slave) {
            selectCompiler = slaveCompiler;
            selectSqlExecutionContext = slaveSqlExecutionContext;
        } else {
            selectCompiler = compiler;
            selectSqlExecutionContext = sqlExecutionContext;
        }

        sink.clear();
        CompiledQuery query = selectCompiler.compile(selectSql, selectSqlExecutionContext);
        try (
                RecordCursorFactory factory = query.getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(selectSqlExecutionContext)) {
            printer.print(cursor, factory.getMetadata(), true);
        }
        return sink.toString();
    }

    private IntList getListenPorts(ObjList<CharSequence> masterIps) throws IOException {
        List<ServerSocket> sockets = new ArrayList<>();
        for (int n = 0; n < masterIps.size(); n++) {
            CharSequence masterIp = masterIps.get(n);
            ServerSocket s = new ServerSocket();
            s.bind(new InetSocketAddress(masterIp.toString(), 0));
            sockets.add(s);
        }

        IntList masterPorts = new IntList(sockets.size());
        for (int n = 0; n < sockets.size(); n++) {
            ServerSocket s = sockets.get(n);
            int port = s.getLocalPort();
            masterPorts.add(port);
            s.close();
        }

        return masterPorts;
    }

    private void replicateTable(String tableName, String tableCreateFields, long nFirstRow, long maxRowsPerFrame)
            throws SqlException, IOException {
        WorkerPoolConfiguration workerPoolConfig = new WorkerPoolConfiguration() {
            private final int[] affinity = { -1, -1 };

            @Override
            public boolean haltOnError() {
                return false;
            }

            @Override
            public int getWorkerCount() {
                return affinity.length;
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }
        };
        WorkerPool workerPool = new WorkerPool(workerPoolConfig);

        ObjList<CharSequence> masterIps = new ObjList<>();
        masterIps.add("0.0.0.0");
        IntList masterPorts = getListenPorts(masterIps);
        MasterReplicationConfiguration masterReplicationConf = new MasterReplicationConfiguration(masterIps, masterPorts, 4);
        SlaveReplicationService slaveReplicationService = new SlaveReplicationService(slaveConfiguration, NF, slaveEngine, workerPool);
        MasterReplicationService masterReplicationService = new MasterReplicationService(NF, configuration.getFilesFacade(),
                configuration.getRoot(), engine, masterReplicationConf,
                workerPool);
        workerPool.start(LOG);

        if (null != tableCreateFields) {
            LOG.info().$("Replicating [sourceTableName=").$(tableName).$(", destTableName=").$(tableName).$();
            slaveCompiler.compile("CREATE TABLE " + tableName + " " + tableCreateFields + ";", slaveSqlExecutionContext);
        }

        ObjList<CharSequence> slaveIps = new ObjList<>();
        slaveIps.add("127.0.0.1");
        SlaveReplicationConfiguration replicationConf = new SlaveReplicationConfiguration(tableName, slaveIps, masterPorts);
        Assert.assertTrue(slaveReplicationService.tryAdd(replicationConf));

        // Wait for slave to commit
        long epochMsTimeout = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(2);
        while (true) {
            String countText = getSlaveTableRowCount(tableName);
            if (ZERO_COUNT.equals(countText)) {
                if (System.currentTimeMillis() > epochMsTimeout) {
                    LOG.error().$("timed out").$();
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error().$("interrupted").$();
                    break;
                }
            } else {
                break;
            }
        }

        workerPool.halt();
        assertTableEquals(tableName, tableName);
    }

    private String getSlaveTableRowCount(String tableName) {
        path.of(slaveRoot).concat(tableName).concat(TableUtils.TXN_FILE_NAME).$();
        if (!FilesFacadeImpl.INSTANCE.exists(path) || FilesFacadeImpl.INSTANCE.length(path) < 16) {
            return ZERO_COUNT;
        }
        String countText;
        try {
            countText = select("SELECT count() FROM " + tableName, true);
        } catch (SqlException ex) {
            countText = ZERO_COUNT;
        }
        return countText;
    }

    private void assertTableEquals(String sourceTableName, String destTableName) throws SqlException {
        CompiledQuery querySrc = compiler.compile("select * from " + sourceTableName, sqlExecutionContext);
        try (
                RecordCursorFactory factorySrc = querySrc.getRecordCursorFactory();
                RecordCursor cursorSrc = factorySrc.getCursor(sqlExecutionContext);) {
            CompiledQuery queryDst = slaveCompiler.compile("select * from " + destTableName, slaveSqlExecutionContext);
            try (RecordCursorFactory factoryDst = queryDst.getRecordCursorFactory();
                    RecordCursor cursorDst = factoryDst.getCursor(sqlExecutionContext)) {
                TestUtils.assertEquals(cursorSrc, factorySrc.getMetadata(), cursorDst, factoryDst.getMetadata());
            }
        }
    }
}
