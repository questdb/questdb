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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationTest.class);
    private static NetworkFacade NF;

    @BeforeClass
    public static void setUp() throws IOException {
        AbstractCairoTest.setUp();
        NF = MockConnection.NETWORK_FACADE_INSTANCE;
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
        CharSequence slaveRoot = temp.newFolder("dbSlaveRoot").getAbsolutePath();
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
    }

    @Test
    public void testSimple1() throws Exception {
        runTest("testSimple1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(20)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source", false);
            replicateTable("source", expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts)", 0, Long.MAX_VALUE);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            slaveEngine.releaseAllReaders();
            slaveEngine.releaseAllWriters();
        });
    }

    private void runTest(String name, LeakProneCode runnable) throws Exception {
        LOG.info().$("Starting test ").$(name).$();
        TestUtils.assertMemoryLeak(runnable);
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

    private void replicateTable(String tableName, String expected, String tableCreateFields, long nFirstRow, long maxRowsPerFrame)
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
        masterIps.add("127.0.0.1");
        IntList masterPorts = getListenPorts(masterIps);
        MasterReplicationConfiguration masterReplicationConf = new MasterReplicationConfiguration(masterIps, masterPorts, 4);
        SlaveReplicationService slaveReplicationService = new SlaveReplicationService(slaveConfiguration, NF, slaveEngine, workerPool);
        MasterReplicationService masterReplicationService = new MasterReplicationService(NF, slaveConfiguration.getFilesFacade(),
                slaveConfiguration.getRoot(), engine, masterReplicationConf,
                workerPool);
        workerPool.start(LOG);

        if (null != tableCreateFields) {
            LOG.info().$("Replicating [sourceTableName=").$(tableName).$(", destTableName=").$(tableName).$();
            slaveCompiler.compile("CREATE TABLE " + tableName + " " + tableCreateFields + ";", slaveSqlExecutionContext);
        }

        SlaveReplicationConfiguration replicationConf = new SlaveReplicationConfiguration(tableName, masterIps, masterPorts);
        Assert.assertTrue(slaveReplicationService.tryAdd(replicationConf));

        // Wait for slave to commit
        long epochMsTimeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2);
        while (true) {
            String countText = select("SELECT count() FROM " + tableName, true);
            if ("count\n0\n".equals(countText)) {
                if (System.currentTimeMillis() > epochMsTimeout) {
                    LOG.error().$("timed out").$();
                    break;
                }
                Thread.yield();
            } else {
                break;
            }
        }

        workerPool.halt();

        // String actual = select("SELECT * FROM " + tableName, true);
        // Assert.assertEquals(expected, actual);
    }

}
