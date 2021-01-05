package io.questdb.cairo.replication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReplicationPageFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReplicationRecordCursorFactory;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.replication.ReplicationMasterConnectionDemultiplexer.ReplicationMasterCallbacks;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorResult;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationMasterConnectionDemultiplexerTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionDemultiplexerTest.class);
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

    @Test
    public void testSimple1() throws Exception {
        runTest("testSimple1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(20)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");
            replicateTable("source", "dest", expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts)", 0, Long.MAX_VALUE);
            engine.releaseInactive();
        });
    }

    private void runTest(String name, LeakProneCode runnable) throws Exception {
        LOG.info().$("Starting test ").$(name).$();
        TestUtils.assertMemoryLeak(runnable);
        LOG.info().$("Finished test ").$(name).$();
    }

    private String select(CharSequence selectSql) throws SqlException {
        sink.clear();
        CompiledQuery query = compiler.compile(selectSql, sqlExecutionContext);
        try (
                RecordCursorFactory factory = query.getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            printer.print(cursor, factory.getMetadata(), true);
        }
        return sink.toString();
    }

    private void replicateTable(String sourceTableName, String destTableName, String expected, String tableCreateFields, long nFirstRow, long maxRowsPerFrame) throws SqlException {
        final int muxProducerQueueLen = 4;
        final int muxConsumerQueueLen = 4;
        final long peerId = 1;
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
        final AtomicInteger refReadyToCommitMasterTableId = new AtomicInteger(Integer.MIN_VALUE);
        ReplicationMasterCallbacks masterConnMuxCallbacks = new ReplicationMasterCallbacks() {
            @Override
            public void onSlaveReadyToCommit(long sid, int tableId) {
                Assert.assertEquals(peerId, sid);
                refReadyToCommitMasterTableId.set(tableId);
            }

            @Override
            public void onPeerDisconnected(long sid, long fd) {
                Assert.assertEquals(peerId, sid);
                Assert.fail();
            }
        };
        ReplicationMasterConnectionDemultiplexer masterConnDemux = new ReplicationMasterConnectionDemultiplexer(NF, workerPool, muxProducerQueueLen, 1,
                muxConsumerQueueLen, masterConnMuxCallbacks);
        workerPool.start(LOG);
        MockConnection conn1 = new MockConnection();
        boolean added = masterConnDemux.tryAddConnection(peerId, conn1.acceptorFd);
        Assert.assertTrue(added);
        LOG.info().$("Replicating [sourceTableName=").$(sourceTableName).$(", destTableName=").$(destTableName).$();
        compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);

        ReplicationStreamReceiver streamReceiver = new ReplicationStreamReceiver(NF);
        IntObjHashMap<SlaveWriter> slaveWriteByMasterTableId = new IntObjHashMap<>();
        streamReceiver.of(conn1.connectorFd, slaveWriteByMasterTableId, () -> {
            throw new RuntimeException("Unexpectedly disconnected");
        });

        try (
                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, sourceTableName);
                TableReplicationRecordCursorFactory factory = new TableReplicationRecordCursorFactory(engine, sourceTableName, maxRowsPerFrame);
                TableReplicationPageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, reader.getMetadata().getTimestampIndex(), nFirstRow);
                ReplicationStreamGenerator streamGenerator = new ReplicationStreamGenerator()) {

            int masterTableId = reader.getMetadata().getId();
            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, destTableName);
            SlaveWriter slaveWriter = new SlaveWriterImpl(configuration).of(writer);
            slaveWriteByMasterTableId.put(masterTableId, slaveWriter);

            IntList initialSymbolCounts = new IntList();
            for (int columnIndex = 0, sz = reader.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                if (reader.getMetadata().getColumnType(columnIndex) == ColumnType.SYMBOL) {
                    initialSymbolCounts.add(0);
                } else {
                    initialSymbolCounts.add(-1);
                }
            }

            // Send replication frames
            streamGenerator.of(masterTableId, workerPool.getWorkerCount() * muxConsumerQueueLen / 2, cursor, reader.getMetadata(), initialSymbolCounts);
            ReplicationStreamGeneratorResult streamResult;
            while ((streamResult = streamGenerator.nextDataFrame()) != null) {
                if (!streamResult.isRetry()) {
                    ReplicationStreamGeneratorFrame frame = streamResult.getFrame();
                    while (true) {
                        boolean frameQueued = masterConnDemux.tryQueueSendFrame(peerId, frame);
                        if (!frameQueued) {
                            streamReceiver.handleIO();
                            masterConnDemux.handleTasks();
                        } else {
                            break;
                        }
                    }
                } else {
                    // streamGenerator.nConcurrentFrames has been reached
                    Thread.yield();
                }
            }

            // Wait for slave ready to commit
            while (true) {
                int readyToCommitMasterTableId = refReadyToCommitMasterTableId.get();
                if (readyToCommitMasterTableId == Integer.MIN_VALUE) {
                    streamReceiver.handleIO();
                    masterConnDemux.handleTasks();
                } else {
                    Assert.assertEquals(reader.getMetadata().getId(), readyToCommitMasterTableId);
                    break;
                }
            }

            // Send commit frame
            while ((streamResult = streamGenerator.generateCommitBlockFrame()).isRetry()) {
                Thread.yield();
            }
            while (true) {
                boolean frameQueued = masterConnDemux.tryQueueSendFrame(peerId, streamResult.getFrame());
                if (!frameQueued) {
                    streamReceiver.handleIO();
                    masterConnDemux.handleTasks();
                } else {
                    break;
                }
            }

            // Wait for slave to commit
            while (streamReceiver.getnCommits() == 0) {
                streamReceiver.handleIO();
                masterConnDemux.handleTasks();
            }

            slaveWriteByMasterTableId.remove(masterTableId);
            slaveWriter.close();
            writer.close();
        }

        streamReceiver.close();

        conn1.close();
        workerPool.halt();
        masterConnDemux.close();

        String actual = select("SELECT * FROM " + destTableName);
        Assert.assertEquals(expected, actual);
    }
}
