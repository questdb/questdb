package io.questdb.cairo.replication;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TablePageFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReplicationRecordCursorFactory;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.replication.ReplicationSlaveConnectionMultiplexer.ReplicationSlaveCallbacks;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
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
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationSlaveConnectionMultiplexerTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationSlaveConnectionMultiplexerTest.class);

    @BeforeClass
    public static void setUp() throws IOException {
        AbstractCairoTest.setUp();
        final FilesFacade ff = MockConnection.FILES_FACADE_INSTANCE;
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };
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

    private void replicateTable(String sourceTableName, String destTableName, String expected, String tableCreateFields, long nFirstRow, long maxRowsPerFrame)
            throws SqlException, IOException {
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
        ReplicationSlaveCallbacks slaveConnMuxCallbacks = new ReplicationSlaveCallbacks() {
            @Override
            public void onPeerDisconnected(long sid, long fd) {
                Assert.assertEquals(peerId, sid);
                Assert.fail();
            }
        };
        ReplicationSlaveConnectionMultiplexer slaveConnMux = new ReplicationSlaveConnectionMultiplexer(configuration, workerPool, muxProducerQueueLen,
                muxConsumerQueueLen, slaveConnMuxCallbacks);
        workerPool.start(LOG);
        MockConnection conn1 = new MockConnection();
        long recvBufferSz = TableReplicationStreamHeaderSupport.SCR_HEADER_SIZE;
        long recvBuffer = Unsafe.malloc(recvBufferSz);
        boolean done = slaveConnMux.tryAddConnection(peerId, conn1.connectorFd);
        Assert.assertTrue(done);
        LOG.info().$("Replicating [sourceTableName=").$(sourceTableName).$(", destTableName=").$(destTableName).$();
        compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);

        try (
                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, sourceTableName);
                TableReplicationRecordCursorFactory factory = new TableReplicationRecordCursorFactory(engine, sourceTableName, maxRowsPerFrame);
                TablePageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, reader.getMetadata().getTimestampIndex(), nFirstRow);
                ReplicationStreamGenerator streamGenerator = new ReplicationStreamGenerator()) {

            int masterTableId = reader.getMetadata().getId();
            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, destTableName);
            @SuppressWarnings("resource")
            SlaveWriter slaveWriter = new SlaveWriterImpl(configuration).of(writer);
            done = slaveConnMux.tryAddSlaveWriter(masterTableId, slaveWriter);
            Assert.assertTrue(done);

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
                    sendFrame(conn1, frame);
                } else {
                    // streamGenerator.nConcurrentFrames has been reached
                    Thread.yield();
                }
            }

            // Wait for slave ready to commit (SCR) frame
            recv(MockConnection.FILES_FACADE_INSTANCE, conn1.acceptorFd, recvBuffer, recvBufferSz, () -> {
                return false;
            });

            String countText = select("SELECT count() FROM " + destTableName);
            Assert.assertEquals("count\n0\n", countText);

            // Send commit frame
            while ((streamResult = streamGenerator.generateCommitBlockFrame()).isRetry()) {
                Thread.yield();
            }
            ReplicationStreamGeneratorFrame frame = streamResult.getFrame();
            sendFrame(conn1, frame);

            // Wait for slave to commit
            while (true) {
                countText = select("SELECT count() FROM " + destTableName);
                if ("count\n0\n".equals(countText)) {
                    Thread.yield();
                } else {
                    break;
                }
            }

            AtomicInteger ackCounter = new AtomicInteger(workerPoolConfig.getWorkerCount());
            done = slaveConnMux.tryRemoveSlaveWriter(masterTableId, ackCounter);
            Assert.assertTrue(done);
            while (ackCounter.get() > 0) {
                Thread.yield();
            }

            slaveWriter.close();
            writer.close();
        }

        conn1.close();
        workerPool.halt();
        slaveConnMux.close();
        Unsafe.free(recvBuffer, recvBufferSz);

        String actual = select("SELECT * FROM " + destTableName);
        Assert.assertEquals(expected, actual);
    }

    private void sendFrame(MockConnection conn1, ReplicationStreamGeneratorFrame frame) throws IOException {
        send(MockConnection.FILES_FACADE_INSTANCE, conn1.acceptorFd, frame.getFrameHeaderAddress(), frame.getFrameHeaderLength(), () -> {
            return false;
        });
        if (frame.getFrameDataAddress() != 0) {
            send(MockConnection.FILES_FACADE_INSTANCE, conn1.acceptorFd, frame.getFrameDataAddress(), frame.getFrameDataLength(), () -> {
                return false;
            });
        }
        frame.complete();
    }

    private void send(FilesFacade ff, long fd, long buffer, long buflen, Callable<Boolean> onIdle) throws IOException {
        long offset = 0;
        while (offset < buflen) {
            long len = buflen - offset;
            long nSent = ff.write(fd, buffer, len, offset);
            if (nSent < 0) {
                throw new IOException("disconnected");
            }
            if (nSent > 0) {
                offset += nSent;
            }
            if (offset < buflen) {
                boolean busy;
                try {
                    busy = onIdle.call();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                if (!busy) {
                    Thread.yield();
                }
            }
        }
    }

    private void recv(FilesFacade ff, long fd, long buffer, long buflen, Callable<Boolean> onIdle) throws IOException {
        long offset = 0;
        while (offset < buflen) {
            long len = buflen - offset;
            long nReceived = ff.read(fd, buffer, len, offset);
            if (nReceived < 0) {
                throw new IOException("disconnected");
            }
            if (nReceived > 0) {
                offset += nReceived;
            }
            if (offset < buflen) {
                boolean busy;
                try {
                    busy = onIdle.call();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                if (!busy) {
                    Thread.yield();
                }
            }
        }
    }
}
