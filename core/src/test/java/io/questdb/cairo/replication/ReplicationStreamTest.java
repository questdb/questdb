package io.questdb.cairo.replication;

import java.io.IOException;

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
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationStreamTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationStreamTest.class);
    private static final int STREAM_TARGET_FD = Integer.MAX_VALUE;

    interface FilesFacadeRecvHandler {
        int recv(long fd, long buf, int len);
    }

    interface FilesFacadeSendHandler {
        int send(long fd, long buf, int len);
    }

    private static FilesFacadeRecvHandler RECV_HANDLER = null;
    private static FilesFacadeSendHandler SEND_HANDLER = null;
    private static NetworkFacade NF;

    @BeforeClass
    public static void setUp() throws IOException {
        AbstractCairoTest.setUp();
        NF = new NetworkFacadeImpl() {
            @Override
            public int recv(long fd, long buffer, int bufferLen) {
                if (fd != STREAM_TARGET_FD) {
                    return super.recv(fd, buffer, bufferLen);
                }
                return RECV_HANDLER.recv(fd, buffer, bufferLen);
            }

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (fd != STREAM_TARGET_FD) {
                    return super.send(fd, buffer, bufferLen);
                }
                return SEND_HANDLER.send(fd, buffer, bufferLen);
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

    @Test
    public void testPartitioned1() throws Exception {
        runTest("testPartitioned1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(100)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");
            replicateTable("source", "dest", expected, "(ts TIMESTAMP, l LONG) TIMESTAMP(ts) PARTITION BY DAY", 0, Long.MAX_VALUE);
            engine.releaseInactive();
        });
    }

    @Test
    public void testNoTimestamp() throws Exception {
        runTest("testNoTimestamp", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                    ");",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");
            replicateTable("source", "dest", expected, "(l LONG)", 0, Long.MAX_VALUE);
            engine.releaseInactive();
        });
    }

    @Test
    public void testString1() throws Exception {
        runTest("testString1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_str(5,10,2) s FROM long_sequence(300)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");
            replicateTable("source", "dest", expected, "(ts TIMESTAMP, s STRING) TIMESTAMP(ts)", 0, Long.MAX_VALUE);
            engine.releaseInactive();
        });
    }

    @Test
    public void testSymbol1() throws Exception {
        runTest("testSymbol1", () -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_symbol(60,2,16,2) sym FROM long_sequence(100)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");
            replicateTable("source", "dest", expected, "(ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", 0, Long.MAX_VALUE);
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
        LOG.info().$("Replicating [sourceTableName=").$(sourceTableName).$(", destTableName=").$(destTableName).$();
        compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, destTableName)) {
            sendReplicationStream(sourceTableName, nFirstRow, maxRowsPerFrame, writer);
        }
        String actual = select("SELECT * FROM " + destTableName);
        Assert.assertEquals(expected, actual);
    }

    private int streamTargetReadBufferSize = 1000;
    private long streamTargetReadBufferAddress;
    private int streamTargetReadBufferOffset;
    private int streamTargetReadBufferLength;

    private int streamTargetWriteBufferSize = 1000;
    private long streamTargetWriteBufferAddress;
    private int streamTargetWriteBufferOffset;

    private void sendReplicationStream(String sourceTableName, long nFirstRow, long maxRowsPerFrame, TableWriter writer) {
        RECV_HANDLER = new FilesFacadeRecvHandler() {
            @Override
            public int recv(long fd, long buf, int len) {
                int nRead = 0;
                if (streamTargetReadBufferOffset < streamTargetReadBufferLength) {
                    nRead = streamTargetReadBufferLength - streamTargetReadBufferOffset;
                    if (nRead > len) {
                        nRead = len;
                    }
                    Unsafe.getUnsafe().copyMemory(streamTargetReadBufferAddress + streamTargetReadBufferOffset, buf, nRead);
                    streamTargetReadBufferOffset += nRead;
                }
                return nRead;
            }
        };
        streamTargetReadBufferAddress = Unsafe.malloc(streamTargetReadBufferSize);
        streamTargetWriteBufferAddress = Unsafe.malloc(streamTargetWriteBufferSize);
        streamTargetWriteBufferOffset = 0;

        SEND_HANDLER = new FilesFacadeSendHandler() {
            @Override
            public int send(long fd, long buf, int len) {
                int nWrote = streamTargetWriteBufferSize - streamTargetWriteBufferOffset;
                if (nWrote > len) {
                    nWrote = len;
                }
                Unsafe.getUnsafe().copyMemory(buf, streamTargetWriteBufferAddress + streamTargetWriteBufferOffset, nWrote);
                streamTargetWriteBufferOffset += nWrote;
                return nWrote;
            }
        };

        IntObjHashMap<SlaveWriter> slaveWriteByMasterTableId = new IntObjHashMap<>();

        try (
                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, sourceTableName);
                TableReplicationRecordCursorFactory factory = new TableReplicationRecordCursorFactory(engine, sourceTableName, maxRowsPerFrame);
                TablePageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, reader.getMetadata().getTimestampIndex(), nFirstRow);
                ReplicationStreamGenerator streamGenerator = new ReplicationStreamGenerator();
                ReplicationStreamReceiver streamReceiver = new ReplicationStreamReceiver(NF)) {

            IntList initialSymbolCounts = new IntList();
            for (int columnIndex = 0, sz = reader.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                if (reader.getMetadata().getColumnType(columnIndex) == ColumnType.SYMBOL) {
                    initialSymbolCounts.add(0);
                } else {
                    initialSymbolCounts.add(-1);
                }
            }

            // Setup stream frame generator
            int masterTableId = reader.getMetadata().getId();
            streamGenerator.of(masterTableId, 1, cursor, reader.getMetadata(), initialSymbolCounts);

            // Setup stream frame receiver
            SlaveWriter slaveWriter = new SlaveWriterImpl(configuration).of(writer);
            slaveWriteByMasterTableId.put(masterTableId, slaveWriter);
            streamReceiver.of(STREAM_TARGET_FD, slaveWriteByMasterTableId, () -> {
                throw new RuntimeException("Unexpectedly disconnected");
            });

            ReplicationStreamGeneratorResult streamResult;
            while ((streamResult = streamGenerator.nextDataFrame()) != null) {
                if (!streamResult.isRetry()) {
                    try {
                        sendFrame(streamReceiver, streamResult.getFrame());
                    } finally {
                        streamResult.getFrame().complete();
                    }
                } else {
                    Thread.yield();
                }
            }

            // Wait for ready to commit from slave
            Assert.assertEquals(0, streamTargetWriteBufferOffset);
            while (streamTargetWriteBufferOffset != TableReplicationStreamHeaderSupport.SCR_HEADER_SIZE) {
                streamReceiver.handleIO();
            }

            while ((streamResult = streamGenerator.generateCommitBlockFrame()).isRetry()) {
                Thread.yield();
            }
            try {
                sendFrame(streamReceiver, streamResult.getFrame());
            } finally {
                streamResult.getFrame().complete();
            }

            slaveWriter.close();
            slaveWriteByMasterTableId.remove(masterTableId);
        }

        Unsafe.free(streamTargetReadBufferAddress, streamTargetReadBufferSize);
        Unsafe.free(streamTargetWriteBufferAddress, streamTargetWriteBufferSize);
    }

    private void sendFrame(ReplicationStreamReceiver streamWriter, ReplicationStreamGeneratorFrame streamFrameMeta) {
        int sz = streamFrameMeta.getFrameHeaderLength() + streamFrameMeta.getFrameDataLength();
        if (sz > streamTargetReadBufferSize) {
            streamTargetReadBufferAddress = Unsafe.realloc(streamTargetReadBufferAddress, streamTargetReadBufferSize, sz);
            streamTargetReadBufferSize = sz;
        }
        Unsafe.getUnsafe().copyMemory(streamFrameMeta.getFrameHeaderAddress(), streamTargetReadBufferAddress, streamFrameMeta.getFrameHeaderLength());
        if (streamFrameMeta.getFrameDataLength() > 0) {
            Unsafe.getUnsafe().copyMemory(streamFrameMeta.getFrameDataAddress(), streamTargetReadBufferAddress + streamFrameMeta.getFrameHeaderLength(),
                    streamFrameMeta.getFrameDataLength());
        }
        streamTargetReadBufferOffset = 0;
        streamTargetReadBufferLength = sz;

        while (streamTargetReadBufferLength > streamTargetReadBufferOffset) {
            streamWriter.handleIO();
        }
        streamTargetReadBufferOffset = 0;
        streamTargetReadBufferLength = 0;
    }
}
