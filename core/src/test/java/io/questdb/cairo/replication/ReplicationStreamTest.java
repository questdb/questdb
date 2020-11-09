package io.questdb.cairo.replication;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TablePageFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReplicationRecordCursorFactory;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamFrameMeta;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TestUtils.LeakProneCode;

public class ReplicationStreamTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(ReplicationStreamTest.class);

    interface FilesFacadeReadHandler {
        long read(long fd, long buf, long len, long offset);
    }

    private static FilesFacadeReadHandler READ_HANDLER = null;

    @BeforeClass
    public static void setUp() throws IOException {
        AbstractCairoTest.setUp();
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                return READ_HANDLER.read(fd, buf, len, offset);
            }
        };

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
        runTest("testSimple", () -> {
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
        LOG.info().$("Replicating [sourceTableName=").$(sourceTableName).$(", destTableName=").$(destTableName).$();
        compiler.compile("CREATE TABLE " + destTableName + " " + tableCreateFields + ";", sqlExecutionContext);
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, destTableName)) {
            sendReplicationStream(sourceTableName, nFirstRow, maxRowsPerFrame, writer);
        }
        String actual = select("SELECT * FROM " + destTableName);
        Assert.assertEquals(expected, actual);
    }

    private long streamTargetReadBufferSize = 1000;
    private long streamTargetReadBufferAddress;
    private long streamTargetReadBufferOffset;
    private long streamTargetReadBufferLength;

    private void sendReplicationStream(String sourceTableName, long nFirstRow, long maxRowsPerFrame, TableWriter writer) {
        int streamTargetFd = 1000;
        READ_HANDLER = new FilesFacadeReadHandler() {
            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (fd != streamTargetFd) {
                    Assert.fail();
                }
                long nRead = 0;
                if (streamTargetReadBufferOffset < streamTargetReadBufferLength) {
                    nRead = streamTargetReadBufferLength - streamTargetReadBufferOffset;
                    if (nRead > len) {
                        nRead = len;
                    }
                    Unsafe.getUnsafe().copyMemory(streamTargetReadBufferAddress + streamTargetReadBufferOffset, buf + offset, nRead);
                    streamTargetReadBufferOffset += nRead;
                }
                return nRead;
            }
        };
        streamTargetReadBufferAddress = Unsafe.malloc(streamTargetReadBufferSize);

        SlaveWriter slaveWriter = new SlaveWriterImpl().of(writer.newBlock());

        ReplicationSlaveManager recvMgr = new ReplicationSlaveManager() {
            @Override
            public SlaveWriter getSlaveWriter(int masterTableId) {
                // TODO Auto-generated method stub
                return slaveWriter;
            }

            @Override
            public void releaseSlaveWriter(int masterTableId, SlaveWriter slaveWriter) {
                // TODO Auto-generated method stub

            }
        };

        try (
                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, sourceTableName);
                TableReplicationRecordCursorFactory factory = new TableReplicationRecordCursorFactory(engine, sourceTableName, maxRowsPerFrame);
                TablePageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, reader.getMetadata().getTimestampIndex(), nFirstRow);
                ReplicationStreamGenerator streamGenerator = new ReplicationStreamGenerator(configuration);
                ReplicationStreamReceiver streamWriter = new ReplicationStreamReceiver(configuration, recvMgr)) {

            streamGenerator.of(reader.getMetadata().getId(), cursor, reader.getMetadata(), null);
            streamWriter.of(streamTargetFd);

            ReplicationStreamFrameMeta streamFrameMeta;
            while ((streamFrameMeta = streamGenerator.next()) != null) {
                long sz = streamFrameMeta.getFrameHeaderSize() + streamFrameMeta.getFrameDataSize();
                if (sz > streamTargetReadBufferSize) {
                    streamTargetReadBufferAddress = Unsafe.realloc(streamTargetReadBufferAddress, streamTargetReadBufferSize, sz);
                    streamTargetReadBufferSize = sz;
                }
                Unsafe.getUnsafe().copyMemory(streamFrameMeta.getFrameHeaderAddress(), streamTargetReadBufferAddress, streamFrameMeta.getFrameHeaderSize());
                if (streamFrameMeta.getFrameDataSize() > 0) {
                    Unsafe.getUnsafe().copyMemory(streamFrameMeta.getFrameDataAddress(), streamTargetReadBufferAddress + streamFrameMeta.getFrameHeaderSize(),
                            streamFrameMeta.getFrameDataSize());
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

        Unsafe.free(streamTargetReadBufferAddress, streamTargetReadBufferSize);
        slaveWriter.commit();
    }
}
