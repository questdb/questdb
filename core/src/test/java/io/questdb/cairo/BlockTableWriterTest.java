package io.questdb.cairo;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.TableWriter.Block;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;

public class BlockTableWriterTest extends AbstractGriffinTest {

    @Test
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (ts TIMESTAMP, l LONG) TIMESTAMP(ts);", sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                    ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (ts TIMESTAMP, l LONG) TIMESTAMP(ts) PARTITION BY DAY;", sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testNoTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT rnd_long(-55, 9009, 2) l FROM long_sequence(500)" +
                    ");",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (l LONG);", sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    private void replicateTable(String sourceTableName, String destTableName) {
        try (RecordCursorFactory factory = createReplicatingRecordCursorFactory(sourceTableName);
                TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), destTableName);
                TableBlockWriter blockWriter = new TableBlockWriter(configuration)) {
            blockWriter.of(writer);
            PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext);
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                long firstTimestamp = frame.getFirstTimestamp();
                long lastTimestamp = frame.getLastTimestamp();
                long pageRowCount = frame.getPageValueCount(0);
                for (int columnIndex = 0, sz = writer.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                    long pageAddress = frame.getPageAddress(columnIndex);
                    int colSz = ColumnType.sizeOf(writer.getMetadata().getColumnType(columnIndex));
                    long blockLength = pageRowCount * colSz;
                    blockWriter.putBlock(firstTimestamp, columnIndex, 0, blockLength, pageAddress);
                }
                blockWriter.commitAppendedBlock(firstTimestamp, lastTimestamp, pageRowCount);
            }
        }
    }

    private String select(CharSequence selectSql) throws SqlException {
        sink.clear();
        CompiledQuery query = compiler.compile(selectSql, sqlExecutionContext);
        try (RecordCursorFactory factory = query.getRecordCursorFactory(); RecordCursor cursor = factory.getCursor(sqlExecutionContext);) {
            printer.print(cursor, factory.getMetadata(), true);
        }
        return sink.toString();
    }

    private RecordCursorFactory createReplicatingRecordCursorFactory(String tableName) {
        return new TableReplicationRecordCursorFactory(engine, tableName);
    }
}
