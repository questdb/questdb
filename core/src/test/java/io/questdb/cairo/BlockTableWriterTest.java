package io.questdb.cairo;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.TableWriter.Block;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
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
                TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), destTableName);) {
            PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext);
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                long firstTimestamp = frame.getFirstTimestamp();
                long lastTimestamp = frame.getLastTimestamp();
                long pageRowCount = frame.getPageValueCount(0);
                Block block = writer.newBlock(firstTimestamp, lastTimestamp, (int) pageRowCount);
                for (int columnIndex = 0, sz = writer.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                    long pageAddress = frame.getPageAddress(columnIndex);
                    block.putBlock(columnIndex, pageAddress);
                }
                block.append();
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
        final RecordMetadata metadata;
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName, -1)) {
            metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
        }
        return new TableReplicationRecordCursorFactory(metadata, engine, tableName);
    }

    private RecordCursorFactory createReplicatingRecordCursorFactoryX(String tableName) {
        final RecordMetadata metadata;
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName, -1)) {
            metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
        }

        IntList columnIndexes = new IntList();
        IntList columnSizes = new IntList();

        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizes.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
        }
        return new TableReaderRecordCursorFactory(
                metadata,
                engine,
                tableName,
                TableUtils.ANY_TABLE_VERSION,
                columnIndexes,
                columnSizes,
                true);
    }
}
