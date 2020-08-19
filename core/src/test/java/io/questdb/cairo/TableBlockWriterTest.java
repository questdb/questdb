package io.questdb.cairo;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;

public class TableBlockWriterTest extends AbstractGriffinTest {

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

    @Test
    public void testString1() throws Exception {
        testString(false);
    }

    @Test
    public void testString2() throws Exception {
        testString(true);
    }

    private void testString(boolean endsWithNull) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_str(5,10,2) s FROM long_sequence(300)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            if (endsWithNull) {
                compiler.compile("INSERT INTO source (ts, s) SELECT ts+1, null FROM (" +
                        "SELECT ts, s FROM source ORDER BY ts DESC LIMIT 1" +
                        ")",
                        sqlExecutionContext);
            } else {
                compiler.compile("INSERT INTO source (ts, s) SELECT ts+1, 'ABC' FROM (" +
                        "SELECT ts, s FROM source ORDER BY ts DESC LIMIT 1" +
                        ")",
                        sqlExecutionContext);
            }
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (ts TIMESTAMP, s STRING) TIMESTAMP(ts);", sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testBinary1() throws Exception {
        testBinary(false);
    }

    @Test
    public void testBinary2() throws Exception {
        testBinary(true);
    }

    private void testBinary(boolean endsWithNull) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_bin(10, 20, 2) bin FROM long_sequence(500)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            if (endsWithNull) {
                compiler.compile("INSERT INTO source (ts) SELECT ts+1 FROM (" +
                        "SELECT ts, bin FROM source ORDER BY ts DESC LIMIT 1" +
                        ")",
                        sqlExecutionContext);
            } else {
                compiler.compile("INSERT INTO source (ts, bin) SELECT ts+1, rnd_bin() FROM (" +
                        "SELECT ts, bin FROM source ORDER BY ts DESC LIMIT 1" +
                        ")",
                        sqlExecutionContext);
            }
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (ts TIMESTAMP, bin BINARY) TIMESTAMP(ts);", sqlExecutionContext);
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
            final int columnCount = writer.getMetadata().getColumnCount();
            while ((frame = cursor.next()) != null) {
                long firstTimestamp = frame.getFirstTimestamp();
                long lastTimestamp = frame.getLastTimestamp();
                long pageRowCount = frame.getPageValueCount(0);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    long pageAddress = frame.getPageAddress(columnIndex);
                    long blockLength = frame.getPageLength(columnIndex);
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

    @BeforeClass
    public static void setUp3() {
        sqlExecutionContext.getRandom().reset(0, 1);
    }
}
