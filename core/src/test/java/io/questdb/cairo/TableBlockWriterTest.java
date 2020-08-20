package io.questdb.cairo;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.questdb.cairo.TableReplicationRecordCursorFactory.TableReplicationRecordCursor;
import io.questdb.cairo.sql.PageFrame;
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
    public void testSimpleResumeBlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int nConsecuriveRows = 50;
            long tsStart = 0;
            long tsInc = 1000000000;
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT" +
                    " rnd_long(100,200,2) j," +
                    " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                    " from long_sequence(" + nConsecuriveRows + ")" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile(
                    "CREATE TABLE dest (j LONG, ts TIMESTAMP) TIMESTAMP(ts);",
                    sqlExecutionContext);
            replicateTable("source", "dest");
            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            tsStart += nConsecuriveRows * tsInc;
            compiler.compile("INSERT INTO source(j, ts) " +
                    "SELECT" +
                    " rnd_long(100,200,2) j," +
                    " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts" +
                    " from long_sequence(" + nConsecuriveRows + ")" +
                    ";",
                    sqlExecutionContext);
            expected = select("SELECT * FROM source");
            replicateTable("source", "dest", nConsecuriveRows);
            actual = select("SELECT * FROM dest");
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

    @Test
    public void testSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT timestamp_sequence(0, 1000000000) ts, rnd_symbol(60,2,16,2) sym FROM long_sequence(500)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile("CREATE TABLE dest (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts);", sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testAllTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT" +
                    " rnd_char() ch," +
                    " rnd_long256() ll," +
                    " rnd_int() a1," +
                    " rnd_int(0, 30, 2) a," +
                    " rnd_boolean() b," +
                    " rnd_str(3,3,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_short() f1," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long(100,200,2) j," +
                    " rnd_long() j1," +
                    " timestamp_sequence(0, 1000000000) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m" +
                    " from long_sequence(1000)" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile(
                    "CREATE TABLE dest (ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts);",
                    sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testAllTypesPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT" +
                    " rnd_char() ch," +
                    " rnd_long256() ll," +
                    " rnd_int() a1," +
                    " rnd_int(0, 30, 2) a," +
                    " rnd_boolean() b," +
                    " rnd_str(3,3,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_short() f1," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long(100,200,2) j," +
                    " rnd_long() j1," +
                    " timestamp_sequence(0, 1000000000) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m" +
                    " from long_sequence(1000)" +
                    ") TIMESTAMP (ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile(
                    "CREATE TABLE dest (ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts) PARTITION BY DAY;",
                    sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            engine.releaseInactive();
        });
    }

    @Test
    public void testAllTypesResumeBlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int nConsecuriveRows = 50;
            long tsStart = 0;
            long tsInc = 1000000000;
            compiler.compile("CREATE TABLE source AS (" +
                    "SELECT" +
                    " rnd_char() ch," +
                    " rnd_long256() ll," +
                    " rnd_int() a1," +
                    " rnd_int(0, 30, 2) a," +
                    " rnd_boolean() b," +
                    " rnd_str(3,3,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_short() f1," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long(100,200,2) j," +
                    " rnd_long() j1," +
                    " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m" +
                    " from long_sequence(" + nConsecuriveRows + ")" +
                    ") TIMESTAMP (ts);",
                    sqlExecutionContext);
            String expected = select("SELECT * FROM source");

            compiler.compile(
                    "CREATE TABLE dest (ch CHAR, ll LONG256, a1 INT, a INT, b BOOLEAN, c STRING, d DOUBLE, e FLOAT, f SHORT, f1 SHORT, g DATE, h TIMESTAMP, i SYMBOL, j LONG, j1 LONG, ts TIMESTAMP, l BYTE, m BINARY) TIMESTAMP(ts);",
                    sqlExecutionContext);
            replicateTable("source", "dest");

            String actual = select("SELECT * FROM dest");
            Assert.assertEquals(expected, actual);

            tsStart += nConsecuriveRows * tsInc;
            compiler.compile("INSERT INTO source(ch, ll, a1, a, b, c, d, e, f, f1, g, h, i, j, j1, ts, l, m) " +
                    "SELECT" +
                    " rnd_char() ch," +
                    " rnd_long256() ll," +
                    " rnd_int() a1," +
                    " rnd_int(0, 30, 2) a," +
                    " rnd_boolean() b," +
                    " rnd_str(3,3,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_short() f1," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long(100,200,2) j," +
                    " rnd_long() j1," +
                    " timestamp_sequence(" + tsStart + ", " + tsInc + ") ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m" +
                    " from long_sequence(" + nConsecuriveRows + ")" +
                    ";",
                    sqlExecutionContext);
            expected = select("SELECT * FROM source");
            replicateTable("source", "dest", nConsecuriveRows);

            engine.releaseInactive();
        });
    }

    private void replicateTable(String sourceTableName, String destTableName) {
        replicateTable(sourceTableName, destTableName, 0);
    }

    private void replicateTable(String sourceTableName, String destTableName, long nFirstRow) {
        try (TableReplicationRecordCursorFactory factory = createReplicatingRecordCursorFactory(sourceTableName);
                TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), destTableName);
                TableBlockWriter blockWriter = new TableBlockWriter(configuration)) {
            blockWriter.of(writer);
            TableReplicationRecordCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, nFirstRow);
            PageFrame frame;
            final int columnCount = writer.getMetadata().getColumnCount();
            while ((frame = cursor.next()) != null) {
                long firstTimestamp = frame.getFirstTimestamp();
                long lastTimestamp = frame.getLastTimestamp();
                long pageRowCount = frame.getPageValueCount(0);
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    int columnType = writer.getMetadata().getColumnType(columnIndex);
                    if (columnType == ColumnType.SYMBOL) {
                        long pageAddress = frame.getSymbolCharsPageAddress(columnIndex);
                        long blockLength = frame.getSymbolCharsPageLength(columnIndex);
                        blockWriter.putSymbolCharsBlock(columnIndex, 0, blockLength, pageAddress);
                    }
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

    private TableReplicationRecordCursorFactory createReplicatingRecordCursorFactory(String tableName) {
        return new TableReplicationRecordCursorFactory(engine, tableName);
    }

    @BeforeClass
    public static void setUp3() {
        sqlExecutionContext.getRandom().reset(0, 1);
    }
}
