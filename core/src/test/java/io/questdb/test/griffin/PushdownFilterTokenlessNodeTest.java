package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

// Regression test for claim C1: a retained filter containing a tokenless
// (sub-query) node must not NPE in PushdownFilterExtractor when the table has
// parquet-format partitions. Extraction skips the tokenless node (best-effort;
// fewer extracted conditions is always safe) and the filter evaluates normally.
public class PushdownFilterTokenlessNodeTest extends AbstractCairoTest {

    private static final String BOTH_ROWS = "v\tts\n1\t2018-01-01T00:00:00.000000Z\n2\t2018-01-02T00:00:00.000000Z\n";

    @Test
    public void testTokenlessOrOperandOnParquetPartitionNonTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            printSql("select * from p where v = 1 or (select b from x limit 1)");
            TestUtils.assertEquals(BOTH_ROWS, sink);
            printSql("select * from p where v = 1 or (select b from x_false limit 1)");
            TestUtils.assertEquals("v\tts\n1\t2018-01-01T00:00:00.000000Z\n", sink);
        });
    }

    @Test
    public void testTokenlessOrOperandOnParquetPartitionTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the retained-filter shape produced by the WhereClauseParser tokenless-node fix
            printSql("select * from p where ts = '2018-01-01' or (select b from x limit 1)");
            TestUtils.assertEquals(BOTH_ROWS, sink);
            printSql("select * from p where ts = '2018-01-01' or (select b from x_false limit 1)");
            TestUtils.assertEquals("v\tts\n1\t2018-01-01T00:00:00.000000Z\n", sink);
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE p (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO p VALUES (1, '2018-01-01T00:00:00.000000Z'), (2, '2018-01-02T00:00:00.000000Z')");
        execute("CREATE TABLE x (b BOOLEAN)");
        execute("INSERT INTO x VALUES (true)");
        execute("CREATE TABLE x_false (b BOOLEAN)");
        execute("INSERT INTO x_false VALUES (false)");
        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2018-01-01'");
    }
}
