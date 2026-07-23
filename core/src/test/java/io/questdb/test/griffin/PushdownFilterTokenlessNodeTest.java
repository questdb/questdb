package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
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
            assertQuery("select * from p where v = 1 or (select b from x limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(BOTH_ROWS);
            assertQuery("select * from p where v = 1 or (select b from x_false limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("v\tts\n1\t2018-01-01T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testTokenlessOrOperandOnParquetPartitionTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the retained-filter shape produced by the WhereClauseParser tokenless-node fix
            assertQuery("select * from p where ts = '2018-01-01' or (select b from x limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns(BOTH_ROWS);
            assertQuery("select * from p where ts = '2018-01-01' or (select b from x_false limit 1)")
                    .timestamp("ts")
                    .noLeakCheck()
                    .returns("v\tts\n1\t2018-01-01T00:00:00.000000Z\n");
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
