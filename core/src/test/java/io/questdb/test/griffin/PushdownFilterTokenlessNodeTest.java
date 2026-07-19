package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

// Red regression test for claim C1: a retained filter containing a tokenless
// (subquery) node must not NPE in PushdownFilterExtractor when the table has
// parquet-format partitions; instead the query must fail with the same clean
// type-mismatch error the native path produces.
public class PushdownFilterTokenlessNodeTest extends AbstractCairoTest {

    @Test
    public void testTokenlessOrOperandOnParquetPartitionNonTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // pre-existing on master too (never reaches interval extraction)
            assertExceptionNoLeakCheck(
                    "select * from p where v = 1 or (select b from x limit 1)",
                    32,
                    "expression type mismatch, expected: BOOLEAN, actual: CURSOR"
            );
        });
    }

    @Test
    public void testTokenlessOrOperandOnParquetPartitionTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the exact retained-filter shape produced by the PR's WhereClauseParser fix
            assertExceptionNoLeakCheck(
                    "select * from p where ts = '2018-01-01' or (select b from x limit 1)",
                    44,
                    "expression type mismatch, expected: BOOLEAN, actual: CURSOR"
            );
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE p (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO p VALUES (1, '2018-01-01T00:00:00.000000Z'), (2, '2018-01-02T00:00:00.000000Z')");
        execute("CREATE TABLE x (b BOOLEAN)");
        execute("INSERT INTO x VALUES (true)");
        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2018-01-01'");
    }
}
