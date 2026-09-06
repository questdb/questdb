package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Wave 0: a composite refusal must arrive at the statement that caused it.
 * <p>
 * Both gates here previously fired somewhere else — one at the next commit, one at query time —
 * so the user learned about the restriction at a point where they could not act on it, and an
 * unrelated operation took the blame.
 */
public class CompositeEarliestRefusalTest extends AbstractCairoTest {

    /**
     * SET FORMAT PARQUET used to be accepted and suspend the table on the NEXT commit.
     */
    @Test
    public void testSetFormatParquetRefusedAtTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();

            assertExceptionNoLeakCheck("ALTER TABLE c SET FORMAT PARQUET", -1,
                    "composite partitioning does not yet support FORMAT PARQUET");

            // the table must be untouched and still writable -- a refused statement changes nothing
            Assert.assertFalse("a refused statement must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            execute("INSERT INTO c VALUES ('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            assertQuery("SELECT count() FROM c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * A PLAIN table must be entirely unaffected: SET FORMAT PARQUET still works.
     */
    @Test
    public void testSetFormatParquetStillWorksOnPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            execute("ALTER TABLE p SET FORMAT PARQUET");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p")));
        });
    }
}
