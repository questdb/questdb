package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * INVARIANT 6: a refusal must fire at the STATEMENT that caused it, not brick the table later.
 * <p>
 * {@code UPDATE} is permanently unsupported on composite tables -- see
 * {@code UpdateOperatorImpl.executeUpdate}, whose ban is load-bearing for column-file purge
 * correctness, not merely a scope reduction. But that gate lives in the WAL APPLY path, so the
 * statement itself succeeds and the table SUSPENDS on apply.
 * <p>
 * Found by a random-seed fuzz sweep: every one of six runs logged exactly one
 * "composite partitioning does not support UPDATE" suspension while still reporting 0 failures,
 * because the harness classifies UPDATE as a gated op and tolerates it. The suspension was real
 * regardless.
 * <p>
 * Same defect SHAPE as the FORMAT PARQUET and POSTING gates fixed earlier on this branch: accepted
 * DDL/DML, table bricks afterwards.
 */
public class CompositeUpdateGateTest extends AbstractCairoTest {

    /**
     * POSITIVE CONTROL. The identical UPDATE against a PLAIN table must succeed, so a passing
     * composite assertion cannot be explained by the statement being invalid generally.
     */
    @Test
    public void testPlainTableAcceptsUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','A',1.0)");
            drainWalQueue();

            execute("UPDATE p SET px = 5.0 WHERE exch = 'A'");
            drainWalQueue();

            Assert.assertFalse("plain table must not suspend on UPDATE",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p")));
            final io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT px FROM p", sink);
            TestUtils.assertContains(sink, "5.0");
        });
    }

    @Test
    public void testCompositeUpdateIsRefusedAtTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','B',2.0)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("c");

            try {
                execute("UPDATE c SET px = 5.0 WHERE exch = 'A'");
                Assert.fail("UPDATE must be refused at the statement on a composite table");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "composite partitioning does not support UPDATE");
            }

            drainWalQueue();
            Assert.assertFalse("a refused UPDATE must leave the table live, not suspended",
                    engine.getTableSequencerAPI().isSuspended(token));

            // still writable afterwards
            execute("INSERT INTO c VALUES ('2023-01-01T03:00:00.000000Z','A',3.0)");
            drainWalQueue();
            Assert.assertFalse("table must still accept writes after the refusal",
                    engine.getTableSequencerAPI().isSuspended(token));
            final io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM c", sink);
            TestUtils.assertContains(sink, "3");
        });
    }
}
