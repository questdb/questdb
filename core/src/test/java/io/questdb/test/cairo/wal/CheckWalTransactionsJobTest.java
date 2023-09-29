package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.mp.MCSequence;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CheckWalTransactionsJobTest extends AbstractCairoTest {

    @Test
    public void testRespectsGivenTablesToCheck() throws Exception {
        assertMemoryLeak(() -> {
            final String table1 = "table1";
            final String table2 = "table2";
            ddl("create table " + table1 + " (x long, ts timestamp) timestamp(ts) partition by DAY WAL");
            ddl("create table " + table2 + " (x long, ts timestamp) timestamp(ts) partition by DAY WAL");

            insert("insert into " + table1 + " values (1, 1000)");
            insert("insert into " + table2 + " values (2, 2000)");

            // Throw away all WAL tasks in the queue.
            flushWalQueue();

            // Use check WAL txns job to put table1 transactions on the queue.
            final CheckWalTransactionsJob job = new CheckWalTransactionsJob(engine);
            job.addTableToCheck(engine.getTableTokenIfExists(table1));
            Assert.assertTrue(job.run(0));

            // Process what's on the queue.
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                // noinspection StatementWithEmptyBody
                while (walApplyJob.run(0)) ;
            }

            // Now we expect rows in table1, but not in table2.
            assertQuery("count\n1\n", "select count() from " + table1, null, false, true);
            assertQuery("count\n0\n", "select count() from " + table2, null, false, true);
        });
    }

    private static void flushWalQueue() {
        final MCSequence seq = engine.getMessageBus().getWalTxnNotificationSubSequence();
        long cursor;
        while ((cursor = seq.next()) != -1) {
            if (cursor > -1) {
                seq.done(cursor);
            }
        }
    }
}
