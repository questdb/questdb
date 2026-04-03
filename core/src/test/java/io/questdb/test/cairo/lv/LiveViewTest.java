package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewTest extends AbstractCairoTest {

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));

        execute("DROP LIVE VIEW live_rn");

        Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
    }

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        execute("CREATE TABLE t1 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW lv1 AS SELECT val, ts, row_number() OVER () AS rn FROM t1");
        // should not throw
        execute("CREATE LIVE VIEW IF NOT EXISTS lv1 AS SELECT val, ts, row_number() OVER () AS rn FROM t1");

        execute("DROP LIVE VIEW lv1");
    }

    @Test
    public void testDropLiveViewIfExists() throws Exception {
        // should not throw
        execute("DROP LIVE VIEW IF EXISTS nonexistent");
    }

    @Test
    public void testDropNonExistentLiveViewFails() throws Exception {
        try {
            execute("DROP LIVE VIEW nonexistent");
            Assert.fail("expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("live view does not exist"));
        }
    }

    @Test
    public void testLiveViewQueryEmpty() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        assertSql(
                "symbol\tprice\tts\trn\n",
                "SELECT * FROM live_rn"
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewQueryWithRefresh() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // insert first batch
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");

        drainWalQueue();
        drainLiveViewQueue();

        assertSql(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n",
                "SELECT * FROM live_rn"
        );

        // insert second batch (incremental)
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:03.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:04.000000Z')");

        drainWalQueue();
        drainLiveViewQueue();

        assertSql(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:03.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:04.000000Z\t3\n",
                "SELECT * FROM live_rn"
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewBaseTableMustBeWal() throws Exception {
        execute("CREATE TABLE non_wal (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");

        try {
            execute("CREATE LIVE VIEW lv AS SELECT val, ts, row_number() OVER () AS rn FROM non_wal");
            Assert.fail("expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("WAL"));
        }
    }

    private static void drainLiveViewQueue() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(engine)) {
            //noinspection StatementWithEmptyBody
            while (job.run(0, Job.RUNNING_STATUS)) ;
        }
    }
}
