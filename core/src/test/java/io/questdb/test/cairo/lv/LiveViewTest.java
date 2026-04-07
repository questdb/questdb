package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewTest extends AbstractCairoTest {

    @Test
    public void testBaseTableDropColumnInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("ALTER TABLE trades DROP COLUMN price");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("drop column operation", instance.getInvalidationReason());
    }

    @Test
    public void testBaseTableDropInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("DROP TABLE trades");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("base table drop", instance.getInvalidationReason());
    }

    @Test
    public void testBaseTableTruncateInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("TRUNCATE TABLE trades");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("truncate operation", instance.getInvalidationReason());
    }

    @Test
    public void testCannotAlterLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "ALTER TABLE live_rn ADD COLUMN x INT",
                12,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotDropLiveViewAsTable() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "DROP TABLE live_rn",
                11,
                "table name expected, got live view name: live_rn"
        );
    }

    @Test
    public void testCannotDropTableAsLiveView() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        assertException(
                "DROP LIVE VIEW trades",
                15,
                "live view name expected [name=trades]"
        );
    }

    @Test
    public void testCannotGetReaderForLiveView() throws Exception {
        createBaseTableAndLiveView();
        TableToken token = engine.getTableTokenIfExists("live_rn");
        Assert.assertNotNull(token);
        Assert.assertTrue(token.isLiveView());
        try {
            engine.getReader(token);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("cannot get a reader for view"));
        }
    }

    @Test
    public void testCannotInsertIntoLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "INSERT INTO live_rn VALUES ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z', 1)",
                12,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotReindexLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "REINDEX TABLE live_rn COLUMN symbol LOCK EXCLUSIVE",
                14,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotRenameLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "RENAME TABLE live_rn TO live_rn2",
                13,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotTruncateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "TRUNCATE TABLE live_rn",
                15,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotUpdateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "UPDATE live_rn SET price = 0",
                0,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotVacuumLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "VACUUM TABLE live_rn",
                13,
                "cannot modify live view [view=live_rn]"
        );
    }

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
    public void testLiveViewAllColumnTypes() throws Exception {
        execute(
                "CREATE TABLE all_types (" +
                        " b BOOLEAN," +
                        " bt BYTE," +
                        " sh SHORT," +
                        " i INT," +
                        " l LONG," +
                        " f FLOAT," +
                        " d DOUBLE," +
                        " ch CHAR," +
                        " sym SYMBOL," +
                        " str STRING," +
                        " vc VARCHAR," +
                        " dt DATE," +
                        " ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY HOUR WAL"
        );
        drainWalQueue();

        execute(
                "CREATE LIVE VIEW lv_all AS" +
                        " SELECT b, bt, sh, i, l, f, d, ch, sym, str, vc, dt, ts," +
                        " row_number() OVER () AS rn" +
                        " FROM all_types"
        );

        execute(
                "INSERT INTO all_types VALUES" +
                        " (true, 1, 2, 3, 4, 1.5, 2.5, 'A', 'SYM1', 'hello', 'world'," +
                        "  '2024-01-01', '2024-01-01T00:00:00.000000Z')," +
                        " (false, 10, 20, 30, 40, 10.5, 20.5, 'B', 'SYM2', NULL, 'test'," +
                        "  '2024-01-02', '2024-01-01T00:00:01.000000Z')"
        );
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "b\tbt\tsh\ti\tl\tf\td\tch\tsym\tstr\tvc\tdt\tts\trn\n" +
                        "true\t1\t2\t3\t4\t1.5\t2.5\tA\tSYM1\thello\tworld\t2024-01-01T00:00:00.000Z\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "false\t10\t20\t30\t40\t10.5\t20.5\tB\tSYM2\t\ttest\t2024-01-02T00:00:00.000Z\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM lv_all",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW lv_all");
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

    @Test
    public void testLiveViewQueryEmpty() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
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

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // insert second batch (incremental)
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:03.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:04.000000Z')");

        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:03.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:04.000000Z\t3\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewSurvivesRestart() throws Exception {
        createBaseTableAndLiveView();

        // insert data and refresh before restart
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // simulate restart: clear in-memory registry, reload from disk
        engine.getLiveViewRegistry().clear();
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));

        engine.reloadTableNames();
        engine.buildViewGraphs();

        // verify live view was rebuilt from disk
        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        // InMemoryTable is empty after restart; insert new data to trigger refresh
        execute("INSERT INTO trades VALUES ('MSFT', 400.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // full recompute includes both pre- and post-restart data
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:02.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewSurvivesRestartWithDroppedBaseTable() throws Exception {
        createBaseTableAndLiveView();

        // drop the base table
        execute("DROP TABLE trades");
        drainWalQueue();

        // simulate restart
        engine.getLiveViewRegistry().clear();
        engine.reloadTableNames();
        engine.buildViewGraphs();

        // live view should be loaded but invalidated
        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("base table does not exist", instance.getInvalidationReason());

        execute("DROP LIVE VIEW live_rn");
    }

    private static void drainLiveViewQueue() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(engine)) {
            //noinspection StatementWithEmptyBody
            while (job.run(0, Job.RUNNING_STATUS)) ;
        }
    }

    private void createBaseTableAndLiveView() throws SqlException {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");
    }
}
