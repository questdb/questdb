/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

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
    public void testCreateLiveViewRejectsDedupBase() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, symbol)");
        drainWalQueue();

        // Incremental refresh reads the pre-dedup WAL row stream; rows dropped or replaced
        // at apply time would be double-counted, so DEDUP base tables are rejected for V1.
        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                17,
                "live view cannot be created over a base table with DEDUP keys"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsJoin() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE TABLE refs (symbol SYMBOL, name STRING, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT t.symbol, t.price, t.ts, row_number() OVER () AS rn" +
                        " FROM trades t JOIN refs r ON (symbol)",
                17,
                "live view select must be a simple scan of a single WAL base table"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsNonZeroPassWindow() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // first_value(...) IGNORE NULLS OVER (PARTITION BY ...) is a TWO_PASS window function;
        // it cannot be maintained incrementally.
        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, ts, first_value(price) ignore nulls over (partition by symbol) AS fv FROM trades",
                17,
                "live view select may only use window functions that support incremental refresh"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsSubquery() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn" +
                        " FROM (SELECT symbol, price, ts FROM trades)",
                27,
                "live view requires a single base table in FROM clause"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsWhereClause() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades WHERE price > 100",
                17,
                "live view select cannot use a WHERE clause yet"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsWindowOrderedByNonTimestamp() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // Ordering the window by a non-timestamp column forces the planner onto the cached
        // window path, which requires sorting the full base dataset on every refresh.
        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, price, ts, row_number() OVER (ORDER BY price) AS rn FROM trades",
                17,
                "live view select may only use window functions that support incremental refresh"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testDropDuringRefreshDefersFree() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            // Simulate a refresh in flight: hold the refresh latch across the DROP.
            Assert.assertTrue(instance.tryLockForRefresh());
            try {
                execute("DROP LIVE VIEW live_rn");
                // The view is removed from the registry and marked as dropped, but the
                // table is NOT freed yet because we hold the refresh latch.
                Assert.assertTrue(instance.isDropped());
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
            } finally {
                instance.unlockAfterRefresh();
            }
            // The refresh finally hook would normally do this; call it explicitly here.
            instance.tryCloseIfDropped();
        });
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
    public void testDropWithActiveReadLockDefersFree() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            // Simulate an active reader cursor: hold the read lock across the DROP.
            Assert.assertTrue(instance.tryLockForRead());
            try {
                execute("DROP LIVE VIEW live_rn");
                // The view is removed from the registry and marked dropped, but the
                // table is NOT freed yet because we hold the read lock.
                Assert.assertTrue(instance.isDropped());
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
            } finally {
                instance.unlockAfterRead();
            }
            // The cursor close hook would normally do this; call it explicitly here.
            instance.tryCloseIfDropped();
        });
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
    public void testLiveViewMultipleRefreshBatches() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // batch 1
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // batch 2
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')," +
                " ('MSFT', 400.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // batch 3
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:04.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:05.000000Z')," +
                " ('MSFT', 401.0, '2024-01-01T00:00:06.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:04.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:05.000000Z\t3\n" +
                        "MSFT\t401.0\t2024-01-01T00:00:06.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
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
    public void testLiveViewRefreshAfterMultipleInserts() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // bootstrap with first batch
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // multiple INSERTs before draining the queue: each INSERT creates
        // a separate WAL transaction, so the refresh job processes multiple
        // WAL txns in one pass
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");
        execute("INSERT INTO trades VALUES" +
                " ('MSFT', 400.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewRefreshAfterUpdate() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // bootstrap
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // UPDATE produces a non-DATA WAL event, which triggers full recompute
        execute("UPDATE trades SET price = 999.0 WHERE symbol = 'GOOG'");
        drainWalQueue();
        drainLiveViewQueue();

        // the full recompute must reflect the updated price
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t999.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // verify that subsequent INSERT still works after the recompute
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t999.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:03.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewRefreshRowNumberNoPartition() throws Exception {
        execute("CREATE TABLE events (val INT, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_seq AS" +
                " SELECT val, ts, row_number() OVER () AS rn FROM events");

        // bootstrap
        execute("INSERT INTO events VALUES" +
                " (10, '2024-01-01T00:00:00.000000Z')," +
                " (20, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "val\tts\trn\n" +
                        "10\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "20\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM live_seq",
                null,
                "ts",
                true,
                true
        );

        // second batch: row_number continues from where it left off
        execute("INSERT INTO events VALUES" +
                " (30, '2024-01-01T00:00:02.000000Z')," +
                " (40, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "val\tts\trn\n" +
                        "10\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "20\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "30\t2024-01-01T00:00:02.000000Z\t3\n" +
                        "40\t2024-01-01T00:00:03.000000Z\t4\n",
                "SELECT * FROM live_seq",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_seq");
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

    @Test
    public void testExplainLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "QUERY PLAN\n" +
                        "LiveView\n" +
                        "  name: live_rn\n",
                "EXPLAIN SELECT * FROM live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testInformationSchemaTablesLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "table_type\n" +
                        "LIVE VIEW\n",
                "SELECT table_type FROM information_schema.tables() WHERE table_name = 'live_rn'"
        );
        assertSql(
                "is_insertable_into\n" +
                        "false\n",
                "SELECT is_insertable_into FROM information_schema.tables() WHERE table_name = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewsFunction() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "view_name\tbase_table_name\tlag\tlag_unit\tretention\tretention_unit\tview_status\tinvalidation_reason\tview_sql\n" +
                        "live_rn\ttrades\t0\t\t0\t\tvalid\t\tSELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn FROM trades\n",
                "SELECT * FROM live_views()"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewsFunctionWithLagAndRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv_lr LAG 5s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades");

        assertSql(
                "view_name\tlag\tlag_unit\tretention\tretention_unit\n" +
                        "lv_lr\t5\tSECOND\t10\tMINUTE\n",
                "SELECT view_name, lag, lag_unit, retention, retention_unit FROM live_views()"
        );
        execute("DROP LIVE VIEW lv_lr");
    }

    @Test
    public void testPgClassRelkindLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "relkind\n" +
                        "v\n",
                "SELECT relkind FROM pg_class() WHERE relname = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testShowColumnsLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "symbol\tSYMBOL\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                        "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n",
                "SHOW COLUMNS FROM live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testShowCreateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "ddl\n" +
                        "CREATE LIVE VIEW 'live_rn' AS (\n" +
                        "SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn FROM trades\n" +
                        ");\n",
                "SHOW CREATE LIVE VIEW live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testShowCreateLiveViewWithLagAndRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv_lr LAG 5s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades");

        assertSql(
                "ddl\n" +
                        "CREATE LIVE VIEW 'lv_lr' LAG 5s RETENTION 10m AS (\n" +
                        "SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades\n" +
                        ");\n",
                "SHOW CREATE LIVE VIEW lv_lr"
        );
        execute("DROP LIVE VIEW lv_lr");
    }

    @Test
    public void testTablesTypeLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "table_type\n" +
                        "L\n",
                "SELECT table_type FROM tables() WHERE table_name = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testTryLockForReadFailsAfterDrop() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            execute("DROP LIVE VIEW live_rn");
            Assert.assertTrue(instance.isDropped());

            // A reader arriving after the drop must be turned away.
            Assert.assertFalse(instance.tryLockForRead());
        });
    }

    private static void drainLiveViewQueue() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
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
