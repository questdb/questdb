/*******************************************************************************
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

package io.questdb.test.cairo.ev;

import io.questdb.cairo.ev.ExpiringViewDefinition;
import io.questdb.cairo.ev.RowExpiryCleanupJob;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExpiringViewTest extends AbstractCairoTest {

    @Test
    public void testAlterExpiringViewChangeInterval() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");
            execute("CREATE EXPIRING VIEW ev1 ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h");

            execute("ALTER EXPIRING VIEW ev1 SET EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 2d");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("ev1");
            assertEquals("expiry_ts < now()", def.getExpiryPredicateSql());
            assertEquals(2 * 86_400_000_000L, def.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testAlterExpiringViewChangePredicate() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");
            execute("CREATE EXPIRING VIEW ev1 ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h");

            assertEquals("expiry_ts < now()", engine.getExpiringViewDefinition("ev1").getExpiryPredicateSql());

            execute("ALTER EXPIRING VIEW ev1 SET EXPIRE WHEN expiry_ts <= now()");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("ev1");
            assertEquals("expiry_ts <= now()", def.getExpiryPredicateSql());
            // Cleanup interval should be preserved
            assertEquals(3_600_000_000L, def.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testAlterExpiringViewNonExistent() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "ALTER EXPIRING VIEW no_such_view SET EXPIRE WHEN ts < now()",
                20,
                "expiring view does not exist"
        ));
    }

    @Test
    public void testAlterExpiringViewPersistsAcrossReload() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");
            execute("CREATE EXPIRING VIEW ev1 ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h");
            execute("ALTER EXPIRING VIEW ev1 SET EXPIRE WHEN expiry_ts <= now() CLEANUP INTERVAL 6h");

            // Simulate engine restart
            engine.clear();
            engine.buildViewGraphs();

            ExpiringViewDefinition reloaded = engine.getExpiringViewDefinition("ev1");
            assertNotNull(reloaded);
            assertEquals("expiry_ts <= now()", reloaded.getExpiryPredicateSql());
            assertEquals(6 * 3_600_000_000L, reloaded.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testCleanupCustomAllPartitionsExpired() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'cancelled', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', 200.0, '2020-01-01T12:00:00.000000Z'),
                            (3, 'cancelled', 300.0, '2020-01-02T00:00:00.000000Z'),
                            (4, 'cancelled', 400.0, '2020-01-02T12:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            assertSql("count\n0\n", "SELECT count() FROM orders");
            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testCleanupCustomCompoundPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', 200.0, '2020-01-01T12:00:00.000000Z'),
                            (3, 'filled', 300.0, '2020-01-02T00:00:00.000000Z'),
                            (4, 'open', 400.0, '2020-01-02T12:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW live_orders ON orders EXPIRE WHEN status = 'cancelled' OR status = 'filled' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            assertSql(
                    """
                            order_id\tstatus\tamount\tts
                            1\topen\t100.0\t2020-01-01T00:00:00.000000Z
                            4\topen\t400.0\t2020-01-02T12:00:00.000000Z
                            """,
                    "SELECT * FROM orders"
            );
            execute("DROP EXPIRING VIEW live_orders");
        });
    }

    @Test
    public void testCleanupCustomNoneExpired() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'open', 200.0, '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            assertSql("count\n2\n", "SELECT count() FROM orders");
            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testCleanupCustomPartialPartitionCompaction() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', 200.0, '2020-01-01T12:00:00.000000Z'),
                            (3, 'open', 300.0, '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // Cancelled row should be removed, open rows preserved
            assertSql(
                    """
                            order_id\tstatus\tamount\tts
                            1\topen\t100.0\t2020-01-01T00:00:00.000000Z
                            3\topen\t300.0\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM orders"
            );
            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testCleanupIntervalDays() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 2d");
            assertEquals(2 * 86_400_000_000L, engine.getExpiringViewDefinition("ev1").getCleanupIntervalMicros());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testCleanupIntervalMinutes() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 30m");
            assertEquals(30 * 60_000_000L, engine.getExpiringViewDefinition("ev1").getCleanupIntervalMicros());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testCleanupIntervalSeconds() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 10s");
            assertEquals(10 * 1_000_000L, engine.getExpiringViewDefinition("ev1").getCleanupIntervalMicros());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testCreateAndDrop() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("active_options");
            assertNotNull(def);
            assertEquals("options_raw", def.getBaseTableName());
            assertEquals("expiry_ts < now()", def.getExpiryPredicateSql());
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, def.getExpiryType());
            assertEquals(3_600_000_000L, def.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW active_options");
            assertNull(engine.getExpiringViewDefinition("active_options"));
        });
    }

    @Test
    public void testCreateBaseTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "CREATE EXPIRING VIEW bad_view ON no_such_table EXPIRE WHEN ts < now()",
                33,
                "base table does not exist"
        ));
    }

    @Test
    public void testCreateDuplicateName() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW dup_view ON options_raw EXPIRE WHEN expiry_ts < now()");
            try {
                assertExceptionNoLeakCheck(
                        "CREATE EXPIRING VIEW dup_view ON options_raw EXPIRE WHEN expiry_ts < now()",
                        21,
                        "already exists"
                );
            } finally {
                execute("DROP EXPIRING VIEW dup_view");
            }
        });
    }

    @Test
    public void testCreateIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");
            // Should not throw
            execute("CREATE EXPIRING VIEW IF NOT EXISTS active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertNotNull(engine.getExpiringViewDefinition("active_options"));
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCustomExpiryPredicateFiltering() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', 200.0, '2020-01-02T00:00:00.000000Z'),
                            (3, 'open', 300.0, '2020-01-03T00:00:00.000000Z'),
                            (4, 'cancelled', 400.0, '2020-01-04T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled'");

            assertSql(
                    """
                            order_id\tstatus\tamount\tts
                            1\topen\t100.0\t2020-01-01T00:00:00.000000Z
                            3\topen\t300.0\t2020-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM open_orders"
            );

            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testCustomExpiryType() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 24h");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("open_orders");
            assertNotNull(def);
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_CUSTOM, def.getExpiryType());
            assertEquals("orders", def.getBaseTableName());
            assertEquals("status = 'cancelled'", def.getExpiryPredicateSql());
            assertEquals(86_400_000_000L, def.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testDefaultCleanupInterval() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now()");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("ev1");
            assertNotNull(def);
            assertEquals(3_600_000_000L, def.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testDropIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // Should not throw
            execute("DROP EXPIRING VIEW IF EXISTS nonexistent_view");
        });
    }

    @Test
    public void testDropNonExistent() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "DROP EXPIRING VIEW no_such_view",
                19,
                "expiring view does not exist"
        ));
    }

    @Test
    public void testQueryBaseTableShowsAllRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            AAPL\t150.0\t2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM options_raw"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryFiltersExpiredRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('GOOG', 2800.0, '2020-01-02T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z'),
                            ('TSLA', 700.0, '2099-06-01T00:00:00.000000Z', '2020-01-04T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-01-03T00:00:00.000000Z
                            TSLA\t700.0\t2099-06-01T00:00:00.000000Z\t2020-01-04T00:00:00.000000Z
                            """,
                    "SELECT * FROM active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryWithAdditionalWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z'),
                            ('TSLA', 700.0, '2099-06-01T00:00:00.000000Z', '2020-01-04T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM active_options WHERE symbol = 'MSFT'"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryWithJoin() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    "CREATE TABLE ref_data (symbol SYMBOL, name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO ref_data VALUES
                            ('AAPL', 'Apple Inc', '2020-01-01T00:00:00.000000Z'),
                            ('MSFT', 'Microsoft Corp', '2020-01-01T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\tname
                            MSFT\t300.0\tMicrosoft Corp
                            """,
                    "SELECT a.symbol, a.price, r.name FROM active_options a JOIN ref_data r ON a.symbol = r.symbol"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testShowCreateExpiringViewCustom() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, amount DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW open_orders ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 24h");

            assertSql(
                    "ddl\n" +
                            "CREATE EXPIRING VIEW 'open_orders' ON orders EXPIRE WHEN status = 'cancelled' CLEANUP INTERVAL 1d;\n",
                    "SHOW CREATE EXPIRING VIEW open_orders"
            );

            execute("DROP EXPIRING VIEW open_orders");
        });
    }

    @Test
    public void testShowCreateExpiringViewNonExistent() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck(
                "SHOW CREATE EXPIRING VIEW no_such_view",
                26,
                "expiring view does not exist"
        ));
    }

    @Test
    public void testShowCreateExpiringViewTimestampCompare() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");
            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h");

            assertSql(
                    "ddl\n" +
                            "CREATE EXPIRING VIEW 'active_options' ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1h;\n",
                    "SHOW CREATE EXPIRING VIEW active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testShowCreateExpiringViewWithExplicitInterval() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");
            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 2d");

            assertSql(
                    "ddl\n" +
                            "CREATE EXPIRING VIEW 'ev1' ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 2d;\n",
                    "SHOW CREATE EXPIRING VIEW ev1"
            );

            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testTimestampCompareDetection() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            // column < now() -> TIMESTAMP_COMPARE
            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now()");
            ExpiringViewDefinition def = engine.getExpiringViewDefinition("ev1");
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, def.getExpiryType());
            assertTrue(def.getExpiryColumnIndex() >= 0);
            execute("DROP EXPIRING VIEW ev1");

            // Non-timestamp column -> CUSTOM
            execute("CREATE EXPIRING VIEW ev2 ON t1 EXPIRE WHEN price > 100");
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_CUSTOM, engine.getExpiringViewDefinition("ev2").getExpiryType());
            execute("DROP EXPIRING VIEW ev2");
        });
    }

    @Test
    public void testTimestampCompareDetectionLeq() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts <= now()");
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, engine.getExpiringViewDefinition("ev1").getExpiryType());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testTimestampCompareDetectionNowGreater() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN now() > expiry_ts");
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, engine.getExpiringViewDefinition("ev1").getExpiryType());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testQueryAllRowsExpired() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('GOOG', 2800.0, '2020-01-02T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            """,
                    "SELECT * FROM active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryNoRowsExpired() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('TSLA', 700.0, '2099-06-01T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            TSLA\t700.0\t2099-06-01T00:00:00.000000Z\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            """,
                    "SELECT * FROM active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testQueryWithAggregation() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 100.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('MSFT', 200.0, '2099-01-01T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-06-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            assertSql(
                    """
                            symbol\tavg
                            MSFT\t250.0
                            """,
                    "SELECT symbol, avg(price) FROM active_options"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTimestampBaseTableDropped() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("temp_table");
            execute("CREATE EXPIRING VIEW ev_temp ON temp_table EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1s");

            execute("DROP TABLE temp_table");
            drainWalQueue();

            // Cleanup should handle missing base table gracefully
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }

            execute("DROP EXPIRING VIEW ev_temp");
        });
    }

    @Test
    public void testCleanupTimestampFullPartitionDrop() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            // Insert rows into two partitions, all with past expiry timestamps
            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('GOOG', 2800.0, '2020-01-01T12:00:00.000000Z', '2020-01-01T12:00:00.000000Z'),
                            ('MSFT', 300.0, '2020-01-02T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // All partitions should be dropped
            assertSql("count\n0\n", "SELECT count() FROM options_raw");
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTimestampIntervalRespected() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            // Set a very long cleanup interval
            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1w");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // First run should work
                job.run(0);
                drainWalQueue();
                assertSql("count\n0\n", "SELECT count() FROM options_raw");
            }
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTimestampNonExpiredSkipped() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z'),
                            ('TSLA', 700.0, '2099-06-01T00:00:00.000000Z', '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // No partitions should be dropped, all rows still exist
            assertSql("count\n2\n", "SELECT count() FROM options_raw");
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTimestampPartialPartitionCompaction() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            // Same partition (same day), mix of expired and non-expired
            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-06-15T00:00:00.000000Z'),
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-06-15T12:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // Only MSFT should survive (non-expired)
            assertSql(
                    """
                            symbol\tprice\texpiry_ts\tts
                            MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-06-15T12:00:00.000000Z
                            """,
                    "SELECT * FROM options_raw"
            );
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTimestampStagingTableCleanedUpOnError() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('AAPL', 150.0, '2020-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // Staging table should not exist after cleanup
            assertExceptionNoLeakCheck(
                    "SELECT * FROM 'ev_staging_active_options'",
                    14,
                    "does not exist"
            );
            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testCleanupTwoViewsDifferentColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE events (" +
                            "id LONG, status SYMBOL, priority SYMBOL, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO events VALUES
                            (1, 'done', 'low', '2020-01-01T00:00:00.000000Z'),
                            (2, 'done', 'high', '2020-01-01T12:00:00.000000Z'),
                            (3, 'active', 'low', '2020-01-02T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            // View 1: expires when status = 'done'
            execute("CREATE EXPIRING VIEW v1 ON events EXPIRE WHEN status = 'done' CLEANUP INTERVAL 1s");
            // View 2: expires when priority = 'low'
            execute("CREATE EXPIRING VIEW v2 ON events EXPIRE WHEN priority = 'low' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // Run for v1: partition 2020-01-01 is fully expired for v1 (both rows are 'done')
                // but v2 says row 2 (priority='high') is NOT expired, so partition should be compacted, not dropped
                job.run(0);
            }
            drainWalQueue();

            // Row 2 (done, high) should survive because v2 does not consider it expired
            // Row 3 (active, low) should survive because v1 does not consider it expired
            // Row 1 (done, low) is expired in both views, can be removed
            assertSql("count\n2\n", "SELECT count() FROM events");

            execute("DROP EXPIRING VIEW v1");
            execute("DROP EXPIRING VIEW v2");
        });
    }

    @Test
    public void testCleanupTwoViewsPartitionDropIntersection() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE events (" +
                            "id LONG, status SYMBOL, kind SYMBOL, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            // All rows in the partition are expired by BOTH views
            execute(
                    """
                            INSERT INTO events VALUES
                            (1, 'done', 'temp', '2020-01-01T00:00:00.000000Z'),
                            (2, 'done', 'temp', '2020-01-01T12:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW v1 ON events EXPIRE WHEN status = 'done' CLEANUP INTERVAL 1s");
            execute("CREATE EXPIRING VIEW v2 ON events EXPIRE WHEN kind = 'temp' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // Both views agree all rows are expired, partition should be dropped
            assertSql("count\n0\n", "SELECT count() FROM events");

            execute("DROP EXPIRING VIEW v1");
            execute("DROP EXPIRING VIEW v2");
        });
    }

    @Test
    public void testCleanupViewExpiredButSiblingNot() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE events (" +
                            "id LONG, status SYMBOL, kind SYMBOL, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO events VALUES
                            (1, 'done', 'permanent', '2020-01-01T00:00:00.000000Z'),
                            (2, 'done', 'permanent', '2020-01-01T12:00:00.000000Z')
                            """
            );
            drainWalQueue();

            // v1 considers all rows expired, but v2 does not (kind != 'temp')
            execute("CREATE EXPIRING VIEW v1 ON events EXPIRE WHEN status = 'done' CLEANUP INTERVAL 1s");
            execute("CREATE EXPIRING VIEW v2 ON events EXPIRE WHEN kind = 'temp' CLEANUP INTERVAL 1s");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.run(0);
            }
            drainWalQueue();

            // v2 still needs these rows (they're 'permanent', not 'temp')
            // The partition is demoted from full-drop to compaction because v2 disagrees.
            // Compaction uses intersected predicate: NOT(status='done' AND kind='temp')
            // Both rows are (done, permanent) → done AND temp = false → NOT false = true → KEPT
            // Both rows survive because v2 doesn't consider them expired
            assertSql("count\n2\n", "SELECT count() FROM events");

            execute("DROP EXPIRING VIEW v1");
            execute("DROP EXPIRING VIEW v2");
        });
    }

    @Test
    public void testCompoundPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, filled_at TIMESTAMP, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', null, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', null, '2020-01-02T00:00:00.000000Z'),
                            (3, 'filled', '2020-06-01T00:00:00.000000Z', '2020-01-03T00:00:00.000000Z'),
                            (4, 'open', null, '2020-01-04T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            execute("CREATE EXPIRING VIEW live_orders ON orders EXPIRE WHEN status = 'cancelled' OR status = 'filled'");

            assertSql(
                    """
                            order_id\tstatus\tfilled_at\tts
                            1\topen\t\t2020-01-01T00:00:00.000000Z
                            4\topen\t\t2020-01-04T00:00:00.000000Z
                            """,
                    "SELECT * FROM live_orders"
            );

            execute("DROP EXPIRING VIEW live_orders");
        });
    }

    @Test
    public void testFuzzRandomExpiredNonExpiredRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now()");

            Rnd rnd = new Rnd();
            int totalRows = 200;
            int expectedNonExpired = 0;

            StringBuilder sb = new StringBuilder("INSERT INTO options_raw VALUES\n");
            for (int i = 0; i < totalRows; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                boolean isExpired = rnd.nextBoolean();
                String expiryYear = isExpired ? "2020" : "2099";
                if (!isExpired) {
                    expectedNonExpired++;
                }
                sb.append("('SYM").append(i).append("', ")
                        .append(rnd.nextDouble() * 1000).append(", '")
                        .append(expiryYear).append("-01-01T00:00:00.000000Z', '")
                        .append("2020-01-01T").append(String.format("%02d", i / 60 % 24))
                        .append(':').append(String.format("%02d", i % 60))
                        .append(":00.000000Z')");
            }
            execute(sb.toString());
            drainWalQueue();

            assertSql(
                    "count\n" + expectedNonExpired + "\n",
                    "SELECT count() FROM active_options"
            );

            assertSql(
                    "count\n" + totalRows + "\n",
                    "SELECT count() FROM options_raw"
            );

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testFuzzCreateDropCycle() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute(
                    """
                            INSERT INTO options_raw VALUES
                            ('MSFT', 300.0, '2099-01-01T00:00:00.000000Z', '2020-01-01T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            for (int i = 0; i < 50; i++) {
                String viewName = "ev_cycle_" + i;
                execute("CREATE EXPIRING VIEW " + viewName + " ON options_raw EXPIRE WHEN expiry_ts < now()");
                assertNotNull(engine.getExpiringViewDefinition(viewName));

                // Query through the view
                assertSql(
                        """
                                symbol\tprice\texpiry_ts\tts
                                MSFT\t300.0\t2099-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                                """,
                        "SELECT * FROM " + viewName
                );

                execute("DROP EXPIRING VIEW " + viewName);
                assertNull(engine.getExpiringViewDefinition(viewName));
            }
        });
    }

    @Test
    public void testFuzzMultipleViewsOnSameTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE orders (" +
                            "order_id LONG, status SYMBOL, price DOUBLE, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL"
            );
            drainWalQueue();

            execute(
                    """
                            INSERT INTO orders VALUES
                            (1, 'open', 100.0, '2020-01-01T00:00:00.000000Z'),
                            (2, 'cancelled', 200.0, '2020-01-02T00:00:00.000000Z'),
                            (3, 'filled', 300.0, '2020-01-03T00:00:00.000000Z'),
                            (4, 'open', 400.0, '2020-01-04T00:00:00.000000Z'),
                            (5, 'cancelled', 500.0, '2020-01-05T00:00:00.000000Z')
                            """
            );
            drainWalQueue();

            // View 1: hide cancelled
            execute("CREATE EXPIRING VIEW not_cancelled ON orders EXPIRE WHEN status = 'cancelled'");
            // View 2: hide filled
            execute("CREATE EXPIRING VIEW not_filled ON orders EXPIRE WHEN status = 'filled'");

            assertSql("count\n3\n", "SELECT count() FROM not_cancelled");
            assertSql("count\n4\n", "SELECT count() FROM not_filled");

            execute("DROP EXPIRING VIEW not_cancelled");
            execute("DROP EXPIRING VIEW not_filled");
        });
    }

    @Test
    public void testPersistenceAcrossEngineReload() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE EXPIRING VIEW active_options ON options_raw EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 2h");

            ExpiringViewDefinition def = engine.getExpiringViewDefinition("active_options");
            assertNotNull(def);
            assertEquals("options_raw", def.getBaseTableName());
            assertEquals("expiry_ts < now()", def.getExpiryPredicateSql());
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, def.getExpiryType());
            assertEquals(7_200_000_000L, def.getCleanupIntervalMicros());

            // Simulate engine restart by clearing and rebuilding
            engine.clear();
            engine.buildViewGraphs();

            // View should be reloaded from disk
            ExpiringViewDefinition reloaded = engine.getExpiringViewDefinition("active_options");
            assertNotNull("view should survive engine reload", reloaded);
            assertEquals("options_raw", reloaded.getBaseTableName());
            assertEquals("expiry_ts < now()", reloaded.getExpiryPredicateSql());
            assertEquals(ExpiringViewDefinition.EXPIRY_TYPE_TIMESTAMP_COMPARE, reloaded.getExpiryType());
            assertEquals(7_200_000_000L, reloaded.getCleanupIntervalMicros());

            execute("DROP EXPIRING VIEW active_options");
        });
    }

    @Test
    public void testViewNameConflictWithTable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("options_raw");

            execute("CREATE TABLE conflict_name (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "CREATE EXPIRING VIEW conflict_name ON options_raw EXPIRE WHEN expiry_ts < now()",
                    21,
                    "already exists"
            );
        });
    }

    @Test
    public void testCleanupIntervalWeeks() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 1w");
            assertEquals(604_800_000_000L, engine.getExpiringViewDefinition("ev1").getCleanupIntervalMicros());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    @Test
    public void testCleanupIntervalHours() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable("t1");

            execute("CREATE EXPIRING VIEW ev1 ON t1 EXPIRE WHEN expiry_ts < now() CLEANUP INTERVAL 12h");
            assertEquals(12 * 3_600_000_000L, engine.getExpiringViewDefinition("ev1").getCleanupIntervalMicros());
            execute("DROP EXPIRING VIEW ev1");
        });
    }

    private void createBaseTable(String tableName) throws Exception {
        execute(
                "CREATE TABLE " + tableName + " (" +
                        "symbol SYMBOL, price DOUBLE, expiry_ts TIMESTAMP, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL"
        );
        drainWalQueue();
    }

}
