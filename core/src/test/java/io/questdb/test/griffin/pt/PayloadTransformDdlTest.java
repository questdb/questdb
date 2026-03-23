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

package io.questdb.test.griffin.pt;

import io.questdb.cairo.pt.PayloadTransformDefinition;
import io.questdb.cairo.pt.PayloadTransformStore;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PayloadTransformDdlTest extends AbstractCairoTest {

    private static final String VALID_SELECT = "SELECT now() AS ts, 'sym1' AS sym, 1.0 AS price";
    private static final String VALID_SELECT_2 = "SELECT now() AS ts, payload()::STRING AS sym, 2.0 AS price";

    @Test
    public void testAllowCte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM cte_transform INTO target AS WITH cte AS (SELECT now() AS ts, 'x' AS sym, 1.0 AS price) SELECT * FROM cte");
        });
    }

    @Test
    public void testAllowLongSequence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM seq_transform INTO target AS SELECT now() AS ts, 'sym' AS sym, x::DOUBLE AS price FROM long_sequence(1)");
        });
    }

    @Test
    public void testAllowPureExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM expr_transform INTO target AS " + VALID_SELECT);
        });
    }

    @Test
    public void testCreateOrReplacePayloadTransform() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();
            // Should replace without error
            execute("CREATE OR REPLACE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals(VALID_SELECT_2, def.getSelectSql());
        });
    }

    @Test
    public void testCreatePayloadTransform() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            Assert.assertTrue(store.isTransformExists(sqlExecutionContext, "my_transform"));
        });
    }

    @Test
    public void testCreatePayloadTransformDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            try {
                execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform already exists"));
            }
        });
    }

    @Test
    public void testCreatePayloadTransformIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();
            // Should not throw
            execute("CREATE PAYLOAD TRANSFORM IF NOT EXISTS my_transform INTO target AS " + VALID_SELECT);
        });
    }

    @Test
    public void testCreatePayloadTransformTargetDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE PAYLOAD TRANSFORM my_transform INTO nonexistent AS SELECT 1");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("target table does not exist"));
            }
        });
    }

    @Test
    public void testCreatePayloadTransformWithDlq() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target DLQ my_dlq PARTITION BY DAY TTL 7 DAYS AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals("my_dlq", def.getDlqTable());
        });
    }

    @Test
    public void testDropMultipleTransforms() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM t1 INTO target AS " + VALID_SELECT);
            execute("CREATE PAYLOAD TRANSFORM t2 INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            execute("DROP PAYLOAD TRANSFORM t1");
            drainWalQueue();
            PayloadTransformStore store = engine.getPayloadTransformStore();
            Assert.assertFalse(store.isTransformExists(sqlExecutionContext, "t1"));
            Assert.assertTrue(store.isTransformExists(sqlExecutionContext, "t2"));

            execute("DROP PAYLOAD TRANSFORM t2");
            drainWalQueue();
            Assert.assertFalse(store.isTransformExists(sqlExecutionContext, "t1"));
            Assert.assertFalse(store.isTransformExists(sqlExecutionContext, "t2"));
        });
    }

    @Test
    public void testDropPayloadTransform() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            Assert.assertTrue(store.isTransformExists(sqlExecutionContext, "my_transform"));

            execute("DROP PAYLOAD TRANSFORM my_transform");
            drainWalQueue();
            Assert.assertFalse(store.isTransformExists(sqlExecutionContext, "my_transform"));
        });
    }

    @Test
    public void testDropPayloadTransformDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("DROP PAYLOAD TRANSFORM nonexistent");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform does not exist"));
            }
        });
    }

    @Test
    public void testDropPayloadTransformIfExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("DROP PAYLOAD TRANSFORM IF EXISTS nonexistent");
        });
    }

    @Test
    public void testLookupNonexistentTransform() throws Exception {
        assertMemoryLeak(() -> {
            PayloadTransformStore store = engine.getPayloadTransformStore();
            store.init(sqlExecutionContext);
            drainWalQueue();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "nonexistent", def);
            Assert.assertNull(def);
        });
    }

    @Test
    public void testLookupTransform() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals("my_transform", def.getName());
            Assert.assertEquals("target", def.getTargetTable());
            Assert.assertEquals(VALID_SELECT, def.getSelectSql());
        });
    }

    @Test
    public void testRejectInsertStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS INSERT INTO target VALUES (now(), 'x', 1.0)");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform requires a SELECT query"));
            }
        });
    }

    @Test
    public void testRejectJoinWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE lookup (sym SYMBOL, label STRING)");
            try {
                execute("""
                        CREATE PAYLOAD TRANSFORM bad INTO target
                        AS SELECT now() AS ts, a.sym, 1.0 AS price
                        FROM (SELECT 'x' AS sym) a
                        JOIN lookup b ON a.sym = b.sym
                        """);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform must not reference tables"));
            }
        });
    }

    @Test
    public void testRejectTableInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT * FROM (SELECT * FROM target)");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform must not reference tables"));
            }
        });
    }

    @Test
    public void testRejectTableInUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS price UNION ALL SELECT * FROM target");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform must not reference tables"));
            }
        });
    }

    @Test
    public void testRejectTableReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT * FROM target");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("payload transform must not reference tables"));
            }
        });
    }

    @Test
    public void testShowPayloadTransforms() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM t1 INTO target AS " + VALID_SELECT);
            execute("CREATE PAYLOAD TRANSFORM t2 INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            assertSql(
                    "name\ttarget_table\tselect_sql\tdlq_table\n"
                            + "t1\ttarget\t" + VALID_SELECT + "\t\n"
                            + "t2\ttarget\t" + VALID_SELECT_2 + "\t\n",
                    "SHOW PAYLOAD TRANSFORMS"
            );
        });
    }

    @Test
    public void testShowPayloadTransformsEmpty() throws Exception {
        assertMemoryLeak(() -> {
            PayloadTransformStore store = engine.getPayloadTransformStore();
            store.init(sqlExecutionContext);
            drainWalQueue();

            assertSql(
                    "name\ttarget_table\tselect_sql\tdlq_table\n",
                    "SHOW PAYLOAD TRANSFORMS"
            );
        });
    }

    @Test
    public void testShowPayloadTransformsExcludesDropped() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM t1 INTO target AS " + VALID_SELECT);
            execute("CREATE PAYLOAD TRANSFORM t2 INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            execute("DROP PAYLOAD TRANSFORM t1");
            drainWalQueue();

            assertSql(
                    "name\ttarget_table\tselect_sql\tdlq_table\n"
                            + "t2\ttarget\t" + VALID_SELECT_2 + "\t\n",
                    "SHOW PAYLOAD TRANSFORMS"
            );
        });
    }
}
