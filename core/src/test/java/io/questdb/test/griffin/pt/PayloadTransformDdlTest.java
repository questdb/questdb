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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pt.PayloadTransformDefinition;
import io.questdb.cairo.pt.PayloadTransformStore;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
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
    public void testRejectColumnNotInTargetTablePosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS price, 'extra' AS extra");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("column not found in target table [column=extra"));
                // Position must point within the SELECT SQL, not at 0
                Assert.assertTrue("error position should be > 0", e.getPosition() > 0);
            }
        });
    }

    @Test
    public void testRejectColumnNotInTargetTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS price, 'extra' AS extra");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("column not found in target table [column=extra"));
            }
        });
    }

    @Test
    public void testRejectDlqTableWithWrongColumnName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE TABLE bad_dlq (
                        ts TIMESTAMP,
                        wrong_name SYMBOL,
                        payload VARCHAR,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("DLQ table missing column [column=transform_name"));
            }
        });
    }

    @Test
    public void testRejectDlqTableWithWrongColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // payload and transform_name swapped
            execute("""
                    CREATE TABLE bad_dlq (
                        ts TIMESTAMP,
                        payload VARCHAR,
                        transform_name SYMBOL,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("DLQ table column in wrong position [column=transform_name"));
            }
        });
    }

    @Test
    public void testRejectDlqTableWithWrongColumnType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // transform_name is STRING instead of SYMBOL
            execute("""
                    CREATE TABLE bad_dlq (
                        ts TIMESTAMP,
                        transform_name STRING,
                        payload VARCHAR,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("DLQ table column type mismatch [column=transform_name"));
            }
        });
    }

    @Test
    public void testRejectDlqTableTooFewColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bad_dlq (ts TIMESTAMP, transform_name SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("DLQ table has incompatible schema"));
            }
        });
    }

    @Test
    public void testRejectDlqWithoutInsertPermission() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE TABLE my_dlq (
                        ts TIMESTAMP,
                        transform_name SYMBOL,
                        payload VARCHAR,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            engine.getPayloadTransformStore().init(sqlExecutionContext);
            drainWalQueue();

            // Security context that allows everything except INSERT on the DLQ table
            AllowAllSecurityContext noInsertCtx = new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    if ("my_dlq".equals(tableToken.getTableName())) {
                        throw CairoException.authorization().put("Write permission denied");
                    }
                }
            };
            SqlExecutionContextImpl restrictedCtx = new SqlExecutionContextImpl(engine, 1)
                    .with(noInsertCtx, bindVariableService, null, -1, null);
            try {
                execute("CREATE PAYLOAD TRANSFORM denied INTO target DLQ my_dlq PARTITION BY DAY AS " + VALID_SELECT, restrictedCtx);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("Write permission denied"));
            }
        });
    }

    @Test
    public void testAcceptDlqTableWithCorrectSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE TABLE good_dlq (
                        ts TIMESTAMP,
                        transform_name SYMBOL,
                        payload VARCHAR,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Should not throw
            execute("CREATE PAYLOAD TRANSFORM ok INTO target DLQ good_dlq PARTITION BY DAY AS " + VALID_SELECT);
        });
    }

    @Test
    public void testRejectIncompatibleColumnType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, value UUID) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS value");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage().contains("inconvertible types"));
                Assert.assertTrue("error position should be > 0", e.getPosition() > 0);
            }
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
