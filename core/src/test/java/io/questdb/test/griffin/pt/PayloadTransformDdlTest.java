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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pt.PayloadTransformDefinition;
import io.questdb.cairo.pt.PayloadTransformStore;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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
            drainWalQueue();
            Assert.assertTrue(engine.getPayloadTransformStore().hasTransform(sqlExecutionContext, "cte_transform"));
        });
    }

    @Test
    public void testAllowLongSequence() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM seq_transform INTO target AS SELECT now() AS ts, 'sym' AS sym, x::DOUBLE AS price FROM long_sequence(1)");
            drainWalQueue();
            Assert.assertTrue(engine.getPayloadTransformStore().hasTransform(sqlExecutionContext, "seq_transform"));
        });
    }

    @Test
    public void testAllowUnnest() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM unnest_transform INTO target AS " +
                    "SELECT now() AS ts, 'sym' AS sym, value AS price FROM UNNEST(ARRAY[1.0, 2.0, 3.0])");
            drainWalQueue();
            Assert.assertTrue(engine.getPayloadTransformStore().hasTransform(sqlExecutionContext, "unnest_transform"));
        });
    }

    @Test
    public void testAllowPureExpressions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM expr_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();
            Assert.assertTrue(engine.getPayloadTransformStore().hasTransform(sqlExecutionContext, "expr_transform"));
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
            Assert.assertTrue(store.hasTransform(sqlExecutionContext, "my_transform"));
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
                TestUtils.assertContains(e.getMessage(), "payload transform already exists");
            }
        });
    }

    @Test
    public void testCreatePayloadTransformIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();
            // Should not throw; use different SQL to verify the original is preserved
            execute("CREATE PAYLOAD TRANSFORM IF NOT EXISTS my_transform INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals(VALID_SELECT, def.getSelectSql());
        });
    }

    @Test
    public void testCreatePayloadTransformTargetDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE PAYLOAD TRANSFORM my_transform INTO nonexistent AS SELECT 1");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "target table does not exist");
            }
        });
    }

    @Test
    public void testCreatePayloadTransformIdentifiersSurviveLexerReuse() throws Exception {
        // Regression for lexer aliasing: if the parser stored raw lexer flyweights
        // for the transform name / target table / DLQ table, subsequent lexing of
        // the PARTITION BY / TTL / AS tokens would mutate the underlying buffer
        // and the built operation would end up holding wrong identifiers. Use
        // distinctive names so any aliasing surfaces as a wrong-name comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE distinctive_target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM distinctive_xform INTO distinctive_target " +
                    "DLQ distinctive_dlq PARTITION BY DAY TTL 3 DAYS AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "distinctive_xform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals("distinctive_xform", def.getName());
            Assert.assertEquals("distinctive_target", def.getTargetTable());
            Assert.assertEquals("distinctive_dlq", def.getDlqTable());
            Assert.assertEquals(PartitionBy.DAY, def.getDlqPartitionBy());
            Assert.assertEquals(3, def.getDlqTtlValue());
            Assert.assertEquals("DAYS", def.getDlqTtlUnit());
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
            Assert.assertEquals(PartitionBy.DAY, def.getDlqPartitionBy());
            Assert.assertEquals(7, def.getDlqTtlValue());
            Assert.assertEquals("DAYS", def.getDlqTtlUnit());
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
            Assert.assertFalse(store.hasTransform(sqlExecutionContext, "t1"));
            Assert.assertTrue(store.hasTransform(sqlExecutionContext, "t2"));

            execute("DROP PAYLOAD TRANSFORM t2");
            drainWalQueue();
            Assert.assertFalse(store.hasTransform(sqlExecutionContext, "t1"));
            Assert.assertFalse(store.hasTransform(sqlExecutionContext, "t2"));
        });
    }

    @Test
    public void testDropPayloadTransform() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            Assert.assertTrue(store.hasTransform(sqlExecutionContext, "my_transform"));

            execute("DROP PAYLOAD TRANSFORM my_transform");
            drainWalQueue();
            Assert.assertFalse(store.hasTransform(sqlExecutionContext, "my_transform"));
        });
    }

    @Test
    public void testDropPayloadTransformDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("DROP PAYLOAD TRANSFORM nonexistent");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "payload transform does not exist");
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
                TestUtils.assertContains(e.getMessage(), "column not found in target table [column=extra");
                // Position must point within the SELECT SQL, not at 0
                Assert.assertTrue("error position should be > 0", e.getPosition() > 0);
            }
        });
    }

    @Test
    public void testCacheInvalidationAfterDropAndRecreate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();

            // Populate cache via lookup
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals(VALID_SELECT, def.getSelectSql());

            // Drop and recreate with different SQL
            execute("DROP PAYLOAD TRANSFORM my_transform");
            drainWalQueue();
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT_2);
            drainWalQueue();

            // Lookup must return the new SQL, not the stale cached value
            def = store.lookupTransform(sqlExecutionContext, "my_transform", def);
            Assert.assertNotNull(def);
            Assert.assertEquals(VALID_SELECT_2, def.getSelectSql());
        });
    }

    @Test
    public void testRejectCreateWithReadOnlyContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            SqlExecutionContextImpl readOnlyCtx = new SqlExecutionContextImpl(engine, 1)
                    .with(ReadOnlySecurityContext.INSTANCE, bindVariableService, null, -1, null);
            try {
                execute("CREATE PAYLOAD TRANSFORM denied INTO target AS " + VALID_SELECT, readOnlyCtx);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "Write permission denied");
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
            final String sql = "CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT;
            try {
                execute(sql);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "DLQ table missing column [column=transform_name");
                Assert.assertEquals("error should point at DLQ table name",
                        sql.indexOf("bad_dlq"), e.getPosition());
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
                TestUtils.assertContains(e.getMessage(), "DLQ table column in wrong position [column=transform_name");
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
                TestUtils.assertContains(e.getMessage(), "DLQ table column type mismatch [column=transform_name");
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
                TestUtils.assertContains(e.getMessage(), "DLQ table has incompatible schema");
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
                TestUtils.assertContains(e.getMessage(), "Write permission denied");
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
    public void testRejectDropWithReadOnlyContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM my_transform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            SqlExecutionContextImpl readOnlyCtx = new SqlExecutionContextImpl(engine, 1)
                    .with(ReadOnlySecurityContext.INSTANCE, bindVariableService, null, -1, null);
            try {
                execute("DROP PAYLOAD TRANSFORM my_transform", readOnlyCtx);
                Assert.fail("expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "Write permission denied");
            }
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
                TestUtils.assertContains(e.getMessage(), "inconvertible types");
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
                // The query body is parsed by the regular SELECT parser (parseViewSql),
                // which rejects INSERT as an unquoted keyword in the table-name position.
                TestUtils.assertContains(e.getMessage(), "INSERT");
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
                TestUtils.assertContains(e.getMessage(), "payload transform must not reference tables");
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
                TestUtils.assertContains(e.getMessage(), "payload transform must not reference tables");
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
                TestUtils.assertContains(e.getMessage(), "payload transform must not reference tables");
            }
        });
    }

    @Test
    public void testAllowCteReferenceFromInnerSubquery() throws Exception {
        // Regression for the CTE-scope check in validateNoTableReferences: a
        // nested subquery that references an outer-scope CTE name must NOT be
        // mis-classified as a table reference.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM nested_cte INTO target AS " +
                    "WITH cte AS (SELECT now() AS ts, 'x' AS sym, 1.0 AS price) " +
                    "SELECT * FROM (SELECT ts, sym, price FROM cte) sub");
            drainWalQueue();
            Assert.assertTrue(engine.getPayloadTransformStore().hasTransform(sqlExecutionContext, "nested_cte"));
        });
    }

    @Test
    public void testRejectTableHiddenInsideCte() throws Exception {
        // Regression for validateNoTableReferences CTE recursion: a table reference
        // hidden inside a CTE body must be detected, not just references at the
        // outer SELECT level.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE other_table (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS WITH cte AS (SELECT * FROM other_table) SELECT * FROM cte");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "payload transform must not reference tables");
                TestUtils.assertContains(e.getMessage(), "other_table");
            }
        });
    }

    @Test
    public void testRejectDuplicateOutputAlias() throws Exception {
        // Two SELECT columns aliased to the same name must be rejected. QuestDB's
        // regular SELECT parser enforces unique output column names, so the
        // rejection happens at parse time rather than inside validateTransformSql -
        // this test pins that behaviour so a future change to the parser cannot
        // silently let two source expressions map to the same target column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS price, 2.0 AS price");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "Duplicate column");
                TestUtils.assertContains(e.getMessage(), "price");
            }
        });
    }

    @Test
    public void testRejectDuplicateOutputAliasCaseInsensitive() throws Exception {
        // Mixed-case duplicates must also be rejected - the parser's column-name
        // check is case-insensitive, matching target column matching semantics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS SELECT now() AS ts, 'x' AS sym, 1.0 AS Price, 2.0 AS PRICE");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "Duplicate column");
            }
        });
    }

    @Test
    public void testIntoStripsPublicSchema() throws Exception {
        // Regression for INTO/DLQ name normalisation: a leading public. prefix
        // must be stripped (matching CREATE TABLE / CREATE MAT VIEW behaviour)
        // so the persisted target_table is the bare name.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM stripped INTO public.target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition def = new PayloadTransformDefinition();
            def = store.lookupTransform(sqlExecutionContext, "stripped", def);
            Assert.assertNotNull(def);
            Assert.assertEquals("target", def.getTargetTable());
        });
    }

    @Test
    public void testCreateOrReplaceLeavesOriginalLiveOnValidationFailure() throws Exception {
        // Regression for the non-destructive CREATE OR REPLACE fix: if the
        // replacement body fails validation, the previous definition must
        // remain live and unchanged - the live ingest path must not be taken
        // offline by a bad DDL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE PAYLOAD TRANSFORM live_xform INTO target AS " + VALID_SELECT);
            drainWalQueue();

            PayloadTransformStore store = engine.getPayloadTransformStore();
            PayloadTransformDefinition before = store.lookupTransform(sqlExecutionContext, "live_xform", new PayloadTransformDefinition());
            Assert.assertNotNull(before);
            final String beforeSql = before.getSelectSql();

            // Attempt OR REPLACE with a body that references a column that does
            // not exist in the target table - this fails inside validateTransformSql
            // which used to run AFTER the early dropTransform.
            try {
                execute("CREATE OR REPLACE PAYLOAD TRANSFORM live_xform INTO target AS " +
                        "SELECT now() AS ts, 'x' AS sym, 1.0 AS price, 99.0 AS not_a_column");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "column not found in target table");
            }
            drainWalQueue();

            // Original transform must still be live AND unchanged.
            PayloadTransformDefinition after = store.lookupTransform(sqlExecutionContext, "live_xform", new PayloadTransformDefinition());
            Assert.assertNotNull("original transform must still be live after failed REPLACE", after);
            Assert.assertEquals(beforeSql, after.getSelectSql());
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
                TestUtils.assertContains(e.getMessage(), "payload transform must not reference tables");
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

    @Test
    public void testRejectDlqTableWithWrongDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // DLQ with correct columns but 'error' as designated timestamp instead of 'ts'
            execute("""
                    CREATE TABLE bad_dlq (
                        ts TIMESTAMP,
                        transform_name SYMBOL,
                        payload VARCHAR,
                        query VARCHAR,
                        stage SYMBOL,
                        error VARCHAR,
                        other_ts TIMESTAMP
                    ) TIMESTAMP(other_ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target DLQ bad_dlq PARTITION BY DAY AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "DLQ table must have 'ts' as designated timestamp");
            }
        });
    }

    @Test
    public void testRejectNonWalTargetTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS " + VALID_SELECT);
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "target table must be WAL-enabled");
            }
        });
    }

    @Test
    public void testRejectTrailingTokensAfterSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE PAYLOAD TRANSFORM bad INTO target AS (SELECT now() AS ts, 'x' AS sym, 1.0 AS price) JUNK");
                Assert.fail("expected SqlException");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "unexpected token");
            }
        });
    }
}
