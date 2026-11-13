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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * ALTER MATERIALIZED VIEW ... DROP COLUMN of a column that the view's EXPIRE ROWS policy reads. Every read of a
 * view with a policy goes through the policy's keep-filter, LATEST ON or window rewrite, which references the
 * columns the policy names, so dropping one of them would leave a view that no query can read. The statement is
 * refused at the column name, like the drop of the designated timestamp, and names the policy and the way out:
 * change the policy with SET EXPIRE, or remove it with DROP EXPIRE, first. Columns the policy does not read drop
 * as usual. The check reads the policy the view has applied, so a SET EXPIRE or DROP EXPIRE still in the view's WAL
 * does not count yet.
 * <p>
 * ALTER MATERIALIZED VIEW ... SET EXPIRE checks the columns of a scalar WHEN or KEEP policy against the view's
 * sequencer metadata, which holds the view's ADD and DROP COLUMN statements still in its WAL: a policy that reads a
 * column a pending DROP COLUMN removes fails at compile instead of at apply, which would suspend the view's WAL, and
 * a scalar WHEN policy that reads a column a pending ADD COLUMN adds is accepted. The read probe of a KEEP policy
 * still compiles against the view's applied columns.
 * <p>
 * Expected rows follow from the base rows each test inserts and the policy's documented semantics: WHEN hides
 * the rows its predicate holds for, KEEP LATEST keeps the newest row per key, KEEP [N] HIGHEST / LOWEST keeps the
 * rows with the highest or lowest values per key.
 */
public class MatViewDropColumnExpiryPolicyTest extends AbstractCairoTest {
    // The buckets of createSampledView() that the policies of the tests that use it leave visible.
    private static final String SAMPLED_VISIBLE = """
            ts\tsym\tprice\tamount
            2024-01-01T00:00:00.000000Z\ta\t3.5\t30.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            """;
    private static final String SAMPLED_VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Test
    public void testDropColumnAllowedAfterDropExpire() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);
            assertDropColumnRefused("price_1h", "price", 2);

            execute("ALTER MATERIALIZED VIEW price_1h DROP EXPIRE");
            // The DROP EXPIRE waits in the view's WAL, so reads of the view still apply the policy.
            assertDropColumnFails("price_1h", "price");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalAndMatViewQueues();
            // Without the policy the bucket the policy hid (b at 2.5) is visible again.
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t40.0
                    """);
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testDropColumnAllowedAfterSetExpireStopsReadingIt() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0 AND amount > 15.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);
            // A predicate over two columns reads both.
            assertDropColumnRefused("price_1h", "price", 2);
            assertDropColumnRefused("price_1h", "amount", 2);

            execute("ALTER MATERIALIZED VIEW price_1h SET EXPIRE ROWS WHEN amount > 35.0");
            // The SET EXPIRE waits in the view's WAL, so reads of the view still apply the policy that reads price.
            assertDropColumnFails("price_1h", "price");
            drainWalAndMatViewQueues();
            assertDropColumnRefused("price_1h", "amount", 2);
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalAndMatViewQueues();
            // The new policy hides the bucket whose amount is above 35 (a at 01:00).
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    """);
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testDropColumnNamedOnlyAsTextOrCallInPolicyAllowed() throws Exception {
        // The policy names abs as a function and sym as a string, and reads neither column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, abs DOUBLE, note VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, 10.0, 'keep', '2024-01-01T00:00:00.000000Z'),
                    ('b', -3.0, 20.0, 'keep', '2024-01-02T00:00:00.000000Z'),
                    ('c', 1.5, 30.0, 'sym', '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN abs(v) > 2.0 OR note = 'sym'");
            drainWalAndMatViewQueues();
            // b expires on |v| > 2, c on its note
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    sym\tv\tabs\tnote\tts
                    a\t1.0\t10.0\tkeep\t2024-01-01T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 1);
            assertDropColumnRefused("mv", "note", 1);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN abs");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN sym");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    v\tnote\tts
                    1.0\tkeep\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnLexicalCheckEdges() throws Exception {
        // The check looks for the column's name among the policy's tokens and errs on the side of refusal (design
        // section 11, rule 8): a name the policy uses as a type in a cast, as a keyword or as the view's prefix counts
        // as read. A name followed by '(' is a function call, also with a comment or a line break before the
        // parenthesis, and does not count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, \"long\" DOUBLE, mv DOUBLE, \"and\" DOUBLE, abs DOUBLE, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, 10.0, 20.0, 30.0, 40.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', -3.0, 11.0, 21.0, 31.0, 41.0, '2024-01-02T00:00:00.000000Z'),
                    ('C', 5.0, 12.0, 22.0, 32.0, 42.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)");
            drainWalAndMatViewQueues();
            // the policy reads v and k: B and C expire, |v| is above 2 and k is not A
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN abs /* a call */ (v::long) > 2 AND k <> 'A'");
            drainWalAndMatViewQueues();
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tlong\tmv\tand\tabs\tts
                    A\t1.0\t10.0\t20.0\t30.0\t40.0\t2024-01-01T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 1);
            assertDropColumnRefused("mv", "k", 1);
            // a type name in a cast and a keyword count as read
            assertDropColumnRefused("mv", "\"long\"", 1);
            assertDropColumnRefused("mv", "\"and\"", 1);
            // abs is a call, with a comment before its parenthesis
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN abs");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tlong\tmv\tand\tts
                    A\t1.0\t10.0\t20.0\t30.0\t2024-01-01T00:00:00.000000Z
                    """);

            // A window policy that reads v with the view's name as a prefix, and calls abs across a line break. No row
            // has |v| below its key's maximum v, so every row stays.
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN abs DOUBLE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN abs\n(mv.v) < max(mv.v) OVER (PARTITION BY k)");
            drainWalAndMatViewQueues();
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tlong\tmv\tand\tts\tabs
                    A\t1.0\t10.0\t20.0\t30.0\t2024-01-01T00:00:00.000000Z\tnull
                    B\t-3.0\t11.0\t21.0\t31.0\t2024-01-02T00:00:00.000000Z\tnull
                    C\t5.0\t12.0\t22.0\t32.0\t2024-01-03T00:00:00.000000Z\tnull
                    """);
            // the view's prefix counts as read
            assertDropColumnRefused("mv", "mv", 3);
            assertDropColumnRefused("mv", "v", 3);
            assertDropColumnRefused("mv", "k", 3);
            // the columns the new policy does not name drop
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN abs");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN \"long\"");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN \"and\"");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tmv\tts
                    A\t1.0\t20.0\t2024-01-01T00:00:00.000000Z
                    B\t-3.0\t21.0\t2024-01-02T00:00:00.000000Z
                    C\t5.0\t22.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnNonAsciiNameMatchedIgnoringCase() throws Exception {
        // The policy and the statement name the columns in any letter case, quoted or not; the error names the
        // column as the view has it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, \"Ärger\" DOUBLE, \"Ωmega\" DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.5, 3.0, '2024-01-01T00:20:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN \"ärger\" < 2.0 AND ωMEGA < 2.0");
            drainWalAndMatViewQueues();
            assertExpireClause("\"ärger\" < 2.0 AND ωMEGA < 2.0");
            // a expires: 1.5 < 2.0 and 1.0 < 2.0
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    sym\tÄrger\tΩmega\tts
                    b\t2.5\t3.0\t2024-01-01T00:20:00.000000Z
                    """);
            for (String column : new String[]{"ÄRGER", "\"ÄRGER\"", "Ärger", "ärger", "\"ärger\""}) {
                assertDropColumnRefused("mv", column, 1);
                assertExceptionNoLeakCheck(
                        "ALTER MATERIALIZED VIEW mv DROP COLUMN " + column,
                        39,
                        "the EXPIRE ROWS policy of the materialized view reads the column [column=Ärger]"
                );
            }
            for (String column : new String[]{"ΩMEGA", "ωmega", "\"ωMEGA\"", "Ωmega"}) {
                assertDropColumnRefused("mv", column, 1);
                assertExceptionNoLeakCheck(
                        "ALTER MATERIALIZED VIEW mv DROP COLUMN " + column,
                        39,
                        "the EXPIRE ROWS policy of the materialized view reads the column [column=Ωmega]"
                );
            }
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN SYM");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv").timestamp("ts").noLeakCheck().returns("""
                    Ärger\tΩmega\tts
                    2.5\t3.0\t2024-01-01T00:20:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnRefusedAfterRestart() throws Exception {
        // The check reads the policy from the metadata a restarted engine loads.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
                    ('a', 3.5, 30.0, '2024-01-01T00:30:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS KEEP LATEST PARTITION BY sym");
            drainWalAndMatViewQueues();
            engine.releaseInactive();
            engine.releaseAllWriters();
            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                final String sql = "ALTER MATERIALIZED VIEW mv DROP COLUMN sym";
                try {
                    restarted.execute(sql, context);
                    Assert.fail("the statement must fail: " + sql);
                } catch (SqlException e) {
                    Assert.assertEquals(sql.indexOf("sym", sql.indexOf("COLUMN")), e.getPosition());
                    TestUtils.assertEquals(
                            "the EXPIRE ROWS policy of the materialized view reads the column [column=sym], change the policy "
                                    + "with SET EXPIRE or remove it with DROP EXPIRE first, and retry once that has applied",
                            e.getFlyweightMessage()
                    );
                }
                restarted.execute("ALTER MATERIALIZED VIEW mv DROP COLUMN amount", context);
                drainWalAndMatViewQueues(restarted);
                // the newest row of each sym: a at 3.5 and b at 2.5
                assertQuery("mv ORDER BY ts").withEngine(restarted).withContext(context).timestamp("ts").noLeakCheck().expectSize().returns("""
                        sym\tprice\tts
                        b\t2.5\t2024-01-01T00:20:00.000000Z
                        a\t3.5\t2024-01-01T00:30:00.000000Z
                        """);
                assertQuery("SELECT view_status, expire_clause FROM materialized_views()").withEngine(restarted).withContext(context)
                        .noLeakCheck().noRandomAccess().returns("""
                                view_status\texpire_clause
                                valid\tKEEP LATEST PARTITION BY sym
                                """);
            }
        });
    }

    @Test
    public void testKeepLatestKeyNoLongerProducedByBaseDropColumn() throws Exception {
        // A passthrough view keeps a column the base drops, which reads NULL in the rows refreshed from then on. A
        // KEEP LATEST policy over it treats NULL as one more key: of the new rows, only the newest stays visible. The
        // design leaves the policy as it is.
        assertKeepLatestKeyNoLongerProduced("ALTER TABLE base DROP COLUMN k");
    }

    @Test
    public void testKeepLatestKeyNoLongerProducedBySqlChange() throws Exception {
        // The same through ALTER ... AS: the new SQL does not produce k, so k reads NULL in the rows refreshed from
        // then on.
        assertKeepLatestKeyNoLongerProduced("ALTER MATERIALIZED VIEW mv AS (SELECT v, w, ts FROM base)");
    }

    @Test
    public void testSetExpireOverPendingDropAndReAddWithOtherType() throws Exception {
        // The view drops price and adds it back as LONG, and both statements wait in the view's WAL. SET EXPIRE checks a
        // scalar policy against the column as the view's sequencer metadata has it, the LONG, and a KEEP policy's read
        // probe against the applied DOUBLE; either way the column exists once the statements apply.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.5, '2024-01-01T00:20:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("mv");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN price");
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN price LONG");
            try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                Assert.assertEquals(ColumnType.DOUBLE, metadata.getColumnType("price"));
            }
            // 'x' is a CHAR, which a DOUBLE takes and a LONG does not
            assertSetExpireRefused("WHEN price = 'x'", "invalid EXPIRE ROWS predicate: inconvertible value: x [CHAR -> LONG]");
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN price < 2");
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS KEEP LATEST PARTITION BY price");
            drainWalAndMatViewQueues();

            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertExpireClause("KEEP LATEST PARTITION BY price");
            // the re-added price is NULL in both rows, so the newest row is the latest of the one key
            assertQuery("mv").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tts\tprice
                    b\t2024-01-01T00:20:00.000000Z\tnull
                    """);
            // the refresh writes the base's DOUBLE price into the LONG column
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-01T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tts\tprice
                    b\t2024-01-01T00:20:00.000000Z\tnull
                    c\t2024-01-01T00:30:00.000000Z\t7
                    """);
        });
    }

    @Test
    public void testDropColumnReadByQuotedOrPrefixedNameRefused() throws Exception {
        // A column the policy reads under a quoted name, or with the view's name as a prefix.
        assertMemoryLeak(() -> {
            createPassthroughView("");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN \"V\" < max(mv.v) OVER (PARTITION BY mv.k)");
            drainWalAndMatViewQueues();
            // The policy hides every row below its key's maximum v: A at 1.0 and B at 0.5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);
            // the message names the column as the view has it
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW mv DROP COLUMN \"V\"",
                    39,
                    "the EXPIRE ROWS policy of the materialized view reads the column [column=v], change the policy with "
                            + "SET EXPIRE or remove it with DROP EXPIRE first, and retry once that has applied"
            );

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnReadByKeepHighestRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS KEEP HIGHEST ON v PARTITION BY k");
            // The view has not refreshed yet: the policy comes with CREATE. The policy reads the ranked column
            // and the key.
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);
            // The highest v of each key: A at 2.0 and B at 3.0.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
            // A new highest v of B reaches the view.
            execute("INSERT INTO base VALUES ('B', 9.0, 90.0, '2024-01-05T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t9.0\t2024-01-05T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnReadByKeepLatestRefused() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, \"group key\" SYMBOL, \"select\" SYMBOL, v DOUBLE, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 'X', 'S', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'X', 'S', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('B', 'X', 'S', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS KEEP LATEST PARTITION BY K, \"group key\", \"select\"");
            // The view has not refreshed yet: the policy comes with CREATE.
            assertDropColumnRefused("mv", "k", 2);
            // The newest row of each key: A at 2.0 and B at 3.0.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tgroup key\tselect\tv\tts
                    A\tX\tS\t2.0\t2024-01-02T00:00:00.000000Z
                    B\tX\tS\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            // The key is matched case-insensitively, and quoted and keyword names are keys like any other.
            assertDropColumnRefused("mv", "k", 2);
            assertDropColumnRefused("mv", "\"K\"", 2);
            assertDropColumnRefused("mv", "\"group key\"", 2);
            assertDropColumnRefused("mv", "\"select\"", 2);
            assertColumns("mv", """
                    column\ttype
                    k\tSYMBOL
                    group key\tSYMBOL
                    select\tSYMBOL
                    v\tDOUBLE
                    ts\tTIMESTAMP
                    """);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN v");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tgroup key\tselect\tts
                    A\tX\tS\t2024-01-02T00:00:00.000000Z
                    B\tX\tS\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    @Test
    public void testDropColumnReadByKeepLowestTopNRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS KEEP 2 LOWEST ON v");
            drainWalAndMatViewQueues();
            // The two lowest v of all rows: A at 1.0 and B at 0.5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t1.0\t10.0\t2024-01-01T00:00:00.000000Z
                    B\t0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);

            // Without PARTITION BY the policy reads no key.
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN k");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    v\tw\tts
                    1.0\t10.0\t2024-01-01T00:00:00.000000Z
                    0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    @Test
    public void testDropColumnReadByScalarPredicateRefused() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);

            // A view WAL transaction that has not applied yet does not change the answer.
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            assertDropColumnRefused("price_1h", "price", 2);
            assertDropColumnRefused("price_1h", "PRICE", 2);
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    extra\tINT
                    """);

            // A column the policy does not read drops as usual, and the policy still applies.
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t3.5\tnull
                    2024-01-01T01:00:00.000000Z\ta\t4.5\tnull
                    """);
            // A base row reaches the view, which the policy still filters.
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("price_1h");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t3.5\tnull
                    2024-01-01T01:00:00.000000Z\ta\t4.5\tnull
                    2024-01-01T01:00:00.000000Z\tb\t5.5\tnull
                    """);
        });
    }

    @Test
    public void testDropColumnReadByWindowPredicateRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k)");
            drainWalAndMatViewQueues();
            // The policy hides every row below its key's maximum v: A at 1.0 and B at 0.5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    @Test
    public void testSetExpireReadingColumnOfPendingAddColumnAccepted() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("mv");
            // The base gains x, which the view's SQL produces and the view takes by name once it has a column x.
            execute("ALTER TABLE base ADD COLUMN x DOUBLE");
            drainWalQueue();
            execute("ALTER MATERIALIZED VIEW mv ADD COLUMN x DOUBLE");
            // The ADD COLUMN waits in the view's WAL.
            try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                Assert.assertEquals(-1, metadata.getColumnIndexQuiet("x"));
            }
            // The read probe of a KEEP policy compiles against the view's applied columns, which lack x yet.
            assertSetExpireRefused("KEEP HIGHEST ON x", "invalid EXPIRE ROWS policy: Invalid column: x");
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN x > 5.0");
            execute("""
                    INSERT INTO base VALUES
                    ('C', 5.0, 50.0, '2024-01-05T00:00:00.000000Z', 1.0),
                    ('D', 6.0, 60.0, '2024-01-06T00:00:00.000000Z', 9.0)
                    """);
            drainWalAndMatViewQueues();

            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertExpireClause("x > 5.0");
            // The rows from before the ADD COLUMN read NULL x, which the policy keeps; D's x is above 5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts\tx
                    A\t1.0\t10.0\t2024-01-01T00:00:00.000000Z\tnull
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z\tnull
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z\tnull
                    B\t0.5\t40.0\t2024-01-04T00:00:00.000000Z\tnull
                    C\t5.0\t50.0\t2024-01-05T00:00:00.000000Z\t1.0
                    """);

            // Once the ADD COLUMN has applied, the KEEP policy over x is accepted.
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS KEEP HIGHEST ON x");
            drainWalAndMatViewQueues();
            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertExpireClause("KEEP HIGHEST ON x");
        });
    }

    @Test
    public void testSetExpireReadingColumnOfPendingDropColumnRefused() throws Exception {
        // Accepted, such a SET EXPIRE would fail when it applies and suspend the view's WAL.
        assertMemoryLeak(() -> {
            createPassthroughView("");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("mv");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN k");
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            // Both DROP COLUMN statements wait in the view's WAL.
            try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                Assert.assertEquals(0, metadata.getColumnIndexQuiet("k"));
                Assert.assertEquals(2, metadata.getColumnIndexQuiet("w"));
            }
            assertSetExpireRefused("WHEN w > 25.0", "invalid EXPIRE ROWS predicate: Invalid column: w");
            assertSetExpireRefused("WHEN v > 2.5 AND K = 'B'", "invalid EXPIRE ROWS predicate: Invalid column: K");
            assertSetExpireRefused("KEEP LATEST PARTITION BY k", "invalid EXPIRE ROWS KEEP LATEST PARTITION BY column: k");
            assertSetExpireRefused("KEEP HIGHEST ON w", "invalid EXPIRE ROWS KEEP column: w");
            assertSetExpireRefused("KEEP 2 LOWEST ON v PARTITION BY k", "invalid EXPIRE ROWS KEEP LOWEST PARTITION BY column: k");
            // A policy over a column the view keeps passes.
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN v < 1.5");
            drainWalAndMatViewQueues();

            // The view's WAL applied both DROP COLUMN statements and the SET EXPIRE, and goes on.
            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertExpireClause("v < 1.5");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    v\tts
                    2.0\t2024-01-02T00:00:00.000000Z
                    3.0\t2024-01-03T00:00:00.000000Z
                    """);
            // A base row reaches the view through the policy.
            execute("INSERT INTO base VALUES ('C', 7.0, 70.0, '2024-01-05T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertWalApplied(viewToken);
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    v\tts
                    2.0\t2024-01-02T00:00:00.000000Z
                    3.0\t2024-01-03T00:00:00.000000Z
                    7.0\t2024-01-05T00:00:00.000000Z
                    """);
        });
    }

    private void assertKeepLatestKeyNoLongerProduced(String keyRemoval) throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS KEEP LATEST PARTITION BY k");
            drainWalAndMatViewQueues();
            // the newest row of each key: A at 2.0 and B at 0.5
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    """);
            execute(keyRemoval);
            drainWalAndMatViewQueues();
            execute("""
                    INSERT INTO base (v, w, ts) VALUES
                    (5.0, 50.0, '2024-01-05T00:00:00.000000Z'),
                    (6.0, 60.0, '2024-01-06T00:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertExpireClause("KEEP LATEST PARTITION BY k");
            assertWalApplied(engine.verifyTableName("mv"));
            // A and B keep their newest rows; of the rows with a NULL k, the newest stays
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    \t6.0\t60.0\t2024-01-06T00:00:00.000000Z
                    """);
        });
    }

    private void assertColumns(String viewName, String expected) throws Exception {
        assertQuery("SELECT \"column\", type FROM table_columns('" + viewName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    private void assertExpireClause(String expected) throws Exception {
        assertQuery("SELECT expire_clause FROM materialized_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("expire_clause\n" + expected + '\n');
    }

    // The DROP COLUMN fails at the column name, with a message that names the policy and how to change it, and
    // writes nothing to the view's WAL.
    private void assertDropColumnFails(String viewName, String columnToken) throws Exception {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final long seqTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
        final String sql = "ALTER MATERIALIZED VIEW " + viewName + " DROP COLUMN " + columnToken;
        try {
            execute(sql);
            Assert.fail("the statement must fail: " + sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, sql.lastIndexOf(columnToken), e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "EXPIRE ROWS");
            TestUtils.assertContains(e.getFlyweightMessage(), "DROP EXPIRE");
        }
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(viewToken));
    }

    // The DROP COLUMN fails and writes nothing: the view keeps the column, stays valid and stays readable.
    private void assertDropColumnRefused(String viewName, String columnToken, int visibleRowCount) throws Exception {
        assertDropColumnFails(viewName, columnToken);
        drainWalAndMatViewQueues();
        assertViewValid(viewName);
        // Every read of the view goes through the policy's rewrite.
        sink.clear();
        printSql("SELECT count() FROM " + viewName, sink);
        TestUtils.assertEquals("count\n" + visibleRowCount + "\n", sink);
    }

    // ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS <policy> fails at compile, at the policy, and writes nothing to the
    // view's WAL.
    private void assertSetExpireRefused(String policy, String message) throws Exception {
        final TableToken viewToken = engine.verifyTableName("mv");
        final long seqTxn = engine.getTableSequencerAPI().lastTxn(viewToken);
        final String sql = "ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS " + policy;
        // the error points at the first token of a WHEN predicate, or at KEEP
        final int position = policy.startsWith("WHEN ") ? sql.indexOf(policy) + "WHEN ".length() : sql.indexOf(policy);
        assertExceptionNoLeakCheck(sql, position, message);
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(viewToken));
    }

    private void assertViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    // The view's WAL is not suspended and has applied every transaction.
    private void assertWalApplied(TableToken viewToken) throws Exception {
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));
        assertQuery("SELECT suspended, writerTxn = sequencerTxn applied FROM wal_tables() WHERE name = '" + viewToken.getTableName() + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("suspended\tapplied\nfalse\ttrue\n");
    }

    // Base rows (k, v, w, ts): A 1.0, A 2.0, B 3.0, B 0.5 on four days; a passthrough view with the policy.
    private static void createPassthroughView(String expireClause) throws SqlException {
        execute("CREATE TABLE base (k SYMBOL, v DOUBLE, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('A', 1.0, 10.0, '2024-01-01T00:00:00.000000Z'),
                ('A', 2.0, 20.0, '2024-01-02T00:00:00.000000Z'),
                ('B', 3.0, 30.0, '2024-01-03T00:00:00.000000Z'),
                ('B', 0.5, 40.0, '2024-01-04T00:00:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " + expireClause);
    }

    // A SAMPLE BY view: buckets a@00:00 (3.5, 30.0), b@00:00 (2.5, 20.0) and a@01:00 (4.5, 40.0).
    private static void createSampledView(String expireClause) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
                ('a', 3.5, 30.0, '2024-01-01T00:40:00.000000Z'),
                ('a', 4.5, 40.0, '2024-01-01T01:10:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h AS (" + SAMPLED_VIEW_SQL + ") PARTITION BY DAY " + expireClause);
        drainWalAndMatViewQueues();
    }
}
