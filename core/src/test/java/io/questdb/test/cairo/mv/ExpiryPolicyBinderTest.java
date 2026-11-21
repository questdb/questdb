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

import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.ExpiryValidationResult;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.bind.ExpiryPolicyBinder;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the validation and classification of scalar EXPIRE ROWS policies that {@link ExpiryPolicyBinder} parses once
 * and binds in each of its shapes. The expectations follow the documented rules of each property; for the errors of
 * shapes those rules do not spell out, they are the verdicts of the implementation before the binder existed, which
 * parsed the policy text once per shape, so a difference here is a change of behaviour.
 */
public class ExpiryPolicyBinderTest extends AbstractCairoTest {
    // Policies whose keep-filter the test compares with the parse of the read's text; they only have to parse.
    private static final String[] POLICIES = {
            "s = 12345",
            "12345 = s",
            "\"k\" = 'a'",
            "t.k = 'a'",
            "s = null",
            "i = -1",
            "i = 1 + 1",
            "-1 = i",
            "i != 1",
            "i <> 1",
            "i > 1 AND d < 2",
            "i > 1 OR NOT (d < 2)",
            "NOT null",
            "NOT (i = 1)",
            "k IN ('a', 'b')",
            "k NOT IN ('a')",
            "ts BETWEEN '2024' AND '2025'",
            "ts IN '$today'",
            "s || vc = 'ab'",
            "concat(s, vc) = 'a' || 'b'",
            "i::long > 2",
            "cast(i AS long) > 2",
            "CASE WHEN i > 0 THEN true ELSE false END",
            "CASE i WHEN 1 THEN true ELSE false END",
            "CASE WHEN i = 1 THEN true END",
            "d = 1.5 AND true",
            "(i = 1)",
            "((i > 1))",
            "ts < dateadd('d', -1, now())",
            "s ~ 'a.*' AND vc LIKE 'x%'",
            "b",
            "NOT b",
            "true"
    };
    private static final int POSITION = 7;

    @Test
    public void testClassificationKeepsWhatFoldsDrop() throws Exception {
        // Binding folds a constant operand away together with what it holds, while the classification reports every
        // clock, session value, bind variable and column the text names, so it reads the parsed policy.
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                assertClassification(compiler, metadata, "false AND ts > now()", true, false, false, "ts");
                assertClassification(compiler, metadata, "true OR ts < now()", true, false, false, "ts");
                assertClassification(compiler, metadata, "false AND ts IN '$today'", true, false, false, "ts");
                assertClassification(compiler, metadata, "false AND owner = current_user()", false, false, false, "owner");
                assertClassification(compiler, metadata, "false AND i > 1", false, true, true, "i");
                assertClassification(compiler, metadata, "i > 1 OR (false AND d < 0)", false, true, true, "i,d");
                assertRejected(compiler, metadata, null, "false AND i = $1", "invalid EXPIRE ROWS predicate: bind variables are not supported");
                assertRejected(compiler, metadata, "t", "true OR i = $2", "invalid EXPIRE ROWS predicate: bind variables are not supported");
                assertRejected(compiler, metadata, null, "coalesce(i, l, $1) > 0", "invalid EXPIRE ROWS predicate: bind variables are not supported");
                // a named bind variable has no value to infer a type from, so the bind refuses it first
                assertRejected(compiler, metadata, null, "true OR i = :v", "invalid EXPIRE ROWS predicate: undefined bind variable: :v");
            }
        });
    }

    @Test
    public void testClassificationOfClockThresholds() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                // the expire-old direction, with the timestamp on either side
                assertClassification(compiler, metadata, "ts < now()", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts <= now()", true, false, true, "ts");
                assertClassification(compiler, metadata, "now() > ts", true, false, true, "ts");
                assertClassification(compiler, metadata, "now() >= ts", true, false, true, "ts");
                assertClassification(compiler, metadata, "\"TS\" < sysdate()", true, false, true, "ts");
                assertClassification(compiler, metadata, "TS < systimestamp()", true, false, true, "ts");
                // the expire-recent direction un-expires rows as time passes
                assertClassification(compiler, metadata, "ts > now()", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts >= now()", true, false, false, "ts");
                assertClassification(compiler, metadata, "now() < ts", true, false, false, "ts");
                assertClassification(compiler, metadata, "now() <= ts", true, false, false, "ts");
                // look-back offsets the text spells as literals are proven; anything else is not
                assertClassification(compiler, metadata, "ts < now() - 2_000_000", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < now() - 2_000_000L", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < now() - 0", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < now() - -1", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < now() - 1_000_000 * 2", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < now() + 1", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < now() - abs(1)", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('d', abs(-1), now())", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('d', -1, now() + 1)", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('d', -1, now())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('w', 0, now())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('T', -500, now())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('s', -30, now())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('m', -30, sysdate())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('w', -15_250, now())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('w', -15_251, now())", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('d', 1, now())", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('M', -1, now())", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('y', -1, now())", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('d', -1, ts)", false, true, true, "ts");
                assertClassification(compiler, metadata, "dt < sysdate()", true, false, false, "dt");
                assertClassification(compiler, metadata, "now() > now()", true, false, false, "");
                // a clock outside the threshold, or a threshold that is one side of an AND, proves nothing
                assertClassification(compiler, metadata, "ts < now() AND i > 0", true, false, false, "ts,i");
                assertClassification(compiler, metadata, "l < now()::long", true, false, false, "l");
                // constant thresholds depend on the row alone
                assertClassification(compiler, metadata, "ts < '2024-01-01'", false, true, true, "ts");
                assertClassification(compiler, metadata, "'2024-01-01' > ts", false, true, true, "ts");
                assertClassification(compiler, metadata, "ts IN '2024-01'", false, true, true, "ts");
                // a date variable in text reads the clock
                assertClassification(compiler, metadata, "ts IN '$now - 1h..$now'", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts BETWEEN null AND now()", true, false, false, "ts");
                assertClassification(compiler, metadata, "owner = '$yesterday'", true, false, false, "owner");
                assertClassification(compiler, metadata, "owner = 'US$'", false, true, true, "owner");
                assertClassification(compiler, metadata, "coalesce(s, vc, '$today') = 'x'", true, false, false, "vc,s");
            }
            execute("CREATE TABLE tns (v DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("tns")); SqlCompiler compiler = engine.getSqlCompiler()) {
                assertClassification(compiler, metadata, "ts < now()", true, false, true, "ts");
                assertClassification(compiler, metadata, "now_ns() > ts", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts < dateadd('n', -5, now_ns())", true, false, true, "ts");
                assertClassification(compiler, metadata, "ts > systimestamp_ns()", true, false, false, "ts");
                assertClassification(compiler, metadata, "v > 0 AND ts < '2024-01-01T00:00:00.000000001'", false, true, true, "v,ts");
                assertRejected(compiler, metadata, null, "ts < cast(null AS TIMESTAMP_NS)", "invalid EXPIRE ROWS predicate: the threshold is NULL, so no row can ever expire");
            }
        });
    }

    @Test
    public void testClassificationOfRowOnlyAndSessionPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                // The columns follow the nodes of the text: an operation lists its left operand first, a function its
                // arguments last to first, and each column once.
                assertClassification(compiler, metadata, "coalesce(i, l) > 0 AND d < 1", false, true, true, "i,l,d");
                assertClassification(compiler, metadata, "coalesce(i, l, d) > 0", false, true, true, "d,l,i");
                assertClassification(compiler, metadata, "coalesce(i, l, rnd_int()) > 0", false, false, false, "l,i");
                assertClassification(compiler, metadata, "d < 1 AND coalesce(i, l) > 0 AND i > 2", false, true, true, "d,i,l");
                assertClassification(compiler, metadata, "k IN ('a', 'b') AND s LIKE 'x%'", false, true, true, "k,s");
                assertClassification(compiler, metadata, "lower(vc) = 'x' OR abs(d) > 1", false, true, true, "vc,d");
                assertClassification(compiler, metadata, "\"i\" > 1 AND \"D\" < 2", false, true, true, "i,d");
                assertClassification(compiler, metadata, "1 < 2", false, true, true, "");
                assertClassification(compiler, metadata, "i % 0 = 1 OR length(s) > 1_000_000_000_000", false, true, true, "i,s");
                // a function off the list, a session value or a random value only filters
                assertClassification(compiler, metadata, "sqrt(d) > 2", false, false, false, "d");
                assertClassification(compiler, metadata, "owner != current_user()", false, false, false, "owner");
                assertClassification(compiler, metadata, "k IN (session_user(), 'x')", false, false, false, "k");
                assertClassification(compiler, metadata, "d < rnd_double()", false, false, false, "d");
                assertClassification(compiler, metadata, "version() = 'x'", false, false, false, "");
                // an IN over text values parses each at runtime, so only constant values are proven
                assertClassification(compiler, metadata, "ts IN concat('$', 'today')", false, false, false, "ts");
                assertClassification(compiler, metadata, "ts IN vc", false, false, false, "ts,vc");
            }
        });
    }

    @Test
    public void testDdlBindsTheKeepFilterAsReadsDo() throws Exception {
        // A view name selects the keep-filter bind of the read path, which the classification of a stored policy
        // skips: a policy an older version stored is classified even when the reads of today refuse it.
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                // a negation that cancels another leaves NULL at the root of the keep-filter
                for (String predicate : new String[]{"NOT null", "not (null)", "NOT NOT NOT null", "NOT ((null))"}) {
                    assertClassification(compiler, metadata, predicate, false, true, true, "");
                    assertRejected(compiler, metadata, "t", predicate, "invalid EXPIRE ROWS predicate: boolean expression expected");
                    // at CREATE the view's name names no table yet
                    assertRejected(compiler, metadata, "not_created_yet", predicate, "invalid EXPIRE ROWS predicate: boolean expression expected");
                }
                assertValidated(compiler, metadata, "not_created_yet", "i > 1 OR ts < now()", true, false, false, "i,ts");
                assertValidated(compiler, metadata, "t", "ts < dateadd('d', -1, now())", true, false, true, "ts");
                assertValidated(compiler, metadata, "t", "null OR b", false, true, true, "b");
                // the classification binds first, so its error wins over the read's
                assertRejected(compiler, metadata, "t", "i + 1", "invalid EXPIRE ROWS predicate: expected a boolean expression");
                assertRejected(compiler, metadata, "t", "NOT null AND i = $1", "invalid EXPIRE ROWS predicate: bind variables are not supported");
                // the bare predicate binds with no qualifier, so no qualifier reaches the read's, whose alias is the view
                assertRejected(compiler, metadata, "t", "t.i > 1", "invalid EXPIRE ROWS predicate: Invalid table name or alias");
                assertRejected(compiler, metadata, null, "\"t\".\"i\" > 1", "invalid EXPIRE ROWS predicate: Invalid table name or alias");
                // the compiler stays usable after a rejection
                assertValidated(compiler, metadata, "t", "i > 1", false, true, true, "i");
            }
            execute("CREATE TABLE tns (v DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("tns")); SqlCompiler compiler = engine.getSqlCompiler()) {
                // a timestamp literal past the range of the designated timestamp's precision fails both binds
                for (String viewName : new String[]{null, "tns"}) {
                    assertRejected(compiler, metadata, viewName, "ts > '2300-01-01'",
                            "invalid EXPIRE ROWS predicate: inconvertible value: 10413792000000000 [TIMESTAMP -> TIMESTAMP_NS]");
                    assertValidated(compiler, metadata, viewName, "ts > '2200-01-01'", false, true, true, "ts");
                }
            }
        });
    }

    @Test
    public void testKeepFilterIsTheParseOfTheReadText() throws Exception {
        // DDL negates the parsed policy where a read parses NOT (<policy>), and the read path pushes either negation
        // down the same way, so both bind the same filter.
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final ObjectPool<ExpressionNode> nodes = new ObjectPool<>(ExpressionNode.FACTORY, 64);
                final StringSink text = new StringSink();
                for (String policy : POLICIES) {
                    text.clear();
                    RowExpiryUtil.appendKeepFilter(text, policy);
                    final ExpressionNode parsed = SqlUtil.optimiseBooleanNot(compiler.testParseExpression(text, (QueryModel) null), nodes);
                    final ExpressionNode built = SqlUtil.optimiseBooleanNot(
                            compiler.getExpiryPolicyBinderForTesting().parseKeepFilterForTesting(policy, compiler), nodes);
                    assertSameTree(policy, parsed, built);
                    nodes.clear();
                }
            }
        });
    }

    @Test
    public void testNullThresholdsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                final String error = "invalid EXPIRE ROWS predicate: the threshold is NULL, so no row can ever expire";
                // every ordering operator, with the timestamp on either side
                assertRejected(compiler, metadata, null, "ts < cast(null AS TIMESTAMP)", error);
                assertRejected(compiler, metadata, "t", "ts <= null::timestamp", error);
                assertRejected(compiler, metadata, null, "ts > null", error);
                assertRejected(compiler, metadata, "t", "ts >= cast(null AS DATE)", error);
                assertRejected(compiler, metadata, null, "null::timestamp > ts", error);
                assertRejected(compiler, metadata, null, "cast(null AS TIMESTAMP) < ts", error);
                // arithmetic that lands on the NULL sentinel of its type
                assertRejected(compiler, metadata, null, "ts < 4611686018427387904 * 2", error);
                assertRejected(compiler, metadata, null, "ts < 1073741824 * 2", error);
                assertRejected(compiler, metadata, null, "ts < 0.0 / 0.0", error);
                assertRejected(compiler, metadata, null, "ts < cast(null AS FLOAT)", error);
                assertRejected(compiler, metadata, "t", "ts < cast(null AS VARCHAR)", error);
                assertRejected(compiler, metadata, null, "ts < cast(null AS STRING)", error);
                // a threshold that is not a NULL constant, or that no ordering comparison of the timestamp holds
                assertClassification(compiler, metadata, "ts < 86400 * 1000000", false, true, true, "ts");
                assertClassification(compiler, metadata, "ts < -9.3e18", false, true, true, "ts");
                assertClassification(compiler, metadata, "ts < 1.5", false, true, true, "ts");
                assertClassification(compiler, metadata, "ts < 5::short", false, true, true, "ts");
                assertClassification(compiler, metadata, "ts < now() + null::long", true, false, false, "ts");
                assertClassification(compiler, metadata, "ts = null", false, true, true, "ts");
                assertClassification(compiler, metadata, "l < null", false, true, true, "l");
                assertClassification(compiler, metadata, "ts < l + null", false, true, true, "ts,l");
            }
        });
    }

    @Test
    public void testParseAndBindErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String viewName : new String[]{null, "t"}) {
                    assertRejected(compiler, metadata, viewName, "i > 1 oops", "invalid EXPIRE ROWS predicate: unexpected token after expression: oops");
                    assertRejected(compiler, metadata, viewName, "", "invalid EXPIRE ROWS predicate: expected a boolean expression");
                    assertRejected(compiler, metadata, viewName, "i + 1", "invalid EXPIRE ROWS predicate: expected a boolean expression");
                    assertRejected(compiler, metadata, viewName, "null", "invalid EXPIRE ROWS predicate: expected a boolean expression");
                    assertRejected(compiler, metadata, viewName, "sum(i)", "invalid EXPIRE ROWS predicate: aggregate functions are not supported");
                    assertRejected(compiler, metadata, viewName, "sum(i) > 1", "invalid EXPIRE ROWS predicate: Aggregate function cannot be passed as an argument");
                    assertRejected(compiler, metadata, viewName, "k IN (SELECT k FROM t)", "invalid EXPIRE ROWS predicate: query is not allowed here");
                    assertRejected(compiler, metadata, viewName, "nope > 1", "invalid EXPIRE ROWS predicate: Invalid column: nope");
                    assertRejected(compiler, metadata, viewName, "i = 'abc'", "invalid EXPIRE ROWS predicate: inconvertible value: `abc` [STRING -> INT]");
                    assertRejected(compiler, metadata, viewName, "i > $1", "invalid EXPIRE ROWS predicate: bind variables are not supported");
                    assertRejected(compiler, metadata, viewName, "i IN ()", "invalid EXPIRE ROWS predicate: too few arguments for 'in'");
                    assertRejected(compiler, metadata, viewName, "version", "invalid EXPIRE ROWS predicate: expected a boolean expression");
                    assertRejected(compiler, metadata, viewName, "ts < dateadd(d, -1, now())",
                            "invalid EXPIRE ROWS predicate: there is no matching function `dateadd` with the argument types: (DOUBLE, INT, TIMESTAMP)");
                    assertRejected(compiler, metadata, viewName, "b = 1", "invalid EXPIRE ROWS predicate: there is no matching operator `=` with the argument types: BOOLEAN = INT");
                }
            }
        });
    }

    @Test
    public void testSweepShapeRejectsWhatTheSwitchRejects() throws Exception {
        // A column compared with a constant by = binds as a switch keyed on the column in the sweep's strict form, whose
        // conversions are stricter than those of the comparison, in either operand order; the operand bound on its own
        // then has to be text for a text column and a value of the column for any other.
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String viewName : new String[]{null, "t"}) {
                    assertRejected(compiler, metadata, viewName, "s = 12345", "invalid EXPIRE ROWS predicate: type mismatch [expected=STRING, actual=INT]");
                    assertRejected(compiler, metadata, viewName, "12345 = s", "invalid EXPIRE ROWS predicate: type mismatch [expected=STRING, actual=INT]");
                    assertRejected(compiler, metadata, viewName, "k = 12345", "invalid EXPIRE ROWS predicate: type mismatch [expected=SYMBOL, actual=INT]");
                    assertRejected(compiler, metadata, viewName, "vc = 1.5", "invalid EXPIRE ROWS predicate: type mismatch [expected=VARCHAR, actual=DOUBLE]");
                    assertRejected(compiler, metadata, viewName, "d = 'abc'", "invalid EXPIRE ROWS predicate: inconvertible value: `abc` [STRING -> DOUBLE]");
                    assertRejected(compiler, metadata, viewName, "c = 'xy'", "invalid EXPIRE ROWS predicate: inconvertible value: `xy` [STRING -> CHAR]");
                    assertRejected(compiler, metadata, viewName, "u = 'x'", "invalid EXPIRE ROWS predicate: inconvertible value: `x` [STRING -> UUID]");
                    // under any other root the comparison binds as a CASE, whose WHEN takes the comparison as it is
                    assertValidated(compiler, metadata, viewName, "s = 12345 OR false", false, true, true, "s");
                    assertValidated(compiler, metadata, viewName, "(s = 'x') = true", false, true, true, "s");
                    // keys the switch converts, and the NULL key
                    assertValidated(compiler, metadata, viewName, "u = '11111111-1111-1111-1111-111111111111'", false, true, true, "u");
                    assertValidated(compiler, metadata, viewName, "'11111111-1111-1111-1111-111111111111' = u", false, true, true, "u");
                    assertValidated(compiler, metadata, viewName, "i = 1.5", false, true, true, "i");
                    assertValidated(compiler, metadata, viewName, "k = 'a'", false, true, true, "k");
                    assertValidated(compiler, metadata, viewName, "'a' = k", false, true, true, "k");
                    assertValidated(compiler, metadata, viewName, "s = null", false, true, true, "s");
                    assertValidated(compiler, metadata, viewName, "ts = '2024-01-01'", false, true, true, "ts");
                    assertValidated(compiler, metadata, viewName, "ip = '1.2.3.4'", false, true, true, "ip");
                    assertValidated(compiler, metadata, viewName, "c = 'x'", false, true, true, "c");
                }
            }
        });
    }

    @Test
    public void testTimestampThresholdFastPath() throws Exception {
        // The cleanup job reads the threshold of an expire-old comparison of the designated timestamp in the unit of
        // that column, and falls back to its scan for anything else.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE tns (v DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            // 2024-01-02T00:00:00Z
            final long day2Micros = 1_704_153_600_000_000L;
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertEquals(day2Micros, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02'::timestamp", "ts"));
                Assert.assertEquals(day2Micros, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "'2024-01-02'::timestamp >= \"TS\"", "ts"));
                // text compares as a timestamp only within the predicate, so on its own it is no timestamp threshold
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02'", "ts"));
                Assert.assertEquals(day2Micros + 1, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts <= 1_704_153_600_000_001::timestamp", "ts"));
                // a qualifier names the table, and a nanosecond threshold rounds down to the microseconds of the column
                Assert.assertEquals(day2Micros, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "t.ts < '2024-01-02'::timestamp", "ts"));
                Assert.assertEquals(day2Micros, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02T00:00:00.000000007'::timestamp_ns", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "nope < '2024-01-02'::timestamp", "ts"));
                // no designated timestamp, a name of no column, a column that is no timestamp
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02'", null));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02'", "nope"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "l < 5", "l"));
                // shapes other than an expire-old comparison of the timestamp with a column-free threshold
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts > '2024-01-02'", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "'2024-01-02' < ts", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "NOT ts < '2024-01-02'", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02' AND i > 0", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts IN '2024'", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "b", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < dt", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < dateadd('d', -1, ts)", "ts"));
                // a threshold that is no timestamp, a NULL one, one that is no constant, and one that fails to bind
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < 5", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < null::timestamp", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < rnd_timestamp(0::timestamp, 1::timestamp, 0)", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < nope()", "ts"));
                // a clock threshold evaluates against the clock of the call
                Assert.assertNotEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < now()", "ts"));
            }
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("tns")); SqlCompiler compiler = engine.getSqlCompiler()) {
                // a microsecond threshold converts to the nanosecond unit of the column, and an overflow reads as none
                Assert.assertEquals(day2Micros * 1_000, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02'::timestamp", "ts"));
                Assert.assertEquals(day2Micros * 1_000 + 7, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < '2024-01-02T00:00:00.000000007'::timestamp_ns", "ts"));
                Assert.assertEquals(Numbers.LONG_NULL, compiler.expiryTimestampThreshold(sqlExecutionContext, metadata, "ts < 9_223_372_036_854_775_807::timestamp", "ts"));
            }
        });
    }

    @Test
    public void testTextTheReadsCannotEmbedIsRejected() throws Exception {
        // Every read embeds the stored text in NOT (<policy>), and the sweep in a CASE, so a text whose last comment
        // swallows the closing parenthesis fails every read even though the policy parses on its own.
        assertMemoryLeak(() -> {
            createTable();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t")); SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String viewName : new String[]{null, "t"}) {
                    for (String predicate : new String[]{
                            "i > 1 -- c", "i > 1 --", "i > 1 -- c\n", "i > 1 -- ')' \n", "i > 1 /* c", "i > 1 /*+ hint",
                            "i > 1 /* a /* b */", "i > 1 /* c */ -- d"
                    }) {
                        assertRejected(compiler, metadata, viewName, predicate, "invalid EXPIRE ROWS predicate: unbalanced (");
                    }
                    for (String predicate : new String[]{
                            "i > 1 /* c */", "i > 1 /*+ hint */", "i > 1 /* a /* b */ */", "-- c\ni > 1", "i > 1\n", "\ti > 1\t"
                    }) {
                        assertValidated(compiler, metadata, viewName, predicate, false, true, true, "i");
                    }
                    assertValidated(compiler, metadata, viewName, "i > 1 -- c\nAND d < 1", false, true, true, "i,d");
                    assertValidated(compiler, metadata, viewName, "s = '--' OR s = '/*' OR s = ')'", false, true, true, "s");
                }
                // the cleanup job does not reclaim under a stored policy it cannot classify
                Assert.assertFalse(compiler.isExpiryCleanupReclaiming(sqlExecutionContext, metadata, "i > 1 -- c"));
                Assert.assertTrue(compiler.isExpiryCleanupReclaiming(sqlExecutionContext, metadata, "i > 1 /* c */"));
            }
        });
    }

    private static void assertClassification(
            SqlCompiler compiler,
            TableMetadata metadata,
            String predicate,
            boolean hasClock,
            boolean isDeterministic,
            boolean isMonotonic,
            String referencedColumns
    ) throws SqlException {
        assertValidated(compiler, metadata, null, predicate, hasClock, isDeterministic, isMonotonic, referencedColumns);
    }

    private static void assertRejected(
            SqlCompiler compiler,
            TableMetadata metadata,
            @Nullable String viewName,
            String predicate,
            String message
    ) {
        try {
            compiler.validateExpiryPolicy(sqlExecutionContext, viewName, metadata, predicate, POSITION);
            Assert.fail("expected a rejection [predicate=" + predicate + ", viewName=" + viewName + ']');
        } catch (SqlException e) {
            Assert.assertEquals("position [predicate=" + predicate + ']', POSITION, e.getPosition());
            TestUtils.assertEquals("message [predicate=" + predicate + ", viewName=" + viewName + ']', message, e.getFlyweightMessage());
        }
    }

    private static void assertResult(
            ExpiryValidationResult result,
            TableMetadata metadata,
            String predicate,
            boolean hasClock,
            boolean isDeterministic,
            boolean isMonotonic,
            String referencedColumns
    ) {
        Assert.assertEquals("hasClock [predicate=" + predicate + ']', hasClock, result.hasClock());
        Assert.assertEquals("isDeterministic [predicate=" + predicate + ']', isDeterministic, result.isDeterministic());
        Assert.assertEquals("isMonotonic [predicate=" + predicate + ']', isMonotonic, result.isMonotonic());
        final StringSink names = new StringSink();
        final IntList columns = result.getReferencedColumnIndexes();
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                names.put(',');
            }
            names.put(metadata.getColumnName(columns.getQuick(i)));
        }
        TestUtils.assertEquals("referenced columns [predicate=" + predicate + ']', referencedColumns, names);
    }

    private static void assertSameTree(String policy, ExpressionNode expected, ExpressionNode actual) {
        Assert.assertNotNull(actual);
        TestUtils.assertEquals("text [policy=" + policy + ']', expected.toString(), actual.toString());
        Assert.assertTrue("tree [policy=" + policy + ", expected=" + expected + ", actual=" + actual + ']',
                ExpressionNode.compareNodesExact(expected, actual) && isSameShape(expected, actual));
    }

    private static void assertValidated(
            SqlCompiler compiler,
            TableMetadata metadata,
            @Nullable String viewName,
            String predicate,
            boolean hasClock,
            boolean isDeterministic,
            boolean isMonotonic,
            String referencedColumns
    ) throws SqlException {
        assertResult(compiler.validateExpiryPolicy(sqlExecutionContext, viewName, metadata, predicate, POSITION),
                metadata, predicate, hasClock, isDeterministic, isMonotonic, referencedColumns);
    }

    private static void createTable() throws SqlException {
        execute("""
                CREATE TABLE t (
                    s STRING, vc VARCHAR, k SYMBOL, i INT, l LONG, d DOUBLE, b BOOLEAN, c CHAR, u UUID, ip IPv4,
                    owner VARCHAR, dt DATE, ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
    }

    // Whether the two trees hold the same node types, tokens and argument counts in the same places, which
    // compareNodesExact does not compare.
    private static boolean isSameShape(ExpressionNode a, ExpressionNode b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.type != b.type || a.paramCount != b.paramCount || a.args.size() != b.args.size()
                || !isSameShape(a.lhs, b.lhs) || !isSameShape(a.rhs, b.rhs)) {
            return false;
        }
        for (int i = 0, n = a.args.size(); i < n; i++) {
            if (!isSameShape(a.args.getQuick(i), b.args.getQuick(i))) {
                return false;
            }
        }
        return true;
    }
}
