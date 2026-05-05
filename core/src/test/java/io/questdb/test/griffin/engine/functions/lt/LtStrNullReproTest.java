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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Regression test for the query fuzzer's literal-vs-bind divergence on
 * {@code NOT (... > null)} predicates. The SqlOptimiser rewrites
 * {@code NOT (X > Y)} to {@code X <= Y} when it appears in a WHERE clause.
 * The {@code <=} factory is built from the {@code <} factory by wrapping
 * with {@code NegatingFunctionFactory}, which used to flip the
 * short-circuit {@code BooleanConstant.FALSE} returned for null operands
 * to TRUE -- causing the bind-variable form to admit every row while the
 * literal form was correctly filtered.
 */
public class LtStrNullReproTest extends AbstractCairoTest {

    @Test
    public void testFuzzerWhereDivergenceBindMatchesLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, c4 SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES ('a', 'X', 0), ('b', null, 1)");
            String expected = "sym\tc4\tts\n" +
                    "b\t\t1970-01-01T00:00:00.000001Z\n";
            // Literal form: 'BGZT' folds at compile time, optimiseBooleanNot
            // rewrites NOT(X > null) into X <= null which evaluates to FALSE.
            assertSql(expected,
                    "SELECT * FROM t WHERE (upper(c4) IS NULL OR NOT (upper(upper('BGZT')) > null)) ORDER BY ts ASC");
            // Bind variant: same predicate must match the literal pivot.
            bindVariableService.clear();
            bindVariableService.setStr("b0", "BGZT");
            assertSql(expected,
                    "SELECT * FROM t WHERE (upper(c4) IS NULL OR NOT (upper(upper(:b0::SYMBOL)) > null)) ORDER BY ts ASC");
        });
    }

    @Test
    public void testNullComparisonStaysFalseUnderNegation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s VARCHAR)");
            execute("INSERT INTO t VALUES ('A')");
            // All four directions must filter the row out -- the runtime path
            // (Chars.lessThan with null operand) and the compile-time
            // short-circuit must agree on QuestDB's "null comparisons are
            // always false" convention, so NOT-rewriting them to <= or >=
            // cannot flip the result.
            assertSql("s\n", "SELECT * FROM t WHERE s > null");
            assertSql("s\n", "SELECT * FROM t WHERE s < null");
            assertSql("s\n", "SELECT * FROM t WHERE s >= null");
            assertSql("s\n", "SELECT * FROM t WHERE s <= null");
            assertSql("s\n", "SELECT * FROM t WHERE null > s");
            assertSql("s\n", "SELECT * FROM t WHERE null < s");
            assertSql("s\n", "SELECT * FROM t WHERE null >= s");
            assertSql("s\n", "SELECT * FROM t WHERE null <= s");
        });
    }
}
