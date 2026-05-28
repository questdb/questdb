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
public class LtNullComparisonTest extends AbstractCairoTest {

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
    public void testNullComparisonAgainstNonNullValueIsAlwaysFalse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s VARCHAR)");
            execute("INSERT INTO t VALUES ('A')");
            // When the table side is non-null and the literal side is null, all
            // four directions filter the row out: the one-null branch in
            // Chars.lessThan / Utf8s.lessThan returns false regardless of the
            // negated flag, and the compile-time short-circuit must agree.
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

    @Test
    public void testNullComparisonAgainstNullValueHonoursEqualityConvention() throws Exception {
        // Both the literal-null short-circuit (StrNullSideFunc /
        // VarcharNullSideFunc) and the runtime path (Chars.lessThan /
        // Utf8s.lessThan) must agree on QuestDB's NULL = NULL convention:
        //   <  / >  with any null operand  -> false
        //   <= / >= with both null operands -> true
        // The literal form goes through the short-circuit; the bind-variable
        // form goes through the runtime Func. Both must produce the same
        // single-row outcome over a NULL row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tv (s VARCHAR)");
            execute("INSERT INTO tv VALUES (null), ('A')");
            // <, > stay false in both directions
            assertSql("s\n", "SELECT s FROM tv WHERE s > null");
            assertSql("s\n", "SELECT s FROM tv WHERE s < null");
            assertSql("s\n", "SELECT s FROM tv WHERE null > s");
            assertSql("s\n", "SELECT s FROM tv WHERE null < s");
            // <=, >= return the both-null row only
            assertSql("s\n\n", "SELECT s FROM tv WHERE s <= null");
            assertSql("s\n\n", "SELECT s FROM tv WHERE s >= null");
            assertSql("s\n\n", "SELECT s FROM tv WHERE null <= s");
            assertSql("s\n\n", "SELECT s FROM tv WHERE null >= s");

            execute("CREATE TABLE ts (s STRING)");
            execute("INSERT INTO ts VALUES (null), ('A')");
            assertSql("s\n", "SELECT s FROM ts WHERE s > null");
            assertSql("s\n", "SELECT s FROM ts WHERE s < null");
            assertSql("s\n", "SELECT s FROM ts WHERE null > s");
            assertSql("s\n", "SELECT s FROM ts WHERE null < s");
            assertSql("s\n\n", "SELECT s FROM ts WHERE s <= null");
            assertSql("s\n\n", "SELECT s FROM ts WHERE s >= null");
            assertSql("s\n\n", "SELECT s FROM ts WHERE null <= s");
            assertSql("s\n\n", "SELECT s FROM ts WHERE null >= s");

            // Bind-variable parity: a NULL bind variable goes through the
            // runtime Func instead of the short-circuit; the row-set must
            // still match the literal form.
            bindVariableService.clear();
            bindVariableService.setVarchar("b0", null);
            assertSql("s\n", "SELECT s FROM tv WHERE s > :b0");
            assertSql("s\n", "SELECT s FROM tv WHERE s < :b0");
            assertSql("s\n\n", "SELECT s FROM tv WHERE s <= :b0");
            assertSql("s\n\n", "SELECT s FROM tv WHERE s >= :b0");
        });
    }
}
