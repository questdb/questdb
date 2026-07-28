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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Pins the agreement between the constant-folded path and the column/bind path for
 * INT arithmetic that overflows. The query fuzzer's literal-vs-bind oracle surfaced a
 * divergence: a constant {@code INT * INT} product that overflowed was folded to a
 * wider LONG (2764486628), while the same expression over a column wrapped to INT
 * (-1530480668). That flipped a {@code > 2} comparison and changed a row count.
 * <p>
 * The fix keeps the overflowing arithmetic at INT static type (so both paths wrap),
 * while wider numeric/temporal casts read the widened value, preserving the overflow
 * widening introduced by PR #4824 (e.g. {@code to_utc(<seconds> * 1_000_000)}).
 */
public class IntArithmeticOverflowFoldingTest extends AbstractCairoTest {

    @Test
    public void testComparisonAgreesBetweenConstantAndColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (839_759)");

            // plain INT projection wraps mod 2^32 on both paths
            assertQuery("SELECT 839_759::INT * 330_972L::SHORT AS v").noLeakCheck().expectSize().returns("v\n-1530480668\n");
            assertQuery("SELECT x::INT * 330_972L::SHORT AS v FROM t").noLeakCheck().expectSize().returns("v\n-1530480668\n");

            // the offending comparison: false on both paths (was true for the folded LONG)
            assertQuery("SELECT (839_759::INT * 330_972L::SHORT) > 2 AS v").noLeakCheck().expectSize().returns("v\nfalse\n");
            assertQuery("SELECT (x::INT * 330_972L::SHORT) > 2 AS v FROM t").noLeakCheck().expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testFilterOnAliasReadsTheSameWidthAsTheProjection() throws Exception {
        // A WHERE clause over a projection alias compiled against the base factory's bare metadata,
        // which cannot answer the width question and takes the conservative true default, so the
        // filter emitted a plain IntColumn where the projection above it emitted IntWideColumn. The
        // two halves of the same query then disagreed about the same alias: the row's own a::LONG
        // was the bound the predicate tested for, yet the predicate excluded the row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE faw (i INT, j INT)");
            execute("INSERT INTO faw VALUES (2_000_000_000, 2_000_000_000)");

            // i*j is 4000000000000000000 read at long width and -1651507200 read at INT width
            assertQuery("SELECT (i*j)::LONG AS v FROM faw").noLeakCheck().expectSize().returns("v\n4000000000000000000\n");
            assertQuery("SELECT i*j AS v FROM faw").noLeakCheck().expectSize().returns("v\n-1651507200\n");

            // the un-aliased spelling keeps the row: the filter sees the arithmetic function itself
            assertQuery("SELECT (i*j)::LONG AS v FROM faw WHERE i*j = 4000000000000000000")
                    .noLeakCheck().returns("v\n4000000000000000000\n");

            // the alias must agree with it. The projection reads a at long width, so the filter has
            // to as well, or the query contradicts itself.
            assertQuery("SELECT a::LONG AS v FROM (SELECT i*j AS a FROM faw) WHERE a = 4000000000000000000")
                    .noLeakCheck().returns("v\n4000000000000000000\n");

            // and it must agree in the other direction too: the wrapped value is not the row's
            // long-width value, so a long-width predicate on it excludes the row on both spellings
            assertQuery("SELECT (i*j)::LONG AS v FROM faw WHERE i*j = -1651507200L")
                    .noLeakCheck().returns("v\n");
            assertQuery("SELECT a::LONG AS v FROM (SELECT i*j AS a FROM faw) WHERE a = -1651507200L")
                    .noLeakCheck().returns("v\n");

            // The plain INT read of the alias still wraps in a filter, exactly as the arithmetic
            // does, so an INT-width predicate keeps the row on both spellings. Without this arm the
            // fix could have moved every read to long width and still passed.
            assertQuery("SELECT a::LONG AS v FROM (SELECT i*j AS a FROM faw) WHERE a = -1651507200")
                    .noLeakCheck().returns("v\n4000000000000000000\n");
            assertQuery("SELECT (i*j)::LONG AS v FROM faw WHERE i*j = -1651507200")
                    .noLeakCheck().returns("v\n4000000000000000000\n");

            // IN is the consumer that changes most: the key's isIntWidthStable decides whether
            // InLongFunctionFactory splits the key and probes the INT and LONG element sets
            // separately. A filter that reported the alias width-stable read the wide element at
            // INT width and matched nothing. The projection spelling of the same predicate is the
            // reference - the two must agree, which is the whole point of the alias contract.
            assertQuery("SELECT a IN (4000000000000000000) AS v FROM (SELECT i*j AS a FROM faw)")
                    .noLeakCheck().expectSize().returns("v\ntrue\n");
            assertQuery("SELECT a::LONG AS v FROM (SELECT i*j AS a FROM faw) WHERE a IN (4000000000000000000)")
                    .noLeakCheck().returns("v\n4000000000000000000\n");
            // ... and the wrapped value must still not match at long width, on both spellings
            assertQuery("SELECT a IN (-1651507200L) AS v FROM (SELECT i*j AS a FROM faw)")
                    .noLeakCheck().expectSize().returns("v\nfalse\n");
            assertQuery("SELECT a::LONG AS v FROM (SELECT i*j AS a FROM faw) WHERE a IN (-1651507200L)")
                    .noLeakCheck().returns("v\n");

            // A post-join filter is compiled the same way and over the same live master record:
            // JoinRecord hands the master through, so the alias has to read wide there too. The OR
            // keeps the predicate from being pushed below the join, and the LEFT JOIN finds no
            // match, so k IS NULL and only the wide read can keep the row.
            execute("CREATE TABLE fay (k INT)");
            execute("INSERT INTO fay VALUES (7)");
            assertQuery("SELECT x.a::LONG AS v FROM (SELECT i*j AS a, i FROM faw) x LEFT JOIN fay ON x.i = fay.k WHERE x.a = 4000000000000000000 OR fay.k = 5")
                    .noLeakCheck().noRandomAccess()
                    .withPlanContaining("Filter filter: (x.a=4000000000000000000L or fay.k=5)")
                    .returns("v\n4000000000000000000\n");

            // ... and so is a post-UNNEST filter, over UnnestRecord's pass-through of the master.
            // Neither array element exceeds 100, so again only the wide read keeps the rows.
            execute("CREATE TABLE fau (i INT, j INT, arr DOUBLE[])");
            execute("INSERT INTO fau VALUES (2_000_000_000, 2_000_000_000, ARRAY[1.0, 2.0])");
            assertQuery("SELECT tt.a::LONG AS v FROM (SELECT i*j AS a, arr FROM fau) tt, UNNEST(tt.arr) u(val) WHERE tt.a = 4000000000000000000 OR u.val > 100")
                    .noLeakCheck().noRandomAccess()
                    .withPlanContaining("Filter filter: (tt.a=4000000000000000000L or 100<u.val)")
                    .returns("v\n4000000000000000000\n4000000000000000000\n");
        });
    }

    @Test
    public void testFloatingPointPairAgreesBetweenConstantAndColumn() throws Exception {
        // A floating-point constant pair used to regroup: (dblCol * 1e300) * 1e-300 became
        // dblCol * (1e300 * 1e-300) = dblCol * 1.0. IEEE-754 * is not associative - the
        // un-regrouped form overflows to Infinity at the intermediate and stays there - and
        // reassociateConstants never regroups the all-literal form, so only the column form
        // moved and the two diverged. Neither form regroups now, so both overflow alike.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (d DOUBLE, f FLOAT)");
            execute("INSERT INTO u VALUES (1e10, 1e10)");

            // multiplication: the intermediate overflows to Infinity, which prints as null
            assertQuery("SELECT 1e10 * 1e300 * 1e-300 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT d * 1e300 * 1e-300 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // addition: 1e16 + 1.0 rounds back to 1e16 (ulp is 2), so the left-associative form
            // yields 1.0000000000000002E16, while regrouping to d + 3.0 would yield ...04E16
            assertQuery("SELECT 1e16 + 1.0 + 2.0 AS v").noLeakCheck().expectSize().returns("v\n1.0000000000000002E16\n");
            assertQuery("SELECT (d * 1e6) + 1.0 + 2.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n1.0000000000000002E16\n");

            // a FLOAT pair diverges the same way: 1e10f * 1e30f overflows the float range
            assertQuery("SELECT 1e10f * 1e30f * 1e-30f AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT f * 1e30f * 1e-30f AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testImplicitDoublePromotionWrapsLikeConstantAndColumn() throws Exception {
        // (intCol + intConst) + floatConst: the constant-reassociation pass used to
        // regroup the column form into intCol + (intConst + floatConst), folding the
        // two constants to a single DOUBLE and evaluating intCol + intConst at double
        // width - so an overflowing INT addition widened instead of wrapping. The
        // literal form folds the inner INT arithmetic first (wrapping) and never
        // regroups, so the two diverged. The reassociation now leaves an integer/
        // floating-point constant pair un-regrouped, so both paths wrap alike. An
        // explicit ::LONG cast (and a LONG constant) still widens on both paths, while
        // an explicit ::DOUBLE cast wraps like the implicit promotion it must agree with.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, a INT)");
            execute("INSERT INTO u VALUES (2_147_483_647, 100_000)");

            // implicit DOUBLE promotion wraps on both the constant and column paths
            assertQuery("SELECT (2_147_483_647 + 3) + 0.0 AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (3 + 2_147_483_647) + 0.0 AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (3 + y) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (2_147_483_647 + 3) AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT 0.0 + (y + 3) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // FLOAT promotion wraps alike (-2147483646 rounds to -2.1474836E9)
            assertQuery("SELECT (2_147_483_647 + 3) + 0.0f AS v").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");
            assertQuery("SELECT (y + 3) + 0.0f AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");

            // multiplication overflow under a DOUBLE promotion wraps alike
            assertQuery("SELECT (100_000 * 100_000) * 2.0 AS v").noLeakCheck().expectSize().returns("v\n2.820130816E9\n");
            assertQuery("SELECT (a * 100_000) * 2.0 AS v FROM u").noLeakCheck().expectSize().returns("v\n2.820130816E9\n");

            // an explicit ::LONG cast widens on both paths, matching an implicit LONG read
            assertQuery("SELECT (2_147_483_647 + 3)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");

            // an explicit ::DOUBLE cast wraps, matching the implicit DOUBLE promotion above
            assertQuery("SELECT (2_147_483_647 + 3)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            // a LONG constant still combines and widens identically on both paths
            assertQuery("SELECT (2_147_483_647 + 3) + 0L AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3) + 0L AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");
        });
    }

    @Test
    public void testConditionalsCarryWidenedIntThroughToWiderCasts() throws Exception {
        // CASE, COALESCE and NULLIF are INT-typed wrappers around an INT branch. They used to
        // override only getInt(), inheriting the wrapping IntFunction.getLong(), so a wider cast
        // of the wrapper disagreed with the same cast of the branch it returns: (a+b)::LONG gave
        // 4000000000 while coalesce(a+b,0)::LONG gave -294967296. Each now reads the branch it
        // picked at long width, and each picks that branch at INT width so both getters agree on
        // which one won.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (a INT, b INT, z INT)");
            execute("INSERT INTO u VALUES (2_000_000_000, 2_000_000_000, NULL)");

            assertQuery("SELECT (a + b)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT coalesce(a + b, 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT coalesce(z, a + b, 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT (CASE WHEN true THEN a + b END)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT nullif(a + b, 1)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n4000000000\n");

            // the INT-width read still wraps on every spelling, so the projections agree
            assertQuery("SELECT a + b AS v FROM u").noLeakCheck().expectSize().returns("v\n-294967296\n");
            assertQuery("SELECT coalesce(a + b, 0) AS v FROM u").noLeakCheck().expectSize().returns("v\n-294967296\n");
            assertQuery("SELECT (CASE WHEN true THEN a + b END) AS v FROM u").noLeakCheck().expectSize().returns("v\n-294967296\n");
            assertQuery("SELECT nullif(a + b, 1) AS v FROM u").noLeakCheck().expectSize().returns("v\n-294967296\n");

            // NULL still propagates as NULL at both widths
            assertQuery("SELECT coalesce(z, NULL)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (CASE WHEN false THEN a + b END)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT nullif(z, NULL)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // A NULL first argument survives an UNEQUAL pair as well. getInt() returns the INT_NULL
            // sentinel there, and widening it with plain sign extension would hand every 64-bit read
            // a real -2147483648: wrong values for ::LONG, ::TIMESTAMP and ::DATE, and a NULL
            // written to disk as data by the store path.
            assertQuery("SELECT nullif(z, 5)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT nullif(z, 5) + 0L AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT nullif(z, 5)::TIMESTAMP IS NULL AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");
            assertQuery("SELECT nullif(z, 5)::DATE IS NULL AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");
            execute("CREATE TABLE dstNullIf (l LONG)");
            execute("INSERT INTO dstNullIf SELECT nullif(z, 5) FROM u");
            assertQuery("SELECT l FROM dstNullIf").noLeakCheck().expectSize().returns("l\nnull\n");

            // nullif nulls out an equal pair at long width too, judged at INT width
            assertQuery("SELECT nullif(a + b, a + b)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // The branch is picked at INT width, where -2^31 IS the sentinel. Picking at long
            // width instead would make getInt() and getLong() disagree on which branch won:
            // ~2_147_483_647 is INT_NULL at int width but an ordinary -2147483648 at long width,
            // so coalesce must skip it and nullif must null the pair out.
            execute("CREATE TABLE w (y INT)");
            execute("INSERT INTO w VALUES (2_147_483_647)");
            assertQuery("SELECT coalesce(~y, 7) AS v FROM w").noLeakCheck().expectSize().returns("v\n7\n");
            assertQuery("SELECT coalesce(~y, 7)::LONG AS v FROM w").noLeakCheck().expectSize().returns("v\n7\n");
            assertQuery("SELECT nullif(a + b, -294_967_296)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // Each argument is read once per row. A second read of a non-deterministic argument is
            // a fresh draw, and a null draw would let coalesce return null despite a non-null
            // fallback - rnd_int(1,10,3) nulls roughly one row in four.
            assertQuery("SELECT count() AS c FROM (SELECT coalesce(rnd_int(1, 10, 3), 0)::LONG AS v FROM long_sequence(1000)) WHERE v = null")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");

            // and the value a conditional stores matches the cast that reads it
            execute("CREATE TABLE dst (l LONG)");
            execute("INSERT INTO dst SELECT coalesce(a + b, 0) FROM u");
            assertQuery("SELECT l FROM dst").noLeakCheck().expectSize().returns("l\n4000000000\n");
        });
    }

    @Test
    public void testConstantReassociationGuardCoversQuotedNumericLiterals() throws Exception {
        // The reassociation guard screened constants by their first character, and a string
        // literal keeps its quotes, so '02' took the non-numeric early-out with both fold flags
        // clear and the pair regrouped anyway. Overload resolution then cast it to a number, so
        // l * '02' * 4 regrouped to l * 8 and produced 0 where the unquoted l * 2 * 4 produced
        // null - spelling-dependent semantics for the same arithmetic.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, d DOUBLE)");
            execute("INSERT INTO t VALUES (4_611_686_018_427_387_904, 1.0)");

            // the quoted spelling now agrees with the unquoted one, on the column and the literal
            assertQuery("SELECT l * 2 * 4 AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT l * '02' * 4 AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT 4_611_686_018_427_387_904 * '02' * 4 AS v").noLeakCheck().expectSize().returns("v\nnull\n");

            // the regrouping is gone from the plan, so the multiplication stays left-associative
            assertQuery("SELECT l * '02' * 4 AS v FROM t").noLeakCheck().expectSize()
                    .withPlanContaining("l*'02'*4").returns("v\nnull\n");

            // a quoted floating-point literal resolves against the DOUBLE column it sits next to
            // instead of being folded into an INT pair, which used to raise ImplicitCastException
            assertQuery("SELECT d + '0.1' + 1 AS v FROM t").noLeakCheck().expectSize().returns("v\n2.1\n");
            assertQuery("SELECT 1.0 + '0.1' + 1 AS v").noLeakCheck().expectSize().returns("v\n2.1\n");

            // unquoted non-numeric constants stay reassociable: boolean logic still regroups
            assertQuery("SELECT true AND true AND false AS v").noLeakCheck().expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testCtasOfOverflowingConstantKeepsIntColumnType() throws Exception {
        // A BEHAVIOUR CHANGE from master, and a direct consequence of the width rule. Master's
        // functionToConstant0 folded an overflowing constant INT arithmetic to a LongConstant, so the
        // expression's declared TYPE depended on its runtime value: CTAS over the literal spelling
        // created a LONG column holding 1000000000000, while CTAS over the column spelling of the
        // same arithmetic created an INT column holding the wrap. The fold now leaves the INT
        // function unfolded, so the literal and column spellings agree - an INT column carrying the
        // wrapped value - and ::LONG widens on both. Pinned here because the type of a CTAS column
        // is a persisted, wire-visible contract.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (a INT, b INT)");
            execute("INSERT INTO src VALUES (1_000_000, 1_000_000)");

            // Both spellings are INT expressions and both wrap. 10^12 mod 2^32 is -727379968. The
            // projection's own type is pinned too: it is wire-visible, mapping to PG_INT4 where
            // master's folded LongConstant mapped to PG_INT8.
            assertQuery("SELECT 1_000_000 * 1_000_000 AS v")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n-727379968\n");
            assertQuery("SELECT a * b AS v FROM src")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n-727379968\n");

            // CTAS over the literal spelling: an INT column storing the wrap. On master this was a
            // LONG column holding 1000000000000.
            execute("CREATE TABLE ctas_const AS (SELECT 1_000_000 * 1_000_000 AS v)");
            assertQuery("SELECT \"column\", \"type\" FROM table_columns('ctas_const')")
                    .noLeakCheck().noRandomAccess().returns("column\ttype\nv\tINT\n");
            assertQuery("SELECT v FROM ctas_const").noLeakCheck().expectSize().returns("v\n-727379968\n");

            // CTAS over the column spelling now agrees on both the type and the stored value.
            execute("CREATE TABLE ctas_col AS (SELECT a * b AS v FROM src)");
            assertQuery("SELECT \"column\", \"type\" FROM table_columns('ctas_col')")
                    .noLeakCheck().noRandomAccess().returns("column\ttype\nv\tINT\n");
            assertQuery("SELECT v FROM ctas_col").noLeakCheck().expectSize().returns("v\n-727379968\n");

            // INSERT into an EXISTING INT column is the loudest half of the change: master's fold
            // handed the copier a LONG source, so SqlUtil.implicitCastLongAsInt threw
            // "inconvertible value". The source is now INT, so it stores the wrap instead. A loud
            // error became silent truncation - defensible, since it is what the column spelling has
            // always done, but it must not change again unnoticed.
            execute("CREATE TABLE dest_int (v INT)");
            execute("INSERT INTO dest_int SELECT 1_000_000 * 1_000_000");
            execute("INSERT INTO dest_int SELECT a * b FROM src");
            assertQuery("SELECT v FROM dest_int").noLeakCheck().expectSize()
                    .returns("v\n-727379968\n-727379968\n");

            // The wide half is still reachable, on both spellings, and CTAS over the cast keeps LONG.
            assertQuery("SELECT (1_000_000 * 1_000_000)::LONG AS v")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n1000000000000\n");
            assertQuery("SELECT (a * b)::LONG AS v FROM src")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n1000000000000\n");
            execute("CREATE TABLE ctas_cast AS (SELECT (1_000_000 * 1_000_000)::LONG AS v)");
            assertQuery("SELECT \"column\", \"type\" FROM table_columns('ctas_cast')")
                    .noLeakCheck().noRandomAccess().returns("column\ttype\nv\tLONG\n");
            assertQuery("SELECT v FROM ctas_cast").noLeakCheck().expectSize().returns("v\n1000000000000\n");
        });
    }

    @Test
    public void testDateAndTimestampCastsAgreeOnTheSameExpression() throws Exception {
        // ::DATE and ::TIMESTAMP are both 64-bit temporal reads of the same value and must not
        // disagree. IntFunction.getTimestamp() delegated to getLong() while getDate() wrapped
        // through getInt(), so an overflowing INT arithmetic widened under one and wrapped under
        // the other. Both now read at long width, on the cast and on the implicit read.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (a INT, b INT)");
            execute("INSERT INTO u VALUES (2_000_000_000, 2_000_000_000)");

            assertQuery("SELECT (a + b)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-01T01:06:40.000000Z\n");
            assertQuery("SELECT (a + b)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-02-16T07:06:40.000Z\n");

            // the literal spelling agrees with the column one
            assertQuery("SELECT (2_000_000_000 + 2_000_000_000)::DATE AS v").noLeakCheck().expectSize().returns("v\n1970-02-16T07:06:40.000Z\n");

            // NULL survives both casts
            assertQuery("SELECT (NULL::INT + 1)::DATE AS v").noLeakCheck().expectSize().returns("v\n\n");

            // a DATE column stores what ::DATE reads
            execute("CREATE TABLE d (v DATE)");
            execute("INSERT INTO d SELECT a + b FROM u");
            assertQuery("SELECT v FROM d").noLeakCheck().expectSize().returns("v\n1970-02-16T07:06:40.000Z\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensAcrossChunkedCopier() throws Exception {
        // generateChunkedCopier hands back to the single-method generator whenever the estimated
        // bytecode fits one chunk (CHUNK_TARGET_SIZE), so a one- or two-column table never reaches
        // its own INT branch however the copier type is forced. This table is wide enough to split.
        final int columnCount = 500;
        setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, RecordToRowCopierUtils.COPIER_TYPE_CHUNKED);
        assertMemoryLeak(() -> {
            final StringBuilder columns = new StringBuilder();
            final StringBuilder projection = new StringBuilder();
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    columns.append(", ");
                    projection.append(", ");
                }
                columns.append('c').append(i).append(" LONG");
                projection.append("abs(a + b) AS c").append(i);
            }
            execute("CREATE TABLE wsrc (a INT, b INT)");
            execute("INSERT INTO wsrc VALUES (2_000_000_000, 2_000_000_000)");
            execute("CREATE TABLE wide (" + columns + ")");
            execute("INSERT INTO wide SELECT " + projection + " FROM wsrc");
            assertQuery("SELECT c0, c" + (columnCount / 2) + ", c" + (columnCount - 1) + " FROM wide")
                    .noLeakCheck().expectSize()
                    .returns("c0\tc250\tc499\n4000000000\t4000000000\t4000000000\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensLikeExplicitCast() throws Exception {
        // An overflowing INT expression stored into a LONG or TIMESTAMP column must persist the
        // same value that an explicit cast of the same expression reads. The row copier reads the
        // source column at its declared INT width, so an INT-typed expression whose getLong()
        // carries the un-wrapped result used to be truncated on the way into the column while
        // ::LONG over the identical expression returned the wide value - the stored row could
        // then no longer be found by the predicate that produced it.
        //
        // The single-method and looping copiers carry the rule and are both exercised here; the
        // chunked one needs a wide table and has its own test above.
        final int[] copierTypes = {
                RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD,
                RecordToRowCopierUtils.COPIER_TYPE_CHUNKED,
                RecordToRowCopierUtils.COPIER_TYPE_LOOPING
        };
        for (int c = 0; c < copierTypes.length; c++) {
            setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierTypes[c]);
            final String s = "_" + c; // tables outlive the block, so each copier gets its own
            assertMemoryLeak(() -> {
                execute("CREATE TABLE lt" + s + " (v LONG)");
                execute("INSERT INTO lt" + s + " VALUES (1_000_000 * 1_000_000)");
                assertQuery("SELECT v FROM lt" + s).noLeakCheck().expectSize().returns("v\n1000000000000\n");
                assertQuery("SELECT (1_000_000 * 1_000_000)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
                assertQuery("SELECT count() AS c FROM lt" + s + " WHERE v = 1_000_000 * 1_000_000")
                        .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

                // a TIMESTAMP target widens too, matching ::TIMESTAMP
                execute("CREATE TABLE tt" + s + " (v TIMESTAMP)");
                execute("INSERT INTO tt" + s + " VALUES (2_000_000 * 2_000)");
                assertQuery("SELECT v FROM tt" + s).noLeakCheck().expectSize().returns("v\n1970-01-01T01:06:40.000000Z\n");
                assertQuery("SELECT (2_000_000 * 2_000)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1970-01-01T01:06:40.000000Z\n");

                // a plain INT column keeps its INT-width read - it has no wider value to give
                execute("CREATE TABLE ic" + s + " (i INT)");
                execute("INSERT INTO ic" + s + " VALUES (-2_147_483_648), (7)");
                execute("CREATE TABLE il" + s + " (l LONG)");
                execute("INSERT INTO il" + s + " SELECT i FROM ic" + s);
                assertQuery("SELECT l FROM il" + s).noLeakCheck().expectSize().returns("l\nnull\n7\n");

                // INSERT ... SELECT over a column expression widens the same way
                execute("CREATE TABLE src" + s + " (a INT, b INT)");
                execute("INSERT INTO src" + s + " VALUES (2_000_000_000, 2_000_000_000)");
                execute("CREATE TABLE dst" + s + " (l LONG)");
                execute("INSERT INTO dst" + s + " SELECT abs(a + b) FROM src" + s);
                assertQuery("SELECT l FROM dst" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");
                assertQuery("SELECT abs(a + b)::LONG AS v FROM src" + s).noLeakCheck().expectSize().returns("v\n4000000000\n");

                // a transparent wrapper over the projection must not change what is stored: LIMIT
                // and column selection hand the projection's own record straight through
                execute("CREATE TABLE lim" + s + " (l LONG)");
                execute("INSERT INTO lim" + s + " SELECT abs(a + b) FROM src" + s + " LIMIT 1");
                assertQuery("SELECT l FROM lim" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // reordering shape, so the cross index is not the identity and SelectedRecord
                // really remaps
                execute("CREATE TABLE sel" + s + " (l LONG)");
                execute("INSERT INTO sel" + s + " SELECT v FROM (SELECT a AS x, abs(a + b) AS v FROM src" + s + ")");
                assertQuery("SELECT l FROM sel" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // a residual filter and a light sort both re-read the projection's own record
                execute("CREATE TABLE flt" + s + " (l LONG)");
                execute("INSERT INTO flt" + s + " SELECT v FROM (SELECT abs(a + b) AS v FROM src" + s + ") WHERE v > 0");
                assertQuery("SELECT l FROM flt" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                execute("CREATE TABLE ord" + s + " (l LONG)");
                execute("INSERT INTO ord" + s + " SELECT abs(a + b) FROM src" + s + " ORDER BY 1");
                assertQuery("SELECT l FROM ord" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // CREATE TABLE ... AS SELECT with an explicit widening CAST takes the same route
                execute("CREATE TABLE ctas" + s + " AS (SELECT abs(a + b) AS v FROM src" + s + "), CAST(v AS LONG)");
                assertQuery("SELECT v FROM ctas" + s).noLeakCheck().expectSize().returns("v\n4000000000\n");

                // an INT expression landing exactly on INT_NULL is not null at long width: the
                // projection prints null while the stored LONG keeps -2147483648, matching ::LONG
                assertQuery("SELECT -1_073_741_824 * 2 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
                assertQuery("SELECT (-1_073_741_824 * 2)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
                execute("CREATE TABLE sent" + s + " (l LONG)");
                execute("INSERT INTO sent" + s + " VALUES (-1_073_741_824 * 2)");
                assertQuery("SELECT l FROM sent" + s).noLeakCheck().expectSize().returns("l\n-2147483648\n");

                // NULL propagates as NULL at both widths
                execute("CREATE TABLE ns" + s + " (a INT, b INT)");
                execute("INSERT INTO ns" + s + " VALUES (NULL, 1)");
                execute("CREATE TABLE nd" + s + " (l LONG)");
                execute("INSERT INTO nd" + s + " SELECT a + b FROM ns" + s);
                assertQuery("SELECT l FROM nd" + s).noLeakCheck().expectSize().returns("l\nnull\n");
            });
        }
    }

    @Test
    public void testInsertIntoWiderColumnWidensOnWalTable() throws Exception {
        // The same store rule, over the WAL write path. The copier resolves the INT-width question
        // against the WAL writer's metadata and the rows only become visible once the apply job
        // has run, so a rule that held on the non-WAL writer could still be lost here. The fuzzer
        // that motivated the rule generates WAL tables exclusively, and every other store test in
        // this class uses a non-WAL table.
        final int[] copierTypes = {
                RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD,
                RecordToRowCopierUtils.COPIER_TYPE_CHUNKED,
                RecordToRowCopierUtils.COPIER_TYPE_LOOPING
        };
        for (int c = 0; c < copierTypes.length; c++) {
            setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierTypes[c]);
            final String s = "_w" + c; // tables outlive the block, so each copier gets its own
            assertMemoryLeak(() -> {
                // INSERT ... VALUES: the VirtualRecord answers the width question from its own
                // functions, and the overflowing expression must persist its long-width value.
                execute("CREATE TABLE wl" + s + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wl" + s + " VALUES (1_000_000 * 1_000_000, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue();
                assertQuery("SELECT v FROM wl" + s).noLeakCheck().expectSize().returns("v\n1000000000000\n");

                // INSERT ... SELECT: the width answer comes from the source factory instead.
                execute("CREATE TABLE wsrc" + s + " (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wsrc" + s + " VALUES (2_000_000_000, 2_000_000_000, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue(); // the source rows must be applied before the SELECT can read them
                execute("CREATE TABLE wdst" + s + " (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wdst" + s + " SELECT abs(a + b), ts FROM wsrc" + s);
                drainWalQueue();
                assertQuery("SELECT l FROM wdst" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // A real stored INT column has only 4 bytes, so it must keep its INT-width read.
                execute("CREATE TABLE wic" + s + " (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wic" + s + " VALUES (-2_147_483_648, '2024-01-01T00:00:00.000000Z'), (7, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                execute("CREATE TABLE wil" + s + " (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO wil" + s + " SELECT i, ts FROM wic" + s);
                drainWalQueue();
                assertQuery("SELECT l FROM wil" + s).noLeakCheck().expectSize().returns("l\nnull\n7\n");
            });
        }
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughAliasedProjection() throws Exception {
        // An alias is a column reference, and a column reference over a width-unstable base column
        // used to throw the wide half away: the projection emitted a plain IntColumn, whose
        // getLong() is intToLong(getInt()) and which reports isIntWidthStable() == true. So
        // `a::LONG` over `SELECT i+j AS a` re-wrapped while `(i+j)::LONG` widened, and - worse -
        // the STORED value depended on whether an unrelated sibling column was projected: with no
        // sibling the outer projection is elided and the copier sees the arithmetic function
        // (widens), with a sibling it sees the IntColumn (wraps).
        //
        // The projection now emits a wide-reading column function whenever the base factory reports
        // the referenced column width-unstable, so the alias is transparent: the same expression
        // reads and stores the same value whatever plan shape carries it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE apt (i INT, j INT)");
            execute("INSERT INTO apt VALUES (2_000_000_000, 2_000_000_000)"); // i+j = 4000000000, wraps to -294967296

            // read path: the direct cast and the cast through an alias must agree
            assertQuery("SELECT (i+j)::LONG AS v FROM apt").noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT a::LONG AS v FROM (SELECT i+j AS a, i AS s FROM apt)")
                    .noLeakCheck().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT a::TIMESTAMP AS v FROM (SELECT i+j AS a, i AS s FROM apt)")
                    .noLeakCheck().expectSize().returns("v\n1970-01-01T01:06:40.000000Z\n");

            // the plain INT read of the alias still wraps, exactly as the arithmetic does
            assertQuery("SELECT a FROM (SELECT i+j AS a, i AS s FROM apt)")
                    .noLeakCheck().expectSize().returns("a\n-294967296\n");

            // store path: with no sibling column the outer projection is elided, so this pins the
            // reference value the sibling shapes below must match
            execute("CREATE TABLE apd1 (l LONG)");
            execute("INSERT INTO apd1 SELECT a FROM (SELECT i+j AS a FROM apt)");
            assertQuery("SELECT l FROM apd1").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // a second computed projection level over the alias keeps the outer projection alive:
            // the stored value must not change because of it
            execute("CREATE TABLE apd2 (l1 LONG, l2 LONG)");
            execute("INSERT INTO apd2 SELECT a, a+1 FROM (SELECT i+j AS a FROM apt)");
            assertQuery("SELECT l1, l2 FROM apd2").noLeakCheck().expectSize().returns("l1\tl2\n4000000000\t4000000001\n");

            // a plain sibling column has the same effect on the plan shape
            execute("CREATE TABLE apd3 (l LONG, s INT)");
            execute("INSERT INTO apd3 SELECT a, s FROM (SELECT i+j AS a, i AS s FROM apt)");
            assertQuery("SELECT l, s FROM apd3").noLeakCheck().expectSize().returns("l\ts\n4000000000\t2000000000\n");

            // a self-referencing projection resolves the alias against the projection's own
            // functions rather than the base factory, and must widen there too. The plan pins the
            // reference: were b compiled as a duplicate of the whole expression it would widen for
            // the wrong reason and this arm would assert nothing.
            assertQuery("SELECT i+j AS a, a::LONG AS b FROM apt")
                    .noLeakCheck().expectSize()
                    .withPlan("""
                            VirtualRecord
                              functions: [i+j,a::long]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: apt
                            """)
                    .returns("a\tb\n-294967296\t4000000000\n");

            // nullif reads BOTH widths of its first argument on the same row, and picks between one
            // long-width read and two INT-width reads on isRowStable(). A column reference is a
            // proxy, so it has to report the REFERENCED expression's answer: a row-stable alias
            // compares at INT width, exactly as the un-aliased spelling does...
            assertQuery("SELECT nullif(i+j, -294967296)::LONG AS v FROM apt")
                    .noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT nullif(a, -294967296)::LONG AS v FROM (SELECT i+j AS a, i AS s FROM apt)")
                    .noLeakCheck().expectSize().returns("v\nnull\n");

            // ... while a row-unstable one cannot be read twice at all, so both spellings move the
            // whole comparison to long width. Claiming row stability for the alias would read the
            // expression twice - two draws of a real rnd_* argument - and null out a row the
            // un-aliased spelling keeps. rnd_int(0,1,0) * 0 is row unstable with a fixed value, so
            // the arm is observable without a non-deterministic result.
            assertQuery("SELECT nullif(rnd_int(0,1,0) * 0 + i + j, -294967296)::LONG AS v FROM apt")
                    .noLeakCheck().noRandomAccess().expectSize().returns("v\n4000000000\n");
            assertQuery("SELECT nullif(a, -294967296)::LONG AS v FROM (SELECT rnd_int(0,1,0) * 0 + i + j AS a, i AS s FROM apt)")
                    .noLeakCheck().noRandomAccess().expectSize().returns("v\n4000000000\n");

            // Both answers have to travel together through EVERY delegating wrapper. The ORDER BY
            // below cannot be hoisted (s is not projected outward), so the plan gains a light sort
            // between the two projections; a wrapper that forwarded only the width answer would
            // report the alias row-unstable and move the comparison to long width, making the value
            // depend on an unrelated ORDER BY.
            assertQuery("SELECT nullif(a, -294967296)::LONG AS v FROM (SELECT i+j AS a, i AS s FROM apt ORDER BY s)")
                    .noLeakCheck().expectSize().returns("v\nnull\n");

            // UNION ALL emits either leg, so it answers the row question with AND where it answers
            // the width question with OR. Both legs here are width-unstable and row-stable.
            assertQuery("SELECT nullif(a, -294967296)::LONG AS v FROM (SELECT i+j AS a FROM apt UNION ALL SELECT i+j AS a FROM apt)")
                    .noLeakCheck().noRandomAccess().expectSize().returns("v\nnull\nnull\n");

            // a real stored INT column is width-stable: it must keep its 4-byte read through an
            // alias, or getLong() would splice the next row into the value
            execute("CREATE TABLE api (i INT, k INT)");
            execute("INSERT INTO api VALUES (-2_147_483_648, 1), (7, 2)");
            assertQuery("SELECT a::LONG AS v FROM (SELECT i AS a, k FROM api)")
                    .noLeakCheck().expectSize().returns("v\nnull\n7\n");
            execute("CREATE TABLE apil (l LONG)");
            execute("INSERT INTO apil SELECT a FROM (SELECT i AS a, k FROM api)");
            assertQuery("SELECT l FROM apil").noLeakCheck().expectSize().returns("l\nnull\n7\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughAliasedProjectionWithMemoization() throws Exception {
        // Production compiles projections with function memoization ON while the tests default it
        // off, and a multiply-referenced alias is exactly what gets wrapped. The wrapper has to
        // preserve the wrapped function's width and row-stability answers, because the column
        // reference the projection emitted was resolved against the unwrapped function.
        allowFunctionMemoization();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE apm (i INT, j INT)");
            execute("INSERT INTO apm VALUES (2_000_000_000, 2_000_000_000)");

            assertQuery("SELECT a::LONG AS v, a+1 AS w FROM (SELECT i+j AS a, i AS s FROM apm)")
                    .noLeakCheck().expectSize()
                    // the pin proves a memoizer really wraps the referenced expression, so the
                    // arm cannot pass by quietly compiling without one
                    .withPlan("""
                            VirtualRecord
                              functions: [a::long,a+1]
                                VirtualRecord
                                  functions: [memoize(i+j)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: apm
                            """)
                    .returns("v\tw\n4000000000\t-294967295\n");

            execute("CREATE TABLE apmd (l1 LONG, l2 LONG)");
            execute("INSERT INTO apmd SELECT a, a+1 FROM (SELECT i+j AS a FROM apm)");
            assertQuery("SELECT l1, l2 FROM apmd").noLeakCheck().expectSize().returns("l1\tl2\n4000000000\t4000000001\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughJoinMaster() throws Exception {
        // A join hands the master cursor's live record straight through JoinRecord: getInt(col < split)
        // -> master.getInt(col) wraps, getLong(col < split) -> master.getLong(col) widens. The master is
        // never value-materialised in any join, so - like limit / filter / sort / selection wrappers -
        // the join must report a widened master INT projection as width-unstable, and the row copier
        // then stores the widened value the store rule already keeps for those wrappers. The default
        // true truncated it on store, so INSERT ... SELECT through a join disagreed with the same
        // INSERT ... SELECT through a plain sub-select. (A named column read back as ::long wraps for
        // both shapes; that pre-existing behaviour is unrelated - the store is what must agree.)
        final int[] copierTypes = {
                RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD,
                RecordToRowCopierUtils.COPIER_TYPE_CHUNKED,
                RecordToRowCopierUtils.COPIER_TYPE_LOOPING
        };
        for (int c = 0; c < copierTypes.length; c++) {
            setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierTypes[c]);
            final String s = "_j" + c; // tables outlive the block, so each copier gets its own
            assertMemoryLeak(() -> {
                execute("CREATE TABLE mx" + s + " (a INT, b INT, k INT)");
                execute("INSERT INTO mx" + s + " VALUES (2_000_000_000, 2_000_000_000, 1)");
                execute("CREATE TABLE my" + s + " (k INT)");
                execute("INSERT INTO my" + s + " VALUES (1)");

                // the master subquery is an overflowing INT projection, streamed live through the join
                execute("CREATE TABLE jd" + s + " (l LONG)");
                execute("INSERT INTO jd" + s + " SELECT m.v FROM (SELECT a + b AS v, k FROM mx" + s + ") m JOIN my" + s + " y ON m.k = y.k");
                assertQuery("SELECT l FROM jd" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // the same store through a plain sub-select (no join) must produce the identical value
                execute("CREATE TABLE sd" + s + " (l LONG)");
                execute("INSERT INTO sd" + s + " SELECT m.v FROM (SELECT a + b AS v FROM mx" + s + ") m");
                assertQuery("SELECT l FROM sd" + s).noLeakCheck().expectSize().returns("l\n4000000000\n");

                // a plain INT master column has no wider value: it must keep its INT-width read
                execute("CREATE TABLE mi" + s + " (i INT, k INT)");
                execute("INSERT INTO mi" + s + " VALUES (-2_147_483_648, 1)");
                execute("CREATE TABLE jdi" + s + " (l LONG)");
                execute("INSERT INTO jdi" + s + " SELECT m.i FROM mi" + s + " m JOIN my" + s + " y ON m.k = y.k");
                assertQuery("SELECT l FROM jdi" + s).noLeakCheck().expectSize().returns("l\nnull\n");
            });
        }
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughSetOpsWindowJoinsAndCachedWindow() throws Exception {
        // The forcing function for the INT-width store rule. isColumnIntWidthStable defaults to the
        // UNSAFE direction (true) for a live pass-through, so a factory that hands an overflowing INT
        // projection through and forgets the override truncates it on store while ::LONG over the same
        // expression widens - a silent, plan-shape-dependent stored value. The per-shape tests above each
        // pin exactly one factory, so by construction they cannot catch a factory nobody added a test for.
        //
        // This test drives INSERT INTO wide_long_col SELECT <overflowing-int-expr> through EVERY remaining
        // operator that can sit atop a projection as a LIVE pass-through - UNION distinct, EXCEPT / EXCEPT
        // ALL, INTERSECT / INTERSECT ALL, the serial and fast window joins, and the cached-window LIGHT
        // factory - and asserts the stored value is the widened 4000000000, matching the direct INSERT and
        // UNION ALL. Each SELECT pins the factory in its plan, so a rewrite that stops reaching a factory
        // reddens here rather than passing silently. The projection itself wraps to -294967296 (a plain
        // INT read), so the store widening is what these assert.
        //
        // Two shapes that must NOT widen are pinned at the end, and they carry different weight. The
        // group-by HORIZON JOIN is a real guard: it emits a VirtualRecord over the aggregation map, so a
        // delegating override there would read 8 bytes off a 4-byte map slot and change the stored value,
        // reddening the assertion. The streaming Window only DOCUMENTS its case - its outer v is a plain
        // width-stable Column reference whose getLong() already re-wraps, so it stores -294967296 whether
        // or not the factory delegates. Both keep the default true and store the wrapped value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT)");
            execute("INSERT INTO t VALUES (2_000_000_000, 2_000_000_000)"); // a+b = 4000000000 wraps to -294967296
            execute("CREATE TABLE u (c INT, d INT)");
            execute("INSERT INTO u VALUES (1_500_000_000, 1_500_000_000)"); // c+d = 3000000000 wraps to -1294967296
            execute("CREATE TABLE t2 (a INT, b INT)");
            execute("INSERT INTO t2 VALUES (2_000_000_000, 2_000_000_000)"); // a+b = 4000000000, matches t for INTERSECT

            // Each operator's SELECT is pinned to its factory via the plan; the overflowing projection it
            // carries reads -294967296 as a plain INT. The store into the LONG column is what must widen it
            // to 4000000000 - a value a 32-bit wrap cannot hold, so the stored value alone proves widening.

            // UNION distinct: UnionRecord delegates getInt/getLong to the active leg, so both legs widen
            // when both are function-backed (the both-legs rule, like UNION ALL). Store widens both.
            assertQuery("SELECT a+b AS v FROM t UNION SELECT c+d AS v FROM u").assertsPlanContaining("Union");
            execute("CREATE TABLE d_union (l LONG)");
            execute("INSERT INTO d_union SELECT a+b FROM t UNION SELECT c+d FROM u");
            assertQuery("SELECT l FROM d_union ORDER BY l").noLeakCheck().expectSize().returns("l\n3000000000\n4000000000\n");

            // EXCEPT / EXCEPT ALL emit only leg A's live record, so the width answer is leg A's.
            assertQuery("SELECT a+b AS v FROM t EXCEPT SELECT c+d AS v FROM u").assertsPlanContaining("Except");
            execute("CREATE TABLE d_except (l LONG)");
            execute("INSERT INTO d_except SELECT a+b FROM t EXCEPT SELECT c+d FROM u");
            assertQuery("SELECT l FROM d_except").noLeakCheck().expectSize().returns("l\n4000000000\n");

            assertQuery("SELECT a+b AS v FROM t EXCEPT ALL SELECT c+d AS v FROM u").assertsPlanContaining("Except All");
            execute("CREATE TABLE d_except_all (l LONG)");
            execute("INSERT INTO d_except_all SELECT a+b FROM t EXCEPT ALL SELECT c+d FROM u");
            assertQuery("SELECT l FROM d_except_all").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // INTERSECT / INTERSECT ALL emit only leg A's live record too.
            assertQuery("SELECT a+b AS v FROM t INTERSECT SELECT a+b AS v FROM t2").assertsPlanContaining("Intersect");
            execute("CREATE TABLE d_intersect (l LONG)");
            execute("INSERT INTO d_intersect SELECT a+b FROM t INTERSECT SELECT a+b FROM t2");
            assertQuery("SELECT l FROM d_intersect").noLeakCheck().expectSize().returns("l\n4000000000\n");

            assertQuery("SELECT a+b AS v FROM t INTERSECT ALL SELECT a+b AS v FROM t2").assertsPlanContaining("Intersect All");
            execute("CREATE TABLE d_intersect_all (l LONG)");
            execute("INSERT INTO d_intersect_all SELECT a+b FROM t INTERSECT ALL SELECT a+b FROM t2");
            assertQuery("SELECT l FROM d_intersect_all").noLeakCheck().expectSize().returns("l\n4000000000\n");

            execute("CREATE TABLE wt (a INT, b INT, sym SYMBOL, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wt VALUES (2_000_000_000, 2_000_000_000, 'x', 1, '2024-01-01T00:00:00.000000Z')");
            execute("CREATE TABLE wp (sym SYMBOL, k INT, price INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wp VALUES ('x', 1, 1, '2024-01-01T00:00:00.000000Z')");

            // Fast (symbol-keyed) serial window join: JoinRecord hands the live master through for master
            // columns, so an overflowing INT master projection widens on store.
            final String wjFast = "SELECT v FROM (SELECT (a+b) AS v, sym, ts FROM wt) t " +
                    "WINDOW JOIN wp p ON (t.sym = p.sym) RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING";
            assertQuery(wjFast).assertsPlanContaining("Window Fast Join");
            execute("CREATE TABLE d_wj_fast (l LONG)");
            execute("INSERT INTO d_wj_fast " + wjFast);
            assertQuery("SELECT l FROM d_wj_fast").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // General (non-symbol-keyed) serial window join.
            final String wjSerial = "SELECT v FROM (SELECT (a+b) AS v, k, ts FROM wt) t " +
                    "WINDOW JOIN wp p ON (t.k = p.k) RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING";
            assertQuery(wjSerial).assertsPlanContaining("Window Join");
            execute("CREATE TABLE d_wj_serial (l LONG)");
            execute("INSERT INTO d_wj_serial " + wjSerial);
            assertQuery("SELECT l FROM d_wj_serial").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // Cached-window LIGHT: WindowLightRecord reads base columns from the live base cursor record
            // (sourceMap code >= 0), so the base projection column a+b widens on store. The window-function
            // columns (row_number/avg/sum) come from the materialised narrow chain and are stored verbatim.
            final String cwl = "SELECT (a+b) AS v, row_number() OVER (PARTITION BY sym) r1, " +
                    "avg(k) OVER () a1, sum(k) OVER () s1 FROM wt";
            assertQuery(cwl).assertsPlanContaining("CachedWindowLight");
            execute("CREATE TABLE d_cwl (l LONG, r1 LONG, a1 DOUBLE, s1 LONG)");
            execute("INSERT INTO d_cwl " + cwl);
            assertQuery("SELECT l FROM d_cwl").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // Boundary 1 (documenting, not guarding) - streaming Window: the outer v is a plain column
            // reference over the base projection, and IntColumn.getLong() is intToLong(getInt()), so the
            // wide value is already gone by the time the copier reads it. The store keeps the wrap either
            // way; this pins the behaviour rather than catching a wrong override.
            final String stream = "SELECT v FROM (SELECT (a+b) AS v, ts, row_number() OVER (ORDER BY ts) rn FROM wt)";
            assertQuery(stream).assertsPlanContaining("Window");
            execute("CREATE TABLE d_stream (l LONG)");
            execute("INSERT INTO d_stream " + stream);
            assertQuery("SELECT l FROM d_stream").noLeakCheck().expectSize().returns("l\n-294967296\n");

            // Boundary 2 - HORIZON JOIN is a markout GROUP BY: the overflowing key is copied into the map
            // before the VirtualRecord emits it, so the store must KEEP the wrap (a long-width read would
            // over-read the 4-byte key slot).
            final String horizon = "SELECT v FROM (SELECT (a+b) AS v, sym, ts FROM wt) t " +
                    "HORIZON JOIN wp AS p ON (t.sym = p.sym) RANGE FROM 0s TO 0s STEP 1s AS h GROUP BY v";
            assertQuery(horizon).assertsPlanContaining("Horizon Join");
            execute("CREATE TABLE d_horizon (l LONG)");
            execute("INSERT INTO d_horizon " + horizon);
            assertQuery("SELECT l FROM d_horizon").noLeakCheck().expectSize().returns("l\n-294967296\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughWindowJoinNullPad() throws Exception {
        // A WINDOW JOIN with a constant-false residual ON filter degenerates to the master with its
        // window-aggregate columns null-padded by ExtraNullColumnCursorFactory. Its base columns
        // (columnIndex < columnSplit) hand the master record straight through, so an overflowing INT
        // projection under the pad must stay width-unstable and widen on store, exactly like the join
        // master case. The default true truncated it. The plan pins ExtraNullColumnRecord in the store
        // path, so the test stays meaningful if the optimiser stops degenerating this shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wt (a INT, b INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wt VALUES (2_000_000_000, 2_000_000_000, 'x', '2024-01-01T00:00:00.000000Z')");
            execute("CREATE TABLE wp (sym SYMBOL, price INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wp VALUES ('x', 1, '2024-01-01T00:00:00.000000Z')");

            final String wj = "SELECT v FROM (SELECT v, sum(p.price) AS s FROM (SELECT (a + b) AS v, sym, ts FROM wt) t " +
                    "WINDOW JOIN wp p ON (t.sym = p.sym) AND 1 > 2 RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING)";
            assertQuery(wj).noLeakCheck().expectSize().withPlanContaining("ExtraNullColumnRecord").returns("v\n-294967296\n");

            execute("CREATE TABLE wd (l LONG)");
            execute("INSERT INTO wd " + wj);
            assertQuery("SELECT l FROM wd").noLeakCheck().expectSize().returns("l\n4000000000\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughUnionAll() throws Exception {
        // UNION ALL is a live pass-through (UnionRecord/UnionCastRecord delegate getInt/getLong to the
        // active leg), so when BOTH legs are overflowing INT projections the union column widens on
        // store exactly like the join master - both legs' getLong() are safe to read. When a leg is a
        // plain INT column, though, its getLong() would over-read the 4-byte slot, so the column must be
        // read at INT width for the whole union; the projection leg then wraps (unavoidable without a
        // per-leg-width read, and no worse than master). The row-copier reads one width per column, so
        // the guard is per union, not per leg.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ua (a INT, b INT)");
            execute("INSERT INTO ua VALUES (2_000_000_000, 2_000_000_000)");
            execute("CREATE TABLE ub (a INT, b INT)");
            execute("INSERT INTO ub VALUES (2_000_000_000, 2_000_000_000)");

            // both legs overflowing projections: the union widens both, like the direct/join store
            execute("CREATE TABLE ud (l LONG)");
            execute("INSERT INTO ud SELECT a + b FROM ua UNION ALL SELECT a + b FROM ub");
            assertQuery("SELECT l FROM ud").noLeakCheck().expectSize().returns("l\n4000000000\n4000000000\n");

            // mixed: projection leg + plain INT column leg. The projection leg wraps (INT-width read),
            // the real column leg is stored verbatim - crucially NOT over-read (getLong on a 4-byte INT
            // slot would splice adjacent rows into one long).
            execute("CREATE TABLE ur (i INT)");
            execute("INSERT INTO ur VALUES (5), (7)");
            execute("CREATE TABLE umd (l LONG)");
            execute("INSERT INTO umd SELECT a + b FROM ua UNION ALL SELECT i FROM ur");
            assertQuery("SELECT l FROM umd ORDER BY l").noLeakCheck().expectSize().returns("l\n-294967296\n5\n7\n");
        });
    }

    @Test
    public void testInsertIntoWiderColumnWidensThroughUnnest() throws Exception {
        // UNNEST cross-joins its master with the unnested array columns, and UnnestRecord hands the
        // master's live record straight through for every column below the split - getInt/getLong
        // delegate to baseRecord, and the cursor binds the master cursor's own record - so an
        // overflowing INT master projection keeps its wide value at long width and must widen on
        // store, exactly like the join master. The factory kept the default true and truncated it,
        // which the store-rule enumeration missed.
        //
        // The override has to key on the factory's OWN columnSplit rather than the base metadata's
        // column count: a standalone UNNEST has columnSplit == 0 while its synthetic long_sequence(1)
        // base has one column, so a base-count guard would delegate for a column that is not there.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE unt (a INT, b INT, arr DOUBLE[])");
            execute("INSERT INTO unt VALUES (2_000_000_000, 2_000_000_000, ARRAY[1.0, 2.0])");

            // one output row per array element, each carrying the same widened master value
            execute("CREATE TABLE und (l LONG)");
            execute("INSERT INTO und SELECT tt.v FROM (SELECT a+b AS v, arr FROM unt) tt, UNNEST(tt.arr) u(val)");
            assertQuery("SELECT l FROM und").noLeakCheck().expectSize().returns("l\n4000000000\n4000000000\n");

            // the identical store without UNNEST is the reference value
            execute("CREATE TABLE unb (l LONG)");
            execute("INSERT INTO unb SELECT tt.v FROM (SELECT a+b AS v FROM unt) tt");
            assertQuery("SELECT l FROM unb").noLeakCheck().expectSize().returns("l\n4000000000\n");

            // the read path agrees with the store, as it does without UNNEST
            assertQuery("SELECT tt.v::LONG AS v FROM (SELECT a+b AS v, arr FROM unt) tt, UNNEST(tt.arr) u(val)")
                    .noLeakCheck().noRandomAccess().returns("v\n4000000000\n4000000000\n");

            // a real stored INT master column has only 4 bytes and must keep its INT-width read
            execute("CREATE TABLE uni (i INT, arr DOUBLE[])");
            execute("INSERT INTO uni VALUES (-2_147_483_648, ARRAY[1.0])");
            execute("CREATE TABLE unil (l LONG)");
            execute("INSERT INTO unil SELECT tt.i FROM uni tt, UNNEST(tt.arr) u(val)");
            assertQuery("SELECT l FROM unil").noLeakCheck().expectSize().returns("l\nnull\n");

            // the row-stability half of the pair: nullif reads both widths of the master column on
            // one row and picks the comparison width from isColumnRowStable. Answering only the
            // width question moves it to long width and returns 4000000000 where the un-UNNESTed
            // spelling returns null.
            assertQuery("SELECT nullif(tt.v, -294967296)::LONG AS v FROM (SELECT a+b AS v, arr FROM unt) tt, UNNEST(tt.arr) u(val)")
                    .noLeakCheck().noRandomAccess().returns("v\nnull\nnull\n");
            assertQuery("SELECT nullif(v, -294967296)::LONG AS v FROM (SELECT a+b AS v FROM unt)")
                    .noLeakCheck().expectSize().returns("v\nnull\n");

            // the JSON unnest source is a separate implementation over the same factory, so it
            // carries the master column the same way
            execute("CREATE TABLE unj (a INT, b INT, payload VARCHAR)");
            execute("INSERT INTO unj VALUES (2_000_000_000, 2_000_000_000, '[1.0, 2.0]')");
            execute("CREATE TABLE unjd (l LONG)");
            execute("INSERT INTO unjd SELECT tt.v FROM (SELECT a+b AS v, payload FROM unj) tt, UNNEST(tt.payload COLUMNS(val DOUBLE)) u");
            assertQuery("SELECT l FROM unjd").noLeakCheck().expectSize().returns("l\n4000000000\n4000000000\n");

            // standalone UNNEST: columnSplit is 0, so every column is an unnest column and none of
            // them reaches the base. A shape guard rather than a discriminator - the synthetic
            // long_sequence(1) base answers width-stable anyway, so no standalone query can tell a
            // columnSplit guard from a base-column-count one.
            assertQuery("SELECT value FROM UNNEST(ARRAY[1.0, 2.0])")
                    .noLeakCheck().noRandomAccess().returns("value\n1.0\n2.0\n");
        });
    }

    @Test
    public void testReassociationDivModPairWrappingToIntNullWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol + C1) + (C2 / C3) - the inner constant element uses '/' (DivInt) or
        // '%' (RemInt). Both are INT-typed and propagate INT_NULL exactly like + - *, so a
        // regrouped pair (C1 + (C2 op C3)) can land on the INT_NULL sentinel and poison the
        // column to NULL. reassociateConstants modeled only + - * & | ^ when folding the
        // pair, so a '/' or '%' element made the fold bail and the guard missed the poison.
        // The fold now covers '/' and '%' (zero divisor -> INT_NULL), so the pair stays
        // un-regrouped and both paths keep the real wrapped value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (2)");

            // division: 2_147_483_647 + (2 / 2) = 2_147_483_647 + 1 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 + 2_147_483_647) + (2 / 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2_147_483_647) + (2 / 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

            // modulo: the regrouped pair 2_147_483_647 + (7 % 2) = 2_147_483_647 + 1 wraps to
            // -2^31 == INT_NULL, while the left-associative x + 2_147_483_647 = -2147483647 does
            // NOT hit the sentinel, so the un-regrouped form keeps the real wrapped value.
            assertQuery("SELECT (2 + 2_147_483_647) + (7 % 2) AS v").noLeakCheck().expectSize().returns("v\n-2147483646\n");
            assertQuery("SELECT (x + 2_147_483_647) + (7 % 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483646\n");

            // control: a '/' element pair that does NOT hit the sentinel still regroups and stays correct
            assertQuery("SELECT (2 + 1000) + (6 / 2) AS v").noLeakCheck().expectSize().returns("v\n1005\n");
            assertQuery("SELECT (x + 1000) + (6 / 2) AS v FROM t").noLeakCheck().expectSize().returns("v\n1005\n");
        });
    }

    @Test
    public void testReassociationIntDecimalMixWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol + intConst) + decimalConst: the constant-reassociation pass used to regroup
        // the column form into intCol + (intConst + decimalConst), folding the two constants to
        // a single DECIMAL and evaluating intCol + intConst at DECIMAL width - so an
        // overflowing INT addition widened instead of wrapping. The literal form folds the
        // inner INT arithmetic first (wrapping) and never regroups, so the two diverged. The
        // widening guard classified only DOUBLE / FLOAT; it now recognizes DECIMAL ('m' suffix)
        // too, so both paths wrap alike. A DECIMAL-only pair still combines.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ic INT)");
            execute("INSERT INTO t VALUES (2)");

            // implicit DECIMAL promotion wraps on both the constant and column paths
            assertQuery("SELECT (2 + 2_147_483_647) + 1.5m AS v").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT (ic + 2_147_483_647) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

            // commutative (constant on the left) and the mirror shape agree too
            assertQuery("SELECT (2_147_483_647 + ic) + 1.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");
            assertQuery("SELECT 1.5m + (ic + 2_147_483_647) AS v FROM t").noLeakCheck().expectSize().returns("v\n-2147483645.5\n");

            // a DECIMAL-only pair (no integer mixing) still combines and stays correct
            assertQuery("SELECT (ic + 1.5m) + 2.5m AS v FROM t").noLeakCheck().expectSize().returns("v\n6.0\n");
        });
    }

    @Test
    public void testReassociationIntPairWrappingToIntNullWrapsLikeColumnAndLiteral() throws Exception {
        // (intCol op C1) op C2 where the regrouped constant pair (C1 op C2) wraps exactly
        // onto the INT_NULL sentinel (-2^31). The constant-reassociation pass used to hoist
        // that pair under the column - intCol op (C1 op C2) = intCol op INT_NULL - and
        // AddInt/MulInt then return INT_NULL for every row, so the column poisoned to NULL.
        // The fully-constant literal folds left-associatively and never regroups, keeping the
        // real wrapped value. reassociateConstants now refuses to regroup an integer pair that
        // folds to INT_NULL, so both paths agree. A pair that does not hit the sentinel still
        // regroups and is unaffected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (i INT, j INT)");
            execute("INSERT INTO u VALUES (5, 2)");

            // addition: 2_147_483_647 + 1 wraps to -2^31 == INT_NULL
            // Pattern A: (A op C1) op C2
            assertQuery("SELECT (5 + 2_147_483_647) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + 2_147_483_647) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Pattern B: (C1 op A) op C2 (commutative)
            assertQuery("SELECT (2_147_483_647 + 5) + 1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (2_147_483_647 + i) + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror A: C2 op (A op C1) (commutative)
            assertQuery("SELECT 1 + (5 + 2_147_483_647) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (i + 2_147_483_647) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            // Mirror B: C2 op (C1 op A)
            assertQuery("SELECT 1 + (2_147_483_647 + 5) AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT 1 + (2_147_483_647 + i) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

            // multiplication: 65_536 * 32_768 wraps to -2^31 == INT_NULL
            assertQuery("SELECT (2 * 65_536) * 32_768 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT (j * 65_536) * 32_768 AS v FROM u").noLeakCheck().expectSize().returns("v\n0\n");

            // negative-constant pair: -2_147_483_647 + -1 also folds to -2^31 == INT_NULL, but each minus
            // binds as a unary operator, so neither operand is marked constant and the pair never
            // reaches the reassociation guard. It stays un-regrouped for that reason, so both paths keep
            // the real wrapped value (-2147483643) rather than poisoning every row to NULL.
            assertQuery("SELECT (5 + -2_147_483_647) + -1 AS v").noLeakCheck().expectSize().returns("v\n-2147483643\n");
            assertQuery("SELECT (i + -2_147_483_647) + -1 AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483643\n");

            // control: a pair that does not fold to INT_NULL still regroups and stays correct
            assertQuery("SELECT (5 + 3) + 4 AS v").noLeakCheck().expectSize().returns("v\n12\n");
            assertQuery("SELECT (i + 3) + 4 AS v FROM u").noLeakCheck().expectSize().returns("v\n12\n");
        });
    }

    @Test
    public void testReassociationLongPairWrappingToLongNullWrapsLikeColumnAndLiteral() throws Exception {
        // (longCol + C1) + C2 where the regrouped LONG constant pair (C1 + C2) wraps exactly
        // onto the LONG_NULL sentinel (-2^63). The INT-width fold rejects LONG-range /
        // L-suffixed literals (parseInt throws), so integerPairFoldsToNull never saw a LONG pair
        // and the poison slipped through: the column read LONG_NULL while the left-associative literal
        // kept the real wrapped value. The guard now also folds at LONG width and blocks a pair
        // that lands on LONG_NULL, so both paths agree. (This shape diverged before this PR too;
        // it is the LONG-width sibling of the INT_NULL guard, fixed in the same place.)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (lc LONG)");
            execute("INSERT INTO t VALUES (5)");

            // 9_223_372_036_854_775_807 + 1 wraps to -2^63 == LONG_NULL
            assertQuery("SELECT (5 + 9_223_372_036_854_775_807) + 1 AS v").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");
            assertQuery("SELECT (lc + 9_223_372_036_854_775_807) + 1 AS v FROM t").noLeakCheck().expectSize().returns("v\n-9223372036854775803\n");

            // control: a LONG pair that does NOT hit the sentinel still regroups and stays correct
            assertQuery("SELECT (5 + 9_000_000_000_000_000_000) + 100 AS v").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
            assertQuery("SELECT (lc + 9_000_000_000_000_000_000) + 100 AS v FROM t").noLeakCheck().expectSize().returns("v\n9000000000000000105\n");
        });
    }

    @Test
    public void testProductOverflowingToLongNullWrapsLikeColumn() throws Exception {
        // functionToConstant0 folded a constant INT expression to the shared NULL constant whenever
        // its getLong() read LONG_NULL. That test is not a test for nullness: getLong() computes at
        // long width, so a deep enough INT chain lands on -2^63 without being null at all.
        // 1_073_741_824 * 8 * 1_073_741_824 is exactly 2^63 there, while getInt() wraps it to a plain 0 -
        // the same 0 the column path produces. Folding it to NULL printed null for the literal form
        // and 0 for the column form. The fold now needs the sentinel at BOTH widths, which only a
        // genuine null carries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, b INT, c INT)");
            execute("INSERT INTO t VALUES (1_073_741_824, 8, 1_073_741_824)");

            // (1_073_741_824 * 8) wraps to 0 at INT width, and 0 * 1_073_741_824 stays 0.
            assertQuery("SELECT 1_073_741_824 * 8 * 1_073_741_824 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT a * b * c AS v FROM t").noLeakCheck().expectSize().returns("v\n0\n");

            // The wide cast reads getLong(), which is -2^63 == LONG_NULL on both paths.
            assertQuery("SELECT (1_073_741_824 * 8 * 1_073_741_824)::LONG AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (a * b * c)::LONG AS v FROM t").noLeakCheck().expectSize().returns("v\nnull\n");

            // A genuine NULL still folds to the NULL constant: the sentinel shows at both widths.
            assertQuery("SELECT NULL::INT * 8 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testRuntimeConstDivisionWrapsLikeColumnAndLiteral() throws Exception {
        // IntRuntimeConstFunction memoizes a composite runtime-const INT subtree. For + - *
        // (int) getLong() == getInt() (a modular ring homomorphism: the low 32 bits of the
        // widened result equal the wrapped result), but division breaks it: getInt() divides
        // the per-op-wrapped INT operands while getLong() divides the full-width ones. Here
        // (1_000_000 * 1_000_000) wraps to -727379968 at INT width, so the INT division is
        // -727379968 / 7 == -103911424, while the LONG division is 10^12 / 7 == 142857142857
        // (whose low 32 bits, 1123222089, are a different number). Before the fix the wrapper
        // derived the INT value from getLong() and served 1123222089 from getInt(); it must
        // cache each getter at its own width instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, c INT, ai INT)");
            execute("INSERT INTO x VALUES (1_000_000, 1_000_000, 7, 0)");

            // literal and column forms read the division at INT width: -103911424
            assertQuery("SELECT ((1_000_000 * 1_000_000) / 7) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");
            assertQuery("SELECT ((a * b) / c) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");

            // the bind (runtime-const) form must agree, not serve the widened low 32 bits
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\n-103911424\n");

            // a LONG-promoting context still reads the widened division on every path
            assertQuery("SELECT ((1_000_000 * 1_000_000) / 7)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");
            assertQuery("SELECT ((a * b) / c)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", "7");
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n142857142857\n");

            // NULL flows through unchanged: a null divisor yields a null quotient
            bindVariableService.clear();
            bindVariableService.setStr("b0", "1000000");
            bindVariableService.setStr("b1", "1000000");
            bindVariableService.setStr("b2", null);
            assertQuery("SELECT ((:b0::INT * :b1::INT) / :b2::INT) + ai AS v FROM x").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testRuntimeConstWidensLikeColumnAndLiteral() throws Exception {
        // A runtime-constant (but not compile-time-constant) overflowing INT arithmetic subtree -
        // here a string bind variable cast to INT - gets memoized by IntRuntimeConstFunction. The
        // wrapper must preserve the dual getInt()-wraps / getLong()-widens behavior of its arg, so
        // that a LONG-promoting context over the memoized constant agrees with the literal and the
        // column forms. Before the fix the wrapper cached only the INT (wrapped) value and re-widened
        // it, so (:b0::INT * 17_161::SHORT) * nL wrapped to +1438038338 first and the product diverged
        // from the literal/column result by an exact multiple of 2^32. The query fuzzer's literal-vs-
        // bind oracle surfaced this through an nth_value() window projection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (nL LONG, a INT)");
            execute("INSERT INTO x VALUES (6, -166_478)");

            // (-166_478 * 17_161) overflows INT: getInt() wraps to +1438038338, getLong() widens to
            // -2856928958; the outer LONG multiply must see the widened value, so * 6 == -17141573748.
            assertQuery("SELECT (-166_478 * 17_161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");
            assertQuery("SELECT (a * 17_161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17_161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\n-17141573748\n");

            // A direct ::LONG cast of the memoized constant widens identically on both paths.
            assertQuery("SELECT (a * 17_161::SHORT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n-2856928958\n");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "-166478");
            assertQuery("SELECT (:b0::INT * 17_161::SHORT)::LONG AS v FROM x").noLeakCheck().expectSize().returns("v\n-2856928958\n");

            // NULL flows through the wrapper unchanged: a null operand yields a null product.
            bindVariableService.clear();
            bindVariableService.setStr("b0", null);
            assertQuery("SELECT (:b0::INT * 17_161::SHORT) * nL AS v FROM x").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testTimezoneConversionWidensOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sec INT)");
            execute("INSERT INTO s VALUES (1_720_468_802)");

            // PR #4824 use case: seconds * 1_000_000 must not overflow into the timestamp
            assertQuery("SELECT to_utc(1_720_468_802 * 1_000_000, 'Europe/Berlin') AS v")
                    .noLeakCheck().expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
            assertQuery("SELECT to_utc(sec * 1_000_000, 'Europe/Berlin') AS v FROM s")
                    .noLeakCheck().expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
        });
    }

    @Test
    public void testWiderCastsAgreeWhenValueIsExactlyIntNull() throws Exception {
        // Sibling of testWiderCastsWidenWhenProductWrapsToIntNull: there getInt()
        // wraps to -2^31 while getLong() holds a different value; here the genuine
        // full-width value IS exactly -2^31, so getInt() == getLong() == INT_NULL.
        // Before the fix the intConst == longConst disjunct folded the literal to
        // IntConstant.NULL, so a ::LONG cast read LONG_NULL while the column path
        // widened to -2147483648. The fold now folds only when both getters agree
        // AND the value is not the sentinel, so the literal and column paths agree
        // for every cast target.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, m INT, p INT, q INT)");
            // y=2_147_483_647 for ~y; m=-1_073_741_824 for m*2; p&q = 0x80000001 & 0xFFFFFFFE = -2^31
            execute("INSERT INTO u VALUES (2_147_483_647, -1_073_741_824, -2_147_483_647, -2)");

            // ~2_147_483_647 = -2147483648 exactly: full 5-cast matrix, literal vs column
            assertQuery("SELECT (~2_147_483_647)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (~y)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");

            assertQuery("SELECT (~2_147_483_647)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");
            assertQuery("SELECT (~y)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");

            // ::DOUBLE / ::FLOAT read getInt(), which returns the value itself - and that value
            // IS the INT_NULL sentinel, so they read it as NULL. They agree with the plain INT
            // projection below and with an implicit DOUBLE promotion, which is the point: a cast
            // never disagrees with the implicit read of the same expression.
            assertQuery("SELECT (~2_147_483_647)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (~y)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (~y) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            assertQuery("SELECT CAST(~2_147_483_647 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT CAST(~y AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // ::DATE reads getLong() like ::TIMESTAMP, where -2^31 is an ordinary value rather
            // than the sentinel, so the two 64-bit temporal casts agree and neither is NULL.
            assertQuery("SELECT (~2_147_483_647)::DATE AS v").noLeakCheck().expectSize().returns("v\n1969-12-07T03:28:36.352Z\n");
            assertQuery("SELECT (~y)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-07T03:28:36.352Z\n");

            // -1_073_741_824 * 2 = -2147483648 exactly (no overflow, exact product)
            assertQuery("SELECT (-1_073_741_824 * 2)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (m * 2)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (-1_073_741_824 * 2)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT (m * 2)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");

            // bitwise AND landing exactly on 0x80000000: -2_147_483_647 & -2 = -2147483648
            assertQuery("SELECT (-2_147_483_647 & -2)::LONG AS v").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (p & q)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-2147483648\n");
            assertQuery("SELECT (-2_147_483_647 & -2)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");
            assertQuery("SELECT (p & q)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1969-12-31T23:24:12.516352Z\n");

            // control: the plain INT projection displays as null on both paths (getInt() == INT_NULL)
            assertQuery("SELECT ~2_147_483_647 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT ~y AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT -1_073_741_824 * 2 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT m * 2 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT -2_147_483_647 & -2 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT p & q AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }

    @Test
    public void testWiderCastsAgreeWithImplicitReadsOnBothPaths() throws Exception {
        // Each INT cast reads the same getter its IntFunction counterpart reads, so an
        // explicit cast never disagrees with an implicit read of the same expression:
        // getLong() / getTimestamp() / getDate() widen, getDouble() / getFloat() wrap.
        // A cast that widened where the implicit read wraps would make (y * z)::DOUBLE
        // and (y * z) + 0.0 - and round(), and ::DECIMAL - return different values for
        // the same overflowing product.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT, z INT)");
            execute("INSERT INTO u VALUES (1_000_000, 1_000_000)");

            // 1_000_000 * 1_000_000 = 10^12, which wraps to -727379968 at INT width
            assertQuery("SELECT 1_000_000 * 1_000_000 AS v").noLeakCheck().expectSize().returns("v\n-727379968\n");
            assertQuery("SELECT y * z AS v FROM u").noLeakCheck().expectSize().returns("v\n-727379968\n");

            // ::DOUBLE wraps, and agrees with the implicit DOUBLE reads (+ 0.0, round())
            assertQuery("SELECT (1_000_000 * 1_000_000)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-7.27379968E8\n");
            assertQuery("SELECT (y * z)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-7.27379968E8\n");
            assertQuery("SELECT (y * z)::DOUBLE = (y * z) + 0.0 AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");
            assertQuery("SELECT (y * z)::DOUBLE = round(y * z, 0) AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // ::DECIMAL keeps INT width too, so ::DOUBLE must not disagree with it
            assertQuery("SELECT (y * z)::DECIMAL(20,0)::DOUBLE = (y * z)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // the FLOAT cast wraps, and agrees with the implicit FLOAT read. CAST(x AS FLOAT),
            // not x::FLOAT: the :: form resolves FLOAT to DOUBLE and would not reach
            // CastIntToFloatFunctionFactory at all.
            assertQuery("SELECT CAST(1_000_000 * 1_000_000 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\n-7.2738E8\n");
            assertQuery("SELECT CAST(y * z AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\n-7.2738E8\n");
            assertQuery("SELECT CAST(y * z AS FLOAT) = (y * z) + 0.0f AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            // ::DATE widens like ::LONG and ::TIMESTAMP: 1000000000000 ms after the epoch
            assertQuery("SELECT (1_000_000 * 1_000_000)::DATE AS v").noLeakCheck().expectSize().returns("v\n2001-09-09T01:46:40.000Z\n");
            assertQuery("SELECT (y * z)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n2001-09-09T01:46:40.000Z\n");

            // ::LONG and ::TIMESTAMP widen, matching IntFunction.getLong() / getTimestamp()
            assertQuery("SELECT (1_000_000 * 1_000_000)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (y * z)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (y * z)::LONG = (y * z) + 0L AS v FROM u").noLeakCheck().expectSize().returns("v\ntrue\n");

            assertQuery("SELECT (1_000_000 * 1_000_000)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1970-01-12T13:46:40.000000Z\n");
            assertQuery("SELECT (y * z)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-12T13:46:40.000000Z\n");

            // a WHERE clause sees the same wrapped value through the cast as without it,
            // so a bound above the wrapped product filters both forms out alike
            assertQuery("SELECT y FROM u WHERE (y * z)::DOUBLE > 5e11").noLeakCheck().returns("y\n");
            assertQuery("SELECT y FROM u WHERE (y * z) > 5e11").noLeakCheck().returns("y\n");
        });
    }

    @Test
    public void testWiderCastsWidenOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2_147_483_647)");

            // LONG / TIMESTAMP targets hold the un-wrapped value, constant and column alike
            assertQuery("SELECT (2_147_483_647 + 3)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483650\n");

            // TIMESTAMP (micros) widens directly, not only via to_utc()
            assertQuery("SELECT (2_147_483_647 + 3)::TIMESTAMP AS v").noLeakCheck().expectSize().returns("v\n1970-01-01T00:35:47.483650Z\n");
            assertQuery("SELECT (y + 3)::TIMESTAMP AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-01T00:35:47.483650Z\n");

            // DOUBLE / FLOAT keep INT width and wrap, on both paths alike: they must agree with
            // IntFunction.getDouble() / getFloat(), which read getInt().
            // See testWiderCastsAgreeWithImplicitReadsOnBothPaths.
            assertQuery("SELECT (2_147_483_647 + 3)::DOUBLE AS v").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.147483646E9\n");

            assertQuery("SELECT CAST(2_147_483_647 + 3 AS FLOAT) AS v").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");
            assertQuery("SELECT CAST(y + 3 AS FLOAT) AS v FROM u").noLeakCheck().expectSize().returns("v\n-2.1474836E9\n");

            // ::DATE widens with ::LONG and ::TIMESTAMP: 2147483650 ms after the epoch
            assertQuery("SELECT (2_147_483_647 + 3)::DATE AS v").noLeakCheck().expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");
            assertQuery("SELECT (y + 3)::DATE AS v FROM u").noLeakCheck().expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");
        });
    }

    @Test
    public void testWiderCastsWidenRemBitwiseAbsOnBothPaths() throws Exception {
        // BF9 made explicit widening casts read getLong(), so +, -, *, / and unary
        // minus widen an overflowing INT operand under ::LONG. %, & | ^ ~ and abs
        // inherited the wrapping getLong() and stayed narrow, leaving ((a*b)/c)::LONG
        // widened but ((a*b)%c)::LONG wrapped. Those operators now override getLong()
        // to recurse at long width too, so every integer operator widens alike under
        // a widening cast, constant and column paths identical.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (1_000_000)");

            // reference: '/' already widened (DivInt overrides getLong)
            assertQuery("SELECT ((1_000_000 * 1_000_000) / 7)::LONG AS v").noLeakCheck().expectSize().returns("v\n142857142857\n");
            assertQuery("SELECT ((y * y) / 7)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n142857142857\n");

            // '%' now widens to match '/': 10^12 % 7 = 1 (was 0 from the wrapped product)
            assertQuery("SELECT ((1_000_000 * 1_000_000) % 7)::LONG AS v").noLeakCheck().expectSize().returns("v\n1\n");
            assertQuery("SELECT ((y * y) % 7)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1\n");

            // '&' with an all-ones mask is identity: the widened value holds the full product
            assertQuery("SELECT ((1_000_000 * 1_000_000) & -1)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) & -1)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '|' with 0 is identity
            assertQuery("SELECT ((1_000_000 * 1_000_000) | 0)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) | 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '^' with 0 is identity
            assertQuery("SELECT ((1_000_000 * 1_000_000) ^ 0)::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT ((y * y) ^ 0)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // '~' complements the full-width product: ~10^12 = -(10^12) - 1
            assertQuery("SELECT (~(1_000_000 * 1_000_000))::LONG AS v").noLeakCheck().expectSize().returns("v\n-1000000000001\n");
            assertQuery("SELECT (~(y * y))::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n-1000000000001\n");

            // abs() widens: the wrapped product is negative, so a narrow abs would flip
            // its sign; the widened value keeps the true magnitude.
            assertQuery("SELECT (abs(1_000_000 * 1_000_000))::LONG AS v").noLeakCheck().expectSize().returns("v\n1000000000000\n");
            assertQuery("SELECT (abs(y * y))::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n1000000000000\n");

            // control: the plain INT projection (no widening cast) still wraps mod 2^32
            // on both paths - getInt() is unchanged.
            assertQuery("SELECT (1_000_000 * 1_000_000) % 7 AS v").noLeakCheck().expectSize().returns("v\n0\n");
            assertQuery("SELECT (y * y) % 7 AS v FROM u").noLeakCheck().expectSize().returns("v\n0\n");
        });
    }

    @Test
    public void testWiderCastsWidenWhenProductWrapsToIntNull() throws Exception {
        // Boundary case: the wrapped INT product lands EXACTLY on the INT_NULL
        // sentinel (-2^31). functionToConstant0 must not mistake that for a real
        // null. Before the fix it folded the literal to IntConstant.NULL (the
        // intConst == INT_NULL disjunct fired before the leave-unfolded branch),
        // so a wider cast read NULL while the column path widened to the true
        // value. The fold now keys off longConst == LONG_NULL, which only a
        // genuine null has, so both paths widen alike.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2_147_483_647)");

            // 2_147_483_647 + 1 wraps to -2147483648 == INT_NULL; getLong() holds 2147483648.
            assertQuery("SELECT (2_147_483_647 + 1)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483648\n");
            assertQuery("SELECT (y + 1)::LONG AS v FROM u").noLeakCheck().expectSize().returns("v\n2147483648\n");

            // A different product that also wraps to exactly -2^31: 65_536 * 32_768.
            assertQuery("SELECT (65_536 * 32_768)::LONG AS v").noLeakCheck().expectSize().returns("v\n2147483648\n");

            // The plain INT projection still wraps mod 2^32 on both paths.
            assertQuery("SELECT 2_147_483_647 + 1 AS v").noLeakCheck().expectSize().returns("v\nnull\n");
            assertQuery("SELECT y + 1 AS v FROM u").noLeakCheck().expectSize().returns("v\nnull\n");
        });
    }
}
