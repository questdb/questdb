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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A DECIMAL(p,p) column holds values in (-1, 1) with p fraction digits. The leading zero of such a
 * value is not a significant digit, so every literal below one has to fit, at any width.
 */
public class DecimalFullScaleTest extends AbstractCairoTest {

    @Test
    public void testFullScaleColumnHoldsValuesAtEveryWidth() throws Exception {
        assertMemoryLeak(() -> {
            assertRoundTrip(1, "0.5");
            assertRoundTrip(3, "0.125");
            assertRoundTrip(38, "0." + "1".repeat(38));
            assertRoundTrip(76, "0." + "9".repeat(76));
        });
    }

    @Test
    public void testInferredTypeOfLiteralBelowOne() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "select typeOf(0.5m) a, typeOf(0.125m) b, typeOf(-0.125m) c, typeOf(0.00001m) d, typeOf(1.5m) e, typeOf(123.45m) f"
        )
                .noLeakCheck()
                .expectSize()
                .returns("a\tb\tc\td\te\tf\nDECIMAL(1,1)\tDECIMAL(3,3)\tDECIMAL(3,3)\tDECIMAL(5,5)\tDECIMAL(2,1)\tDECIMAL(5,2)\n"));
    }

    @Test
    public void testOrdinaryScaleUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (a decimal(10,2))");
            execute("insert into t values (0.5m)");
            execute("insert into t values (123.45m)");
            execute("insert into t values (-1.5m)");
            execute("insert into t values ('0.05')");
            execute("insert into t values (1.25::decimal(10,2))");

            assertQuery("select a from t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n0.50\n123.45\n-1.50\n0.05\n1.25\n");

            assertExceptionNoLeakCheck(
                    "insert into t values (999999999.99m)",
                    -1,
                    "inconvertible value: `999999999.99` [DECIMAL(11,2) -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testValueOutsideFullScaleRangeRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (a decimal(3,3))");

            assertExceptionNoLeakCheck(
                    "insert into t values (1.5m)",
                    -1,
                    "inconvertible value: `1.500` [DECIMAL(2,1) -> DECIMAL(3,3)]"
            );
            assertExceptionNoLeakCheck("insert into t values ('1.5')", -1, "inconvertible value: `1.5`");
            assertExceptionNoLeakCheck(
                    "insert into t values (1.5::decimal(3,3))",
                    22,
                    "decimal '1.5' requires precision of 4 but is limited to 3"
            );
            assertExceptionNoLeakCheck("select cast('1.0' as decimal(1,1))", -1, "inconvertible value: `1.0`");
            assertExceptionNoLeakCheck("insert into t values ('10')", -1, "inconvertible value: `10`");
            assertExceptionNoLeakCheck("insert into t values ('0.0001')", -1, "inconvertible value: `0.0001`");

            assertQuery("select a from t").noLeakCheck().expectSize().returns("a\n");
        });
    }

    @Test
    public void testZeroTextBoundToFullScaleVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DECIMAL(3,3))");
            bindVariableService.define(0, ColumnType.getDecimalType(3, 3), 0);

            bindVariableService.setStr(0, "0");
            execute("INSERT INTO t VALUES ($1)");
            bindVariableService.setStr(0, "-0");
            execute("INSERT INTO t VALUES ($1)");
            bindVariableService.setStr(0, "0.125");
            execute("INSERT INTO t VALUES ($1)");
            bindVariableService.setStr(0, null);
            execute("INSERT INTO t VALUES ($1)");

            assertQuery("SELECT a FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\n0.000\n0.000\n0.125\n\n");

            try {
                bindVariableService.setStr(0, "1.5");
                Assert.fail("expected '1.5' to be rejected");
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `1.5` [STRING -> DECIMAL(3,3)]");
            }
        });
    }

    @Test
    public void testZeroTextCastsToEveryWidth() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                """
                        SELECT '0'::varchar::decimal(3,3) a,
                               '0.0'::varchar::decimal(3,3) b,
                               '-0'::varchar::decimal(3,3) c,
                               '0'::decimal(1,1) d,
                               '0'::decimal(38,38) e,
                               '0'::decimal(76,76) f"""
        )
                .noLeakCheck()
                .expectSize()
                .returns("a\tb\tc\td\te\tf\n0.000\t0.000\t0.000\t0.0\t0." + "0".repeat(38) + "\t0." + "0".repeat(76) + "\n"));
    }

    @Test
    public void testZeroTextHoldsAtEveryWidth() throws Exception {
        assertMemoryLeak(() -> {
            assertZeroRoundTrip(1);
            assertZeroRoundTrip(3);
            assertZeroRoundTrip(38);
            assertZeroRoundTrip(76);
        });
    }

    private void assertRoundTrip(int precision, String value) throws Exception {
        final String type = "decimal(" + precision + "," + precision + ")";
        execute("create table t (a " + type + ")");
        execute("insert into t values (" + value + "m)");
        // the leading zero may be omitted altogether
        execute("insert into t values (" + value.substring(1) + "m)");
        execute("insert into t values (-" + value + "m)");
        // varchar and double literals reach the column through a cast
        execute("insert into t values ('" + value + "')");
        execute("insert into t values (" + value + "::" + type + ")");

        assertQuery("select a from t")
                .noLeakCheck()
                .expectSize()
                .returns("a\n" + value + "\n" + value + "\n-" + value + "\n" + value + "\n" + value + "\n");
        execute("drop table t");
    }

    private void assertZeroRoundTrip(int precision) throws Exception {
        final String type = "decimal(" + precision + "," + precision + ")";
        final String zero = "0." + "0".repeat(precision);
        execute("CREATE TABLE t (a " + type + ")");
        // every text spelling of zero reaches the column through the parser
        execute("INSERT INTO t VALUES ('0'::varchar), ('-0'), ('0.0'), ('0.'), ('.0'), ('000'), ('0e5')");
        execute("INSERT INTO t VALUES ('0'::varchar::" + type + ")");
        execute("INSERT INTO t VALUES ('0'::" + type + ")");
        execute("INSERT INTO t VALUES (0m), (0)");

        assertQuery("SELECT a FROM t")
                .noLeakCheck()
                .expectSize()
                .returns("a\n" + (zero + "\n").repeat(11));
        execute("DROP TABLE t");
    }
}
