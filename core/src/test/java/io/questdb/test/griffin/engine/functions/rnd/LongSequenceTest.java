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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

/**
 * {@code long_sequence} reads its row count off an ordinary compiled constant with
 * {@code getLong(null)}, so an INT count argument wraps at 32 bits exactly as every other INT
 * expression does - see {@code IntWidthWrapTest} for the rule and {@code IntWidthContextTest} for
 * the contexts that read a wrapped INT at 64 bits. Both cursor factories then clamp a negative
 * count with {@code Math.max(0L, recordCount)}.
 * <p>
 * Released 9.4.3 answered 3000000000 rows for {@code long_sequence(100_000 * 30_000)}, because
 * {@code FunctionParser.functionToConstant0} folded an overflowing INT expression to a
 * {@code LongConstant} and the factory read the full-width count off it. This revision folds the
 * same expression to an {@code IntConstant}, so the count wraps before the factory ever sees it.
 * These are therefore characterization tests of a deliberate, disclosed divergence from released
 * behaviour, not of a correct answer.
 * <p>
 * These tests characterize that composition rather than guard it, and the reason is the same one
 * that keeps an ordinary {@code WHERE} bound unguarded while a window frame width and a
 * {@code SAMPLE BY} stride are refused: the count is a value the caller can see. It has a projected
 * spelling - {@code SELECT 100_000 * 30_000} answers -1294967296 and says so - and, uniquely among
 * these consumers, it IS the output: the cursor emits exactly {@code count} rows, so
 * {@code count()} over the sequence prints the very number the factory received. A wrapped count
 * cannot act unseen the way a wrapped frame width can, where every row still looks plausible.
 * <p>
 * The plan line {@code long_sequence count: n} is a weaker signal than the projected spelling, and
 * only the spelling holds in both directions. Both {@code toPlan} bodies print the cursor's record
 * count, which the constructor has already clamped, so {@code EXPLAIN} names the outcome rather
 * than the wrap: a positive wrap prints {@code long_sequence count: 705032704} and does reveal it,
 * while a negative wrap prints {@code long_sequence count: 0} - byte-identical to what
 * {@code long_sequence(0)} and {@code long_sequence(-2)} print. A reader who consults
 * {@code EXPLAIN} alone cannot tell a wrapped count from an explicit zero.
 * <p>
 * A negative-count rejection was considered and not implemented. It draws its line at the sign of
 * the wrapped result, which is an accident of the multiplicands rather than a property of the
 * wrap: {@code 100_000 * 30_000} lands on -1294967296 and answers zero rows, while
 * {@code 100_000 * 50_000} lands on 705032704 and answers 705 million rows instead of five
 * billion. The second is the quieter failure of the two and no sign check reaches it. The
 * rejection would also break two behaviours that predate the wrap: an explicitly negative count
 * has answered an empty cursor since {@code long_sequence} was introduced (pinned by
 * {@code SqlCodeGeneratorTest.testLongCursor}), and a NULL count - which LONG spells as
 * {@code Long.MIN_VALUE} - reaches the same clamp, so rejecting negatives would turn NULL into an
 * error in the one place the engine otherwise lets a NULL width pass through.
 * <p>
 * The remedy is the ordinary one and it is exact: widen an operand.
 */
public class LongSequenceTest extends AbstractFunctionFactoryTest {

    // 100_000 * 30_000 = 3_000_000_000 needs 32 bits, so the INT product wraps to this.
    private static final String COUNT_WRAPPED_NEGATIVE = "-1294967296";
    // 100_000 * 50_000 = 5_000_000_000 wraps to this - still positive, so rows still come out.
    private static final String COUNT_WRAPPED_POSITIVE = "705032704";

    @Test
    public void testBadArgumentTypeFailsGracefully() throws SqlException {
        assertFailure(0, "invalid arguments", 5.0, 5.0, 5.0);
    }

    @Test
    public void testSeedingArmReadsTheCountExactlyAsTheOneArgArmDoes() throws Exception {
        // The 3+ arg arm builds a different factory, but it reads the count through the same
        // getLong(null) and hands it to the same clamping cursor, so every answer above repeats.
        assertMemoryLeak(() -> {
            assertQuery("SELECT count() FROM long_sequence(3, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3\n");
            assertQuery("SELECT count() FROM long_sequence(0, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
            assertQuery("SELECT count() FROM long_sequence(-2, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
            assertQuery("SELECT count() FROM long_sequence(100_000 * 30_000, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
            assertQuery("SELECT count() FROM long_sequence(100_000 * 50_000, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n" + COUNT_WRAPPED_POSITIVE + "\n");
            assertQuery("SELECT count() FROM long_sequence(100_000 * 30_000L, 1, 2)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3000000000\n");

            // this arm names the count on its plan line too, post-clamp as above
            assertQuery("SELECT * FROM long_sequence(100_000 * 30_000, 1, 2)")
                    .noLeakCheck()
                    .assertsPlan("long_sequence count: 0 seedLo: 1 seedHi: 2\n");
        });
    }

    @Test
    public void testWideningOneOperandRestoresTheFullCount() throws Exception {
        // Every spelling that keeps the multiplication at 64 bits produces the mathematical count.
        assertMemoryLeak(() -> {
            assertQuery("SELECT count() FROM long_sequence(100_000 * 30_000L)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3000000000\n");
            assertQuery("SELECT count() FROM long_sequence(100_000::long * 30_000)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3000000000\n");
            assertQuery("SELECT count() FROM long_sequence(3_000_000_000)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3000000000\n");

            assertQuery("SELECT * FROM long_sequence(100_000 * 30_000L)")
                    .noLeakCheck()
                    .assertsPlan("long_sequence count: 3000000000\n");
        });
    }

    @Test
    public void testZeroNegativeAndNullCountsProduceAnEmptyCursorRatherThanAnError() throws Exception {
        // Pinned since long_sequence was introduced - SqlCodeGeneratorTest.testLongCursor has
        // asserted long_sequence(-2) empty since 2018. Math.max(0L, recordCount) in both factories
        // is what answers, and nothing here raises.
        assertMemoryLeak(() -> {
            assertQuery("SELECT * FROM long_sequence(3)")
                    .noLeakCheck().expectSize()
                    .returns("""
                            x
                            1
                            2
                            3
                            """);
            assertQuery("SELECT * FROM long_sequence(0)")
                    .noLeakCheck().expectSize()
                    .returns("x\n");
            assertQuery("SELECT * FROM long_sequence(-2)")
                    .noLeakCheck().expectSize()
                    .returns("x\n");
            // LONG spells NULL as Long.MIN_VALUE, so a NULL count meets the same clamp
            assertQuery("SELECT * FROM long_sequence(null)")
                    .noLeakCheck().expectSize()
                    .returns("x\n");
            // 2_147_483_647 + 1 folds to INT_NULL, which widens to LONG_NULL
            assertQuery("SELECT * FROM long_sequence(2_147_483_647 + 1)")
                    .noLeakCheck().expectSize()
                    .returns("x\n");
        });
    }

    @Test
    public void testWrappedIntCountIsTheCountTheCursorProduces() throws Exception {
        assertMemoryLeak(() -> {
            // the identical SELECT previews exactly the value long_sequence receives
            assertQuery("SELECT 100_000 * 30_000 AS c")
                    .noLeakCheck().expectSize()
                    .returns("c\n" + COUNT_WRAPPED_NEGATIVE + "\n");
            assertQuery("SELECT 100_000 * 50_000 AS c")
                    .noLeakCheck().expectSize()
                    .returns("c\n" + COUNT_WRAPPED_POSITIVE + "\n");

            // a wrap that lands negative meets the clamp, so the cursor is empty
            assertQuery("SELECT count() FROM long_sequence(100_000 * 30_000)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n0\n");
            // a wrap that lands positive is a row count in its own right - 705 million rows rather
            // than five billion. This is the case a negative-count check cannot reach.
            assertQuery("SELECT count() FROM long_sequence(100_000 * 50_000)")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n" + COUNT_WRAPPED_POSITIVE + "\n");

            // The plan names the count the cursor will use - the post-clamp one. The negative wrap
            // therefore prints the same "count: 0" that long_sequence(0) prints, and only the
            // positive wrap makes EXPLAIN reveal the wrap itself.
            assertQuery("SELECT * FROM long_sequence(100_000 * 30_000)")
                    .noLeakCheck()
                    .assertsPlan("long_sequence count: 0\n");
            assertQuery("SELECT * FROM long_sequence(100_000 * 50_000)")
                    .noLeakCheck()
                    .assertsPlan("long_sequence count: " + COUNT_WRAPPED_POSITIVE + "\n");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LongSequenceFunctionFactory();
    }
}
