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

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The contexts that read an INT expression at 64 bits without an explicit cast asking them to.
 * <p>
 * {@link IntArithmeticOverflowFoldingTest} pins the casts, the implicit-read agreement and the
 * store path. What it does not cover is the set of contexts that reach 64 bits through overload
 * resolution or type escalation - where the width is decided by a PEER or a TARGET rather than
 * by anything visible in the expression's own syntax:
 * <ul>
 *     <li>arithmetic with a 64-bit peer. The LONG row of {@code ColumnType.OVERLOAD_PRIORITY}
 *     has no INT, so {@code +(II)} cannot match an {@code (INT, LONG)} pair, {@code +(LL)} wins
 *     and {@code AddLongFunctionFactory} reads the INT operand through {@code getLong()}.</li>
 *     <li>a conditional with a 64-bit arm. {@code CaseCommon.getCommonType} escalates
 *     {@code (INT, LONG)} to LONG and no {@code (INT, LONG)} cast factory exists, so the LONG
 *     variant of COALESCE / CASE / NULLIF reads the INT arm directly.</li>
 *     <li>a comparison against a function-valued 64-bit peer, whose declared type is known only
 *     once the peer has been built.</li>
 *     <li>an {@code UPDATE} SET target, a typed destination like an {@code INSERT} target but
 *     one that bypasses the row copier: {@code UpdateOperatorImpl.updateColumnValues} reads the
 *     virtual record with {@code getLong()} / {@code getTimestamp()} / {@code getDate()}
 *     dispatched on the target column's type.</li>
 *     <li>the designated timestamp target, whose two spellings behave differently.</li>
 * </ul>
 * Released 9.4.3 and this branch agree on every value asserted here, apart from the DATE arm of
 * the UPDATE test, which is called out in place. So these are not characterization tests: any
 * change to INT width has to keep them green, and a redesign that decides width from syntax
 * alone cannot.
 */
public class IntWidthContextTest extends AbstractCairoTest {

    // 1_720_468_802 * 1_000_000: the GitHub issue #4752 expression.
    private static final String TS_OF_WIDE = "2024-07-08T20:00:02.000000Z";
    private static final String WIDE = "1720468802000000";

    @Test
    public void testComparisonAgainstFunctionValuedPeerComputesWide() throws Exception {
        // A comparison takes its width from the peer's declared type. When the peer is a bare
        // column or literal that type is available syntactically; when it is a function -
        // abs(l), now(), dateadd(...) - it is not, and only a mechanism that can see the built
        // peer knows the comparison runs at 64 bits. Both spellings must agree, because they
        // are the same predicate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cmp (i INT, j INT, l LONG)");
            execute("INSERT INTO cmp VALUES (2_000_000_000, 2_000_000_000, 0)");

            // bare LONG column peer: wide, so the positive sum passes
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > l")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            // function-valued LONG peer: the same predicate, the same answer
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > abs(l)")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > l + 0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n1\n");

            // and the INT-peer spelling of the same comparison keeps wrapping, so the two
            // spellings of the bound genuinely disagree - the documented consequence of
            // taking width from the peer rather than promoting the type outright
            assertQuery("SELECT count() AS c FROM cmp WHERE i + j > 0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("c\n0\n");
        });
    }

    @Test
    public void testConditionalWithWideArmComputesWide() throws Exception {
        // CaseCommon escalates (INT, LONG) to LONG and there is no (INT, LONG) cast factory,
        // so getCastFunction hands the INT arm back unchanged and the LONG conditional reads it
        // through getLong(). The 64-bit context is created by the SIBLING arm, with no cast
        // anywhere in the expression - IntArithmeticOverflowFoldingTest covers the shape where
        // an outer ::LONG supplies the context instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cnd (i INT, j INT, l LONG)");
            execute("INSERT INTO cnd VALUES (2_000_000_000, 2_000_000_000, 0)");

            // COALESCE: the LONG sibling escalates the result type, and the INT arm is read wide
            assertQuery("SELECT coalesce(i + j, l) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");

            // CASE: same escalation through the ELSE arm
            assertQuery("SELECT (CASE WHEN true THEN i + j ELSE l END) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");

            // NULLIF resolves nullif(LL) and both compares and returns at 64 bits
            assertQuery("SELECT nullif(i + j, l) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");

            // control: with an INT sibling the result stays INT and wraps, so the two arms of
            // the matrix cannot both pass by moving every conditional to long width
            assertQuery("SELECT coalesce(i + j, 0) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n-294967296\n");
            assertQuery("SELECT (CASE WHEN true THEN i + j ELSE 0 END) AS v FROM cnd")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n-294967296\n");
        });
    }

    @Test
    public void testDesignatedTimestampTargetRejectsIntSelectAndWidensValues() throws Exception {
        // The designated timestamp is the one store target the row copier skips, so it is the
        // one target whose width does not come from RecordToRowCopierUtils. The two ways in
        // behave differently, and the difference bounds how much store-path work a compile-time
        // retyping design actually owes:
        //
        //  - INSERT ... SELECT is REJECTED outright. SqlCompilerImpl requires the select column
        //    feeding the designated timestamp to be TIMESTAMP, STRING, VARCHAR or NULL, so an
        //    INT expression never reaches copyOrderedBatched0. The guard is identical in
        //    released 9.4.3, so this shape cannot regress however the width rules change - and
        //    that removes the only shape that would have forced expected-type plumbing through
        //    interposed models.
        //  - INSERT ... VALUES IS accepted and stores the wide value, because InsertRowImpl
        //    reads the designated timestamp with timestampFunction.getTimestamp(null) rather
        //    than through the copier. This one does regress if an INT expression stops
        //    answering getTimestamp() at long width.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dtsSel (t TIMESTAMP) TIMESTAMP(t) PARTITION BY YEAR");
            execute("CREATE TABLE dtsSrc (secs INT)");
            execute("INSERT INTO dtsSrc VALUES (1_720_468_802)");
            assertExceptionNoLeakCheck(
                    "INSERT INTO dtsSel SELECT secs * 1_000_000 FROM dtsSrc",
                    12,
                    "expected timestamp column but type is INT"
            );

            // an explicit cast is accepted, and is a 64-bit context in its own right
            execute("INSERT INTO dtsSel SELECT (secs * 1_000_000)::TIMESTAMP FROM dtsSrc");
            assertQuery("SELECT t FROM dtsSel").noLeakCheck().timestamp("t").expectSize()
                    .returns("t\n" + TS_OF_WIDE + "\n");

            // VALUES reaches the writer without a copier and without a cast, and widens
            execute("CREATE TABLE dtsVal (t TIMESTAMP) TIMESTAMP(t) PARTITION BY YEAR");
            execute("INSERT INTO dtsVal VALUES (1_720_468_802 * 1_000_000)");
            assertQuery("SELECT t FROM dtsVal").noLeakCheck().timestamp("t").expectSize()
                    .returns("t\n" + TS_OF_WIDE + "\n");
        });
    }

    @Test
    public void testMixedWidthArithmeticComputesWide() throws Exception {
        // INT arithmetic next to a 64-bit operand. The LONG row of ColumnType.OVERLOAD_PRIORITY
        // does not contain INT, so +(II) cannot match a (INT, LONG) pair and +(LL) is selected;
        // AddLongFunctionFactory then reads the INT operand through getLong(). The 64-bit
        // context comes from the PEER, with no cast and no temporal function in sight.
        //
        // The LONG-constant spelling of this is already pinned by
        // IntArithmeticOverflowFoldingTest#testImplicitDoublePromotionWrapsLikeConstantAndColumn;
        // the column and temporal peers below are not covered anywhere else.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE mix (secs INT, i INT, j INT, l LONG, one LONG)");
            execute("INSERT INTO mix VALUES (1_720_468_802, 2_000_000_000, 2_000_000_000, 0, 1)");

            // LONG column peer, on both sides of the operator
            assertQuery("SELECT (i + j) + l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");
            assertQuery("SELECT l + (i + j) AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");
            assertQuery("SELECT (i + j) * one AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");
            assertQuery("SELECT (i + j) - l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");

            // the issue #4752 expression reaching 64 bits through a peer rather than through
            // to_utc(): seconds-to-micros next to a LONG offset
            assertQuery("SELECT (secs * 1_000_000) + l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n" + WIDE + "\n");

            // a bitwise operator promotes the same way
            assertQuery("SELECT (i + j) | l AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.LONG).returns("v\n4000000000\n");

            // control: an all-INT peer keeps the whole expression at INT width
            assertQuery("SELECT (i + j) + 0 AS v FROM mix")
                    .noLeakCheck().expectSize().columnType(0, ColumnType.INT).returns("v\n-294967296\n");
        });
    }

    @Test
    public void testUpdateIntoWiderColumnWidens() throws Exception {
        // An UPDATE SET target is a typed destination exactly as an INSERT target is, but it
        // does not go through a row copier: when the SET expression's type does not match the
        // column, SqlCodeGenerator falls back to select-virtual without inserting a cast, and
        // UpdateOperatorImpl.updateColumnValues reads the virtual record with getLong() /
        // getTimestamp() / getDate() dispatched on the TARGET column's type.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE upd (l LONG, t TIMESTAMP, d DATE, i INT, j INT)");
            execute("INSERT INTO upd VALUES (0, '1970-01-01T00:00:00.000000Z', '1970-01-01T00:00:00.000Z', 2_000_000_000, 2_000_000_000)");

            execute("UPDATE upd SET l = i + j");
            assertQuery("SELECT l FROM upd").noLeakCheck().expectSize().returns("l\n4000000000\n");

            execute("UPDATE upd SET t = i + j");
            assertQuery("SELECT t FROM upd").noLeakCheck().expectSize().returns("t\n1970-01-01T01:06:40.000000Z\n");

            // A DATE target reads getDate(). This asserts the CURRENT branch, which routes
            // getDate() through getLong() like getTimestamp(); released 9.4.3 returned
            // Numbers.intToLong(getInt()) here and stored the wrap (-294967296 ms), so this is
            // the one cell in this class whose 9.4.3 baseline differs from the asserted value.
            execute("UPDATE upd SET d = i + j");
            assertQuery("SELECT d FROM upd").noLeakCheck().expectSize().returns("d\n1970-02-16T07:06:40.000Z\n");

            // a real stored INT column has only 4 bytes and must keep its INT-width read
            execute("CREATE TABLE updi (l LONG, i INT)");
            execute("INSERT INTO updi VALUES (0, -2_147_483_648), (0, 7)");
            execute("UPDATE updi SET l = i");
            assertQuery("SELECT l FROM updi").noLeakCheck().expectSize().returns("l\nnull\n7\n");
        });
    }
}
