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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

/**
 * Basic tests that compare compiled filter output with the Java implementation.
 */
public class CompiledFilterRegressionTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(CompiledFilterRegressionTest.class);
    // Rows of the a1n fixture built by testMultiElementInIntElementVsFloatKeyPinsBoundaryRows, in
    // insertion order, as CursorPrinter.println() renders "select k, i, f".
    private static final String[] A1N_ROWS = {
            "1970-01-01T00:00:00.000000Z\t16777217\t1.6777216E7\n",
            "1970-01-01T00:00:01.000000Z\t-16777217\t-1.6777216E7\n",
            "1970-01-01T00:00:02.000000Z\t20000001\t2.0E7\n",
            "1970-01-01T00:00:03.000000Z\t2147483647\t2.1474836E9\n",
            "1970-01-01T00:00:04.000000Z\t16777215\t1.6777215E7\n",
            "1970-01-01T00:00:05.000000Z\t16777216\t1.6777216E7\n",
            "1970-01-01T00:00:06.000000Z\t-16777216\t-1.6777216E7\n",
            "1970-01-01T00:00:07.000000Z\t5\t5.0\n",
            "1970-01-01T00:00:08.000000Z\t99\t5.0\n",
            "1970-01-01T00:00:09.000000Z\tnull\tnull\n",
            "1970-01-01T00:00:10.000000Z\tnull\t1.0\n",
            "1970-01-01T00:00:11.000000Z\t1\tnull\n",
    };

    // Rows of the a1o fixture built by testNarrowIntArithVsFloatColumnCoversEveryOperator, in
    // insertion order, as CursorPrinter.println() renders "select k, i".
    private static final String[] A1O_ROWS = {
            "1970-01-01T00:00:00.000000Z\t16777216\n",
            "1970-01-01T00:00:01.000000Z\t16777217\n",
            "1970-01-01T00:00:02.000000Z\t5\n",
            "1970-01-01T00:00:03.000000Z\tnull\n",
            "1970-01-01T00:00:04.000000Z\t1073741824\n",
            "1970-01-01T00:00:05.000000Z\t2000000000\n",
            "1970-01-01T00:00:06.000000Z\t0\n",
            "1970-01-01T00:00:07.000000Z\t16777217\n",
            "1970-01-01T00:00:08.000000Z\t16777217\n",
    };

    // Rows of the a1q fixture, in insertion order, as CursorPrinter.println() renders
    // "select k, b, s, f". See testNarrowIntArithMagnitudeBoundVsFloatColumnPinsBoundaryRows.
    private static final String[] A1Q_ROWS = {
            "1970-01-01T00:00:00.000000Z\t127\t32767\t3.329216E7\n",
            "1970-01-01T00:00:01.000000Z\t127\t32767\t3.352064E7\n",
            "1970-01-01T00:00:02.000000Z\t-128\t-32768\t-3.3554304E7\n",
            "1970-01-01T00:00:03.000000Z\t-128\t-32768\t-3.3521664E7\n",
            "1970-01-01T00:00:04.000000Z\t-128\t-32768\t-1.6777216E7\n",
            "1970-01-01T00:00:05.000000Z\t127\t32767\t1.6646144E7\n",
            "1970-01-01T00:00:06.000000Z\t127\t32767\t1.6776704E7\n",
            "1970-01-01T00:00:07.000000Z\t63\t32000\t32.0\n",
            "1970-01-01T00:00:08.000000Z\t1\t1\t1.0\n",
            "1970-01-01T00:00:09.000000Z\t0\t0\tnull\n",
            "1970-01-01T00:00:10.000000Z\t0\t0\t0.0\n",
            "1970-01-01T00:00:11.000000Z\t5\t5\t5.0\n",
    };

    // Rows of the a2f fixture built by createA2fTable(), in insertion order, as
    // CursorPrinter.println() renders "select k, i, f".
    private static final String[] A2F_ROWS = {
            "1970-01-01T00:00:00.000000Z\t16777216\t1.6777216E7\n",
            "1970-01-01T00:00:01.000000Z\t16777217\t3.3554432E7\n",
            "1970-01-01T00:00:02.000000Z\t5\t6.0\n",
            "1970-01-01T00:00:03.000000Z\tnull\tnull\n",
            "1970-01-01T00:00:04.000000Z\t16777217\t1.6777216E7\n",
            "1970-01-01T00:00:05.000000Z\t0\t1.0\n",
            "1970-01-01T00:00:06.000000Z\t-16777217\t-1.6777216E7\n",
            "1970-01-01T00:00:07.000000Z\tnull\t1.0\n",
            "1970-01-01T00:00:08.000000Z\t1\tnull\n",
    };

    // Rows of the a3w fixture built by createA3wTable(), in insertion order, as
    // CursorPrinter.println() renders "select k".
    private static final String[] A3W_ROWS = {
            "2024-01-01T00:00:00.000000Z\n",
            "2024-01-01T01:00:00.000000Z\n",
            "2024-01-01T02:00:00.000000Z\n",
            "2024-01-01T03:00:00.000000Z\n",
            "2024-01-01T04:00:00.000000Z\n",
            "2024-01-01T05:00:00.000000Z\n",
            "2024-01-01T06:00:00.000000Z\n",
            "2024-01-02T07:00:00.000000Z\n",
            "2024-01-02T08:00:00.000000Z\n",
            "2024-01-02T09:00:00.000000Z\n",
            "2024-01-02T10:00:00.000000Z\n",
    };

    // Rows of the a4f fixture built by createA4fTable(), in insertion order, as
    // CursorPrinter.println() renders "select k, f".
    private static final String[] A4F_ROWS = {
            "1970-01-01T00:00:00.000000Z\t6.0E9\n",
            "1970-01-01T00:00:01.000000Z\t5.0000005E9\n",
            "1970-01-01T00:00:02.000000Z\t5.0E9\n",
            "1970-01-01T00:00:03.000000Z\t4.9999995E9\n",
            "1970-01-01T00:00:04.000000Z\t-5.0E9\n",
            "1970-01-01T00:00:05.000000Z\t-4.9999995E9\n",
            "1970-01-01T00:00:06.000000Z\t-4.983223E9\n",
            "1970-01-01T00:00:07.000000Z\t8388607.5\n",
            "1970-01-01T00:00:08.000000Z\t1.5\n",
            "1970-01-01T00:00:09.000000Z\t0.75\n",
            "1970-01-01T00:00:10.000000Z\t1.25\n",
            "1970-01-01T00:00:11.000000Z\tnull\n",
            "1970-01-01T00:00:12.000000Z\t-1.0\n",
    };

    // Execution hints as getOptions() encodes them, bits 4-5. READ from the constants of the same
    // name in CompiledFilterIRSerializer rather than re-spelled here: they are private, but
    // io.questdb is an open module, so the same reflection
    // CompiledFilterIRSerializerTest#assertUnharmonisedWidthWalk already uses reaches them. This
    // replaces a hand-kept copy of those four values. Nothing compiles a copy against production,
    // so a renumbered or renamed hint leaves it stale and silently wrong; reading the constants
    // removes the copy, and with it the only thing here that CAN drift.
    private static final int EXEC_HINT_MIXED_SIZE_TYPE = serializerExecHint("EXEC_HINT_MIXED_SIZE_TYPE");
    private static final int EXEC_HINT_SCALAR = serializerExecHint("EXEC_HINT_SCALAR");
    private static final int EXEC_HINT_SINGLE_SIZE_TYPE = serializerExecHint("EXEC_HINT_SINGLE_SIZE_TYPE");
    private static final int EXEC_HINT_WIDE_LANE = serializerExecHint("EXEC_HINT_WIDE_LANE");
    // Which defect writeProbeIr() plants in the stream it writes, on top of the comparison that
    // stream spells. IR_DEFECT_NONE is the clean control.
    private static final int IR_DEFECT_I128_LHS = 4;
    private static final int IR_DEFECT_INV = 1;
    private static final int IR_DEFECT_INV_OPERAND = 5;
    private static final int IR_DEFECT_NONE = 0;
    private static final int IR_DEFECT_OUT_OF_ENUM_OPCODE = 3;
    private static final int IR_DEFECT_SHORT_CIRCUIT = 2;
    // opcodes::Inv in core/src/main/c/share/jit/common.h. The serializer's own constant
    // is package-private, so the value is repeated here.
    private static final int IR_OPCODE_INV = -1;
    // One past opcodes::Sx_I64, the highest value the opcodes enum in common.h defines. It stands
    // for the opcode a corrupted stream, or a frontend that has grown one this backend has not,
    // presents to emit_bin_op's default arm.
    private static final int IR_OPCODE_OUT_OF_ENUM = 23;
    // The eight-row LONG column writeProbeIr()'s stream runs against, plus the row ids and the
    // match count a correctly compiled filter must answer with for each comparison that stream can
    // spell. A backend that abandoned code generation and then fell into scalar_tail's
    // unconditional row store answers with EVERY row id instead - the failure a test that only
    // asks "did it compile?" cannot see.
    private static final long[] IR_PROBE_COLUMN = {-3, 0, 7, -1, 5, 0, 2, -9};
    private static final int IR_PROBE_EQ_MATCH_COUNT = 2;
    private static final String IR_PROBE_EQ_MATCHES = "1,5";
    private static final int IR_PROBE_GT_MATCH_COUNT = 3;
    private static final String IR_PROBE_GT_MATCHES = "2,4,6";
    private static final int N_SIMD = 512;
    private static final int N_SIMD_WITH_SCALAR_TAIL = N_SIMD + 3;
    private static final QueryModel queryModel = QueryModel.FACTORY.newInstance();

    private static final StringSink jitSink = new StringSink();
    // Rows the current batch-length sweep has returned across its iterations; see
    // assertJitMatchesJavaOnBatchLengths.
    private int batchSweepRows;

    @Override
    @Before
    public void setUp() {
        // JitUtil.isJitSupported() is true on x86-64 and aarch64 alike, so this gate skips the
        // suite only where the JIT compiler has no backend at all. The class RUNS on ARM64, where
        // Function::compile always selects the scalar loop.
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
        // compiler.setEnableJitNullChecks(true);
    }

    @Test
    public void testBoolean() throws Exception {
        final String query = "x where bool1 or bool2 = false";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_boolean() bool1," +
                " rnd_boolean() bool2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testBooleanExpressionNestedInComparison() throws Exception {
        // Two defects meet in this shape, both selecting wrong rows on a plain user query at the
        // default JIT setting, and both found by QueryFuzzTest's differential oracle.
        //
        // avx2::cmp_eq spells a comparison result as an all-ones lane mask - vpcmpeqb writes
        // 0x00 / 0xFF - while a BOOLEAN column reaches the backend as the raw byte QuestDB stores,
        // 0x00 / 0x01. CompiledFilterIRSerializer#serializeColumn expands a BOOLEAN column that IS
        // the whole predicate into "column = true", so the value the loop scatters row ids with is
        // always a mask; it leaves a BOOLEAN OPERAND of a comparison raw. "(false != b0) = b0"
        // therefore compared 0xFF against 0x01 and dropped every row where b0 is true.
        //
        // The scalar backend spells both as 0 / 1 and gets those shapes right, but int32_and /
        // int32_or wrote their result back into the LEFT operand's register, which ColumnValueCache
        // still hands out for the next read of that column in the same row. "(b0 AND b1) = b0"
        // compiled to "cmp edx, edx" and selected every row. avx2_loop finishes every frame with
        // scalar_tail, so that one moved the vectorized path too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE b (b0 BOOLEAN, b1 BOOLEAN, k TIMESTAMP) TIMESTAMP(k)");
            // 100 rows, deliberately not a multiple of 32. A one-byte lane runs the AVX2 loop 32
            // rows at a time, so one frame of 100 is three vector iterations over rows 0-95 plus a
            // four-row scalar tail - and the two defects sit on opposite sides of that boundary, so
            // a row count that skipped either would have hidden one of them. b0 is true on one row
            // in three and b1 on one in two, which keeps every shape below off both 0 and 100
            // unless it is a genuine tautology or contradiction.
            execute(
                    """
                            INSERT INTO b
                            SELECT (x % 3) = 0, (x % 2) = 0, timestamp_sequence(0, 1)
                            FROM long_sequence(100)"""
            );

            // A comparison nested in a comparison, both operand orders. The mask meets the raw
            // boolean as a column, as a constant, and through NOT.
            assertBooleanFilterInAllModes("(false != b0) = b0", 100);
            assertBooleanFilterInAllModes("b0 = (false != b0)", 100);
            assertBooleanFilterInAllModes("(b0 = b0) = b0", 33);
            assertBooleanFilterInAllModes("b0 = (b0 = b0)", 33);
            assertBooleanFilterInAllModes("(false != b0) = true", 33);
            assertBooleanFilterInAllModes("true = (false != b0)", 33);
            assertBooleanFilterInAllModes("(b0 = true) = b0", 100);
            assertBooleanFilterInAllModes("(b0 = b1) = b0", 50);
            assertBooleanFilterInAllModes("b0 = (b0 = b1)", 50);
            assertBooleanFilterInAllModes("(b0 = b1) != b0", 50);
            assertBooleanFilterInAllModes("(b0 = b1) = true", 49);
            assertBooleanFilterInAllModes("(NOT b0) = b1", 51);
            assertBooleanFilterInAllModes("b1 = (NOT b0)", 51);
            assertBooleanFilterInAllModes("(NOT (b0 = b1)) = b0", 50);
            // Three levels deep: the outer comparison's left operand is itself a mask-against-raw
            // comparison, so the fix has to hold for a value it produced rather than only for one
            // the serializer emitted.
            assertBooleanFilterInAllModes("((b0 = b1) = b0) = b1", 100);

            // NOT over a nested boolean comparison. mask_not is a bitwise complement, so it turns
            // a raw 0x01 into 0xFE - which every "top bit set" test then reads as true. This is the
            // hazard that rules out fixing the mismatch by making comparisons emit 0 / 1.
            assertBooleanFilterInAllModes("NOT ((false != b0) = b0)", 0);
            assertBooleanFilterInAllModes("NOT ((b0 = b1) = b0)", 50);
            assertBooleanFilterInAllModes("NOT ((b0 = b1) OR b0)", 34);
            assertBooleanFilterInAllModes("NOT ((b0 = b1) AND b0)", 84);

            // AND / OR under a comparison. A bitwise OR of 0xFF and 0x01 is 0xFF and a bitwise AND
            // of them is 0x01, so an unharmonised pair leaves a value that is neither spelling.
            assertBooleanFilterInAllModes("(b0 AND b1) = b0", 83);
            assertBooleanFilterInAllModes("(b0 OR b1) = b0", 66);
            assertBooleanFilterInAllModes("((b0 = b1) OR b0) = b1", 16);
            assertBooleanFilterInAllModes("b1 = ((b0 = b1) OR b0)", 16);
            assertBooleanFilterInAllModes("((b0 = b1) AND b0) = b1", 66);
            // The same shapes with the RAW boolean on the LEFT of the AND / OR. avx2::bin_and and
            // bin_or take their result's spelling from the harmonised LEFT operand, so a raw left
            // operand beside a mask right one is the pairing that decides which way harmonisation
            // has to run. Without it the OR of a true raw lane (0x01) and a false mask lane (0x00)
            // is 0x01 - neither spelling - and the enclosing comparison against 0x00 then reads it
            // as false, dropping every such row. The right-operand orders above never produce that
            // lane, so dropping bin_or's harmonise_booleans call alone left them all green.
            assertBooleanFilterInAllModes("(b0 OR (b0 = b1)) = b1", 16);
            assertBooleanFilterInAllModes("b1 = (b0 OR (b0 = b1))", 16);
            assertBooleanFilterInAllModes("(b0 AND (b0 = b1)) = b1", 66);
            // An AND / OR result compared against a second COMPARISON result rather than against a
            // raw column: here the unharmonised 0x01 lane meets a 0xFF one instead of a 0x00 one,
            // so it fails in the opposite direction and a different row set comes back.
            assertBooleanFilterInAllModes("((b0 = b1) OR b0) = (b0 = b0)", 66);
            assertBooleanFilterInAllModes("(b0 OR (b0 = b1)) = (b0 = b0)", 66);

            // Shapes that are already correct. An over-broad fix announces itself here: the inner
            // comparison is always false in the first, so the raw 0x00 and the mask 0x00 agree and
            // it survived the defect; the last two never mix the two spellings at all.
            assertBooleanFilterInAllModes("(b0 != b0) = b0", 67);
            assertBooleanFilterInAllModes("b0", 33);
            assertBooleanFilterInAllModes("NOT b0", 67);
            assertBooleanFilterInAllModes("b0 = b1", 49);
            assertBooleanFilterInAllModes("b0 != b1", 51);
            assertBooleanFilterInAllModes("b0 AND b1", 16);
            assertBooleanFilterInAllModes("b0 OR b1", 67);
            assertBooleanFilterInAllModes("(b0 = b1) = (b0 = b1)", 100);
            assertBooleanFilterInAllModes("(b0 = b1) = (b0 != b1)", 0);
        });
    }

    @Test
    public void testBooleanOperators() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNot().withAnyOf("i8 / 2 = 4")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i16 < 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i32 > 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("i64 <= 0")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("f32 <= 0.34")
                .withBooleanOperator()
                .withOptionalNot().withAnyOf("f64 > 7.5");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testChar() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_char() ch" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Equality still compiles: the IR compares the raw 16-bit lane, which
            // matches EqCharCharFunctionFactory.
            assertQueryNotNullNoLeakCheck("x where ch = 'A' or ch = 'Z'");
            assertJitMatchesJava("x where ch > 'A' and ch < 'Z'", true);
        });
    }

    @Test
    public void testCharOrderingNestedInPredicateUsesCompiledFilter() throws Exception {
        // https://github.com/questdb/questdb/issues/7549
        // A CHAR ordering comparison anywhere but at the predicate ROOT rewound the IR stream over
        // bytes its siblings had already emitted, and nothing re-emitted them. The enclosing
        // operator then reached the backend one operand short, which avx2::emit_bin_op answers with
        // an out-of-bounds pop rather than a JIT decline - so these shapes aborted the JVM instead
        // of returning rows. CompiledFilterIRSerializerTest#testCharOrderingNestedInPredicate pins
        // the stream itself; this pins the rows both engines select from it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR, c2 CHAR, k TIMESTAMP) TIMESTAMP(k)");
            // 35 rows. A CHAR lane is two bytes, so Function::compile sets step = 256 / (2 * 8) =
            // 16 and avx2_loop iterates while input_index < rows_size - step + 1, i.e. while it is
            // below 20 here: two full vector iterations over rows 0-31 and then a three-row scalar
            // tail. The fixture used to hold 12 rows, which is below one vector width - stop came
            // out negative, the loop jumped straight to the exit and scalar_tail handled every row,
            // so the vectorized body this test claims to cover never executed once. The twelve
            // original rows are kept first, inside the first vector iteration, and the rows added
            // after them repeat the same value families - CHAR NULL on either or both sides, the
            // 250 / 32_767 / 32_768 / 65_535 code points that straddle the signed-short boundary,
            // equal pairs and both operand orderings - so the tail is not the only place a match
            // can come from.
            execute(
                    """
                            INSERT INTO x VALUES
                            ('a', 'b', 0),
                            (null, 'b', 1),
                            ('a', null, 2),
                            (null, null, 3),
                            (250::CHAR, 250::CHAR, 4),
                            (250::CHAR, 'b', 5),
                            ('b', 250::CHAR, 6),
                            (32_767::CHAR, 32_768::CHAR, 7),
                            (32_768::CHAR, 32_767::CHAR, 8),
                            (65_535::CHAR, 'b', 9),
                            ('b', 'a', 10),
                            ('q', 'q', 11),
                            ('a', 'a', 12),
                            ('z', 'a', 13),
                            ('a', 'z', 14),
                            (null, 'a', 15),
                            ('z', null, 16),
                            (250::CHAR, 'a', 17),
                            ('a', 250::CHAR, 18),
                            (32_767::CHAR, 'a', 19),
                            ('a', 32_767::CHAR, 20),
                            (32_768::CHAR, 'a', 21),
                            ('a', 32_768::CHAR, 22),
                            (65_535::CHAR, 65_535::CHAR, 23),
                            (65_535::CHAR, 32_768::CHAR, 24),
                            (32_768::CHAR, 65_535::CHAR, 25),
                            (250::CHAR, 32_768::CHAR, 26),
                            (32_768::CHAR, 250::CHAR, 27),
                            (null, 32_768::CHAR, 28),
                            (32_768::CHAR, null, 29),
                            ('q', 'a', 30),
                            ('a', 'q', 31),
                            (32_767::CHAR, 32_768::CHAR, 32),
                            (null, null, 33),
                            ('b', 'b', 34)
                            """
            );

            // (cmp) = (cmp) forms a SINGLE predicate context, so both ordering expansions share one
            // IR stream - the shape the rewind used to truncate. Both operand orders: the traversal
            // descends rhs first, so only an lhs-side ordering node rewound over emitted siblings.
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) = (c2 < c)");
            assertJitMatchesJavaInAllModes("x WHERE (c2 < c) = (c < c2)");
            assertJitMatchesJavaInAllModes("x WHERE (c <= c2) = (c2 <= c)");
            assertJitMatchesJavaInAllModes("x WHERE (c > c2) = (c2 > c)");
            assertJitMatchesJavaInAllModes("x WHERE (c >= c2) = (c2 >= c)");
            assertJitMatchesJavaInAllModes("x WHERE (c < 'b') = (c2 < 'b')");
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) <> (c2 < c)");
            // The ordering node on the RHS is traversed first, so this shape survived the bug. It
            // has to stay CORRECT, not merely non-crashing.
            assertJitMatchesJavaInAllModes("x WHERE (c = c2) = (c < c2)");
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) = (c = c2)");
            // NOT keeps the ordering node one level below the predicate root; AND / OR give each
            // conjunct its own predicate context.
            assertJitMatchesJavaInAllModes("x WHERE NOT (c < c2)");
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) AND (c2 > c)");
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) OR (c2 < c)");
            // An ordering node nested under a longer chain.
            assertJitMatchesJavaInAllModes("x WHERE ((c < c2) = (c2 < c)) AND ((c >= c2) OR (c <= c2))");
            assertJitMatchesJavaInAllModes("x WHERE NOT ((c < c2) = (c2 < c))");
            // A sibling CONSTANT emits a deferred stub that only the backfill pass fills in, so the
            // memory rewind and the backfill map have to move together. Pruning the map by offset
            // keeps the sibling's entry, which the expansion's blanket clear() dropped.
            assertJitMatchesJavaInAllModes("x WHERE (c < c2) = (c = 'a')");
            assertJitMatchesJavaInAllModes("x WHERE (c = 'a') = (c < c2)");
            // The bind variable list is rewound beside the IR, so an expansion re-traversing an
            // operand several times must not renumber a sibling's variables.
            bindVariableService.setChar("mid", 'b');
            assertJitMatchesJavaInAllModes("x WHERE (c < :mid) = (c2 < :mid)");
            assertJitMatchesJavaInAllModes("x WHERE (c < :mid) = (c2 = :mid)");

            // A boolean CONSTANT beside the ordering node is stubbed before the expansion runs, and
            // backfilling it against a CHAR predicate type declines JIT - the disposition this
            // shape had before the CHAR ordering expansion existed. The blanket
            // backfillNodes.clear() the expansion used to run threw that stub away with the rest.
            assertJitMatchesJava("x WHERE (c < c2) = true", false);
            assertJitMatchesJava("x WHERE (c < c2) = false", false);
            assertJitMatchesJava("x WHERE true = (c < c2)", false);
            assertJitMatchesJava("x WHERE false = (c < c2)", false);
        });
    }

    @Test
    public void testCharOrderingUsesCompiledFilter() throws Exception {
        // https://github.com/questdb/questdb/issues/7549
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR, c2 CHAR, k TIMESTAMP) TIMESTAMP(k)");
            execute(
                    """
                            INSERT INTO x VALUES
                            ('a', 'b', 0),
                            (null, 'b', 1),
                            ('a', null, 2),
                            (null, null, 3),
                            (32_767::CHAR, 32_768::CHAR, 4),
                            (32_768::CHAR, 32_767::CHAR, 5),
                            (65_535::CHAR, 'b', 6),
                            ('A', 'A', 7),
                            ('z', 'A', 8),
                            (1::CHAR, 2::CHAR, 9),
                            ('b', 'a', 10),
                            ('A', 'z', 11),
                            (2::CHAR, 1::CHAR, 12),
                            ('m', 'n', 13),
                            ('n', 'm', 14),
                            ('q', 'q', 15),
                            (null, 'b', 16),
                            ('a', null, 17),
                            (null, null, 18),
                            (32_767::CHAR, 32_768::CHAR, 19),
                            (32_768::CHAR, 32_767::CHAR, 20),
                            (65_535::CHAR, 'b', 21)
                            """
            );

            assertJitMatchesJavaInAllModes("x WHERE c > c2");
            assertJitMatchesJavaInAllModes("x WHERE c >= c2");
            assertJitMatchesJavaInAllModes("x WHERE c < c2");
            assertJitMatchesJavaInAllModes("x WHERE c <= c2");
            assertJitMatchesJavaInAllModes("x WHERE c > 'A'");
            assertJitMatchesJavaInAllModes("x WHERE c >= 'A'");
            assertJitMatchesJavaInAllModes("x WHERE c < 'A'");
            assertJitMatchesJavaInAllModes("x WHERE c <= 'A'");

            assertJitMatchesJavaInAllModes("x WHERE c > '\u8000'");
            assertJitMatchesJavaInAllModes("x WHERE c >= '\u8000'");
            assertJitMatchesJavaInAllModes("x WHERE c < '\u8000'");
            assertJitMatchesJavaInAllModes("x WHERE c <= '\u8000'");
            assertJitMatchesJavaInAllModes("x WHERE '\u8000' > c");
            assertJitMatchesJavaInAllModes("x WHERE '\u8000' >= c");
            assertJitMatchesJavaInAllModes("x WHERE '\u8000' < c");
            assertJitMatchesJavaInAllModes("x WHERE '\u8000' <= c");

            assertJitMatchesJavaInAllModes("x WHERE c > '\uffff'");
            assertJitMatchesJavaInAllModes("x WHERE c >= '\uffff'");
            assertJitMatchesJavaInAllModes("x WHERE c < '\uffff'");
            assertJitMatchesJavaInAllModes("x WHERE c <= '\uffff'");
            assertJitMatchesJavaInAllModes("x WHERE '\uffff' > c");
            assertJitMatchesJavaInAllModes("x WHERE '\uffff' >= c");
            assertJitMatchesJavaInAllModes("x WHERE '\uffff' < c");
            assertJitMatchesJavaInAllModes("x WHERE '\uffff' <= c");

            bindVariableService.setChar("highChar", '\uffff');
            assertJitMatchesJavaInAllModes("x WHERE c < :highChar");

            // Equality compares the raw lane and still matches Java, so it keeps compiling.
            assertJitMatchesJava("x WHERE c = 'a'", true);
            assertJitMatchesJava("x WHERE c != 'a'", true);
            // Unrelated pre-existing fallback: serializeNull() has no CHAR arm,
            // the 16-bit lane is only nullable for geohashes.
            assertJitMatchesJava("x WHERE c = null", false);
        });
    }

    @Test
    public void testColumnAddressHoistingExceedsCacheCapacity() throws Exception {
        // Tests column address hoisting with more than 8 columns (exceeds cache capacity)
        // The backend address cache has capacity of 8 elements
        final String query = "x where " +
                "c1 > 0 and c2 > 0 and c3 > 0 and c4 > 0 " +
                "and c5 > 0 and c6 > 0 and c7 > 0 and c8 > 0 " +
                "and c9 > 0 and c10 > 0 and c11 > 0 and c12 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long() c1," +
                " rnd_long() c2," +
                " rnd_long() c3," +
                " rnd_long() c4," +
                " rnd_long() c5," +
                " rnd_long() c6," +
                " rnd_long() c7," +
                " rnd_long() c8," +
                " rnd_long() c9," +
                " rnd_long() c10," +
                " rnd_long() c11," +
                " rnd_long() c12" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnAddressHoistingMixedTypes() throws Exception {
        // Tests column address hoisting with mixed types exceeding cache capacity
        final String query = "x where " +
                "i1 > 0 and i2 > 0 and i3 > 0 and l1 > 0 and l2 > 0 and l3 > 0 " +
                "and f1 > 0.0 and f2 > 0.0 and f3 > 0.0 " +
                "and d1 > 0.0 and d2 > 0.0 and d3 > 0.0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int() i1," +
                " rnd_int() i2," +
                " rnd_int() i3," +
                " rnd_long() l1," +
                " rnd_long() l2," +
                " rnd_long() l3," +
                " rnd_float() f1," +
                " rnd_float() f2," +
                " rnd_float() f3," +
                " rnd_double() d1," +
                " rnd_double() d2," +
                " rnd_double() d3" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnAddressHoistingWithinCacheCapacity() throws Exception {
        // Tests column address hoisting with 8 columns (within cache capacity of 8)
        final String query = "x where " +
                "c1 > 0 and c2 > 0 and c3 > 0 and c4 > 0 " +
                "and c5 > 0 and c6 > 0 and c7 > 0 and c8 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long() c1," +
                " rnd_long() c2," +
                " rnd_long() c3," +
                " rnd_long() c4," +
                " rnd_long() c5," +
                " rnd_long() c6," +
                " rnd_long() c7," +
                " rnd_long() c8" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnArithmetics() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withAnyOf(" = 1");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnArithmeticsNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnArithmeticWidthUnderBooleanEquality() throws Exception {
        // The column analog of testConstantFoldWidthUnderBooleanEquality. A boolean
        // equality of two comparisons - (cmp) = (cmp) - forms a SINGLE predicate
        // context, so a sibling LONG comparison turns the predicate-global narrow-i64
        // widening on for the whole predicate. An overflowing narrow-int arithmetic
        // COLUMN product on the wrap-side of the INT-width comparison was then
        // sign-extended and computed at 64 bits, diverging from the Java filter's
        // MulInt#getInt (which wraps mod 2^32): for ((a*b) = -727_379_968) = (nl > 0)
        // the Java filter returned {1,3} and the JIT {2,4,5}. The narrow-arith leaf
        // widening is now derived per-comparison (i64WrapLeaves), so both agree.
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(x as long) rid," +
                    " cast(case x when 1 then 1_000_000_000_000 when 3 then 5 else 0 end as long) nl," +
                    " x::short cs, x::byte cbyte, timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Previously diverging shapes: an INT-width comparison of a narrow-int
            // product, ANDed as a boolean equality with a LONG comparison.
            // Absolute pin: only rows 1 and 3 (nl > 0) survive; the pre-fix JIT returned {2,4,5}.
            assertJitMatchesJava("p where ((a*b) = -727_379_968) = (nl > 0)", true,
                    "a\tb\trid\tnl\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1\t1000000000000\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t3\t5\t3\t3\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("p where ((a*b) > 0) = (nl > 0)", true);           // no magic constant
            assertJitMatchesJava("p where (nl > 0) = ((a*b) = -727_379_968)", true);  // operand order
            assertJitMatchesJava("p where ((a*b) <> -727_379_968) = (nl > 0)", true);
            // The plain narrow column read on the LONG-comparison side sign-extends
            // value-preservingly; only the narrow product on the INT side wraps.
            assertJitMatchesJava("p where (cs = nl) = (a*b > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where (cbyte = nl) = (a*b = -727_379_968)", true);
            // Controls: a LONG-width comparison inside the boolean equality still
            // widens the product on both paths.
            assertJitMatchesJava("p where (a*b > nl) = (nl > 0)", true);
            // Controls: single comparison, AND, OR each form separate predicate
            // contexts and were always correct.
            assertJitMatchesJava("p where (a*b) = -727_379_968", true);
            assertJitMatchesJava("p where (a*b) = -727_379_968 and nl > 0", true);
            assertJitMatchesJava("p where (a*b) = -727_379_968 or nl > 0", true);
            // Control: INT arith directly compared to a LONG column widens.
            assertJitMatchesJava("p where a*b >= -432_577_000_000L", true);
        });
    }

    @Test
    public void testColumnArithmeticWidthUnderBooleanEqualityWithFloat() throws Exception {
        // A FLOAT anywhere in the predicate suppresses the predicate-global narrow-i64
        // widening (NarrowI64WidenDetector.shouldWiden() returns false when hasFloat),
        // so markFloatI64WidenLeaves is the ONLY pass that can sign-extend a narrow-int
        // arithmetic subtree read at long width. It walked each comparison operand with
        // its OWN static type, so a narrow-int COLUMN product compared against a LONG
        // sibling - ((a*b) > nl) - was left at INT width and wrapped mod 2^32 via the
        // JIT's int32 MUL, while the Java filter reads MulInt#getLong (no wrap). For
        // ((a*b) > nl) = (f32 > 0) the JIT returned 0 rows and the Java filter 4. The
        // boundary now promotes the comparison width across all operands (foldCmpType),
        // matching markI64WrapArithLeaves / markI64WidenFoldRoots, so both agree. The
        // widened leaf emits SX_I64 (no AVX2 path), which forces scalar mode - the query
        // still JIT-compiles (usesCompiledFilter stays true), it just runs scalar.
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(case x when 1 then 1_000_000_000_000 else 0 end as long) nl," +
                    " cast(1.0 as float) f32, x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro: a narrow-int product against a LONG column, in a boolean equality
            // with a float sibling. a*b wraps to -727_379_968, so it is never above nl (0 or
            // 10^12) and the left comparison is false on every row while (f32 > 0) is true - the
            // equality is false throughout. Both engines have to say so.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) > nl) = (f32 > 0)", true);
            // The complementary spelling bites: the wrapped product IS below nl, so the equality
            // holds on every row and an over-widened product would drop them all.
            assertJitMatchesJava("p where ((a*b) < nl) = (f32 > 0)", true,
                    "a\tb\tnl\tf32\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1000000000000\t1.0\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t2\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t4\t4\t1970-01-01T00:00:03.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t5\t5\t1970-01-01T00:00:04.000000Z\n");
            // Operand order and a non-zero float threshold agree with it.
            assertJitMatchesJava("p where (f32 > 0) = ((a*b) < nl)", true);
            assertJitMatchesJava("p where ((a*b) < nl) = (f32 > 0.5)", true);
            // Narrow column read (not a product) vs a LONG column sign-extends
            // value-preservingly; the float sibling does not change that.
            assertJitMatchesJava("p where (cs > nl) = (f32 > 0)", true);
            assertJitMatchesJava("p where (cbyte > nl) = (f32 > 0)", true);
            // Over-widening guard: an INT-width comparison of the narrow product must
            // still WRAP even with a float present - cmpType stays I4 there, so the
            // product folds to exactly -727_379_968 and matches every row. Absolute pin:
            // all 5 rows survive ((a*b) = -727_379_968 is true, f32 > 0 is true).
            assertJitMatchesJava("p where ((a*b) = -727_379_968) = (f32 > 0)", true,
                    "a\tb\tnl\tf32\tcs\tcbyte\tk\n" +
                            "1000000\t1000000\t1000000000000\t1.0\t1\t1\t1970-01-01T00:00:00.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t2\t2\t1970-01-01T00:00:01.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t4\t4\t1970-01-01T00:00:03.000000Z\n" +
                            "1000000\t1000000\t0\t1.0\t5\t5\t1970-01-01T00:00:04.000000Z\n");
            // A pure FLOAT comparison of the product wraps (getDouble -> getInt), so the
            // OPERANDS do not widen: cmpType is floating, cmpLong stays false. The wrapped
            // RESULT does - markIntCmpFloatOperand emits SX_I64 after the multiply. What this
            // assertion observes is the WRAP alone, not the f64 the SX_I64 buys: -727_379_968
            // is 177_583 * 2^12, so it is exactly representable as a float, and rounding it
            // through convert()'s cvt_itof / int32_to_float returns the same value - an f32
            // comparison answers here exactly as an f64 one does. The f32-vs-f64 rounding
            // itself is pinned by testNarrowIntArithMagnitudeBoundVsFloatColumnPinsBoundaryRows,
            // whose odd products above 2^24 have no exact float.
            assertJitMatchesJavaOnEmptyResult("p where (a*b) > f32", true);
            // Controls: AND/OR split into separate predicate contexts. The wrapped product is
            // never above nl, so the AND is empty and the OR reduces to the float conjunct.
            assertJitMatchesJavaOnEmptyResult("p where (a*b > nl) and f32 > 0", true);
            assertJitMatchesJava("p where (a*b < nl) and f32 > 0", true);
            assertJitMatchesJava("p where (a*b > nl) or f32 > 0", true);
        });
    }

    @Test
    public void testColumnFloatComparisonWithNulls() throws Exception {
        // Regression test for ARM64 NaN condition code handling.
        // ARM64 fcmp with NaN sets NZCV=0011. Ordered less-than must use MI (not LT),
        // and ordered less-or-equal must use LS (not LE), otherwise NaN compares as true.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.5", "-0.5")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.3");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnFloatConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("-42.5", "0.0", "0.000", "42.5");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnFloatEpsilonWithNulls() throws Exception {
        // Regression test for ARM64 epsilon comparison NaN handling.
        // Float equality uses epsilon comparison internally. ARM64 must use
        // GT (ordered, false for NaN) not HI (unsigned, true for NaN).
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " != ", " <> ")
                .withAnyOf("0.5", "-0.5", "0.0")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " != ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnFloatNegativeConstant() throws Exception {
        // Regression test for is_float() misclassifying negative floats.
        // Previously is_float() used numeric_limits<float>::min() (smallest positive
        // normal) as lower bound, causing all negative floats to be promoted to f64.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32")
                .withComparisonOperator()
                .withAnyOf("-0.5", "-1.0", "-100.0", "-3.4028235E38");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnFloatSharedConstant() throws Exception {
        // Regression test for constant cache type mismatch on ARM64.
        // When f32 and f64 columns compare against the same constant value (e.g. 0.0),
        // the ConstantCache shares an entry. The cached Vec register may have wrong
        // element size (S vs D), requiring fcvt conversion.
        final String query = "x where f32 > 0.0 and f64 > 0.0 and f32 < 0.5 and f64 < 0.5";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnFreeComparisonDeclinesCompiledFilter() throws Exception {
        // A comparison whose operands reach NO column took its constants' types from
        // predicateContext.columnType - the type of the columns the WHOLE predicate carries -
        // while the Java filter types that same comparison from its own operands, i.e. from the
        // literals themselves. The two disagree, and the compiled filter answered with the
        // COMPLEMENT of the Java row set:
        //   ('60.108.156.35' >= '249.79.14.15') = (ip > '128.0.0.0')
        // orders two STRINGs in the Java filter ('6' > '2', so true) and two IPv4 addresses in the
        // JIT (0x3C6C9C23 >= 0xF94F0E0F, so false). Both JIT backends were wrong the same way, so
        // scalar-versus-vector parity does not catch it; only the Java oracle does.
        //
        // The mis-typing is not specific to IPv4, nor to ordering. SYMBOL reaches it through plain
        // equality (two constants absent from the table both resolve to VALUE_NOT_FOUND, so the
        // JIT calls them equal), UUID through a hex literal differing only in case, TIMESTAMP and
        // DATE through two spellings of one instant, IN through its element pairing, and a STRING
        // bind variable reaches it exactly as a constant does - serializeBindVariable defers it and
        // resolves it against the predicate's symbol column. CHAR is the one type whose constants
        // happen to order the same either way, so it returned the RIGHT rows - on a coincidence
        // rather than on a decision, so it declines here too.
        //
        // The fixture holds 35 rows so that every CONTROL shape below - the column-carrying
        // comparisons that have to keep compiling - runs full AVX2 vector iterations AND leaves a
        // non-empty scalar tail. Function::compile picks step = 256 / (lane_bytes * 8) and
        // avx2_loop runs while input_index < rows - step + 1, so the widest lane here decides the
        // floor: CHAR is i16, step 16, which gives two whole iterations over rows 0-31 and a
        // three-row tail. Every other lane in the fixture has a SMALLER step and therefore more
        // iterations - 8 for IPv4 and SYMBOL (i32), 4 for TIMESTAMP and DATE (i64), 2 for UUID
        // (i128) - and 35 is odd, so none of those divides it either and the tail is never empty.
        // A 12-row fixture would put stop at -3 and run zero iterations, which is how the CHAR
        // sibling of this test used to cover the vectorized body without ever entering it.
        // Every expected count below is a proper non-empty subset of the 35 rows.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE x (ip IPv4, ip2 IPv4, c CHAR, c2 CHAR, s SYMBOL, u UUID," +
                            " t2 TIMESTAMP, d DATE, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY"
            );
            execute(
                    """
                            INSERT INTO x SELECT
                                (CASE WHEN x % 2 = 0 THEN '10.0.0.1' ELSE '200.0.0.1' END)::IPv4,
                                (CASE WHEN x % 3 = 0 THEN '255.255.255.255' ELSE '1.2.3.4' END)::IPv4,
                                (CASE WHEN x % 3 = 0 THEN 'a' ELSE 'z' END)::CHAR,
                                (CASE WHEN x % 4 = 0 THEN 'b' ELSE 'y' END)::CHAR,
                                (CASE WHEN x % 5 = 0 THEN 'aa' ELSE 'bb' END)::SYMBOL,
                                (CASE WHEN x % 4 = 0 THEN '11111111-1111-1111-1111-11111111111a'
                                      ELSE '22222222-2222-2222-2222-222222222222' END)::UUID,
                                (CASE WHEN x % 2 = 0 THEN '2020-01-01T00:00:00.000000Z'
                                      ELSE '2021-01-01T00:00:00.000000Z' END)::TIMESTAMP,
                                (CASE WHEN x % 2 = 0 THEN '2020-01-01T00:00:00.000Z'
                                      ELSE '2021-01-01T00:00:00.000Z' END)::DATE,
                                timestamp_sequence(0, 1_000_000)
                            FROM long_sequence(35)
                            """
            );
            bindVariableService.setStr("sv", "zz");
            // 2020-06-01T00:00:00.000000Z and 2020-01-01T00:00:00.000000Z, for the guard conjuncts
            // among the controls below.
            bindVariableService.setTimestamp("tv", 1_590_969_600_000_000L);
            bindVariableService.setTimestamp("tv2", 1_577_836_800_000_000L);
            bindVariableService.setBoolean("flag", true);

            // IPv4, ordering between two quoted literals - the shape the fuzzer found. Every
            // operator, and both operand orders, so a fix that only moved '>=' cannot pass.
            assertColumnFreeComparisonDeclines("('60.108.156.35' >= '249.79.14.15') = (ip > '128.0.0.0')", 18);
            assertColumnFreeComparisonDeclines("('249.79.14.15' >= '60.108.156.35') = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("('60.108.156.35' > '249.79.14.15') = (ip > '128.0.0.0')", 18);
            assertColumnFreeComparisonDeclines("('60.108.156.35' < '249.79.14.15') = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("('60.108.156.35' <= '249.79.14.15') = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("NOT (('60.108.156.35' >= '249.79.14.15') = (ip > '128.0.0.0'))", 17);
            // IPv4, EQUALITY between two quoted literals: '1.1.1.1' and '01.01.01.01' are two
            // spellings of one address and two different strings. This half of the defect needs no
            // ordering operator at all.
            assertColumnFreeComparisonDeclines("('1.1.1.1' = '01.01.01.01') = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("('1.1.1.1' != '01.01.01.01') = (ip > '128.0.0.0')", 18);
            assertColumnFreeComparisonDeclines("('1.1.1.1' IN ('01.01.01.01')) = (ip > '128.0.0.0')", 17);
            // IN pairs its element list against the left operand one element at a time, so the
            // mis-typing rides on EVERY element, not only on a single-element list, and NOT IN
            // reaches it through the same pairing under a NOT. Both flip the whole predicate: with
            // the verdict removed the compiled filter reads these as IPv4 addresses, finds
            // '1.1.1.1' in the list, and answers 18 rows where the Java filter - comparing STRINGS,
            // which '01.01.01.01' is not one of - answers 17, and the complement again for NOT IN.
            assertColumnFreeComparisonDeclines("('1.1.1.1' IN ('9.9.9.9', '01.01.01.01')) = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("('1.1.1.1' NOT IN ('01.01.01.01')) = (ip > '128.0.0.0')", 18);
            assertColumnFreeComparisonDeclines("('1.1.1.1' NOT IN ('9.9.9.9', '01.01.01.01')) = (ip > '128.0.0.0')", 18);
            // IS [NOT] NULL against a constant parses to a comparison against null, so it lands in
            // the same place: the JIT reads 'null' as the IPv4 null sentinel, the Java filter reads
            // it as a four-character string.
            assertColumnFreeComparisonDeclines("('null' IS NULL) = (ip > '128.0.0.0')", 17);
            assertColumnFreeComparisonDeclines("('null' IS NOT NULL) = (ip > '128.0.0.0')", 18);
            // SYMBOL: neither 'zz' nor 'yy' is in the column's symbol table, so the JIT resolved
            // both to VALUE_NOT_FOUND and called them EQUAL.
            assertColumnFreeComparisonDeclines("('zz' = 'yy') = (s = 'aa')", 28);
            assertColumnFreeComparisonDeclines("('zz' != 'yy') = (s = 'aa')", 7);
            // The SYMBOL route into IN / NOT IN, where the divergence is widest: the JIT resolved
            // 'zz', 'yy' and 'xx' alike to VALUE_NOT_FOUND, so it found 'zz' in the list and
            // selected the 7 rows the Java filter leaves out - and the 28 it selects, for NOT IN.
            assertColumnFreeComparisonDeclines("('zz' IN ('yy', 'xx')) = (s = 'aa')", 28);
            assertColumnFreeComparisonDeclines("('zz' NOT IN ('yy', 'xx')) = (s = 'aa')", 7);
            // The same, with a STRING bind variable in place of the left constant.
            assertColumnFreeComparisonDeclines("(:sv = 'yy') = (s = 'aa')", 28);
            // A bind variable types the comparison the predicate holds beside it, which is what
            // separates the guard conjuncts among the controls below from this shape: the
            // right-hand pair is two STRINGs to the Java filter and two TIMESTAMPs here, and
            // '2020-01-02T00:00:00-05:00' sorts above '2020-01-02T00:00:00Z' as a string and below
            // it as an instant. Neither comparison reads a column, so the count of comparisons is
            // the only thing that tells the two apart.
            assertColumnFreeComparisonDeclines(
                    "((:tv2 > '2019-01-01') = ('2020-01-02T00:00:00-05:00' >= '2020-01-02T00:00:00Z'))"
                            + " != (t2 > '2020-06-01')",
                    18
            );
            // UUID: one uuid, two hex spellings.
            assertColumnFreeComparisonDeclines(
                    "('11111111-1111-1111-1111-11111111111a' = '11111111-1111-1111-1111-11111111111A')"
                            + " = (u = '11111111-1111-1111-1111-11111111111a')",
                    27
            );
            // UUID ORDERING between two literals ABORTED THE JVM. rejectOrderingComparison names
            // UUID, but it reads predicateContext.columnType at VISIT time, and
            // PostOrderTreeTraversalAlgo descends node.rhs first: with the constants on the right
            // the ordering node is visited before any column has typed anything, columnType is
            // still UNDEFINED, the rejection does not fire, and the backfill then emits two 128-bit
            // UUID immediates. The i128 lane has no ordering comparator - it falls through the
            // dispatch's default: __builtin_unreachable() - and the native compiler died inside
            // asmjit::BaseBuilder::_emit with SIGSEGV. The verdict here is taken when the predicate
            // is LEFT, where columnType is settled, so it fires in both operand orders.
            assertColumnFreeComparisonDeclines(
                    "(u = '11111111-1111-1111-1111-11111111111a')"
                            + " != ('05762904-b77a-6040-3d03-4223b4730027' > '889cf1b5-6879-a782-b394-7adab6e457b6')",
                    8
            );
            assertColumnFreeComparisonDeclines(
                    "(u = '11111111-1111-1111-1111-11111111111a')"
                            + " = ('05762904-b77a-6040-3d03-4223b4730027' > '889cf1b5-6879-a782-b394-7adab6e457b6')",
                    27
            );
            assertColumnFreeComparisonDeclines(
                    "('05762904-b77a-6040-3d03-4223b4730027' > '889cf1b5-6879-a782-b394-7adab6e457b6')"
                            + " = (u = '11111111-1111-1111-1111-11111111111a')",
                    27
            );
            assertColumnFreeComparisonDeclines(
                    "(u = '11111111-1111-1111-1111-11111111111a')"
                            + " != ('05762904-b77a-6040-3d03-4223b4730027' < '889cf1b5-6879-a782-b394-7adab6e457b6')",
                    27
            );
            // The IPv4 twin of that operand order: the constant comparison on the RIGHT is visited
            // before the column on the left, so it is serialized while columnType is UNDEFINED and
            // takes the plain GE opcode rather than serializeIPv4Ordering's expansion. Same
            // mis-typing, different route into it.
            assertColumnFreeComparisonDeclines("(ip > '128.0.0.0') = ('60.108.156.35' >= '249.79.14.15')", 18);
            // TIMESTAMP and DATE: one instant, two spellings; and a zone offset that reverses the
            // string order of two instants.
            assertColumnFreeComparisonDeclines("('2020-01-02T00:00:00.000000Z' = '2020-01-02') = (t2 > '2020-06-01')", 17);
            assertColumnFreeComparisonDeclines("('2020-01-02T00:00:00-05:00' >= '2020-01-02T00:00:00Z') = (t2 > '2020-06-01')", 17);
            assertColumnFreeComparisonDeclines("('2020-01-02T00:00:00.000Z' = '2020-01-02') = (d > '2020-06-01')", 17);
            // CHAR: these select the RIGHT rows either way, because a one-character literal orders
            // the same as a CHAR. They decline all the same - the agreement is a property of the
            // values, not of the typing, and nothing keeps it true for the next type added.
            assertColumnFreeComparisonDeclines("('a' < 'b') = (c = 'a')", 11);
            assertColumnFreeComparisonDeclines("('b' < 'a') = (c = 'a')", 24);
            assertColumnFreeComparisonDeclines("('a' = 'b') = (c = 'a')", 24);

            // CONTROLS. A comparison that reads a column is typed by that column in both engines,
            // so `column op quoted-literal` - the shape the IPv4 and CHAR literal support exists
            // for - keeps compiling, nested in another comparison as well as at the predicate root.
            assertColumnComparisonCompiles("ip >= '128.0.0.0'", 18);
            assertColumnComparisonCompiles("ip < '128.0.0.0'", 17);
            assertColumnComparisonCompiles("'128.0.0.0' > ip", 17);
            assertColumnComparisonCompiles("(ip > '128.0.0.0') = (ip2 = '255.255.255.255')", 18);
            assertColumnComparisonCompiles("ip IN ('200.0.0.1')", 18);
            // The verdict turns on the OPERANDS, not on the operator, so a multi-element IN and a
            // NOT IN that read a column keep compiling on both engines.
            assertColumnComparisonCompiles("ip NOT IN ('200.0.0.1')", 17);
            assertColumnComparisonCompiles("ip2 IN ('255.255.255.255', '9.9.9.9')", 11);
            assertColumnComparisonCompiles("s NOT IN ('aa')", 28);
            assertColumnComparisonCompiles("c < 'b'", 11);
            assertColumnComparisonCompiles("(c < 'b') = (c2 < 'y')", 20);
            assertColumnComparisonCompiles("s = 'aa'", 7);
            assertColumnComparisonCompiles("u = '11111111-1111-1111-1111-11111111111a'", 8);
            assertColumnComparisonCompiles("t2 >= '2020-06-01'", 18);
            assertColumnComparisonCompiles("d >= '2020-06-01'", 18);

            // The parameter-only GUARD CONJUNCT - the shape a generated query appends beside the
            // filter that does the work. It reads no column, so the verdict used to decline it, and
            // AND is not a top-level operation: the guard is its own predicate, sitting beside the
            // conjuncts that carry the columns, and visit() throws out of serialize(), so the
            // WHOLE filter fell back to the Java one. The guard holds ONE comparison and types it
            // from its own bind variable, which is where the Java filter types it from too, so
            // there is nothing to diverge and it compiles again. Each assertion below pairs the
            // guard with a non-numeric column conjunct, because a numeric one would have been
            // exempt through isNumeric() either way.
            assertColumnComparisonCompiles(":flag = true AND s = 'aa'", 7);
            assertColumnComparisonCompiles(":tv > '2020-01-01' AND ip > '128.0.0.0'", 18);
            assertColumnComparisonCompiles(":tv > :tv2 AND c < 'b'", 11);
            assertColumnComparisonCompiles(":tv IN ('2020-06-01') AND u = '11111111-1111-1111-1111-11111111111a'", 8);
            // NOT is a top-level operation, so it becomes the predicate root and everything under
            // it belongs to that one predicate. One comparison is still one comparison.
            assertColumnComparisonCompiles("NOT (:flag = false) AND d >= '2020-06-01'", 18);
            // A guard that selects nothing takes the whole filter with it, on both engines. The
            // count pins that the guard is actually evaluated rather than folded away.
            assertJitCountQuery("SELECT count() FROM x WHERE :flag = false AND s = 'aa'", 0);
        });
    }

    @Test
    public void testColumnIntConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("-50", "0", "50");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnIntConstantComparisonBoundaryMatch() throws Exception {
        final int boundary = 42;
        Assert.assertTrue("boundary should be within the range", N_SIMD_WITH_SCALAR_TAIL > boundary);
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i8", "i16", "i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf(String.valueOf(boundary));
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnLessThanNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" <= ", " >= ", " = ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testColumnTimestampLiteralComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select rnd_timestamp(to_timestamp('2019','yyyy'),to_timestamp('2021','yyyy'),2) ts" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + "))";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("ts")
                .withComparisonOperator()
                .withAnyOf("'2020-01-01T01:01:01.111111Z'");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testColumnValueCacheExceedsCapacity() throws Exception {
        // Tests when column value cache exceeds capacity (8 elements)
        // Each column access creates a cache entry
        final String query = "x where " +
                "c1 > 0 and c1 < 100 and c2 > 0 and c2 < 100 " +
                "and c3 > 0 and c3 < 100 and c4 > 0 and c4 < 100 " +
                "and c5 > 0 and c5 < 100";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 200, 0) c1," +
                " rnd_long(0, 200, 0) c2," +
                " rnd_long(0, 200, 0) c3," +
                " rnd_long(0, 200, 0) c4," +
                " rnd_long(0, 200, 0) c5" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingInArithmetic() throws Exception {
        // Tests column value caching when column is used in arithmetic expressions
        final String query = "x where " +
                "i64 + i64 > 100 and i64 * 2 < 180";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingMixedTypes() throws Exception {
        // Tests column value caching with multiple columns of different types
        final String query = "x where " +
                "i32 > 10 and i32 < 90 and i64 > 20 and i64 < 80 " +
                "and f64 > 0.1 and f64 < 0.9";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(0, 100, 0) i32," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testColumnValueCachingSameColumnMultipleTimes() throws Exception {
        // Tests column value caching when the same column is used multiple times
        // The backend caches loaded column values within a single row iteration
        final String query = "x where " +
                "i64 > 10 and i64 < 90 and i64 != 50";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testCombinedHoistingExceedsBothCaches() throws Exception {
        // Tests both constant and column address hoisting exceeding cache capacities
        final String query = "x where " +
                "c1 > 1 and c2 > 2 and c3 > 3 and c4 > 4 " +
                "and c5 > 5 and c6 > 6 and c7 > 7 and c8 > 8 " +
                "and c9 > 9 and c10 > 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) c1," +
                " rnd_long(0, 100, 0) c2," +
                " rnd_long(0, 100, 0) c3," +
                " rnd_long(0, 100, 0) c4," +
                " rnd_long(0, 100, 0) c5," +
                " rnd_long(0, 100, 0) c6," +
                " rnd_long(0, 100, 0) c7," +
                " rnd_long(0, 100, 0) c8," +
                " rnd_long(0, 100, 0) c9," +
                " rnd_long(0, 100, 0) c10" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantArithmeticWidthUnderBooleanEqualityWithFloat() throws Exception {
        // The bare-constant analog of testColumnArithmeticWidthUnderBooleanEqualityWithFloat
        // (which used a LONG column). A FLOAT anywhere in the predicate suppresses the
        // predicate-global narrow-i64 widening, so markFloatI64WidenLeaves is the ONLY pass
        // that can sign-extend a narrow-int arithmetic subtree read at long width. For
        // ((a*b) = 4_999_999_999) = (f32 > 0) it correctly widens the product's leaves (a, b) so
        // the JIT computes a*b = 5e9 at 64 bits, but it returned at the bare CONSTANT leaf and
        // left 4_999_999_999 unwidened. The type observer sees only INT and FLOAT columns (both
        // 4 bytes, so hasMixedSizes() is false), types the constant down to F4, and
        // serializeNumber rounds it to the nearest float - 4_999_999_999 -> 5.0e9f (floats near
        // 2^32 are 512 apart) - so the JIT float-compared 5e9 == 5.0e9f and matched, while the
        // Java filter reads MulInt#getLong vs the LONG literal (5e9 == 4_999_999_999, no match).
        // JIT returned every row, Java none. markI64Widen now widens a bare out-of-INT-range
        // integer constant compared against a narrow-int arithmetic operand, so
        // serializeConstant emits a full I8 IMM. The widened product forces scalar mode
        // (SX_I64 has no AVX2 path); the query still JIT-compiles (usesCompiledFilter stays
        // true).
        assertMemoryLeak(() -> {
            execute("create table p as (select cast(100_000 as int) a, cast(50_000 as int) b," +
                    " cast(1.0 as float) f32, cast(1.0 as double) f64," +
                    " x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro: a*b = 5e9 at long width but 4_999_999_999 rounds to 5.0e9f. Absolute
            // pin: the equality is false on the Java path (705_032_704 != 4_999_999_999), so the boolean
            // equality with (f32 > 0 = true) is false for every row - 0 rows. The pre-fix JIT
            // returned all 5.
            assertJitMatchesJavaOnEmptyResult("select cs from p where ((a*b) = 4_999_999_999) = (f32 > 0)", true, "cs\n");
            // The rounded constant (5.0e9f) lands exactly on the widened product, so at float width
            // "=" and ">=" turn true and "<>" and "<" turn false - those four flip. ">" and "<="
            // read the same either way, because Java's product is MulInt#getLong, the INT wrap
            // 705_032_704 (spelled out at the >= / < pair below), which is below the bound as well.
            // So the > pin is empty, and what it carries is the other direction: a product read at
            // long width (5e9) makes "> 4_999_999_999" true and returns all five rows.
            assertJitMatchesJavaOnEmptyResult("select cs from p where ((a*b) > 4_999_999_999) = (f32 > 0)", true,
                    "cs\n");
            assertJitMatchesJava("select cs from p where ((a*b) <= 4_999_999_999) = (f32 > 0)", true, "cs\n1\n2\n3\n4\n5\n");
            assertJitMatchesJava("select cs from p where ((a*b) <> 4_999_999_999) = (f32 > 0)", true,
                    "cs\n1\n2\n3\n4\n5\n");
            // The product wraps to 705_032_704, so it is BELOW the bound: >= is false on every row
            // and < is true on every row. The pair is complementary, so one of them always bites.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) >= 4_999_999_999) = (f32 > 0)", true);
            assertJitMatchesJava("p where ((a*b) < 4_999_999_999) = (f32 > 0)", true);
            // Operand order (constant on the left, and the two comparisons swapped).
            assertJitMatchesJavaOnEmptyResult("p where (4_999_999_999 = (a*b)) = (f32 > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where (f32 > 0) = ((a*b) = 4_999_999_999)", true);
            // Through a NOT wrapper.
            assertJitMatchesJava("p where not (((a*b) = 4_999_999_999) = (f32 > 0))", true);

            // SAFE boundaries - must keep passing, do not over-widen:
            // Negated / folded constant is an OPERATION handled by the fold-root path, not by
            // the bare-CONSTANT widen: (a*b) < -4_999_999_999 is false, so the equality is false.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) < -4_999_999_999) = (f32 > 0)", true);
            // AND / OR siblings split into separate predicate contexts (no float suppression on
            // the (a*b) = 4_999_999_999 sub-predicate, so needsNarrowI64Widening handles it).
            assertJitMatchesJavaOnEmptyResult("p where (a*b) = 4_999_999_999 and f32 > 0", true);
            assertJitMatchesJava("p where (a*b) = 4_999_999_999 or f32 > 0", true);
            // A DOUBLE sibling makes hasMixedSizes() true (I4 vs F8), so serializeUntypedNumber
            // already emits an exact I8 - the fix widens redundantly, result unchanged.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) = 4_999_999_999) = (f64 > 0)", true);
            // BYTE / SHORT products cannot reach 2^31 and their I1 / I2 size differs from F4, so
            // the constant is already mixed-size (exact I8) and never hits the float gap.
            assertJitMatchesJavaOnEmptyResult("p where ((cbyte * cbyte) = 4_999_999_999) = (f32 > 0)", true);
            assertJitMatchesJavaOnEmptyResult("p where ((cs * cs) = 4_999_999_999) = (f32 > 0)", true);
            // A constant-fold root. Both operands are out of INT range, so the subtree is LONG and
            // folds at full width to 4_999_999_999 - the wrapped product is below it, exactly as
            // against the bare constant above, and the complementary spelling bites.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) > (2_500_000_000 + 2_499_999_999)) = (f32 > 0)", true);
            assertJitMatchesJava("p where ((a*b) < (2_500_000_000 + 2_499_999_999)) = (f32 > 0)", true);
            // An IN key takes its per-element width from serializeIn's inKeyWidthOverride.
            assertJitMatchesJavaOnEmptyResult("p where ((a*b) in (4_999_999_999)) = (f32 > 0)", true);
            // Over-widening guard: an IN-RANGE constant (705_032_704 = 5e9 wrapped mod 2^32) must
            // still WRAP against the INT-width product - cmpType stays I4, the constant stays
            // I4, and MulInt#getInt matches it on both paths. Absolute pin: all 5 rows survive.
            assertJitMatchesJava("select cs from p where ((a*b) = 705_032_704) = (f32 > 0)", true,
                    "cs\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testConstantColumnArithmetics() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64")
                .withArithmeticOperator()
                .withAnyOf("3", "-3.5")
                .withAnyOf(" + ")
                .withAnyOf("42.5", "-42")
                .withArithmeticOperator()
                .withOptionalNegation().withAnyOf("f32", "f64")
                .withAnyOf(" > 1");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testConstantHoistingExceedsCacheCapacity() throws Exception {
        // Tests constant hoisting with more than 8 constants (exceeds cache capacity)
        // The backend constant cache has capacity of 8 elements
        final String query = "x where " +
                "i64 > 1 and i64 < 100 and i64 != 10 and i64 != 20 " +
                "and i64 != 30 and i64 != 40 and i64 != 50 and i64 != 60 " +
                "and i64 != 70 and i64 != 80 and i64 != 90";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 200, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantHoistingMixedTypes() throws Exception {
        // Tests constant hoisting with mixed types exceeding cache capacity
        final String query = "x where " +
                "i32 > 1 and i32 < 100 and i64 > 2 and i64 < 200 " +
                "and f32 > 0.1 and f32 < 0.9 and f64 > 0.2 and f64 < 0.8 " +
                "and i32 != 50 and i64 != 100 and f32 != 0.5 and f64 != 0.5";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(0, 200, 0) i32," +
                " rnd_long(0, 300, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantHoistingWithinCacheCapacity() throws Exception {
        // Tests constant hoisting with 8 constants (within cache capacity of 8)
        final String query = "x where " +
                "i64 > 1 and i64 < 100 and i64 != 50 and i64 != 51 " +
                "and i64 != 52 and i64 != 53 and i64 != 54 and i64 != 55";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 200, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testConstantFoldDivisionAtIntWidth() throws Exception {
        // A folded constant with a non-modular operator (division) must replicate
        // the Java filter's per-op INT wrapping at an INT-width comparison.
        // (1_000_000 * 1_000_000) / 7 folds in long to 142_857_142_857, whose low 32
        // bits are +1_123_222_089, but DivInt#getInt computes
        // (int) (1_000_000 * 1_000_000) / 7 = -103_911_424. Folding in long and
        // truncating diverged: the JIT returned 0 rows where the Java filter
        // returned every row.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8," +
                " cast(x - 100 as short) i16," +
                " cast(x - 100 as int) i32," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Per-op INT wrap = -103_911_424; every narrow/INT row (>= -99) exceeds it.
            assertQueryNotNullNoLeakCheck("x where i8 > (1_000_000 * 1_000_000) / 7");
            assertQueryNotNullNoLeakCheck("x where i16 > (1_000_000 * 1_000_000) / 7");
            assertQueryNotNullNoLeakCheck("x where i32 > (1_000_000 * 1_000_000) / 7");
            // The LONG comparison reads the SAME INT-width fold, not a long-width one: DivInt
            // is an IntFunction, so FunctionParser#functionToConstant0 folds the subtree to
            // IntConstant(-103_911_424) and IntFunction#getLong sign-extends that wrap onto the
            // i64 lane. Every i64 row (>= -99) exceeds it, so this line pins rows exactly like
            // its narrow siblings; a revert to a long-width fold (142_857_142_857) returns none.
            assertQueryNotNullNoLeakCheck("x where i64 > (1_000_000 * 1_000_000) / 7");
        });
    }

    @Test
    public void testConstantFoldIntNullCollision() throws Exception {
        // An inner constant product that wraps to EXACTLY INT_NULL (-2^31)
        // poisons the rest of an INT-width fold: the Java filter's
        // MulInt/AddInt#getInt return INT_NULL once an operand is INT_NULL, so
        // i8 > (65_536 * 32_768) * 2 collapses to i8 > NULL (no rows). The JIT I4
        // fold did pure modular arithmetic (-2^31 * 2 = 0) and returned i8 > 0
        // (rows). tryFoldConstantArithI4 now propagates INT_NULL like the
        // runtime ops, so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1_000_000) k," +
                    " cast(x - 2 as byte) i8," +
                    " cast(x - 2 as short) i16," +
                    " cast(x - 2 as int) i32," +
                    " (x - 2) i64" +
                    " from long_sequence(5)) timestamp(k)");
            // INT-width comparisons: inner 65_536 * 32_768 wraps to INT_NULL, so
            // the whole fold is NULL and no row matches.
            // Absolute pin: the fold collapses to NULL, so no row matches (header only); the
            // pre-fix JIT read i8 > 0 and returned rows.
            assertJitMatchesJavaOnEmptyResult("x where i8 > (65_536 * 32_768) * 2", true, "k\ti8\ti16\ti32\ti64\n");
            assertJitMatchesJavaOnEmptyResult("x where i16 > (65_536 * 32_768) * 2", true);
            assertJitMatchesJavaOnEmptyResult("x where i32 > (65_536 * 32_768) * 2", true);
            // LONG column promotes to long width: getLong() never wraps onto the
            // sentinel, so the constant is the genuine 4_294_967_296 on both paths.
            assertJitMatchesJavaOnEmptyResult("x where i64 > (65_536 * 32_768) * 2", true);
        });
    }

    @Test
    public void testConstantFoldIntNullCollisionAtDoubleObservedWidth() throws Exception {
        // The DOUBLE-observation sibling of testConstantFoldIntNullCollision. A pure-INT constant
        // chain whose LONG-width fold still FITS int - "0 - 2147483647 - 1" is -2^31 at both
        // widths - never reaches descend()'s fold, so the operations are emitted and the backend
        // computes them. Which width it computes at is what diverged: serializeConstant let
        // narrowKeptConstants override only an I8 observation, so a predicate whose only observed
        // column is a DOUBLE typed the chain's constants at I8 and the backend ran int64_sub.
        // -2_147_483_648 is an ordinary number at 64 bits, so cvt_ltod produced a finite bound and
        // every row passed. The Java filter folds the same chain through SubInt#getInt, lands on
        // Numbers.INT_NULL, and IntConstant#getDouble reads that as NaN - so no row passes. The
        // constants now stay at I4 whatever the observation, int32_sub reproduces the sentinel and
        // cvt_itod / int32_to_double map it to the same NaN.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE icd AS (
                        SELECT timestamp_sequence(0, 1_000_000) k,
                            x::double d64,
                            CASE WHEN x = 2 THEN cast(NULL AS LONG) ELSE x END l64
                        FROM long_sequence(4)
                    ) TIMESTAMP(k)
                    """);
            final String allRows = "k\td64\tl64\n" +
                    "1970-01-01T00:00:00.000000Z\t1.0\t1\n" +
                    "1970-01-01T00:00:01.000000Z\t2.0\tnull\n" +
                    "1970-01-01T00:00:02.000000Z\t3.0\t3\n" +
                    "1970-01-01T00:00:03.000000Z\t4.0\t4\n";

            // The shape the divergence was reported on: a DOUBLE column against the INT
            // sentinel chain. The bound is NULL, so nothing matches; the pre-fix JIT compared
            // against -2_147_483_648.0 and returned every row.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("icd where d64 > (0 - 2_147_483_647 - 1)", "k\td64\tl64\n");
            // Control one operation short of the sentinel: -2_147_483_647 is an ordinary INT, so both
            // engines compare against it and every row passes. Without this the test above would
            // pass just as well against a serializer that declined the whole filter.
            assertJitScalarAndVectorMatchJava("icd where d64 > (0 - 2_147_483_647)", allRows);
            // The sentinel poisons what follows it, exactly as AddInt#getInt does: INT_NULL + 1 is
            // INT_NULL, not -2_147_483_647. int32_add's check_int32_null reproduces that; int64_add
            // over the pre-fix i64 immediates produced a finite -2_147_483_647 and every row passed.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("icd where d64 > (0 - 2_147_483_647 - 1) + 1", "k\td64\tl64\n");
            // The opposite ordering operator. This one agreed even before the fix - every d64 is
            // above -2_147_483_648.0 as well as incomparable to NULL - so it is a companion pin
            // rather than a second red signal, and it is here because "> NULL" and "< NULL" are
            // both false only when the bound really is NULL.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("icd where d64 < (0 - 2_147_483_647 - 1)", "k\td64\tl64\n");
            // The chain on the LEFT of the comparison, since markWidthSemantics walks lhs and rhs
            // through different call sites.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("icd where (0 - 2_147_483_647 - 1) < d64", "k\td64\tl64\n");
            // The IN spelling, which reaches markWidthSemantics through its own args loop. The
            // key is NaN on the null l64 row, so the NULL element matches it and the finite
            // element the pre-fix JIT emitted did not.
            assertJitScalarAndVectorMatchJava("icd where (d64 - l64) in ((0 - 2_147_483_647 - 1), 99.0)",
                    "k\td64\tl64\n" +
                            "1970-01-01T00:00:01.000000Z\t2.0\tnull\n");
            // Equality against the sentinel: Numbers#equals and double_cmp_epsilon both call two
            // non-finite values equal, so the NULL bound matches the NULL row and nothing else.
            // The pre-fix JIT compared a finite -2_147_483_648.0 and matched no row at all.
            assertJitScalarAndVectorMatchJava(
                    "icd where (d64 - l64) = (0 - 2_147_483_647 - 1)",
                    "k\td64\tl64\n" +
                            "1970-01-01T00:00:01.000000Z\t2.0\tnull\n"
            );

            // The same sentinel one level deeper, reduced to its mechanism: the chain sits under
            // a DOUBLE division rather than directly under the comparison, so markWidthSemantics
            // reaches its constants one level deeper. The Java filter divides NaN by d64 and gets
            // NaN, which equals the NaN the null l64 row produces on the left; the pre-fix JIT
            // divided a finite -2_147_483_648.0 and matched nothing.
            assertJitScalarAndVectorMatchJava(
                    "icd where ((l64 - d64) * (l64 - 0.0)) = ((-1 - 2_147_483_647) / d64)",
                    "k\td64\tl64\n" +
                            "1970-01-01T00:00:01.000000Z\t2.0\tnull\n"
            );
            // Same shape one unit off the sentinel, so the division really is exercised: the bound
            // is -2_147_483_647.0 / d64, which matches no row, and the null row's NaN no longer has a
            // NaN to pair with. Both engines return nothing for the right reason.
            assertJitScalarAndVectorMatchJavaOnEmptyResult(
                    "icd where ((l64 - d64) * (l64 - 0.0)) = ((-1 - 2_147_483_646) / d64)",
                    "k\td64\tl64\n"
            );

            // A LONG observation was always right - narrowKeptConstants already overrode I8 and
            // the scalar convert() carries INT_NULL to LONG_NULL - and stays right.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("icd where l64 > (0 - 2_147_483_647 - 1)", "k\td64\tl64\n");
            // Ordinary arithmetic in the same position must keep computing: a chain with no
            // sentinel in it emits at I4 now and still compares as the number it names.
            assertJitScalarAndVectorMatchJava("icd where d64 > (1000 - 998)",
                    "k\td64\tl64\n" +
                            "1970-01-01T00:00:02.000000Z\t3.0\t3\n" +
                            "1970-01-01T00:00:03.000000Z\t4.0\t4\n");
        });
    }

    @Test
    public void testConstantFoldIntSubtreeUnderLongRoot() throws Exception {
        // A pure-INT sub-subtree nested under a LONG constant fold must wrap at INT width
        // before the enclosing LONG op reads it, exactly as AddLong#getLong reads
        // AddInt#getLong() = Numbers.intToLong(getInt()). For
        //   (2_000_000_000 + 2_000_000_000) + 5_000_000_000
        // the inner INT add wraps to -294_967_296, so the Java filter's bound is
        // -294_967_296 + 5_000_000_000 = 4_705_032_704. The pre-fix JIT folded the WHOLE
        // subtree at long width (4_000_000_000 + 5_000_000_000 = 9_000_000_000):
        //   i64 > bound : Java keeps (4.705e9, +inf], the JIT only (9e9, +inf] -> under-returned
        //   i64 < bound : Java keeps [-inf, 4.705e9), the JIT [-inf, 9e9)      -> over-returned
        assertMemoryLeak(() -> {
            execute("create table t (i64 long, k timestamp) timestamp(k) partition by day");
            execute("insert into t values " +
                    "(0, '2024-01-01T00:00:00.000000Z')," +
                    "(3_000_000_000, '2024-01-01T00:00:01.000000Z')," +
                    "(6_000_000_000, '2024-01-01T00:00:02.000000Z')," +
                    "(10_000_000_000, '2024-01-01T00:00:03.000000Z')");
            // Java bound 4_705_032_704 keeps 6e9 and 10e9; the pre-fix JIT bound 9e9 dropped
            // 6e9 (under-return).
            assertJitMatchesJava("t where i64 > (2_000_000_000 + 2_000_000_000) + 5_000_000_000", true,
                    "i64\tk\n" +
                            "6000000000\t2024-01-01T00:00:02.000000Z\n" +
                            "10000000000\t2024-01-01T00:00:03.000000Z\n");
            // Opposite direction: Java keeps 0 and 3e9; the pre-fix JIT also kept 6e9
            // (over-return), which is why the two directions together cannot both be vacuous.
            assertJitMatchesJava("t where i64 < (2_000_000_000 + 2_000_000_000) + 5_000_000_000", true,
                    "i64\tk\n" +
                            "0\t2024-01-01T00:00:00.000000Z\n" +
                            "3000000000\t2024-01-01T00:00:01.000000Z\n");
            // Nested-LONG sub-op: (5_000_000_000 - 4_705_032_704) = 294_967_296 is a genuine
            // LONG subtree, so the width-aware fold recurses into its operation while still
            // wrapping the sibling INT add to -294_967_296. Their sum is exactly 0, and only the
            // 0 row matches. The pre-fix JIT folded the whole thing at long width to
            // 4_294_967_296 and matched nothing.
            assertJitMatchesJava("t where i64 = (2_000_000_000 + 2_000_000_000) + (5_000_000_000 - 4_705_032_704)", true,
                    "i64\tk\n" +
                            "0\t2024-01-01T00:00:00.000000Z\n");
            // Control: an all-LONG fold of the same shape was always correct
            // (5_000_000_000 + 5_000_000_000 = 10_000_000_000), so '=' keeps only 10e9.
            assertJitMatchesJava("t where i64 = 5_000_000_000 + 5_000_000_000", true,
                    "i64\tk\n" +
                            "10000000000\t2024-01-01T00:00:03.000000Z\n");
        });
    }

    @Test
    public void testConstantFoldLongNullCollision() throws Exception {
        // The LONG analog of testConstantFoldIntNullCollision. An inner constant
        // product that lands EXACTLY on Long.MIN_VALUE (-2^63, the LONG null
        // sentinel) poisons the rest of a long-width fold: the Java filter's
        // MulLong/AddLong#getLong return LONG_NULL once an operand is LONG_NULL,
        // so i64 = (4_611_686_018_427_387_904 * -2) + 5 folds the RHS to NULL and
        // matches the null row. The JIT long fold kept computing full-width
        // arithmetic ((2^62 * -2) + 5 = Long.MIN + 5) and matched no row.
        // tryFoldConstantArith now propagates LONG_NULL, so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table lp as (select timestamp_sequence(0, 1_000_000) k," +
                    " case when x = 1 then cast(null as long) else x end i64" +
                    " from long_sequence(3)) timestamp(k)");
            // 4_611_686_018_427_387_904 = 2^62; 2^62 * -2 = Long.MIN (intermediate),
            // + 5 a non-sentinel final. The fold collapses to LONG_NULL on both
            // paths, so only the null row matches '='.
            // Absolute pin: the fold collapses to LONG_NULL, so only the null row matches '=';
            // the pre-fix JIT computed Long.MIN + 5 and matched nothing.
            assertJitMatchesJava("lp where i64 = (4_611_686_018_427_387_904 * -2) + 5", true,
                    "k\ti64\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\n");
            assertJitMatchesJava("lp where i64 <> (4_611_686_018_427_387_904 * -2) + 5", true);
            assertJitMatchesJava("lp where i64 in (1, (4_611_686_018_427_387_904 * -2) + 5)", true);
            assertJitMatchesJava("lp where i64 not in (1, (4_611_686_018_427_387_904 * -2) + 5)", true);
            // Control: a fold whose FINAL value is Long.MIN agrees coincidentally
            // (both paths emit the sentinel), so it was never a divergence.
            assertJitMatchesJava("lp where i64 = 4_611_686_018_427_387_904 * -2", true);
        });
    }

    @Test
    public void testConstantFoldNullSentinelUnderNonArithmeticRoot() throws Exception {
        // Only + - * / may become a constant fold root, but both integer folds propagated the
        // NULL sentinel BEFORE looking at the node's own operator, so a COMPARISON whose two
        // children are constant arithmetic subtrees was mistaken for a fold root and replaced
        // with a single IMM:
        //   2_097_152 * 2_097_152 * 2_097_152 is exactly 2^63 at long width, i.e. LONG_NULL,
        //   and 65_536 * 32_768 is exactly 2^31 at int width, i.e. INT_NULL,
        // so the long fold answered LONG_NULL and the int fold answered INT_NULL for the '='
        // node itself. INT_NULL is non-zero, which the IR reads as TRUE, so the JIT kept rows
        // the Java filter dropped - the comparison is really 0 = NULL, which is false.
        // The floating-point folder always rejected a non-arithmetic token before recursing;
        // both integer folders now do the same.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE nasr AS (
                        SELECT timestamp_sequence(0, 1_000_000) k,
                            CASE WHEN x = 1 THEN cast(NULL AS LONG) ELSE x - 2 END i64,
                            CASE WHEN x = 1 THEN cast(NULL AS INT) ELSE cast(x - 2 AS INT) END i32
                        FROM long_sequence(4)
                    ) TIMESTAMP(k)
                    """);
            final String allPositiveRows = "k\ti64\ti32\n" +
                    "1970-01-01T00:00:02.000000Z\t1\t1\n" +
                    "1970-01-01T00:00:03.000000Z\t2\t2\n";
            // NOT arm: the constant comparison is false, so NOT makes the filter i64 > 0. The
            // pre-fix JIT negated the truthy IMM instead and returned nothing.
            assertJitScalarAndVectorMatchJava(
                    "nasr where i64 > 0 and not ((2_097_152 * 2_097_152 * 2_097_152) = (65_536 * 32_768))",
                    allPositiveRows
            );
            // Boolean equality of two comparisons is one predicate context, so the folded IMM
            // landed where the second comparison belonged: pre-fix the JIT compared the boolean
            // i32 > 0 against INT_NULL and matched nothing, where 0 > NULL is simply false.
            assertJitScalarAndVectorMatchJava(
                    "nasr where (i32 > 0) = ((2_097_152 * 2_097_152 * 2_097_152) > (65_536 * 32_768))",
                    "k\ti64\ti32\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\tnull\n" +
                            "1970-01-01T00:00:01.000000Z\t0\t0\n"
            );
            // The remaining arms diverged the other way: the malformed IR failed to compile
            // ("invalid opcode") and dropped the whole filter back to the Java engine, so these
            // assert JIT is used as well as what it returns. A false constant under AND is not
            // among them - the optimizer answers that one with an empty factory and never reaches
            // the JIT - so the roots below are the ones a JIT-compiled filter really meets.
            assertJitScalarAndVectorMatchJava(
                    "nasr where i64 > 0 or (2_097_152 * 2_097_152 * 2_097_152) = (65_536 * 32_768)",
                    allPositiveRows
            );
            // Inequality root: 0 <> NULL is true, so AND keeps exactly i64 > 0, where an ordering
            // root against the sentinel (below) is false and leaves the OR arm alone. The two
            // together pin both truth values, so neither direction can be vacuous.
            assertJitScalarAndVectorMatchJava(
                    "nasr where i64 > 0 and (2_097_152 * 2_097_152 * 2_097_152) <> (65_536 * 32_768)",
                    allPositiveRows
            );
            assertJitScalarAndVectorMatchJava(
                    "nasr where i32 > 0 or (2_097_152 * 2_097_152 * 2_097_152) >= (65_536 * 32_768)",
                    allPositiveRows
            );
            // The opposite ordering operator, so the backend's handling of the sentinel immediate
            // is pinned in both directions.
            assertJitScalarAndVectorMatchJava(
                    "nasr where i32 > 0 or (2_097_152 * 2_097_152 * 2_097_152) < (65_536 * 32_768)",
                    allPositiveRows
            );
            // Control: with both sentinels under ONE arithmetic root the node IS a fold root and
            // must keep folding to the INT sentinel - the guard only rejects a non-arithmetic
            // root. INT arithmetic reads as NULL at every width, so this matches the NULL row.
            assertJitScalarAndVectorMatchJava(
                    "nasr where i32 = (2_097_152 * 2_097_152 * 2_097_152) + 65_536 * 32_768",
                    "k\ti64\ti32\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\tnull\n"
            );
        });
    }

    @Test
    public void testConstantFoldRootUnderLongWithFloat() throws Exception {
        // An overflowing constant product (258_558 * -259_815) nested under a LONG
        // add (c0 + ...) is read at long width by AddLong#getLong, so the Java
        // filter never wraps it. A FLOAT in the predicate suppressed the global
        // narrow-i64 widening, and the old check only looked at LONG *leaves*, so the
        // JIT folded the product to a wrapped I4 IMM and diverged (JIT all rows, Java
        // none). markI64WidenFoldRoots / genuineArithType now tag such fold roots for a
        // full I8 IMM.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1_000_000, 1_000_000, 8), " +
                    "rnd_int(-1_000_000, 1_000_000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L) " +
                    "FROM long_sequence(122)");
            // Previously diverging shapes: fold root under a genuine LONG op with a
            // FLOAT comparison. Still JIT-compiled, now correct.
            // Absolute pin: the fold is a large negative long, so no float c9 is <= it and Java
            // returns 0 rows; the pre-fix JIT wrapped it to a small I4 and returned every row.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (258_558 * -259_815))", true,
                    "c0\tc8\tc9\tts\n995420\t724408\t0.28455776\t2024-01-01T00:00:00.000000Z\n-197185\t530625\t0.13123357\t2024-01-01T00:30:00.000000Z\n931616\t252740\t0.88992864\t2024-01-01T01:00:00.000000Z\n32662\t-492805\t0.46218354\t2024-01-01T01:30:00.000000Z\n305205\t-544245\t0.7261136\t2024-01-01T02:00:00.000000Z\n152628\t444703\t0.709436\t2024-01-01T02:30:00.000000Z\n861047\t-94278\t0.0035983324\t2024-01-01T03:00:00.000000Z\n563251\tnull\t0.7675673\t2024-01-01T03:30:00.000000Z\n277159\t127479\t0.1578663\t2024-01-01T04:00:00.000000Z\n-16687\t611233\t0.5793466\t2024-01-01T04:30:00.000000Z\n646784\t-398329\t0.6761935\t2024-01-01T05:00:00.000000Z\n672088\tnull\t0.97552633\t2024-01-01T05:30:00.000000Z\n782963\t-732673\t0.41381645\t2024-01-01T06:00:00.000000Z\n-457772\tnull\t0.8445258\t2024-01-01T06:30:00.000000Z\n6099\t846598\t0.8847591\t2024-01-01T07:00:00.000000Z\n-834573\t787024\t0.8258367\t2024-01-01T07:30:00.000000Z\n-908023\t-398574\t0.92050034\t2024-01-01T08:00:00.000000Z\n353576\t-494248\t0.86641586\t2024-01-01T08:30:00.000000Z\n942690\t-983929\t0.5659429\t2024-01-01T09:00:00.000000Z\n556636\t-188008\t0.72300154\t2024-01-01T09:30:00.000000Z\n-713605\t-204515\t0.11585981\t2024-01-01T10:00:00.000000Z\n-934082\t-868335\t0.8685154\t2024-01-01T10:30:00.000000Z\n-879266\t465748\t0.7692382\t2024-01-01T11:00:00.000000Z\n19004\t-820402\t0.4202044\t2024-01-01T11:30:00.000000Z\n849301\t-665657\t0.28200203\t2024-01-01T12:00:00.000000Z\n-476491\t-533907\t0.44804686\t2024-01-01T12:30:00.000000Z\n-158298\t-256637\t0.1975137\t2024-01-01T13:00:00.000000Z\n-238138\tnull\t0.34568977\t2024-01-01T13:30:00.000000Z\n-65075\t124773\t0.6192919\t2024-01-01T14:00:00.000000Z\n-799807\t-722539\t0.21858656\t2024-01-01T14:30:00.000000Z\n814264\t-660470\t0.7056586\t2024-01-01T15:00:00.000000Z\n-106009\t-537314\t0.13006097\t2024-01-01T15:30:00.000000Z\n-439312\t224362\t0.069444776\t2024-01-01T16:00:00.000000Z\n176625\t-537751\t0.5893398\t2024-01-01T16:30:00.000000Z\n-786639\t883580\t0.9918093\t2024-01-01T17:00:00.000000Z\n864907\t952476\t0.89989215\t2024-01-01T17:30:00.000000Z\n605553\t-819707\t0.337461\t2024-01-01T18:00:00.000000Z\n851680\t606572\t0.7445999\t2024-01-01T19:00:00.000000Z\n509854\t259494\t0.27115327\t2024-01-01T19:30:00.000000Z\n403404\t563913\t0.6797563\t2024-01-01T20:00:00.000000Z\n149434\t-634730\t0.73651147\t2024-01-01T20:30:00.000000Z\n-602769\t-494000\t0.98840106\t2024-01-01T21:00:00.000000Z\n608028\t-832620\t0.38422543\t2024-01-01T22:00:00.000000Z\n-972221\t993071\t0.17180288\t2024-01-01T22:30:00.000000Z\n-673046\t-324050\t0.8584308\t2024-01-01T23:00:00.000000Z\n-344450\t751031\t0.0436064\t2024-01-01T23:30:00.000000Z\n-389080\t232080\t0.45920676\t2024-01-02T00:30:00.000000Z\n-950241\t-919312\t0.050941825\t2024-01-02T01:00:00.000000Z\n-360026\t632564\t0.8977236\t2024-01-02T01:30:00.000000Z\n22693\t-316025\t0.76947445\t2024-01-02T02:00:00.000000Z\n-796722\t567730\t0.89245474\t2024-01-02T02:30:00.000000Z\n-177608\t805591\t0.5913874\t2024-01-02T03:00:00.000000Z\n-664103\t586563\t0.12642151\t2024-01-02T03:30:00.000000Z\n508000\t-195629\t0.44402504\t2024-01-02T04:00:00.000000Z\n773743\t-725780\t0.24001455\t2024-01-02T04:30:00.000000Z\n289580\t-902629\t0.7419701\t2024-01-02T05:00:00.000000Z\n-677407\t-955834\t0.2739985\t2024-01-02T05:30:00.000000Z\n-865645\t-983333\t0.93598145\t2024-01-02T06:00:00.000000Z\n-994358\t100159\t0.78732294\t2024-01-02T06:30:00.000000Z\n921293\t549344\t0.039732814\t2024-01-02T07:00:00.000000Z\n177398\t-863262\t0.83210003\t2024-01-02T07:30:00.000000Z\n-965634\t819074\t0.62260014\t2024-01-02T08:00:00.000000Z\n399895\t924465\t0.8786111\t2024-01-02T08:30:00.000000Z\n156496\t17575\t0.6334964\t2024-01-02T09:00:00.000000Z\n-413901\t-182824\t0.77072495\t2024-01-02T09:30:00.000000Z\n-557437\t-510256\t0.17405552\t2024-01-02T10:00:00.000000Z\n-133637\t436818\t0.007985413\t2024-01-02T10:30:00.000000Z\n-633667\t-421873\t0.75304943\t2024-01-02T11:00:00.000000Z\n52245\t143965\t0.0024457574\t2024-01-02T11:30:00.000000Z\n423168\t-832011\t0.31212717\t2024-01-02T12:00:00.000000Z\n-980843\t714462\t0.35210842\t2024-01-02T12:30:00.000000Z\n648571\t-240479\t0.18746626\t2024-01-02T13:00:00.000000Z\n251668\t-931379\t0.4740684\t2024-01-02T14:00:00.000000Z\n680935\t211681\t0.5308756\t2024-01-02T14:30:00.000000Z\n-636113\t392285\t0.039509535\t2024-01-02T15:00:00.000000Z\n933033\t458887\t0.7407843\t2024-01-02T15:30:00.000000Z\n-84733\t230348\t0.4349324\t2024-01-02T16:00:00.000000Z\n-816531\t309874\t0.23405439\t2024-01-02T16:30:00.000000Z\n-207761\t47333\t0.90589\t2024-01-02T17:00:00.000000Z\n-959997\t-326872\t0.1104731\t2024-01-02T17:30:00.000000Z\n-792196\t867410\t0.82176524\t2024-01-02T18:00:00.000000Z\n-234108\t-814588\t0.29419792\t2024-01-02T18:30:00.000000Z\n-357294\t-118587\t0.8574212\t2024-01-02T19:00:00.000000Z\n350666\t302933\t0.78424555\t2024-01-02T19:30:00.000000Z\n26643\t725464\t0.7253202\t2024-01-02T21:00:00.000000Z\n-208953\t-607284\t0.9997797\t2024-01-02T21:30:00.000000Z\n-666903\tnull\t0.13312209\t2024-01-02T22:00:00.000000Z\n-436626\t569511\t0.84612113\t2024-01-02T23:00:00.000000Z\n-933184\t-825029\t0.84058154\t2024-01-02T23:30:00.000000Z\n-840879\t207526\t0.3185253\t2024-01-03T00:00:00.000000Z\n-254049\t321599\t0.75662524\t2024-01-03T00:30:00.000000Z\n570349\t511485\t0.9116843\t2024-01-03T01:00:00.000000Z\n313138\t717584\t0.0032519698\t2024-01-03T01:30:00.000000Z\n193462\t-760966\t0.7295366\t2024-01-03T02:00:00.000000Z\n-156252\t-391489\t0.8895916\t2024-01-03T02:30:00.000000Z\n532182\tnull\t0.5251698\t2024-01-03T03:30:00.000000Z\n217303\t-4276\t0.729866\t2024-01-03T04:00:00.000000Z\n-376197\t-394005\t0.32824337\t2024-01-03T04:30:00.000000Z\n405844\t-244727\t0.70870113\t2024-01-03T05:00:00.000000Z\n464032\t157466\t0.514972\t2024-01-03T05:30:00.000000Z\n580700\t671698\t0.94126636\t2024-01-03T06:00:00.000000Z\n641363\t24423\t0.60070705\t2024-01-03T06:30:00.000000Z\n-779414\t-83670\t0.7435991\t2024-01-03T07:00:00.000000Z\n613616\t492985\t0.011099219\t2024-01-03T07:30:00.000000Z\n-323557\t106833\t0.119470954\t2024-01-03T08:00:00.000000Z\n873673\t460727\t0.52387\t2024-01-03T08:30:00.000000Z\n573948\t62494\t0.85041\t2024-01-03T09:00:00.000000Z\n-971520\tnull\t0.5788151\t2024-01-03T09:30:00.000000Z\n251501\t520699\t0.7468602\t2024-01-03T10:00:00.000000Z\n-997581\t109394\t0.34892786\t2024-01-03T10:30:00.000000Z\n-649681\t-112158\t0.93162835\t2024-01-03T11:00:00.000000Z\n617480\t-75765\t0.8898226\t2024-01-03T11:30:00.000000Z\n-926006\t77888\t0.4595378\t2024-01-03T12:00:00.000000Z\n-153783\t488143\t0.73708236\t2024-01-03T12:30:00.000000Z\n");
            // 258_558 * -259_815 is INT arithmetic and wraps to +1_542_229_966, so c0 - it is
            // large negative and no FLOAT c9 is <= it. The commuted spelling adds it instead and
            // every row passes, which is what keeps this pair from being vacuous.
            assertJitMatchesJavaOnEmptyResult("SELECT * FROM t WHERE c9 <= (c0 - (258_558 * -259_815))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((258_558 * -259_815) + c0)", true);
            // Control: direct float-vs-overflow comparison still wraps via
            // getDouble(getInt()), so the wrapped I4 fold remains correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (258_558 * -259_815)", true);
            // Control: pure-INT arithmetic under a float wraps (no genuine LONG). The wrapped
            // sum is an INT-width arithmetic RESULT against a FLOAT column, so SX_I64 widens it
            // after the add and the comparison runs at f64. See markIntCmpFloatOperand.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 + (258_558 * -259_815))", true);
        });
    }

    @Test
    public void testConstantFoldWidthUnderBooleanEquality() throws Exception {
        // A boolean equality of two comparisons - (cmp) = (cmp) - forms a
        // SINGLE predicate context, so a predicate-global width signal applies
        // one width to both comparisons. An overflowing INT fold then took the
        // wrong width:
        //   - I4-where-I8: (1_000_000 * 1_000_000 > i64) = (f32 > 0) - the fold
        //     feeds a LONG comparison, and the float suppressed global widening,
        //     so the fold root went unmarked and the two paths read it at
        //     different widths. The Java filter's fold is an IntConstant carrying
        //     the wrap, sign-extended to -727_379_968 for the comparison, which is
        //     below every i64 here (Java 0 rows); reading it at long width instead
        //     keeps ids 1 and 2 (JIT 2 rows).
        //   - I8-where-I4: (ci > 1_000_000 * 1_000_000) = (cl > 0) - the fold feeds
        //     an INT comparison and must wrap, but the predicate-global
        //     long-widening (driven by the sibling LONG comparison) forced it to
        //     I8 (Java 3 rows, JIT 0).
        // The fold width is now derived from the fold's own comparison context.
        assertMemoryLeak(() -> {
            execute("create table x as (select timestamp_sequence(0, 1_000_000) k, x id," +
                    " (case when x = 1 then 0L when x = 2 then 5L else 2_000_000_000_000L end) i64," +
                    " x::int ci, x::long cl, 1.0f f32" +
                    " from long_sequence(3)) timestamp(k)");
            // Previously diverging shapes, both directions.
            // Absolute pin: the wrapped fold sign-extends to -727_379_968, which is below every
            // i64 in the fixture, so Java keeps nothing. A fold read at long width (10^12) keeps
            // ids 1 and 2 instead, which is what this line reddens on.
            assertJitMatchesJavaOnEmptyResult("select id from x where (1_000_000 * 1_000_000 > i64) = (f32 > 0)", true,
                    "id\n");
            assertJitMatchesJava("select id from x where (ci > 1_000_000 * 1_000_000) = (cl > 0)", true);
            // Controls: single comparison, AND, OR each split into separate
            // predicate contexts and were always correct.
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000", true);
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000 and cl > 0", true);
            assertJitMatchesJava("select id from x where ci > 1_000_000 * 1_000_000 or cl > 0", true);
            // Control: the LONG column reads the fold at long width on both paths.
            assertJitMatchesJava("select id from x where i64 > 1_000_000 * 1_000_000", true);
        });
    }

    @Test
    public void testConstantOperandWidthUnderBooleanEquality() throws Exception {
        // The constant-operand analog of testColumnArithmeticWidthUnderBooleanEquality.
        // A boolean equality of two comparisons - (cmp) = (cmp) - forms a SINGLE
        // predicate context, so a sibling LONG comparison (here i32*3_000_000_000, whose
        // out-of-INT-range constant promotes to long) turns the predicate-global
        // narrow-i64 widening on for the whole predicate. The narrow-int COLUMN leaf
        // on the wrap-side comparison is already kept at i32 (i64WrapLeaves), but the
        // in-range CONSTANT operand it multiplies with (the 2 in i32*2) was still
        // widened to i64 by serializeConstant, so the JIT promoted the whole product
        // to long width and never wrapped - MulInt#getInt wraps mod 2^32 in the Java
        // filter. For (i32*3_000_000_000 > 0) = (i32*2 > 5) the Java filter returned
        // 2 rows and the JIT 4. serializeConstant now keeps an i64WrapLeaves constant
        // at i32, matching the wrap-side column, so both agree. JIT stays enabled.
        assertMemoryLeak(() -> {
            execute("create table t (i32 int, i64 long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(2_147_483_647, 1, 0)," +
                    "(2_147_483_646, 2, 1_000_000)," +
                    "(3, 3, 2_000_000)," +
                    "(null, null, 3_000_000)");
            // Previously diverging shapes: an in-range constant operand on the wrap
            // side of an INT-width comparison, paired with a LONG-width comparison.
            // Absolute pin: Java keeps 2 rows (i32=3 and the null row); the pre-fix JIT widened
            // the in-range "2" operand and returned 4.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 * 2 > 5)", true,
                    "i32\ti64\tts\n" +
                            "3\t3\t1970-01-01T00:00:02.000000Z\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("t where (i32 * 2 > 5) = (i32 * 3_000_000_000 > 0)", true); // operand order
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) <> (i32 * 2 > 5)", true);
            // The LONG-comparison side is an out-of-INT-range constant equality.
            assertJitMatchesJava("t where (i32 = 2_147_483_648) = (i32 * 2 > 5)", true);
            // Addition instead of multiplication on the wrap side.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 + 2_000_000_000 > 5)", true);
            // Mixed-size path (i64 observed too): serializeUntypedNumber keeps the 2 at i32.
            assertJitMatchesJava("t where (i32 * 3_000_000_000 > 0) = (i32 * 2 > 5) and i64 > 0", true);
            // Controls: single comparison, AND, OR each form separate predicate
            // contexts and already wrapped the constant on both paths.
            assertJitMatchesJava("t where i32 * 2 > 5", true);
            assertJitMatchesJava("t where i32 * 2 > 5 and i32 * 3_000_000_000 > 0", true);
            assertJitMatchesJava("t where i32 * 2 > 5 or i32 * 3_000_000_000 > 0", true);
        });
    }

    @Test
    public void testConstantOverflowFoldOnByteColumn() throws Exception {
        // Overflowing INT constant arithmetic must agree across JIT off/scalar/
        // vectorized. A BYTE column compares at INT width, so the JIT folds the
        // constant to a wrapped I4 IMM, matching the Java filter's getInt() wrap.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // -1.04e17 wraps to +1_699_321_072 at INT width; no BYTE exceeds it.
            assertQueryEmptyNoLeakCheck("x where i8 > -286_452 * (-952_151 * -382_988)");
            // Symmetric: +1.04e17 wraps to -1_699_321_072, below every BYTE.
            assertQueryNotNullNoLeakCheck("x where i8 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnIntColumn() throws Exception {
        // INT column compares at INT width: the JIT wraps the constant (I4) like
        // the Java filter rather than widening to i64.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as int) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no INT row exceeds it.
            assertQueryEmptyNoLeakCheck("x where i32 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every INT row.
            assertQueryNotNullNoLeakCheck("x where i32 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnLongColumn() throws Exception {
        // A LONG column does NOT change the constant's width: the fold is all-INT arithmetic, so
        // it wraps at 32 bits and the comparison promotes the already-wrapped value. This test and
        // its SHORT sibling below therefore assert the same two answers now, which is the point -
        // the peer column no longer decides what the constant evaluates to.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no i64 row (they lie in [-99, 414]) exceeds it.
            assertQueryEmptyNoLeakCheck("x where i64 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every i64 row.
            assertQueryNotNullNoLeakCheck("x where i64 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldOnShortColumn() throws Exception {
        // A SHORT column promotes to INT, so the constant wraps at INT width.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as short) i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Wraps to +1_699_321_072: no SHORT row exceeds it.
            assertQueryEmptyNoLeakCheck("x where i16 > -286_452 * (-952_151 * -382_988)");
            // Wraps to -1_699_321_072: below every SHORT row.
            assertQueryNotNullNoLeakCheck("x where i16 > 286_452 * (952_151 * 382_988)");
        });
    }

    @Test
    public void testConstantOverflowFoldVariousOps() throws Exception {
        // The fold path covers +, -, *, /, and unary minus uniformly.
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x - 100 as byte) i8," +
                " (x - 100) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // i64 values lie in [-99, 414]; the four constants all evaluate to a
            // value below every i64 row, so > matches every row and exercises
            // the fold path uniformly across all four operators.
            assertQueryNotNullNoLeakCheck("x where i64 > -5_000_000_000 + -5_000_000_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -5_000_000_000 - 5_000_000_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -100_000 * 100_000");
            assertQueryNotNullNoLeakCheck("x where i64 > -1_000_000_000_000 / 1");
            // Unary minus over an overflowing product: the inner fold wraps to -1_699_321_072 and
            // the minus makes it +1_699_321_072, which no i64 row (they lie in [-99, 414]) exceeds.
            assertQueryEmptyNoLeakCheck("x where i64 > -(286_452 * (-952_151 * -382_988))");
            // The same constant against a BYTE column gives the same answer, because the column no
            // longer decides the constant's width.
            assertQueryEmptyNoLeakCheck("x where i8 > -(286_452 * (-952_151 * -382_988))");
        });
    }

    @Test
    public void testCount() throws Exception {
        final String query = "select count() from x where price > 0 and sym = 'HBC'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testDate() throws Exception {
        final String query = "x where d1 != d2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d1," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 0) d2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testDateNull() throws Exception {
        final String query = "x where d <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_date(to_date('2020', 'yyyy'), to_date('2021', 'yyyy'), 5) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testDeepArithmeticChainMatchesJava() throws Exception {
        // M2: the JIT marker passes (markI64WidenFoldRoots / markI64WrapArithLeaves) prune a narrow-int
        // subtree instead of recomputing genuineArithType at every level. Exercise a deep arithmetic
        // spine so the pruned/short-circuited walk is taken, and confirm the JIT still matches the Java
        // filter under INT- and LONG-width comparisons and with an overflowing constant fold at the leaf.
        //
        // a and b are sized so the spine genuinely overflows int32: (a + b) * b = 101_000_000 still
        // fits, the next multiply by b does not. So the wrapped and the widened chain differ on every
        // row, and each assertion below discriminates the two. The chain wraps to a distinct negative
        // int32 per row (-449_931_736 .. -445_927_736) and widens to 101_000_001_001_000 + 1_001_000*rid.
        // Every assertion carries an absolute pin: JIT-vs-Java parity alone is not an oracle here,
        // because this PR moves the Java filter and the JIT toward the same width model - a shared
        // wrong decision would agree on both paths and pass.
        assertMemoryLeak(() -> {
            execute("create table dc as (select" +
                    " x rid," +
                    " cast(100_000 as int) a," +
                    " cast(1000 as int) b," +
                    " x::short cs," +
                    " x::byte cbyte," +
                    // nl carries the true long-width chain for rows 1 and 4, and the int32-WRAPPED
                    // image of the chain for rows 2 and 5. The chain is an all-narrow INT
                    // expression, so both paths hand a LONG comparison the wrapped image and
                    // {2, 5} is the correct answer; a path that computed the chain at long width
                    // would match {1, 4} instead.
                    " cast(case x when 1 then 101_000_001_001_000 when 2 then -448_930_736 when 3 then 0" +
                    " when 4 then 101_000_004_004_000 else -445_927_736 end as long) nl," +
                    // ni carries the int32-wrapped image of the chain for rows 1 and 3. A path that
                    // wrongly widens under an INT comparison matches nothing.
                    " cast(case x when 1 then -449_931_736 when 2 then 0 when 3 then -447_929_736" +
                    " when 4 then 1 else 0 end as int) ni," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Deep all-narrow-int spine read at INT width: every node is narrow, so the marker walk
            // prunes at the root; the JIT must still wrap (getInt) exactly like the Java filter. The
            // wrapped chain is negative on every row, so < 0 keeps all five and > 0 keeps none; a path
            // that widened instead would invert both.
            assertJitMatchesJava("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) < 0", true,
                    "rid\n1\n2\n3\n4\n5\n");
            assertJitMatchesJavaOnEmptyResult("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) > 0", true,
                    "rid\n");
            // Same spine against an INT column holding the wrapped image: pins the exact wrapped value.
            assertJitMatchesJava("select rid from dc where ni = ((((((a + b) * b) + cs) * b) + cbyte) * b)", true,
                    "rid\n1\n3\n");
            // Same spine compared against a LONG column: the comparison runs at long width, but the
            // spine is still an all-narrow INT expression, and MulInt#getLong hands the comparison
            // the WRAPPED image on both paths. So rows 2 and 5, which hold that image, are the ones
            // that match - not rows 1 and 4, which hold the true long-width chain. The two lines
            // after it pin the same fact from the empty side: the wrapped image is negative on every
            // row, so it never exceeds nl and never reaches 101e12, while a spine computed at long
            // width would return rows on both.
            assertJitMatchesJava("select rid from dc where nl = ((((((a + b) * b) + cs) * b) + cbyte) * b)", true,
                    "rid\n2\n5\n");
            assertJitMatchesJavaOnEmptyResult("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) > nl", true,
                    "rid\n");
            assertJitMatchesJavaOnEmptyResult("select rid from dc where ((((((a + b) * b) + cs) * b) + cbyte) * b) >= 101_000_000_000_000L", true,
                    "rid\n");
            // Deep spine with an overflowing pure-constant fold at the leaf, under an INT column and
            // under a LONG one. 1_000_000 * 1_000_000 folds to an INT-typed node whose getInt()
            // wraps to -727_379_968, and IntFunction#getLong hands that same wrap on
            // (Numbers.intToLong(getInt())), so + 1 + 2 + 3 yields -727_379_962 in BOTH contexts.
            // The INT-column pin names that value directly; the LONG-column pins carry the other
            // half - 1_000_000_000_006, the value a fold read at long width would produce, matches
            // nothing, and the sum clears zero only on nl's two long-width rows.
            assertJitMatchesJava("select rid from dc where (a + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) = -727_279_962", true,
                    "rid\n1\n2\n3\n4\n5\n");
            assertJitMatchesJavaOnEmptyResult("select rid from dc where (a + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) > 0", true,
                    "rid\n");
            // nl is 0 on row 3, so the widened fold is the only value that can match there.
            assertJitMatchesJavaOnEmptyResult("select rid from dc where (nl + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) = 1_000_000_000_006L", true,
                    "rid\n");
            assertJitMatchesJava("select rid from dc where (nl + ((((1_000_000 * 1_000_000) + 1) + 2) + 3)) > 0", true,
                    "rid\n1\n4\n");
        });
    }

    @Test
    public void testNarrowConstArithFoldVectorizesAndMatchesJava() throws Exception {
        // A pure-constant INT literal chain against a 64-bit column reached the backend as its own
        // i32 operations, which no vectorized loop can pair with i64 lanes, so the serializer forced
        // the scalar backend for it: "l > 1000 * 1000" scanned one row per YMM iteration where
        // "l > 1000000" scanned four. The chain now folds to the single immediate the Java filter's
        // own constant fold produces. CompiledFilterIRSerializerTest pins the execution mode and the
        // emitted IR; this pins that the rows the re-enabled vector loop returns are the Java
        // filter's rows, wrapping folds and NULL sentinels included.
        final String ddl = "create table m3 as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " case when x = 1 then cast(null as long) else x - 100 end l," +
                " case when x = 2 then cast(null as timestamp) else cast(59_999_990 + x as timestamp) end ts," +
                " case when x = 3 then cast(null as int) else cast(16_777_215 + x as int) end i," +
                " case when x = 4 then cast(null as double) else cast(x - 100 as double) end d," +
                " case when x = 5 then cast(null as float) else cast(x - 100 as float) / 100 end f" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than inferring it from the rows: every shape below returns the same rows on the
            // scalar loop, so a regression that stopped the fold - leaving
            // forceScalarOnUnharmonisedNarrowArith to force the whole filter scalar - would leave
            // the differentials green. assertExecHint() re-runs the serializer over the same
            // metadata the code generator hands it.
            assertExecHint("m3", "l > -(1000 * 1000)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("m3", "ts > 1_000_000 * 60", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("m3", "l > 1_000_000 * 1_000_000", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("m3", "l + 1000 * 1000 > 0", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("m3", "l in (1000 * 1000, -98)", EXEC_HINT_SINGLE_SIZE_TYPE);
            // The fold removes a filter-wide forceScalarMode write, so its loss shows up beside a
            // conversion-emitting conjunct in one written order before the other. Pin both.
            assertExecHint("m3", "l > -(1000 * 1000) and i > 16777216.0", EXEC_HINT_WIDE_LANE);
            assertExecHint("m3", "i > 16777216.0 and l > -(1000 * 1000)", EXEC_HINT_WIDE_LANE);
            assertExecHint("m3", "f < 1.00000003 or l > -(1000 * 1000)", EXEC_HINT_WIDE_LANE);
            assertExecHint("m3", "l > -(1000 * 1000) or f < 1.00000003", EXEC_HINT_WIDE_LANE);
            // The DOUBLE peer never took the fold, so it only has to stay on the loop it had.
            assertExecHint("m3", "d > -(1000 * 1000)", EXEC_HINT_SINGLE_SIZE_TYPE);
            // LONG peer, the three shapes the review measured going scalar.
            assertQueryNotNullNoLeakCheck("m3 where l > -(1000 * 1000)");
            assertQueryNotNullNoLeakCheck("m3 where l > 60 * 60 * 24 - 100_000");
            assertQueryNotNullNoLeakCheck("m3 where ts > 1_000_000 * 60");
            // A chain that wraps at INT width, and one whose division makes the wrap non-modular.
            assertQueryNotNullNoLeakCheck("m3 where l > 1_000_000 * 1_000_000");
            assertQueryNotNullNoLeakCheck("m3 where l > (1_000_000 * 1_000_000) / 1_000_000");
            // The fold as an operand of a genuinely 64-bit arithmetic node, and as an IN element.
            assertQueryNotNullNoLeakCheck("m3 where l + 1000 * 1000 > 0");
            assertQueryNotNullNoLeakCheck("m3 where l in (1000 * 1000, -98)");
            // Beside a conjunct that emits a wide-lane conversion the filter runs the FOUR-LANE
            // loop, so the folded immediate has to hold there too - in both conjunct orders, since
            // the fold removes a filter-wide forceScalarMode write.
            assertQueryNotNullNoLeakCheck("m3 where l > -(1000 * 1000) and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("m3 where i > 16777216.0 and l > -(1000 * 1000)");
            assertQueryNotNullNoLeakCheck("m3 where ts > 1_000_000 * 60 and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("m3 where i > 16777216.0 and ts > 1_000_000 * 60");
            assertQueryNotNullNoLeakCheck("m3 where f < 1.00000003 or l > -(1000 * 1000)");
            assertQueryNotNullNoLeakCheck("m3 where l > -(1000 * 1000) or f < 1.00000003");
            // A DOUBLE peer never took the fold - its comparison does not run at 64-bit INT width -
            // so this one only has to stay where it was.
            assertQueryNotNullNoLeakCheck("m3 where d > -(1000 * 1000)");
        });
    }

    @Test
    public void testNarrowConstOperandOfLongArithWidensAndMatchesJava() throws Exception {
        // A genuinely 64-bit arithmetic node reads EVERY operand at 64 bits: FunctionParser
        // resolves "446_488 - 114_763L" to the (LL) factory and folds it through
        // IntConstant#getLong(). The predicate-wide type observer types a constant at the widest
        // COLUMN or BIND VARIABLE it saw (PredicateContext#handleColumn, #handleBindVariable), so
        // an all-INT-column predicate typed the narrow half down to I4 and the node reached the
        // backend as (i64 114763L)(i32 446488L)(-) - a 4-byte immediate against an 8-byte one
        // under one operator. serialize()'s areWideLaneWidthsHarmonised() assert reported it, and
        // QueryFuzzTest#testQueryFuzz reddened on seeds 274896052653843 / 1787218116926 with
        // "t.c4 >= (446_488 - 114_763L) AND 0.752714 IS NULL" over an INT c4.
        //
        // That assert is the red signal this test carries, and it runs under -ea only, which
        // core/pom.xml sets for the test JVM. Measured by disabling the fix and re-running:
        // under -ea, five of the six assertExecHint() calls below throw AssertionError from
        // areWideLaneWidthsHarmonised() - all but the negated control, which was already
        // harmonised - and under -da the whole test passes, pins and rows alike.
        //
        // So the exec-hint pins are not a revert detector on their own: getExecHint() answered
        // WIDE_LANE for these shapes before the fix too. Neither are the rows - the four-lane
        // avx2::convert() sign-extends the i32 side of an i32-with-i64 pairing (jit/avx2.h:675-679,
        // :693-697) and 446_488 - 114_763 does not overflow INT, so the answers were already right.
        // What the pins pin is that emitting the immediate at I8 leaves these shapes on the
        // four-lane loop rather than demoting them to SCALAR, and the row assertions pin the value
        // semantics the widening must not move: sign extension is value-preserving, so the bound
        // stays 331_725 and no row crosses it.
        //
        // The emitted WIDTH - what the fix actually changes - is pinned without -ea by
        // CompiledFilterIRSerializerTest#testNarrowConstOperandOfLongArithWidensToI64, which reads
        // (i32 446488L) where it expects (i64 446488L) on a revert, and its consequence for a
        // BYTE / SHORT observation by testNarrowConstOperandOfLongArithCompilesUnderNarrowObservation
        // below.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wlnc (k TIMESTAMP, i32 INT) TIMESTAMP(k) PARTITION BY DAY");
            // 16 rows, so the four-lane loop runs four full iterations, straddling both bounds
            // the queries below use (331_725 and its double, 663_450) and carrying NULL and both
            // INT extremes.
            execute("""
                    INSERT INTO wlnc VALUES
                      (0, NULL),
                      (1_000_000, 0),
                      (2_000_000, 5),
                      (3_000_000, -1),
                      (4_000_000, 331_724),
                      (5_000_000, 331_725),
                      (6_000_000, 331_726),
                      (7_000_000, -331_725),
                      (8_000_000, 663_449),
                      (9_000_000, 663_450),
                      (10_000_000, 663_451),
                      (11_000_000, 2_147_483_647),
                      (12_000_000, -2_147_483_647),
                      (13_000_000, 100_000),
                      (14_000_000, 500_000),
                      (15_000_000, 1)
                    """);
            // The shape the fuzzer found, plus the spellings that reach the same marker by other
            // routes. Every one of these threw the AssertionError before the fix.
            assertExecHint("wlnc", "i32 >= (446_488 - 114_763L)", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlnc", "i32 >= (446_488 - 114_763L) * 2", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlnc", "i32 in (446_488 - 114_763L, 5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlnc", "i32 >= (446_488 - 114_763L) or i32 < 3", EXEC_HINT_WIDE_LANE);
            // A zero divisor at LONG width is deliberately not folded - the native int64_div is
            // what reproduces DivLong's NULL - so the operations reach the backend and their
            // widths still have to agree. A fix that only extended the constant fold would have
            // left this one asserting.
            assertExecHint("wlnc", "i32 >= (446_488 / 0L)", EXEC_HINT_WIDE_LANE);
            // Control: the negated spelling of the same operand was already emitted at 8 bytes,
            // through forceScalarOnUnharmonisedNarrowArith's fold.
            assertExecHint("wlnc", "i32 >= (-446_488 - 114_763L)", EXEC_HINT_WIDE_LANE);

            assertJitScalarAndVectorMatchJava(
                    "wlnc where i32 >= (446_488 - 114_763L)",
                    "k\ti32\n" +
                            "1970-01-01T00:00:05.000000Z\t331725\n" +
                            "1970-01-01T00:00:06.000000Z\t331726\n" +
                            "1970-01-01T00:00:08.000000Z\t663449\n" +
                            "1970-01-01T00:00:09.000000Z\t663450\n" +
                            "1970-01-01T00:00:10.000000Z\t663451\n" +
                            "1970-01-01T00:00:11.000000Z\t2147483647\n" +
                            "1970-01-01T00:00:14.000000Z\t500000\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "wlnc where i32 >= (446_488 - 114_763L) * 2",
                    "k\ti32\n" +
                            "1970-01-01T00:00:09.000000Z\t663450\n" +
                            "1970-01-01T00:00:10.000000Z\t663451\n" +
                            "1970-01-01T00:00:11.000000Z\t2147483647\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "wlnc where i32 in (446_488 - 114_763L, 5)",
                    "k\ti32\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n" +
                            "1970-01-01T00:00:05.000000Z\t331725\n"
            );
            // -446_488 - 114_763 is -561_251, which only the two negative rows and NULL fall below.
            assertJitScalarAndVectorMatchJava(
                    "wlnc where i32 < (-446_488 - 114_763L)",
                    "k\ti32\n" +
                            "1970-01-01T00:00:12.000000Z\t-2147483647\n"
            );
            // The declined-fold shape end to end: int64_div answers LONG_NULL for a zero divisor,
            // exactly as DivLong does. The NULL row is the only match, because IntColumn#getLong
            // reads it as LONG_NULL too and LONG_NULL >= LONG_NULL holds on both engines.
            assertJitScalarAndVectorMatchJava(
                    "wlnc where i32 >= (446_488 / 0L)",
                    "k\ti32\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\n"
            );
        });
    }

    @Test
    public void testNarrowConstOperandOfLongArithCompilesUnderNarrowObservation() throws Exception {
        // The same widening, in a predicate whose widest observed source is a BYTE or SHORT column
        // rather than an INT one. There it changed which width the immediate was emitted at; here
        // it changes whether the filter compiles at all, which is a behaviour change of its own.
        //
        // markWidthSemanticsOperand's I8 arm puts the narrow constant operand in i64WidenConstants,
        // and serializeConstant emits it at I8 instead of at the observed I1 / I2. Without that arm
        // the constant reaches serializeNumber's I1 case, which range-checks the literal against
        // Byte.MIN_VALUE / Byte.MAX_VALUE and throws "byte literal out of range" (I2 throws "short
        // literal out of range"). That SqlException aborts JIT compilation and SqlCodeGenerator
        // runs the Java filter, so the filter used to DECLINE and now compiles -
        // assertJitScalarAndVectorMatchJava's "JIT was not enabled for query" assertion is what
        // reddens on a revert of the arm.
        //
        // It compiles SCALAR rather than vectorized: the predicate carries arithmetic and the
        // observer reports a one- or two-byte width, which is a forceScalarMode term of its own in
        // visit(). The rows are the other half of the pin - a filter that newly compiles and
        // answers wrongly is worse than one that declines - so every shape below carries an
        // absolute result as well as JIT-vs-Java parity.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nbo (k TIMESTAMP, i8 BYTE, i16 SHORT) TIMESTAMP(k) PARTITION BY DAY");
            // 1_000_000 - 999_900L is 100 at LONG width: each operand is out of BYTE and SHORT
            // range, the bound is inside both. The rows straddle it and carry both narrow extremes.
            execute("""
                    INSERT INTO nbo VALUES
                      (0, -128, -32_768),
                      (1_000_000, -1, -1),
                      (2_000_000, 0, 0),
                      (3_000_000, 99, 99),
                      (4_000_000, 100, 100),
                      (5_000_000, 101, 101),
                      (6_000_000, 127, 32_767),
                      (7_000_000, 5, 5)
                    """);
            // The rows first: assertExecHint() calls serialize() directly, so it would report the
            // SqlException itself and hide what the filter does in production.
            assertJitScalarAndVectorMatchJava(
                    "select k, i8 from nbo where i8 >= (1_000_000 - 999_900L)",
                    "k\ti8\n" +
                            "1970-01-01T00:00:04.000000Z\t100\n" +
                            "1970-01-01T00:00:05.000000Z\t101\n" +
                            "1970-01-01T00:00:06.000000Z\t127\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i16 from nbo where i16 >= (1_000_000 - 999_900L)",
                    "k\ti16\n" +
                            "1970-01-01T00:00:04.000000Z\t100\n" +
                            "1970-01-01T00:00:05.000000Z\t101\n" +
                            "1970-01-01T00:00:06.000000Z\t32767\n"
            );
            // Sign extension is value-preserving in the other direction too, so a negative bound
            // keeps exactly the rows below it - the BYTE minimum included.
            assertJitScalarAndVectorMatchJava(
                    "select k, i8 from nbo where i8 < (999_900L - 1_000_000)",
                    "k\ti8\n" +
                            "1970-01-01T00:00:00.000000Z\t-128\n"
            );
            // Control: a bare bound is not an arithmetic operand of a 64-bit node, so it keeps the
            // observed I1 emission and this shape compiled before the arm existed too.
            assertJitScalarAndVectorMatchJava(
                    "select k, i8 from nbo where i8 >= 100",
                    "k\ti8\n" +
                            "1970-01-01T00:00:04.000000Z\t100\n" +
                            "1970-01-01T00:00:05.000000Z\t101\n" +
                            "1970-01-01T00:00:06.000000Z\t127\n"
            );
            // And the loop it compiles onto, which the rows above cannot see: they agree on every
            // mode, so a change that moved this shape between backends would leave them green.
            assertExecHint("nbo", "i8 >= (1_000_000 - 999_900L)", EXEC_HINT_SCALAR);
            assertExecHint("nbo", "i16 >= (1_000_000 - 999_900L)", EXEC_HINT_SCALAR);
        });
    }

    @Test
    public void testNarrowConstArithFoldPinsBoundaryRows() throws Exception {
        // Parity across three modes proves the modes agree; it cannot prove the shared answer is
        // right. These pin the absolute rows against a Java oracle first. The wrapping chain is the
        // decisive one: folded at 64-bit width its bound would be 1_000_000_000_000 and every row
        // would drop, and folded without per-operation wrapping the division below would answer
        // 1000000 instead of -727.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m3p (k TIMESTAMP, l LONG, ts TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO m3p VALUES
                        (0, NULL, NULL),
                        (1_000_000, 999_999, 59_999_999),
                        (2_000_000, 1_000_000, 60_000_000),
                        (3_000_000, 1_000_001, 60_000_001),
                        (4_000_000, -727_379_969, 0),
                        (5_000_000, -727_379_967, 0)
                    """);
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l > 1000 * 1000",
                    "k\tl\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l = 1000 * 1000",
                    "k\tl\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n"
            );
            // The NULL row is admitted by <> exactly as it is against a plain literal bound.
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l <> 1000 * 1000",
                    "k\tl\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n" +
                            "1970-01-01T00:00:04.000000Z\t-727379969\n" +
                            "1970-01-01T00:00:05.000000Z\t-727379967\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l > 60 * 60 * 24",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
            // 1000000 * 1000000 wraps to -727379968 at INT width, which is the bound the Java
            // filter's IntConstant carries.
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l > 1_000_000 * 1_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n" +
                            "1970-01-01T00:00:05.000000Z\t-727379967\n"
            );
            // -727379968 / 1000000 is -727. A single wrap applied after a 64-bit fold would answer
            // 1000000 and drop the first two rows.
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l > (1_000_000 * 1_000_000) / 1_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l > -(1000 * 1000)",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l + 1000 * 1000 > 0",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3p where l in (1000 * 1000, 999_999)",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t999999\n" +
                            "1970-01-01T00:00:02.000000Z\t1000000\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k from m3p where ts > 1_000_000 * 60",
                    "k\n" +
                            "1970-01-01T00:00:03.000000Z\n"
            );

            // A DATE column codes as I8 like a LONG one, but serializeConstant declines an
            // unquoted numeric constant against it, so "d > 3600000" falls back to the Java filter
            // while the folded chain does not - the fold emits its immediate directly, exactly as
            // the pre-existing out-of-INT-range fold root already does for the same column type.
            // That widens the JIT-eligible set, so pin the rows rather than assume them.
            execute("CREATE TABLE m3d (k TIMESTAMP, d DATE) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO m3d VALUES
                        (0, NULL),
                        (1_000_000, cast(3_599_999 AS DATE)),
                        (2_000_000, cast(3_600_000 AS DATE)),
                        (3_000_000, cast(3_600_001 AS DATE))
                    """);
            assertJitScalarAndVectorMatchJava(
                    "select k from m3d where d > 1000 * 60 * 60",
                    "k\n" +
                            "1970-01-01T00:00:03.000000Z\n"
            );

            // The serializer reads column metadata and a page-frame cursor, both of which a WAL
            // table presents exactly as a non-WAL one does once the apply job has run.
            execute("CREATE TABLE m3w (k TIMESTAMP, l LONG) TIMESTAMP(k) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO m3w VALUES
                        (0, NULL),
                        (1_000_000, 999_999),
                        (2_000_000, 1_000_000),
                        (3_000_000, 1_000_001)
                    """);
            drainWalQueue();
            assertJitScalarAndVectorMatchJava(
                    "select k, l from m3w where l > 1000 * 1000",
                    "k\tl\n" +
                            "1970-01-01T00:00:03.000000Z\t1000001\n"
            );
        });
    }

    @Test
    public void testEightByteArithI64ConstantVectorizesAndMatchesJava() throws Exception {
        // markWidthSemanticsOperand widens an out-of-INT-range integer constant under an arithmetic
        // node to a full I8 IMM. It used to record that in the i64-widen LEAF set, whose only other
        // reader forces the scalar backend, so every LONG / TIMESTAMP / DOUBLE predicate carrying
        // such a constant dropped from four rows per YMM iteration to one - with byte-identical IR,
        // since an 8-byte observation emits the immediate at I8 either way. The widening is now
        // recorded as an immediate width only, and the scalar force is kept for the case it is
        // load-bearing for: a predicate whose lanes are narrower than the immediate.
        // CompiledFilterIRSerializerTest pins the execution mode; this pins that the rows the
        // re-enabled vector loop returns are the Java filter's rows, NULL sentinels included.
        final String ddl = "create table c2 as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " case when x = 1 then cast(null as timestamp) else cast(4_999_999_996 + x as timestamp) end ts," +
                " case when x = 2 then cast(null as long) else -5_000_000_003 + x end l," +
                " case when x = 3 then cast(null as double) else cast(x - 100 as double) end d," +
                " case when x = 4 then cast(null as long) else x - 100 end ls," +
                " case when x = 5 then cast(null as float) else cast(x - 100 as float) / 100 end f," +
                " cast(16_777_215 + x as int) i" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than inferring it from the rows: the IR is byte-identical either way and every shape
            // below returns the same rows on the scalar loop, so a regression that put the widened
            // immediate back in the i64-widen LEAF set - whose only other reader forces the scalar
            // backend - would leave the differentials green. assertExecHint() re-runs the
            // serializer over the same metadata the code generator hands it.
            assertExecHint("c2", "ts - 5_000_000_000 > 0", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("c2", "l > -5_000_000_000", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("c2", "l = -5_000_000_003 + 1", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("c2", "d * 2_147_483_648 > 0", EXEC_HINT_SINGLE_SIZE_TYPE);
            // The four-lane shapes, in both conjunct orders: the leaf-set recording read a
            // filter-wide flag mid-traversal, so it forced scalar in one written order only.
            assertExecHint("c2", "ls * 5_000_000_000 > 0 and i > 16777216.0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2", "i > 16777216.0 and ls * 5_000_000_000 > 0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2", "i > 16777216.0 and d * 5_000_000_000 > 0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2", "f < 1.00000003 or l + 5_000_000_000 > 0", EXEC_HINT_WIDE_LANE);
            // TIMESTAMP peer: the observer reports 8 bytes, so the immediate needs no widening at
            // all and the IR is unchanged.
            assertQueryNotNullNoLeakCheck("c2 where ts - 5_000_000_000 > 0");
            assertQueryNotNullNoLeakCheck("c2 where ts + 5_000_000_000 > 0");
            // LONG peer, unary-minus spelling: the marked node sits under the minus and descend()
            // folds it into the negation node, so it is never serialized on its own.
            assertQueryNotNullNoLeakCheck("c2 where l > -5_000_000_000");
            assertQueryNotNullNoLeakCheck("c2 where l <> -5_000_000_000");
            assertQueryNotNullNoLeakCheck("c2 where l = -5_000_000_003 + 1");
            // DOUBLE peer: here the widening does change the immediate - an exact f64 becomes an
            // exact i64 of the same width - so the (f64, i64) pairing has to survive the 8-byte
            // lanes as well as the scalar backend.
            assertQueryNotNullNoLeakCheck("c2 where d * 2_147_483_648 > 0");
            assertQueryNotNullNoLeakCheck("c2 where d - 2_147_483_648 < 0");
            // The conjunct-order pair. Both spellings return the same rows and now pick the same
            // execution mode; a test written in one order alone could not catch the other.
            assertQueryNotNullNoLeakCheck("c2 where ls * 5_000_000_000 > 0 and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("c2 where i > 16777216.0 and ls * 5_000_000_000 > 0");
            // The shapes the relaxed gate hands to the FOUR-LANE loop, which neither the PR nor its
            // base ran them on: an 8-byte arithmetic predicate beside a conjunct that emits a
            // wide-lane conversion. Every operand pairing here - (i64, i64), (i32 -> sx_i64, f64)
            // and (f32 -> cvt_ftod, f64) - has to hold across a full scan.
            assertQueryNotNullNoLeakCheck("c2 where i > 16777216.0 and d * 5_000_000_000 > 0");
            assertQueryNotNullNoLeakCheck("c2 where i > 16777216.0 and ts * 5_000_000_000 > 0");
            assertQueryNotNullNoLeakCheck("c2 where f < 1.00000003 or l + 5_000_000_000 > 0");
            assertQueryNotNullNoLeakCheck("c2 where f < 1.00000003 or d + 5_000_000_000 > 0");
            assertQueryNotNullNoLeakCheck("c2 where f < 1.00000003 or ts + 5_000_000_000 > 0");
        });
    }

    @Test
    public void testEightByteArithI64ConstantPinsBoundaryRows() throws Exception {
        // Parity across three modes proves the modes agree; it cannot prove the shared answer is
        // right. These pin the absolute rows on and around the widened bound, with the LONG NULL
        // sentinel (Long.MIN_VALUE) sitting below every one of them - it must never be admitted by
        // an ordering comparison against a negative out-of-INT-range bound.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE c2b AS (
                        SELECT timestamp_sequence(0, 1_000_000) k,
                            CASE WHEN x = 1 THEN cast(NULL AS LONG) ELSE -5_000_000_002 + x END l,
                            CASE WHEN x = 1 THEN cast(NULL AS DOUBLE) ELSE cast(x - 3 AS DOUBLE) END d
                        FROM long_sequence(5)
                    ) TIMESTAMP(k)
                    """);
            assertJitScalarAndVectorMatchJava(
                    "select k, l from c2b where l = -5_000_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:01.000000Z\t-5000000000\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from c2b where l > -5_000_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:02.000000Z\t-4999999999\n" +
                            "1970-01-01T00:00:03.000000Z\t-4999999998\n" +
                            "1970-01-01T00:00:04.000000Z\t-4999999997\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, l from c2b where l <> -5_000_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:00.000000Z\tnull\n" +
                            "1970-01-01T00:00:02.000000Z\t-4999999999\n" +
                            "1970-01-01T00:00:03.000000Z\t-4999999998\n" +
                            "1970-01-01T00:00:04.000000Z\t-4999999997\n"
            );
            // DOUBLE peer, both signs of the product, with the NaN NULL row excluded by both.
            assertJitScalarAndVectorMatchJava(
                    "select k, d from c2b where d * 2_147_483_648 > 0",
                    "k\td\n" +
                            "1970-01-01T00:00:03.000000Z\t1.0\n" +
                            "1970-01-01T00:00:04.000000Z\t2.0\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, d from c2b where d * 2_147_483_648 < 0",
                    "k\td\n" +
                            "1970-01-01T00:00:01.000000Z\t-1.0\n"
            );

            // The four-lane shapes the relaxed gate newly admits. Neither the PR nor its base ran
            // these on the AVX2 loop, so parity alone is not enough - pin the rows. Each mixes an
            // 8-byte arithmetic predicate with a conjunct that emits a wide-lane conversion, so the
            // loop has to harmonise (i64, i64) beside (f32 -> cvt_ftod, f64) and
            // (i32 -> sx_i64, f64) in one pass.
            execute("CREATE TABLE c2f (k TIMESTAMP, f FLOAT, l LONG, d DOUBLE) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO c2f VALUES
                        (0, 0.5, -6_000_000_000, -6.0E9),
                        (1_000_000, 2.0, -6_000_000_000, -6.0E9),
                        (2_000_000, 2.0, -4_000_000_000, -4.0E9),
                        (3_000_000, NULL, -6_000_000_000, -6.0E9),
                        (4_000_000, 0.5, NULL, NULL)
                    """);
            final String orExpected = "k\tf\n" +
                    "1970-01-01T00:00:00.000000Z\t0.5\n" +
                    "1970-01-01T00:00:02.000000Z\t2.0\n" +
                    "1970-01-01T00:00:04.000000Z\t0.5\n";
            assertJitScalarAndVectorMatchJava(
                    "select k, f from c2f where f < 1.00000003 or l + 5_000_000_000 > 0", orExpected);
            assertJitScalarAndVectorMatchJava(
                    "select k, f from c2f where f < 1.00000003 or d + 5_000_000_000 > 0", orExpected);

            execute("CREATE TABLE c2i (k TIMESTAMP, i INT, l LONG) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO c2i VALUES
                        (0, 16_777_216, 1),
                        (1_000_000, 16_777_217, 1),
                        (2_000_000, 16_777_218, -1),
                        (3_000_000, 16_777_219, 0),
                        (4_000_000, NULL, 1),
                        (5_000_000, 16_777_220, NULL)
                    """);
            assertJitScalarAndVectorMatchJava(
                    "select k, i from c2i where i > 16777216.0 and l * 5_000_000_000 > 0",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n"
            );

            // The serializer reads column metadata and a page-frame cursor, both of which a WAL
            // table presents exactly as a non-WAL one does once the apply job has run. Pin that
            // rather than assume it: the same bound over a WAL table must select the same rows in
            // all three modes.
            execute("CREATE TABLE c2w (k TIMESTAMP, l LONG) TIMESTAMP(k) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO c2w VALUES
                        (0, NULL),
                        (1_000_000, -5_000_000_000),
                        (2_000_000, -4_999_999_999),
                        (3_000_000, -4_999_999_998)
                    """);
            drainWalQueue();
            assertJitScalarAndVectorMatchJava(
                    "select k, l from c2w where l > -5_000_000_000",
                    "k\tl\n" +
                            "1970-01-01T00:00:02.000000Z\t-4999999999\n" +
                            "1970-01-01T00:00:03.000000Z\t-4999999998\n"
            );
        });
    }

    @Test
    public void testFloatArithI64ConstantVectorizesInBothConjunctOrders() throws Exception {
        // A four-byte FLOAT predicate carrying an out-of-INT-range integer constant emits an
        // eight-byte immediate, so it must stay off any loop whose lanes are narrower than that.
        // The four-lane loop is not one of them - its lanes are eight bytes wide whatever the
        // observed columns are - so beside a conjunct that emits a wide-lane conversion the
        // predicate is welcome to it. Whether the filter reaches that loop is settled only when
        // the traversal ends, so the serializer defers the suppression to getExecHint(); before
        // that, the spelling with the FLOAT conjunct written LAST forced the whole filter scalar,
        // and the two spellings ran different loops over the same rows.
        // CompiledFilterIRSerializerTest pins the execution mode. This pins that both spellings
        // return the Java filter's rows on the loop they now share, NULL sentinels included.
        final String ddl = "create table c2fo as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " case when x = 5 then cast(null as float) else cast(x - 100 as float) / 100 end f," +
                " case when x = 4 then cast(null as int) else cast(16_777_215 + x as int) end i," +
                " case when x = 2 then cast(null as long) else 16_777_216 + x end l" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            // Every peer below emits a wide-lane conversion of its own, so the filter runs the
            // four-lane loop and the FLOAT conjunct rides it in either spelling. The AND peers are
            // the ones this data leaves rows for; the two that select nothing under AND are run
            // under OR instead, which the same deferral covers.
            final String[] andPeers = {
                    "i > 16777216.0",
                    "i < 5_000_000_000",
                    "i < l",
                    "l * i > 5_000_000_000",
                    "i + 5_000_000_000 > 1",
                    "f < 1.00000003",
            };
            final String[] orPeers = {
                    "i < 1.00000003",
                    "i in (1, 5_000_000_000)",
            };
            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than inferring it from the rows: the two spellings return the same rows on either
            // loop, which is exactly why the order dependence went unnoticed. assertExecHint()
            // re-runs the serializer over the same metadata the code generator hands it.
            for (String peer : andPeers) {
                assertExecHint("c2fo", "f + 5_000_000_000 > 1.5 and " + peer, EXEC_HINT_WIDE_LANE);
                assertExecHint("c2fo", peer + " and f + 5_000_000_000 > 1.5", EXEC_HINT_WIDE_LANE);
                assertQueryNotNullNoLeakCheck("c2fo where f + 5_000_000_000 > 1.5 and " + peer);
                assertQueryNotNullNoLeakCheck("c2fo where " + peer + " and f + 5_000_000_000 > 1.5");
            }
            for (String peer : orPeers) {
                assertExecHint("c2fo", "f + 5_000_000_000 > 1.5 or " + peer, EXEC_HINT_WIDE_LANE);
                assertExecHint("c2fo", peer + " or f + 5_000_000_000 > 1.5", EXEC_HINT_WIDE_LANE);
                assertQueryNotNullNoLeakCheck("c2fo where f + 5_000_000_000 > 1.5 or " + peer);
                assertQueryNotNullNoLeakCheck("c2fo where " + peer + " or f + 5_000_000_000 > 1.5");
            }
            // The other arithmetic spellings of the same shape.
            assertExecHint("c2fo", "f * 5_000_000_000 > 1.5 and i > 16777216.0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2fo", "i > 16777216.0 and f * 5_000_000_000 > 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2fo", "f - 5_000_000_000 < 1.5 and i > 16777216.0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2fo", "i > 16777216.0 and f - 5_000_000_000 < 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2fo", "f + 2_147_483_648 > 1.5 and i > 16777216.0", EXEC_HINT_WIDE_LANE);
            assertExecHint("c2fo", "i > 16777216.0 and f + 2_147_483_648 > 1.5", EXEC_HINT_WIDE_LANE);
            assertQueryNotNullNoLeakCheck("c2fo where f * 5_000_000_000 > 1.5 and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("c2fo where i > 16777216.0 and f * 5_000_000_000 > 1.5");
            assertQueryNotNullNoLeakCheck("c2fo where f - 5_000_000_000 < 1.5 and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("c2fo where i > 16777216.0 and f - 5_000_000_000 < 1.5");
            assertQueryNotNullNoLeakCheck("c2fo where f + 2_147_483_648 > 1.5 and i > 16777216.0");
            assertQueryNotNullNoLeakCheck("c2fo where i > 16777216.0 and f + 2_147_483_648 > 1.5");

            // Parity across three modes proves the modes agree; it cannot prove the shared answer
            // is right. Pin the absolute rows, in both spellings, with a FLOAT NULL (NaN), an INT
            // NULL and a LONG NULL row present.
            execute("CREATE TABLE c2fp (k TIMESTAMP, f FLOAT, i INT, l LONG) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO c2fp VALUES
                        (0, 0.5, 16_777_215, 16_777_216),
                        (1_000_000, 2.0, 16_777_217, 16_777_218),
                        (2_000_000, NULL, 16_777_218, 16_777_219),
                        (3_000_000, -1.5, NULL, 16_777_220),
                        (4_000_000, 0.5, 16_777_220, NULL)
                    """);
            // f + 5e9 exceeds 1.5 for every non-NULL f, so the INT bound decides: the NaN row and
            // the NULL-INT row drop out, and 16_777_215 is below the bound.
            final String intBoundExpected = "k\tf\ti\n" +
                    "1970-01-01T00:00:01.000000Z\t2.0\t16777217\n" +
                    "1970-01-01T00:00:04.000000Z\t0.5\t16777220\n";
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fp where f + 5_000_000_000 > 1.5 and i > 16777216.0", intBoundExpected);
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fp where i > 16777216.0 and f + 5_000_000_000 > 1.5", intBoundExpected);
            // The product spelling drops the negative FLOAT row as well, which the bound above
            // keeps, so the two are not the same assertion twice.
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fp where f * 5_000_000_000 > 1.5 and i < l", "k\tf\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t0.5\t16777215\n" +
                            "1970-01-01T00:00:01.000000Z\t2.0\t16777217\n");
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fp where i < l and f * 5_000_000_000 > 1.5", "k\tf\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t0.5\t16777215\n" +
                            "1970-01-01T00:00:01.000000Z\t2.0\t16777217\n");

            // A WAL table presents the same column metadata and page-frame cursor once the apply
            // job has run, so both spellings must land on the same loop and the same rows there too.
            execute("CREATE TABLE c2fw (k TIMESTAMP, f FLOAT, i INT) TIMESTAMP(k) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO c2fw VALUES
                        (0, 0.5, 16_777_215),
                        (1_000_000, 2.0, 16_777_217),
                        (2_000_000, NULL, 16_777_218),
                        (3_000_000, 0.5, NULL)
                    """);
            drainWalQueue();
            final String walExpected = "k\tf\ti\n" +
                    "1970-01-01T00:00:01.000000Z\t2.0\t16777217\n";
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fw where f + 5_000_000_000 > 1.5 and i > 16777216.0", walExpected);
            assertJitScalarAndVectorMatchJava(
                    "select k, f, i from c2fw where i > 16777216.0 and f + 5_000_000_000 > 1.5", walExpected);
        });
    }

    @Test
    public void testFloatArithI64ConstUnderWideLaneRunsFourLaneLoop() throws Exception {
        // C6: a wide-lane FLOAT conjunct (afloat * 1.0 > <inexact float>) sets isWideLaneMode,
        // which used to SUPPRESS the scalar force for a sibling afloat + <out-of-INT constant>.
        // That left the out-of-range LONG constant as an 8-byte immediate riding a SINGLE_SIZE
        // (4-byte-lane) loop - an IR/hint mismatch.
        //
        // The C6 fix settled this shape on the SCALAR loop, because the wide-lane conjunct set the
        // mode without emitting any width conversion and getExecHint() demands both.
        // isNarrowLaneDoubleConstArith then made the DOUBLE literal under the f32 arithmetic node a
        // conversion source of its own, so all three filters below now compile at
        // EXEC_HINT_WIDE_LANE, and no mismatch remains either way: the four-lane loop's lanes are
        // eight bytes wide, which is what the widened immediate needs. The IR is unchanged by that
        // move - only the hint is. The assertExecHint() calls below pin the hint; the differentials
        // pin that all three modes agree with the Java filter.
        assertMemoryLeak(() -> {
            execute("create table t (afloat float, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(0.5, 0)," +
                    "(1.0, 1_000_000)," +
                    "(1.5, 2_000_000)," +
                    "(2.0, 3_000_000)," +
                    "(-3.0, 4_000_000)," +
                    "(5.0, 5_000_000)," +
                    "(null, 6_000_000)");
            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than inferring it from the rows: all three filters return the same rows on the
            // scalar loop, so a regression back to scalar would leave the differentials green.
            // assertExecHint() re-runs the serializer over the same metadata the code generator
            // hands it.
            assertExecHint("t", "afloat * 1.0 > 1.00000003 and afloat + 5_000_000_000 > 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("t", "afloat + 5_000_000_000 > 1.5 and afloat * 1.0 > 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("t", "afloat * 1.0 > 1.00000003 and afloat - 5_000_000_000 < -1.5", EXEC_HINT_WIDE_LANE);
            // afloat * 1.0 > 1.00000003 keeps 1.5, 2.0, 5.0; afloat + 5e9 > 1.5 is true for every
            // non-null row, so the AND keeps exactly those three.
            final String expected = "afloat\tts\n" +
                    "1.5\t1970-01-01T00:00:02.000000Z\n" +
                    "2.0\t1970-01-01T00:00:03.000000Z\n" +
                    "5.0\t1970-01-01T00:00:05.000000Z\n";
            assertJitScalarAndVectorMatchJava("t where afloat * 1.0 > 1.00000003 and afloat + 5_000_000_000 > 1.5", expected);
            // Reversed conjunct order is identical.
            assertJitScalarAndVectorMatchJava("t where afloat + 5_000_000_000 > 1.5 and afloat * 1.0 > 1.00000003", expected);
            // Subtraction of an out-of-range constant is the same shape.
            assertJitScalarAndVectorMatchJava("t where afloat * 1.0 > 1.00000003 and afloat - 5_000_000_000 < -1.5", expected);
        });
    }

    @Test
    public void testFloatArithI64LeafAlongsideIntWidenStaysCorrect() throws Exception {
        // C6 companion: hasEmittedWideLaneConversion is a single filter-wide flag, so an int-widen
        // conjunct (anint < along, which emits SX_I64 and sets the flag) sitting next to an
        // unconverted afloat + <out-of-INT constant> leaf can keep the filter on the WIDE_LANE
        // hint. This pins that such mixes still match the Java filter across a full 600-row scan
        // (many SIMD batches), so a future change to the width gating that reintroduced a lane
        // mismatch here would redden. Parity across many rows; the filters keep every row, so the
        // comparison is never vacuous.
        assertMemoryLeak(() -> {
            execute("create table t as (select cast(x as int) anint, (x * 2)::long along, " +
                    "(cast(x as float) - 3.5) afloat, timestamp_sequence(0, 1_000_000) k " +
                    "from long_sequence(600)) timestamp(k)");
            assertJitMatchesJava("t where anint < along and afloat + 5_000_000_000 > 1.5", true, null);
            assertJitMatchesJava("t where afloat + 5_000_000_000 > 1.5 and anint < along", true, null);
            assertJitMatchesJava("t where anint < 5_000_000_000 and afloat + 5_000_000_000 > 1.5", true, null);
            assertJitMatchesJava("t where anint < along and afloat * 1.0 > 1.5 and afloat + 5_000_000_000 > 1.5", true, null);
        });
    }

    @Test
    public void testDoubleConstantInFourByteArithmeticRunsFourLaneLoop() throws Exception {
        // A DOUBLE literal under a FLOAT-column arithmetic node reaches the backend as an 8-byte
        // immediate, because the Java filter evaluates the node at f64. That immediate used to
        // drop the whole filter - co-conjuncts included - onto the scalar backend: a vectorized
        // loop stepping eight 32-bit lanes cannot carry it. The four-lane loop can, its lanes
        // being eight bytes wide whatever the observed columns are, and avx2::convert() promotes
        // the f32 operand through cvt_ftod there.
        //
        // testDoubleConstantInFourByteArithmeticMatchesJava already pins what these shapes return,
        // and now runs the four-lane loop rather than the scalar one, so it re-covers 2^24, NULL,
        // negatives and every operator under the new mode for free. The fixture below adds the
        // magnitudes at which an f32 product OVERFLOWS - 3.0E38 * 2.0 and 2.0E38 * 2.0 are finite
        // at f64 and +Inf at f32. Note that no predicate here can turn that overflow into a row
        // difference: the only shape that does is one multiplying BOTH sides (Inf > Inf is false
        // where 6.0E38 > 4.0E38 is true), and this fixture has only one FLOAT column. What the rows
        // below pin is that the shapes stay correct at those magnitudes, not the width they ran
        // at; the assertExecHint() calls pin the width. Exact representability in f32 is still not
        // on its own a licence to emit the literal at f32 - CompiledFilterIRSerializer.isDoubleConst
        // names the rounding case, 16777216.0f + 1.0f against (double) 16777216.0f + 1.0.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dcwl (k TIMESTAMP, f32 FLOAT, i32 INT) TIMESTAMP(k) PARTITION BY DAY
                    """);
            // 16 rows, so the four-lane loop runs four full iterations, and the values straddle
            // the boundaries the width decision turns on: NULL in both columns, negatives, 2^24
            // and its neighbours, and two magnitudes whose f32 double is infinite. 1.0E-38 is the
            // opposite end: its double differs from it by 1e-38, which Numbers.DOUBLE_TOLERANCE
            // (1e-10) swallows, so "f32 * 2.0 > f32" rejects that row.
            execute("""
                    INSERT INTO dcwl VALUES
                      ('1970-01-01T00:00:00.000000Z', NULL, NULL),
                      ('1970-01-01T00:00:01.000000Z', 0.0, 0),
                      ('1970-01-01T00:00:02.000000Z', 0.75, 10),
                      ('1970-01-01T00:00:03.000000Z', 1.0, -3),
                      ('1970-01-01T00:00:04.000000Z', -2.5, 7),
                      ('1970-01-01T00:00:05.000000Z', 16777215.0, 16_777_215),
                      ('1970-01-01T00:00:06.000000Z', 16777216.0, 16_777_216),
                      ('1970-01-01T00:00:07.000000Z', 16777217.0, 16_777_217),
                      ('1970-01-01T00:00:08.000000Z', -16777216.0, -16_777_216),
                      ('1970-01-01T00:00:09.000000Z', 3.0E38, 2_147_483_646),
                      ('1970-01-01T00:00:10.000000Z', 1.0E-38, -100),
                      ('1970-01-01T00:00:11.000000Z', 0.1, 6),
                      ('1970-01-01T00:00:12.000000Z', 8388608.5, 8),
                      ('1970-01-01T00:00:13.000000Z', 2.0E38, 12),
                      ('1970-01-01T00:00:14.000000Z', -1.0, 20),
                      ('1970-01-01T00:00:15.000000Z', 2.5, 30)
                    """);
            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than inferring it from the rows: every shape below returns the same rows on the
            // scalar loop, so a regression back to scalar would leave the differentials green.
            // CompiledFilter takes the options as an int and keeps no record of them, so
            // assertExecHint() re-runs the serializer over the same metadata the code generator
            // hands it.
            assertExecHint("dcwl", "f32 * 2.0 > f32", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcwl", "f32 * 2.0 > 1.5 and i32 > 5", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcwl", "f32 + 0.0 > 1.5", EXEC_HINT_WIDE_LANE);
            // The INT-column control takes the other branch: isWideLaneEligible declines the
            // (i32, f64) pairing, so this one stays on the scalar backend.
            assertExecHint("dcwl", "i32 / 2.0 > 5", EXEC_HINT_SCALAR);

            // Column against column, with the product overflowing f32 on the two largest rows:
            // 3.0E38 * 2.0 and 2.0E38 * 2.0 are 6.0E38 and 4.0E38 at f64 and both +Inf at f32.
            // This predicate answers the same at either width - +Inf and 6.0E38 alike exceed
            // 3.0E38 - so both rows are in the set below whichever width evaluated the product.
            // The 1.0E-38 row is NOT, and its absence is Numbers.DOUBLE_TOLERANCE (1e-10) rather
            // than arithmetic: 2.0E-38 is greater than 1.0E-38, but they differ by 1e-38, so
            // Numbers.equals() calls them equal and both engines answer "not greater" (the JIT
            // side through DOUBLE_EPSILON in jit/impl/consts.h).
            assertJitScalarAndVectorMatchJava(
                    "dcwl where f32 * 2.0 > f32",
                    "k\tf32\ti32\n" +
                            "1970-01-01T00:00:02.000000Z\t0.75\t10\n" +
                            "1970-01-01T00:00:03.000000Z\t1.0\t-3\n" +
                            "1970-01-01T00:00:05.000000Z\t1.6777215E7\t16777215\n" +
                            "1970-01-01T00:00:06.000000Z\t1.6777216E7\t16777216\n" +
                            "1970-01-01T00:00:07.000000Z\t1.6777216E7\t16777217\n" +
                            "1970-01-01T00:00:09.000000Z\t3.0E38\t2147483646\n" +
                            "1970-01-01T00:00:11.000000Z\t0.1\t6\n" +
                            "1970-01-01T00:00:12.000000Z\t8388608.0\t8\n" +
                            "1970-01-01T00:00:13.000000Z\t2.0E38\t12\n" +
                            "1970-01-01T00:00:15.000000Z\t2.5\t30\n"
            );
            // The producer shape: the DOUBLE-literal conjunct used to take "i32 > 5" down to the
            // scalar backend with it, and both now ride the same four-lane loop.
            assertJitScalarAndVectorMatchJava(
                    "dcwl where f32 * 2.0 > 1.5 and i32 > 5",
                    "k\tf32\ti32\n" +
                            "1970-01-01T00:00:05.000000Z\t1.6777215E7\t16777215\n" +
                            "1970-01-01T00:00:06.000000Z\t1.6777216E7\t16777216\n" +
                            "1970-01-01T00:00:07.000000Z\t1.6777216E7\t16777217\n" +
                            "1970-01-01T00:00:09.000000Z\t3.0E38\t2147483646\n" +
                            "1970-01-01T00:00:12.000000Z\t8388608.0\t8\n" +
                            "1970-01-01T00:00:13.000000Z\t2.0E38\t12\n" +
                            "1970-01-01T00:00:15.000000Z\t2.5\t30\n"
            );
            // The additive-identity spelling, the one literal for which f32 and f64 evaluation
            // really are identical over every input. It takes the same route as the rest.
            assertJitScalarAndVectorMatchJava(
                    "dcwl where f32 + 0.0 > 1.5",
                    "k\tf32\ti32\n" +
                            "1970-01-01T00:00:05.000000Z\t1.6777215E7\t16777215\n" +
                            "1970-01-01T00:00:06.000000Z\t1.6777216E7\t16777216\n" +
                            "1970-01-01T00:00:07.000000Z\t1.6777216E7\t16777217\n" +
                            "1970-01-01T00:00:09.000000Z\t3.0E38\t2147483646\n" +
                            "1970-01-01T00:00:12.000000Z\t8388608.0\t8\n" +
                            "1970-01-01T00:00:13.000000Z\t2.0E38\t12\n" +
                            "1970-01-01T00:00:15.000000Z\t2.5\t30\n"
            );
            // Control: an INT column under the DOUBLE-width node keeps the scalar backend -
            // isWideLaneEligible declines the (i32, f64) pairing - and keeps its rows. Both
            // filters read INT_NULL as NaN (Numbers.intToDouble on the Java side, int32_to_double
            // on the backend's), and NaN answers no comparison, so the NULL row drops.
            assertJitScalarAndVectorMatchJava(
                    "dcwl where i32 / 2.0 > 5",
                    "k\tf32\ti32\n" +
                            "1970-01-01T00:00:05.000000Z\t1.6777215E7\t16777215\n" +
                            "1970-01-01T00:00:06.000000Z\t1.6777216E7\t16777216\n" +
                            "1970-01-01T00:00:07.000000Z\t1.6777216E7\t16777217\n" +
                            "1970-01-01T00:00:09.000000Z\t3.0E38\t2147483646\n" +
                            "1970-01-01T00:00:13.000000Z\t2.0E38\t12\n" +
                            "1970-01-01T00:00:14.000000Z\t-1.0\t20\n" +
                            "1970-01-01T00:00:15.000000Z\t2.5\t30\n"
            );
        });
    }

    @Test
    public void testDoubleConstantInFourByteArithmeticOverSimdBatch() throws Exception {
        // The pinned-row test above runs 16 rows. This one runs the SIMD body plus a scalar tail
        // over random data, so the four-lane loop, its tail and the mask handling all see volume.
        final String ddl = "create table dcsb as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float(2) f32," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            assertQueryNotNullNoLeakCheck("dcsb where f32 * 2.0 > 0.5 and i32 > 0");
            assertQueryNotNullNoLeakCheck("dcsb where f32 + 0.0 > 0.5");
            assertQueryNotNullNoLeakCheck("dcsb where f32 * 3.0 > 1.5 or i32 < 0");
            assertQueryNotNullNoLeakCheck("dcsb where not (f32 / 2.0 > 0.25)");
            assertQueryNotNullNoLeakCheck("dcsb where f32 - 1.0 < 0.0");
            // assertQueryNotNullNoLeakCheck() only asks that the JIT and the Java filter return the
            // same rows, and the scalar loop returns them too - so the four-lane routing this test
            // exists to cover could be reverted and every assertion above would stay green. Pin the
            // loop the name claims. getExecHint() reads the metadata and the parsed filter, not the
            // rows, so one block after the queries covers all five of them.
            assertExecHint("dcsb", "f32 * 2.0 > 0.5 and i32 > 0", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcsb", "f32 + 0.0 > 0.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcsb", "f32 * 3.0 > 1.5 or i32 < 0", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcsb", "not (f32 / 2.0 > 0.25)", EXEC_HINT_WIDE_LANE);
            assertExecHint("dcsb", "f32 - 1.0 < 0.0", EXEC_HINT_WIDE_LANE);
        });
    }

    @Test
    public void testDoubleConstArithChainWithLongConjunctVectorizes() throws Exception {
        // "f32 * 2.0 > 1.5 and l64 > 5" pairs a wide-lane conversion source - a FLOAT-column
        // arithmetic node under a DOUBLE literal - with an EIGHT-byte conjunct. It runs the
        // four-lane loop and emits plain AND, where the mixed-size detector would otherwise have
        // unlocked AND_SC and the sortPredicates reordering. serialize()'s detector gate carries
        // the measurement behind that trade; the legs below pin the choice as it stands, the
        // "l64 = null" leg included, which keeps its AND_SC because that filter does not enter
        // wide-lane mode.
        //
        // testDoubleConstantInFourByteArithmeticRunsFourLaneLoop pins the same admission over a
        // FLOAT/INT table, whose filters read four-byte columns only, so the mixed-size detector
        // would not have unlocked a short circuit there in any case. This fixture carries a LONG
        // column instead, which is the shape where the choice between the two loops is real. The
        // tree does reach that shape elsewhere - testFloatArithI64LeafAlongsideIntWidenStaysCorrect
        // runs "anint < along and afloat * 1.0 > 1.5 and afloat + 5_000_000_000 > 1.5" over a
        // mixed-width table - but through assertJitMatchesJava(query, true, null), which asserts
        // JIT-against-Java parity and pins neither the absolute rows nor which backend loop runs.
        // What is new here is the strength of the assertion: absolute rows across Java,
        // FORCE_SCALAR and the vectorized engine, together with the hint. FORCE_SCALAR is not a
        // formality - serialize() takes scalarModeDetected from forceScalar, which
        // SqlCodeGenerator:4650 passes for JIT_MODE_FORCE_SCALAR, and then routes a pure AND / OR
        // chain of more than one predicate to serializePredicatesAndSc / serializePredicatesOrSc,
        // so each leg below has already returned its rows through a short-circuiting backend. A
        // later decision to prefer short-circuiting would move the assertExecHint() lines and leave
        // the row assertions standing.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dcsc (k TIMESTAMP, f32 FLOAT, l64 LONG) TIMESTAMP(k) PARTITION BY DAY
                    """);
            // 0.75 * 2.0 is exactly 1.5, so that row fails a strict >, while the two f32 values
            // beside it land on either side of the bound: 0.7500001 clears it, 0.74999994 falls
            // short. Those three rows pin the strict comparison AT the bound. They do NOT pin the
            // width the node computes at: doubling the finite values here is exact at both widths,
            // and a NULL fails the comparison whichever width ran. The 2^24 rows and the
            // "f32 + 1.0" leg at the end of the test are what make that width observable. NULL
            // appears in the FLOAT column, in the LONG column and in both at once.
            execute("""
                    INSERT INTO dcsc VALUES
                      ('1970-01-01T00:00:00.000000Z', NULL, NULL),
                      ('1970-01-01T00:00:01.000000Z', 0.0, 0),
                      ('1970-01-01T00:00:02.000000Z', 0.75, 10),
                      ('1970-01-01T00:00:03.000000Z', 1.0, -3),
                      ('1970-01-01T00:00:04.000000Z', -2.5, 7),
                      ('1970-01-01T00:00:05.000000Z', 16777217.0, NULL),
                      ('1970-01-02T00:00:06.000000Z', 0.7500001, 6),
                      ('1970-01-02T00:00:07.000000Z', 0.74999994, 9),
                      ('1970-01-02T00:00:08.000000Z', NULL, 8),
                      ('1970-01-02T00:00:09.000000Z', 2.5, 5)
                    """);
            // A column top: the ten rows above were written before l2 existed, so they read NULL
            // through the column-top path rather than from stored data.
            execute("ALTER TABLE dcsc ADD COLUMN l2 LONG");
            execute("""
                    INSERT INTO dcsc VALUES
                      ('1970-01-03T00:00:10.000000Z', 0.8, 11, 12),
                      ('1970-01-03T00:00:11.000000Z', 0.2, 12, 3),
                      ('1970-01-03T00:00:12.000000Z', 3.0, 13, NULL),
                      ('1970-01-03T00:00:13.000000Z', 16777216.0, 9, 7)
                    """);

            final String andRows = """
                    k\tf32\tl64\tl2
                    1970-01-02T00:00:06.000000Z\t0.7500001\t6\tnull
                    1970-01-03T00:00:10.000000Z\t0.8\t11\t12
                    1970-01-03T00:00:12.000000Z\t3.0\t13\tnull
                    1970-01-03T00:00:13.000000Z\t1.6777216E7\t9\t7
                    """;

            // The producer of the suppression: one conjunct owns the conversion source, the other
            // is an ordinary eight-byte comparison, and the whole chain takes the four-lane loop.
            assertExecHint("dcsc", "f32 * 2.0 > 1.5 and l64 > 5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where f32 * 2.0 > 1.5 and l64 > 5", andRows);

            // Writing the conjuncts the other way round moves neither the rows nor the mode.
            assertExecHint("dcsc", "l64 > 5 and f32 * 2.0 > 1.5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where l64 > 5 and f32 * 2.0 > 1.5", andRows);

            // The same gate governs OR chains: this leg pins that OR_SC is withheld too.
            assertExecHint("dcsc", "f32 * 2.0 > 1.5 or l64 > 5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where f32 * 2.0 > 1.5 or l64 > 5", """
                    k\tf32\tl64\tl2
                    1970-01-01T00:00:02.000000Z\t0.75\t10\tnull
                    1970-01-01T00:00:03.000000Z\t1.0\t-3\tnull
                    1970-01-01T00:00:04.000000Z\t-2.5\t7\tnull
                    1970-01-01T00:00:05.000000Z\t1.6777216E7\tnull\tnull
                    1970-01-02T00:00:06.000000Z\t0.7500001\t6\tnull
                    1970-01-02T00:00:07.000000Z\t0.74999994\t9\tnull
                    1970-01-02T00:00:08.000000Z\tnull\t8\tnull
                    1970-01-02T00:00:09.000000Z\t2.5\t5\tnull
                    1970-01-03T00:00:10.000000Z\t0.8\t11\t12
                    1970-01-03T00:00:11.000000Z\t0.2\t12\t3
                    1970-01-03T00:00:12.000000Z\t3.0\t13\tnull
                    1970-01-03T00:00:13.000000Z\t1.6777216E7\t9\t7
                    """);

            // The eight-byte conjunct reading a column top.
            assertExecHint("dcsc", "f32 * 2.0 > 1.5 and l2 > 5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where f32 * 2.0 > 1.5 and l2 > 5", """
                    k\tf32\tl64\tl2
                    1970-01-03T00:00:10.000000Z\t0.8\t11\t12
                    1970-01-03T00:00:13.000000Z\t1.6777216E7\t9\t7
                    """);

            // ... and reading NULL on the eight-byte side. This leg takes the other branch of the
            // gate: isWideLaneEligible does not admit "l64 = null", so isWideLaneMode is false, the
            // gate's || short-circuits before it asks about the conversion source, the mixed-size
            // detector runs, and the chain keeps its AND_SC. Same conversion source, same columns,
            // scalar backend;
            // CompiledFilterIRSerializerTest#testWideLaneSourceSuppressesShortCircuitFilterWide
            // pins the IR for the same shape.
            assertExecHint("dcsc", "f32 * 2.0 > 1.5 and l64 = null", EXEC_HINT_SCALAR);
            assertJitScalarAndVectorMatchJava("dcsc where f32 * 2.0 > 1.5 and l64 = null", """
                    k\tf32\tl64\tl2
                    1970-01-01T00:00:05.000000Z\t1.6777216E7\tnull\tnull
                    """);

            // A LONG bind variable observes at eight bytes exactly as the column does, so the
            // suppression and the mode follow it.
            bindVariableService.setLong("bv", 5);
            assertExecHint("dcsc", "f32 * 2.0 > 1.5 and l64 > :bv", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where f32 * 2.0 > 1.5 and l64 > :bv", andRows);

            // The width the node computes at, observed rather than assumed. 2^24 + 1 has no f32,
            // so 16777216.0f + 1.0f rounds back to 16777216.0f while (double) 16777216.0f + 1.0 is
            // 16777217.0 - the pair CompiledFilterIRSerializer.isDoubleConst names. "f32 + 1.0"
            // resolves to the +(DD) factory (AddDoubleFunctionFactory; the floating-point overloads
            // are +(FF) and +(DD), with no +(FD)), so the Java filter evaluates the node at f64,
            // which keeps the 2^24 row below. A backend that ran the ADD at the observed f32 width
            // would return no rows here, on either loop, so this leg fails loudly rather than
            // silently agreeing with itself.
            assertExecHint("dcsc", "f32 + 1.0 > 16777216.0 and l64 > 5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("dcsc where f32 + 1.0 > 16777216.0 and l64 > 5", """
                    k\tf32\tl64\tl2
                    1970-01-03T00:00:13.000000Z\t1.6777216E7\t9\t7
                    """);
        });
    }

    @Test
    public void testFloatArithmeticOperandWiden() throws Exception {
        // An out-of-INT-range integer constant used as an ARITHMETIC OPERAND against a
        // narrow-int column, in a predicate that also has a FLOAT column, diverged.
        // INT and FLOAT are both 4 bytes, so hasMixedSizes() is false and the type
        // observer types the constant F4; a co-present float also suppresses the global
        // narrow-i64 widening. serializeConstant then emitted the constant as a lossy
        // 32-bit float and the predicate vectorized, but AVX2 convert() has no i32->i64
        // path, so i32 * 3_000_000_000 was computed as a float multiply. The Java filter
        // computes it at long width (MulInt#getLong) and only then converts to floating
        // point, so for i32=7 it kept the row (21_000_000_000 > 20999999488.0) while the JIT
        // dropped it (20999999488.0f > 20999999488.0f is false).
        // The serializer now flags the out-of-range integer operand: it emits a full I8
        // IMM and runs scalar, where the scalar convert() widens the narrow column to i64
        // and int32 * int64 stays exact. JIT stays enabled (scalar mode).
        assertMemoryLeak(() -> {
            execute("create table t (i32 int, i64 long, fcol float, dcol double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(7, 7, 20999999488.0, 20999999488.0, 0)," +           // 7*3e9=21_000_000_000 > fcol; float multiply drops it
                    "(-7, -7, 20999999488.0, 20999999488.0, 1_000_000)," +   // negative operand exercised via the fold path
                    "(1, 1, 2000000000000.0, 2000000000000.0, 2_000_000)," + // addition: 1+2e12 > fcol; float add loses the 1
                    "(46_341, 46_341, 5.0, 5.0, 3_000_000)," +
                    "(0, 0, -1.0, -1.0, 4_000_000)," +
                    "(null, null, 1.0, 1.0, 5_000_000)");
            // Previously diverging shapes fixed by this change: an out-of-INT-range integer
            // constant as a bare (non-negated) arithmetic operand, alongside a FLOAT column.
            // Still JIT-compiled, now correct.
            // Absolute pin: the i32=7 boundary row (21_000_000_000 > 20999999488.0 at long width)
            // survives; the pre-fix JIT computed it as a float multiply and dropped it.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > fcol", true,
                    "i32\ti64\tfcol\tdcol\tts\n" +
                            "7\t7\t2.1E10\t2.0999999488E10\t1970-01-01T00:00:00.000000Z\n" +
                            "46341\t46341\t5.0\t5.0\t1970-01-01T00:00:03.000000Z\n" +
                            "0\t0\t-1.0\t-1.0\t1970-01-01T00:00:04.000000Z\n");
            assertJitMatchesJava("t where i32 + 2_000_000_000_000 > fcol", true);
            // An AND chain with a LONG column splits into per-comparison predicate contexts,
            // so the "i32 * 3_000_000_000 > fcol" comparison is again pure INT+FLOAT and diverged.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > fcol and i64 > 0", true);
            // Control: a negated out-of-range operand (-3_000_000_000) is a unary-minus subtree
            // that descend() folds via its own long-width path, so it was already correct.
            assertJitMatchesJava("t where i32 * -3_000_000_000 > fcol", true);
            // Control: a DOUBLE column makes the predicate mixed-size (INT 4B, DOUBLE 8B),
            // which already runs scalar and emits the constant as I8 - correct before too.
            assertJitMatchesJava("t where i32 * 3_000_000_000 > dcol", true);
            // Control: a direct FLOAT-vs-out-of-range-constant comparison is not an
            // arithmetic operand; it stays vectorized and unchanged.
            assertJitMatchesJava("t where fcol > 3_000_000_000", true);
            // Control: an in-range constant OPERAND wraps at INT width under a float on both
            // paths (getInt), so it must not widen. Only the wrapped product does, through the
            // SX_I64 markIntCmpFloatOperand emits after the multiply.
            assertJitMatchesJava("t where i32 * 3 > fcol", true);
        });
    }

    @Test
    public void testFloatDirectCompareOutOfRangeConstWiden() throws Exception {
        // A FLOAT column compared directly against an out-of-INT-range integer constant
        // diverged. INT and FLOAT are both 4 bytes, so hasMixedSizes() is false and the type
        // observer types the constant F4; serializeNumber then emitted it as a lossy 32-bit
        // float (3_000_000_200 rounds to 3_000_000_256f). The JIT compared at float width, while the
        // Java filter promotes both operands to double and compares exactly. For a stored
        // fcol=3000000256.0 the JIT dropped the row on "> 3_000_000_200" (3_000_000_256f > 3_000_000_256f
        // is false) that Java kept (3000000256.0 > 3000000200.0 is true), and it spuriously
        // matched the row on "= 3_000_000_200" that Java rejected.
        // The serializer now widens the constant to a full I8 IMM; four-lane AVX2 promotes the
        // float column to double. JIT stays enabled and vectorized.
        assertMemoryLeak(() -> {
            execute("create table t (fcol float, dcol double, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(3000000256.0, 3000000256.0, 0)," +     // boundary: 3_000_000_200 rounds to this float
                    "(3000000000.0, 3000000000.0, 1_000_000)," +
                    "(5.0, 5.0, 2_000_000)," +
                    "(null, null, 3_000_000)");
            // Operators that diverged on the boundary row before the fix.
            // Absolute pin: at float width 3_000_000_256f > 3_000_000_256f is false, but the stored
            // fcol promotes to double (3000000256.0 > 3000000200.0 is true), so Java keeps the
            // boundary row; the pre-fix JIT dropped it.
            assertJitMatchesJava("t where fcol > 3_000_000_200", true,
                    "fcol\tdcol\tts\n" +
                            "3.0000003E9\t3.000000256E9\t1970-01-01T00:00:00.000000Z\n");
            assertJitMatchesJava("t where fcol >= 3_000_000_200", true);
            assertJitMatchesJavaOnEmptyResult("t where fcol = 3_000_000_200", true);
            assertJitMatchesJava("t where fcol <> 3_000_000_200", true);
            assertJitMatchesJava("t where fcol < 3_000_000_200", true);
            assertJitMatchesJava("t where fcol <= 3_000_000_200", true);
            // Single-value IN reduces to equality on the FLOAT key.
            assertJitMatchesJavaOnEmptyResult("t where fcol in (3_000_000_200)", true);
            // Control: a DOUBLE column stores 8 bytes and compares exactly on both paths, so it
            // stays vectorized and unchanged.
            assertJitMatchesJava("t where dcol > 3_000_000_200", true);
            // Control: an in-range constant compares at int width on both paths (the observer
            // types it I4 and int-parse succeeds), so it must not widen.
            assertJitMatchesJava("t where fcol > 5", true);
        });
    }

    @Test
    public void testFloatingDivisionByZeroMatchesJava() throws Exception {
        // DivDoubleFunctionFactory and DivFloatFunctionFactory fold a non-finite quotient to NaN
        // ("Numbers.isFinite(d) ? d : Double.NaN"), so the Java filter reads a division by zero as
        // NULL and orders it against nothing. The native div (divss/divsd in jit/impl/x86.h,
        // vdivps/vdivpd in jit/impl/avx2.h, fdiv in jit/impl/aarch64.h) propagated a real
        // +/-Infinity instead, which the ordering opcodes rank like an ordinary extreme - so the
        // JIT kept rows the Java filter dropped. JIT is on by default (cairo.sql.jit.mode = "on"),
        // making this a silent wrong result: measured 2 rows on the JIT against 1 on the Java
        // filter for "d / e > 0.0".
        //
        // Equality never diverged - double_cmp_epsilon tests the exponent bits of both operands
        // and so calls any two non-finite values equal, exactly as Numbers.equals does. Only the
        // four ordering operators did.
        //
        // The table needs 20 rows, not 4, to reach the vectorized backend at BOTH widths. The AVX2
        // loop step is 256 / (elementBytes * 8) and compiler.cpp skips the SIMD body entirely when
        // rowCount < step: a 4-row table gives step 8 for a single-size FLOAT predicate, so
        // "f / g" would run only the scalar tail and vdivps would never execute. Zero divisors sit
        // at ids 1 and 2 (inside the first f32 vector) and at id 18 (in the scalar tail), so both
        // loops carry a diverging row at both widths.
        assertMemoryLeak(() -> {
            execute("create table dz (id int, d double, e double, f float, g float, i int," +
                    " l long, k timestamp) timestamp(k) partition by day");
            execute("""
                    insert into dz
                    select x::int,
                           case when x = 1 then 2.0 when x = 2 then -2.0 when x = 3 then 6.0
                                when x = 18 then 4.0 end,
                           case when x in (1, 2, 18) then 0.0 when x = 3 then 3.0 else 1.0 end,
                           (case when x = 1 then 2.0 when x = 2 then -2.0 when x = 3 then 6.0
                                 when x = 18 then 4.0 end)::float,
                           (case when x in (1, 2, 18) then 0.0 when x = 3 then 3.0 else 1.0 end)::float,
                           case when x in (1, 2, 18) then 0 when x = 3 then 3 else 1 end,
                           case when x in (1, 2, 18) then 0 when x = 3 then 3 else 1 end,
                           timestamp_sequence(0, 1_000_000)
                    from long_sequence(20)
                    """);
            // Only id 3 has a finite non-zero quotient; every other row divides by zero or has a
            // NULL numerator, and both read as NULL on the Java filter.
            final String onlyThree = "id\n3\n";
            final String noRows = "id\n";
            final String allRows = "id\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n";

            // DOUBLE / DOUBLE - vdivpd in the vector body, divsd in the tail and under FORCE_SCALAR.
            assertJitScalarAndVectorMatchJava("select id from dz where d / e > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e >= 0.0", onlyThree);
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where d / e < 0.0", noRows);
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where d / e <= 0.0", noRows);
            // A constant zero divisor is the same runtime division - the parser cannot fold it
            // away because the numerator is a column.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where d / 0.0 > 0.0", noRows);
            // FLOAT / FLOAT - a single-size 4-byte predicate, so this is the only shape that
            // reaches vdivps. It needs the 20-row fixture above to execute at all.
            assertJitScalarAndVectorMatchJava("select id from dz where f / g > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g >= 0.0", onlyThree);
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where f / g < 0.0", noRows);
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where f / g <= 0.0", noRows);
            // An INT / LONG divisor converts to floating point before the division, so a zero
            // divisor lands on the same non-finite quotient rather than on the integer NULL.
            // These are mixed-size predicates and so run the scalar loop on both JIT modes.
            assertJitScalarAndVectorMatchJava("select id from dz where d / i > 0.0", onlyThree);
            assertJitScalarAndVectorMatchJava("select id from dz where d / l > 0.0", onlyThree);

            // Controls: equality already agreed and must keep agreeing. Every NULL quotient is
            // unequal to 0.0 on both paths, so "!=" keeps the whole table.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select id from dz where d / e = 0.0", noRows);
            assertJitScalarAndVectorMatchJava("select id from dz where d / e != 0.0", allRows);
            assertJitScalarAndVectorMatchJava("select id from dz where f / g != 0.0", allRows);
            // Control: a non-zero divisor divides normally and must not be folded to NULL.
            assertJitScalarAndVectorMatchJava("select id from dz where d / 2.0 > 0.0", "id\n1\n3\n18\n");
            assertJitScalarAndVectorMatchJava("select id from dz where f / 2.0 > 0.0", "id\n1\n3\n18\n");
            // Control: multiplication does NOT fold a non-finite result on either path
            // (MulDoubleFunctionFactory returns the raw product), so overflow must stay +Infinity
            // on both and keep ordering like an extreme.
            assertJitScalarAndVectorMatchJava("select id from dz where d * 1e308 * 1e308 > 0.0",
                    "id\n1\n3\n18\n");
        });
    }

    @Test
    public void testGeoHashConstant() throws Exception {
        final String query = "x " +
                "where geo8 != ##1001 and geo16 != ##100110011001 and geo32 != ##1001100110011001 and geo64 != ##10011001100110011001100110011001";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_geohash(4) geo8," +
                " rnd_geohash(12) geo16," +
                " rnd_geohash(16) geo32," +
                " rnd_geohash(32) geo64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGeoHashNull() throws Exception {
        final String query = "x where geo8 <> null or geo16 <> null or geo32 <> null or geo64 <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_geohash(4) geo8," +
                " rnd_geohash(15) geo16," +
                " rnd_geohash(16) geo32," +
                " rnd_geohash(40) geo64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGeoHashValue() throws Exception {
        final String query = "x where geo8a = geo8b";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_geohash(4) geo8a," +
                " rnd_geohash(4) geo8b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testGroupBy() throws Exception {
        // We don't want parallel GROUP BY to kick in, so we cast string column to symbol to avoid that.
        final String query = "select str::symbol, sum(price)/count() from x where price > 0";
        final String ddl = "create table x as " +
                "(select rnd_str('ABB','HBC','DXR') str, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testHugeFilter() throws Exception {
        final int N = 682; // depends on memory configuration for a jit IR
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long() i64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";

        FilterGenerator gen = new FilterGenerator();
        for (int i = 0; i < N; i++) {
            if (i > 0) {
                gen.withAnyOf(" and ");
            }
            gen.withAnyOf("i64 != 0");
        }
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorChained() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b," +
                " rnd_int(1, 10, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("a")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2)")
                .withBooleanOperator()
                .withAnyOf("b")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, 4)")
                .withBooleanOperator()
                .withAnyOf("c")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5, 6)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorFloat() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(0.1, 0.2, 0.3)", "(0.5, 0.6)", "(-0.1, 0.0, 0.1)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorFloatWithNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(0.1, 0.2, null)", "(null, 0.5)", "(0.0, null, 0.1)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorInt() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, 3)", "(4, 5)", "(6, 7, 8, 9)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorIntWithNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, null)", "(null, 4, 5)", "(6, null)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorManyValues() throws Exception {
        // Tests IN operator with many values (exceeds typical unroll thresholds)
        final String query = "x where " +
                "i64 in (1, 2, 3, 4, 5, 6, 7, 8, 9)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(1, 20, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testInOperatorNestedWithAndOr() throws Exception {
        // Tests IN operator nested within AND/OR expressions
        final String query = "x where " +
                "(a in (1, 2) and b > 5) or (a in (8, 9) and b < 3)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testInOperatorNullElementMatchesEqNullOnNarrowArithKey() throws Exception {
        // An untyped NULL IN element reads a narrow-int key at the width '=' reads it: INT. Both
        // paths used to widen the key (getLong / sx_i64) against a NULL element, which disagrees
        // with '=', IS NULL and the projection for an INT-arithmetic key - the one key whose two
        // widths can disagree about the sentinel. The JIT now keeps the key at i32 for the NULL
        // pairing, mirroring the Java InLong path, and the filter stays vectorized (no sx_i64).
        //
        // Row 1: a*b = 65_536*32_768 wraps onto INT_NULL, so the row IS null; at long width the
        //        product is +2^31, which is not LONG_NULL - the old code missed the row.
        // Row 2: a*b = -2^30 * 2 wraps onto INT_NULL the same way (long width: -2^31).
        // Row 3: a*b*c = 2^30 * 8 * 2^30 has value 0, but its long-width product overflows exactly
        //        onto LONG_NULL - the old code matched a row that is not null.
        // Row 4: a genuinely NULL key. Rows 5+ compute 3*3 = 9.
        // The table has >= 64 rows so the vectorized (AVX2) loop is genuinely exercised.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 65_536 when x = 2 then -1_073_741_824 when x = 3 then 1_073_741_824 when x = 4 then null else 3 end as int) a," +
                    " cast(case when x = 1 then 32_768 when x = 2 then 2 when x = 3 then 8 else 3 end as int) b," +
                    " cast(case when x = 3 then 1_073_741_824 else 1 end as int) c," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: IN (null) selected no row at all (it probed the widened key against
            // LONG_NULL), while '= null' and IS NULL select rows 1, 2 and 4.
            final String nullKeyRows = "rn\n1\n2\n4\n";
            assertJitMatchesJava("select rn from y where (a*b) = null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) is null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null, 999)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) in (null, 999, 7)", true, nullKeyRows);

            // RED on HEAD: row 3's value is 0, but its long-width product is exactly LONG_NULL,
            // so IN (null) matched it while '= null' did not.
            assertJitMatchesJava("select rn from y where (a*b*c) = null", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b*c) in (null)", true, nullKeyRows);

            // The NOT IN inversion: every row that is not null, and row 3 among them.
            Assert.assertEquals(61, runQuery("select rn from y where (a*b) not in (null)"));
            assertJitMatchesJava("select rn from y where (a*b*c) not in (null) and rn <= 4", true, "rn\n3\n");

            // A NULL element next to a genuine LONG element: each pairing keeps its own width, so
            // the LONG element still widens the key (and matches row 1's +2^31 product) while the
            // NULL element wraps it.
            assertJitMatchesJava("select rn from y where (a*b) in (null, 2_147_483_648L)", true, "rn\n1\n2\n4\n");

            // The widened key was also compared against a MIS-SIZED NULL immediate. serializeNull
            // emits the NULL at the observer's width (I4 here), and the backend maps INT_NULL onto
            // LONG_NULL only when the immediate reaches the compare in a register: preload_constants
            // (jit/common.h) hoists the first MAX_CONSTANTS = 8 integer constants, and past that cap
            // imm2reg materializes a bare Imm with a movabs at the KEY's width - a raw -2^31. So the
            // i64 key was tested against -2^31 instead of LONG_NULL: it matched row 2, whose
            // long-width product is exactly -2^31 but which Java does not match, and MISSED row 4,
            // the genuinely-null key. RED on HEAD (JIT returned row 2 alone; Java returned row 4).
            //
            // The divergence needs the NULL to land past the 8th constant in IR order, and IN
            // elements serialize in reverse - so the NULL goes FIRST in the SQL list, behind exactly
            // the 8 constants that fill the cache. One element more and serializeIn declines the
            // filter outright (sqlJitMaxInListSizeThreshold counts the key, so 9 elements is the
            // ceiling), falling back to Java and hiding the bug: this list is the whole window.
            // Keeping the key at i32 removes the mixed-width compare entirely.
            assertJitMatchesJava("select rn from y where (a*b) in (null,11,12,13,14,15,16,17,18)", true, nullKeyRows);
            assertJitMatchesJava("select rn from y where (a*b) not in (null,11,12,13,14,15,16,17,18) and rn <= 4", true, "rn\n3\n");
            // Control: the same list with the NULL in a cached slot (last in the SQL list) always
            // agreed, which is why a short list never exposed this.
            assertJitMatchesJava("select rn from y where (a*b) in (11,12,13,14,15,16,17,18,null)", true, nullKeyRows);

            // Control: a plain INT column key is not width-split, so both widths already agreed.
            assertJitMatchesJava("select rn from y where a in (null)", true, "rn\n4\n");
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnConstantKey() throws Exception {
        // An overflowing INT arithmetic CONSTANT key on the left of a multi-value IN() list.
        // `k IN (e0, e1, ...)` is `k = e0 OR k = e1 OR ...`, and the key is one value: the fold
        // is an IntConstant holding the wrap (-727_379_968), so every pairing compares that,
        // sign-extended where the element is 64 bits. A LONG or TIMESTAMP element cannot pull
        // the key back to its pre-wrap value. The table carries both, so a wrong key width
        // matches the wrong row rather than returning empty.
        // Every assertion carries an absolute pin. Parity alone is not an oracle: this change
        // moves the Java IN path and the JIT together, so a shared wrong width would pass.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as long) i64, cast(v as timestamp) tsc " +
                    "from (select 1_000_000_000_000 v union all select -727_379_968 v))");
            final String header = "i64\ttsc\n";
            final String widened = "1000000000000\t1970-01-12T13:46:40.000000Z\n";
            final String wrapped = "-727379968\t1969-12-31T23:47:52.620032Z\n";
            // A LONG element sign-extends the key's wrapped value, so the wrapped row matches.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, 5)", true, header + wrapped);
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (5, i64)", true, header + wrapped);        // order independent
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (5, 6, i64)", true, header + wrapped);     // multi value
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, i64)", true, header + wrapped);      // all long
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) not in (i64, 5)", true, header + widened);    // inverse
            // A TIMESTAMP element reads the key the same way.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (tsc, 5)", true, header + wrapped);
            // Controls: '=' and the single-value IN agree with the multi-value form.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) = i64", true, header + wrapped);
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64)", true, header + wrapped);
            // The wrapped value as a literal element matches the key on EVERY row, since the key
            // is a constant equal to it.
            assertJitMatchesJava("x where (1_000_000 * 1_000_000) in (i64, -727_379_968)", true,
                    header + widened + wrapped);
        });
    }

    @Test
    public void testInOperatorMixedWidthListOnPlainNarrowKey() throws Exception {
        // A plain INT column reads the same number through getInt() and getLong() (only INT
        // arithmetic wraps mod 2^32 under one and not the other), so InLongFunctionFactory holds a
        // mixed-width IN list in a single set and probes the key once per row instead of twice.
        // The rows pin what that must select: the NULL element matches the NULL row, the wide
        // element matches nothing an INT column can hold, and the narrow elements still match.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as int) i32 " +
                    "from (select 1 v union all select 2 v union all select -2_147_483_647 v union all select null v))");
            assertJitMatchesJava("x where i32 in (1, 2, null)", true, "i32\n1\n2\nnull\n");
            assertJitMatchesJava("x where i32 in (1, 5_000_000_000)", true, "i32\n1\n");
            assertJitMatchesJava("x where i32 in (1, 2, null, 5_000_000_000)", true, "i32\n1\n2\nnull\n");
            assertJitMatchesJava("x where i32 not in (1, 2, null)", true, "i32\n-2147483647\n");
            // A numeric string element is read at the width its value carries as a literal. The JIT
            // declines a string constant against a numeric column, so this one runs on the Java
            // filter, which is exactly the path the single merged set changed.
            assertJitMatchesJava("x where i32 in ('-2147483647', '5000000000')", false, "i32\n-2147483647\n");
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnLongColumn() throws Exception {
        // An overflowing INT arithmetic fold inside an IN() list against a LONG column. The fold
        // is an IntConstant holding the wrap (-727_379_968), so it selects the wrapped row and
        // never the mathematical one: a LONG peer does not change a constant subtree's width.
        // The table carries both values, so a fold at the wrong width matches the wrong row
        // rather than returning empty. Every assertion carries an absolute pin, because parity
        // alone is not an oracle here - this change moves the Java filter and the JIT together.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(v as long) i64 " +
                    "from (select 1_000_000_000_000 v union all select -727_379_968 v))");
            final String widened = "i64\n1000000000000\n";
            final String wrapped = "i64\n-727379968\n";
            assertJitMatchesJava("x where i64 in (1_000_000 * 1_000_000)", true, wrapped);          // single value
            assertJitMatchesJava("x where i64 in (1, 1_000_000 * 1_000_000)", true, wrapped);       // two values
            assertJitMatchesJava("x where i64 in (1, 2, 1_000_000 * 1_000_000)", true, wrapped);    // multi value
            assertJitMatchesJava("x where i64 not in (1, 1_000_000 * 1_000_000)", true, widened);   // inverse
            assertJitMatchesJava("x where i64 = 1_000_000 * 1_000_000", true, wrapped);             // control: plain '='
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowArithKey() throws Exception {
        // C1 regression: an overflowing INT *arithmetic* KEY on the left of IN(). The narrow-int
        // IN fix wrapped the IN-list elements at INT width (matching '=') but every InLong*.getBool
        // still read the KEY via getLong(), which WIDENS an overflowing INT arithmetic. So IN
        // disagreed with '=' even with the JIT off, and the JIT (which computes the key at I4 and
        // wraps) disagreed with the Java IN. The key is now read at INT width too, symmetric with
        // the elements. a + a = 3*10^9 wraps to INT -1_294_967_296; '=' (EqInt) and the JIT both wrap,
        // so a correctly-wrapping IN matches the single row.
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(1_500_000_000 as int) a, cast(1_500_000_000 as int) b)");

            // const IN shapes: the JIT wraps the key at I4, the Java IN must wrap too.
            assertJitMatchesJava("x where (a + a) in (1_500_000_000 + 1_500_000_000)", true);        // single const
            assertJitMatchesJava("x where (a + a) in (1, 1_500_000_000 + 1_500_000_000)", true);     // two const
            assertJitMatchesJava("x where (a + a) in (1, 2, 1_500_000_000 + 1_500_000_000)", true);  // multi const
            assertJitMatchesJavaOnEmptyResult("x where (a + a) not in (1_500_000_000 + 1_500_000_000)", true);    // inverse
            // an in-range literal equal to the wrapped key must match too (pre-existing slice).
            assertJitMatchesJava("x where (a + a) in (-1_294_967_296)", true);
            assertJitMatchesJava("x where (a + a) = 1_500_000_000 + 1_500_000_000", true);           // control: '='

            // Java (JIT-disabled) IN must agree with '=' across the const InLong* variants.
            Assert.assertEquals(1, runQuery("x where (a + a) in (1_500_000_000 + 1_500_000_000)"));      // single
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 1_500_000_000 + 1_500_000_000)"));   // two
            Assert.assertEquals(1, runQuery("x where (a + a) in (1, 2, 1_500_000_000 + 1_500_000_000)"));// multi
            Assert.assertEquals(0, runQuery("x where (a + a) not in (1_500_000_000 + 1_500_000_000)"));  // inverse
            Assert.assertEquals(1, runQuery("x where (a + a) = 1_500_000_000 + 1_500_000_000"));         // '='

            // runtime-const variant: a bind variable forces InLongRuntimeConstFunction. The const
            // element still matches the wrapped key; :p (= 0) does not.
            bindVariableService.setInt("p", 0);
            Assert.assertEquals(1, runQuery("x where (a + a) in (1_500_000_000 + 1_500_000_000, :p)"));

            // var variant: a non-constant element forces InLongVarFunction.
            Assert.assertEquals(1, runQuery("x where (a + a) in (b + b)"));
            Assert.assertEquals(1, runQuery("x where (a + a) = (b + b)"));
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowColumn() throws Exception {
        // An overflowing INT arithmetic fold inside an IN() list against a NARROW integer
        // key (INT/SHORT/BYTE). '=' (EqInt) wraps the fold mod 2^32 and the JIT emits the
        // wrapped I4, but the Java IN path read the element via getLong() and WIDENED it to
        // the full-width product. So IN disagreed with '=' even with the JIT off, and the
        // JIT disagreed with the Java IN. InLongFunctionFactory now reads narrow-key IN
        // elements at INT width (wrap), matching '='. The columns hold the wrapped image of
        // each overflow product so the correct (wrapping) behaviour matches a row.
        assertMemoryLeak(() -> {
            // 1_000_000 * 1_000_000 = 10^12 wraps to INT -727_379_968; 3 * 1_431_655_767 = 2^32 + 5
            // wraps to INT 5 (matchable by SHORT/BYTE keys too).
            execute("create table x as (select cast(-727_379_968 as int) i32b, " +
                    "cast(5 as int) i32, cast(5 as short) i16, cast(5 as byte) i8)");

            // INT key, canonical 10^12 product, JIT-vs-Java parity across IN forms.
            assertJitMatchesJava("x where i32b in (1_000_000 * 1_000_000)", true);         // single value
            assertJitMatchesJava("x where i32b in (1, 1_000_000 * 1_000_000)", true);      // two values
            assertJitMatchesJava("x where i32b in (1, 2, 1_000_000 * 1_000_000)", true);   // multi value
            assertJitMatchesJavaOnEmptyResult("x where i32b not in (1, 1_000_000 * 1_000_000)", true);  // inverse
            assertJitMatchesJava("x where i32b = 1_000_000 * 1_000_000", true);            // control: '='

            // The Java (JIT-disabled) IN must agree with '=' - it widened before the fix.
            Assert.assertEquals(1, runQuery("x where i32b in (1_000_000 * 1_000_000)"));
            Assert.assertEquals(1, runQuery("x where i32b = 1_000_000 * 1_000_000"));

            // INT/SHORT/BYTE keys all wrap the element to 5 and match.
            assertJitMatchesJava("x where i32 in (3 * 1_431_655_767)", true);
            assertJitMatchesJava("x where i16 in (3 * 1_431_655_767)", true);
            assertJitMatchesJava("x where i8 in (3 * 1_431_655_767)", true);
            Assert.assertEquals(1, runQuery("x where i32 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i16 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i8 in (3 * 1_431_655_767)"));
            Assert.assertEquals(1, runQuery("x where i32 = 3 * 1_431_655_767"));
            Assert.assertEquals(1, runQuery("x where i16 = 3 * 1_431_655_767"));
            Assert.assertEquals(1, runQuery("x where i8 = 3 * 1_431_655_767"));
        });
    }

    @Test
    public void testInOperatorOverflowFoldMatchesJavaOnNarrowKeyWithLongElement() throws Exception {
        // C4 regression: a narrow/INT KEY IN() list that mixes an overflowing INT-arithmetic
        // element with a genuine-LONG element. The Java InLong path reads the INT element at
        // the narrow key's width (getInt -> wrap), so the JIT must too. But
        // markI64WidenFoldRoots used to fold ONE comparison width across the whole IN list,
        // so a single coexisting LONG element (3_000_000_000) promoted the list to I8 and WIDENED
        // the overflowing INT element to its full-width product - the JIT then read 10^12
        // while the Java filter wrapped to -727_379_968 == the stored key, so the row matched in
        // Java but not in the JIT. The width is now derived per element (key vs that element),
        // so the INT element wraps (matching the key) while the LONG element stays at long
        // width (and never matches the narrow key either way).
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(-727_379_968 as int) c)"); // -727_379_968 = (int)(1_000_000*1_000_000)

            // overflow-INT element coexists with a genuine-LONG element against the narrow key.
            assertJitMatchesJava("x where c in (1_000_000 * 1_000_000, 3_000_000_000)", true);        // RED on HEAD
            assertJitMatchesJava("x where c in (3_000_000_000, 1_000_000 * 1_000_000)", true);        // element order swapped
            assertJitMatchesJava("x where c in (1, 1_000_000 * 1_000_000, 3_000_000_000)", true);     // plus a plain element
            assertJitMatchesJavaOnEmptyResult("x where c not in (1_000_000 * 1_000_000, 3_000_000_000)", true);    // inverse

            // The Java (JIT-disabled) path is the oracle: c matches the wrapped INT element only.
            Assert.assertEquals(1, runQuery("x where c in (1_000_000 * 1_000_000, 3_000_000_000)"));
            Assert.assertEquals(0, runQuery("x where c not in (1_000_000 * 1_000_000, 3_000_000_000)"));

            // control: an all-INT list against the narrow key already wrapped correctly.
            assertJitMatchesJava("x where c in (1, 1_000_000 * 1_000_000)", true);
        });
    }

    @Test
    public void testInOperatorOverflowLongElementUsesScalarPath() throws Exception {
        // A LONG element lifts the whole IN list to 64 bits, and the narrow arithmetic key
        // sign-extends its WRAPPED product to meet it. The pre-wrap value is therefore unreachable
        // through any element. See CompiledFilterIRSerializerTest#testInNullElementKeepsNarrowKeyVectorized
        // for the exact IR and exec-hint pins.
        //
        // That pairing - a 64-bit IN list against an I4-typed arithmetic key holding columns - is
        // exactly what forceScalarOnUnharmonisedNarrowArith() drops onto the SCALAR backend, and
        // getExecHint() answers forceScalarMode before it ever consults wide-lane mode. So none of
        // the widened spellings below runs a vectorized loop at all; only the all-narrow lists keep
        // one, at EXEC_HINT_SINGLE_SIZE_TYPE. The method was named for the wide lane and asserted
        // no mode at all, so nothing held it to that claim; the pins at the end of the body are
        // what it says now.
        //
        // The table has >= 64 rows so the vectorized loop is genuinely exercised (a 1-row table runs
        // entirely in the scalar tail and hides the bug). Row 1 overflows: a*b = 1_000_000*1_000_000
        // wraps to INT -727_379_968; all other rows compute 3*3 = 9.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1_000_000 else 3 end as int) a," +
                    " cast(case when x = 1 then 1_000_000 else 3 end as int) b," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // A LONG element paired with an INT constant: the key is one value, so only the
            // -727_379_968 arm can match, and it matches row 1.
            assertJitMatchesJava("select a from y where (a*b) in (1_000_000_000_000, -727_379_968)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968, 1_000_000_000_000)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (1_000_000_000_000, -727_379_968)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (1_000_000_000_000, -727_379_968)"));
            Assert.assertEquals(63, runQuery("select a from y where (a*b) not in (1_000_000_000_000, -727_379_968)"));
            // The pre-wrap value alone matches nothing: the key never carries it. Both engines
            // have to agree on that, and the wrapped spelling right below keeps the parity honest.
            assertJitMatchesJavaOnEmptyResult("select a from y where (a*b) in (1_000_000_000_000)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968)", true);
            Assert.assertEquals(0, runQuery("select a from y where (a*b) in (1_000_000_000_000)"));
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (-727_379_968)"));

            // control: an all-narrow list emits no SX_I64 and stays vectorized.
            assertJitMatchesJava("select a from y where (a*b) in (5, -727_379_968)", true);
            // control: a NULL element wraps the key, so it stays vectorized and matches no row here
            // (no product wraps onto INT_NULL in this table).
            assertJitMatchesJava("select a from y where (a*b) in (null, -727_379_968)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (null, -727_379_968)"));

            // The backend each spelling above runs on. Row parity cannot see it - the scalar
            // backend answers every one of them correctly - so the assertions above stay green
            // whichever loop the filter takes.
            assertExecHint("y", "(a*b) in (1_000_000_000_000, -727_379_968)", EXEC_HINT_SCALAR);
            assertExecHint("y", "(a*b) in (-727_379_968, 1_000_000_000_000)", EXEC_HINT_SCALAR);
            assertExecHint("y", "(a*b) not in (1_000_000_000_000, -727_379_968)", EXEC_HINT_SCALAR);
            assertExecHint("y", "(a*b) in (1_000_000_000_000)", EXEC_HINT_SCALAR);
            assertExecHint("y", "(a*b) in (-727_379_968)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("y", "(a*b) in (5, -727_379_968)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("y", "(a*b) in (null, -727_379_968)", EXEC_HINT_SINGLE_SIZE_TYPE);
        });
    }

    @Test
    public void testInOperatorOverflowNumericStringElementMatchesLiteral() throws Exception {
        // M1 regression: a numeric STRING/VARCHAR IN-list element against an overflowing
        // INT-arithmetic (narrow) key. isIntWidthElement wrapped the key only for
        // INT/SHORT/BYTE-TYPED elements, so a numeric string left the key on getLong()
        // (widen). Thus (a * b) IN ('-727379968') returned 0 rows while the equivalent
        // (a * b) IN (-727_379_968) and (a * b) = -727_379_968 both matched. The width is now
        // derived from the parsed VALUE: an INT-range numeric string wraps the key mod 2^32
        // (matching the numeric literal and '='), a wider value widens it. String IN-lists
        // do not compile to the JIT, so this is a Java-path parity check against the
        // numeric-literal spellings.
        assertMemoryLeak(() -> {
            // a * b wraps to INT -727_379_968 and widens to LONG 10^12; s/v carry the wrapped
            // image as a numeric string/varchar, sw the widened image.
            execute("create table x as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b, " +
                    "'-727379968' s, cast('-727379968' as varchar) v, '1000000000000' sw)");

            // The string IN-list never compiles to the JIT; enabling it falls back to Java
            // and matches, so there is no JIT divergence to fix here.
            assertJitMatchesJava("x where (a * b) in ('-727379968')", false);

            // INT-range numeric string wraps the key, matching IN (intLiteral) and '='.
            Assert.assertEquals(1, runQuery("x where (a * b) in ('-727379968')"));         // single const string
            Assert.assertEquals(1, runQuery("x where (a * b) in (-727_379_968)"));           // control: int literal
            Assert.assertEquals(1, runQuery("x where (a * b) = -727_379_968"));              // control: '='
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, '-727379968')"));      // two const, mixed widths
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, 9, '-727379968')"));   // multi const
            Assert.assertEquals(0, runQuery("x where (a * b) not in ('-727379968')"));     // inverse
            Assert.assertEquals(1, runQuery("x where (a * b) in (cast('-727379968' as varchar))")); // varchar const

            // A wider-than-INT numeric string widens the key, matching IN (longLiteral).
            Assert.assertEquals(0, runQuery("x where (a * b) in ('1000000000000')"));      // single const string
            Assert.assertEquals(0, runQuery("x where (a * b) in (1_000_000_000_000)"));        // control: long literal
            Assert.assertEquals(0, runQuery("x where (a * b) in ('999')"));                // matches neither width

            // runtime-const variant: a string bind variable forces InLongRuntimeConstFunction.
            bindVariableService.setStr("sp", "-727379968");
            Assert.assertEquals(1, runQuery("x where (a * b) in (:sp)"));                  // wraps to match
            Assert.assertEquals(1, runQuery("x where (a * b) in (:sp, 7)"));               // mixed runtime + const
            bindVariableService.setStr("swp", "1000000000000");
            Assert.assertEquals(0, runQuery("x where (a * b) in (:swp)"));                 // widens to match

            // var variant: a non-constant string/varchar column forces InLongVarFunction.
            Assert.assertEquals(1, runQuery("x where (a * b) in (s)"));                    // INT-range string col wraps
            Assert.assertEquals(1, runQuery("x where (a * b) in (v)"));                    // INT-range varchar col wraps
            Assert.assertEquals(0, runQuery("x where (a * b) in (sw)"));                   // wider string col widens
            Assert.assertEquals(0, runQuery("x where (a * b) not in (s)"));               // inverse
            Assert.assertEquals(1, runQuery("x where (a * b) in (7, s)"));                 // mixed var + const
        });
    }

    @Test
    public void testInOperatorOverflowSingleValueKeyUnderBooleanEquality() throws Exception {
        // C2 regression (level-3 review): the SINGLE-VALUE IN form never set inKeyWidthOverride,
        // so the key column leaves fell back to the predicate-global widening decision. A boolean
        // equality of an IN check and a LONG comparison - ((a*b) in (c)) = (nl > 0) - is a single
        // predicate, so the LONG sibling turned needsNarrowI64Widening on and the JIT computed the
        // overflowing narrow-int key at 64 bits (10^12) where the Java InLong path wraps it to the
        // INT element's width (-727_379_968). Bugfix 40 fixed this shape for '=' comparisons and
        // Bugfix 38 for multi-value IN lists; the single-value IN sat between them. serializeIn now
        // drives the override for the single-value form too, from its one element.
        //
        // Row 1 overflows: a*b wraps to INT -727_379_968 and widens to LONG 10^12. Row 2 wraps
        // exactly onto INT_MIN (the INT_NULL sentinel): 65_536*32_768. Rows 3+ compute 3*3 = 9.
        // The table has >= 64 rows so the shapes that stay vectorized exercise AVX2.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1_000_000 when x = 2 then 65_536 else 3 end as int) a," +
                    " cast(case when x = 1 then 1_000_000 when x = 2 then 32_768 else 3 end as int) b," +
                    " cast(case when x = 1 then 1_000_000_000_000 when x = 3 then 5 else 0 end as long) nl," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: Java wraps the key against the INT element (row 1 matches the IN
            // check), the JIT widened it (no row matched). not in / <> diverge the same way.
            assertJitMatchesJava("select a from y where ((a*b) in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) not in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) in (-727_379_968)) <> (nl > 0)", true);
            assertJitMatchesJava("select a from y where (nl > 0) = ((a*b) in (-727_379_968))", true);

            // The Java (JIT-disabled) path is the oracle.
            Assert.assertEquals(63, runQuery("select a from y where ((a*b) in (-727_379_968)) = (nl > 0)"));
            Assert.assertEquals(1, runQuery("select a from y where ((a*b) not in (-727_379_968)) = (nl > 0)"));
            Assert.assertEquals(1, runQuery("select a from y where ((a*b) in (-727_379_968)) <> (nl > 0)"));

            // The override also drives the degenerate single-element NULL list: the key wraps
            // (INT width) against the NULL element on both paths, so row 2 - whose product wraps
            // onto INT_MIN, i.e. IS null - matches, exactly as '= null' does.
            assertJitMatchesJava("select a from y where (a*b) in (null)", true);
            Assert.assertEquals(1, runQuery("select a from y where (a*b) in (null)"));
            Assert.assertEquals(1, runQuery("select a from y where (a*b) = null"));

            // controls that already agreed: a LONG element widens the key on both paths; the
            // multi-value list drives the override per element (Bugfix 38); a constant-fold key
            // takes its width from its own comparison; separate predicates never shared the flag.
            assertJitMatchesJava("select a from y where ((a*b) in (1_000_000_000_000)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((a*b) in (5, -727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where ((1_000_000*1_000_000) in (-727_379_968)) = (nl > 0)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968)", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968) and nl > 0", true);
            assertJitMatchesJava("select a from y where (a*b) in (-727_379_968) or nl > 0", true);
        });
    }

    @Test
    public void testInOperatorOverflowWidenColumnArithKeyPerElement() throws Exception {
        // C2 regression: a COLUMN-arithmetic IN key (a * b) whose product overflows INT, in a
        // multi-value list that mixes a genuine-LONG element with an overflowing-INT element.
        // The Java InLong path reads the key per element - widened (getLong) against the LONG
        // element, wrapped (getInt) against the INT element. But the JIT decided narrow-int
        // widening once for the WHOLE predicate: a single coexisting LONG element flipped
        // needsNarrowI64Widening on, so the JIT sign-extended a and b for EVERY comparison,
        // computing a * b at 64 bits (10^12) even against the INT element where Java wraps the
        // key to -727_379_968. A row whose wrapped key matched the INT element matched in Java
        // but not in the JIT (which returned empty). The key width is now driven per element
        // (widen against LONG/TIMESTAMP, wrap against INT), matching the Java path.
        //
        // a*b = 10^12 widens to 10^12, wraps to INT -727_379_968. el = 0 (matches neither image);
        // the INT element -727_379_968 matches only the WRAPPED key.
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int, el long, k timestamp) timestamp(k)");
            execute("insert into y values (1_000_000, 1_000_000, 0, 1)");

            // A LONG COLUMN element coexists with an overflowing-INT constant element. RED on
            // HEAD: the JIT widened the key against the INT element and missed the wrapped match.
            assertJitMatchesJava("select a from y where (a * b) in (el, -727_379_968)", true);    // RED on HEAD
            assertJitMatchesJava("select a from y where (a * b) in (-727_379_968, el)", true);    // element order swapped
            assertJitMatchesJava("select a from y where (a * b) in (5, el, -727_379_968)", true); // plus a plain element
            assertJitMatchesJavaOnEmptyResult("select a from y where (a * b) not in (el, -727_379_968)", true);// inverse

            // The Java (JIT-disabled) path is the oracle: the wrapped key matches the INT element.
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a * b) in (el, -727_379_968)"));
            Assert.assertEquals("a\n", runJavaToString("select a from y where (a * b) not in (el, -727_379_968)"));

            // controls that already agreed: an all-INT list wraps the key, a single LONG element
            // widens it, and '=' wraps - none of these mix widths across the list.
            assertJitMatchesJava("select a from y where (a * b) = -727_379_968", true);           // '=' wraps
            assertJitMatchesJava("select a from y where (a * b) in (-727_379_968)", true);        // single INT: wraps
            assertJitMatchesJava("select a from y where (a * b) in (5, -727_379_968)", true);     // all-INT list: wraps
            assertJitMatchesJavaOnEmptyResult("select a from y where (a * b) in (el)", true);                // single LONG: widens
        });
    }

    @Test
    public void testInOperatorOverflowWidenColumnKeyArithElement() throws Exception {
        // C1 regression: a PLAIN narrow-int COLUMN key (i32/i16/i8) IN a list whose ELEMENT is an
        // overflowing INT arithmetic (m * 3) and which also carries a coexisting genuine-LONG
        // element (nl). The Java InLong path reads the column key per element - wrapped (getInt)
        // against the INT-arith element, so m * 3 = 4_294_967_301 wraps to INT 5 and matches a key
        // holding 5. But the JIT only drove the per-element width override for an ARITHMETIC key,
        // never a plain-column one: with a column key the override stayed unset, so the coexisting
        // LONG element flipped the predicate-global needsNarrowI64Widening on and sign-extended the
        // element's narrow leaf (m). The JIT then computed m * 3 at 64 bits (4_294_967_301, no wrap)
        // and the row that matched in Java (and under '=') matched nothing in the JIT. The key is
        // now width-sensitive for a genuine narrow-int column too, so the arith element wraps
        // against it, matching Java. Existing IN-key column-arith tests only used arithmetic KEYS
        // with arithmetic/LONG elements, never a plain-column key with an arithmetic element.
        //
        // m * 3 = 4_294_967_301 wraps to INT 5 (fits SHORT and BYTE); nl = 99 matches no narrow key.
        assertMemoryLeak(() -> {
            execute("create table t (id long, i32 int, i16 short, i8 byte, m int, nl long, k timestamp) timestamp(k)");
            execute("insert into t values" +
                    " (1, 5, 5, 5, 1_431_655_767, 99, 1)," +   // key holds the WRAPPED image 5 -> matches
                    " (2, 42, 42, 42, 1_431_655_767, 99, 2)"); // no narrow key equals 5 or 99 -> no match

            // RED on HEAD: the LONG element widened the arith element against the column key, so the
            // wrapped match (5) was missed and the JIT returned empty where Java matched id 1.
            assertJitMatchesJava("select id from t where i32 in (m * 3, nl)", true);        // INT key
            assertJitMatchesJava("select id from t where i16 in (m * 3, nl)", true);        // SHORT key
            assertJitMatchesJava("select id from t where i8 in (m * 3, nl)", true);         // BYTE key
            assertJitMatchesJava("select id from t where i32 in (nl, m * 3)", true);        // element order swapped
            assertJitMatchesJava("select id from t where i32 in (7, m * 3, nl)", true);     // plus a plain element
            assertJitMatchesJava("select id from t where i32 not in (m * 3, nl)", true);    // inverse
            // Single-value plain-column key under a boolean equality: the sibling LONG comparison
            // (nl > 0) flips the predicate-global flag on, yet the arith element must still wrap
            // against the column key. Needs the override on the single-value form too.
            assertJitMatchesJava("select id from t where (i32 in (m * 3)) = (nl > 0)", true);

            // The Java (JIT-disabled) path is the oracle: the wrapped element matches id 1 only.
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i32 in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i16 in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where i8 in (m * 3, nl)"));
            Assert.assertEquals("id\n2\n", runJavaToString("select id from t where i32 not in (m * 3, nl)"));
            Assert.assertEquals("id\n1\n", runJavaToString("select id from t where (i32 in (m * 3)) = (nl > 0)"));
            // IN must agree with '=' on the wrapped image (both wrap the arith element at INT width).
            Assert.assertEquals(
                    runJavaToString("select id from t where i32 = m * 3"),
                    runJavaToString("select id from t where i32 in (m * 3, nl)"));

            // Controls that already agreed - none mix an arith element with a LONG element: an
            // all-narrow list wraps, '=' wraps, and a single LONG element widens the key (no match).
            assertJitMatchesJava("select id from t where i32 in (m * 3, 7)", true);         // all-narrow list: wraps
            assertJitMatchesJava("select id from t where i32 = m * 3", true);               // '=' wraps
            assertJitMatchesJavaOnEmptyResult("select id from t where i32 in (nl)", true);               // single LONG: widens
            Assert.assertEquals("id\n", runJavaToString("select id from t where i32 in (nl)"));
        });
    }

    @Test
    public void testInOperatorNullElementBesideLongColumnKeepsNarrowKey() throws Exception {
        // C2 regression: a narrow-INT COLUMN key IN a list carrying a NULL element AND an OBSERVED
        // wide (LONG) COLUMN element. The type observer sees the LONG column and returns I8, so
        // serializeNull emitted the null as LONG_NULL at I8 while the null pairing keeps the key at
        // I4. In wide-lane mode the LONG element's SX_I64 no longer forces scalar, so the four-lane
        // backend compared the i32 key against the i64 LONG_NULL immediate (jit/avx2.h#convert has no
        // i32->i64 case), matching INT_NULL rows only in some lane positions and diverging from the
        // Java InLong path (which matches INT_NULL rows via the null element). The null is now emitted
        // at the key's kept width (INT_NULL at I4). Existing coverage used a wide CONSTANT element,
        // which leaves the observer at I4 and hides the bug. The table has >= 64 rows so the
        // vectorized loop runs past the scalar tail (a 1-row table would hide the divergence).
        assertMemoryLeak(() -> {
            execute("create table t as (select" +
                    " x id," +
                    " cast(case when x % 3 = 0 then null else x end as int) i32," +   // 21 of 64 are INT_NULL
                    " cast(case when x % 3 = 0 then null else x end as short) i16," +  // SHORT: null casts to 0, never null
                    " cast(case when x % 3 = 0 then null else x end as byte) i8," +    // BYTE: null casts to 0, never null
                    " cast(10_000_000_000 as long) nl," +                             // out of INT range: matches no widened key
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // RED on HEAD: JIT wide-lane drops (position-dependently) the INT_NULL rows the null
            // element matches; Java matches all of them (on HEAD the INT key returned 10 of 21).
            assertJitMatchesJava("select id from t where i32 in (nl, null)", true);      // wide COLUMN element + null
            assertJitMatchesJava("select id from t where i32 in (null, nl)", true);      // element order swapped
            assertJitMatchesJava("select id from t where i32 in (7, nl, null)", true);   // plus a plain element

            // The Java (JIT-disabled) oracle: the null element matches exactly the INT_NULL rows
            // (x % 3 == 0 -> 21 of 64); nl matches none.
            Assert.assertEquals(21, runQuery("select id from t where i32 in (nl, null)"));

            // SHORT/BYTE keys are also width-sensitive (see isWidthSensitiveInKey) and take the I4
            // override, so the fix routes their null pairing through INT_NULL at I4 too. Those types
            // are never null (a null cast lands on 0), so the null element matches nothing - the i16/i8
            // key value never equals INT_NULL - and JIT agrees with Java at 0 rows.
            assertJitMatchesJavaOnEmptyResult("select id from t where i16 in (nl, null)", true);
            assertJitMatchesJavaOnEmptyResult("select id from t where i8 in (nl, null)", true);
            Assert.assertEquals(0, runQuery("select id from t where i16 in (nl, null)"));
            Assert.assertEquals(0, runQuery("select id from t where i8 in (nl, null)"));

            // Control that agreed before the fix: a wide CONSTANT element leaves the observer at I4,
            // so the null already emitted INT_NULL and matched.
            assertJitMatchesJava("select id from t where i32 in (5_000_000_000, null)", true);
            Assert.assertEquals(21, runQuery("select id from t where i32 in (5_000_000_000, null)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenConstOperandKeyPerElement() throws Exception {
        // C1 regression: an IN key that is a narrow-int column arithmetic with a CONSTANT operand
        // - (a * 3), (a + const), (n - const) - whose product/sum overflows INT, in a multi-value
        // list mixing a genuine-LONG element with the in-INT-range element the WRAPPED key equals.
        // The column leaf honored the per-element key width (maybeEmitI64Widening) but the constant
        // operand did not: it was backfilled at predicate exit, after serializeIn reset the width
        // override, so a coexisting LONG element emitted it at I8 and the JIT computed the whole
        // product at long width (no wrap), missing the wrapped match Java keeps (getInt). Existing
        // IN-key column-arith tests only used two-column keys (a*b)/(a+a), never a constant operand.
        //
        // a*3 = 3e9 wraps to INT -1_294_967_296, widens to 3e9; the INT element matches only the WRAP.
        assertMemoryLeak(() -> {
            execute("create table z (a int, n int, k timestamp) timestamp(k)");
            execute("insert into z values (1_000_000_000, -2_000_000_000, 1)");

            // RED on HEAD: the LONG element widened the key against the INT element, missing the wrap.
            assertJitMatchesJava("select a from z where (a * 3) in (5_000_000_000, -1_294_967_296)", true);
            assertJitMatchesJava("select a from z where (a * 3) in (-1_294_967_296, 5_000_000_000)", true); // order swapped
            assertJitMatchesJava("select a from z where (a * 3) in (5, 5_000_000_000, -1_294_967_296)", true); // extra element
            assertJitMatchesJavaOnEmptyResult("select a from z where (a * 3) not in (5_000_000_000, -1_294_967_296)", true); // inverse
            // '+' and '-' operand forms overflow the same way; '-' underflows via the n column.
            assertJitMatchesJava("select a from z where (a + 2_000_000_000) in (5_000_000_000, -1_294_967_296)", true);
            assertJitMatchesJava("select a from z where (n - 1_000_000_000) in (-5_000_000_000, 1_294_967_296)", true);

            // The Java (JIT-disabled) path is the oracle: the wrapped key matches the INT element.
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (a * 3) in (5_000_000_000, -1_294_967_296)"));
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (a + 2_000_000_000) in (5_000_000_000, -1_294_967_296)"));
            Assert.assertEquals("a\n1000000000\n", runJavaToString("select a from z where (n - 1_000_000_000) in (-5_000_000_000, 1_294_967_296)"));
            Assert.assertEquals("a\n", runJavaToString("select a from z where (a * 3) not in (5_000_000_000, -1_294_967_296)"));

            // controls that already agreed - none mix widths across the list: '=' wraps, a single
            // INT element wraps, a single LONG element widens (matches neither image, so no row).
            assertJitMatchesJava("select a from z where (a * 3) = -1_294_967_296", true);
            assertJitMatchesJava("select a from z where (a * 3) in (-1_294_967_296)", true);
            assertJitMatchesJavaOnEmptyResult("select a from z where (a * 3) in (5_000_000_000)", true);
            Assert.assertEquals("a\n", runJavaToString("select a from z where (a * 3) in (5_000_000_000)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenElementPerPairing() throws Exception {
        // C1/C2: an overflowing INT-arithmetic IN key against a list that mixes a genuine-LONG
        // element with an overflowing INT-arith element (C1) or a NULL element (C2). The Java
        // InLong path reads the key per element: wrapped (getInt) against the INT-arith and NULL
        // elements, widened (getLong) against the LONG one. The JIT set the key width per element
        // but left the element on the predicate-global widen flag, so a coexisting LONG element
        // over-widened the INT-arith element (C1) and, with the NULL element, the key too (C2).
        // serializeIn now reads both the element and the key at the pairing width.
        //
        // row1: a*b and c*d both wrap to -727_379_968 (widen to 10^12); el=999 matches neither.
        // row2: a*b = 2^31 wraps to INT_MIN (== INT_NULL), so the key IS null; c*d = 1; el=999.
        assertMemoryLeak(() -> {
            execute("create table y (a int, b int, c int, d int, el long, k timestamp) timestamp(k)");
            execute("insert into y values (1_000_000, 1_000_000, 1_000_000, 1_000_000, 999, 1)," +
                    " (2, 1_073_741_824, 1, 1, 999, 2)");

            // C1: the INT-arith element must wrap with the key. RED on HEAD - the key wrapped
            // but the element widened, so the (key = c*d) pairing missed the wrapped match.
            assertJitMatchesJava("select a from y where (a*b) in (c*d, el)", true);
            assertJitMatchesJava("select a from y where (a*b) in (el, c*d)", true);
            assertJitMatchesJava("select a from y where (a*b) in (5, c*d, el)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (c*d, el)", true);
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a*b) in (c*d, el)"));

            // C2: the NULL element wraps the key (INT width), so row2 - whose product wraps onto
            // INT_MIN, i.e. IS null, as '= null' and the projection both report - matches it. The
            // coexisting LONG element must not widen the key for the NULL pairing.
            assertJitMatchesJava("select a from y where (a*b) in (null, el)", true);
            assertJitMatchesJava("select a from y where (a*b) in (el, null)", true);
            assertJitMatchesJava("select a from y where (a*b) not in (null, el)", true);
            Assert.assertEquals("a\n2\n", runJavaToString("select a from y where (a*b) in (null, el)"));
            Assert.assertEquals("a\n2\n", runJavaToString("select a from y where (a*b) = null"));
            Assert.assertEquals("a\n1000000\n", runJavaToString("select a from y where (a*b) not in (null, el)"));

            // control: an all-narrow list wraps both key and element (no width to mix).
            assertJitMatchesJava("select a from y where (a*b) in (c*d, 5)", true);
        });
    }

    @Test
    public void testInOperatorOverflowWidenKeyOnLongElement() throws Exception {
        // An overflowing INT *arithmetic* KEY on the left of IN() against a LONG element.
        // 'in (LONG)' is 'key = LONG', which resolves the LONG comparison - and that reads the
        // key through getLong(), an exact sign extension of the value it already wrapped to.
        // So the key carries -727_379_968 here, not 10^12, and IN, '=' and CAST all agree on
        // that. The JIT has to reach the same answer, which is what this pins.
        //
        // Rows: a*b wraps to -727_379_968 (both rows); el row1 = -727_379_968 (the wrapped
        // value), el row2 = 10^12; ei = -727_379_968 (both rows).
        assertMemoryLeak(() -> {
            execute("create table x (id long, a int, b int, el long, ei int, k timestamp) timestamp(k)");
            execute("insert into x values (1, 1_000_000, 1_000_000, -727_379_968, -727_379_968, 1)," +
                    " (2, 1_000_000, 1_000_000, 1_000_000_000_000, -727_379_968, 2)");

            // A LONG COLUMN element forces InLongVarFunction (non-constant). The key carries
            // its wrapped value, so it matches row 1, NOT row 2. '=', CAST and the JIT all
            // agree, so JIT-vs-Java parity holds too.
            assertJitMatchesJava("select id from x where (a * b) in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) not in (el)", true);
            assertJitMatchesJava("select id from x where (a * b) = el", true);              // control: '='
            // absolute correctness: the wrapped key selects row 1, not row 2.
            Assert.assertEquals("id\n1\n", runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals("id\n2\n", runJavaToString("select id from x where (a * b) not in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where (a * b) = el"),
                    runJavaToString("select id from x where (a * b) in (el)"));
            Assert.assertEquals(
                    runJavaToString("select id from x where cast(a * b as long) in (el)"),
                    runJavaToString("select id from x where (a * b) in (el)"));

            // A LONG CONSTANT element cannot reach the pre-wrap product either, so 10^12 now
            // matches nothing while the wrapped value matches both rows. Each empty assertion
            // is paired with its complement, so parity never becomes vacuous.
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) = 1_000_000_000_000", true);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000)", true);        // single
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000, 5)", true);     // two
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (1_000_000_000_000, 5, 6)", true);  // multi
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) > 999_999_999_999", true);            // relational
            assertJitMatchesJava("select id from x where (a * b) = -727_379_968", true);
            assertJitMatchesJava("select id from x where (a * b) in (-727_379_968)", true);
            assertJitMatchesJava("select id from x where (a * b) not in (1_000_000_000_000)", true);
            Assert.assertEquals("id\n", runJavaToString("select id from x where (a * b) = 1_000_000_000_000"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) = -727_379_968"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) not in (1_000_000_000_000)"));

            // A runtime-constant (bind variable) LONG element forces InLongRuntimeConstFunction.
            // Bound to the pre-wrap product it matches nothing; bound to the wrapped one it matches
            // both rows, so the pair still bites.
            bindVariableService.setLong("p", 1_000_000_000_000L);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) in (:p)", true);
            assertJitMatchesJavaOnEmptyResult("select id from x where (a * b) = :p", true);
            Assert.assertEquals("id\n", runJavaToString("select id from x where (a * b) in (:p)"));
            bindVariableService.setLong("p", -727_379_968L);
            assertJitMatchesJava("select id from x where (a * b) in (:p)", true);
            assertJitMatchesJava("select id from x where (a * b) = :p", true);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (:p)"));

            // Control - an INT element must still WRAP the key (matching EqInt and the JIT).
            // master read the key via getLong() and WIDENED it, so IN wrongly returned no row
            // where '=' matched both; the wrapped key now matches the stored wrapped image.
            assertJitMatchesJava("select id from x where (a * b) in (ei)", true);
            assertJitMatchesJava("select id from x where (a * b) = ei", true);
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (ei)"));
            Assert.assertEquals("id\n1\n2\n", runJavaToString("select id from x where (a * b) in (1_000_000 * 1_000_000)"));
        });
    }

    @Test
    public void testInOperatorOverflowWidenLongKeyArithElementWithFloat() throws Exception {
        // C3 regression (level-3 review): a LONG IN key against a narrow-int COLUMN-ARITHMETIC
        // element (a*b), inside a boolean equality with a FLOAT/DOUBLE sibling. A float suppresses
        // the predicate-global narrow-i64 widening, so markFloatI64WidenLeaves is the only pass that
        // can sign-extend the element. It returned at the IN FUNCTION node and never descended, so
        // the JIT left a*b at INT width without the sign extension the Java path applies. Java does
        // not read the element at long width either: InLong takes a narrow element through
        // getLong(), which is Numbers.intToLong(getInt()) for every INT/SHORT/BYTE function
        // (IntFunction:149-152, and InLongFunctionFactory states the consequence in its own
        // comments at :183-186 and :465-468), so it hands the key the SIGN-EXTENDED WRAP,
        // -727_379_968. inKeyWidthOverride cannot help - it fires only for a narrow-int key.
        // markI64Widen now descends into the IN and widens each element the key reads at long
        // width, deriving the width per element (mirroring markI64WidenFoldRoots).
        //
        // a*b = 10^12 wraps to INT -727_379_968, and WIDENING that element means sign-extending
        // the wrapped RESULT, not recomputing the product at long width. What the fixture pins is
        // that both paths hand the key that same 64-bit value, and nl carries one probe per way of
        // getting it wrong: 10^12 in row 1 is the image an element recomputed at long width would
        // match, -727_379_968 in row 3 is the sign-extended wrap the element must match, and
        // 3_567_587_328 - the ZERO-extension of that same wrap - is deliberately absent from nl.
        // Every IN shape below whose element is a*b therefore answers with cs = 3; a cs = 1 answer
        // means the element was recomputed at long width, and an empty answer means it was
        // zero-extended. Row 3 is what makes the NEGATIVE element observable at all: without it
        // every such shape answered empty, and a zero-extending backend passed unnoticed.
        assertMemoryLeak(() -> {
            execute("create table w as (select cast(1_000_000 as int) a, cast(1_000_000 as int) b," +
                    " cast(case x when 1 then 1_000_000_000_000 when 2 then 2" +
                    " when 3 then -727_379_968 else 0 end as long) nl," +
                    " cast(1.0 as float) f32, cast(1.0 as double) f64," +
                    " x::short cs, x::byte cbyte," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");

            // Primary repro: the element a*b reaches the LONG key as the sign-extended INT wrap on
            // both paths, so it matches nl row 3 and nothing else. Row 1 (nl = 10^12) is the row an
            // element recomputed at long width would pull in instead.
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f32 > 0)", true, "cs\n3\n");
            // Single-value IN (unrolled path), operand order, non-zero float threshold, DOUBLE
            // sibling: all present the element to the key the same way.
            assertJitMatchesJava("select cs from w where (nl in (a*b)) = (f32 > 0)", true, "cs\n3\n");
            assertJitMatchesJava("select cs from w where (f32 > 0) = (nl in (a*b, 7))", true, "cs\n3\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f32 > 0.5)", true, "cs\n3\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = (f64 > 0)", true, "cs\n3\n");
            // NOT / not in flip the match to the complementary rows.
            assertJitMatchesJava("select cs from w where not ((nl in (a*b, 7)) = (f32 > 0))", true,
                    "cs\n1\n2\n4\n5\n");
            assertJitMatchesJava("select cs from w where (nl not in (a*b, 7)) = (f32 > 0)", true,
                    "cs\n1\n2\n4\n5\n");

            // Over-widening guard: the SAME a*b appears twice - as an IN element against the LONG
            // key, and inside a sibling INT-width comparison. markI64WrapArithLeaves must keep the
            // wrap-side product at INT width, where it is -727_379_968, so the right-hand side is
            // true on every row; the left-hand side is true on row 3 alone, and true = true keeps
            // exactly that row. An over-widened wrap side flips the right-hand side false on every
            // row and returns the complement - 1, 2, 4 and 5 - so parity, not just the pin, catches
            // it.
            assertJitMatchesJava("select cs from w where (nl in (a*b, 7)) = ((a*b) = -727_379_968)", true,
                    "cs\n3\n");

            // Controls that must keep passing. Plain narrow COLUMN elements sign-extend value-
            // preservingly (row 2: nl = 2 matches cs = 2), and neither cs nor cbyte reaches nl's
            // row 3, so that row stays the a*b probe alone; a coexisting LONG-const element changes
            // nothing for the arith element, and 999_999_999_999 is one short of nl's 10^12 so it
            // adds no row of its own; AND/OR split into separate float-free predicates.
            assertJitMatchesJava("select cs from w where (nl in (cs, cbyte)) = (f32 > 0)", true, "cs\n2\n");
            assertJitMatchesJava("select cs from w where (nl in (a*b, 999_999_999_999)) = (f32 > 0)", true, "cs\n3\n");
            assertJitMatchesJava("select cs from w where nl in (a*b, 7) and f32 > 0", true, "cs\n3\n");
            assertJitMatchesJava("select cs from w where nl in (a*b, 7) or f32 > 0", true,
                    "cs\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testInOperatorSingleValue() throws Exception {
        // Tests single-value IN() which has a special unrolled code path in CompiledFilterIRSerializer
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueChained() throws Exception {
        // Tests multiple single-value IN() conditions chained with AND/OR
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 5, 0) a," +
                " rnd_int(1, 5, 0) b," +
                " rnd_int(1, 5, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("a")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1)")
                .withBooleanOperator()
                .withAnyOf("b")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(2)")
                .withBooleanOperator()
                .withAnyOf("c")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueWithBooleanOperators() throws Exception {
        // Tests single-value IN() combined with AND/OR for short-circuit evaluation
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(5)")
                .withBooleanOperator()
                .withAnyOf("i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(7)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorSingleValueWithNull() throws Exception {
        // Tests single-value IN() with null, special unrolled code path
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(null)", "(5)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorSymbolAndCharKeysAreNotWidthSensitive() throws Exception {
        // Scoping guard for the C1 fix (isWidthSensitiveInKey). SYMBOL and CHAR IN keys map to the
        // same narrow type code as a genuine BYTE/SHORT/INT key (columnTypeCode collapses SYMBOL and
        // IPv4 onto I4, CHAR onto I2), and both compile to the JIT. But they route through
        // InSymbol/InChar, NOT the width-sensitive InLong path, so they must NOT take the per-element
        // width override. isWidthSensitiveInKey keys off the real column type tag (not the collapsed
        // code) so these stay untouched; simply dropping the OPERATION guard would flag any I4/I2 key
        // and hand the override to symbol/char keys too. That stays result-correct (sign-extending a
        // symbol/char key is value-preserving), so this test cannot catch it directly - but it emits
        // an unnecessary SX_I64 that forces the whole filter onto the scalar path, a JIT performance
        // regression for what today vectorizes. This test locks the correctness invariant: symbol and
        // char IN keys still produce identical JIT and Java results.
        assertMemoryLeak(() -> {
            execute("create table s (sym symbol, ch char, ip ipv4, k timestamp) timestamp(k)");
            execute("insert into s values ('a','x','1.1.1.1', 1)," +
                    " ('b','y','2.2.2.2', 2)," +
                    " ('c','z','3.3.3.3', 3)");

            // SYMBOL and CHAR IN keys compile to the JIT; the result must match Java exactly.
            assertJitMatchesJava("select k from s where sym in ('a','c')", true);
            assertJitMatchesJava("select k from s where sym not in ('a','c')", true);
            assertJitMatchesJava("select k from s where ch in ('x','z')", true);
            assertJitMatchesJava("select k from s where ch not in ('x','z')", true);
            // IPv4 IN with string constants also compiles to the JIT: serializeConstant parses the
            // dotted-quad literal into its I4 key. The IN key is IPv4, not BYTE/SHORT/INT, so
            // isWidthSensitiveInKey leaves it alone for the same reason it leaves SYMBOL and CHAR
            // alone, even though columnTypeCode collapses IPv4 onto I4 as well.
            assertJitMatchesJava("select k from s where ip in ('1.1.1.1','3.3.3.3')", true);

            // Absolute oracle: the symbol/char keys select the expected rows.
            Assert.assertEquals("k\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000003Z\n",
                    runJavaToString("select k from s where sym in ('a','c')"));
            Assert.assertEquals("k\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000003Z\n",
                    runJavaToString("select k from s where ch in ('x','z')"));
        });
    }

    @Test
    public void testInOperatorTwoValues() throws Exception {
        // Tests two-value IN() which also has an unrolled code path (args.size() < 3)
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) i32," +
                " rnd_long(1, 10, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, 7)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorTwoValuesWithNull() throws Exception {
        // Tests two-value IN() with null, special unrolled code path
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 5) i32," +
                " rnd_long(1, 10, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(3, null)", "(null, 7)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorWithBooleanOperators() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 20, 0) i32," +
                " rnd_long(1, 20, 0) i64," +
                " rnd_float() f32," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, 3)", "(10, 11, 12)")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withComparisonOperator()
                .withAnyOf("0.5");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testInOperatorWithBooleanOperatorsAndNull() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 20, 5) i32," +
                " rnd_long(1, 20, 5) i64," +
                " rnd_float(5) f32," +
                " rnd_double(5) f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64")
                .withAnyOf(" in ", " not in ")
                .withAnyOf("(1, 2, null)", "(null, 10, 11)")
                .withBooleanOperator()
                .withAnyOf("f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testInOperatorWithOrChain() throws Exception {
        // Tests IN operator combined with OR chain
        final String query = "x where " +
                "a in (1, 2, 3) or b in (4, 5, 6) or c > 90";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) a," +
                " rnd_int(1, 10, 0) b," +
                " rnd_long(0, 100, 0) c" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testIntColumnVsOutOfRangeConstant() throws Exception {
        // A narrow-int column compared against an integer constant beyond INT range
        // reads at long width in the Java filter (the constant is a LONG literal that
        // promotes the INT column via getLong), so no INT value equals it. The JIT
        // type observer sees only columns, so it typed the constant down to the INT
        // width, and serializeNumber emitted it as a 32-bit float on the int-parse
        // overflow; floats near 2^31 are spaced 256 apart, so distinct INT rows
        // (2_147_483_647 / 2_147_483_646) collapsed onto one float and matched spuriously.
        // The serializer now sign-extends the narrow leaf and emits the constant as a
        // full I8 IMM, keeping the comparison at long width so both paths agree.
        assertMemoryLeak(() -> {
            execute("create table t (i8 byte, i16 short, i32 int, i64 long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values " +
                    "(127, 32_767, 2_147_483_647, 9_223_372_036_854_775_806, 0)," +
                    "(126, 32_766, 2_147_483_646, 9_223_372_036_854_775_805, 1_000_000)," +
                    "(null, null, null, null, 2_000_000)");
            // Absolute pins on the repro shapes. Parity alone is not an oracle here: the two INT rows
            // that the pre-fix JIT collapsed onto one float differ by 1, so a shared wrong width keeps
            // or drops both together and JIT-vs-Java parity still holds. maxRow and nextRow name them
            // individually, so a collapse fails the pin.
            final String header = "i8\ti16\ti32\ti64\tts\n";
            final String maxRow = "127\t32767\t2147483647\t9223372036854775806\t1970-01-01T00:00:00.000000Z\n";
            final String nextRow = "126\t32766\t2147483646\t9223372036854775805\t1970-01-01T00:00:01.000000Z\n";
            final String nullRow = "0\t0\tnull\tnull\t1970-01-01T00:00:02.000000Z\n";
            // All-narrow path (only i32 observed): the constant would fall back to float. No INT value
            // equals 2^31, so the equality keeps nothing - that is exactly the spurious match the
            // pre-fix JIT produced.
            assertJitMatchesJavaOnEmptyResult("t where i32 = 2_147_483_648", true, header);
            assertJitMatchesJavaOnEmptyResult("t where i32 >= 2_147_483_648", true, header);
            assertJitMatchesJavaOnEmptyResult("t where i32 > 2_147_483_648", true, header);
            assertJitMatchesJava("t where i32 <= 2_147_483_648", true, header + maxRow + nextRow);
            assertJitMatchesJava("t where i32 < 2_147_483_648", true, header + maxRow + nextRow);
            assertJitMatchesJava("t where i32 <> 2_147_483_648", true, header + maxRow + nextRow + nullRow);
            assertJitMatchesJavaOnEmptyResult("t where i32 = 5_000_000_000", true, header);
            // Negative out-of-range constant (unary minus of an overflowing literal).
            assertJitMatchesJavaOnEmptyResult("t where i32 = -3_000_000_000", true, header);
            assertJitMatchesJava("t where i32 > -3_000_000_000", true, header + maxRow + nextRow);
            // Mixed-size path (i32 + i64 observed): the column must sign-extend too.
            assertJitMatchesJavaOnEmptyResult("t where i32 = 2_147_483_648 and i64 > 0", true, header);
            // The in-range constant on that same path, pinned on a row it MATCHES. "i32 = 5" was
            // here instead, and no row carries 5, so it observed nothing: an empty result reads the
            // same whether the mixed-size path sign-extends the column or mangles it. Naming
            // 2_147_483_646 makes the line discriminate - it is the row the float collapse merged
            // with 2_147_483_647, so a return to f32 width here matches BOTH rows and fails the pin.
            assertJitMatchesJava("t where i32 = 2_147_483_646 and i64 > 0", true, header + nextRow);
            // ... and the never-matching spelling stays as a declared-empty control beside it.
            assertJitMatchesJavaOnEmptyResult("t where i32 = 5 and i64 > 0", true, header);
            // Single- and multi-value IN, including mixed in-range / out-of-range elements.
            assertJitMatchesJavaOnEmptyResult("t where i32 in (2_147_483_648)", true, header);
            assertJitMatchesJavaOnEmptyResult("t where i32 in (2_147_483_648, 5)", true, header);
            assertJitMatchesJavaOnEmptyResult("t where i32 in (5, 2_147_483_648)", true, header);
            // NULL is a comparable sentinel here, not SQL unknown, so the null row survives the
            // negated forms - the same way it survives i32 <> 2_147_483_648 above.
            assertJitMatchesJava("t where i32 not in (2_147_483_648, 5)", true, header + maxRow + nextRow + nullRow);
            // OR chain: the same column appears at two widths in one predicate. The in-range arm must
            // still tell 2_147_483_647 apart from 2_147_483_646 - the float collapse matched both.
            assertJitMatchesJava("t where i32 = 2_147_483_648 or i32 = 2_147_483_647", true, header + maxRow);
            // BYTE / SHORT column vs an out-of-INT-range constant: previously declined
            // JIT (serializeNumber threw on the int-parse overflow); now stays on JIT.
            assertJitMatchesJavaOnEmptyResult("t where i8 = 2_147_483_648", true, header);
            assertJitMatchesJavaOnEmptyResult("t where i16 = 3_000_000_000", true, header);
            // Control: a pure LONG column already computes at long width.
            assertJitMatchesJavaOnEmptyResult("t where i64 = 2_147_483_648", true, header);
        });
    }

    @Test
    public void testIntWideLaneBatchLengthsAndBooleanMasks() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_i");
                if (rowCount == 0) {
                    execute("create table wide_i (i32 int)");
                } else {
                    execute("create table wide_i as (select cast(case x "
                            + "when 1 then null when 2 then -2_147_483_647 "
                            + "when 3 then 7 else x end as int) i32 "
                            + "from long_sequence(" + rowCount + "))");
                }

                assertJitMatchesJavaOnBatchLengths("wide_i where i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 = 7 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 < 5_000_000_000 and i32 = 7", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 = 7 or i32 > 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_i where i32 > 5_000_000_000 or i32 = 7", true);
                assertJitMatchesJava("select count() from wide_i where i32 < 5_000_000_000", true);
                assertJitMatchesJava("select count() from wide_i "
                        + "where i32 = 7 or i32 > 5_000_000_000", true);
            }
            // Row parity cannot see which loop ran - the scalar loop answers these shapes too - so
            // the sweep stays green if the wide-lane routing is reverted. Pin the mode the test is
            // named for. getExecHint() reads the metadata and the parsed filter, not the row count,
            // so one block after the sweep covers every iteration of it.
            assertExecHint("wide_i", "i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_i", "i32 = 7 and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_i", "i32 < 5_000_000_000 and i32 = 7", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_i", "i32 = 7 or i32 > 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_i", "i32 > 5_000_000_000 or i32 = 7", EXEC_HINT_WIDE_LANE);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneFloatArithmeticVsIntegerConstant() throws Exception {
        // f32 + f64 harmonises to f64 through convert()'s f32 arm, and the comparison then puts that
        // f64 against the bare integer bound. serializeUntypedNumber keeps such a bound at I4
        // whenever the predicate carries a float (so that i32 * 2 still wraps mod 2^32), so the IR is
        // (i32 5)(f64 ...)(<) - and avx2::convert() had no f64-with-i32 arm, so it fell through with
        // the operands unchanged and emitted vcmppd against a vpbroadcastd register. The bound 5 was
        // read as 0x0000000500000005 = 1.06e-314 and a negative bound as a NaN, so the four-lane loop
        // selected almost nothing while the scalar loop and Java agreed. convert() now widens the i32
        // side, so all three run the same comparison.
        Assume.assumeTrue("wide-lane lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // rnd_float()/rnd_double() draw from [0, 1), so every bound below holds for EVERY row
            // whatever the seed: the expected count is the row count, not a property of the draw.
            // (Integer-valued float columns do not reproduce this - the bound has to land where the
            // misread denormal separates it from the real one.)
            execute("create table wlf as (select timestamp_sequence(0, 1_000_000) k," +
                    " rnd_float() f32, rnd_double() f64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            final String allRows = "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n";
            // The inexact float bound is what flips the whole filter to four lanes; the arithmetic
            // comparison beside it is correct in isolation and was dragged in by it.
            assertJitScalarAndVectorMatchJava("select count() from wlf where f32 + f64 < 5 and f32 < 1.00000003", allRows);
            // A negative bound was misread as a NaN rather than as a small denormal, so every
            // comparison against it came out false. Both operand orders reach the same pairing.
            assertJitScalarAndVectorMatchJava("select count() from wlf where f64 + f32 > -1 and f32 < 1.00000003", allRows);
            assertJitScalarAndVectorMatchJava("select count() from wlf where f64 >= (1.5 + f32) * -3 and f32 < 1.00000003", allRows);
            // The count is the same whichever loop produces it, so pin the loop as well: the
            // inexact float bound is what earns four lanes, and reverting that eligibility would
            // leave every assertion above standing.
            assertExecHint("wlf", "f32 + f64 < 5 and f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlf", "f64 + f32 > -1 and f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlf", "f64 >= (1.5 + f32) * -3 and f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
        });
    }

    @Test
    public void testWideLaneInNarrowElementUnderConstantKey() throws Exception {
        // markWidthSemantics harmonises an IN element against the key only when the pairing folds to
        // I8, and a CONSTANT key that fits in an int folds to I4 - so the i32 element got no SX_I64.
        // The key constant is emitted at I8 anyway, because serializeUntypedNumber follows the
        // predicate's type observer and the LONG element put I8 in it. That left an i64 key against
        // an i32 element in the four-lane loop, which loads the INT column as four packed i32 in the
        // low half of the register. isWidthSensitiveInKey now covers a numeric CONSTANT key, so the
        // per-element override drives the key's width from each element in turn.
        Assume.assumeTrue("wide-lane lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            execute("create table wlk as (select timestamp_sequence(0, 1_000_000) k," +
                    " (x % 4)::int i32, ((x % 4) + 1) * 1_000_000_000L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // The key matches through the INT element (i32 == 0): the pairing the fix harmonises.
            assertJitScalarAndVectorMatchJava("select count() from wlk where (0 in (i32, i64)) and i32 < i64",
                    "count\n128\n");
            assertJitScalarAndVectorMatchJava("select count() from wlk where (3 in (i32, i64)) and i32 < i64",
                    "count\n129\n");
            // A key that only the LONG element can match must keep matching through it.
            assertJitScalarAndVectorMatchJava("select count() from wlk where (2000000000 in (i32, i64)) and i32 < i64",
                    "count\n129\n");
            // The per-element key width the fix drives only matters inside the four-lane loop, and
            // the counts above hold on the scalar loop as well. Pin that these do take four lanes.
            assertExecHint("wlk", "(0 in (i32, i64)) and i32 < i64", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlk", "(3 in (i32, i64)) and i32 < i64", EXEC_HINT_WIDE_LANE);
            assertExecHint("wlk", "(2_000_000_000 in (i32, i64)) and i32 < i64", EXEC_HINT_WIDE_LANE);
        });
    }

    @Test
    public void testInNullElementNonNullableNarrowKeyFolds() throws Exception {
        // BYTE and SHORT carry no NULL sentinel, so a NULL element matches nothing - which is what
        // the Java IN functions return. On master the whole filter declined here (serializeNull
        // throws "byte type is not nullable"); the untyped-NULL-at-INT-width rule made the element
        // compile as an I4 immediate instead, so the four-lane loop compared an i8 column against
        // vpbroadcastd(0x80000000), whose byte layout is 00 00 00 80 - three of every four lanes
        // testing the column against 0. serializeIn now folds the pairing away.
        //
        // The fold is a frontend rule and the divergence showed up in the SINGLE-SIZE AVX2 loop, not
        // the four-lane one (a BYTE or SHORT IN key is not wide-lane eligible), so this test is not
        // AVX2-gated. It still only DETECTS the regression on an AVX2 host - without AVX2
        // compiler.cpp falls back to the scalar loop, whose convert() has the complete int table -
        // and ARM64, which setUp()'s JitUtil.isJitSupported() gate admits, runs that same scalar
        // loop unconditionally.
        // The host-independent guarantee is the IR pin in CompiledFilterIRSerializerTest.
        assertMemoryLeak(() -> {
            execute("create table wln as (select timestamp_sequence(0, 1_000_000) k," +
                    " (x % 5)::byte i8, (x % 7)::short i16, (x % 3)::char c," +
                    " x::int i32, (x * 1_000_000_000)::long i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // A NULL-only list is false on every row, and it must stay vectorized rather than decline.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null)", "count\n0\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i16 in (null)", "count\n0\n");
            // A NULL beside a real element must not disturb that element's own matches.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1)", "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i16 in (null, 1)", "count\n74\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1) and i16 in (null, 2)",
                    "count\n15\n");

            // Every key shape isWidthSensitiveInKey accepts folds, not just a bare column. A unary
            // minus reads at BYTE width and, unlike binary narrow arithmetic, does not force scalar
            // mode - so it was the shape that stayed wrong after the column and bind-variable keys
            // were covered.
            assertJitScalarAndVectorMatchJava("select count() from wln where -i8 in (null)", "count\n0\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where -i16 in (null, -1)", "count\n74\n");

            // NOT IN inverts the fold, so the never-matching pairing has to come out true for every
            // row - the answer the Java filter gives.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 not in (null)",
                    "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 not in (null, 1)", "count\n412\n");

            // A sibling INT-vs-LONG comparison mixes the fold's size-1 mask with wider ones in the
            // same predicate (mixed sizes, so this runs the scalar loop rather than four lanes).
            // Both the empty and the non-empty answer are pinned: the fold emits an all-zero mask,
            // which reads the same at every lane width.
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null, 1) and i32 < i64",
                    "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where i8 in (null) and i32 < i64", "count\n0\n");

            // A numeric CONSTANT key reaches the same pairing from the other side: the key is the
            // constant and the narrow column is the element. Making that key width-sensitive is what
            // fixes the constant-key IN test below, and it also stops serializeNull declining here,
            // so the pairing has to fold or it returns three of every four rows.
            assertJitScalarAndVectorMatchJava("select count() from wln where 0 in (null, i8)", "count\n103\n");
            assertJitScalarAndVectorMatchJava("select count() from wln where 0 in (null, i16)", "count\n73\n");
            // A key with nothing but NULL elements has no column left to read, so the filter never
            // reaches the JIT at all; pin that it still answers no rows.
            assertJitMatchesJava("select count() from wln where 0 in (null)", false, "count\n0\n");

            // CHAR shares the I2 type code but DOES read (char) 0 as NULL, so it must keep the
            // ordinary pairing. It declines the JIT, as it did before, and the Java filter answers.
            assertJitMatchesJava("select count() from wln where c in (null)", false, "count\n171\n");
        });
    }

    @Test
    public void testWideLaneFloatArithmeticAndInBatchLengths() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_mixed");
                if (rowCount == 0) {
                    execute("create table wide_mixed (i32 int, i64 long, f32 float)");
                } else {
                    execute("create table wide_mixed as (select "
                            + "cast(case x when 1 then null when 2 then -7 else x end as int) i32, "
                            + "cast(case x when 1 then null else 2 end as long) i64, "
                            + "cast(case x when 1 then null when 2 then 1.0 else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }

                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 = 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 + 0 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where f32 in (1.00000003, 2.5)", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where i32 * i64 = 14", true);
                assertJitMatchesJavaOnBatchLengths("wide_mixed where i32 * 2 in (1, 5_000_000_000)", true);
                assertJitMatchesJava("select count() from wide_mixed where f32 > 0.99999998", true);
                assertJitMatchesJava("select count() from wide_mixed where i32 * 2 in (1, 5_000_000_000)", true);
            }
            // The sweep asserts parity only, which every loop delivers. Pin which loop each shape
            // takes - and they do not all take the same one. The float comparisons and the I8-typed
            // product are wide-lane eligible; "i32 * 2" is an I4-typed arithmetic key against a
            // 64-bit list, so forceScalarOnUnharmonisedNarrowArith() drops it onto the scalar
            // backend instead.
            assertExecHint("wide_mixed", "f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "f32 = 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "f32 + 0 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "f32 in (1.00000003, 2.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "f32 > 0.99999998", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "i32 * i64 = 14", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_mixed", "i32 * 2 in (1, 5_000_000_000)", EXEC_HINT_SCALAR);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneInElementNarrowColumnUnderLongKey() throws Exception {
        // A bare narrow-int column used as an IN ELEMENT under a LONG key was never
        // sign-extended: isWidthSensitiveInKey covers a narrow KEY only, and the IN branch of
        // markWidthSemantics returned before the narrow-leaf widening rule (which is gated on a
        // comparison token, false for "in"). A widening sibling conjunct flips the filter to
        // four-lane AVX2, where the element loads as four packed i32 in the low XMM half while
        // the key spans four 64-bit lanes, so each lane compares the key against a pair of
        // ADJACENT rows' elements.
        assertMemoryLeak(() -> {
            // i32 alternates 0, 1 so lanes 0 and 1 pack to (1 << 32) | 0 = 4_294_967_296, which
            // equals the key. No row satisfies the predicate at 64-bit width, but the pre-fix
            // four-lane path returned half the SIMD-body rows.
            execute("create table in_elem as (select timestamp_sequence(0, 1_000_000) k," +
                    " ((x + 1) % 2)::int i32, 4_294_967_296L i64, 7::int g32, 7L g64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from in_elem where i64 in (i32) and g32 = g64",
                    true, "count\n0\n");
            assertJitMatchesJavaOnEmptyResult("in_elem where i64 in (i32) and g32 = g64", true, "k\ti32\ti64\tg32\tg64\n");
            assertJitMatchesJava("select count() from in_elem where i64 in (i32, 5) and g32 = g64",
                    true, "count\n0\n");
            // An empty result reads the same off either loop, so pin that the widening sibling does
            // put these on four lanes - that is where the un-widened element scrambled the lanes.
            assertExecHint("in_elem", "i64 in (i32) and g32 = g64", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem", "i64 in (i32, 5) and g32 = g64", EXEC_HINT_WIDE_LANE);

            // Batch-length and operand-order parity across the sign-extension boundary.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists in_elem_b");
                if (rowCount == 0) {
                    execute("create table in_elem_b (i32 int, i64 long, f32 float)");
                } else {
                    execute("create table in_elem_b as (select "
                            + "cast(case x when 1 then null when 2 then -1 else x - 3 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 4_294_967_295 else 2 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32)", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32) and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i32 in (i64)", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i32 in (i64) and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("in_elem_b where i64 in (i32, 5_000_000_000)", true);
                assertJitMatchesJava("select count() from in_elem_b where i64 in (i32) and f32 < 1.00000003", true);
            }
            assertExecHint("in_elem_b", "i64 in (i32)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem_b", "i64 in (i32) and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem_b", "i32 in (i64)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem_b", "i32 in (i64) and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem_b", "i64 in (i32, 5_000_000_000)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_elem_b", "i64 in (i32) and f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneInNullElementUnderWidenedKey() throws Exception {
        // An untyped NULL IN element is wide-lane eligible, but its immediate took the local
        // type observer's width. The observer sees columns only, so a predicate that
        // sign-extends its narrow leaves to i64 still typed the NULL down to I4 and emitted
        // INT_NULL; against the key's i64 lanes that broadcasts as 0x8000000080000000 per qword
        // and no genuinely-NULL key ever matched. IN (null) and = null disagreed as a result.
        assertMemoryLeak(() -> {
            execute("create table in_null as (select timestamp_sequence(0, 1_000_000) k," +
                    " null::int i32" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            // The key is INT + an out-of-INT-range constant, so it widens to i64; every row is
            // NULL, so every row must match. Pre-fix the four-lane path returned none.
            assertJitMatchesJava("select count() from in_null where (i32 + 5_000_000_000) in (null)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            // The = null control has always been correct; the two spellings must agree on rows.
            // They do NOT agree on the loop that produces them, and the pins below say so. An
            // untyped NULL is a wide-lane IN ELEMENT - isWideLaneIntegerInElement() is
            // "isWideLaneIntegerExpression(node) || isNullConstant(node)" - but it is not a
            // wide-lane comparison operand, because isWideLaneEligible()'s integer arm needs
            // isWideLaneIntegerExpression() on BOTH sides and a NULL constant is not an integer
            // constant. So "= null" never enters wide-lane mode, the key's SX_I64 stays in
            // i64WidenLeaves, and visit() turns that into forceScalarMode.
            assertJitMatchesJava("select count() from in_null where (i32 + 5_000_000_000) = null",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            assertExecHint("in_null", "(i32 + 5_000_000_000) in (null)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_null", "(i32 + 5_000_000_000) = null", EXEC_HINT_SCALAR);

            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists in_null_b");
                if (rowCount == 0) {
                    execute("create table in_null_b (i32 int, i64 long)");
                } else {
                    execute("create table in_null_b as (select "
                            + "cast(case x when 1 then null when 2 then -7 else x end as int) i32, "
                            + "cast(case x when 1 then null else 2 end as long) i64 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 + 5_000_000_000) in (null)", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 + 5_000_000_000) = null", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 * i64) in (null)", true);
                // A narrow key with no LONG in the predicate must keep INT_NULL at I4.
                assertJitMatchesJavaOnBatchLengths("in_null_b where (i32 * 2) in (null)", true);
                assertJitMatchesJavaOnBatchLengths("in_null_b where i32 in (null)", true);
                // A genuine LONG key already typed the NULL at I8; it must stay vectorized.
                assertJitMatchesJavaOnBatchLengths("in_null_b where i64 in (null)", true);
            }
            // Three different loops over six shapes, and row parity distinguishes none of them.
            // An I8-typed key admits the NULL element to wide-lane mode; a key that stays at I4
            // emits no SX_I64 and observes one size, so it keeps the single-size loop; and the
            // "= null" spelling is not wide-lane eligible at all (see above), so it forces scalar.
            assertExecHint("in_null_b", "(i32 + 5_000_000_000) in (null)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_null_b", "(i32 + 5_000_000_000) = null", EXEC_HINT_SCALAR);
            assertExecHint("in_null_b", "(i32 * i64) in (null)", EXEC_HINT_WIDE_LANE);
            assertExecHint("in_null_b", "(i32 * 2) in (null)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("in_null_b", "i32 in (null)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("in_null_b", "i64 in (null)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneIntColumnVsLongColumn() throws Exception {
        // A bare INT-column vs LONG-column comparison (no arithmetic, no out-of-range
        // constant) is wide-lane eligible, but nothing sign-extended its INT leaf. A
        // widening sibling conjunct (i32 < 5_000_000_000, or an inexact-float compare)
        // flips the whole filter to four-lane AVX2, dragging the un-widened comparison
        // into a vector compare whose operands differ in width - the INT column loads as
        // 4x32 in the low XMM and the LONG column as 4x64 in the YMM - so the lanes are
        // scrambled and the JIT returns the wrong rows. Standalone the comparison stayed
        // on the correct mixed-size scalar path, so only the sibling-flipped shape
        // regressed. Now serializeColumn sign-extends the narrow leaf (markWidthSemantics),
        // so the four-lane compare runs at i64 width and matches the Java filter.
        //
        // The regression manifests ONLY in the four-lane AVX2 compare (compiler.cpp runs the
        // wide-lane loop only when exec_hint == wide_lane AND has_avx2()); on a non-AVX2 x86 host
        // or on aarch64 the same predicate runs the always-correct scalar loop, so this test would
        // pass green there without ever touching the regressed path. Gate it on AVX2 so it either
        // exercises the four-lane compare or is skipped - never a false pass. The width-semantics
        // IR itself (both narrow leaves carry sx_i64, exec hint WIDE_LANE) is pinned host-
        // independently on every CI leg by CompiledFilterIRSerializerTest#
        // testDirectIntLongColumnComparisonWidensAndUsesWideLane.
        Assume.assumeTrue("wide-lane regression lives in the four-lane AVX2 path", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // Deterministic all-match pin: every row satisfies both conjuncts, so the Java
            // filter keeps all rows; the pre-fix JIT ran i32 < i64 in four-lane mode without
            // widening i32 and dropped SIMD-body rows (count < N).
            execute("create table allmatch as (select timestamp_sequence(0, 1_000_000) k," +
                    " 1::int i32, 3_000_000_000L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from allmatch where i32 < i64 and i32 < 5_000_000_000",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            // An all-match count reads the same off the scalar loop, so pin the loop too.
            assertExecHint("allmatch", "i32 < i64 and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);

            // Batch-length + sign-extension-boundary parity, exercising both widening-sibling
            // shapes (out-of-INT-range integer constant and inexact float) and the reversed
            // operand order.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists wide_cc");
                if (rowCount == 0) {
                    execute("create table wide_cc (i8 byte, i16 short, i32 int, i64 long, f32 float)");
                } else {
                    execute("create table wide_cc as (select "
                            + "cast(case x when 1 then null when 2 then 127 when 3 then -127 else x - 5 end as byte) i8, "
                            + "cast(case x when 1 then null when 2 then 32_000 when 3 then -32_000 else x - 5 end as short) i16, "
                            + "cast(case x when 1 then null when 2 then 2_147_483_647 "
                            + "when 3 then -2_147_483_647 when 4 then 0 else x - 5 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 5_000_000_000 "
                            + "when 3 then -5_000_000_000 else x * 1_000_000_000 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                // Out-of-INT-range integer constant flips the filter to four-lane.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 = i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i64 > i32 and i32 < 5_000_000_000", true);
                // Inexact-float sibling flips the filter to four-lane.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64 and f32 < 1.00000003", true);
                // Control: the standalone comparison, with no widening sibling to flip the filter.
                // It was always correct, but it no longer stays off the four-lane loop: at HEAD the
                // pairing is wide-lane eligible in its own right and serializeColumn sign-extends
                // the narrow leaf, so it runs the same compare the sibling-flipped shapes do. The
                // pin after the sweep is what says so, and
                // CompiledFilterIRSerializerTest#testDirectIntLongColumnComparisonWidensAndUsesWideLane
                // pins the same answer host-independently.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i32 < i64", true);
                assertJitMatchesJava("select count() from wide_cc where i32 < i64 and i32 < 5_000_000_000", true);
                // BYTE / SHORT columns are never wide-lane eligible, so a narrow-vs-LONG
                // comparison stays scalar whether or not a widening sibling is present; the
                // added sign-extension must not disturb that correct scalar path.
                assertJitMatchesJavaOnBatchLengths("wide_cc where i8 < i64", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i16 < i64", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i8 < i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("wide_cc where i16 = i64 and i32 < 5_000_000_000", true);
            }
            // The whole point of the test is WHICH loop runs, and the sweep above cannot see it.
            // INT-vs-LONG takes four lanes with or without a widening sibling; BYTE and SHORT are
            // not wide-lane eligible, so maybeEmitI64Widening() emits their SX_I64 outside wide-lane
            // mode and visit() forces the scalar backend - which is the path that was already right.
            assertExecHint("wide_cc", "i32 < i64 and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_cc", "i32 = i64 and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_cc", "i64 > i32 and i32 < 5_000_000_000", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_cc", "i32 < i64 and f32 < 1.00000003", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_cc", "i32 < i64", EXEC_HINT_WIDE_LANE);
            assertExecHint("wide_cc", "i8 < i64", EXEC_HINT_SCALAR);
            assertExecHint("wide_cc", "i16 < i64", EXEC_HINT_SCALAR);
            assertExecHint("wide_cc", "i8 < i64 and i32 < 5_000_000_000", EXEC_HINT_SCALAR);
            assertExecHint("wide_cc", "i16 = i64 and i32 < 5_000_000_000", EXEC_HINT_SCALAR);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testWideLaneNarrowConstAndIntSentinelBatchLengths() throws Exception {
        // The four-lane loop steps four rows at a time and hands the remainder to a scalar tail
        // that x86::emit_code writes over the SAME IR the SIMD body ran - a different backend, a
        // different convert() table, a different set of arithmetic helpers. A shape whose two
        // halves disagree therefore shows up only where the row count is NOT a multiple of four,
        // and only in the rows that land in the tail.
        //
        // Two of this branch's fixtures cannot see that. wlnc holds exactly 16 rows in one
        // partition and icd exactly 4, so both run whole four-lane iterations and neither ever
        // enters the tail, and neither has a volume twin the way a3w has a3x, dcwl and dcsc have
        // dcsb, and icfw has icfb. This is the sweep that closes it: 0 to 9 rows crosses two full
        // steps and every tail length, over both shapes.
        //
        // The other execution-mode variants those fixtures lack are deliberately NOT added here:
        // - a WAL twin changes nothing the serializer reads. It takes column metadata and a
        //   page-frame cursor, and a WAL table presents both exactly as a non-WAL one once the
        //   apply job has run - already pinned by m3w, c2w, c2fw, a1w and
        //   testIntColumnVsFloatColumnMatchesJavaOnWalTable. The one thing that does change what a
        //   frame looks like to the filter is a COLUMN TOP, and dcsc already carries one.
        // - an out-of-order variant changes which rows land in which partition, not what a frame
        //   looks like: the reader presents merged, in-order data once the commit is through.
        //   a3x and dcsb already run these filters over 515 rows of random data. (icfb is not one
        //   of those - it is a 0..13 batch sweep over hand-built rows, so it adds length coverage
        //   rather than volume.)
        // - an unpartitioned variant moves frame boundaries, and the fixtures already sit on both
        //   sides of that line - icfb, wide_cc and in_null_b are unpartitioned, while a3w and dcsc
        //   are partitioned so their filters cross a frame boundary.
        // - nbo needs no sweep at all: its shapes compile SCALAR, and the scalar loop has no
        //   vector body to leave a tail behind.
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists tail_b");
                if (rowCount == 0) {
                    execute("create table tail_b (i32 int, d64 double)");
                } else {
                    // i32 straddles both bounds the wlnc shapes use - 331_725 and its double,
                    // 663_450 - and carries NULL and both INT extremes. d64 carries NULL and
                    // values on both sides of zero.
                    execute("create table tail_b as (select "
                            + "cast(case x when 1 then null when 2 then 331_725 when 3 then 331_724 "
                            + "when 4 then 663_450 when 5 then 663_449 when 6 then -561_252 "
                            + "when 7 then 2_147_483_647 when 8 then -2_147_483_647 else x - 3 end as int) i32, "
                            + "cast(case x when 2 then null else x - 3 end as double) d64 "
                            + "from long_sequence(" + rowCount + "))");
                }

                // The wlnc shapes: a narrow constant operand of a LONG-width arithmetic node,
                // which markWidthSemanticsOperand widens to an I8 immediate beside a
                // sign-extended INT column.
                assertJitMatchesJavaOnBatchLengths("tail_b where i32 >= (446_488 - 114_763L)", true);
                assertJitMatchesJavaOnBatchLengths("tail_b where i32 >= (446_488 - 114_763L) * 2", true);
                assertJitMatchesJavaOnBatchLengths("tail_b where i32 in (446_488 - 114_763L, 5)", true);
                assertJitMatchesJavaOnBatchLengths("tail_b where i32 < (-446_488 - 114_763L)", true);
                // The declined fold: int64_div answers LONG_NULL for a zero divisor, and only the
                // NULL row matches. Body and tail have to reproduce that sentinel alike.
                assertJitMatchesJavaOnBatchLengths("tail_b where i32 >= (446_488 / 0L)", true);
                // The icd shape: an INT arithmetic chain that folds onto INT_NULL, against a
                // DOUBLE column. Spelled through NOT so the predicate returns rows - the bound is
                // NULL, so the un-negated form is empty by construction and a tail that computed a
                // finite bound instead would show up as ADDED rows, which parity sees either way,
                // but only the negated form also pins the row count the sweep guard counts.
                assertJitMatchesJavaOnBatchLengths("tail_b where not (d64 > (0 - 2_147_483_647 - 1))", true);
                // The control one operation short of the sentinel: an ordinary INT bound every row
                // clears.
                assertJitMatchesJavaOnBatchLengths("tail_b where d64 > (0 - 2_147_483_647)", true);
            }
            // Row parity cannot see which loop ran - the scalar loop answers all of these
            // correctly too - so pin the modes the tail argument depends on. getExecHint() reads
            // the metadata and the parsed filter rather than the row count, so one check after the
            // sweep covers every iteration of it.
            assertExecHint("tail_b", "i32 >= (446_488 - 114_763L)", EXEC_HINT_WIDE_LANE);
            assertExecHint("tail_b", "i32 in (446_488 - 114_763L, 5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("tail_b", "not (d64 > (0 - 2_147_483_647 - 1))", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testUnaryMinusNarrowLeafAtLongWidthForcesScalar() throws Exception {
        // Unary minus over a narrow-int leaf compared at long width was never sign-extended:
        // the widening rule fires only for a bare leaf (-i32 is an OPERATION), and the global
        // fallback missed too because isArithmeticOperation() requires two operands, so
        // NarrowI64WidenDetector.hasArithmetic stayed false. A widening sibling conjunct flips
        // the filter to four-lane AVX2, where NEG then ran with 32-bit lane semantics over
        // 64-bit lanes and corrupted the high half of each lane.
        //
        // HEAD does not answer that by widening the operand. forceScalarOnUnharmonisedNarrowArith()
        // treats a unary minus as an arithmetic node (its isUnaryMinus arm), sees an I4 type with a
        // column under it, and sets forceScalarMode - which getExecHint() answers before it consults
        // wide-lane mode at all. So every shape below runs the SCALAR backend, whose convert()
        // carries the complete int table, INT_NULL to LONG_NULL included; the four-lane loop never
        // sees them. That method's own javadoc records the measurement behind the choice: the
        // four-lane loop returned zero rows for "-i32 < 2147483649" where Java returns every row.
        // The pins after the sweep are what holds the shapes there, and they are why this method is
        // no longer named for the wide lane - it was, and it asserted no mode whatever.
        assertMemoryLeak(() -> {
            // -i32 is -1, whose i32 pattern packs with its neighbour into 0xFFFFFFFFFFFFFFFF in
            // some lanes and 0x00000000FFFFFFFF (= 4_294_967_295, the i64 value) in others. No
            // row satisfies the predicate at 64-bit width; pre-fix the JIT returned half the
            // SIMD-body rows.
            execute("create table neg_leaf as (select timestamp_sequence(0, 1_000_000) k," +
                    " 1::int i32, 4_294_967_295L i64, 7::int g32, 7L g64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from neg_leaf where -i32 = i64 and g32 = g64",
                    true, "count\n0\n");
            assertJitMatchesJavaOnEmptyResult("neg_leaf where -i32 = i64 and g32 = g64", true,
                    "k\ti32\ti64\tg32\tg64\n");
            // Also route the shape through assertJitCountQuery: that helper compares the
            // scalar-mode and vectorized-mode counts, and a lane scramble is exactly what it
            // exists to catch, so it must observe the vectorized count rather than re-check
            // the scalar one.
            assertJitCountQuery("select count() from neg_leaf where -i32 = i64 and g32 = g64", 0);
            assertExecHint("neg_leaf", "-i32 = i64 and g32 = g64", EXEC_HINT_SCALAR);

            // Batch lengths, NULL handling (INT_NULL is Integer.MIN_VALUE, so -i32 stays NULL)
            // and every comparison operator, with both widening-sibling shapes.
            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists neg_b");
                if (rowCount == 0) {
                    execute("create table neg_b (i8 byte, i16 short, i32 int, i64 long, f32 float)");
                } else {
                    execute("create table neg_b as (select "
                            + "cast(case x when 1 then null when 2 then 127 else x - 5 end as byte) i8, "
                            + "cast(case x when 1 then null when 2 then 32767 else x - 5 end as short) i16, "
                            + "cast(case x when 1 then null when 2 then 2147483647 when 3 then -2147483647 else x - 5 end as int) i32, "
                            + "cast(case x when 1 then null when 2 then 4_294_967_295 else x - 5 end as long) i64, "
                            + "cast(case x when 1 then null else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 <> i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 < i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 <= i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 > i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 >= i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where i64 = -i32", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64 and i32 < 5_000_000_000", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i32 = i64 or i32 < 5_000_000_000", true);
                assertJitMatchesJava("select count() from neg_b where -i32 = i64 and f32 < 1.00000003", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where i64 in (-i32)", true);
                // BYTE / SHORT are never wide-lane eligible; the added widening must leave
                // their correct scalar path alone.
                assertJitMatchesJavaOnBatchLengths("neg_b where -i8 = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_b where -i16 = i64", true);
            }
            // Parity holds on every loop, so it cannot see that these run on the scalar one. The
            // widening siblings ("i32 < 5_000_000_000", "f32 < 1.00000003") make the REST of the
            // filter wide-lane eligible, and the force still wins: forceScalarMode is the first
            // term getExecHint() reads.
            assertExecHint("neg_b", "-i32 = i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 <> i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 < i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 <= i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 > i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 >= i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "i64 = -i32", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 = i64 and i32 < 5_000_000_000", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 = i64 or i32 < 5_000_000_000", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i32 = i64 and f32 < 1.00000003", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "i64 in (-i32)", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i8 = i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_b", "-i16 = i64", EXEC_HINT_SCALAR);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testUnaryMinusOperandWrapsUnderIntComparison() throws Exception {
        // The mirror of testUnaryMinusNarrowLeafAtLongWidthForcesScalar: under an INT-width
        // comparison the unary-minus operand has to WRAP, not widen. markWidthSemantics recursed into the
        // operand directly instead of going through markWidthSemanticsOperand, which is the
        // only path that reaches i64WrapLeaves, so the predicate-global widening flag
        // sign-extended it and the product then ran at 64 bits where the Java filter wraps
        // mod 2^32 (NegInt/MulInt#getInt).
        //
        // Nothing here runs the four-lane loop, and the pins below say which loop each shape does
        // run. The boolean-of-boolean spellings never reach a 64-bit pairing over the narrow
        // subtree - the inner comparison is INT-width - so nothing widens and nothing forces
        // scalar, and the predicate simply observes a four-byte and an eight-byte column:
        // EXEC_HINT_MIXED_SIZE_TYPE. The two genuinely long-width spellings meet
        // forceScalarOnUnharmonisedNarrowArith() on the I4 product and go scalar. The name says
        // what the streams below take - an INT-width comparison over a unary-minus operand - and
        // not the wide-lane family the defect came from, which no pin here reaches.
        assertMemoryLeak(() -> {
            // -(-65536) * 65536 = 4_294_967_296, which wraps to 0 in INT arithmetic, so the
            // inner comparison is true for every row and the whole predicate is true = true.
            // Pre-fix the JIT computed the product at 64 bits and returned no rows.
            execute("create table neg_wrap as (select timestamp_sequence(0, 1_000_000) k," +
                    " (-65536)::int a32, 65536::int b32, 1L i64" +
                    " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)");
            assertJitMatchesJava("select count() from neg_wrap where ((-a32 * b32) = 0) = (i64 > 0)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            // The binary-only spelling was always correct; both must agree.
            assertJitMatchesJava("select count() from neg_wrap where ((a32 * b32) = 0) = (i64 > 0)",
                    true, "count\n" + N_SIMD_WITH_SCALAR_TAIL + "\n");
            assertExecHint("neg_wrap", "((-a32 * b32) = 0) = (i64 > 0)", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("neg_wrap", "((a32 * b32) = 0) = (i64 > 0)", EXEC_HINT_MIXED_SIZE_TYPE);

            for (int rowCount = 0; rowCount <= 9; rowCount++) {
                execute("drop table if exists neg_wrap_b");
                if (rowCount == 0) {
                    execute("create table neg_wrap_b (a32 int, b32 int, i64 long)");
                } else {
                    execute("create table neg_wrap_b as (select "
                            + "cast(case x when 1 then null when 2 then -65536 else x end as int) a32, "
                            + "cast(case x when 1 then null when 2 then 65536 else x + 1 end as int) b32, "
                            + "cast(case x when 1 then null else 1 end as long) i64 "
                            + "from long_sequence(" + rowCount + "))");
                }
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 * b32) = 0) = (i64 > 0)", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 + b32) = 0) = (i64 > 0)", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where ((-a32 * b32) > 0) = (i64 > 0)", true);
                // A genuine long-width comparison over the same shape must still widen.
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where (-a32 * b32) = i64", true);
                assertJitMatchesJavaOnBatchLengths("neg_wrap_b where -a32 * b32 = 4_294_967_296", true);
            }
            assertExecHint("neg_wrap_b", "((-a32 * b32) = 0) = (i64 > 0)", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("neg_wrap_b", "((-a32 + b32) = 0) = (i64 > 0)", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("neg_wrap_b", "((-a32 * b32) > 0) = (i64 > 0)", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("neg_wrap_b", "(-a32 * b32) = i64", EXEC_HINT_SCALAR);
            assertExecHint("neg_wrap_b", "-a32 * b32 = 4_294_967_296", EXEC_HINT_SCALAR);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testIntColumnsCount() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i8", "i16", "i32", "i64")
                .withAnyOf(" != 0", " < 42");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testIntConstantColumnComparisonBoundaryMatch() throws Exception {
        final int boundary = 101;
        Assert.assertTrue("boundary should be within the range", N_SIMD_WITH_SCALAR_TAIL > boundary);
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " x i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf(String.valueOf(boundary))
                .withComparisonOperator()
                .withAnyOf("i8", "i16", "i32", "i64", "f32", "f64");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testFloatColumnVsConstantWithNoExactFloat() throws Exception {
        // A FLOAT column always compares at DOUBLE width in the Java filter: there is no
        // (FLOAT, FLOAT) comparison factory, only the double ones ("<(DD)"), so both operands
        // promote. The JIT typed the constant down to F4 (the observer sees one 4-byte column, so
        // no mixed size) and serializeNumber rounded it to the NEAREST float, comparing at float
        // width - which selects different rows in either direction:
        //   RED on HEAD: (a) "< 1.00000003" rounds the bound DOWN to 1.0f and drops the row
        //   holding 1.0f that Java keeps; (b) "> 0.99999998" rounds it UP to 1.0f and drops the
        //   same row; (c) "= 1.00000003" rounds it to 1.0f and MATCHES that row - returning a row
        //   whose value is provably not the one asked for. markNarrowConstCmpWidenPair now sends any
        //   constant with no exact float to the filter as a full double, so it compares at double
        //   width exactly as the Java filter does (tolerance and all). Those predicates run scalar;
        //   a constant WITH an exact float is untouched and keeps the vectorized path.
        //
        // The table has >= 64 rows so the vectorized loop is exercised on the shapes that keep it.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 1.0 when x = 2 then 16777216.0 else 5.0 end as float) f," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // (a) the bound rounds DOWN to 1.0f: row 1 satisfies 1.0 < 1.00000003 at double width.
            assertJitMatchesJava("select rn from y where f < 1.00000003", true, "rn\n1\n");
            // (b) the bound rounds UP to 1.0f: row 1 satisfies 1.0 > 0.99999998 at double width.
            //     Rows 3+ hold 5.0 and row 2 holds 2^24, so every row matches.
            Assert.assertEquals(64, runQuery("select rn from y where f > 0.99999998"));
            assertJitMatchesJava("select rn from y where f > 0.99999998 and rn <= 2", true, "rn\n1\n2\n");
            // (c) the false positive: no float equals 1.00000003, so nothing may match.
            assertJitMatchesJavaOnEmptyResult("select rn from y where f = 1.00000003", true, "rn\n");
            assertJitMatchesJava("select rn from y where f <> 1.00000003 and rn <= 2", true, "rn\n1\n2\n");

            // An integer literal is no safer above 2^24: (float) 16_777_217 is 16_777_216, so the
            // bound lands exactly on row 2's value.
            assertJitMatchesJava("select rn from y where f < 16_777_217 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f >= 16_777_217", true, "rn\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f = 16_777_217", true, "rn\n");

            // A negated constant takes the same route (the literal sits under a unary minus).
            assertJitMatchesJava("select rn from y where f > -1.00000003 and rn <= 2", true, "rn\n1\n2\n");

            // IN over a FLOAT key is an OR of equalities, so it takes the equality route: no float
            // reproduces the bound, and the nearest one matched the row that rounds to it.
            assertJitMatchesJavaOnEmptyResult("select rn from y where f in (1.00000003, 2.5)", true, "rn\n");
            assertJitMatchesJava("select rn from y where f not in (1.00000003, 2.5) and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f in (1.00000003)", true, "rn\n");
            // ... and an exactly-representable element still matches, and still vectorizes.
            assertJitMatchesJava("select rn from y where f in (5.0, 2.5) and rn <= 4", true, "rn\n3\n4\n");

            // An ARITHMETIC float leaf reads the bound the same way a bare column does. These ops
            // are value-preserving, so the key is still row 1's 1.0f either way.
            assertJitMatchesJava("select rn from y where f + 0 < 1.00000003", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f * 1 < 1.00000003", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where -f > -1.00000003", true, "rn\n1\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f + 0 = 1.00000003", true, "rn\n");

            // Controls: a bound WITH an exact float compares the same at either width, so it must
            // keep the vectorized path and the same rows.
            assertJitMatchesJava("select rn from y where f < 1.5 and rn <= 2", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f = 5.0 and rn <= 4", true, "rn\n3\n4\n");
            assertJitMatchesJava("select rn from y where f > 4 and rn <= 4", true, "rn\n2\n3\n4\n");
            // An explicit widening to DOUBLE selects the same rows (the JIT declines a cast and
            // falls back to Java, so this pins the double-width answer the fix now agrees with).
            assertJitMatchesJava("select rn from y where f::double < 1.00000003", false, "rn\n1\n");

            // The comparison carries a TOLERANCE: QuestDB reads "f < d" as
            // "!Numbers.equals(f, d) && f < d" with DOUBLE_TOLERANCE = 1e-10, so a row within 1e-10
            // of the bound is EQUAL to it and "<" excludes it while ">=" keeps it. Only the double
            // comparison reproduces that. Emitting a float bound instead - even one rounded in the
            // direction the operator preserves - cannot: one float ulp near 1.0 is 1.2e-7, over a
            // thousand times the tolerance, so the bound steps clean over the band and flips these
            // rows in both directions. Every bound here sits inside the tolerance band around 1.0.
            assertJitMatchesJavaOnEmptyResult("select rn from y where f < 1.00000000005", true, "rn\n");
            assertJitMatchesJava("select rn from y where f >= 1.00000000005 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJava("select rn from y where f <= 0.99999999995", true, "rn\n1\n");
            assertJitMatchesJava("select rn from y where f > 0.99999999995 and rn <= 2", true, "rn\n2\n");
            assertJitMatchesJava("select rn from y where f = 1.00000000005", true, "rn\n1\n");
        });
    }

    @Test
    public void testNarrowIntColumnVsFloatingPointConstant() throws Exception {
        // An INT column compared against a floating-point constant promotes to DOUBLE in the Java
        // filter - IntFunction#getDouble feeds "<(DD)" - so both operands compare at f64. The JIT
        // typed the constant down to F4 (the observer sees one 4-byte column, so no mixed size) and
        // serializeNumber rounded it to the nearest float; cvt_itof then rounded the COLUMN to float
        // as well, so the comparison ran entirely at f32. Both roundings drop or invent rows:
        //   RED on HEAD: (a) "< 1.00000003" rounds the bound DOWN to 1.0f and loses the row holding
        //   1; (b) "= 1.00000003" matches that row instead, returning a value provably not the one
        //   asked for; (c) above 2^24 one float ulp exceeds 1, so an ordinary bound like
        //   "< 20000000.5" silently loses every row equal to 20000000; (d) a bound WITH an exact
        //   float still diverges once the COLUMN is the side that rounds - (float) 16777217 is
        //   16777216, so "> 16777216.0" drops it.
        // markNarrowConstCmpWidenPair now routes the constant through the 64-bit arm and
        // sign-extends the int leaf, so the pair compares at double width exactly as Java does.
        assertMemoryLeak(() -> {
            execute("create table z as (select" +
                    " cast(case when x = 1 then 1 when x = 2 then 20_000_000 when x = 3 then 16_777_217 else 5 end as int) i," +
                    " cast(case when x = 1 then 1 else 5 end as byte) b," +
                    " cast(case when x = 1 then 1 else 5 end as short) s," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // (a) the bound rounds DOWN to 1.0f, so the row holding 1 stops satisfying it.
            assertJitScalarAndVectorMatchJava("select rn from z where i < 1.00000003 and rn <= 3", "rn\n1\n");
            // (b) the false positive: no INT equals 1.00000003, so nothing may match.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from z where i = 1.00000003", "rn\n");
            // (c) not a corner case - above 2^24 the float ulp is 2, so a plain fractional bound
            //     rounds onto an ordinary column value and drops it.
            assertJitScalarAndVectorMatchJava("select rn from z where i < 20000000.5 and rn <= 3", "rn\n1\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i <> 20000000.5 and rn <= 3", "rn\n1\n2\n3\n");
            // (d) the constant has an exact float here; the COLUMN is what rounds.
            assertJitScalarAndVectorMatchJava("select rn from z where i > 16777216.0 and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i <= 16777216.0 and rn <= 3", "rn\n1\n");
            // ... and the f-suffixed spelling of it reads the same. parseDouble rejects that token,
            // so both the widening analysis (floatCmpConstValue) and the 64-bit arm of
            // serializeNumber need a parseFloat fallback to handle it: without the first the shape
            // silently kept the rounded F4 bound and dropped row 3, and without the second the JIT
            // declined a filter it can compile correctly. It stays compiled.
            assertJitScalarAndVectorMatchJava("select rn from z where i > 16777216.0f and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i <= 16777216.0f and rn <= 3", "rn\n1\n");

            // A negated constant takes the same route (the literal sits under a unary minus).
            assertJitScalarAndVectorMatchJava("select rn from z where i > -1.00000003 and rn <= 3", "rn\n1\n2\n3\n");

            // An INT ARITHMETIC subtree reads the bound exactly as a bare column does, but it is not
            // a leaf, so markNarrowConstCmpWidenPair does not see it - maybeWidenCmpConstOperand
            // widens the bound for it instead. Only the CONSTANT widens: the subtree keeps computing
            // at i32 and wrapping, and the backend's convert() promotes its result at the
            // comparison. These ops are value-preserving, so the answer is the column's.
            assertJitScalarAndVectorMatchJava("select rn from z where i + 0 > 16777216.0 and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i * 1 > 16777216.0 and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i - 0 > 16777216.0 and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i + 0 < 1.00000003 and rn <= 3", "rn\n1\n");
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from z where i + 0 = 1.00000003", "rn\n");
            // ... including under a unary minus on BOTH sides, where the bound is an OPERATION too.
            assertJitScalarAndVectorMatchJava("select rn from z where -i < -16777216.0 and rn <= 3", "rn\n2\n3\n");

            // BYTE and SHORT leaves take the same arm. These used to DECLINE the filter outright -
            // serializeNumber's I1/I2 arms parse an integer and throw on a fractional token - so they
            // were never wrong, just uncompiled; now they compile and must agree.
            assertJitScalarAndVectorMatchJava("select rn from z where b < 1.00000003 and rn <= 3", "rn\n1\n");
            assertJitScalarAndVectorMatchJava("select rn from z where s < 1.00000003 and rn <= 3", "rn\n1\n");
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from z where b = 1.00000003", "rn\n");
            assertJitScalarAndVectorMatchJava("select rn from z where s > 0.99999998 and rn <= 3", "rn\n1\n2\n3\n");

            // An INT BIND VARIABLE is a leaf like the column, and reads the bound the same way.
            bindVariableService.setInt("bv", 16_777_217);
            assertJitScalarAndVectorMatchJava("select rn from z where :bv > 16777216.0 and rn <= 2", "rn\n1\n2\n");
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from z where :bv < 1.00000003 and rn <= 2", "rn\n");

            // Controls: a bound WITH an exact float below 2^24 cannot diverge - neither side needs
            // rounding - and must keep selecting the same rows. 16777215.0 is the largest exact
            // float under the limit and pins that the threshold is not off by one ulp: no INT
            // rounds onto it, so it must stay on the untouched, vectorized path.
            assertJitScalarAndVectorMatchJava("select rn from z where i > 16777215.0 and rn <= 3", "rn\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i < 1.5 and rn <= 3", "rn\n1\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i = 5.0 and rn <= 5", "rn\n4\n5\n");
            // An explicit widening to DOUBLE pins the double-width answer the fix now agrees with.
            assertJitMatchesJava("select rn from z where i::double < 1.00000003 and rn <= 3", false, "rn\n1\n");

            // The comparison carries a TOLERANCE: QuestDB reads "i < d" as
            // "!Numbers.equals(i, d) && i < d" with DOUBLE_TOLERANCE = 1e-10, so a row within 1e-10
            // of the bound is EQUAL to it. One float ulp near 1.0 is 1.2e-7, a thousand times the
            // tolerance, so a float bound steps clean over the band; only the double one reproduces
            // it.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from z where i < 1.00000000005 and rn <= 3", "rn\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i >= 1.00000000005 and rn <= 3", "rn\n1\n2\n3\n");
            assertJitScalarAndVectorMatchJava("select rn from z where i = 1.00000000005 and rn <= 3", "rn\n1\n");
        });
    }

    @Test
    public void testNarrowIntColumnVsExactToleranceBoundConstant() throws Exception {
        // The Java filter and the compiled one carry the same tolerance AND the same INCLUSIVE
        // reading of it: Numbers.equals is "Math.abs(l - r) <= DOUBLE_TOLERANCE"
        // (Numbers.java:843/:847) and the native double_cmp_epsilon / float_cmp_epsilon are
        // "epsilon >= |lhs - rhs|" on every backend - x86 ucomisd/setae (jit/impl/x86.h), aarch64
        // fcmp/cset GE (jit/impl/aarch64.h) and the AVX2 pair's vcmppd/vcmpps with CmpImm::kLE
        // (jit/impl/avx2.h). This test pins the one point where that inclusiveness is the only thing
        // separating the two answers: a bound spelled exactly DOUBLE_TOLERANCE away from a stored
        // value. Everywhere else the same arithmetic closes the tolerance band on both sides and the
        // two agree regardless.
        //
        // The native comparators were STRICT until this change, and every assertion below reddens if
        // any of the six sites reverts. Reverting takes both the C++ sources and the committed
        // per-platform binaries under core/src/main/resources/io/questdb/bin/, since CI runs those
        // and not a local build; the C++ sources alone do not move CI.
        //
        // Only four of the six operators ever depended on it. "<=" and ">" combine the plain
        // comparison with the epsilon test in the direction that already covers the boundary row, so
        // they agreed under the strict comparator too; they are asserted here to catch a change that
        // breaks THEM while fixing the other four.
        //
        // The fixture holds 1_000 rows rather than a handful ON PURPOSE. The compiled filter runs
        // its AVX2 loop only once there are whole vectors to fill and handles the remainder with the
        // scalar tail, so a two-row table exercises jit/impl/x86.h in BOTH JIT modes and never
        // reaches jit/impl/avx2.h at all. Counts, not row lists, keep the expectations readable:
        // rows 1..500 hold 0 and sit exactly on the bound, rows 501..1_000 hold 5, so 500 and 1_000
        // name the two halves unambiguously.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE zt AS (
                      SELECT
                        (case when x <= 500 then 0 else 5 end)::byte b,
                        (case when x <= 500 then 0 else 5 end)::short s,
                        (case when x <= 500 then 0 else 5 end)::int i,
                        (case when x <= 500 then 0 else 5 end)::long l,
                        (case when x <= 500 then 0 else 5 end)::float f,
                        (case when x <= 500 then 0 else 5 end)::double d,
                        timestamp_sequence(0, 1_000_000) k
                      FROM long_sequence(1_000)
                    ) TIMESTAMP(k)""");

            // BYTE and SHORT are the pair this PR newly reaches: serializeNumber's I1 / I2 arms parse
            // an integer and throw on a fractional token, so these shapes used to decline JIT
            // compilation outright and both engines ran the Java filter. The narrow-int widening
            // compiles them now, and they land on a comparator that agrees.
            // Under the strict comparator the four moving operators answered 500 / 500 / 0 / 1_000.
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b <> 1e-10", "count\n500\n");

            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE s <> 1e-10", "count\n500\n");

            // INT, LONG, FLOAT and DOUBLE reached the comparator on base as well and disagreed there
            // identically, so these close a PRE-EXISTING divergence rather than a regression this PR
            // introduced. They are pinned here rather than in a separate test because they share the
            // single comparator this change moves: a fix that reached only the narrow ints would
            // leave every one of them open.
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i <> 1e-10", "count\n500\n");

            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE l <> 1e-10", "count\n500\n");

            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE f <> 1e-10", "count\n500\n");

            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d < 1e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d <= 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d > 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d = 1e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE d <> 1e-10", "count\n500\n");

            // The arithmetic spellings take a different marking path - maybeWidenCmpConstOperand
            // widens only the constant - and land on the same comparator, so they agree too. A fix
            // that declined JIT for the leaf shape alone would have left these open.
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b + 0 >= 1e-10", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE -s <= -1e-10", "count\n1000\n");

            // Controls: the boundary is the ONLY point the inclusive comparator moved. A bound half a
            // tolerance inside the band, and one half a tolerance outside it, answered the same under
            // the strict comparator and still do - so this change did not widen or narrow the band
            // itself, it only closed its edge.
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b < 5e-11", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b >= 5e-11", "count\n1000\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b = 5e-11", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE b <> 5e-11", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i < 1.5e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i >= 1.5e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i = 1.5e-10", "count\n0\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM zt WHERE i <> 1.5e-10", "count\n1000\n");
            // ... and the same bound against a BYTE / SHORT column is not compiled at all:
            // isNarrowIntCmpWideningConst widens only when an integer falls in the band round the
            // bound, and 1.5e-10 leaves none there, so serializeNumber's I1 / I2 arms reject the
            // fractional token. Both engines run the Java filter.
            assertJitMatchesJava("SELECT count() FROM zt WHERE b < 1.5e-10", false, "count\n500\n");
            assertJitMatchesJava("SELECT count() FROM zt WHERE s >= 1.5e-10", false, "count\n500\n");
        });
    }

    @Test
    public void testIntColumnVsFloatToleranceBoundConstantStillDivergesOnF32Width() throws Exception {
        // The SURVIVING half of the tolerance asymmetry, which the inclusive comparators do NOT
        // close: FLOAT_EPSILON is (float) DOUBLE_TOLERANCE (consts.h), i.e. 1.000000013351432e-10, a
        // shade LARGER than the 1e-10 the Java filter uses. An INT leaf compared against a fractional
        // bound falls through serializeNumber's I4 arm to a 32-bit float bound and runs the compiled
        // filter's f32 arm, so the two filters carry two different tolerances and disagree wherever a
        // value lands between them. That is pre-existing and independent of this change.
        //
        // What the inclusive comparators DID move is which single point falls in that gap. The strict
        // f32 test disagreed with Java at |row - bound| == DOUBLE_TOLERANCE, the bound a query
        // realistically spells ("col >= 1e-10"); the inclusive one agrees there - see
        // testNarrowIntColumnVsExactToleranceBoundConstant - and disagrees instead at
        // |row - bound| == FLOAT_EPSILON, reachable only by spelling out the float representation of
        // the tolerance in full. The count of disagreeing points is unchanged; the one that remains
        // is far harder to write by accident.
        //
        // This is also the test that covers the two f32 sites of the change (x86 float_cmp_epsilon,
        // AVX2 cmp_eq_float). The fixture holds 1_000 rows so the AVX2 loop runs: a small table is
        // handled entirely by the scalar tail and would leave jit/impl/avx2.h untested.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE ft AS (
                      SELECT
                        (case when x <= 500 then 0 else 5 end)::int i,
                        timestamp_sequence(0, 1_000_000) k
                      FROM long_sequence(1_000)
                    ) TIMESTAMP(k)""");

            // 1.000000013351432e-10 IS (float) 1e-10 exactly, so |0 - bound| at f32 width is exactly
            // FLOAT_EPSILON and the inclusive f32 test calls the 500 zero rows EQUAL to the bound. At
            // f64 width the same distance exceeds DOUBLE_TOLERANCE, so the Java filter calls them
            // UNEQUAL. Both answers are pinned.
            assertJitDivergesFromJavaAtF32ToleranceBound(
                    "SELECT count() FROM ft WHERE i < 1.000000013351432e-10", "count\n500\n", "count\n0\n");
            assertJitDivergesFromJavaAtF32ToleranceBound(
                    "SELECT count() FROM ft WHERE i >= 1.000000013351432e-10", "count\n500\n", "count\n1000\n");
            assertJitDivergesFromJavaAtF32ToleranceBound(
                    "SELECT count() FROM ft WHERE i = 1.000000013351432e-10", "count\n0\n", "count\n500\n");
            assertJitDivergesFromJavaAtF32ToleranceBound(
                    "SELECT count() FROM ft WHERE i <> 1.000000013351432e-10", "count\n1000\n", "count\n500\n");
            // "<=" and ">" cover the boundary rows through the plain comparison in the same direction
            // as the epsilon test, so they agree at this bound as they do at 1e-10.
            assertJitScalarAndVectorMatchJava("SELECT count() FROM ft WHERE i <= 1.000000013351432e-10", "count\n500\n");
            assertJitScalarAndVectorMatchJava("SELECT count() FROM ft WHERE i > 1.000000013351432e-10", "count\n500\n");
        });
    }

    @Test
    public void testFloatWithLongOperandVsConstantWithNoExactFloat() throws Exception {
        // A FLOAT leaf whose arithmetic has a LONG operand: QuestDB resolves "f + l" to
        // AddDoubleFunctionFactory (LONG has no FLOAT overload), so the Java filter computes the sum
        // at DOUBLE width, and so does the JIT - convert() promotes both operands to f64.
        //
        // This shape is NOT red without the production fix, and is not meant to be: a predicate over
        // a FLOAT (4-byte) and a LONG (8-byte) column has mixed sizes, so the observer already types
        // the constant F8 and serializeConstant already emits it exactly. It is here because
        // isFloatLeaf now accepts an arithmetic subtree, which brings this shape into the marking
        // path for the first time - the pin says the marking changes nothing for it.
        //
        // Row 1: f = 5e-8, l = 1, so (double) f + l = 1.0000000500000006, which is NOT < 1.00000003.
        // Row 2: f = 5.0, l = 1 -> 6.0. The bound sits between the two, and only the double-width
        // comparison places row 1 on the right side of it.
        assertMemoryLeak(() -> {
            execute("create table y as (select" +
                    " cast(case when x = 1 then 5e-8 else 5.0 end as float) f," +
                    " cast(case when x = 1 then 1 else 1 end as long) l," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            assertJitMatchesJavaOnEmptyResult("select rn from y where f + l < 1.00000003", true, "rn\n");
            assertJitMatchesJava("select rn from y where f + l > 1.00000003 and rn <= 2", true, "rn\n1\n2\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f + l <= 1.00000003", true, "rn\n");
            assertJitMatchesJavaOnEmptyResult("select rn from y where f + l = 1.00000003", true, "rn\n");
        });
    }

    @Test
    public void testIntFloatColumnsComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_byte() i8," +
                " rnd_short() i16," +
                " rnd_int() i32," +
                " rnd_long() i64," +
                " rnd_float() f32," +
                " rnd_double() f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        // -i32 against f32 stays in the matrix: negation makes the operand an INT-width
        // arithmetic RESULT, which markIntCmpFloatOperand sign-extends with an SX_I64 emitted
        // after the NEG, so the comparison runs at f64 like the Java filter's.
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i8", "i16", "i32", "i64")
                .withComparisonOperator()
                .withOptionalNegation().withAnyOf("f32", "f64");
        assertGeneratedQueryNotNull(ddl, gen);
        assertMemoryLeak(() -> {
            assertJitMatchesJava("x where -i32 > f32", true);
            assertJitMatchesJava("x where -i32 < f32", true);
            assertJitMatchesJava("x where -i32 >= -f32", true);
            assertJitMatchesJava("x where -i32 <= -f32", true);
        });
    }

    @Test
    public void testIntFloatColumnsComparisonFilterOutNulls() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        // See testIntFloatColumnsComparison for how -i32 against f32 keeps the JIT.
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64")
                .withComparisonOperator()
                .withOptionalNegation().withAnyOf("f32", "f64");
        assertGeneratedQueryNullable(ddl, gen);
        assertMemoryLeak(() -> {
            assertJitMatchesJava("x where -i32 > f32", true);
            assertJitMatchesJava("x where -i32 < f32", true);
            assertJitMatchesJava("x where -i32 >= -f32", true);
            assertJitMatchesJava("x where -i32 <= -f32", true);
        });
    }

    @Test
    public void testIntCmpFloatColumnWideLaneMatchesJavaFilter() throws Exception {
        // QuestDB registers no (FLOAT, FLOAT) comparison factory, so `int_col <op> float_col`
        // resolves to the (DOUBLE, DOUBLE) one and the Java filter compares both operands at f64,
        // reading the INT through IntFunction#getDouble. markIntCmpFloatOperand sign-extends the
        // INT leaf so the JIT compares at f64 too.
        //
        // That SX_I64 runs on the scalar backend and on the four-lane loop only - the (i64, f32)
        // arm of avx2::convert (jit/avx2.h:698-704) - so the pairing has to enter wide-lane mode to
        // take four rows per iteration instead of one.
        // CompiledFilterIRSerializer#isWideLaneIntCmpFloatLeafPair is what admits it, and this
        // test is the row-level counterpart of the hint assertions in
        // CompiledFilterIRSerializerTest#testIntCmpFloatColumnWidensIntToI64.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE icfw (k TIMESTAMP, rn INT, i32 INT, i64 LONG, f32 FLOAT, f64 DOUBLE, i8 BYTE)
                      TIMESTAMP(k) PARTITION BY DAY
                    """);
            // 13 rows, so the four-lane loop runs three full iterations and a one-row tail. Every
            // boundary the pairing turns on is present: NULL on each side and on both at once,
            // the 2^24 pairs where an f32 comparison and an f64 one disagree (rn 4 and rn 12),
            // both INT extremes, zero, negatives, and ordinary rows on each side of the operator.
            execute("""
                    INSERT INTO icfw VALUES
                      ('1970-01-01T00:00:01.000000Z',  1, NULL,           1, 1.5,           0.0, 1),
                      ('1970-01-01T00:00:02.000000Z',  2, 7,              2, NULL,          0.0, 1),
                      ('1970-01-01T00:00:03.000000Z',  3, NULL,           3, NULL,          0.0, 1),
                      ('1970-01-01T00:00:04.000000Z',  4, 16_777_217,     4, 16777216.0,    0.0, 1),
                      ('1970-01-01T00:00:05.000000Z',  5, 16_777_216,     5, 16777220.0,    0.0, 1),
                      ('1970-01-01T00:00:06.000000Z',  6, -2_147_483_647, 6, -2.5,          0.0, 1),
                      ('1970-01-01T00:00:07.000000Z',  7, 2_147_483_647,  7, 2.5,           0.0, 1),
                      ('1970-01-01T00:00:08.000000Z',  8, 0,              8, 0.0,           0.0, 1),
                      ('1970-01-01T00:00:09.000000Z',  9, -3,             9, -3.0,          0.0, 1),
                      ('1970-01-01T00:00:10.000000Z', 10, 5,             10, 4.5,           0.0, 1),
                      ('1970-01-01T00:00:11.000000Z', 11, 4,             11, 4.5,           0.0, 1),
                      ('1970-01-01T00:00:12.000000Z', 12, -16_777_217,   12, -16777216.0,   0.0, 1),
                      ('1970-01-01T00:00:13.000000Z', 13, 100,           13, 99.5,          0.0, 1)
                    """);
            // A COLUMN TOP: f32b exists from row 14 on, so rows 1-13 read NULL for it with no
            // stored sentinel - the frame serves them from the column top rather than from data.
            // Rows 14-17 put values on both sides of the comparison and a NULL INT beside one.
            execute("ALTER TABLE icfw ADD COLUMN f32b FLOAT");
            execute("""
                    INSERT INTO icfw VALUES
                      ('1970-01-01T00:00:14.000000Z', 14, 9,    14, 1.0, 0.0, 1, 8.5),
                      ('1970-01-01T00:00:15.000000Z', 15, 9,    15, 1.0, 0.0, 1, 9.5),
                      ('1970-01-01T00:00:16.000000Z', 16, NULL, 16, 1.0, 0.0, 1, 1.0),
                      ('1970-01-01T00:00:17.000000Z', 17, 3,    17, 1.0, 0.0, 1, 3.0)
                    """);

            // The name of this test is a claim about the execution mode, so pin the mode rather
            // than infer it from the rows: every shape below returns the same rows on the scalar
            // loop, so a regression back to scalar would leave the differentials green.
            assertExecHint("icfw", "i32 > f32", EXEC_HINT_WIDE_LANE);
            assertExecHint("icfw", "f32 < i32", EXEC_HINT_WIDE_LANE);
            assertExecHint("icfw", "not (i32 > f32)", EXEC_HINT_WIDE_LANE);
            assertExecHint("icfw", "i32 > f32 * 2", EXEC_HINT_WIDE_LANE);
            assertExecHint("icfw", "i32 > f32b", EXEC_HINT_WIDE_LANE);
            // The chain rides the same loop, which is what costs it AND_SC: serialize() suppresses
            // the scalar-mode detector once hasWideLaneConversionSource() reports a source, and
            // hasIntCmpFloatLeafPair now reports one here. See the trade recorded at that gate.
            assertExecHint("icfw", "i32 > f32 and i64 > 5", EXEC_HINT_WIDE_LANE);
            // A FOUR-byte peer conjunct, which reaches a different arm of the mask combination
            // than the eight-byte one above: "i32 > 10" compares at i32 width, so mask_type gives
            // it an i32 mask (jit/avx2.h:34-43) and bin_and sign-extends that mask's low four lanes
            // through normalize_wide_mask (jit/avx2.h:505-512, 557-562) before ANDing. The widened
            // conjunct's mask is i64, and so is "i64 > 5"'s, so normalize_wide_mask returns those
            // unchanged.
            assertExecHint("icfw", "i32 > f32 and i32 > 10", EXEC_HINT_WIDE_LANE);
            // Controls, and the two shapes this change does NOT reach. i8 has no four-lane
            // widening at all: avx2::sx_i64 declines anything that is not i32 (jit/avx2.h:534-539).
            // "i64 > f32" and "i32 > f64" emit no conversion for the serializer to predict - the
            // backend's convert() does their widening - so hasEmittedWideLaneConversion stays
            // false and getExecHint() cannot answer WIDE_LANE however eligibility is spelled.
            // They keep the scalar loop, as they did before this change; reaching four lanes for
            // them needs a conversion source the serializer does not emit, not a wider eligibility
            // test.
            assertExecHint("icfw", "i8 > f32", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("icfw", "i64 > f32", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("icfw", "i32 > f64", EXEC_HINT_MIXED_SIZE_TYPE);

            // Rows. rn 4 and rn 12 are the pairs that separate an f32 comparison from an f64 one:
            // (float) 16777217 is 16777216, so at f32 width rn 4 compares EQUAL to its 16777216.0
            // peer and rn 12 to its -16777216.0 one. Only the f64 comparison orders them, so a
            // regression to cvt_itof moves them out of ">" / "<" and into "=".
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > f32",
                    "rn\n4\n7\n10\n13\n14\n15\n17\n");
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 < f32",
                    "rn\n5\n6\n11\n12\n");
            // rn 3 holds NULL on BOTH sides, and Numbers.equals() calls two NULLs equal
            // (Numbers.java:846-848), so it belongs to "=" while rn 1, 2 and 16 - a NULL against a
            // number - do not. The backend has to reproduce that asymmetry through the widening:
            // sx_i64 carries INT_NULL to LONG_NULL and cvt_ltod carries that to the f64 NULL, so
            // both sides arrive as the sentinel the vectorized equality reads: avx2::cmp_eq_double
            // ORs a "both operands are NaN" mask over its epsilon comparison
            // (jit/impl/avx2.h:295-310). This assertion runs the scalar loop too, and there the
            // equality is double_cmp_epsilon (jit/impl/x86.h:813, jit/impl/aarch64.h:692).
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 = f32",
                    "rn\n3\n8\n9\n");
            // ">=" and "<=" are spelled "equal or ordered" (LtDoubleVVFunctionFactory:68-69), so
            // they pick up rn 3 with the equality and nothing else from the NULL rows.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 >= f32",
                    "rn\n3\n4\n7\n8\n9\n10\n13\n14\n15\n17\n");
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 <= f32",
                    "rn\n3\n5\n6\n8\n9\n11\n12\n");
            // "<>" is where the one-sided NULL rows show up: rn 1, 2 and 16 are not equal to their
            // peer, while rn 3 is.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 <> f32",
                    "rn\n1\n2\n4\n5\n6\n7\n10\n11\n12\n13\n14\n15\n16\n17\n");

            // INT_NULL under the four-lane loop. The same question for a SYMBOL column is not
            // asked here: isWideLaneIntCmpFloatLeafPair gates on isGenuineIntegerLeaf, which
            // admits INT / LONG / DATE / TIMESTAMP only, so a SYMBOL leaf never reaches this
            // widening. avx2::sx_i64 sign-extends the low four i32 lanes with
            // vpmovsxdq and then, with null checks on, blends LONG_NULL over the lanes that held
            // INT_NULL (jit/avx2.h:547-553); cvt_ltod blends the f64 NULL over those in turn. Without
            // that blend INT_NULL sign-extends to -2_147_483_648, an ordinary number below every
            // bound in this fixture, and rn 1 and rn 16 would join "i32 < f32" - taking the count
            // below from 4 to 6. They must not: Numbers.intToDouble(INT_NULL) is NaN
            // (Numbers.java:1013-1017) on the Java side, and NaN answers no ordering comparison.
            // The f32 side needs no blend - FLOAT NULL is Float.NaN and vcvtps2pd widens it to a
            // double NaN exactly - which is why rn 2 is absent from every ordering result above
            // without any INT-side machinery.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select rn from icfw where i32 < f32 and rn < 5", "rn\n");
            assertJitScalarAndVectorMatchJava("select count() from icfw where i32 < f32", "count\n4\n");

            // Operand order is symmetric: the reversed spelling selects the same rows.
            assertJitScalarAndVectorMatchJava("select rn from icfw where f32 < i32",
                    "rn\n4\n7\n10\n13\n14\n15\n17\n");
            // NOT keeps the pairing inside one predicate. Its answer is the NEGATED comparison
            // rather than the boolean complement of ">": the factory reads the negation flag and
            // evaluates "eq || l > r" (LtDoubleVVFunctionFactory:69), where l and r are the
            // factory's own operands - QuestDB registers no (DOUBLE, DOUBLE) ">" factory, so ">"
            // arrives here as a swapped "<". The one-sided NULL rows - rn 1, 2 and 16 - are
            // therefore outside both ">" and "not (>)". This is the Java filter's own answer, and
            // the widened pairing has to reproduce it.
            assertJitScalarAndVectorMatchJava("select rn from icfw where not (i32 > f32)",
                    "rn\n3\n5\n6\n8\n9\n11\n12\n");
            // An F4 arithmetic peer computes at f32 in the Java filter too - "*(FF)" exists - so
            // only the INT side widens, and the comparison still runs at f64.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > f32 * 2",
                    "rn\n7\n9\n12\n14\n15\n17\n");
            // The chain that loses AND_SC to the wide-lane suppression has to keep its rows.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > f32 and i64 > 5",
                    "rn\n7\n10\n13\n14\n15\n17\n");
            // The four-byte-peer chain has to keep its rows through that mask normalization.
            // Both conjuncts do work here: rn 5 fails only the widened one, rn 10, 14, 15 and 17
            // fail only the peer, and the three NULL-INT rows - rn 1, 3 and 16 - fail both.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > f32 and i32 > 10",
                    "rn\n4\n7\n13\n");

            // The COLUMN TOP. Rows 1-13 read f32b as NULL, so no ordering comparison admits them,
            // and rows 14-17 sit on either side of the bound and on it.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > f32b", "rn\n14\n");
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 < f32b", "rn\n15\n");
            // rn 1 and rn 3 hold a NULL INT, and the column top serves them a NULL f32b, so the
            // two-NULL equality above admits them here as well.
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 = f32b", "rn\n1\n3\n17\n");

            // A FLOAT bind variable is observed exactly like a FLOAT column and takes the same
            // widening; CompiledFilterIRSerializerTest#testIntCmpFloatColumnWidensIntToI64 pins
            // the hint for it and for the INT-bind-variable spelling.
            bindVariableService.clear();
            bindVariableService.setFloat("fv", 4.5f);
            assertJitScalarAndVectorMatchJava("select rn from icfw where i32 > :fv",
                    "rn\n2\n4\n5\n7\n10\n13\n14\n15\n");
        });
    }

    @Test
    public void testIntCmpFloatColumnWideLaneBatchLengths() throws Exception {
        // The four-lane loop steps four rows at a time and leaves the remainder to a scalar tail,
        // so a row count that is not a multiple of four exercises both. Sweeping 0 to 13 rows
        // crosses three full steps and every tail length, with a NULL INT at row 1, a NULL FLOAT
        // at row 2 and the 2^24 pair at row 4 - so the widening, the NULL blends and the tail all
        // see rows on both sides of each predicate.
        assertMemoryLeak(() -> {
            for (int rowCount = 0; rowCount <= 13; rowCount++) {
                execute("drop table if exists icfb");
                if (rowCount == 0) {
                    execute("create table icfb (i32 int, i64 long, f32 float)");
                } else {
                    execute("create table icfb as (select "
                            + "cast(case x when 1 then null when 4 then 16_777_217 when 5 then -16_777_217 else x - 3 end as int) i32, "
                            + "cast(x as long) i64, "
                            + "cast(case x when 2 then null when 4 then 16777216.0 when 5 then -16777216.0 else 2.5 end as float) f32 "
                            + "from long_sequence(" + rowCount + "))");
                }

                assertJitMatchesJavaOnBatchLengths("icfb where i32 > f32", true);
                assertJitMatchesJavaOnBatchLengths("icfb where i32 < f32", true);
                assertJitMatchesJavaOnBatchLengths("icfb where i32 = f32", true);
                assertJitMatchesJavaOnBatchLengths("icfb where i32 <> f32", true);
                assertJitMatchesJavaOnBatchLengths("icfb where f32 <= i32", true);
                assertJitMatchesJavaOnBatchLengths("icfb where not (i32 > f32)", true);
                assertJitMatchesJavaOnBatchLengths("icfb where i32 > f32 * 2", true);
                assertJitMatchesJavaOnBatchLengths("icfb where i32 > f32 and i64 > 2", true);
                assertJitMatchesJava("select count() from icfb where i32 > f32", true);
                assertJitMatchesJava("select count() from icfb where i32 < f32", true);
            }
            // Row parity cannot see which loop ran: the scalar loop answers these shapes
            // correctly too, so the sweep above stays green if the wide-lane eligibility is
            // reverted. Pin the mode the test is named for. getExecHint() reads the metadata
            // and the parsed filter, not the row count, so one check after the sweep covers
            // every iteration of it; the remaining shapes are pinned over the icfw fixture by
            // testIntCmpFloatColumnWideLaneMatchesJavaFilter.
            assertExecHint("icfb", "i32 > f32", EXEC_HINT_WIDE_LANE);
            assertExecHint("icfb", "i32 > f32 and i64 > 2", EXEC_HINT_WIDE_LANE);
            assertBatchSweepReturnedRows();
        });
    }

    @Test
    public void testInterval() throws Exception {
        final String query = "x where k in '2021-11-29' and i32 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(to_timestamp('2021-11-29T10:00:00', 'yyyy-MM-ddTHH:mm:ss'), 500_000_000) as k," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testIPv4OrderingNestedInPredicateUsesCompiledFilter() throws Exception {
        // https://github.com/questdb/questdb/issues/7547
        // The IPv4 twin of testCharOrderingNestedInPredicateUsesCompiledFilter. The fixture holds
        // IPv4 NULL (0), 128.0.0.0 (INT_MIN) and 255.255.255.255 (-1), so the unsigned-order repair
        // the expansion performs is exercised on both sides of the sign boundary.
        assertMemoryLeak(() -> {
            createIPv4TestTable();

            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) = (ip2 < ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip2 < ip) = (ip < ip2)");
            assertJitMatchesJavaInAllModes("x WHERE (ip <= ip2) = (ip2 <= ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip > ip2) = (ip2 > ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip >= ip2) = (ip2 >= ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip < '128.0.0.0') = (ip2 < '255.255.255.255')");
            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) <> (ip2 < ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip = ip2) = (ip < ip2)");
            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) = (ip = ip2)");
            assertJitMatchesJavaInAllModes("x WHERE NOT (ip < ip2)");
            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) AND (ip2 > ip)");
            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) OR (ip2 < ip)");
            assertJitMatchesJavaInAllModes("x WHERE ((ip < ip2) = (ip2 < ip)) AND ((ip >= ip2) OR (ip <= ip2))");
            assertJitMatchesJavaInAllModes("x WHERE NOT ((ip < ip2) = (ip2 < ip))");
            assertJitMatchesJavaInAllModes("x WHERE (ip < ip2) = (ip = '128.0.0.0')");
            assertJitMatchesJavaInAllModes("x WHERE (ip = '128.0.0.0') = (ip < ip2)");

            assertJitMatchesJava("x WHERE (ip < ip2) = true", false);
            assertJitMatchesJava("x WHERE (ip < ip2) = false", false);
            assertJitMatchesJava("x WHERE true = (ip < ip2)", false);
            assertJitMatchesJava("x WHERE false = (ip < ip2)", false);
        });
    }

    @Test
    public void testIPv4OrderingUsesCompiledFilter() throws Exception {
        // https://github.com/questdb/questdb/issues/7547
        assertMemoryLeak(() -> {
            createIPv4TestTable();

            assertJitMatchesJavaInAllModes("x WHERE ip > ip2");
            assertJitMatchesJavaInAllModes("x WHERE ip >= ip2");
            assertJitMatchesJavaInAllModes("x WHERE ip < ip2");
            assertJitMatchesJavaInAllModes("x WHERE ip <= ip2");
            assertJitMatchesJavaInAllModes("x WHERE ip > null");
            assertJitMatchesJavaInAllModes("x WHERE ip >= null");
            assertJitMatchesJavaInAllModes("x WHERE ip < null");
            assertJitMatchesJavaInAllModes("x WHERE ip <= null");
            assertJitMatchesJavaInAllModes("x WHERE null <= ip");
            for (String literal : new String[]{
                    "'127.255.255.255'",
                    "'128.0.0.0'",
                    "'255.255.255.255'",
                    "'0.0.0.0'",
                    "'null'"
            }) {
                for (String operator : new String[]{"<", "<=", ">", ">="}) {
                    assertJitMatchesJavaInAllModes("x WHERE ip " + operator + " " + literal);
                    assertJitMatchesJavaInAllModes("x WHERE " + literal + " " + operator + " ip");
                }
            }

            // Equality agrees with the Java filter, so it keeps compiling.
            assertJitMatchesJava("x WHERE ip = ip2", true);
            assertJitMatchesJava("x WHERE ip != ip2", true);
            assertJitMatchesJava("x WHERE ip = null", true);
        });
    }

    @Test
    public void testIPv4QuotedLiteralPredicatesUseCompiledFilter() throws Exception {
        assertMemoryLeak(() -> {
            createIPv4TestTable();

            for (String predicate : new String[]{
                    "ip = '128.0.0.0'",
                    "'128.0.0.0' = ip",
                    "ip != '255.255.255.255'",
                    "'255.255.255.255' != ip",
                    "ip <> '127.255.255.255'",
                    "'127.255.255.255' <> ip",
                    "ip = 'NuLl'",
                    "'NuLl' = ip",
                    "ip IN ('128.0.0.0')",
                    "ip NOT IN ('128.0.0.0')",
                    "ip IN ('127.255.255.255', '128.0.0.0', '255.255.255.255', 'NuLl')",
                    "ip NOT IN ('127.255.255.255', '128.0.0.0', '255.255.255.255', 'NuLl')",
                    "k >= 0 AND ip IN ('NuLl')",
                    "k >= 0 AND ip NOT IN ('128.0.0.0')",
                    "k >= 0 AND ip IN ('127.255.255.255', '128.0.0.0', '255.255.255.255', 'NuLl')",
                    "k >= 0 AND ip NOT IN ('127.255.255.255', '128.0.0.0', '255.255.255.255', 'NuLl')"
            }) {
                assertJitMatchesJavaInAllModes("x WHERE " + predicate);
            }

            for (int jitMode : new int[]{
                    SqlJitMode.JIT_MODE_DISABLED,
                    SqlJitMode.JIT_MODE_FORCE_SCALAR,
                    SqlJitMode.JIT_MODE_ENABLED
            }) {
                sqlExecutionContext.setJitMode(jitMode);
                assertExceptionNoLeakCheck(
                        "x WHERE ip = '999.1.1.1'",
                        0,
                        "invalid IPv4 format: 999.1.1.1"
                );
            }
        });
    }

    @Test
    public void testNarrowIntArithUnderLongWithFloat() throws Exception {
        // A narrow INT arithmetic subtree (c8 * -776_782) that overflows int32
        // and feeds a LONG-width multiply diverged between the JIT and the Java
        // filter when a FLOAT comparison suppressed the narrow-to-i64 widening:
        // the JIT wrapped the inner product mod 2^32 while the Java filter read
        // it at long width via MulInt#getLong. The serializer now sign-extends
        // exactly the narrow leaves under the LONG-width subtree, so the JIT
        // stays on and both paths agree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c0 LONG, c2 SHORT, c8 INT, c9 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_long(-1_000_000, 1_000_000, 8), rnd_short(), " +
                    "rnd_int(-1_000_000, 1_000_000, 8), rnd_float(8), " +
                    "timestamp_sequence(to_timestamp('2024-01-01', 'yyyy-MM-dd'), 1_800_000_000L) " +
                    "FROM long_sequence(122)");

            // The rows come from rnd_*, so there is no expected-rows string to pin. Pin the Java
            // filter's row count instead: it is the independent oracle here (the fix is entirely in
            // the serializer, so the Java path never moved), and it also proves each predicate is
            // non-vacuous. Parity over an all-empty or an all-122 result would prove nothing.
            Assert.assertEquals(55, runQuery("SELECT * FROM t WHERE c9 <= ((c0 - c2) * (c8 * -776_782))"));
            Assert.assertEquals(55, runQuery("SELECT * FROM t WHERE c9 <= (c0 * (c8 * -776_782))"));
            Assert.assertEquals(61, runQuery("SELECT * FROM t WHERE c9 <= (c0 + (c8 * -776_782))"));
            Assert.assertEquals(60, runQuery("SELECT * FROM t WHERE c9 <= (c0 * (c8 + 2_000_000_000))"));
            Assert.assertEquals(66, runQuery("SELECT * FROM t WHERE c9 <= (c8 * -776_782)"));
            Assert.assertEquals(54, runQuery("SELECT * FROM t WHERE c9 <= (c0 * c8)"));
            Assert.assertEquals(67, runQuery("SELECT * FROM t WHERE (c8 * -776_782) > 0"));

            // Previously diverging shapes: still JIT-compiled, now correct.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= ((c0 - c2) * (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 + (c8 * -776_782))", true);
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * (c8 + 2_000_000_000))", true);
            // An INT-width product against a FLOAT column keeps the JIT: the Java filter reads
            // the wrapped product at f64, and SX_I64 after the multiply widens that same wrapped
            // result so convert() takes the (i64, f32) arm instead of rounding to f32. See
            // markIntCmpFloatOperand.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c8 * -776_782)", true);
            // Control shapes that were always correct under JIT. c0 * c8 is LONG-width, so its
            // pairing with the FLOAT column already lands on the (i64, f32) arm.
            assertJitMatchesJava("SELECT * FROM t WHERE c9 <= (c0 * c8)", true);
            assertJitMatchesJava("SELECT * FROM t WHERE (c8 * -776_782) > 0", true);
        });
    }

    @Test
    public void testNarrowIntChainOverflowUnderLong() throws Exception {
        // C2: a narrow-int (SHORT / BYTE) arithmetic CHAIN that overflows int32 was
        // computed at int32 by the JIT and wrapped, while the Java filter read it at
        // long width (MulInt#getLong recurses through getLong, so the whole chain is
        // 64-bit). NarrowI64WidenDetector.shouldWiden() required hasI4(): a narrow-only
        // product observes only I2/I1 + I8 (no I4), so the widening never fired. A
        // 2-factor narrow product stays inside int32 (32_767^2 < 2^31), but 3+ factors
        // overflow (1500^3 = 3_375_000_000 wraps to -919_967_296). shouldWiden() now
        // also triggers on hasNarrowInt(), so the JIT widens the chain to match Java.
        assertMemoryLeak(() -> {
            execute("create table nchain as (select" +
                    " x rid," +
                    " cast(case x when 1 then 1500 when 2 then 1000 when 3 then 1500 when 4 then 2000 else 1500 end as short) cs," +
                    " cast(case x when 1 then 100 when 2 then 10 when 3 then 100 when 4 then 50 else 100 end as byte) cbyte," +
                    " cast(case x when 1 then 3_375_000_000 when 2 then 1_000_000_000 when 3 then 0 when 4 then 8_000_000_000 else -919_967_296 end as long) nl," +
                    " cast(case x when 1 then 10_000_000_000 when 2 then 100_000 when 3 then 0 when 4 then 312_500_000 else 1_410_065_408 end as long) nb," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(5)) timestamp(k)");
            // Primary repro (SHORT, 3-factor). The chain computes at i32 and wraps, so it equals
            // the wrapped image rows - row 5 holds -919_967_296, the int32 cube of 1500 - and never
            // the long-width cube. Row 2's 1000^3 fits int32, so it matches at either width.
            // Absolute pin: {2,5}. The JIT and Java must agree on the wrap, which is the point.
            assertJitMatchesJava("select rid from nchain where nl = cs * cs * cs", true,
                    "rid\n2\n5\n");
            // BYTE, 5-factor (127^3 still fits int32, so a byte chain needs 5 factors to
            // overflow). Row 5 holds the int32-wrapped 100^5 (1_410_065_408) and matches; rows 2
            // and 4 stay inside int32. Absolute pin: {2,4,5}.
            assertJitMatchesJava("select rid from nchain where nb = cbyte * cbyte * cbyte * cbyte * cbyte", true,
                    "rid\n2\n4\n5\n");
            // Operand order agrees, and a wider chain does too. cs^4 wraps negative for every cs
            // in this table, so '>' matches nothing while '<' matches - both engines have to say so.
            assertJitMatchesJava("nchain where cs * cs * cs = nl", true);
            assertJitMatchesJavaOnEmptyResult("nchain where cs * cs * cs * cs > nl", true);
            assertJitMatchesJava("nchain where cs * cs * cs * cs < nl", true);
            // A boolean equality mixes a LONG-peer comparison (nl = cs*cs*cs) with an INT one
            // (cs*cs*cs = 0). The chain computes at i32 in both, so the peer cannot change it.
            assertJitMatchesJava("nchain where (nl = cs * cs * cs) = (cs * cs * cs = 0)", true);
            // Controls: the same chain with no LONG operand at all, and a 2-factor narrow product
            // that never overflows int32.
            assertJitMatchesJavaOnEmptyResult("nchain where cs * cs * cs = 0", true);
            assertJitMatchesJavaOnEmptyResult("nchain where nl = cs * cs", true);
        });
    }

    @Test
    public void testNarrowIntColumnVsFloatingPointConstantInMixedWidthChain() throws Exception {
        // The pairing in testNarrowIntColumnVsFloatingPointConstant, chained with a conjunct of a
        // DIFFERENT width. The mixed INT / LONG widths send the chain through the scalar-mode
        // detector, which serialize() runs only when hasWideLaneConversionSource() proves no
        // conversion can be emitted. That predicate modelled the F4-leaf source and the
        // narrow-leaf-vs-64-bit-operand source, not this third one, so the chain took the
        // short-circuit path, tripped its own wide-lane guard, and SqlCodeGenerator turned the
        // resulting SqlException into a Java-filter fallback: the rows stayed right and the
        // compiled filter disappeared. assertJitScalarAndVectorMatchJava asserts the compiled
        // filter IS used in both JIT modes, so it pins the fallback itself.
        assertMemoryLeak(() -> {
            execute("create table zm as (select" +
                    " cast(case when x = 1 then 1 when x = 2 then 16_777_217 else 5 end as int) i," +
                    " cast(5 as long) l," +
                    " cast(2.0 as double) d," +
                    " cast(x as int) rn," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            // (float) 16777217 is 16777216, so only the double-width comparison keeps row 2.
            assertJitScalarAndVectorMatchJava("select rn from zm where i > 16777216.0 and l = 5 and rn <= 3", "rn\n2\n");
            // The OR spelling rides the same gate. l = 6 matches nothing, so the bound decides.
            assertJitScalarAndVectorMatchJava("select rn from zm where i > 16777216.0 or l = 6", "rn\n2\n");
            // A DOUBLE sibling mixes the widths the same way a LONG one does. 1.00000003 has no
            // exact float; rounded to 1.0f it would drop the row holding 1.
            assertJitScalarAndVectorMatchJava("select rn from zm where i < 1.00000003 and d > 1.5", "rn\n1\n");
            // Negating the conjunct keeps the pairing inside one predicate.
            assertJitScalarAndVectorMatchJava("select rn from zm where not (i > 16777216.0) and l = 5 and rn <= 3", "rn\n1\n3\n");

            // Control: an arithmetic subtree widens only its CONSTANT, emits no SX_I64, and so is
            // not a conversion source - the chain keeps its scalar short-circuit path and must
            // still compile and agree.
            assertJitScalarAndVectorMatchJava("select rn from zm where i + 0 > 16777216.0 and l = 5 and rn <= 3", "rn\n2\n");
            // Control: a bound whose rounding cannot reach any INT row is not a source either.
            assertJitScalarAndVectorMatchJava("select rn from zm where i > 1.1 and l = 5 and rn <= 3", "rn\n2\n3\n");
        });
    }

    @Test
    public void testNonFiniteConstantArithmeticMatchesJava() throws Exception {
        // FunctionParser folds every constant subtree bottom-up through
        // DoubleConstant#newInstance, which maps +/-Infinity and NaN onto the NULL
        // sentinel, so the Java filter compares against NULL. The serializer emitted the
        // operations instead and let the backend compute them, so the JIT compared against
        // a real +/-Infinity. double_cmp_epsilon (jit/impl/x86.h) reads both as NULL, but
        // double_lt/le/gt/ge order an infinity like an ordinary number, so every ordering
        // operator disagreed: "d <= 1e308 * 10.0" returned the NULL row alone on the Java
        // filter and EVERY row on the JIT.
        //
        // The serializer now declines such a filter, so the Java filter decides it - hence
        // expectJit is false on the diverging shapes.
        assertMemoryLeak(() -> {
            execute("create table nf as (select" +
                    " cast(x as double) d," +
                    " cast(x as float) f," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(3)) timestamp(k)");
            execute("insert into nf values (null, null, '1970-01-01T00:00:03.000000Z')");

            // Multiplicative overflow. Absolute pin: the NULL row alone, because the Java
            // filter's bound is NULL and only a NULL row is tolerance-equal to it.
            assertJitMatchesJava("nf where d <= 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // Strict ordering against the same bound keeps nothing at all.
            assertJitMatchesJavaOnEmptyResult("nf where d < 1e308 * 10.0", false, "d\tf\tk\n");
            assertJitMatchesJavaOnEmptyResult("nf where d > -1e308 * 10.0", false, "d\tf\tk\n");
            assertJitMatchesJava("nf where d >= -1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // Additive overflow and a unary minus over a non-finite subtree reach the same
            // fold through different operators.
            assertJitMatchesJava("nf where d <= 1e308 + 1e308", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d >= -1e308 - 1e308", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d >= -(1e308 * 10.0)", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // A constant division by zero folds the same way.
            assertJitMatchesJavaOnEmptyResult("nf where 1.0 / 0.0 > d", false, "d\tf\tk\n");
            // The fold must normalise at EVERY step, not just at the end: the parser turns
            // 1e308 * 10.0 into NULL before the enclosing division sees it, so the whole
            // expression is NULL to the Java filter - while raw IEEE gives a finite 0.0 and
            // the pre-fix JIT selected rows against that instead.
            assertJitMatchesJava("nf where d <= 1.0 / (1e308 * 10.0)", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            // A non-finite fold anywhere declines the whole filter, not just its conjunct.
            assertJitMatchesJavaOnEmptyResult("nf where d <= 1e308 * 10.0 and d > -100.0", false, "d\tf\tk\n");

            // Leaf shapes the type classifiers and the numeric parsers disagree about. The guard
            // is fail-closed on subtree SHAPE, so these decline too. Underscore separators are the
            // style CLAUDE.md mandates and arithExprType reads them through Numbers.parseInt,
            // while Numbers.parseDouble rejects them; 'd'/'D' suffixes are accepted by
            // FunctionParser.createConstant but unknown to floatConstantTypeCode. Both used to
            // slip past the guard and return every row on the JIT.
            assertJitMatchesJava("nf where d <= 1_000_000_000 * 1e300", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJavaOnEmptyResult("nf where d < 1_000_000_000 * 1e300", false, "d\tf\tk\n");
            assertJitMatchesJava("nf where d >= -1_000_000_000 * 1e300", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d <= 1d * 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");
            assertJitMatchesJava("nf where d <= 1D * 1e308 * 10.0", false,
                    "d\tf\tk\n" +
                            "null\tnull\t1970-01-01T00:00:03.000000Z\n");

            // Controls: a constant subtree that folds finite still compiles and still runs
            // on the JIT, at both widths and through division.
            assertJitMatchesJava("nf where d <= 1e10 * 10.0", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where d > 4.0 / 2.0", true,
                    "d\tf\tk\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where f <= 1e10 * 10.0", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            // Controls: the same awkward leaf shapes folding FINITE must keep their JIT. Failing
            // closed on shape alone would have cost the JIT on ordinary filters, since the
            // underscore separator is the mandated style - parseFoldLeaf walks createConstant's
            // parser ladder so these fold rather than decline.
            assertJitMatchesJava("nf where d <= 1_000_000_000 * 1e3", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            assertJitMatchesJava("nf where d <= 1d * 1e3", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
            // Controls: an INTEGER constant division by zero keeps its JIT. tryFoldConstantArith0
            // declines that fold deliberately so the IR carries the division and the native
            // int32_div/int64_div produce the same NULL sentinel DivInt/DivLong do. Judging it by
            // float rules would read 1 / 0 as an infinity and decline a filter that already agrees.
            assertJitMatchesJavaOnEmptyResult("nf where d > 1 / 0", true, "d\tf\tk\n");
            assertJitMatchesJavaOnEmptyResult("nf where d > 10 / (5 - 5)", true, "d\tf\tk\n");
            assertJitMatchesJavaOnEmptyResult("nf where d > 1 / 0 + 5", true, "d\tf\tk\n");
            // Control: an integer constant fold that overflows LONG wraps on both paths and
            // must NOT be mistaken for a non-finite float fold.
            assertJitMatchesJava("nf where d > 9223372036854775807 * 2", true,
                    "d\tf\tk\n" +
                            "1.0\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "2.0\t2.0\t1970-01-01T00:00:01.000000Z\n" +
                            "3.0\t3.0\t1970-01-01T00:00:02.000000Z\n");
        });
    }

    @Test
    public void testNotInOperatorFloat() throws Exception {
        // Tests NOT IN operator with floats
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_double() f64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("f64")
                .withAnyOf(" not in ")
                .withAnyOf("(0.1, 0.2, 0.3)", "(0.5, 0.6)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testNotInOperatorInt() throws Exception {
        // Tests NOT IN operator with integers
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 0) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" not in ")
                .withAnyOf("(1, 2, 3)", "(5, 6, 7, 8)");
        assertGeneratedQueryNotNull(ddl, gen);
    }

    @Test
    public void testNotInOperatorWithNull() throws Exception {
        // Tests NOT IN operator with null in the list
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(1, 10, 5) i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32")
                .withAnyOf(" not in ")
                .withAnyOf("(1, 2, null)", "(5, null, 7)");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withOptionalNegation().withAnyOf("i32", "i64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null")
                .withBooleanOperator()
                .withOptionalNegation().withAnyOf("f32", "f64")
                .withAnyOf(" = ", " <> ")
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testNullValueComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int(-10, 10, 10) i32," +
                " rnd_long(-10, 10, 10) i64," +
                " rnd_float(10) f32," +
                " rnd_double(10) f64 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("i32", "i64", "f32", "f64")
                .withComparisonOperator()
                .withAnyOf("1");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testOrderByAsc() throws Exception {
        testOrderBy("order by ts asc");
    }

    @Test
    public void testOrderByDesc() throws Exception {
        testOrderBy("order by ts desc");
    }

    @Test
    public void testRepeatedColumnReadsReuseOneLoad() throws Exception {
        // Every backend now reuses one load per column per loop body, so a predicate reading the
        // same operand several times reads memory once. The CHAR and IPv4 ordering expansions are
        // what make that common: they re-traverse each operand four and five times respectively,
        // to build the unsigned ordering out of signed comparisons. The vectorized backend had no
        // value cache at all, and the scalar ones added an i128 value through addXmm but looked it
        // up through find(), which answers only for a general-purpose register, so a UUID column
        // was cached and never found.
        //
        // Sharing one register is sound only while no backend helper writes into an operand
        // register, and four sites did. impl/avx2.h's cmp_eq folded its per-qword result into lhs
        // for the i128 arm and mul did the same with muleven for the i8 one; impl/x86.h's
        // int128_cmp folded pcmpeqb, which is the two-operand SSE form, into lhs, and x86.h's
        // flag-based short-circuit arm carried a second copy of that same sequence. None could
        // show before, because every read allocated a register of its own. Once the reads share
        // one, the first comparison rewrites the very register the second one has to read.
        //
        // The OR chain below is what reaches the short-circuit arm: it emits Or_Sc, so the
        // equality never materialises a boolean and compares on flags instead. It is a separate
        // call site from cmp_eq, so a fix in int128_cmp alone leaves it wrong - the shape returned
        // only the rows matching the FIRST constant.
        //
        // The i128 arms are the reachable half, in both backends: a UUID column is 16 bytes, so
        // Function::compile picks step = 256 / (16 * 8) = 2 and the filter runs an AVX2 loop, and
        // the scalar loop runs it in forced-scalar mode and in the tail. BYTE arithmetic never
        // reaches an AVX2 loop at all, since visit() forces the scalar backend for it.
        //
        // 35 rows: odd, and above the widest step here, so every lane in the fixture runs full
        // vector iterations AND leaves a non-empty scalar tail. assertJitMatchesJavaInAllModes
        // covers forced-scalar too, which is what pins the scalar half.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (u UUID, ip IPv4, c CHAR, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY");
            execute(
                    """
                            INSERT INTO x SELECT
                                (CASE WHEN x % 3 = 0 THEN '11111111-1111-1111-1111-11111111111a'
                                      WHEN x % 3 = 1 THEN '22222222-2222-2222-2222-222222222222'
                                      ELSE '33333333-3333-3333-3333-333333333333' END)::UUID,
                                (CASE WHEN x % 3 = 0 THEN '10.0.0.1'
                                      WHEN x % 3 = 1 THEN '200.0.0.1'
                                      ELSE '128.0.0.0' END)::IPv4,
                                (CASE WHEN x % 3 = 0 THEN 'a' WHEN x % 3 = 1 THEN 'm' ELSE 'z' END)::CHAR,
                                timestamp_sequence(0, 1_000_000)
                            FROM long_sequence(35)
                            """
            );
            bindVariableService.setChar("lo", 'a');
            bindVariableService.setChar("hi", 'z');

            // Two i128 comparisons over ONE column. Whichever is serialized first clobbered the
            // shared register, so the second compared the first one's result against a UUID. Both
            // backends' cmp_ne routes through their cmp_eq, so it carries the same arm. Both
            // conjunct orders, because PostOrderTreeTraversalAlgo descends node.rhs first and only
            // the second comparison to run reads the damaged register.
            assertJitMatchesJavaInAllModes("x WHERE u = '11111111-1111-1111-1111-11111111111a'"
                    + " OR u = '33333333-3333-3333-3333-333333333333'");
            assertJitMatchesJavaInAllModes("x WHERE u = '33333333-3333-3333-3333-333333333333'"
                    + " OR u = '11111111-1111-1111-1111-11111111111a'");
            assertJitMatchesJavaInAllModes("x WHERE u <> '22222222-2222-2222-2222-222222222222'"
                    + " AND u <> '33333333-3333-3333-3333-333333333333'");
            assertJitMatchesJavaInAllModes("x WHERE u = '22222222-2222-2222-2222-222222222222'"
                    + " AND u <> '33333333-3333-3333-3333-333333333333'");
            assertJitCountQuery("SELECT count() FROM x WHERE u = '11111111-1111-1111-1111-11111111111a'"
                    + " OR u = '33333333-3333-3333-3333-333333333333'", 23);
            assertJitCountQuery("SELECT count() FROM x WHERE u <> '22222222-2222-2222-2222-222222222222'"
                    + " AND u <> '33333333-3333-3333-3333-333333333333'", 11);

            // The expansions themselves, which is what the cache is for: an IPv4 range reads each
            // operand five times per body and a CHAR one four times.
            assertJitMatchesJavaInAllModes("x WHERE ip > '10.0.0.0' AND ip < '200.0.0.0'");
            assertJitMatchesJavaInAllModes("x WHERE c > 'a' AND c < 'z'");
            assertJitCountQuery("SELECT count() FROM x WHERE ip > '10.0.0.0' AND ip < '200.0.0.0'", 23);
            assertJitCountQuery("SELECT count() FROM x WHERE c > 'a' AND c < 'z'", 12);

            // Same shape with bind variables, which the cache holds under a numbering of their
            // own: read_vars_mem broadcasts each of these four times per body without it. CHAR
            // rather than IPv4 only because BindVariableService names no IPv4 setter.
            assertJitMatchesJavaInAllModes("x WHERE c > :lo AND c < :hi");
            assertJitCountQuery("SELECT count() FROM x WHERE c > :lo AND c < :hi", 12);
        });
    }

    @Test
    public void testShortCircuitAndDeepChain() throws Exception {
        // Tests short-circuit AND with a deep chain of predicates
        final String query = "x where " +
                "i64 > 10 and i64 < 90 and i64 != 20 and i64 != 30 " +
                "and i64 != 40 and i64 != 50 and i64 != 60 and i64 != 70";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitAndEarlyExit() throws Exception {
        // Tests short-circuit AND where first predicate is usually false
        // This tests early exit optimization
        final String query = "x where " +
                "i64 > 95 and i64 < 100 and i32 > 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationAndEq() throws Exception {
        // Tests flag-based optimization for equality in AND chains.
        // When EQ is followed by And_Sc, the backend emits CMP + JNE directly
        // instead of CMP + SETE + TEST + JZ (kFlagsEq optimization).
        final String query = "x " +
                "where i64 = 95 and i32 = 13 and i16 = 12_107";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationAndNeq() throws Exception {
        // Tests flag-based optimization for inequality in AND chains.
        // When NE is followed by And_Sc, the backend emits CMP + JE directly
        // instead of CMP + SETNE + TEST + JZ (kFlagsNe optimization).
        final String query = "x " +
                "where i64 != 50 and i32 != 25 and i16 != 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationMixedEqNeq() throws Exception {
        // Tests flag-based optimization with mixed EQ and NE in the same chain.
        final String query = "x " +
                "where i64 = 26 and i32 != 42 and i16 = 6201";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationOrEq() throws Exception {
        // Tests flag-based optimization for equality in OR chains.
        // When EQ is followed by Or_Sc, the backend emits CMP + JE directly
        // instead of CMP + SETE + TEST + JNZ (kFlagsEq optimization).
        final String query = "x where " +
                "i64 = 50 or i32 = 25 or i16 = 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationOrNeq() throws Exception {
        // Tests flag-based optimization for inequality in OR chains.
        // When NE is followed by Or_Sc, the backend emits CMP + JNE directly
        // instead of CMP + SETNE + TEST + JNZ (kFlagsNe optimization).
        final String query = "x where " +
                "i64 != 50 or i32 != 25 or i16 != 10";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int(0, 50, 0) i32," +
                " rnd_short() i16" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationUuid() throws Exception {
        // Tests flag-based optimization for UUID (i128) comparisons.
        // UUID comparison uses pcmpeqb + pmovmskb + cmp, then JE/JNE.
        final String query = "x " +
                "where uuid1 = 'd37facdc-c648-4f32-887c-c184027ff724' and i64 = 57";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitFlagOptimizationUuidNeq() throws Exception {
        // Tests flag-based optimization for UUID (i128) inequality comparisons.
        final String query = "x where " +
                "uuid1 != '11111111-1111-1111-1111-111111111111' and i64 = 50";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitMixedAndOr() throws Exception {
        // Tests mixed AND/OR chains with short-circuit evaluation
        final String query = "x where " +
                "(i64 > 20 and i64 < 40) or (i64 > 60 and i64 < 80)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitNestedAndOr() throws Exception {
        // Tests nested AND/OR with multiple levels
        final String query = "x where " +
                "(a > 10 and b > 10) or (c > 10 and d > 10) or (a < 5 and c < 5)";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) a," +
                " rnd_long(0, 100, 0) b," +
                " rnd_long(0, 100, 0) c," +
                " rnd_long(0, 100, 0) d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitOrDeepChain() throws Exception {
        // Tests short-circuit OR with a deep chain of predicates
        final String query = "x where " +
                "i64 = 10 or i64 = 20 or i64 = 30 or i64 = 40 " +
                "or i64 = 50 or i64 = 60 or i64 = 70 or i64 = 80";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testShortCircuitOrEarlyExit() throws Exception {
        // Tests short-circuit OR where first predicate is usually true
        // This tests early exit optimization
        final String query = "x where " +
                "i64 < 95 or i64 = 99 or i32 < 0";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_long(0, 100, 0) i64," +
                " rnd_int() i32" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testSymbolKnownConstant() throws Exception {
        // The column order is important here, since we want
        // query and table column indexes to be different.
        final String query = "select price, sym from x where sym = 'HBC' or sym = 'DXR'";
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    @Test
    public void testSymbolNegativeNumericConstant() throws Exception {
        // https://github.com/questdb/questdb/issues/7548
        // The parser splits `-5` into a unary minus over the token "5", and the
        // SYMBOL arm of the constant serializer used to drop the sign, resolving
        // the key of '5' instead of '-5'.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO x VALUES ('5', 0), ('-5', 1), (null, 2)");

            assertJitMatchesJava("x WHERE sy = -5", true);
            assertJitMatchesJava("x WHERE sy != -5", true);
            assertJitMatchesJava("x WHERE sy = '-5'", true);
            assertJitMatchesJava("x WHERE sy = 5", true);
            // Deferred (not yet in the symbol table) negative constant. No row carries '-42', so
            // the correct answer is the empty result.
            assertJitMatchesJavaOnEmptyResult("x WHERE sy = -42", true);
        });
    }

    @Test
    public void testSymbolNegativeQuotedNumericConstantEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO x VALUES ('-5', 0), ('5', 1), ('other', 2), (null, 3)");
            execute("CREATE TABLE y (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO y VALUES ('-5', 0), ('other', 1), (null, 2)");

            assertJitMatchesJavaInAllModes("x WHERE sy = -'5'");
            assertJitMatchesJavaInAllModes("y WHERE sy = -'5'");
        });
    }

    @Test
    public void testSymbolNegativeQuotedNumericConstantNotEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO x VALUES ('-5', 0), ('5', 1), ('other', 2), (null, 3)");
            execute("CREATE TABLE y (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO y VALUES ('-5', 0), ('other', 1), (null, 2)");

            assertJitMatchesJavaInAllModes("x WHERE sy != -'5'");
            assertJitMatchesJavaInAllModes("y WHERE sy != -'5'");
        });
    }

    @Test
    public void testSymbolNumericNegativeZeroEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO x VALUES ('0', 0), ('-0', 1), (null, 2)");

            assertJitMatchesJavaInAllModes("x WHERE sy = -0");
        });
    }

    @Test
    public void testSymbolNumericNegativeZeroNotEquals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sy SYMBOL, k TIMESTAMP) TIMESTAMP(k)");
            execute("INSERT INTO x VALUES ('0', 0), ('-0', 1), (null, 2)");

            assertJitMatchesJavaInAllModes("x WHERE sy != -0");
        });
    }

    @Test
    public void testSymbolNull() throws Exception {
        final String query = "x where sym <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_symbol(10,1,3,5) sym" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampComparison() throws Exception {
        final String query = "x where t1 != t2";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t1," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 0) t2" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testTimestampComparison2() throws Exception {
        final String query = "x where ts >= 0";
        final String ddl = "create table x as " +
                "(select case when x < 10 then cast(NULL as TIMESTAMP) else cast(x as TIMESTAMP) end ts" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + "))";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testTimestampNull() throws Exception {
        final String query = "x where t <> null";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_timestamp(to_timestamp('2020', 'yyyy'), to_timestamp('2021', 'yyyy'), 5) t" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNullable(query, ddl);
    }

    @Test
    public void testUnaryMinusNarrowLeafVsOutOfRangeConstantMatchesJava() throws Exception {
        // markNarrowConstCmpWidenPair only recognises a bare LITERAL / BIND_VARIABLE as a narrow-int
        // leaf, so a unary-minus-wrapped column slipped past it, and the fallback
        // maybeWidenCmpConstOperand was gated on isFloatActive and never ran. serializeNumber then
        // emitted 2147483649 as a lossy F4 immediate. The scalar and vectorized backends disagreed
        // with EACH OTHER on the OR shape, so the same query on the same data returned different
        // rows depending on whether the host has AVX2.
        assertMemoryLeak(() -> {
            execute("create table n as (select" +
                    " cast(-2147483647 as int) i," +
                    " false b," +
                    " timestamp_sequence(0, 1_000_000) k" +
                    " from long_sequence(64)) timestamp(k)");

            final String rows = "i\n" + "-2147483647\n".repeat(64);
            assertJitScalarAndVectorMatchJava("select i from n where -i < 2147483649", rows);
            assertJitScalarAndVectorMatchJava("select i from n where -i < 2147483649 or b", rows);
            // the IN spelling already agreed; kept so the pair cannot drift apart again
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select i from n where -i in (2147483649)", "i\n");
        });
    }

    @Test
    public void testUuidConstantComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_uuid4() uuid1, " +
                " rnd_uuid4() uuid2 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "'33333333-3333-3333-3333-333333333333'")
                .withBooleanOperator()
                .withOptionalNot()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "'33333333-3333-3333-3333-333333333333'");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testUuidConstantIntMixedComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_int() int, " +
                " rnd_uuid4() uuid " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("int")
                .withEqualityOperator()
                .withAnyOf("3", "-1", "null")
                .withBooleanOperator()
                .withOptionalNot()
                .withAnyOf("uuid")
                .withEqualityOperator()
                .withAnyOf("'22222222-2222-2222-2222-222222222222'", "null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testUuidNullComparison() throws Exception {
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_uuid4() uuid1, " +
                " rnd_uuid4() uuid2 " +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        FilterGenerator gen = new FilterGenerator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("null")
                .withBooleanOperator()
                .withAnyOf("uuid1", "uuid2")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testUuidOrderingFallsBackToJavaFilter() throws Exception {
        // https://github.com/questdb/questdb/issues/7546
        // An i128 lane reached the backends' ordering comparators, whose
        // data_type_t switch ends in `default: __builtin_unreachable()`, and the
        // resulting jump-table overrun killed the JVM with SIGSEGV at
        // filter-compile time - no data needed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (uu UUID, u2 UUID, k TIMESTAMP) TIMESTAMP(k)");

            // Empty table: the crash used to happen before a single row was read.
            assertJitMatchesJava("SELECT count() FROM x WHERE uu > u2", false);

            execute(
                    "INSERT INTO x VALUES " +
                            "('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 0), " +
                            "('ffffffff-ffff-ffff-ffff-ffffffffffff', '22222222-2222-2222-2222-222222222222', 1), " +
                            "(null, '22222222-2222-2222-2222-222222222222', 2), " +
                            "('11111111-1111-1111-1111-111111111111', null, 3), " +
                            "(null, null, 4)"
            );

            assertJitMatchesJava("x WHERE uu > u2", false);
            assertJitMatchesJava("x WHERE uu >= u2", false);
            assertJitMatchesJava("x WHERE uu < u2", false);
            assertJitMatchesJava("x WHERE uu <= u2", false);
            assertJitMatchesJava("x WHERE uu > '22222222-2222-2222-2222-222222222222'", false);

            // Equality has a real i128 comparator in both backends, so it keeps compiling.
            assertJitMatchesJava("x WHERE uu = u2", true);
            assertJitMatchesJava("x WHERE uu != u2", true);
            assertJitMatchesJava("x WHERE uu = '11111111-1111-1111-1111-111111111111'", true);
        });
    }

    @Test
    public void testUuidSameConstantAndChain() throws Exception {
        final String query = "x " +
                "where uuid1 != '11111111-1111-1111-1111-111111111111'" +
                "  and uuid2 != '11111111-1111-1111-1111-111111111111'" +
                "  and uuid3 != '11111111-1111-1111-1111-111111111111'";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_uuid4() uuid1," +
                " rnd_uuid4() uuid2," +
                " rnd_uuid4() uuid3" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertQueryNotNull(query, ddl);
    }

    @Test
    public void testVarSizeNullComparison() throws Exception {
        final String ddl = "create table x as (select" +
                " x," +
                " timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_str(2, 1, 5, 3) string_value," +
                " rnd_varchar(1, 5, 3) varchar_value," +
                " rnd_bin(1, 32, 3) binary_value" +
                " from long_sequence(1000)) timestamp(k)";
        final FilterGenerator gen = new FilterGenerator()
                .withAnyOf("string_value", "varchar_value", "binary_value")
                .withEqualityOperator()
                .withAnyOf("null")
                .withBooleanOperator()
                .withAnyOf("string_value", "varchar_value", "binary_value")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testVarcharNullComparison() throws Exception {
        final String ddl = "create table x as (select" +
                " x," +
                " timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " rnd_varchar(1, 5, 3) varchar_value" +
                " from long_sequence(1000)) timestamp(k)";
        final FilterGenerator gen = new FilterGenerator()
                .withAnyOf("varchar_value")
                .withEqualityOperator()
                .withAnyOf("null");
        assertGeneratedQueryNullable(ddl, gen);
    }

    @Test
    public void testIntColumnVsFloatColumnPinsBoundaryRows() throws Exception {
        // Parity across three modes proves the modes agree; it cannot prove the shared answer is
        // right, and here both JIT backends shared the SAME wrong answer. These pin the absolute
        // rows against the Java filter, which is the specification: it has no (FLOAT, FLOAT)
        // comparison factory, so `i <op> f` compares at f64 - the INT through
        // IntFunction#getDouble, the FLOAT through FloatFunction#getDouble.
        //
        // The first three rows are the ones the review measured. (float) -2_147_483_647 and
        // (float) -2147483647.5 are both -2147483648.0f, and (float) 16777217 is 16777216.0f,
        // so an f32 comparison calls those pairs equal where f64 does not. Rows 3 to 5 and 10 carry
        // the NULL sentinels: INT NULL is Integer.MIN_VALUE, FLOAT NULL is NaN, and both reach
        // the comparison as NaN, so NULL = NULL matches and every ordering against a NULL does
        // not. Row 10 pairs an INT NULL against the float -2147483648.0, which is what
        // Integer.MIN_VALUE would look like had the sentinel been carried through as a value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1p (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1p VALUES
                        (0, -2_147_483_647, -2147483647.5),
                        (1_000_000, 16_777_217, 16777216.0),
                        (2_000_000, 20_000_000, 20000000.5),
                        (3_000_000, NULL, 1.0),
                        (4_000_000, 5, NULL),
                        (5_000_000, NULL, NULL),
                        (6_000_000, 1, 2.0),
                        (7_000_000, 3, 2.0),
                        (8_000_000, 2_147_483_647, 2.147483647E9),
                        (9_000_000, -2_147_483_646, -2147483646.0),
                        (10_000_000, NULL, -2147483648.0)
                    """);
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i = f",
                    "k\ti\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i <> f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:03.000000Z\tnull\n" +
                            "1970-01-01T00:00:04.000000Z\t5\n" +
                            "1970-01-01T00:00:06.000000Z\t1\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:08.000000Z\t2147483647\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n" +
                            "1970-01-01T00:00:10.000000Z\tnull\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i < f",
                    "k\ti\n" +
                            "1970-01-01T00:00:06.000000Z\t1\n" +
                            "1970-01-01T00:00:08.000000Z\t2147483647\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i <= f",
                    "k\ti\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n" +
                            "1970-01-01T00:00:06.000000Z\t1\n" +
                            "1970-01-01T00:00:08.000000Z\t2147483647\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i >= f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
            // The reversed spelling: the review measured `f >= i` wrong too.
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where f >= i",
                    "k\ti\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n" +
                            "1970-01-01T00:00:06.000000Z\t1\n" +
                            "1970-01-01T00:00:08.000000Z\t2147483647\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where f > i",
                    "k\ti\n" +
                            "1970-01-01T00:00:06.000000Z\t1\n" +
                            "1970-01-01T00:00:08.000000Z\t2147483647\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where f < i",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where f <= i",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
            // The IN spelling of the same pairing.
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where f in (i)",
                    "k\ti\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:05.000000Z\tnull\n"
            );
            // A conjunct beside the pairing must not lose rows either, in either order.
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where i > f and k > 0",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1p where k > 0 and i > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:07.000000Z\t3\n" +
                            "1970-01-01T00:00:09.000000Z\t-2147483646\n"
            );
        });
    }

    @Test
    public void testMultiElementInIntElementVsFloatKeyPinsBoundaryRows() throws Exception {
        // `f IN (i, 5)` is the only spelling that reaches the ARGS-LOOP call site of
        // markIntCmpFloatOperand. A single-element `f IN (i)` keeps its key and its element in
        // lhs / rhs and takes the binary-comparison site instead, and an all-literal list carries
        // no INT operand to mark, so neither of those covers the loop. InDoubleFunctionFactory
        // ("in(DV)") admits an INT column as an element; the reverse spelling `i IN (f, 5)` is not
        // expressible at all, because "in(LV)" wins the signature for an INT key and rejects a
        // FLOAT element outright - the assertion below pins that.
        //
        // The Java filter is the specification: InDoubleVarFunction reads the key through
        // FloatFunction#getDouble and the INT element through IntFunction#getDouble, so the list
        // compares at f64. Unmarked, the backend pairs an i32 element against an f32 key and
        // compares at f32, which calls equal four pairs that f64 keeps apart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1n (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1n VALUES
                        (0, 16_777_217, 16777216.0),
                        (1_000_000, -16_777_217, -16777216.0),
                        (2_000_000, 20_000_001, 20000000.0),
                        (3_000_000, 2_147_483_647, 2147483648.0),
                        (4_000_000, 16_777_215, 16777215.0),
                        (5_000_000, 16_777_216, 16777216.0),
                        (6_000_000, -16_777_216, -16777216.0),
                        (7_000_000, 5, 5.0),
                        (8_000_000, 99, 5.0),
                        (9_000_000, NULL, NULL),
                        (10_000_000, NULL, 1.0),
                        (11_000_000, 1, NULL)
                    """);
            // The fixture trap: `CASE ... END::float` yields a DOUBLE column, and a DOUBLE key
            // compares at f64 in BOTH engines, so a fixture built that way passes whatever the
            // marking does. Pin the declared types, so a later fixture edit fails loudly instead
            // of hollowing the row assertions out.
            assertQuery("SELECT typeOf(i) it, typeOf(f) ft FROM a1n LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("it\tft\nINT\tFLOAT\n");

            // Rows 0 to 3 are the discriminators: the float of the INT lands exactly on the FLOAT
            // column value, while the two doubles differ by at least 1.
            //   (float) 16_777_217    is 16777216.0f   (ulp 2 above 2^24, the tie rounds to even)
            //   (float) -16_777_217   is -16777216.0f
            //   (float) 20_000_001    is 20000000.0f
            //   (float) 2_147_483_647 is 2147483648.0f (ulp 128 below 2^31)
            // They must be ABSENT from IN and PRESENT in NOT IN. Rows 4 to 6 are the exact
            // controls that stay in IN at either width, row 7 matches through both elements, row 8
            // matches through the LITERAL element only, and rows 9 to 11 carry the NULLs: an INT
            // NULL and a FLOAT NULL both reach the comparison as NaN, so NULL against NULL matches
            // and NULL against a value does not.
            assertA1nRows("f in (i, 5)", 4, 5, 6, 7, 8, 9);
            assertA1nRows("f not in (i, 5)", 0, 1, 2, 3, 10, 11);
            // Element order must not matter - the loop marks every element against the key.
            assertA1nRows("f in (5, i)", 4, 5, 6, 7, 8, 9);
            assertA1nRows("f not in (5, i)", 0, 1, 2, 3, 10, 11);
            // A conjunct beside the pairing must not move the rows either.
            assertA1nRows("f in (i, 5) and k > 0", 4, 5, 6, 7, 8, 9);
            // Controls for the BINARY call site, which was already covered: dropping the literal
            // drops row 8, and the equality spelling of the same pairing agrees with what is left.
            assertA1nRows("f in (i)", 4, 5, 6, 7, 9);
            assertA1nRows("f = i", 4, 5, 6, 7, 9);

            // The same shape over 12 * 64 = 768 rows, the width at which the review measured the
            // defect: the f32 list returned 640 rows where the Java filter returns 384. Counts
            // rather than rows here, so a page frame wide enough to run many loop iterations still
            // reads as one assertion.
            execute("CREATE TABLE a1nw AS (SELECT i, f FROM a1n CROSS JOIN long_sequence(64))");
            assertQuery("SELECT typeOf(i) it, typeOf(f) ft FROM a1nw LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("it\tft\nINT\tFLOAT\n");
            assertJitScalarAndVectorMatchJava("select count() from a1nw where f in (i, 5)", "count\n384\n");
            assertJitScalarAndVectorMatchJava("select count() from a1nw where f not in (i, 5)", "count\n384\n");

            // The reverse operand order has no args-loop site to reach, because it does not
            // compile: InLongFunctionFactory ("in(LV)") wins the signature for an INT key.
            assertQuery("SELECT k FROM a1n WHERE i IN (f, 5)")
                    .noLeakCheck()
                    .fails(30, "cannot compare LONG with type FLOAT");
        });
    }

    @Test
    public void testIntColumnVsFloatColumnMatchesJava() throws Exception {
        // Row-for-row parity across JIT-off / FORCE_SCALAR / JIT-on over a full SIMD body plus a
        // scalar tail, so a wrong lane in either backend shows up. The INT column straddles the
        // f32 exact-integer limit (16777215 / 16777216 / 16777217) and reaches INT_MAX, and every
        // column carries a NULL row.
        final String ddl = "create table a1 as " +
                "(select timestamp_sequence(400_000_000_000, 500_000_000) as k," +
                " case when x = 1 then cast(null as int) else cast(16_777_213 + x * 7 as int) end i," +
                " case when x = 2 then cast(null as float) else cast(16_777_213 + x * 7 + (x % 3 - 1) as float) end f," +
                " case when x = 3 then cast(null as int) else cast(x % 100 as int) end j," +
                " case when x = 4 then cast(null as float) else cast(x % 100 + (x % 3 - 1) as float) end g," +
                " case when x = 5 then cast(null as byte) else cast(x % 100 as byte) end b," +
                " case when x = 6 then cast(null as short) else cast(x % 100 as short) end s," +
                " case when x = 7 then cast(null as long) else cast(16_777_213 + x * 7 as long) end l," +
                " case when x = 8 then cast(null as double) else cast(16_777_213 + x * 7 + (x % 3 - 1) as double) end d" +
                " from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp(k)";
        assertMemoryLeak(() -> {
            execute(ddl);
            final String[] ops = {"=", "<>", "<", "<=", ">", ">="};
            for (int i = 0; i < ops.length; i++) {
                final String op = ops[i];
                assertQueryNotNullNoLeakCheck("a1 where i " + op + " f");
                assertQueryNotNullNoLeakCheck("a1 where f " + op + " i");
                assertQueryNotNullNoLeakCheck("a1 where j " + op + " g");
                assertQueryNotNullNoLeakCheck("a1 where i " + op + " f * 1");
                assertQueryNotNullNoLeakCheck("a1 where not (i " + op + " f)");
                assertQueryNotNullNoLeakCheck("a1 where i " + op + " f and l > 0");
                assertQueryNotNullNoLeakCheck("a1 where l > 0 and i " + op + " f");
                // Controls: the pairings that were already exact must keep their rows.
                assertQueryNotNullNoLeakCheck("a1 where b " + op + " g");
                assertQueryNotNullNoLeakCheck("a1 where s " + op + " g");
                assertQueryNotNullNoLeakCheck("a1 where l " + op + " f");
                assertQueryNotNullNoLeakCheck("a1 where i " + op + " d");
            }
        });
    }

    @Test
    public void testIntColumnVsFloatColumnMatchesJavaOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1w (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO a1w VALUES
                        (0, -2_147_483_647, -2147483647.5),
                        (1_000_000, 16_777_217, 16777216.0),
                        (2_000_000, 20_000_000, 20000000.5),
                        (3_000_000, NULL, NULL)
                    """);
            drainWalQueue();
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1w where i > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t-2147483647\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n"
            );
            assertJitScalarAndVectorMatchJava(
                    "select k, i from a1w where f >= i",
                    "k\ti\n" +
                            "1970-01-01T00:00:02.000000Z\t20000000\n" +
                            "1970-01-01T00:00:03.000000Z\tnull\n"
            );
        });
    }

    @Test
    public void testNarrowIntArithVsFloatColumnWidensResultAndMatchesJava() throws Exception {
        // An INT-width arithmetic operand against a FLOAT one wraps at 32 bits in the Java filter
        // (MulInt/AddInt/NegInt#getInt) and is only then read at f64 through
        // IntFunction#getDouble. The serializer reproduces exactly that: the subtree keeps
        // computing at i32 and an SX_I64 emitted AFTER the operator widens the WRAPPED result, so
        // convert() takes the (i64, f32) arm. Every assertion below pins the absolute rows the
        // Java filter returns AND that the JIT claimed the filter, under FORCE_SCALAR and the
        // vectorized mode alike.
        //
        // Rows 4 and 5 are the discriminators between widening the RESULT and widening the
        // OPERANDS. 1073741824 * 2 wraps onto INT_MIN, which is the INT NULL sentinel, so the
        // Java filter answers NaN and the row drops; 2000000000 * 2 wraps to -294967296, which is
        // below the bound. A long-width multiply would return 2147483648 and 4000000000 and keep
        // both rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1a (k TIMESTAMP, i INT, f FLOAT, b BYTE) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1a VALUES
                        (0, 16_777_216, 16777216.0, 1),
                        (1_000_000, 16_777_217, 16777216.0, 2),
                        (2_000_000, 5, 6.0, 3),
                        (3_000_000, NULL, NULL, 0),
                        (4_000_000, 1_073_741_824, 1.0, 4),
                        (5_000_000, 2_000_000_000, 1.0, 5),
                        (6_000_000, 0, 2.0E9, 7)
                    """);
            assertJitScalarAndVectorMatchJava("select k, i from a1a where i + 1 > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t16777216\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:04.000000Z\t1073741824\n" +
                            "1970-01-01T00:00:05.000000Z\t2000000000\n");
            assertJitScalarAndVectorMatchJava("select k, i from a1a where i * 2 > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t16777216\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n");
            // The reversed operand order has to widen the same subtree.
            assertJitScalarAndVectorMatchJava("select k, i from a1a where f < i * 2",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t16777216\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n");
            assertJitScalarAndVectorMatchJava("select k, i from a1a where -i < f",
                    "k\ti\n" +
                            "1970-01-01T00:00:00.000000Z\t16777216\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n" +
                            "1970-01-01T00:00:04.000000Z\t1073741824\n" +
                            "1970-01-01T00:00:05.000000Z\t2000000000\n" +
                            "1970-01-01T00:00:06.000000Z\t0\n");
            assertJitScalarAndVectorMatchJava("select k, i from a1a where i / 2 > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:04.000000Z\t1073741824\n" +
                            "1970-01-01T00:00:05.000000Z\t2000000000\n");
            // A BYTE leaf under an INT-width add: the sum is far above 2^24, so the row whose
            // FLOAT column sits on the rounded bound is the one an f32 comparison dropped.
            // 2000000007 rounds to 2.0E9f, which is exactly the column value in row 6.
            assertJitScalarAndVectorMatchJava("select k, b from a1a where b + 2_000_000_000 > f",
                    "k\tb\n" +
                            "1970-01-01T00:00:00.000000Z\t1\n" +
                            "1970-01-01T00:00:01.000000Z\t2\n" +
                            "1970-01-01T00:00:02.000000Z\t3\n" +
                            "1970-01-01T00:00:04.000000Z\t4\n" +
                            "1970-01-01T00:00:05.000000Z\t5\n" +
                            "1970-01-01T00:00:06.000000Z\t7\n");
            // The bare column, which round one already fixed, still holds.
            assertJitScalarAndVectorMatchJava("select k, i from a1a where i > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:04.000000Z\t1073741824\n" +
                            "1970-01-01T00:00:05.000000Z\t2000000000\n");
            // A conjunct beside the widened subtree must keep its rows, in either order.
            assertJitScalarAndVectorMatchJava("select k, i from a1a where i * 2 > f and k > 0",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n");
            assertJitScalarAndVectorMatchJava("select k, i from a1a where k > 0 and i * 2 > f",
                    "k\ti\n" +
                            "1970-01-01T00:00:01.000000Z\t16777217\n" +
                            "1970-01-01T00:00:02.000000Z\t5\n");
        });
    }

    @Test
    public void testDeclinedConstantFoldVsFloatColumnMatchesJava() throws Exception {
        // The two ways markFoldedI64ConstArith declines a pure-constant INT subtree sitting against
        // a FLOAT operand. Both collapse to the INT NULL sentinel in the Java filter - DivInt#getInt
        // answers INT_NULL for a zero divisor, and 1_073_741_824 * 2 wraps onto INT_MIN - and
        // Numbers.intToDouble(INT_NULL) then makes the comparison read NaN.
        //
        // The serializer treats them differently and this pins the rows for both. The sentinel
        // shape folds to one I4 immediate and rides the vectorized loop, where cvt_itof's null
        // check turns INT_NULL into a float NaN. The zero-divisor shape keeps its per-operation IR,
        // takes an SX_I64 over the division's result and rides the scalar loop, where int32_div
        // answers INT_NULL, int32_to_int64 carries it to LONG_NULL and int64_to_double reads that
        // as NaN. Different lowerings, same answer - which is what makes the frontend free to
        // choose either, and what a relaxation of the scalar force would have to keep true.
        //
        // NULL semantics, from Numbers.equals(double, double) and the negated ordering factories:
        // NaN orders against nothing, so the ordering operators drop every row; NaN equals NaN, so
        // '=' keeps exactly the rows whose FLOAT is NULL and '<>' keeps exactly the rest.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1d (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1d VALUES
                        (0, 1, 1.0),
                        (1_000_000, 2, NULL),
                        (2_000_000, NULL, -1.5),
                        (3_000_000, 7, 16777216.0)
                    """);
            final String noRows = "k\tf\n";
            final String nullFloatRow = "k\tf\n" +
                    "1970-01-01T00:00:01.000000Z\tnull\n";
            final String valueFloatRows = "k\tf\n" +
                    "1970-01-01T00:00:00.000000Z\t1.0\n" +
                    "1970-01-01T00:00:02.000000Z\t-1.5\n" +
                    "1970-01-01T00:00:03.000000Z\t1.6777216E7\n";
            // Decline reason one: a divisor that is zero at LONG width. tryFoldConstantArith throws
            // for it exactly as it throws for a column, so the subtree joins i64WidenArithRoots and
            // the whole filter drops to the scalar backend.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, f from a1d where f > 10 / 0", noRows);
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, f from a1d where f < 10 / 0", noRows);
            // '>=' and '<=' are the negated strict orderings, so NULL against NULL matches them.
            assertJitScalarAndVectorMatchJava("select k, f from a1d where 10 / 0 >= f", nullFloatRow);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f = 10 / 0", nullFloatRow);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f <> 10 / 0", valueFloatRows);
            // The divisor folds to zero rather than being spelled as one.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, f from a1d where f > 10 / (3 - 3)", noRows);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f <> 10 / (3 - 3)", valueFloatRows);
            // Decline reason two: an INT-width fold that lands ON the sentinel. This one keeps the
            // vectorized loop, so the pair of assertions above and below cross-check the two
            // backends against the same Java answer.
            assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, f from a1d where f > 1_073_741_824 * 2", noRows);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f <= 1_073_741_824 * 2", nullFloatRow);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f = 1_073_741_824 * 2", nullFloatRow);
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f <> 1_073_741_824 * 2", valueFloatRows);
            // A divisor that is non-zero at LONG width but wraps to zero at INT width takes the
            // sentinel exit too - tryFoldConstantArith accepts it, only the width-aware fold does
            // not - so it must answer the same NULL.
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f <> 7 / (65_536 * 65_536)", valueFloatRows);
            // The scalar force is filter-wide, so an OR peer with a real row set rides it too. Its
            // rows must not change with the loop the filter ends up on.
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f > 10 / 0 or i > 1",
                    "k\tf\n" +
                            "1970-01-01T00:00:01.000000Z\tnull\n" +
                            "1970-01-01T00:00:03.000000Z\t1.6777216E7\n");
            // The IN spelling routes through the same marker.
            assertJitScalarAndVectorMatchJava("select k, f from a1d where f in (10 / 0, -1.5)",
                    "k\tf\n" +
                            "1970-01-01T00:00:01.000000Z\tnull\n" +
                            "1970-01-01T00:00:02.000000Z\t-1.5\n");
        });
    }

    @Test
    public void testNarrowIntArithVsFloatColumnCoversEveryOperator() throws Exception {
        // All six operators in both operand orders, for the two arithmetic shapes that carry the
        // whole population's hazards: a multiply whose product wraps (row 4 onto the INT NULL
        // sentinel, row 5 past it) and a unary minus. Absolute rows, asserted against the
        // JIT-disabled cursor first, so parity between the two JIT backends cannot launder a
        // shared wrong answer.
        //
        // NULL semantics, from Numbers.equals(double, double) and the negated ordering factories:
        // NULL against NULL is equal - so it matches =, <= and >= - and NULL against a value
        // matches only <>. Numbers.intToDouble(INT_NULL) is NaN, which is how an INT NULL and a
        // wrap that LANDS on INT_MIN both reach the comparison.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1o (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1o VALUES
                        (0, 16_777_216, 16777216.0),
                        (1_000_000, 16_777_217, 16777216.0),
                        (2_000_000, 5, 6.0),
                        (3_000_000, NULL, NULL),
                        (4_000_000, 1_073_741_824, 1.0),
                        (5_000_000, 2_000_000_000, 1.0),
                        (6_000_000, 0, 2.0E9),
                        (7_000_000, 16_777_217, 33554432.0),
                        (8_000_000, 16_777_217, -16777216.0)
                    """);
            // Rows 7 and 8 are the f32-rounding discriminators. 16777217 * 2 is 33554434, whose
            // nearest float is 33554432 (ulp is 4 above 2^25, and the tie rounds to the even
            // mantissa), and -16777217's nearest float is -16777216 (ulp is 2 above 2^24). Both
            // land exactly on the FLOAT column value, so an f32 comparison calls the pair equal
            // where the Java filter's f64 one does not.
            //
            // i * 2 per row, at INT width: 33554432, 33554434, 10, NULL, INT_MIN (the wrap lands
            // on the sentinel), -294967296 (4000000000 wrapped), 0, 33554434, 33554434.
            assertA1oRows("i * 2 = f", 3);
            assertA1oRows("i * 2 <> f", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oRows("i * 2 < f", 5, 6);
            assertA1oRows("i * 2 <= f", 3, 5, 6);
            assertA1oRows("i * 2 > f", 0, 1, 2, 7, 8);
            assertA1oRows("i * 2 >= f", 0, 1, 2, 3, 7, 8);
            assertA1oRows("f = i * 2", 3);
            assertA1oRows("f <> i * 2", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oRows("f < i * 2", 0, 1, 2, 7, 8);
            assertA1oRows("f <= i * 2", 0, 1, 2, 3, 7, 8);
            assertA1oRows("f > i * 2", 5, 6);
            assertA1oRows("f >= i * 2", 3, 5, 6);
            // -i per row: -16777216, -16777217, -5, NULL, -1073741824, -2000000000, 0, -16777217,
            // -16777217. NegInt maps the sentinel to itself, so no value wraps.
            assertA1oRows("-i = f", 3);
            assertA1oRows("-i <> f", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oRows("-i < f", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oRows("-i <= f", 0, 1, 2, 3, 4, 5, 6, 7, 8);
            assertA1oNoRows("-i > f");
            assertA1oRows("-i >= f", 3);
            assertA1oRows("f = -i", 3);
            assertA1oRows("f <> -i", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oNoRows("f < -i");
            assertA1oRows("f <= -i", 3);
            assertA1oRows("f > -i", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA1oRows("f >= -i", 0, 1, 2, 3, 4, 5, 6, 7, 8);
        });
    }

    @Test
    public void testNarrowIntArithMagnitudeBoundVsFloatColumnPinsBoundaryRows() throws Exception {
        // intCmpFloatMagnitudeBound decides whether a narrow arithmetic operand may KEEP its f32
        // pairing against a FLOAT one. Reporting a bound that is too large only costs an
        // unnecessary SX_I64. Reporting one that is too SMALL drops a widening the pairing needed:
        // the INT side then rounds through cvt_itof / int32_to_float, and every product above 2^24
        // whose value is odd compares as its even neighbour. These are absolute rows, taken from
        // the JIT-disabled cursor first, so the two JIT backends agreeing with each other cannot
        // launder a shared wrong answer.
        //
        // The discriminators are rows 0 and 1. A BYTE leaf reaches 127 and a SHORT leaf 32_767 in
        // ODD magnitude, and an odd product above 2^24 has no exact float - the spacing there is
        // 2, and the tie rounds to the even mantissa:
        //   127    * 262_143 is 33_292_161, whose nearest float is 3.329216E7
        //   32_767 * 1023    is 33_520_641, whose nearest float is 3.352064E7
        // Both land exactly on the FLOAT column of their row, so an f32 comparison calls the pair
        // equal where the Java filter's f64 one does not. The bound keeps them apart: 128 * 262_143
        // and 32_768 * 1023 both exceed 2^24, so both subtrees widen.
        //
        // BYTE and SHORT have no NULL sentinel - every bit pattern is a value - so the NULL in the
        // INSERT below stores a plain 0, which row 10 pins. The FLOAT NULL in row 9 is a NaN, and
        // Numbers.equals(double, double) makes NaN equal to NaN, so an INT NULL sentinel reaching
        // the comparison as NaN matches it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a1q (k TIMESTAMP, b BYTE, s SHORT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a1q VALUES
                        (0, 127, 32_767, 3.329216E7),
                        (1_000_000, 127, 32_767, 3.352064E7),
                        (2_000_000, -128, -32_768, -3.3554304E7),
                        (3_000_000, -128, -32_768, -3.3521664E7),
                        (4_000_000, -128, -32_768, -1.6777216E7),
                        (5_000_000, 127, 32_767, 1.6646144E7),
                        (6_000_000, 127, 32_767, 1.6776704E7),
                        (7_000_000, 63, 32_000, 32.0),
                        (8_000_000, 1, 1, 1.0),
                        (9_000_000, 0, 0, NULL),
                        (10_000_000, NULL, NULL, 0.0),
                        (11_000_000, 5, 5, 5.0)
                    """);
            // The fixture trap H3 documented: a column that is not really BYTE / SHORT / FLOAT
            // compares at f64 in BOTH engines and would pass whatever the bound reports. Pin the
            // declared types so a later fixture edit fails loudly instead of hollowing the row
            // assertions out.
            assertQuery("SELECT typeOf(b) bt, typeOf(s) st, typeOf(f) ft FROM a1q LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("bt\tst\tft\nBYTE\tSHORT\tFLOAT\n");

            // The I1 leaf bound. 128 * 262_143 is 33_554_304, past 2^24, so the subtree widens and
            // row 0 stays OUT of the equality and IN the inequality. A leaf bound of 64 would put
            // the product at 16_777_152, inside 2^24, drop the SX_I64 and pull row 0 in.
            assertA1qRows("b * 262_143 = f", 2, 10);
            assertA1qRows("b * 262_143 <> f", 0, 1, 3, 4, 5, 6, 7, 8, 9, 11);
            assertA1qRows("b * 262_143 > f", 0, 5, 6, 7, 8, 11);
            // The reversed operand order has to widen the same subtree.
            assertA1qRows("f < b * 262_143", 0, 5, 6, 7, 8, 11);
            // The I2 leaf bound, the same way: 32_768 * 1023 is 33_521_664, past 2^24, so row 1
            // stays out. This pairing also pins the consumer's boundary from above - a test that
            // accepted anything up to 2 * 2^24 would keep the SX_I64 off and pull row 1 in.
            assertA1qRows("s * 1023 = f", 3, 10);
            assertA1qRows("s * 1023 > f", 0, 1, 2, 5, 6, 7, 8, 11);
            assertA1qRows("f <= s * 1023", 0, 1, 2, 3, 5, 6, 7, 8, 10, 11);
            // The other direction of the same decision: a subtree bounded EXACTLY by 2^24 keeps
            // its f32 pairing, and these rows prove that keeping it is right. 128 * 131_072 and
            // 32_768 * 512 are both 2^24 on the nose, and no value either subtree can take - the
            // extremes are -16_777_216 and 16_646_144, and -16_777_216 and 16_776_704 - misses an
            // exact float.
            assertA1qRows("b * 131_072 = f", 4, 5, 10);
            assertA1qRows("b * 131_072 > f", 2, 3, 7, 8, 11);
            assertA1qRows("s * 512 = f", 4, 6, 10);
            // The recursion's two early-outs sit on the same boundary. An operand bounded exactly
            // by 2^24 still combines with its sibling, so these keep the f32 pairing too.
            assertA1qRows("b * 131_072 + 0 = f", 4, 5, 10);
            assertA1qRows("0 + b * 131_072 = f", 4, 5, 10);
            // The bound is RECURSIVE, and a leaf bound one step too small breaks it one step
            // from the constant: a constant applied AFTER the product makes the extreme value
            // carry the -128 magnitude AND an odd offset. At b = -128, b * 131_072 - 1 is
            // -16_777_217 - odd, past 2^24, so it has no exact float, and it rounds ties-to-even
            // onto -1.6777216E7, which is exactly row 4's f. The true bound
            // 128 * 131_072 + 1 = 16_777_217 exceeds 2^24, so the subtree widens and row 4 stays
            // out of the equality. A leaf bound of 127 bounds the same subtree by 16_646_145,
            // keeps the f32 pairing, and pulls row 4 in - wrong rows, not merely a lost SX_I64.
            assertA1qNoRows("b * 131_072 - 1 = f");
            assertA1qRows("b * 131_072 - 1 <> f", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

            // The division arm. A non-zero integer constant divisor carries the numerator's own
            // bound through, so these keep the f32 pairing - and these rows are what makes that
            // safe: integer division never grows the magnitude.
            assertA1qRows("b / 2 = f", 10);
            assertA1qRows("s / 1_000 = f", 7, 10);
            assertA1qRows("s / 1_000 > f", 2, 3, 4);
            // constantMagnitudeBound unwraps a unary minus, so a negative divisor bounds the
            // quotient by its magnitude.
            assertA1qRows("b / -1 = f", 10);
            // A numerator bounded exactly by 2^24 passes the lhs early-out and then takes the
            // division arm, so the two boundary tests compose without either loosening.
            assertA1qRows("b * 131_072 / 1 = f", 4, 5, 10);
            // A pure-constant numerator is the one division shape with no narrow-int COLUMN in the
            // predicate, so it is also the one that keeps a vectorized loop rather than merely
            // saving an instruction: TypesObserver#hasNarrowInt forces every BYTE / SHORT
            // arithmetic filter above onto the scalar backend whatever the bound reports.
            assertA1qRows("f = 10 / 2", 11);

            // Controls - the two ways the divisor check declines, both of which widen. DivInt#
            // getInt answers INT_NULL for a zero divisor, Numbers.intToDouble turns that into NaN
            // on the Java side, and the SX_I64 carries it to LONG_NULL and then to the same NaN in
            // the backend. NaN orders against nothing and equals only NaN, so the FLOAT NULL row
            // is the only match.
            assertA1qRows("b / 0 = f", 9);
            assertA1qRows("b / 0 <> f", 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11);
            // A divisor that is not a constant declines for the same reason - it can be zero at
            // runtime - and row 9 divides zero by zero to prove the sentinel still arrives.
            assertA1qRows("b / s = f", 8, 9);
            assertA1qRows("b / s <> f", 0, 1, 2, 3, 4, 5, 6, 7, 10, 11);
        });
    }

    @Test
    public void testDoubleConstantInFourByteArithmeticMatchesJava() throws Exception {
        // A DOUBLE-spelled literal makes its arithmetic node a DOUBLE one in the Java filter -
        // "+(FF)" cannot take a DOUBLE operand, so FLOAT + DOUBLE resolves to "+(DD)". The type
        // observer sees columns and bind variables only, so a predicate whose widest source is a
        // 4-byte column typed that literal down to F4 and both backends ran the whole subtree at
        // f32. The bound was rounded too, but the subtree width is the deeper half: "f + 1.0 > f"
        // carries no inexact constant at all and still diverged, because 16777216.0f + 1.0f is
        // 16777216.0f while (double) 16777216.0f + 1.0 is 16777217.0.
        //
        // Float literals are spelled without CLAUDE.md's thousands separators on purpose:
        // Numbers.parseDouble rejects an underscore, so a separated DOUBLE literal makes the
        // serializer decline the filter and the test would stop exercising the JIT at all.
        assertMemoryLeak(() -> {
            createA2fTable();
            // f + 1.0 per row, at f64: 16777217.0, 33554433.0, 7.0, NaN, 16777217.0, 2.0,
            // -16777215.0, 2.0, NaN. At f32 rows 0 and 4 collapse back onto 16777216.0f, and the
            // 16777216.5 bound rounds onto it as well, so the f32 filter answers the opposite way
            // for both the ordering and the equality operators.
            assertA2fNoRows("f + 1.0 = 16777216.5");
            assertA2fRows("f + 1.0 <> 16777216.5", 0, 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fRows("f + 1.0 < 16777216.5", 2, 5, 6, 7);
            assertA2fRows("f + 1.0 <= 16777216.5", 2, 5, 6, 7);
            assertA2fRows("f + 1.0 > 16777216.5", 0, 1, 4);
            assertA2fRows("f + 1.0 >= 16777216.5", 0, 1, 4);
            assertA2fNoRows("16777216.5 = f + 1.0");
            assertA2fRows("16777216.5 <> f + 1.0", 0, 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fRows("16777216.5 < f + 1.0", 0, 1, 4);
            assertA2fRows("16777216.5 <= f + 1.0", 0, 1, 4);
            assertA2fRows("16777216.5 > f + 1.0", 2, 5, 6, 7);
            assertA2fRows("16777216.5 >= f + 1.0", 2, 5, 6, 7);
            // No constant is compared here at all - the divergence is purely the width the ADD
            // runs at. NULL (NaN) compares equal to NULL and unordered against everything else.
            assertA2fRows("f + 1.0 = f", 3, 8);
            assertA2fRows("f + 1.0 <> f", 0, 1, 2, 4, 5, 6, 7);
            assertA2fNoRows("f + 1.0 < f");
            assertA2fRows("f + 1.0 <= f", 3, 8);
            assertA2fRows("f + 1.0 > f", 0, 1, 2, 4, 5, 6, 7);
            assertA2fRows("f + 1.0 >= f", 0, 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fRows("f = f + 1.0", 3, 8);
            assertA2fRows("f <> f + 1.0", 0, 1, 2, 4, 5, 6, 7);
            assertA2fRows("f < f + 1.0", 0, 1, 2, 4, 5, 6, 7);
            assertA2fRows("f <= f + 1.0", 0, 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fNoRows("f > f + 1.0");
            assertA2fRows("f >= f + 1.0", 3, 8);
            // The other three operators, a nested node, a negated literal and a unary minus over
            // the whole subtree - each has to keep computing at f64.
            assertA2fRows("f * 1.0 > 16777216.5", 1);
            assertA2fRows("f - 1.0 < 16777215.5", 0, 2, 4, 5, 6, 7);
            assertA2fNoRows("f / 1.0 = 16777216.5");
            assertA2fRows("f + 1.0 + 1.0 > 16777217.5", 0, 1, 4);
            assertA2fRows("f + -1.0 > 16777214.5", 0, 1, 4);
            assertA2fRows("-(f + 1.0) < -16777216.5", 0, 1, 4);
            // IN over a DOUBLE-width key is an OR of equalities and takes the same rule.
            assertA2fNoRows("f + 1.0 in (16777216.5)");
            assertA2fNoRows("f + 1.0 in (16777216.5, 2.5)");
            // NOT, and both conjunct orders beside a second predicate: the execution mode a
            // widened constant forces is filter-wide, so the answer must not depend on which
            // conjunct the traversal reaches first.
            assertA2fRows("not (f + 1.0 > 16777216.5)", 2, 5, 6, 7);
            assertA2fRows("f + 1.0 > 16777216.5 and i > 0", 0, 1, 4);
            assertA2fRows("i > 0 and f + 1.0 > 16777216.5", 0, 1, 4);
            assertA2fRows("f + 1.0 > 16777216.5 or i > 16_777_216", 0, 1, 4);
            assertA2fRows("i > 16_777_216 or f + 1.0 > 16777216.5", 0, 1, 4);
            // NULL against the subtree: FLOAT NULL is NaN, and so is the sum that reads it.
            assertA2fRows("f + 1.0 = null", 3, 8);
            assertA2fRows("f + 1.0 <> null", 0, 1, 2, 4, 5, 6, 7);
            // Controls, all three already correct before this fix and unchanged by it: an F4
            // subtree ("+(FF)" resolves, so the Java filter computes at f32 as well) and a bare
            // FLOAT column. Only the bound needs the double width, which it already had.
            assertA2fRows("f + 1.0f > 16777216.5", 1);
            assertA2fRows("f + 1 > 16777216.5", 1);
            assertA2fRows("f > 16777216.5", 1);
        });
    }

    @Test
    public void testDoubleConstantInIntArithmeticMatchesJava() throws Exception {
        // The same defect with no FLOAT column in the subtree: an INT column against a DOUBLE
        // literal is "+(DD)" / "/(DD)" in the Java filter, which reads the column through
        // IntFunction#getDouble. The JIT typed the literal at I4, serializeNumber's int parse
        // rejected it and fell through to a 32-bit float, and cvt_itof / int32_to_float then
        // rounded every INT above 2^24 into the f32 computation. Emitting the literal at F8 puts
        // the pairing on convert()'s int32_to_double arm, which is exact over the whole INT range
        // and maps INT_NULL to NaN exactly as Numbers.intToDouble does.
        assertMemoryLeak(() -> {
            createA2fTable();
            // i + 1.0 per row, at f64: 16777217.0, 16777218.0, 6.0, NaN, 16777218.0, 1.0,
            // -16777216.0, NaN, 2.0. At f32 rows 1 and 4 round to 16777216.0f + 1.0f =
            // 16777216.0f, which is what makes the equality answer differently.
            assertA2fRows("i + 1.0 = 16777217.0", 0);
            assertA2fRows("i + 1.0 <> 16777217.0", 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fRows("i + 1.0 < 16777217.0", 2, 5, 6, 8);
            assertA2fRows("i + 1.0 <= 16777217.0", 0, 2, 5, 6, 8);
            assertA2fRows("i + 1.0 > 16777217.0", 1, 4);
            assertA2fRows("i + 1.0 >= 16777217.0", 0, 1, 4);
            assertA2fRows("16777217.0 = i + 1.0", 0);
            assertA2fRows("16777217.0 <> i + 1.0", 1, 2, 3, 4, 5, 6, 7, 8);
            assertA2fRows("16777217.0 < i + 1.0", 1, 4);
            assertA2fRows("16777217.0 <= i + 1.0", 0, 1, 4);
            assertA2fRows("16777217.0 > i + 1.0", 2, 5, 6, 8);
            assertA2fRows("16777217.0 >= i + 1.0", 0, 2, 5, 6, 8);
            // Division is the sharpest case: at f32 the JIT divided a rounded INT, so
            // 16777217 / 2.0 came out 8388608.0 where the Java filter answers 8388608.5.
            assertA2fRows("i / 2.0 = f", 3);
            assertA2fRows("i / 2.0 <> f", 0, 1, 2, 4, 5, 6, 7, 8);
            assertA2fRows("i / 2.0 < f", 0, 1, 2, 4, 5);
            assertA2fRows("i / 2.0 <= f", 0, 1, 2, 3, 4, 5);
            assertA2fRows("i / 2.0 > f", 6);
            assertA2fRows("i / 2.0 >= f", 3, 6);
            assertA2fRows("i * 1.0 > 16777216.5", 1, 4);
            // An INT operand of the comparison, with the DOUBLE-width subtree on the other side:
            // the pairing reaches the backend as (i32, f64) and needs no sign extension, because
            // int32_to_double is exact over the whole INT range.
            assertA2fRows("i = f + 1.0", 3, 4);
            assertA2fRows("i <> f + 1.0", 0, 1, 2, 5, 6, 7, 8);
            assertA2fRows("i < f + 1.0", 0, 1, 2, 5, 6);
            assertA2fRows("i <= f + 1.0", 0, 1, 2, 3, 4, 5, 6);
            assertA2fNoRows("i > f + 1.0");
            assertA2fRows("i >= f + 1.0", 3, 4);
            assertA2fRows("f + 1.0 = i", 3, 4);
            assertA2fRows("f + 1.0 <> i", 0, 1, 2, 5, 6, 7, 8);
            assertA2fNoRows("f + 1.0 < i");
            assertA2fRows("f + 1.0 <= i", 3, 4);
            assertA2fRows("f + 1.0 > i", 0, 1, 2, 5, 6);
            assertA2fRows("f + 1.0 >= i", 0, 1, 2, 3, 4, 5, 6);
            // A wrapping INT product against a DOUBLE-width float subtree. The product still
            // wraps at i32 (int32_mul) and only its result promotes, so row 1's 16777217 * 2 =
            // 33554434 stays 33554434 at f64 where f32 rounds it onto the column's 33554432.0.
            assertA2fRows("i * 2 > f * 1.0", 0, 1, 2, 4);
            // INT NULL reaches the comparison as NaN through int32_to_double, exactly as
            // Numbers.intToDouble(INT_NULL) does on the Java side.
            assertA2fRows("i + 1.0 = null", 3, 7);
        });
    }

    @Test
    public void testDoubleConstArithBindVariableLeafPinsMode() throws Exception {
        // hasEightByteLeaf() answers for a BIND VARIABLE exactly as it does for a column, and no
        // SQL in the tree put a bind variable inside an arithmetic subtree that also carries a
        // DOUBLE literal, so that arm had never executed. Reaching it needs both: the DOUBLE
        // literal is what makes isNarrowLaneDoubleConstArith() ask the question, and the bind
        // variable is what the walk has to classify when it does.
        //
        // What the arm changes is the EXECUTION MODE, and that is what these pins hold. It decides
        // whether isNarrowLaneDoubleConstArith() claims the node, which moves the filter between
        // backends and leaves the node's own IR byte for byte the same, and every pair below does
        // return identical rows. That is a measurement over these inputs, not a proof: the two
        // backends implement the same IR separately, so a shape whose answer differs between them
        // would make the arm answer-changing too. No such input turned up here.
        //
        // The first two pairs are the ones that isolate the arm: both members read "l", so the
        // type observer sees a four-byte and an eight-byte source whichever bind variable is used
        // and hasMixedSizes() answers the same for both, which leaves the width of the leaf INSIDE
        // the arithmetic node as the only difference between them. Measured by deleting the
        // BIND_VARIABLE disjunct from hasEightByteLeaf(): both ":dv" legs move to WIDE_LANE and
        // every row set stays exactly where it is. The third pair drops "l" to cover operand
        // order, where the observer differs too and the isolation is weaker.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a4b (k TIMESTAMP, f FLOAT, l LONG) TIMESTAMP(k) PARTITION BY DAY");
            execute("""
                    INSERT INTO a4b VALUES
                        (0, 0.75, 10),
                        (1_000_000, 0.5, 3),
                        (2_000_000, NULL, 7),
                        (3_000_000, 2.0, NULL),
                        (4_000_000, 16777216.0, 9)
                    """);
            // The fixture trap: "CASE ... END::float" yields a DOUBLE column, and a DOUBLE leaf
            // inside the arithmetic node makes hasEightByteLeaf() answer true through the LITERAL
            // arm whatever the bind variable is, which would collapse both halves of every pair
            // onto the same mode. Pin the declared types so a later fixture edit fails loudly.
            assertQuery("SELECT typeOf(f) ft, typeOf(l) lt FROM a4b LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ft\tlt\nFLOAT\tLONG\n");

            bindVariableService.clear();
            bindVariableService.setFloat("fv", 0.25f);
            bindVariableService.setDouble("dv", 0.25);

            final String twoRows = """
                    k\tf\tl
                    1970-01-01T00:00:03.000000Z\t2.0\tnull
                    1970-01-01T00:00:04.000000Z\t1.6777216E7\t9
                    """;
            final String oneRow = """
                    k\tf\tl
                    1970-01-01T00:00:04.000000Z\t1.6777216E7\t9
                    """;

            // A FLOAT bind variable is a four-byte leaf, so hasEightByteLeaf() answers false, the
            // DOUBLE literal is the only eight-byte source in the node and the filter takes the
            // four-lane loop. A DOUBLE bind variable is an eight-byte leaf, the arm answers true,
            // the node is no longer a conversion source and the filter falls back on what the
            // observer reports, which is mixed sizes.
            assertExecHint("a4b", "(f + :fv) * 1.0 > 1.5 and l > 5", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4b", "(f + :dv) * 1.0 > 1.5 and l > 5", EXEC_HINT_MIXED_SIZE_TYPE);
            assertJitScalarAndVectorMatchJava(
                    "select k, f, l from a4b where (f + :fv) * 1.0 > 1.5 and l > 5", oneRow);
            assertJitScalarAndVectorMatchJava(
                    "select k, f, l from a4b where (f + :dv) * 1.0 > 1.5 and l > 5", oneRow);

            // The same pair with a sibling conjunct the four-lane loop cannot take. isWideLaneMode
            // is false either way here, so the DOUBLE literal's widened immediate has no eight-byte
            // lane to ride: with the FLOAT bind variable the node IS a conversion source, the
            // widening happens and getExecHint()'s hasPendingWidthChangingI64Constant gate drops
            // the filter to the scalar backend; with the DOUBLE bind variable there is no widening
            // to strand, and the filter keeps the mixed-size loop. Same arm, a different pair of
            // modes.
            assertExecHint("a4b", "(f + :fv) * 1.0 > 1.5 and l = null", EXEC_HINT_SCALAR);
            assertExecHint("a4b", "(f + :dv) * 1.0 > 1.5 and l = null", EXEC_HINT_MIXED_SIZE_TYPE);
            final String nullLongRow = """
                    k\tf\tl
                    1970-01-01T00:00:03.000000Z\t2.0\tnull
                    """;
            assertJitScalarAndVectorMatchJava(
                    "select k, f, l from a4b where (f + :fv) * 1.0 > 1.5 and l = null", nullLongRow);
            assertJitScalarAndVectorMatchJava(
                    "select k, f, l from a4b where (f + :dv) * 1.0 > 1.5 and l = null", nullLongRow);

            // Operand order inside the arithmetic node does not matter - the walk visits both
            // operands - and neither does dropping the eight-byte conjunct. Without "l" the
            // observer reports four bytes alone for the ":fv" spelling and mixed sizes for the
            // ":dv" one, so these four legs pin the modes without isolating their cause.
            assertExecHint("a4b", "(f + :fv) * 1.0 > 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4b", "(f + :dv) * 1.0 > 1.5", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("a4b", "(:fv + f) * 1.0 > 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4b", "(:dv + f) * 1.0 > 1.5", EXEC_HINT_MIXED_SIZE_TYPE);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where (f + :fv) * 1.0 > 1.5", twoRows);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where (f + :dv) * 1.0 > 1.5", twoRows);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where (:fv + f) * 1.0 > 1.5", twoRows);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where (:dv + f) * 1.0 > 1.5", twoRows);

            // Controls for the two neighbouring arms, so the pins above cannot be read as saying
            // something the bind variable alone decides. A LONG COLUMN in the same position takes
            // the LITERAL arm and lands where the DOUBLE bind variable does; removing the leaf
            // from inside the node altogether leaves the eight-byte column in a SIBLING conjunct,
            // which hasEightByteLeaf() deliberately does not see, and that filter is wide-lane.
            assertExecHint("a4b", "(f + l) * 1.0 > 1.5 and l > 5", EXEC_HINT_MIXED_SIZE_TYPE);
            assertExecHint("a4b", "f * 1.0 > 1.5 and l > 5", EXEC_HINT_WIDE_LANE);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where (f + l) * 1.0 > 1.5 and l > 5", """
                    k\tf\tl
                    1970-01-01T00:00:00.000000Z\t0.75\t10
                    1970-01-01T00:00:04.000000Z\t1.6777216E7\t9
                    """);
            assertJitScalarAndVectorMatchJava("select k, f, l from a4b where f * 1.0 > 1.5 and l > 5", oneRow);
        });
    }

    @Test
    public void testDoubleConstArithInListPinsRows() throws Exception {
        // The arithmetic-key IN spellings are pinned WIDE_LANE by
        // CompiledFilterIRSerializerTest#testDoubleConstantInFourByteArithmeticEmitsAtF8, and the
        // only row-level coverage two of them had - "f + 1.0 in (16777216.5)" and
        // "f + 1.0 in (16777216.5, 2.5)" over the a2f fixture - matches nothing and never could:
        // f + 1.0 = 16777216.5 needs f = 16777215.5, and the float ulp just below 2^24 is 1.0, so
        // no FLOAT column value spells it. An empty result is the same on all three engines
        // whatever the backend does with the list, so those two sites pin nothing about the
        // composition they name. "f * 2.0 in (1.5, 2.5)" had no row-level site at all.
        //
        // These elements are reachable. 8388607.5 sits in [2^22, 2^23) where the float ulp is 0.5,
        // so the column holds it exactly, and f + 1.0 is exactly 8388608.5 at f64 - while the same
        // sum at f32 lands in [2^23, 2^24) where the ulp is 1.0 and rounds to 8388608.0, missing
        // the element. Row 7 is therefore the row that changes its answer if the KEY ever stops
        // computing at f64, which is what the IN spelling of this shape is about.
        assertMemoryLeak(() -> {
            createA4fTable();
            assertA4fDeclaredType();

            assertExecHint("a4f", "f + 1.0 in (8388608.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f + 1.0 in (8388608.5, 2.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f + 1.0 not in (8388608.5, 2.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f * 2.0 in (1.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f * 2.0 in (1.5, 2.5)", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f * 2.0 not in (1.5, 2.5)", EXEC_HINT_WIDE_LANE);

            // The single-element list keeps its key and its element in lhs / rhs and serializes as
            // a bare equality; the two-element list is an OR of two, so both shapes of the widened
            // key run.
            assertA4fRows("f + 1.0 in (8388608.5)", 7);
            assertA4fRows("f + 1.0 in (8388608.5, 2.5)", 7, 8);
            // NOT IN over the same list. The NULL row answers TRUE here: NaN equals no element.
            assertA4fRows("f + 1.0 not in (8388608.5, 2.5)", 0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12);
            // The product spelling, whose only pin anywhere was an execution hint. Doubling a
            // finite float is exact at both widths, so these rows pin the composition and the list
            // rather than the width - the sum spellings above are what make the width observable.
            assertA4fRows("f * 2.0 in (1.5)", 9);
            assertA4fRows("f * 2.0 in (1.5, 2.5)", 9, 10);
            assertA4fRows("f * 2.0 not in (1.5, 2.5)", 0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12);

            // The element the a2f sites use, kept here as the control that says WHY they are
            // empty: no FLOAT value makes it true, so no rows is the correct answer, not a fixture
            // accident. This is the one leg in this test that pins an empty result, and it says so.
            assertExecHint("a4f", "f + 1.0 in (16777216.5)", EXEC_HINT_WIDE_LANE);
            assertA4fNoRows("f + 1.0 in (16777216.5)");
        });
    }

    @Test
    public void testDoubleConstArithVsOutOfIntConstantPinsRows() throws Exception {
        // "f + 1.0 > 5_000_000_001" is the composition that puts an (i64, f64) pairing on the
        // four-lane loop: the DOUBLE literal pulls the FLOAT column's ADD to f64 through
        // isNarrowLaneDoubleConstArith(), and the out-of-INT bound emits beside it as a full I8
        // immediate. CompiledFilterIRSerializerTest#testDoubleConstantInFourByteArithmeticEmitsAtF8
        // pins the IR and the WIDE_LANE hint for it, and that was all that held it - the spelling
        // appeared nowhere else in the tree, so no cursor ever opened over it and a wrong
        // avx2::convert() arm for the pairing would have returned silent wrong rows.
        //
        // The bound is chosen so the rows straddle it exactly. 5.0E9 sits in [2^32, 2^33) where
        // the float ulp is 512 and is exactly representable, as are its two neighbours: row 2's
        // f + 1.0 lands ON the bound, rows 1 and 3 one ulp either side of it.
        assertMemoryLeak(() -> {
            createA4fTable();
            assertA4fDeclaredType();

            // The name of this test is a claim about a composition the IR test pins WIDE_LANE, so
            // pin the mode here too rather than inferring it from the rows: the same rows come
            // back on the scalar backend, which is exactly why an unobserved mode change is quiet.
            for (String op : new String[]{"=", "<>", "<", "<=", ">", ">="}) {
                assertExecHint("a4f", "f + 1.0 " + op + " 5_000_000_001", EXEC_HINT_WIDE_LANE);
                assertExecHint("a4f", "5_000_000_001 " + op + " f + 1.0", EXEC_HINT_WIDE_LANE);
            }
            assertExecHint("a4f", "not (f + 1.0 > 5_000_000_001)", EXEC_HINT_WIDE_LANE);

            // f + 1.0 per row at f64: 6000000001, 5000000513, 5000000001, 4999999489, -4999999999,
            // -4999999487, -4983222783, 8388608.5, 2.5, 1.75, 2.25, NaN, 0.0. NaN fails every
            // ordering operator and the equality, and satisfies "<>" alone.
            assertA4fRows("f + 1.0 = 5_000_000_001", 2);
            assertA4fRows("f + 1.0 <> 5_000_000_001", 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
            assertA4fRows("f + 1.0 < 5_000_000_001", 3, 4, 5, 6, 7, 8, 9, 10, 12);
            assertA4fRows("f + 1.0 <= 5_000_000_001", 2, 3, 4, 5, 6, 7, 8, 9, 10, 12);
            assertA4fRows("f + 1.0 > 5_000_000_001", 0, 1);
            assertA4fRows("f + 1.0 >= 5_000_000_001", 0, 1, 2);
            // The immediate on the left: emit_bin_op types the comparison from its left operand,
            // so the two operand orders reach the backend as different pairings.
            assertA4fRows("5_000_000_001 = f + 1.0", 2);
            assertA4fRows("5_000_000_001 <> f + 1.0", 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
            assertA4fRows("5_000_000_001 < f + 1.0", 0, 1);
            assertA4fRows("5_000_000_001 <= f + 1.0", 0, 1, 2);
            assertA4fRows("5_000_000_001 > f + 1.0", 3, 4, 5, 6, 7, 8, 9, 10, 12);
            assertA4fRows("5_000_000_001 >= f + 1.0", 2, 3, 4, 5, 6, 7, 8, 9, 10, 12);
            // NOT over the whole comparison keeps the mode, and drops the NaN row - which fails
            // the predicate and its negation alike.
            assertA4fRows("not (f + 1.0 > 5_000_000_001)", 2, 3, 4, 5, 6, 7, 8, 9, 10, 12);
        });
    }

    @Test
    public void testFloatArithI64AndDoubleConstChainWidensToWideLane() throws Exception {
        // The one three-way (f32, i64 IMM, f64 IMM) chain, and the boundary between two adjacent
        // spellings. The two literals settle the mode between them: promoteArithType(F4, I8)
        // answers F4 - the F4 clause fires before the integer-width one - so
        // "f + 5_000_000_000" stays a FLOAT computation, and promoteArithType(F4, F8) then makes
        // "... + 1.0" F8. hasEightByteLeaf() counts columns and bind variables only, so the I8
        // CONSTANT does not suppress the source, isNarrowLaneDoubleConstArith() claims the outer
        // node and the filter takes the four-lane loop. Drop the DOUBLE literal and the same
        // predicate is SCALAR, which
        // CompiledFilterIRSerializerTest#testEightByteArithI64ConstantKeepsVectorization pins.
        //
        // The two spellings return the SAME rows against this bound, so nothing but the hint tells
        // them apart - which is the reason to pin it.
        // CompiledFilterIRSerializerTest#testFloatArithI64AndDoubleConstChainEmitsAtF8 pins the IR.
        assertMemoryLeak(() -> {
            createA4fTable();
            assertA4fDeclaredType();

            assertExecHint("a4f", "f + 5_000_000_000 + 1.0 > 1.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f + 5_000_000_000 > 1.5", EXEC_HINT_SCALAR);
            // f + 5_000_000_000 per row, computed at f32 because the inner node is F4-typed:
            // 1.1E10, 1.0000001E10, 1.0E10, 9.999999E9, 0.0, 512.0, 1.6777216E7, 5.0083886E9,
            // 5.0E9, 5.0E9, 5.0E9, NaN, 5.0E9. Only row 4, where the column exactly cancels the
            // constant, falls below the bound.
            assertA4fRows("f + 5_000_000_000 + 1.0 > 1.5", 0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12);
            assertA4fRows("f + 5_000_000_000 > 1.5", 0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12);

            // The same pair against a bound that DOES separate them, so the chain is not pinned by
            // its mode alone. Row 6's inner sum is exactly 2^24, where the float ulp is 2: adding
            // 1.0 is a no-op at f32 and answers 16777217.0 at f64, and the bound sits between the
            // two. So the three-way chain admits row 6 and the two-way spelling does not.
            //
            // Both spellings are WIDE_LANE against this bound, for different reasons: 16777216.5
            // has no exact float, which makes it an isFloatWideningConst() conversion source in its
            // own right, whereas 1.5 does have one and is not.
            assertExecHint("a4f", "f + 5_000_000_000 + 1.0 > 16777216.5", EXEC_HINT_WIDE_LANE);
            assertExecHint("a4f", "f + 5_000_000_000 > 16777216.5", EXEC_HINT_WIDE_LANE);
            assertA4fRows("f + 5_000_000_000 + 1.0 > 16777216.5", 0, 1, 2, 3, 6, 7, 8, 9, 10, 12);
            assertA4fRows("f + 5_000_000_000 > 16777216.5", 0, 1, 2, 3, 7, 8, 9, 10, 12);
            assertA4fRows("f + 5_000_000_000 + 1.0 <= 16777216.5", 4, 5);
            assertA4fRows("f + 5_000_000_000 <= 16777216.5", 4, 5, 6);
        });
    }

    @Test
    public void testNarrowConstArithVsEightByteLanePinsBoundaryRows() throws Exception {
        // A pure-constant INT arithmetic subtree the width-aware fold declines keeps its
        // per-operation IR at i32 - "(0 - 1000)" emits as (i32 1000L)(i32 0L)(-) - because the
        // Java filter computes it at INT width and wraps there. The type observer counts columns
        // and bind variables only, so a predicate whose other operand is a LONG or TIMESTAMP
        // column still reports a single observed size of eight bytes and getExecHint() used to
        // hand the filter to the single-size AVX2 loop.
        //
        // Before b8418b7629, avx2::convert() gated its (i32, f64) arm on the wide_lane FLAG rather
        // than on the loop's lane count, and fell THROUGH unconverted when the flag was clear -
        // including in a single-size loop that was already four lanes wide. emit_bin_op types the
        // comparison from the left operand alone, so it compared an f64 register against one
        // holding eight packed i32 and selected the wrong rows; the mask it left was built at
        // 32-bit granularity while avx2::compress_register permutes the row-id register with
        // vpermps, so the two halves of the mask selected the halves of DIFFERENT row ids and the
        // spliced 64-bit result was an id no page frame holds. PageFrameMemoryRecord.getLong
        // dereferenced it and the JVM took a SIGSEGV.
        //
        // That arm now gates on the lane count instead: it converts through cvt_itod at four lanes
        // and DECLINES the filter otherwise (jit/avx2.h:680-686), so a miss costs the compiled backend
        // rather than rows. cvt_itod reads only the low 128 bits of its operand, which is why four
        // lanes is the count it is correct at. The predicates below run the single-size loop over
        // an eight-byte column, which steps four lanes (compiler.cpp:378 - 256 / (8 * 8)), so
        // they take the arm and the rows here are what it has to produce.
        //
        // The last predicate below is the spelling that faulted; it runs last so that a regression
        // reddens the assertions above it before it takes the fork down.
        assertMemoryLeak(() -> {
            createA3wTable();
            // -(2147483647 / 5) is -429_496_729 at INT width, so the comparison keeps every row
            // whose l64 exceeds -858_993_458. NULL l64 makes the product NaN, which no ordering
            // comparison accepts.
            assertA3wRows("(-(2147483647 / 5)) < ((l64 + 0) * 0.5)", 3, 4, 5, 6, 7, 8, 9);
            // 1.0E-30 / ts underflows towards zero for a realistic timestamp, so the right side is
            // a tiny negative number and the left one is around -2.9E30: every row matches.
            assertA3wRows("((2.0 - ts) * ts) <= ((1.0E-30 / ts) * (0 - 1000))",
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            // Correct answer is NO rows - -0.0 is never 500 - and the defect ADDED rows here, so
            // the empty result is the oracle rather than an absence of coverage.
            assertA3wNoRows("(-(0.0 * l64)) = (1000 / 2)");
            assertA3wRows("not (0 > ((-3 * 0) / (0.5 * l64)))", 0, 1, 2, 3, 4, 5, 7, 8, 9);
            assertA3wRows("l64 * 0.5 > (0 - 1000)", 5, 6, 7, 8, 9);
            assertA3wRows("(0 - 1000) > (0.5 * l64)", 0, 1, 2, 3);
        });
    }

    @Test
    public void testNarrowConstArithVsEightByteLaneKeepsVectorizedLoop() throws Exception {
        // Pins the loop itself, not just the rows. The rows above agree on all three modes
        // whichever backend runs them, so a change that quietly dropped this shape onto the
        // scalar loop - or onto the Java filter - would leave those assertions green while the
        // vectorization the eight-byte lane earns silently went away.
        //
        // Eight-byte lanes step four rows per YMM iteration, which is exactly the lane count
        // avx2::convert()'s width-changing arms are correct at, so these filters keep
        // EXEC_HINT_SINGLE_SIZE_TYPE and the backend harmonises the (i32, f64) pairing.
        //
        // What the hints alone do NOT do is fail on a revert. The a3w fixture names eight-byte
        // columns only, so master answers EXEC_HINT_SINGLE_SIZE_TYPE for all of them too: its type
        // observer counts columns and bind variables, sees one size, and its getExecHint() reads
        // "hasMixedSizes() ? MIXED_SIZE : SINGLE_SIZE" - EXEC_HINT_WIDE_LANE has no value there at
        // all. So every hint below holds on a tree with none of this work in it.
        //
        // The last leg is what makes the method revert-sensitive, and it is a row leg rather than a
        // hint. Master folds a pure-constant integer chain at LONG width - tryFoldConstantArith()
        // multiplies at 64 bits and emits 10^12 as one I8 IMM - on the premise that FunctionParser
        // hands the Java filter a LongConstant for it. It does not: the Java filter runs
        // MulInt#getInt, which wraps 1_000_000 * 1_000_000 to -727_379_968, and
        // foldConstantArithWidthAware() is what now reproduces that width. The a3w column straddles
        // the two answers, so the two trees select different rows here.
        //
        // The other five controls get row legs too, since they had none; those legs pin
        // JIT-against-Java parity but, like the hints, do not separate the two trees. The six
        // shapes above keep their rows in the sibling method rather than repeating them here.
        assertMemoryLeak(() -> {
            createA3wTable();
            assertExecHint("a3w", "(-(2147483647 / 5)) < ((l64 + 0) * 0.5)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("a3w", "((2.0 - ts) * ts) <= ((1.0E-30 / ts) * (0 - 1000))", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("a3w", "(-(0.0 * l64)) = (1000 / 2)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("a3w", "not (0 > ((-3 * 0) / (0.5 * l64)))", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("a3w", "l64 * 0.5 > (0 - 1000)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertExecHint("a3w", "(0 - 1000) > (0.5 * l64)", EXEC_HINT_SINGLE_SIZE_TYPE);
            // Controls. A filter whose constant chain folds to one eight-byte immediate - or
            // whose operands were eight bytes to begin with - never had a mixed-width pairing and
            // keeps the same loop. Each carries its rows as well as its hint: -1000 and 1_000_000
            // do not wrap at INT width and 10 / 3 truncates to 3 there, which is the value the
            // Java filter compares against.
            assertExecHint("a3w", "l64 > (0 - 1000)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("l64 > (0 - 1000)", 6, 7, 8, 9);
            assertExecHint("a3w", "(0 - 1000) < l64", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("(0 - 1000) < l64", 6, 7, 8, 9);
            assertExecHint("a3w", "d64 > (10 / 3)", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("d64 > (10 / 3)", 6, 7, 8, 9);
            assertExecHint("a3w", "l64 * 0.5 > d64", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("l64 * 0.5 > d64", 8, 9);
            assertExecHint("a3w", "l64 > 1000 * 1000", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("l64 > 1000 * 1000", 9);
            // The one spelling over this fixture whose ROWS separate this branch from master.
            // 1_000_000 * 1_000_000 is 10^12 at 64 bits and -727_379_968 at INT width: rows 3 to 9
            // clear the wrapped bound while only row 9 clears 10^12. The Java filter answers the
            // first set on either tree, so a JIT that folds at long width diverges from it, and
            // this leg is what a revert reddens.
            assertExecHint("a3w", "l64 > 1_000_000 * 1_000_000", EXEC_HINT_SINGLE_SIZE_TYPE);
            assertA3wRows("l64 > 1_000_000 * 1_000_000", 3, 4, 5, 6, 7, 8, 9);
        });
    }

    @Test
    public void testNarrowConstArithVsEightByteLaneOverSimdBatch() throws Exception {
        // The fixture the defect was reported on: rnd_* columns over a batch long enough to run
        // the SIMD body and its scalar tail, partitioned so the filter crosses page frames. The
        // pinned fixture above pins the rows a chosen eleven-row table produces; this one covers
        // the batch length the rest of the class uses, adds the count() cross-check, and keeps
        // the reported spellings running against values nobody picked for them.
        final String ddl = "CREATE TABLE a3x AS (SELECT" +
                " timestamp_sequence(400_000_000_000, 500_000_000) AS k," +
                " rnd_float() f32," +
                " rnd_int() i32," +
                " rnd_long() l64," +
                " rnd_double() d64," +
                " timestamp_sequence(500_000_000_000, 700_000_000) ts," +
                " rnd_float() f32b," +
                " rnd_int() i32b" +
                " FROM long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) TIMESTAMP(k) PARTITION BY DAY";
        assertMemoryLeak(() -> {
            execute(ddl);
            assertQueryNotNullNoLeakCheck("a3x where (-(2147483647 / 5)) < ((l64 + 0) * 0.5)");
            assertQueryNotNullNoLeakCheck("a3x where ((2.0 - ts) * ts) <= ((1.0E-30 / ts) * (0 - 1000))");
            assertQueryEmptyNoLeakCheck("a3x where (-(0.0 * l64)) = (1000 / 2)");
            assertQueryNotNullNoLeakCheck("a3x where not (0 > ((-3 * 0) / (0.5 * l64)))");
            assertQueryNotNullNoLeakCheck("a3x where l64 * 0.5 > (0 - 1000)");
            assertQueryNotNullNoLeakCheck("a3x where (0 - 1000) > (0.5 * l64)");
        });
    }

    @Test
    public void testAvx2BackendDeclinesI128PairedWithNarrowerOperand() throws Exception {
        // avx2::convert() harmonises the two operands of a binary op before emit_bin_op issues the
        // instruction, and the instruction types itself from the LEFT operand alone. Every pairing
        // the function cannot harmonise therefore has to fail CLOSED - and every arm does, bar one:
        // the i128 arm returned the pairing before the terminal "lhs.dtype() != rhs.dtype()" check,
        // so an (i128, i64) pairing came back unharmonised and undeclined while its mirror image,
        // (i64, i128), declined through the inner switch's default. The unharmonised direction
        // compares 128-bit lanes against a register holding 64-bit ones: wrong rows, silently.
        //
        // No IR stream the frontend emits reaches that arm today, which is why the stream is
        // hand-written here rather than spelled in SQL. An i128 operand comes from a UUID or
        // LONG128 column, a UUID literal or a UUID / LONG128 bind variable; PredicateContext's
        // updateType rejects any other column beside a UUID one outright, and SQL resolves no
        // comparison operator for LONG128 against another type at all ("there is no matching
        // operator `=` with the argument types: LONG128 = LONG"). Even driving the serializer
        // straight past those, an i128 operand makes getExecHint() answer MIXED_SIZE or SCALAR the
        // moment anything narrower joins the filter, and neither hint runs an AVX2 loop. The arm is
        // a fail-open hole in a switch that was deliberately rewritten to fail closed; this test is
        // what keeps it closed.
        //
        // The comparison is EQ rather than the GT the other probe streams spell because
        // impl/avx2.h implements cmp_eq for i128 and cmp_lt does not. A GT here would be undefined
        // behaviour one layer BELOW the arm under test, which would mask what this pins.
        Assume.assumeTrue("both avx2_loop variants sit behind an AVX2 check", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3) plus EXEC_HINT_SINGLE_SIZE_TYPE in bits 4-5,
            // which is what compiler.cpp turns into avx2_loop() at step 4.
            final int options = (3 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                assertJitBackendCompiles(ir, options, CompiledFilterIRSerializer.EQ, "avx2_loop");
                // Unlike the out-of-enum opcode below, this reason survives to the JVM as the AVX2
                // backend wrote it: x86::convert() hands an i128 pairing back without declining, so
                // the scalar tail avx2_loop() emits over the same stream reports nothing after it.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.EQ, IR_DEFECT_I128_LHS,
                        "no conversion for this operand pairing", "avx2_loop");
            }
        });
    }

    @Test
    public void testAvx2BackendDeclinesMixedWidthPairingOutsideFourLanes() throws Exception {
        // avx2::convert() gates every conversion that CHANGES a lane width on the loop's lane
        // count, and this is the pin for that gate. sx_i64, cvt_itod, cvt_ftod and cvt_ltod each
        // produce exactly four results, so each is correct at four lanes and only at four lanes -
        // and compiler.cpp runs four lanes for EXEC_HINT_WIDE_LANE and for EXEC_HINT_SINGLE_SIZE
        // over an eight-byte column alike, which is why the gate reads the lane count rather than
        // the wide_lane flag it used to read.
        //
        // The control is the SAME IR STREAM under a different options word: an (i64, f64) pairing
        // compiles at four lanes, harmonises through cvt_ltod and selects the right rows, and the
        // identical stream declines the moment the options say the loop is eight lanes wide. That
        // isolates the lane count as the only variable - a row-level control over a second probe
        // column could not, because the operand widths would have to move with it.
        //
        // Both directions of the pairing are here. They are the two arms the third run had to gate
        // and the ones a mirror-image miss hides in: emit_bin_op types its instruction from the
        // LEFT operand alone, so (i64, f64) and (f64, i64) reach different arms and a gate added to
        // one of them leaves the other comparing a register of four f64 against eight packed i32.
        //
        // No SQL reaches either arm at a lane count other than four - getExecHint() answers
        // SINGLE_SIZE only over a uniform observed width, and an eight-byte operand makes that
        // width eight - so the streams are hand-written, like the other backend probes here.
        Assume.assumeTrue("both avx2_loop variants sit behind an AVX2 check", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3): compiler.cpp computes step = 256 / (8 * 8),
            // i.e. the four-lane loop.
            final int fourLaneOptions = (3 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            // A four-byte lane (log2 2): step = 256 / (4 * 8), i.e. eight lanes. The stream still
            // carries eight-byte operands, which is exactly the disagreement the gate exists for.
            final int eightLaneOptions = (2 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                // (i64 column) > (f64 immediate 0.0). cvt_ltod widens the column at four lanes.
                writePairingProbeIr(ir, CompiledFilterIRSerializer.I8_TYPE, CompiledFilterIRSerializer.F8_TYPE, false, false);
                assertProbeStreamCompiles(ir, fourLaneOptions, CompiledFilterIRSerializer.GT, "avx2_loop");
                assertProbeStreamDeclines(ir, eightLaneOptions, CompiledFilterIRSerializer.GT,
                        "i64-with-f64 pairing outside the four-lane loop", "avx2_loop");

                // The mirror: (f64 immediate 0.0) < (i64 column), the same predicate with the two
                // operands the other way round. The immediate carries the f64 side rather than the
                // column because a probe column READ as f64 cannot serve: the row values 7, 5 and 2
                // are subnormal doubles, and the comparison answers as though they were zero, so
                // that spelling has no rows to pin. probeMatches(GT) is still the expected answer -
                // "imm < col" and "col > imm" select the same rows.
                writePairingProbeIr(ir, CompiledFilterIRSerializer.I8_TYPE, CompiledFilterIRSerializer.F8_TYPE, false, true);
                assertProbeStreamCompiles(ir, fourLaneOptions, CompiledFilterIRSerializer.GT, "avx2_loop");
                assertProbeStreamDeclines(ir, eightLaneOptions, CompiledFilterIRSerializer.GT,
                        "f64-with-i64 pairing outside the four-lane loop", "avx2_loop");
            }
        });
    }

    @Test
    public void testAvx2BackendDeclinesOutOfEnumOpcode() throws Exception {
        // avx2::emit_bin_op()'s switch over instr.opcode used to end in
        // "default: __builtin_unreachable()". emit_code() routes EVERY opcode it does not handle
        // itself into that function, so the default arm is what a corrupted IR stream - or one
        // written by a frontend that has grown an opcode this backend has not - lands on, and
        // __builtin_unreachable() makes that undefined behaviour: the compiler drops the range
        // check on the jump table and the opcode indexes past its end, inside the JVM, with no
        // recovery. Declining costs a JIT decline and the Java filter instead.
        //
        // No SQL reaches it - serializeOperator emits only the opcodes common.h defines - so the
        // stream is hand-written. The control is the same stream with a real GT in place of the
        // out-of-enum opcode, and assertJitBackendCompiles() RUNS it over IR_PROBE_COLUMN, so a
        // decline below cannot be blamed on the stream around it.
        //
        // The reason that reaches the JVM is the SCALAR backend's, not the SIMD one's:
        // Function::avx2_loop() finishes by emitting a scalar_tail() over the SAME stream, so
        // x86::emit_bin_op reports second and JitErrorHandler::handle_error assigns rather than
        // latches. What pins the AVX2 arm here is that the arm has to survive being reached at all
        // for the scalar tail to get its turn.
        Assume.assumeTrue("both avx2_loop variants sit behind an AVX2 check", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3) plus EXEC_HINT_SINGLE_SIZE_TYPE in bits 4-5,
            // which is what compiler.cpp turns into avx2_loop() at step 4.
            final int options = (3 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                assertJitBackendCompiles(ir, options, CompiledFilterIRSerializer.GT, "avx2_loop");
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_OUT_OF_ENUM_OPCODE,
                        "unsupported opcode in the scalar path", "avx2_loop");
            }
        });
    }

    @Test
    public void testAvx2BackendDeclinesSignExtensionOutsideWideLaneLoop() throws Exception {
        // avx2::emit_code()'s Sx_I64 arm declines outside the wide-lane loop. sx_i64 sign-extends
        // the LOW 128 bits of its operand into four i64, so it fills a whole register only where
        // the register holds four lanes and the operand's four i32 are the ones the loop is
        // reading. The wide-lane loop guarantees both; a single-size loop over an eight-byte
        // column reads its i32 operand as eight packed lanes and sx_i64 would silently drop four
        // of them.
        //
        // As with the lane-count test above, the control is the SAME IR STREAM under a different
        // options word: it compiles under EXEC_HINT_WIDE_LANE, where the arm is legitimate, and
        // selects the right rows. Both options words run at four lanes, so the wide-lane flag is
        // the only variable between them - which is what the arm gates on.
        //
        // The arm cannot be reached from SQL: the serializer emits SX_I64 only for a leaf
        // isWideLaneEligible() has admitted, and getExecHint() answers WIDE_LANE for every filter
        // that emits one. So the stream is hand-written. The declining arm keeps emitting after it
        // declines, deliberately - abandoning would leave the value stack short and avx2_loop would
        // pop an empty ArenaVector - and this test is what pins that the decline happens at all.
        Assume.assumeTrue("both avx2_loop variants sit behind an AVX2 check", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // Both words spell an eight-byte lane, so both run four lanes. Only the exec hint,
            // hence only wide_lane, differs.
            final int wideLaneOptions = (3 << 1) | (EXEC_HINT_WIDE_LANE << 4);
            final int singleSizeOptions = (3 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                // (i64 column) > sx_i64(i32 immediate 0). The sign extension sits over the
                // IMMEDIATE rather than over the column so that the stream reads the probe column
                // at its real width in both runs; that keeps the control's row set the one every
                // other probe here expects.
                writePairingProbeIr(ir, CompiledFilterIRSerializer.I8_TYPE, CompiledFilterIRSerializer.I4_TYPE, true, false);
                assertProbeStreamCompiles(ir, wideLaneOptions, CompiledFilterIRSerializer.GT, "avx2_loop");
                assertProbeStreamDeclines(ir, singleSizeOptions, CompiledFilterIRSerializer.GT,
                        "SX_I64 outside the wide-lane loop", "avx2_loop");
            }
        });
    }

    @Test
    public void testAvx2BackendDeclinesWhenCodeGenerationAbandonsMidStream() throws Exception {
        // avx2::emit_code() abandons code generation with a bare return for the opcodes it cannot
        // vectorize - opcodes::Inv and the four short-circuit opcodes. Abandoning leaves the value
        // stack unbalanced, and avx2_loop() then pops it: ArenaVector::pop() asserts only in a
        // debug build, so the release build we ship underflows the size to UINT32_MAX and reads out
        // of bounds inside the JVM.
        //
        // No SQL reaches either opcode today - backfillNode() overwrites every Inv placeholder or
        // aborts JIT compilation, and serializePredicatesAndSc / serializePredicatesOrSc throw when
        // the exec hint is a SIMD one - so this drives the backend directly with a hand-written IR
        // stream. The control stream compiles, which is what proves the stream and the options are
        // well-formed and that the two declines below come from the planted opcode rather than from
        // the stream around it. assertJitBackendCompiles() RUNS that control filter over
        // IR_PROBE_COLUMN and pins the row ids and the count it answers with, so a stream that
        // compiled into the wrong code cannot pass itself off as a valid control.
        // assertJitBackendDeclines() expects compile() to throw instead, and runs a filter only on
        // the failure path, where compile() wrongly handed one back and the row ids name the damage.
        Assume.assumeTrue("both avx2_loop variants sit behind an AVX2 check", Vect.getSupportedInstructionSet() >= 8);
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3) plus EXEC_HINT_SINGLE_SIZE_TYPE in bits 4-5,
            // which is what compiler.cpp turns into avx2_loop() at step 4.
            final int options = (3 << 1) | (EXEC_HINT_SINGLE_SIZE_TYPE << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                assertJitBackendCompiles(ir, options, CompiledFilterIRSerializer.GT, "avx2_loop");
                // A short-circuit opcode sits part-way through the stream, so emit_code() abandons
                // with the comparison mask still on the value stack. avx2_loop()'s empty-stack
                // guard cannot see that one - the And_Sc arm has to decline for itself.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_SHORT_CIRCUIT,
                        "short-circuit opcode in the SIMD path", "avx2_loop");
                // opcodes::Inv at the head of the stream abandons with NOTHING on the value stack.
                // That is what avx2_loop()'s values.is_empty() guard exists for. Three sites report
                // for this one stream, in order: avx2::emit_code's Inv arm ("invalid opcode in the
                // SIMD path"), the guard ("AVX2 code generation abandoned mid-stream"), and finally
                // x86::emit_code's own Inv arm, because avx2_loop() finishes by emitting a
                // scalar_tail() over the SAME stream. JitErrorHandler::handle_error assigns rather
                // than latches, so the last reporter is the one that reaches the log - and that is
                // the scalar tail's. The guard is still load-bearing: delete it and the compile
                // SIGSEGVs in values.pop() long before the tail is emitted.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_INV,
                        "invalid opcode in the scalar path", "avx2_loop");
                // The same opcode one instruction later, in the left operand's slot - the position
                // the serializer actually writes an Inv in. The value stack is short but NOT empty
                // when emit_code() abandons, so BOTH is_empty() guards stay silent and both loops
                // pop the surviving immediate and scatter row ids by it. Only the Inv arm's own
                // decline_filter() call declines this stream, which is why it earns a pin of its
                // own rather than being folded into the head-of-stream case above.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_INV_OPERAND,
                        "invalid opcode in the scalar path", "avx2_loop");
            }
        });
    }

    @Test
    public void testScalarBackendDeclinesOutOfEnumOpcode() throws Exception {
        // The scalar twin of the AVX2 test above. x86::emit_bin_op and aarch64::emit_bin_op carried
        // the identical "default: __builtin_unreachable()", so an out-of-enum opcode was undefined
        // behaviour on every backend rather than only the SIMD one - and on ARM64 the scalar
        // backend is the ONLY one there is, since Function::compile() always selects scalar_loop.
        //
        // EXEC_HINT_SCALAR routes Function::compile and CountOnlyFunction::compile to scalar_loop
        // on every CPU, so this needs no instruction-set assumption and covers aarch64::emit_bin_op
        // when the suite runs on ARM64.
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3) plus EXEC_HINT_SCALAR in bits 4-5.
            final int options = (3 << 1) | (EXEC_HINT_SCALAR << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                assertJitBackendCompiles(ir, options, CompiledFilterIRSerializer.GT, "scalar_tail");
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_OUT_OF_ENUM_OPCODE,
                        "unsupported opcode in the scalar path", "scalar_tail");
            }
        });
    }

    @Test
    public void testScalarBackendDeclinesWhenCodeGenerationAbandonsMidStream() throws Exception {
        // The scalar twin of the AVX2 test above, and the worse half of the same defect: x86 and
        // aarch64 emit_code() abandon code generation with a bare return at opcodes::Inv, and
        // scalar_tail() then FAILS OPEN. Its "if (!values.is_empty())" guard exists for the
        // legitimate case where every predicate resolved through a short-circuit jump, so on an
        // empty value stack it simply skips the final test/jz and falls straight into
        // "bind(l_store_row); mov(rows[output_index], input_index); add(output_index, 1)" - an
        // unconditional store that selects every row, and an unconditional increment that counts
        // every row. Nothing signals it, which is why this test asserts the ROWS and not merely
        // that compilation survived.
        //
        // As with the AVX2 twin, no SQL reaches opcodes::Inv: the serializer writes it only as the
        // placeholder for a symbol bind variable and a constant stub, and backfillNode() either
        // overwrites those 24 bytes or throws. So the stream is hand-written here.
        //
        // EXEC_HINT_SCALAR routes Function::compile and CountOnlyFunction::compile to scalar_loop
        // on every CPU, so unlike the AVX2 twin this needs no instruction-set assumption - and on
        // ARM64, where Function::compile is scalar_loop unconditionally, it covers the only JIT
        // backend that architecture has.
        assertMemoryLeak(() -> {
            // An eight-byte lane (log2 3 in bits 1-3) plus EXEC_HINT_SCALAR in bits 4-5.
            final int options = (3 << 1) | (EXEC_HINT_SCALAR << 4);
            try (MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT)) {
                assertJitBackendCompiles(ir, options, CompiledFilterIRSerializer.GT, "scalar_tail");
                // Inv at the head of the stream: the empty-stack shape the is_empty() guard was
                // written for, where the fail-open store selects EVERY row.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_INV,
                        "invalid opcode in the scalar path", "scalar_tail");
                // Inv in the left operand's slot, which is where the serializer writes one. The
                // stack is short but not empty, so the !values.is_empty() guard passes and
                // scalar_tail() emits its test/jz over the surviving immediate instead of over a
                // comparison result: a filter that runs and answers with the wrong rows rather than
                // with every row. Nothing but the Inv arm's decline_filter() call catches it.
                assertJitBackendDeclines(ir, options, CompiledFilterIRSerializer.GT, IR_DEFECT_INV_OPERAND,
                        "invalid opcode in the scalar path", "scalar_tail");
            }
        });
    }

    // Writes "l64 <comparison> 0" over column 0 as a raw IR stream, optionally planting one defect
    // in it. The comparison and the defect are orthogonal: every defect keeps the surrounding
    // stream byte-identical to the control the same comparison produces, so a decline can only
    // have come from the defect.
    private static void writeProbeIr(MemoryCARW ir, int comparison, int defect) {
        ir.truncate();
        if (defect == IR_DEFECT_INV) {
            putIrInstruction(ir, IR_OPCODE_INV, 0, 0);
        }
        // get_arguments() pops lhs first, so the stream pushes the right-hand operand first.
        putIrInstruction(ir, CompiledFilterIRSerializer.IMM, CompiledFilterIRSerializer.I8_TYPE, 0);
        if (defect == IR_DEFECT_INV_OPERAND) {
            // Inv in the LEFT OPERAND's slot, which is where the serializer writes one: it is the
            // placeholder for a symbol bind variable and for a constant stub, and both stand in for
            // an operand rather than for the head of a stream. emit_code() abandons here with the
            // right-hand operand still on the value stack - short, but NOT empty - so neither
            // avx2_loop()'s values.is_empty() backstop nor scalar_tail()'s !values.is_empty() guard
            // fires. Both loops pop the surviving operand and use it as the row mask, and the
            // filter RUNS: it tests the immediate rather than a comparison result. Only the Inv
            // arm's own decline_filter() call stands between that stream and a filter that answers
            // with the wrong rows, which is what makes this the dangerous position and the
            // stream-head one below the survivable one.
            putIrInstruction(ir, IR_OPCODE_INV, 0, 0);
        }
        // I16_TYPE is data_type_t::i128, the type code a UUID or LONG128 column carries. Beside the
        // I8 immediate above it spells the (i128, i64) pairing avx2::convert() cannot harmonise.
        putIrInstruction(
                ir,
                CompiledFilterIRSerializer.MEM,
                defect == IR_DEFECT_I128_LHS ? CompiledFilterIRSerializer.I16_TYPE : CompiledFilterIRSerializer.I8_TYPE,
                0
        );
        putIrInstruction(ir, defect == IR_DEFECT_OUT_OF_ENUM_OPCODE ? IR_OPCODE_OUT_OF_ENUM : comparison, 0, 0);
        if (defect == IR_DEFECT_SHORT_CIRCUIT) {
            // AND_SC against label 0 (next_row), exactly as serializePredicatesAndSc() writes it.
            putIrInstruction(ir, CompiledFilterIRSerializer.AND_SC, 0, 0);
        }
        putIrInstruction(ir, CompiledFilterIRSerializer.RET, 0, 0);
    }

    // Writes the predicate "column0 > immediate" over IR_PROBE_COLUMN as a raw IR stream, with the
    // operand type codes given, optionally a sign extension over the immediate, and optionally with
    // the immediate as the LEFT operand. Sibling of writeProbeIr(): that one holds the operand
    // types fixed at (i64, i64) and varies the DEFECT planted in the stream, this one plants no
    // defect and varies the operand TYPES and their ORDER, which is what avx2::convert() and the
    // Sx_I64 arm gate on. Both spell the same "operands then operator then ret" shape; keeping them
    // apart keeps each signature to the axis its callers vary.
    //
    // Either order answers with the row set probeMatches() returns for GT, because "imm < col" and
    // "col > imm" are the same predicate. The order matters to the BACKEND and not to the answer:
    // convert() and emit_bin_op both read the left operand first, so (i64, f64) and (f64, i64) take
    // different arms of the same switch.
    private static void writePairingProbeIr(
            MemoryCARW ir,
            int columnTypeCode,
            int immTypeCode,
            boolean isSignExtended,
            boolean isImmediateLhs
    ) {
        ir.truncate();
        // get_arguments() pops lhs first, so the stream pushes the right-hand operand first. An
        // all-zero payload reads as the integer 0 and as the double 0.0 alike, so one payload
        // serves whichever type code the immediate carries.
        if (isImmediateLhs) {
            putIrInstruction(ir, CompiledFilterIRSerializer.MEM, columnTypeCode, 0);
        }
        putIrInstruction(ir, CompiledFilterIRSerializer.IMM, immTypeCode, 0);
        if (isSignExtended) {
            // SX_I64 carries no type code of its own: both backends type its result from the
            // operand they pop, and hasUnharmonisedOperandWidths() pushes I8 for it without
            // reading the options field.
            putIrInstruction(ir, CompiledFilterIRSerializer.SX_I64, 0, 0);
        }
        if (!isImmediateLhs) {
            putIrInstruction(ir, CompiledFilterIRSerializer.MEM, columnTypeCode, 0);
        }
        putIrInstruction(ir, isImmediateLhs ? CompiledFilterIRSerializer.LT : CompiledFilterIRSerializer.GT, 0, 0);
        putIrInstruction(ir, CompiledFilterIRSerializer.RET, 0, 0);
    }

    // The match count a correctly compiled probe filter answers with for the comparison its stream
    // spells over IR_PROBE_COLUMN.
    private static int probeMatchCount(int comparison) {
        return comparison == CompiledFilterIRSerializer.EQ ? IR_PROBE_EQ_MATCH_COUNT : IR_PROBE_GT_MATCH_COUNT;
    }

    // The row ids a correctly compiled probe filter answers with for the comparison its stream
    // spells over IR_PROBE_COLUMN.
    private static String probeMatches(int comparison) {
        return comparison == CompiledFilterIRSerializer.EQ ? IR_PROBE_EQ_MATCHES : IR_PROBE_GT_MATCHES;
    }

    // Reads one of CompiledFilterIRSerializer's private execution-hint constants. Failing here
    // fails the whole class at class-init, naming the field - which is the point: a constant this
    // class cannot find is a constant production renamed or removed, and a copy would have gone on
    // asserting the old value instead.
    private static int serializerExecHint(String name) {
        try {
            final Field field = CompiledFilterIRSerializer.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.getInt(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot read CompiledFilterIRSerializer." + name, e);
        }
    }

    // instruction_t in common.h: a 4-byte opcode, a 4-byte options field, then a 16-byte payload.
    private static void putIrInstruction(MemoryCARW ir, int opcode, int typeCode, long payload) {
        ir.putInt(opcode);
        ir.putInt(typeCode);
        ir.putLong(payload);
        ir.putLong(0L);
    }

    private static void assertJitBackendCompiles(MemoryCARW ir, int options, int comparison, String loopName) throws SqlException {
        writeProbeIr(ir, comparison, IR_DEFECT_NONE);
        assertProbeStreamCompiles(ir, options, comparison, loopName);
    }

    // The half of assertJitBackendCompiles() that runs an ALREADY WRITTEN stream, for the tests
    // that vary the operand types rather than planting a defect: those compile the same stream
    // twice under two different options words, so they cannot let the assertion choose the stream.
    private static void assertProbeStreamCompiles(MemoryCARW ir, int options, int comparison, String loopName) throws SqlException {
        try (CompiledFilter filter = new CompiledFilter()) {
            filter.compile(ir, options);
            // Running the control stream is what makes the declines below attributable: it pins
            // that this IR and these options compile into a filter that selects the RIGHT rows, so
            // a wrong row set later can only have come from the planted defect.
            Assert.assertEquals(
                    "Function::" + loopName + " compiled the control stream into a filter that selects the wrong rows",
                    probeMatches(comparison),
                    callProbeFilter(filter)
            );
        }
        try (CompiledCountOnlyFilter filter = new CompiledCountOnlyFilter()) {
            filter.compile(ir, options);
            Assert.assertEquals(
                    "CountOnlyFunction::" + loopName + " compiled the control stream into a filter that counts the wrong rows",
                    probeMatchCount(comparison),
                    callProbeCountFilter(filter)
            );
        }
    }

    private static void assertJitBackendDeclines(MemoryCARW ir, int options, int comparison, int defect, String reason, String loopName) {
        writeProbeIr(ir, comparison, defect);
        assertProbeStreamDeclines(ir, options, comparison, reason, loopName);
    }

    // The half of assertJitBackendDeclines() that runs an ALREADY WRITTEN stream. See
    // assertProbeStreamCompiles().
    private static void assertProbeStreamDeclines(MemoryCARW ir, int options, int comparison, String reason, String loopName) {
        try (CompiledFilter filter = new CompiledFilter()) {
            filter.compile(ir, options);
            // compile() handed back a callable function pointer, so run it: the scalar backends
            // fail OPEN at opcodes::Inv and the row ids name the damage.
            Assert.fail("Function::" + loopName + " compiled a stream it must decline [reason=" + reason
                    + ", selectedRows=" + callProbeFilter(filter) + ", expectedRows=" + probeMatches(comparison) + ']');
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), reason);
        }
        try (CompiledCountOnlyFilter filter = new CompiledCountOnlyFilter()) {
            filter.compile(ir, options);
            Assert.fail("CountOnlyFunction::" + loopName + " compiled a stream it must decline [reason=" + reason
                    + ", count=" + callProbeCountFilter(filter) + ", expectedCount=" + probeMatchCount(comparison) + ']');
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), reason);
        }
    }

    // Runs the compiled count-only probe filter over IR_PROBE_COLUMN and returns the number of
    // rows it counted.
    private static long callProbeCountFilter(CompiledCountOnlyFilter filter) {
        final int rowCount = IR_PROBE_COLUMN.length;
        final long columnSize = (long) rowCount * Long.BYTES;
        // Allocate inside the try so a throw part-way through the sequence still frees whatever
        // the earlier mallocs handed back; 0 marks an allocation that never happened.
        long columnAddress = 0;
        long dataAddress = 0;
        long auxAddress = 0;
        long varsAddress = 0;
        try {
            columnAddress = Unsafe.malloc(columnSize, MemoryTag.NATIVE_DEFAULT);
            dataAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            auxAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            varsAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            writeProbeColumn(columnAddress, dataAddress, auxAddress, varsAddress);
            return filter.call(dataAddress, 1, auxAddress, varsAddress, 0, rowCount);
        } finally {
            if (varsAddress != 0) {
                Unsafe.free(varsAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (auxAddress != 0) {
                Unsafe.free(auxAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (dataAddress != 0) {
                Unsafe.free(dataAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (columnAddress != 0) {
                Unsafe.free(columnAddress, columnSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    // Runs the compiled probe filter over IR_PROBE_COLUMN and returns the row ids it selected,
    // comma separated. The stream spells whichever comparison writeProbeIr() was asked for, so a
    // correctly compiled filter answers probeMatches() for that comparison.
    private static String callProbeFilter(CompiledFilter filter) {
        final int rowCount = IR_PROBE_COLUMN.length;
        final long columnSize = (long) rowCount * Long.BYTES;
        // avx2_loop() scatters row ids a whole YMM register at a time, so the output buffer carries
        // four longs of slack past the last row id it can legitimately write.
        final long rowsSize = (long) (rowCount + 4) * Long.BYTES;
        // Allocate inside the try so a throw part-way through the sequence still frees whatever
        // the earlier mallocs handed back; 0 marks an allocation that never happened.
        long columnAddress = 0;
        long dataAddress = 0;
        long auxAddress = 0;
        long varsAddress = 0;
        long rowsAddress = 0;
        try {
            columnAddress = Unsafe.malloc(columnSize, MemoryTag.NATIVE_DEFAULT);
            dataAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            auxAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            varsAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            rowsAddress = Unsafe.malloc(rowsSize, MemoryTag.NATIVE_DEFAULT);
            writeProbeColumn(columnAddress, dataAddress, auxAddress, varsAddress);
            final long selected = filter.call(dataAddress, 1, auxAddress, varsAddress, 0, rowsAddress, rowCount);
            final StringSink sink = new StringSink();
            for (long i = 0; i < selected; i++) {
                if (i > 0) {
                    sink.putAscii(',');
                }
                sink.put(Unsafe.getUnsafe().getLong(rowsAddress + i * Long.BYTES));
            }
            return sink.toString();
        } finally {
            if (rowsAddress != 0) {
                Unsafe.free(rowsAddress, rowsSize, MemoryTag.NATIVE_DEFAULT);
            }
            if (varsAddress != 0) {
                Unsafe.free(varsAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (auxAddress != 0) {
                Unsafe.free(auxAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (dataAddress != 0) {
                Unsafe.free(dataAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            if (columnAddress != 0) {
                Unsafe.free(columnAddress, columnSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    // Lays out the argument block the compiled filter's C ABI expects: one column of LONGs, a
    // one-entry column address array pointing at it, and empty aux and bind variable blocks.
    private static void writeProbeColumn(long columnAddress, long dataAddress, long auxAddress, long varsAddress) {
        for (int i = 0; i < IR_PROBE_COLUMN.length; i++) {
            Unsafe.getUnsafe().putLong(columnAddress + (long) i * Long.BYTES, IR_PROBE_COLUMN[i]);
        }
        Unsafe.getUnsafe().putLong(dataAddress, columnAddress);
        Unsafe.getUnsafe().putLong(auxAddress, 0L);
        Unsafe.getUnsafe().putLong(varsAddress, 0L);
    }

    // The self-comparison WhereClauseParser.nodesEqual() recognises, spelled the same way it is:
    // BOTH sides a LITERAL or a CONSTANT carrying the same token. It is deliberately narrower than
    // "the two subtrees are equal" - "f + 1.0 > f + 1.0" is an OPERATION on both sides and the
    // parser leaves it in the filter, so rejecting it here would cost a pin for nothing.
    private static boolean isSelfComparison(ExpressionNode node) {
        return node.type == ExpressionNode.OPERATION
                && node.lhs != null
                && node.rhs != null
                && (node.lhs.type == ExpressionNode.LITERAL || node.lhs.type == ExpressionNode.CONSTANT)
                && (node.rhs.type == ExpressionNode.LITERAL || node.rhs.type == ExpressionNode.CONSTANT)
                && Chars.equals(node.lhs.token, node.rhs.token)
                && isCollapsingComparisonToken(node.token);
    }

    // The operators whose nodesEqual() arm collapses the node: analyzeEquals0, analyzeGreater,
    // analyzeLess and analyzeNotEquals0.
    private static boolean isCollapsingComparisonToken(CharSequence token) {
        return Chars.equals(token, "=")
                || Chars.equals(token, "!=")
                || Chars.equals(token, "<>")
                || Chars.equals(token, ">")
                || Chars.equals(token, ">=")
                || Chars.equals(token, "<")
                || Chars.equals(token, "<=");
    }

    private void createA3wTable() throws SqlException {
        // Only the eight-byte columns the predicates read matter: the type observer never sees a
        // column the filter does not name, so the FLOAT and INT columns of the reported table
        // would change nothing here.
        execute("CREATE TABLE a3w (k TIMESTAMP, l64 LONG, d64 DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(k) PARTITION BY DAY");
        execute("""
                INSERT INTO a3w VALUES
                    ('2024-01-01T00:00:00.000000Z', -9_223_372_036_854_775_807, -1.5, '2024-01-01T00:00:00.000000Z'),
                    ('2024-01-01T01:00:00.000000Z', -858_993_459, -0.5, '2024-01-01T00:00:01.000000Z'),
                    ('2024-01-01T02:00:00.000000Z', -858_993_458, 0.0, '2024-01-01T00:00:02.000000Z'),
                    ('2024-01-01T03:00:00.000000Z', -2_001, 0.5, '2024-01-01T00:00:03.000000Z'),
                    ('2024-01-01T04:00:00.000000Z', -2_000, 1.5, '2024-01-01T00:00:04.000000Z'),
                    ('2024-01-01T05:00:00.000000Z', -1_999, 2.5, '2024-01-01T00:00:05.000000Z'),
                    ('2024-01-01T06:00:00.000000Z', 0, 3.5, '2024-01-01T00:00:06.000000Z'),
                    ('2024-01-02T07:00:00.000000Z', 1, 4.5, '2024-01-02T00:00:07.000000Z'),
                    ('2024-01-02T08:00:00.000000Z', 1_000_000, 5.5, '2024-01-02T00:00:08.000000Z'),
                    ('2024-01-02T09:00:00.000000Z', 9_223_372_036_854_775_807, 6.5, '2024-01-02T00:00:09.000000Z'),
                    ('2024-01-02T10:00:00.000000Z', null, null, null)
                """);
    }

    private void assertA3wRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A3W_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k from a3w where " + predicate, expected);
    }

    // Declares that no a3w row satisfies predicate. An empty index list would otherwise reach
    // assertJitScalarAndVectorMatchJava as a header-only expected result and read as coverage.
    private void assertA3wNoRows(String predicate) throws SqlException {
        assertJitScalarAndVectorMatchJavaOnEmptyResult("select k from a3w where " + predicate, "k\n");
    }

    private void createA2fTable() throws SqlException {
        execute("CREATE TABLE a2f (k TIMESTAMP, i INT, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
        execute("""
                INSERT INTO a2f VALUES
                    (0, 16_777_216, 16777216.0),
                    (1_000_000, 16_777_217, 33554432.0),
                    (2_000_000, 5, 6.0),
                    (3_000_000, NULL, NULL),
                    (4_000_000, 16_777_217, 16777216.0),
                    (5_000_000, 0, 1.0),
                    (6_000_000, -16_777_217, -16777216.0),
                    (7_000_000, NULL, 1.0),
                    (8_000_000, 1, NULL)
                """);
    }

    private void assertA2fRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\ti\tf\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A2F_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k, i, f from a2f where " + predicate, expected);
    }

    // Declares that no a2f row satisfies predicate. See assertA3wNoRows.
    private void assertA2fNoRows(String predicate) throws SqlException {
        assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, i, f from a2f where " + predicate, "k\ti\tf\n");
    }

    // Builds the fixture the four-lane compositions of a DOUBLE literal with a FLOAT column run
    // over. Every value is exactly representable as a FLOAT, so the column stores what the literal
    // spells and the hand-computed sums below are the real ones:
    //   6.0E9, 5000000512.0, 5.0E9, 4999999488.0, -5.0E9, -4999999488.0 and -4983222784.0 all sit
    //   in [2^32, 2^33) where the ulp is 512 and are whole multiples of it;
    //   8388607.5 sits in [2^22, 2^23) where the ulp is 0.5;
    //   1.5, 0.75, 1.25 and -1.0 are exact everywhere.
    // Thirteen rows is three full four-lane steps plus a one-row scalar tail.
    //
    // Float literals carry no thousands separators on purpose: Numbers.parseDouble rejects an
    // underscore, so a separated one would make the serializer decline the filter and the test
    // would stop exercising the JIT at all.
    private void createA4fTable() throws SqlException {
        execute("CREATE TABLE a4f (k TIMESTAMP, f FLOAT) TIMESTAMP(k) PARTITION BY DAY");
        execute("""
                INSERT INTO a4f VALUES
                    (0, 6000000000.0),
                    (1_000_000, 5000000512.0),
                    (2_000_000, 5000000000.0),
                    (3_000_000, 4999999488.0),
                    (4_000_000, -5000000000.0),
                    (5_000_000, -4999999488.0),
                    (6_000_000, -4983222784.0),
                    (7_000_000, 8388607.5),
                    (8_000_000, 1.5),
                    (9_000_000, 0.75),
                    (10_000_000, 1.25),
                    (11_000_000, NULL),
                    (12_000_000, -1.0)
                """);
    }

    // The fixture trap: "CASE ... END::float" yields a DOUBLE column, and a DOUBLE key computes at
    // f64 in both engines, so a fixture built that way would pass whatever the widening does. Pin
    // the declared type, so a later fixture edit fails loudly instead of hollowing the row
    // assertions out.
    private void assertA4fDeclaredType() throws Exception {
        assertQuery("SELECT typeOf(f) ft FROM a4f LIMIT 1")
                .noLeakCheck()
                .expectSize()
                .returns("ft\nFLOAT\n");
    }

    private void assertA4fRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\tf\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A4F_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k, f from a4f where " + predicate, expected);
    }

    // Declares that no a4f row satisfies predicate. See assertA3wNoRows.
    private void assertA4fNoRows(String predicate) throws SqlException {
        assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, f from a4f where " + predicate, "k\tf\n");
    }

    private void assertA1nRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\ti\tf\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A1N_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k, i, f from a1n where " + predicate, expected);
    }

    private void assertA1oRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\ti\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A1O_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k, i from a1o where " + predicate, expected);
    }

    // Declares that no a1o row satisfies predicate. See assertA3wNoRows.
    private void assertA1oNoRows(String predicate) throws SqlException {
        assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, i from a1o where " + predicate, "k\ti\n");
    }

    private void assertA1qRows(String predicate, int... expectedRows) throws SqlException {
        final StringSink expected = new StringSink();
        expected.put("k\tb\ts\tf\n");
        for (int i = 0; i < expectedRows.length; i++) {
            expected.put(A1Q_ROWS[expectedRows[i]]);
        }
        assertJitScalarAndVectorMatchJava("select k, b, s, f from a1q where " + predicate, expected);
    }

    // Declares that no a1q row satisfies predicate. See assertA3wNoRows.
    private void assertA1qNoRows(String predicate) throws SqlException {
        assertJitScalarAndVectorMatchJavaOnEmptyResult("select k, b, s, f from a1q where " + predicate, "k\tb\ts\tf\n");
    }

    private void assertGeneratedQuery(CharSequence ddl, FilterGenerator gen, boolean notNull) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            long maxCount = 0;
            List<String> filters = gen.generate();
            LOG.info().$("generated ").$(filters.size()).$(" filter expressions for base query: select * from x").$();
            Assert.assertFalse(filters.isEmpty());
            for (String filter : filters) {
                long count = runQuery("x where " + filter);
                maxCount = Math.max(maxCount, count);

                assertJitQuery("x where " + filter, notNull);
                assertJitCountQuery("select count() from x where " + filter, count);
            }
            Assert.assertTrue("at least one query is expected to return rows", maxCount > 0);
        });
    }

    private void assertGeneratedQueryNotNull(CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(ddl, gen, true);
    }

    private void assertGeneratedQueryNullable(CharSequence ddl, FilterGenerator gen) throws Exception {
        assertGeneratedQuery(ddl, gen, false);
    }

    private void createIPv4TestTable() throws SqlException {
        execute("CREATE TABLE x (ip IPv4, ip2 IPv4, k TIMESTAMP) TIMESTAMP(k)");
        execute(
                """
                        INSERT INTO x
                        SELECT
                            (CASE x % 8
                                WHEN 0 THEN 0
                                WHEN 1 THEN 2_147_483_647
                                WHEN 2 THEN -2_147_483_648
                                WHEN 3 THEN -1
                                WHEN 4 THEN 167_772_161
                                WHEN 5 THEN -2_147_483_647
                                WHEN 6 THEN 1
                                ELSE 2_130_706_433
                            END)::INT::IPv4,
                            (CASE x % 8
                                WHEN 0 THEN 2_147_483_647
                                WHEN 1 THEN 2_147_483_647
                                WHEN 2 THEN -1
                                WHEN 3 THEN 0
                                WHEN 4 THEN -2_147_483_647
                                WHEN 5 THEN 167_772_161
                                WHEN 6 THEN 2_130_706_433
                                ELSE 1
                            END)::INT::IPv4,
                            timestamp_sequence(0, 1)
                        FROM long_sequence(""" + N_SIMD_WITH_SCALAR_TAIL + ")"
        );
    }

    /**
     * Runs {@code whereExpr} over the {@code b} fixture built by
     * {@link #testBooleanExpressionNestedInComparison()} in every execution mode the filter has -
     * the Java filter, {@code JIT_MODE_FORCE_SCALAR}, {@code JIT_MODE_ENABLED}, and the serial
     * (non-parallel) filter - and asserts they all select the same rows.
     * <p>
     * {@code expectedRows} is not redundant with the parity
     * {@link #assertJitMatchesJavaInAllModes(CharSequence)} already checks. Parity is an oracle
     * only for a defect that moves ONE path, and this shape family carries two defects that move
     * different ones, so a count both paths got wrong the same way would pass on parity alone. It
     * also supplies the non-empty guard that helper does not carry: several of these predicates are
     * tautologies or contradictions, and this states which, so a fixture that stops producing rows
     * cannot turn the whole battery vacuous.
     * <p>
     * The serial arm runs the JAVA filter whatever the JIT mode says -
     * {@code SqlCodeGenerator#generateFilter} only reaches the JIT behind
     * {@code executionContext.isParallelFilterEnabled()} - so it pins the oracle itself rather than
     * a fourth backend, and asserts that no compiled filter is in play there.
     */
    private void assertBooleanFilterInAllModes(String whereExpr, long expectedRows) throws SqlException {
        assertJitMatchesJavaInAllModes("b WHERE " + whereExpr);

        final String countQuery = "SELECT count() FROM b WHERE " + whereExpr;
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        assertBooleanFilterRowCount(countQuery, SqlJitMode.JIT_MODE_DISABLED, expectedRows);
        // The two JIT modes go through assertJitCountQuery, which walks the same count cursor and
        // additionally asserts usesCompiledFilter(). Without that the count() query's JIT usage
        // sits unpinned: count() takes a different code path from the row-returning form
        // assertJitMatchesJavaInAllModes covers, so a decline that only moved the count path would
        // leave the whole battery green on parity and on the absolute counts alike.
        assertJitCountQuery(countQuery, expectedRows);

        sqlExecutionContext.setParallelFilterEnabled(false);
        try {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try (RecordCursorFactory factory = select("b WHERE " + whereExpr)) {
                Assert.assertFalse(
                        "serial filter is expected to run the Java filter for: " + whereExpr,
                        factory.usesCompiledFilter()
                );
            }
            assertBooleanFilterRowCount(countQuery, SqlJitMode.JIT_MODE_ENABLED, expectedRows);
        } finally {
            sqlExecutionContext.setParallelFilterEnabled(true);
        }
    }

    private void assertBooleanFilterRowCount(String countQuery, int jitMode, long expectedRows) throws SqlException {
        try (RecordCursorFactory factory = select(countQuery)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(countQuery, cursor.hasNext());
                Assert.assertEquals(
                        "row count mismatch at jitMode=" + jitMode + " for: " + countQuery,
                        expectedRows,
                        cursor.getRecord().getLong(0)
                );
            }
        }
    }

    /**
     * Runs a {@code predicate} whose comparison reads a column over the {@code x} fixture built by
     * {@link #testColumnFreeComparisonDeclinesCompiledFilter()} and asserts that it still compiles
     * a filter, in both JIT modes and on both the row and the count path.
     * <p>
     * {@code expectedRows} is the guard {@link #assertJitMatchesJavaInAllModes(CharSequence)} does
     * not carry: parity between two engines that both return nothing is no oracle at all, so the
     * absolute count states which proper non-empty subset of the 35-row fixture the shape selects.
     */
    private void assertColumnComparisonCompiles(String predicate, long expectedRows) throws SqlException {
        assertJitMatchesJavaInAllModes("x WHERE " + predicate);
        assertJitCountQuery("SELECT count() FROM x WHERE " + predicate, expectedRows);
    }

    /**
     * Runs a {@code predicate} whose comparison reads NO column over the {@code x} fixture built by
     * {@link #testColumnFreeComparisonDeclinesCompiledFilter()} and asserts that the JIT declines
     * it in every execution mode, and that the Java filter selects {@code expectedRows}.
     * <p>
     * The decline is the assertion the defect breaks: the serializer used to compile these shapes
     * and, for every type but CHAR, answer with the COMPLEMENT of the Java row set. Parity alone
     * cannot pin that once the shape declines - both runs are then the same Java filter - so
     * {@code expectJit == false} is what keeps the site live, and {@code expectedRows} keeps it
     * from going vacuous if the fixture ever stops discriminating. Every count passed here is a
     * proper non-empty subset of the fixture's 35 rows.
     */
    private void assertColumnFreeComparisonDeclines(String predicate, long expectedRows) throws SqlException {
        final String rowQuery = "x WHERE " + predicate;
        final String countQuery = "SELECT count() FROM x WHERE " + predicate;
        // JIT_MODE_DISABLED versus JIT_MODE_ENABLED, plus the non-empty guard.
        assertJitMatchesJava(rowQuery, false);
        // count() builds a different factory, so the decline has to hold there too - and this is
        // where the absolute row count gets pinned.
        assertJitMatchesJava(countQuery, false, "count\n" + expectedRows + "\n");
        // JIT_MODE_FORCE_SCALAR is the third execution mode. Both backends must decline, not only
        // the vectorized one: they were wrong in the same direction, which is why parity between
        // them never flagged the shape.
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        try (RecordCursorFactory factory = select(rowQuery)) {
            Assert.assertFalse("compiled filter is expected to decline for: " + rowQuery, factory.usesCompiledFilter());
        }
        try (RecordCursorFactory factory = select(countQuery)) {
            Assert.assertFalse("compiled filter is expected to decline for: " + countQuery, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(countQuery, cursor.hasNext());
                Assert.assertEquals(
                        "[scalar mode] count mismatch for query: " + countQuery,
                        expectedRows,
                        cursor.getRecord().getLong(0)
                );
            }
        }
    }

    private void assertJitMatchesJavaInAllModes(CharSequence query) throws SqlException {
        StringSink javaSink = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
            }
        }

        for (int jitMode : new int[]{SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
            StringSink jitSink = new StringSink();
            sqlExecutionContext.setJitMode(jitMode);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("JIT was disabled for query: " + query, factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), jitSink);
                }
            }
            TestUtils.assertEquals(
                    (jitMode == SqlJitMode.JIT_MODE_FORCE_SCALAR ? "[scalar mode] " : "[vectorized mode] ")
                            + "JIT vs Java result mismatch for query: " + query,
                    javaSink,
                    jitSink
            );
        }
    }

    /**
     * Runs {@code query} with JIT disabled and with JIT enabled and asserts the
     * two cursors produce identical output. {@code expectJit} pins whether the
     * JIT-enabled run is expected to compile a filter (true) or fall back to the
     * Java filter (false), guarding against both the divergence and over-eager
     * fallback.
     * <p>
     * Parity is only an oracle for a bug that moves ONE of the two paths. A test
     * covering a width decision the Java filter and the JIT now share must also
     * pin the absolute result - via the {@code expected} overload, or a
     * {@link #runQuery} row-count assertion when the rows come from {@code rnd_*}
     * and no expected string can be written. Otherwise a wrong shared decision
     * agrees on both paths and the assertion passes.
     */
    private void assertJitMatchesJava(CharSequence query, boolean expectJit) throws SqlException {
        assertJitMatchesJava(query, expectJit, null, false);
    }

    /**
     * Companion to {@link #assertJitMatchesJava(CharSequence, boolean)} for the shapes whose CORRECT
     * answer is no rows - a constant fold that collapses onto the NULL sentinel, or a widened
     * constant that matches nothing. Parity alone cannot vouch for those, so this PINS the empty
     * result rather than merely tolerating it: a fixture that silently starts matching, or a bug
     * both engines share that starts returning rows, has to redden a test somewhere.
     */
    private void assertJitMatchesJavaOnEmptyResult(CharSequence query, boolean expectJit) throws SqlException {
        final int rows = assertJitMatchesJava(query, expectJit, null, true);
        Assert.assertEquals("query is expected to return no rows: " + query, 0, rows);
    }

    /**
     * Variant for the batch-length sweeps, which run one query over row counts 0..9 so that the
     * SIMD body, its scalar tail and the empty table are all covered. Emptiness there is a property
     * of the ITERATION, not of the query, so neither the non-empty guard nor the empty-result
     * assertion fits a single call. The sweep instead accumulates what its iterations returned and
     * {@link #assertBatchSweepReturnedRows} checks at the end that the shape matched something -
     * otherwise a fixture that stopped matching would leave every iteration trivially in parity.
     */
    private void assertJitMatchesJavaOnBatchLengths(CharSequence query, boolean expectJit) throws SqlException {
        batchSweepRows += assertJitMatchesJava(query, expectJit, null, true);
    }

    private void assertBatchSweepReturnedRows() {
        Assert.assertTrue("no iteration of the batch-length sweep returned rows", batchSweepRows > 0);
        batchSweepRows = 0;
    }

    // Runs the query with JIT off then on, asserts the two agree and that JIT usage matches
    // expectJit. When expected is non-null it also pins the absolute result: parity alone would
    // pass even if BOTH paths shared the same bug, so the expected rows lock the actual result
    // the divergence was about.
    //
    // Demands rows, exactly like the two-arg form. A header-only expected pins "no rows" without
    // saying so anywhere the reader can see it, so a site that MEANT to observe rows - and whose
    // fixture cannot produce any - reads as coverage while observing nothing. A site whose correct
    // answer IS no rows says so through assertJitMatchesJavaOnEmptyResult.
    private void assertJitMatchesJava(CharSequence query, boolean expectJit, CharSequence expected) throws SqlException {
        assertJitMatchesJava(query, expectJit, expected, false);
    }

    /**
     * Companion to {@link #assertJitMatchesJava(CharSequence, boolean, CharSequence)} for the
     * shapes whose CORRECT answer is no rows - a bound that collapses onto a NULL sentinel, a
     * constant no column value can equal, an operator whose complement takes the whole table.
     * Those cannot pass the non-empty guard, and forcing them through it would mean redesigning
     * the fixture rather than fixing anything, so this DECLARES the empty answer instead and PINS
     * it: a fixture that silently starts matching, or a defect that starts adding rows, still has
     * to redden a test.
     * <p>
     * {@code expected} stays mandatory and is still compared in full - it names the columns the
     * cursor prints, so a projection change fails here rather than passing on a shorter header.
     */
    private void assertJitMatchesJavaOnEmptyResult(CharSequence query, boolean expectJit, CharSequence expected) throws SqlException {
        final int rows = assertJitMatchesJava(query, expectJit, expected, true);
        Assert.assertEquals("query is expected to return no rows: " + query, 0, rows);
    }

    private int assertJitMatchesJava(CharSequence query, boolean expectJit, CharSequence expected, boolean isEmptyAllowed) throws SqlException {
        StringSink javaSink = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
            }
        }

        StringSink jit = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals("unexpected compiled-filter usage for query: " + query,
                    expectJit, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), jit);
            }
        }
        TestUtils.assertEquals("JIT vs Java result mismatch for query: " + query, javaSink, jit);
        if (expected != null) {
            TestUtils.assertEquals("absolute result mismatch for query: " + query, expected, javaSink);
        }
        final int rows = countPrintedRows(javaSink);
        if (!isEmptyAllowed) {
            // Parity on its own is not an oracle: a query returning nothing on BOTH engines agrees
            // trivially, so such a site pins nothing at all. Demand rows, as assertQuery does. This
            // holds for the expected-result form too - a header-only expected is the same vacuous
            // site with the emptiness spelled out in a string literal instead of left implicit. A
            // site whose correct answer IS no rows says so through assertJitMatchesJavaOnEmptyResult.
            Assert.assertTrue("query is expected to return rows: " + query, rows > 0);
        }
        return rows;
    }

    /**
     * Pins the execution mode the JIT frontend picks for {@code whereExpr} over {@code tableName}.
     * {@link io.questdb.jit.CompiledFilter} takes the options as an int and keeps no record of
     * them, and no plan or cursor surfaces the hint, so this re-runs the serializer over the
     * factory's own metadata and page frame cursor and reads bits 4-5 of the options it returns.
     * <p>
     * It is a REPLICA of {@code SqlCodeGenerator}, not a call into it. It serializes the RAW parsed
     * {@code whereExpr}, whereas the code generator serializes {@code model.getWhereClause()} - the
     * residual {@code WhereClauseParser} leaves after lifting extractable intrinsics out of the
     * filter ({@code SqlCodeGenerator.java:11987} assigns it, {@code :4555} reads it back). So a
     * {@code whereExpr} carrying an extractable intrinsic reports a hint for a filter production
     * never compiles. Measured on {@code m3}:
     * {@code k > '1970-01-01' and l > -(1000 * 1000) and i > 16777216.0} reports SCALAR here, while
     * the plan shows the interval scan taking {@code k} and leaving the code generator the residual
     * {@code l > -(1000 * 1000) and i > 16777216.0}, which is WIDE_LANE.
     * <p>
     * {@link #assertNoExtractableIntrinsic} therefore REJECTS such a predicate rather than
     * answering for it. It screens for the two removals a RAW predicate can be tested for
     * syntactically. The first is a LIFT: a predicate over the designated timestamp becomes an
     * interval scan, and one over an INDEXED column becomes a key scan
     * ({@code WhereClauseParser#isColumnPreferredOrIndexedAndKeyColumnAllowed}). Naming either
     * column is the necessary condition for that lift, so the guard rejects on the NAME and errs
     * towards rejecting: a predicate the parser would have left alone - {@code k * 2 > 5}, say - is
     * turned away too.
     * <p>
     * The second is a COLLAPSE, and it is not confined to those columns. {@code nodesEqual} folds a
     * comparison of a column or constant against an identically spelled one to an intrinsic TRUE or
     * FALSE on ANY column - in {@code analyzeEquals0}, {@code analyzeGreater}, {@code analyzeLess}
     * and {@code analyzeNotEquals0} alike - and the code generator then compiles the residual.
     * Measured on {@code m3}: {@code i = i and l > 5} reports EXEC_HINT_MIXED_SIZE_TYPE here, while
     * production compiles {@code 5 < l} and answers EXEC_HINT_SINGLE_SIZE_TYPE.
     * <p>
     * Those two are what the guard COVERS. They are not a proof that the parser leaves everything
     * else in place: it is a large walk, and a shape it starts folding tomorrow would go unnoticed
     * here. When the guard turns a predicate away, pin the shape over a column the table does not
     * carry as its timestamp or index, or assert the rows with
     * {@link #assertJitScalarAndVectorMatchJava}, which runs production's own compile.
     * <p>
     * The cursor order is production's: the serializer reads the page frame cursor only through
     * {@code getSymbolTable()} for symbol-constant lookups, so the order cannot reach the hint at
     * all, but matching {@code SqlCodeGenerator.java:4649} keeps the replica one thing shorter.
     */
    private void assertExecHint(CharSequence tableName, CharSequence whereExpr, int expectedHint) throws SqlException {
        final ObjList<Function> bindVarFunctions = new ObjList<>();
        final MemoryCARW irMemory = Vm.getCARWInstance(2048, 1, MemoryTag.NATIVE_JIT);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = select("SELECT * FROM " + tableName)
        ) {
            queryModel.clear();
            final ExpressionNode filter = compiler.testParseExpression(whereExpr, queryModel);
            assertNoExtractableIntrinsic(filter, factory.getMetadata(), whereExpr);
            Assert.assertTrue("page frames for: " + tableName, factory.supportsPageFrameCursor());
            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ANY)) {
                final int options = new CompiledFilterIRSerializer()
                        .of(irMemory, sqlExecutionContext, factory.getMetadata(), cursor, bindVarFunctions)
                        .serialize(filter, false, false, true);
                Assert.assertEquals("exec hint for: " + whereExpr, expectedHint, (options >> 4) & 0b11);
            }
        } finally {
            Misc.freeObjList(bindVarFunctions);
            irMemory.close();
        }
    }

    /**
     * Fails the calling {@link #assertExecHint} when {@code filter} carries something
     * {@code WhereClauseParser} takes out of it - a column it lifts into an interval or key scan,
     * or a self-comparison it collapses to an intrinsic value - because production would then
     * compile a SMALLER tree than the helper serializes and the hint the helper reports would be
     * for a filter that never runs. See {@link #assertExecHint} for what these two checks cover,
     * what they deliberately over-reject, and what they do not cover at all.
     */
    private void assertNoExtractableIntrinsic(ExpressionNode filter, RecordMetadata metadata, CharSequence whereExpr) {
        final ObjList<ExpressionNode> stack = new ObjList<>();
        stack.add(filter);
        while (stack.size() > 0) {
            final ExpressionNode node = stack.getQuick(stack.size() - 1);
            stack.remove(stack.size() - 1);
            if (node == null) {
                continue;
            }
            Assert.assertFalse(
                    "assertExecHint cannot answer for a self-comparison - WhereClauseParser collapses"
                            + " it to an intrinsic value and production compiles the residual: " + whereExpr,
                    isSelfComparison(node)
            );
            if (node.type == ExpressionNode.LITERAL) {
                final int columnIndex = metadata.getColumnIndexQuiet(node.token);
                if (columnIndex > -1) {
                    Assert.assertFalse(
                            "assertExecHint cannot answer for the designated timestamp - WhereClauseParser"
                                    + " lifts it into the interval scan and production compiles the residual: " + whereExpr,
                            columnIndex == metadata.getTimestampIndex()
                    );
                    Assert.assertFalse(
                            "assertExecHint cannot answer for an indexed column - WhereClauseParser lifts it"
                                    + " into a key scan and production compiles the residual: " + whereExpr,
                            metadata.isColumnIndexed(columnIndex)
                    );
                }
            }
            stack.add(node.lhs);
            stack.add(node.rhs);
            for (int i = 0, n = node.args.size(); i < n; i++) {
                stack.add(node.args.getQuick(i));
            }
        }
    }

    // Runs the query with JIT off, then in FORCE_SCALAR mode, then vectorized, and asserts all
    // three agree with the expected rows. assertJitMatchesJava exercises the vectorized backend
    // only, so a divergence living in the scalar backend (jit/impl/x86.h) rather than the
    // four-lane one (jit/impl/avx2.h) would pass it unnoticed.
    //
    // Demands rows, as assertQuery does: an expected result that is a header line only makes the
    // three engines agree on nothing at all, so the site would read as coverage while pinning
    // none. A site whose correct answer IS no rows says so through
    // assertJitScalarAndVectorMatchJavaOnEmptyResult.
    private void assertJitScalarAndVectorMatchJava(CharSequence query, CharSequence expected) throws SqlException {
        assertJitScalarAndVectorMatchJava(query, expected, false);
    }

    /**
     * Companion to {@link #assertJitScalarAndVectorMatchJava(CharSequence, CharSequence)} for the
     * shapes whose CORRECT answer is no rows - a bound that collapses onto a NULL sentinel, a
     * widened constant no column value can equal, an operator whose complement takes the whole
     * table. Those cannot pass the non-empty guard, and forcing them through it would mean
     * redesigning the fixture rather than fixing anything, so this DECLARES the empty answer
     * instead: it pins the row count at zero, so a fixture that silently starts matching, or a
     * defect that starts adding rows, still has to redden a test.
     * <p>
     * {@code expected} stays mandatory and is still compared in full - it names the columns the
     * cursor prints, so a projection change fails here rather than passing on a shorter header.
     */
    private void assertJitScalarAndVectorMatchJavaOnEmptyResult(CharSequence query, CharSequence expected) throws SqlException {
        assertJitScalarAndVectorMatchJava(query, expected, true);
    }

    private void assertJitScalarAndVectorMatchJava(
            CharSequence query,
            CharSequence expected,
            boolean isEmptyRequired
    ) throws SqlException {
        final int callerJitMode = sqlExecutionContext.getJitMode();
        try {
            StringSink javaSink = new StringSink();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
                }
            }
            TestUtils.assertEquals("absolute result mismatch for query: " + query, expected, javaSink);
            final int rows = countPrintedRows(javaSink);
            if (isEmptyRequired) {
                Assert.assertEquals("query is expected to return no rows: " + query, 0, rows);
            } else {
                Assert.assertTrue("query is expected to return rows: " + query, rows > 0);
            }

            final int[] jitModes = {SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED};
            for (int i = 0; i < jitModes.length; i++) {
                final StringSink jit = new StringSink();
                sqlExecutionContext.setJitMode(jitModes[i]);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), jit);
                    }
                }
                TestUtils.assertEquals(
                        "JIT vs Java result mismatch [scalarMode=" + (i == 0) + "] for query: " + query,
                        javaSink,
                        jit
                );
            }
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    /**
     * Pins a query whose Java and compiled filters DISAGREE because they compare at DIFFERENT
     * TOLERANCES, not because they read the same tolerance differently. {@code FLOAT_EPSILON} is
     * {@code (float) DOUBLE_TOLERANCE} (consts.h), a shade larger than the {@code 1e-10} the Java
     * filter uses, so a shape that runs the compiled filter's f32 arm - an INT leaf against a
     * fractional bound, via {@code serializeNumber}'s I4 arm - answers differently for any value
     * that lands between the two. Both answers are recorded, so the divergence is visible rather
     * than merely absent from the suite.
     * <p>
     * This is separate from, and untouched by, the inclusive-vs-strict question: the native
     * comparators now read their epsilon inclusively and agree with {@code Numbers.equals} wherever
     * the two tolerances coincide (see
     * {@link #testNarrowIntColumnVsExactToleranceBoundConstant}).
     * <p>
     * The method also insists the two answers really differ, so the pin cannot outlive the
     * limitation: narrowing {@code FLOAT_EPSILON} to the f64 tolerance, or declining JIT compilation
     * for the shape, reddens every site here and forces the expectations to be revisited instead of
     * leaving a stale record of a divergence that no longer exists.
     * <p>
     * FORCE_SCALAR and the vectorized mode are asserted separately, since the two backends carry
     * their own copy of the comparator ({@code jit/impl/x86.h} and {@code jit/impl/avx2.h}) and a
     * divergence could in principle live in one and not the other.
     */
    private void assertJitDivergesFromJavaAtF32ToleranceBound(
            CharSequence query,
            CharSequence javaExpected,
            CharSequence jitExpected
    ) throws SqlException {
        Assert.assertFalse(
                "site claims a divergence but pins the same rows for both filters: " + query,
                Chars.equals(javaExpected, jitExpected)
        );
        final int callerJitMode = sqlExecutionContext.getJitMode();
        try {
            final StringSink javaSink = new StringSink();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
                }
            }
            TestUtils.assertEquals("Java filter result mismatch for query: " + query, javaExpected, javaSink);

            final int[] jitModes = {SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED};
            for (int i = 0; i < jitModes.length; i++) {
                final StringSink jit = new StringSink();
                sqlExecutionContext.setJitMode(jitModes[i]);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), jit);
                    }
                }
                TestUtils.assertEquals(
                        "compiled filter result mismatch [scalarMode=" + (i == 0) + "] for query: " + query,
                        jitExpected,
                        jit
                );
            }
        } finally {
            sqlExecutionContext.setJitMode(callerJitMode);
        }
    }

    private void assertJitCountQuery(CharSequence countQuery, long expectedCount) throws SqlException {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        long actualCount = runJitCountQuery(countQuery);
        Assert.assertEquals("[scalar mode] count mismatch for query: " + countQuery, expectedCount, actualCount);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        long vectorCount = runJitCountQuery(countQuery);
        Assert.assertEquals("[vectorized mode] count mismatch for query: " + countQuery, expectedCount, vectorCount);
    }

    private void assertJitQuery(CharSequence query, boolean notNull) throws SqlException {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        runJitQuery(query);
        TestUtils.assertEquals("[scalar mode] result mismatch for query: " + query, sink, jitSink);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runJitQuery(query);
        TestUtils.assertEquals("[vectorized mode] result mismatch for query: " + query, sink, jitSink);

        // At the moment, there is no way for users to disable null checks in the
        // JIT compiler output. Yet, we want to test this part of the compiler.
        if (notNull) {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
            runJitQuery(query);
            TestUtils.assertEquals("[scalar mode, not null] result mismatch for query: " + query, sink, jitSink);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            runJitQuery(query);
            TestUtils.assertEquals("[vectorized mode, not null] result mismatch for query: " + query, sink, jitSink);
        }
    }

    private void assertQuery(CharSequence query, CharSequence ddl, boolean notNull) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            long count = runQuery(query);
            Assert.assertTrue("query is expected to return rows", count > 0);

            assertJitQuery(query, notNull);
            assertJitCountQuery("select count() from " + query, count);
        });
    }

    /**
     * No-leak-check counterpart of {@link #assertQueryNullable} for the shapes whose CORRECT
     * answer is no rows - an INT-width constant fold that wraps onto a bound no column value can
     * exceed. A helper that only cross-checks the two engines against each other asserts nothing
     * at all about the row count, so a regression both engines share - a partial revert of the
     * INT-width fold that restores the un-wrapped LONG bound on the JIT and the Java filter alike -
     * leaves it trivially in parity and green. This pins the empty result on top of the same
     * scalar/vectorized parity and count() cross-checks.
     */
    private void assertQueryEmptyNoLeakCheck(CharSequence query) throws SqlException {
        long count = runQuery(query);
        Assert.assertEquals("query is expected to return no rows: " + query, 0, count);
        assertJitQuery(query, true);
        assertJitCountQuery("select count() from " + query, count);
    }

    private void assertQueryNotNull(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, false);
    }

    private void assertQueryNotNullNoCount(CharSequence query, CharSequence ddl) throws Exception {
        assertMemoryLeak(() -> {
            if (ddl != null) {
                execute(ddl);
            }

            long count = runQuery(query);
            Assert.assertTrue("query is expected to return rows", count > 0);

            assertJitQuery(query, false);
        });
    }

    /**
     * Same checks as {@link #assertQueryNotNull} but without a surrounding
     * {@code assertMemoryLeak} - call this from inside a single
     * {@code assertMemoryLeak} when running multiple queries against shared
     * DDL.
     */
    private void assertQueryNotNullNoLeakCheck(CharSequence query) throws SqlException {
        long count = runQuery(query);
        Assert.assertTrue("query is expected to return rows", count > 0);
        assertJitQuery(query, false);
        assertJitCountQuery("select count() from " + query, count);
    }

    private void assertQueryNullable(CharSequence query, CharSequence ddl) throws Exception {
        assertQuery(query, ddl, true);
    }

    // Rows in a CursorPrinter.println() dump, which always emits one header line first.
    private static int countPrintedRows(CharSequence printed) {
        int lines = 0;
        for (int i = 0, n = printed.length(); i < n; i++) {
            if (printed.charAt(i) == '\n') {
                lines++;
            }
        }
        return Math.max(0, lines - 1);
    }

    private long runJitCountQuery(CharSequence countQuery) throws SqlException {
        try (RecordCursorFactory factory = select(countQuery)) {
            Assert.assertTrue("JIT was not enabled for query: " + countQuery, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private void runJitQuery(CharSequence query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                CursorPrinter.println(cursor, metadata, jitSink);
            }
        }
    }

    private String runJavaToString(CharSequence query) throws SqlException {
        StringSink javaSink = new StringSink();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                CursorPrinter.println(cursor, factory.getMetadata(), javaSink);
            }
        }
        return javaSink.toString();
    }

    private long runQuery(CharSequence query) throws SqlException {
        long resultSize;
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertFalse("JIT was enabled for query: " + query, factory.usesCompiledFilter());
            try (CountingRecordCursor cursor = new CountingRecordCursor(factory.getCursor(sqlExecutionContext))) {
                println(factory, cursor);
                resultSize = cursor.count();
            }
        }
        return resultSize;
    }

    private void testOrderBy(String orderByClause) throws Exception {
        final String query = "x where price > 0 " + orderByClause;
        final String ddl = "create table x as " +
                "(select rnd_symbol('ABB','HBC','DXR') sym, \n" +
                " rnd_double() price, \n" +
                " timestamp_sequence(172_800_000_000, 360_000_000) ts \n" +
                "from long_sequence(" + N_SIMD_WITH_SCALAR_TAIL + ")) timestamp (ts)";
        assertQueryNotNullNoCount(query, ddl);
    }

    private static class CountingRecordCursor implements RecordCursor {

        private final RecordCursor delegate;
        private long count;

        public CountingRecordCursor(RecordCursor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            delegate.close();
        }

        public long count() {
            return count;
        }

        @Override
        public Record getRecord() {
            return delegate.getRecord();
        }

        @Override
        public Record getRecordB() {
            return delegate.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = delegate.hasNext();
            if (hasNext) {
                count++;
            }
            return hasNext;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return delegate.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            delegate.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
    }

    private static class FilterGenerator {

        private static final String[] ARITHMETIC_OPERATORS = new String[]{" + ", " - ", " * ", " / "};
        private static final String[] BOOLEAN_OPERATORS = new String[]{" and ", " or "};
        private static final String[] COMPARISON_OPERATORS = new String[]{" = ", " != ", " > ", " >= ", " < ", " <= "};
        private static final String[] EQUALITY_OPERATORS = new String[]{" = ", " != ", " <> "};
        private static final String[] OPTIONAL_NEGATION = new String[]{"", "-"};
        private static final String[] OPTIONAL_NOT = new String[]{"", " not "};
        private final List<String[]> filterParts = new ArrayList<>();

        /**
         * Generates a simple cartesian product of the given filter expression parts.
         * <p>
         * The algorithm originates from Generating All n-tuple, of The Art Of Computer
         * Programming by Knuth.
         */
        public List<String> generate() {
            if (filterParts.isEmpty()) {
                return Collections.emptyList();
            }

            int combinations = 1;
            for (String[] parts : filterParts) {
                combinations *= parts.length;
            }

            final List<String> filters = new ArrayList<>();
            final StringBuilder sb = new StringBuilder();

            for (int i = 0; i < combinations; i++) {
                int j = 1;
                for (String[] parts : filterParts) {
                    sb.append(parts[(i / j) % parts.length]);
                    j *= parts.length;
                }
                filters.add(sb.toString());
                sb.setLength(0);
            }
            return filters;
        }

        public FilterGenerator withAnyOf(String... parts) {
            filterParts.add(parts);
            return this;
        }

        public FilterGenerator withArithmeticOperator() {
            filterParts.add(ARITHMETIC_OPERATORS);
            return this;
        }

        public FilterGenerator withBooleanOperator() {
            filterParts.add(BOOLEAN_OPERATORS);
            return this;
        }

        public FilterGenerator withComparisonOperator() {
            filterParts.add(COMPARISON_OPERATORS);
            return this;
        }

        public FilterGenerator withEqualityOperator() {
            filterParts.add(EQUALITY_OPERATORS);
            return this;
        }

        public FilterGenerator withOptionalNegation() {
            filterParts.add(OPTIONAL_NEGATION);
            return this;
        }

        public FilterGenerator withOptionalNot() {
            filterParts.add(OPTIONAL_NOT);
            return this;
        }
    }
}
