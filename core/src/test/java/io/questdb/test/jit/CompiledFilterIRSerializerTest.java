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

package io.questdb.test.jit;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.jit.CompiledFilterIRSerializer.*;

public class CompiledFilterIRSerializerTest extends BaseFunctionFactoryTest {
    // Byte width of one instruction_t - see putIrInstruction() for the layout. READ from
    // CompiledFilterIRSerializer's INSTRUCTION_SIZE rather than re-spelled here: it is private, but
    // io.questdb is an open module, so the same reflection assertUnharmonisedWidthWalk() and
    // assertSerializerFlag() already use reaches it. A hand-kept copy silently keeps asserting the
    // old layout when the production constant moves; this way the class fails at init instead.
    private static final int IR_INSTRUCTION_SIZE = serializerInt("INSTRUCTION_SIZE");
    // The stub opcode serializeConstantStub() and the symbol bind-variable path write for a value
    // they backfill later - common.h spells the same number opcodes::Inv. Package-private in
    // production, so the same reflection IR_INSTRUCTION_SIZE uses reaches it.
    private static final int IR_UNDEFINED_CODE = serializerInt("UNDEFINED_CODE");
    private static final String KNOWN_SYMBOL_1 = "ABC";
    private static final String KNOWN_SYMBOL_2 = "DEF";
    private static final String UNKNOWN_SYMBOL = "XYZ";

    private static ObjList<Function> bindVarFunctions;
    private static MemoryCARW irMemory;
    private static CompiledFilterIRSerializer serializer;

    private RecordCursorFactory factory;
    private RecordMetadata metadata;

    @BeforeClass
    public static void setUpStatic2() {
        bindVarFunctions = new ObjList<>();
        irMemory = Vm.getCARWInstance(2048, 1, MemoryTag.NATIVE_JIT);
        serializer = new CompiledFilterIRSerializer();
    }

    @AfterClass
    public static void tearDownStatic2() {
        irMemory.close();
    }

    @Before
    public void setUp2() throws SqlException {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE);
        model.col("aboolean", ColumnType.BOOLEAN)
                .col("abyte", ColumnType.BYTE)
                .col("ageobyte", ColumnType.GEOBYTE)
                .col("ashort", ColumnType.SHORT)
                .col("ageoshort", ColumnType.GEOSHORT)
                .col("achar", ColumnType.CHAR)
                .col("anint", ColumnType.INT)
                .col("anipv4", ColumnType.IPv4)
                .col("ageoint", ColumnType.GEOINT)
                .col("asymbol", ColumnType.SYMBOL)
                .col("anothersymbol", ColumnType.SYMBOL)
                .col("afloat", ColumnType.FLOAT)
                .col("along", ColumnType.LONG)
                .col("ageolong", ColumnType.GEOLONG)
                .col("adate", ColumnType.DATE)
                .col("atimestamp", ColumnType.TIMESTAMP)
                .col("atimestampns", ColumnType.TIMESTAMP_NANO)
                .col("adouble", ColumnType.DOUBLE)
                .col("astring", ColumnType.STRING)
                .col("astring2", ColumnType.STRING)
                .col("avarchar", ColumnType.VARCHAR)
                .col("avarchar2", ColumnType.VARCHAR)
                .col("abinary", ColumnType.BINARY)
                .col("abinary2", ColumnType.BINARY)
                .col("auuid", ColumnType.UUID)
                .col("along128", ColumnType.LONG128)
                .col("along256", ColumnType.LONG256)
                .timestamp();
        AbstractCairoTest.create(model);

        try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
            TableWriter.Row row = writer.newRow();
            row.putSym(writer.getColumnIndex("asymbol"), KNOWN_SYMBOL_1);
            row.putSym(writer.getColumnIndex("anothersymbol"), KNOWN_SYMBOL_2);
            row.append();
            writer.commit();
        }

        factory = select("select * from x");
        Assert.assertTrue(factory.supportsPageFrameCursor());
        metadata = factory.getMetadata();
    }

    @After
    public void tearDown2() {
        factory.close();
    }

    @Test
    public void testAndChainShortCircuit() throws Exception {
        // Pure AND chain with mixed sizes -> short-circuit with predicate reordering
        serialize("along = 1 and anint = 2");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i32 2L)(i32 anint)(=)(ret)");

        serialize("along = 1 and anint = 2 and ashort = 3");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i32 2L)(i32 anint)(=)(&&_sc)(i16 3L)(i16 ashort)(=)(ret)");

        // With NOT operator
        serialize("along = 1 and not anint = 2");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i32 2L)(i32 anint)(=)(!)(ret)");

        // With arithmetic
        serialize("along + 1 > 0 and anint - 2 < 10");
        assertIR("(i64 0L)(i64 1L)(i64 along)(+)(>)(&&_sc)(i32 10L)(i32 2L)(i32 anint)(-)(<)(ret)");
    }

    @Test
    public void testAndChainShortCircuitAllPriorities() throws Exception {
        // AND chain covering all 11 priority levels (0-10)
        // Predicates are sorted by ascending priority (lower value = evaluated first)
        // Priority order: i128= < i64= < i32= < sym= < other= < other_cmp < other!= < sym!= < i32!= < i64!= < i128!=
        serialize(
                "auuid = '11111111-1111-1111-1111-111111111111' " + // priority 0: i128 eq
                        "and along = 1 " + // priority 1: i64 eq
                        "and anint = 2 " + // priority 2: i32 eq
                        "and asymbol = 'ABC' " + // priority 3: sym eq
                        "and ashort = 3 " + // priority 4: other eq (i16 is "other")
                        "and abyte > 0 " + // priority 5: other comparison (non-eq/neq)
                        "and achar != 'x' " + // priority 6: other neq
                        "and anothersymbol != 'DEF' " + // priority 7: sym neq
                        "and ageoint != #sp05 " + // priority 8: i32 neq
                        "and adate != '1980-01-01' " + // priority 9: i64 neq
                        "and auuid != '22222222-2222-2222-2222-222222222222'" // priority 10: i128 neq
        );
        // Expected order: priority 0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10
        assertIR(
                "(i128 1229782938247303441 1229782938247303441L)(i128 auuid)(=)(&&_sc)" + // priority 0: auuid =
                        "(i64 1L)(i64 along)(=)(&&_sc)" + // priority 1: along =
                        "(i32 2L)(i32 anint)(=)(&&_sc)" + // priority 2: anint =
                        "(i32 0L)(i32 asymbol)(=)(&&_sc)" + // priority 3: asymbol = (key 0 for 'ABC')
                        "(i16 3L)(i16 ashort)(=)(&&_sc)" + // priority 4: ashort =
                        "(i8 0L)(i8 abyte)(>)(&&_sc)" + // priority 5: abyte >
                        "(i16 120L)(i16 achar)(<>)(&&_sc)" + // priority 6: achar != ('x' = 120)
                        "(i32 0L)(i32 anothersymbol)(<>)(&&_sc)" + // priority 7: anothersymbol != (key 0 for 'DEF')
                        "(i32 807941L)(i32 ageoint)(<>)(&&_sc)" + // priority 8: ageoint !=
                        "(i64 315532800000L)(i64 adate)(<>)(&&_sc)" + // priority 9: adate !=
                        "(i128 2459565876494606882 2459565876494606882L)(i128 auuid)(<>)(ret)" // priority 10: auuid !=
        );
    }

    @Test
    public void testAndChainShortCircuitSamePriorityOrder() throws Exception {
        // Predicates with the same priority (same type size) should preserve their original order
        // Here along and adate are both i64, anint and ageoint are both i32
        serialize("along = 1 and adate = '1980-01-01' and anint = 3");
        // anint (i32) comes first due to smaller size, then along and adate in original order
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i64 315532800000L)(i64 adate)(=)(&&_sc)(i32 3L)(i32 anint)(=)(ret)");

        serialize("adate = '1980-01-01' and along = 2 and anipv4 = null and anint = 4");
        // i64 columns (adate, along) in original order, then i32 columns (anipv4, anint) first in original order
        assertIR("(i64 315532800000L)(i64 adate)(=)(&&_sc)(i64 2L)(i64 along)(=)(&&_sc)(i32 0L)(i32 anipv4)(=)(&&_sc)(i32 4L)(i32 anint)(=)(ret)");

        // Three predicates of same size - order should be preserved
        serialize("along = 1 and adate = '1980-01-01' and atimestamp = '1980-01-02' and anint = 4");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i64 315532800000L)(i64 adate)(=)(&&_sc)(i64 315619200000000L)(i64 atimestamp)(=)(&&_sc)(i32 4L)(i32 anint)(=)(ret)");
    }

    @Test
    public void testArithmeticOperators() throws Exception {
        for (String op : new String[]{"+", "-", "*", "/"}) {
            serialize("along " + op + " 42 != -1");
            assertIR("(i64 -1L)(i64 42L)(i64 along)(" + op + ")(<>)(ret)");
        }
    }

    @Test
    public void testBinaryNullConstant() throws Exception {
        serialize("abinary <> null");
        assertIR("(i64 -1L)(binary_header abinary)(<>)(ret)");
        serialize("abinary = null");
        assertIR("(i64 -1L)(binary_header abinary)(=)(ret)");
    }

    @Test
    public void testBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setBoolean("aboolean", false);
        bindVariableService.setByte("abyte", (byte) 1);
        bindVariableService.setGeoHash("ageobyte", 0, ColumnType.getGeoHashTypeWithBits(4));
        bindVariableService.setShort("ashort", (short) 243);
        bindVariableService.setGeoHash("ageoshort", 0b011011000010L, ColumnType.getGeoHashTypeWithBits(12));
        bindVariableService.setChar("achar", 'O');
        bindVariableService.setInt("anint", 2085282008);
        bindVariableService.setGeoHash("ageoint", 0b0101011010111101L, ColumnType.getGeoHashTypeWithBits(16));
        bindVariableService.setStr("asymbol", "HYRX");
        bindVariableService.setFloat("afloat", 0.48820507526397705f);
        bindVariableService.setLong("along", -4986232506486815364L);
        bindVariableService.setDouble("adouble", 0.42281342727402726);
        bindVariableService.setDate("adate", 1443479385706L);
        bindVariableService.setGeoHash("ageolong", 0b11010000001110101000110100011010L, ColumnType.getGeoHashTypeWithBits(32));
        bindVariableService.setTimestamp("atimestamp", 400500000000L);
        bindVariableService.setTimestampNano("atimestampns", 400500000000000L);
        bindVariableService.setUuid("auuid", 2085282008, 2085282008);

        serialize(
                "auuid = :auuid" + // i128
                        " or aboolean = :aboolean or abyte = :abyte or ageobyte = :ageobyte" + // i8
                        " or ashort = :ashort or ageoshort = :ageoshort or achar = :achar" + // i16
                        " or anint = :anint or ageoint = :ageoint or asymbol = :asymbol" + // i32
                        " or along = :along or adate = :adate or ageolong = :ageolong or atimestamp = :atimestamp or atimestampns = :atimestampns" + // i64
                        " or afloat = :afloat" + // f32
                        " or adouble = :adouble" // f64
        );
        assertIR(
                "(i8 :0)(i8 aboolean)(=)(||_sc)(i8 :1)(i8 abyte)(=)(||_sc)(i8 :2)(i8 ageobyte)(=)(||_sc)" +
                        "(i16 :3)(i16 ashort)(=)(||_sc)(i16 :4)(i16 ageoshort)(=)(||_sc)(i16 :5)(i16 achar)(=)(||_sc)" +
                        "(f32 :6)(f32 afloat)(=)(||_sc)(f64 :7)(f64 adouble)(=)(||_sc)(i32 :8)(i32 asymbol)(=)(||_sc)" +
                        "(i32 :9)(i32 anint)(=)(||_sc)(i32 :10)(i32 ageoint)(=)(||_sc)(i64 :11)(i64 along)(=)(||_sc)" +
                        "(i64 :12)(i64 adate)(=)(||_sc)(i64 :13)(i64 ageolong)(=)(||_sc)(i64 :14)(i64 atimestamp)(=)(||_sc)" +
                        "(i64 :15)(i64 atimestampns)(=)(||_sc)(i128 :16)(i128 auuid)(=)(ret)"
        );

        Assert.assertEquals(17, bindVarFunctions.size());
    }

    @Test
    public void testBindVariablesMixed() throws Exception {
        bindVariableService.clear();
        bindVariableService.setShort("ashort", (short) 1);
        bindVariableService.setInt("anint", 2);
        bindVariableService.setLong(0, 3);

        serialize("anint = :anint or along = $1 or ashort = :ashort");
        assertIR("(i16 :0)(i16 ashort)(=)(||_sc)(i32 :1)(i32 anint)(=)(||_sc)(i64 :2)(i64 along)(=)(ret)");

        Assert.assertEquals(3, bindVarFunctions.size());
        Assert.assertEquals(ColumnType.SHORT, bindVarFunctions.get(0).getType());
        Assert.assertEquals(ColumnType.INT, bindVarFunctions.get(1).getType());
        Assert.assertEquals(ColumnType.LONG, bindVarFunctions.get(2).getType());
    }

    @Test
    public void testBooleanConstant() throws Exception {
        serialize("aboolean = true or not aboolean = not false");
        assertIR("(i8 0L)(!)(i8 aboolean)(=)(!)(i8 1L)(i8 aboolean)(=)(||)(ret)");
    }

    @Test
    public void testBooleanOperators() throws Exception {
        serialize("anint = 0 and not (abyte = 0) or along = 0");
        assertIR("(i64 0L)(i64 along)(=)(i8 0L)(i8 abyte)(=)(!)(i32 0L)(i32 anint)(=)(&&)(||)(ret)");
    }

    @Test
    public void testBracketsBreakChain() throws Exception {
        // Brackets around sub-expressions break pure chain detection
        serialize("(along = 1 and anint = 2) or ashort = 3");
        assertIR("(i16 3L)(i16 ashort)(=)(i32 2L)(i32 anint)(=)(i64 1L)(i64 along)(=)(&&)(||)(ret)");

        serialize("along = 1 and (anint = 2 or ashort = 3)");
        assertIR("(i16 3L)(i16 ashort)(=)(i32 2L)(i32 anint)(=)(||)(i64 1L)(i64 along)(=)(&&)(ret)");

        // Nested brackets
        serialize("(along = 1) and ((anint = 2) and (ashort = 3))");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i32 2L)(i32 anint)(=)(&&_sc)(i16 3L)(i16 ashort)(=)(ret)");
    }

    @Test
    public void testCharConstant() throws Exception {
        serialize("achar = 'a'");
        assertIR("(i16 97L)(i16 achar)(=)(ret)");
    }

    @Test
    public void testColumnTypes() throws Exception {
        Map<String, String[]> typeToColumn = new HashMap<>();
        typeToColumn.put("i8", new String[]{"aboolean", "abyte", "ageobyte"});
        typeToColumn.put("i16", new String[]{"ashort", "ageoshort", "achar"});
        typeToColumn.put("i32", new String[]{"anint", "ageoint", "asymbol"});
        typeToColumn.put("i64", new String[]{"along", "ageolong", "adate", "atimestamp", "atimestampns"});
        typeToColumn.put("i128", new String[]{"auuid", "along128"});
        typeToColumn.put("f32", new String[]{"afloat"});
        typeToColumn.put("f64", new String[]{"adouble"});

        for (String type : typeToColumn.keySet()) {
            for (String col : typeToColumn.get(type)) {
                serialize(col + " = " + col);
                assertIR("different results for " + type, "(" + type + " " + col + ")(" + type + " " + col + ")(=)(ret)");
            }
        }
    }

    @Test
    public void testComparisonOperators() throws Exception {
        for (String op : new String[]{"<", "<=", ">", ">=", "<>", "="}) {
            serialize("along " + op + " 0");
            assertIR("(i64 0L)(i64 along)(" + op + ")(ret)");
        }
    }

    @Test
    public void testConstantArithFoldOnByteColumn() throws Exception {
        // Pure-INT arithmetic subtree whose long-precision value overflows INT:
        // collapsed to a single IMM. A BYTE column compares at INT width, so the
        // subtree folds to a wrapped I4 IMM ((int) 10000000000L = 1410065408),
        // mirroring the Java filter's getInt() wrap.
        serialize("abyte > 100000 * 100000");
        assertIR("(i32 1410065408L)(i8 abyte)(>)(ret)");
    }

    @Test
    public void testConstantArithFoldOnIntColumn() throws Exception {
        // Same shape on an INT column: both operands stay at INT width, so the
        // fold wraps to an I4 IMM and no SX_I64 widening is needed.
        serialize("anint > 100000 * 100000");
        assertIR("(i32 1410065408L)(i32 anint)(>)(ret)");
    }

    @Test
    public void testConstantArithFoldOnLongColumn() throws Exception {
        // The subtree is pure INT arithmetic, so it wraps exactly as the Java filter's
        // FunctionParser#functionToConstant0 fold does ((int) 10000000000L = 1410065408). A LONG
        // column reads that IntConstant through getLong(), a plain sign extension here, so the
        // wrapped value is emitted as a single I8 IMM - the width the i64 peer compares at, and the
        // one that keeps the predicate on a vectorized loop.
        serialize("along > 100000 * 100000");
        assertIR("(i64 1410065408L)(i64 along)(>)(ret)");
    }

    @Test
    public void testConstantArithFoldOnShortColumn() throws Exception {
        // A SHORT column promotes to INT, so the fold wraps to an I4 IMM.
        serialize("ashort > 100000 * 100000");
        assertIR("(i32 1410065408L)(i16 ashort)(>)(ret)");
    }

    @Test
    public void testConstantArithFoldUnaryMinus() throws Exception {
        // Unary minus of an overflowing product is itself a fold root.
        serialize("along > -(100000 * 100000)");
        assertIR("(i64 -1410065408L)(i64 along)(>)(ret)");
    }

    @Test
    public void testConstantArithFoldVariousOps() throws Exception {
        // A constant subtree folds at its own DECLARED type. An operand outside the INT range makes
        // the subtree LONG, so it folds at full width; an all-INT one folds at INT width and wraps,
        // exactly as FunctionParser#functionToConstant0 does. The comparison peer does not enter
        // into it - that is the whole point of the one-value rule.
        serialize("along > 5000000000 + 5000000000");
        assertIR("(i64 10000000000L)(i64 along)(>)(ret)");
        serialize("along > 5000000000 - -5000000000");
        assertIR("(i64 10000000000L)(i64 along)(>)(ret)");
        // 100000 * 100000 is INT arithmetic: it wraps to 1410065408 here and in the Java filter.
        // The i64 peer then reads that wrapped value at 8 bytes, so the IMM is emitted at I8.
        serialize("along > 100000 * 100000");
        assertIR("(i64 1410065408L)(i64 along)(>)(ret)");
        serialize("along > 10000000000 / 1");
        assertIR("(i64 10000000000L)(i64 along)(>)(ret)");
    }

    @Test
    public void testConstantArithNoFoldInIntRange() throws Exception {
        // Long-precision result fits in INT: no fold, the subtree is emitted
        // node-by-node so the existing arithmetic IR ops still run.
        serialize("anint > 100 * 100");
        assertIR("(i32 100L)(i32 100L)(*)(i32 anint)(>)(ret)");
    }

    @Test
    public void testConstantArithNoFoldOnNonArithmeticRoot() throws Exception {
        // Only + - * / may be a fold root. Both integer folders propagated the NULL sentinel
        // before validating the node's own operator, so a comparison over two constant
        // arithmetic subtrees - 2_097_152 * 2_097_152 * 2_097_152 is exactly the LONG sentinel
        // and 65_536 * 32_768 exactly the INT one - answered LONG_NULL / INT_NULL for the '='
        // node itself and replaced the whole comparison with one IMM. Each side must fold on
        // its own and the comparison must survive.
        serialize("(2_097_152 * 2_097_152 * 2_097_152) = (65_536 * 32_768)");
        assertIR("(i32 -2147483648L)(i32 0L)(=)(ret)");
        // Under a column predicate the collapsed IMM left the operand stack short and the IR
        // failed to compile at all ("invalid opcode"), silently dropping the filter to Java.
        serialize("along > 0 and (2_097_152 * 2_097_152 * 2_097_152) = (65_536 * 32_768)");
        assertIR("(i32 -2147483648L)(i32 0L)(=)(i64 0L)(i64 along)(>)(&&)(ret)");
        // Boolean equality of two comparisons is one predicate, so there the truthy IMM took the
        // place of a comparison the backend then read as a value.
        serialize("(anint > 0) = ((2_097_152 * 2_097_152 * 2_097_152) > (65_536 * 32_768))");
        assertIR("(i32 -2147483648L)(i32 0L)(>)(i32 0L)(i32 anint)(>)(=)(ret)");
        // Control: both sentinels under ONE arithmetic root still fold, to the INT sentinel.
        serialize("anint = (2_097_152 * 2_097_152 * 2_097_152) + 65_536 * 32_768");
        assertIR("(i32 -2147483648L)(i32 anint)(=)(ret)");
        // Control: a '/' root reaches the tail of each folder, which the operator whitelist above
        // now leaves for division alone. The INT-width fold of the sentinel child is 0, so this
        // divides 0 by 2 rather than declining.
        serialize("anint = (2_097_152 * 2_097_152 * 2_097_152) / 2");
        assertIR("(i32 0L)(i32 anint)(=)(ret)");
    }

    @Test
    public void testConstantArithNoFoldOnNonConstantSubtree() throws Exception {
        // Subtree mixes constants with a column reference: not pure-constant,
        // so no fold and the arithmetic stays in the IR. The mixed I4 (anint)
        // + I8 (along) operands still drive the existing NarrowI64WidenDetector
        // path, which lifts the constant to i64 and sign-extends anint.
        serialize("along > anint * 100000");
        assertIR("(i32 100000L)(i32 anint)(*)(i64 along)(>)(ret)");
    }

    @Test
    public void testFloatArithI64ConstantForcesScalarNotWideLane() throws Exception {
        // C6: a DOUBLE-width conjunct (adouble * 1.0 > <inexact float>) sets isWideLaneMode and
        // emits NO width conversion, so hasEmittedWideLaneConversion stays false.
        // requiresWideLanePair's first clause accepts an F8 expression against a widening
        // constant, which is what sets the mode; markFloatCmpConst then declines the pair, because
        // isFloatLeaf admits F4 only - a DOUBLE column already compares at the width the Java
        // filter reads it at - and markDoubleWidthArithConstOperand declines it too, because
        // isNarrowLaneDoubleConstArith's hasEightByteLeaf test rejects an eight-byte leaf such as
        // adouble. A sibling afloat + <out-of-INT constant> then puts the LONG constant in
        // i64WidenConstants (an IMM I8 in the IR) and sets hasI64WidenArithConstant, with no SX_I64
        // behind it; that predicate's local observer types constants at F4, so
        // hasWidthChangingI64WidenConstant() reports the lane-width hazard and getExecHint()
        // resolves it. The scalar force must still fire: no conversion was emitted, so the
        // four-lane loop - the only one whose lanes are eight bytes wide whatever the columns are -
        // is not what runs, and every other vectorized loop takes its lane width from the observed
        // columns, which never count the widened immediate. Forcing scalar keeps the IR and the
        // emitted hint consistent. Gating the WIDE_LANE hint on isWideLaneMode alone turns both
        // assertions below into WIDE_LANE, which is the regression they pin.
        //
        // The FLOAT spelling of the peer (afloat * 1.0 > <inexact float>) no longer holds the
        // premise: isNarrowLaneDoubleConstArith claims that shape and
        // markDoubleWidthArithConstOperand announces the conversion, which is what puts it on the
        // four-lane loop. The FLOAT column has no spelling that sets the mode without emitting -
        // for an F4 leaf the mode clause and markFloatCmpConst key on the same widening constant -
        // so the peer has to read DOUBLE, and the filter is then mixed-size rather than
        // single-size. The IR assertions pin the premise directly: with the mode set, serialize()
        // skips the mixed-size detector (hasWideLaneConversionSource() answers true here), so the
        // conjunction serializes as a plain (&&). A peer that does NOT set the mode runs the
        // detector, which reports mixed sizes over adouble and afloat and rewrites the filter onto
        // the short-circuit path as (&&_sc) - a red assertion rather than a silent hollowing.
        int options = serialize("adouble * 1.0 > 1.00000003 and afloat + 5_000_000_000 > 1.5", false, false, false);
        assertIR("float wide-lane + out-of-INT constant",
                "(f32 1.5D)(i64 5000000000L)(f32 afloat)(+)(>)(f64 1.00000003D)(f64 1.0D)(f64 adouble)(*)(>)(&&)(ret)");
        assertOptionsHint("float wide-lane + out-of-INT constant", options, OptionsHint.SCALAR);

        // Reversed conjunct order behaves the same.
        options = serialize("afloat + 5_000_000_000 > 1.5 and adouble * 1.0 > 1.00000003", false, false, false);
        assertIR("reversed order",
                "(f64 1.00000003D)(f64 1.0D)(f64 adouble)(*)(>)(f32 1.5D)(i64 5000000000L)(f32 afloat)(+)(>)(&&)(ret)");
        assertOptionsHint("reversed order", options, OptionsHint.SCALAR);

        // Control: a single afloat + <out-of-INT constant> conjunct was already scalar via the
        // plain !isWideLaneMode branch, and stays scalar.
        options = serialize("afloat + 5_000_000_000 > 1.5", false, false, false);
        assertOptionsHint("single i64-leaf float conjunct", options, OptionsHint.SCALAR);

        // Control: a genuine WIDE_LANE filter - an INT column vs a LONG column with an out-of-INT
        // constant sibling, where SX_I64 is emitted so hasEmittedWideLaneConversion is true - must
        // NOT be dragged to scalar by the fix.
        options = serialize("anint < along and anint < 5_000_000_000", false, false, true);
        assertOptionsHint("genuine wide-lane stays wide-lane", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testEightByteArithI64ConstantKeepsVectorization() throws Exception {
        // An out-of-INT-range integer constant under an arithmetic node widens to a full I8 IMM.
        // In a predicate the type observer already reports at 8 bytes that widening changes no
        // width - for an I8 observation it is not even a change of opcode - so the predicate must
        // keep the vectorized loop it would have without the mark. Marking it as an i64-widen LEAF
        // instead sent every such predicate to the scalar backend, four rows per YMM iteration down
        // to one, with byte-identical IR.
        int options = serialize("atimestamp - 5_000_000_000 > 0", false, false, true);
        assertIR("atimestamp - 5_000_000_000 > 0", "(i64 0L)(i64 5000000000L)(i64 atimestamp)(-)(>)(ret)");
        assertOptionsHint("atimestamp - 5_000_000_000 > 0", options, OptionsHint.SINGLE_SIZE);

        options = serialize("atimestamp + 5_000_000_000 > 0", false, false, true);
        assertIR("atimestamp + 5_000_000_000 > 0", "(i64 0L)(i64 5000000000L)(i64 atimestamp)(+)(>)(ret)");
        assertOptionsHint("atimestamp + 5_000_000_000 > 0", options, OptionsHint.SINGLE_SIZE);

        options = serialize("atimestamp * 2_147_483_648 > 0", false, false, true);
        assertIR("atimestamp * 2_147_483_648 > 0", "(i64 0L)(i64 2147483648L)(i64 atimestamp)(*)(>)(ret)");
        assertOptionsHint("atimestamp * 2_147_483_648 > 0", options, OptionsHint.SINGLE_SIZE);

        // The unary-minus spelling marks the CONSTANT under the minus, which descend() folds into
        // the negation node and never serializes on its own - so the mark could only ever cost the
        // execution mode.
        for (String op : new String[]{"=", "<>", "<", ">"}) {
            options = serialize("along " + op + " -5_000_000_000", false, false, true);
            assertIR("along " + op + " -5_000_000_000", "(i64 -5000000000L)(i64 along)(" + op + ")(ret)");
            assertOptionsHint("along " + op + " -5_000_000_000", options, OptionsHint.SINGLE_SIZE);
        }

        // A DOUBLE peer observes 8 bytes too. Here the widening does change the immediate - an
        // exact f64 becomes an exact i64 of the same width - and avx2::convert harmonises the
        // (f64, i64) pairing through its ungated cvt_ltod arm, so the eight-byte lanes stay correct.
        options = serialize("adouble * 2_147_483_648 > 0", false, false, true);
        assertIR("adouble * 2_147_483_648 > 0", "(i64 0L)(i64 2147483648L)(f64 adouble)(*)(>)(ret)");
        assertOptionsHint("adouble * 2_147_483_648 > 0", options, OptionsHint.SINGLE_SIZE);

        options = serialize("adouble - 2_147_483_648 > 0", false, false, true);
        assertIR("adouble - 2_147_483_648 > 0", "(i64 0L)(i64 2147483648L)(f64 adouble)(-)(>)(ret)");
        assertOptionsHint("adouble - 2_147_483_648 > 0", options, OptionsHint.SINGLE_SIZE);

        options = serialize("along * 5_000_000_000 > 0 and atimestamp = 1", false, false, true);
        assertOptionsHint("along * 5_000_000_000 > 0 and atimestamp = 1", options, OptionsHint.SINGLE_SIZE);

        // Boundary, both directions. A 4-byte predicate must still go scalar: the observer counts
        // columns only, so the I8 IMM would ride eight 32-bit lanes under a single-size hint.
        options = serialize("afloat + 5_000_000_000 > 1.5", false, false, true);
        assertOptionsHint("afloat + 5_000_000_000 > 1.5", options, OptionsHint.SCALAR);
        // And a narrow ARITHMETIC operand of a 64-bit comparison keeps its own scalar force, which
        // has nothing to do with the constant.
        options = serialize("along + anint * 2 > 5_000_000_000", false, false, true);
        assertOptionsHint("along + anint * 2 > 5_000_000_000", options, OptionsHint.SCALAR);
    }

    @Test
    public void testNarrowConstArithFoldsToI64Immediate() throws Exception {
        // A pure-constant INT literal chain compared against a 64-bit column used to reach the
        // backend as its own operations at INT width - (i32 1000)(i32 1000)(*)(i64 along)(>) -
        // which no vectorized loop can run: the single-size loop's lanes are eight bytes and the
        // immediate half of that. markCmpOperandWidenedToI64 answered that by forcing the scalar
        // backend, so "along > 1000 * 1000" scanned one row per YMM iteration where the same bound
        // spelled "along > 1000000" scanned four. The Java filter never runs the chain either -
        // FunctionParser folds it to one IntConstant that the comparison reads through getLong() -
        // so the frontend folds it to that same immediate and the loop comes back.
        int options = serialize("along > 1000 * 1000", false, false, true);
        assertIR("along > 1000 * 1000", "(i64 1000000L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 1000 * 1000", options, OptionsHint.SINGLE_SIZE);

        options = serialize("atimestamp > 1_000_000 * 60", false, false, true);
        assertIR("atimestamp > 1_000_000 * 60", "(i64 60000000L)(i64 atimestamp)(>)(ret)");
        assertOptionsHint("atimestamp > 1_000_000 * 60", options, OptionsHint.SINGLE_SIZE);

        options = serialize("along > 60 * 60 * 24", false, false, true);
        assertIR("along > 60 * 60 * 24", "(i64 86400L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 60 * 60 * 24", options, OptionsHint.SINGLE_SIZE);

        // The bound written as a single literal is what the chain must become, IR included.
        options = serialize("along > 1_000_000", false, false, true);
        assertIR("along > 1_000_000", "(i64 1000000L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 1_000_000", options, OptionsHint.SINGLE_SIZE);

        // The fold runs at INT width and sign-extends, exactly as IntConstant#getLong() does, so a
        // chain that WRAPS emits the wrapped value rather than the mathematical one. Folding at
        // 64-bit width instead would emit 1_000_000_000_000 here and select no row at all.
        options = serialize("along > 1_000_000 * 1_000_000", false, false, true);
        assertIR("along > 1_000_000 * 1_000_000", "(i64 -727379968L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 1_000_000 * 1_000_000", options, OptionsHint.SINGLE_SIZE);

        // Division is not modular, so the wrap has to happen per operation and not once at the end:
        // -727379968 / 1000000 is -727, where (int) (1_000_000_000_000 / 1_000_000) is 1000000.
        options = serialize("along > (1_000_000 * 1_000_000) / 1_000_000", false, false, true);
        assertIR("along > (1_000_000 * 1_000_000) / 1_000_000", "(i64 -727L)(i64 along)(>)(ret)");
        assertOptionsHint("along > (1_000_000 * 1_000_000) / 1_000_000", options, OptionsHint.SINGLE_SIZE);

        // A unary minus over the chain is a fold root too - arithExprType looks through it.
        options = serialize("along > -(1000 * 1000)", false, false, true);
        assertIR("along > -(1000 * 1000)", "(i64 -1000000L)(i64 along)(>)(ret)");
        assertOptionsHint("along > -(1000 * 1000)", options, OptionsHint.SINGLE_SIZE);

        // The same fold as an operand of a genuinely 64-bit arithmetic node, and as an IN element.
        options = serialize("along + 1000 * 1000 > 0", false, false, true);
        assertIR("along + 1000 * 1000 > 0", "(i64 0L)(i64 1000000L)(i64 along)(+)(>)(ret)");
        assertOptionsHint("along + 1000 * 1000 > 0", options, OptionsHint.SINGLE_SIZE);

        options = serialize("along in (1000 * 1000, 5)", false, false, true);
        assertIR("along in (1000 * 1000, 5)", "(i64 5L)(i64 along)(=)(i64 1000000L)(i64 along)(=)(||)(ret)");
        assertOptionsHint("along in (1000 * 1000, 5)", options, OptionsHint.SINGLE_SIZE);

        // A narrow chain NESTED under an out-of-INT-range one is marked as a fold root and then
        // never reached: descend() folds the enclosing I8 root first and skips the whole subtree.
        // The enclosing fold is width-aware, so the inner product still wraps at INT width.
        options = serialize("along > 5_000_000_000 + (1000 * 1000)", false, false, true);
        assertIR("along > 5_000_000_000 + (1000 * 1000)", "(i64 5001000000L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 5_000_000_000 + (1000 * 1000)", options, OptionsHint.SINGLE_SIZE);

        // A chain deep enough to wrap only at the last operation.
        options = serialize("along > 1000 * 1000 * 1000 * 1000", false, false, true);
        assertIR("along > 1000 * 1000 * 1000 * 1000", "(i64 -727379968L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 1000 * 1000 * 1000 * 1000", options, OptionsHint.SINGLE_SIZE);
    }

    @Test
    public void testNarrowConstOperandOfLongArithWidensToI64() throws Exception {
        // A genuinely 64-bit arithmetic node reads EVERY operand at 64 bits - FunctionParser
        // resolves it to the (LL) factory and folds it through IntConstant#getLong() - but the
        // predicate-wide type observer types a constant at the widest COLUMN or BIND VARIABLE it
        // saw (PredicateContext#handleColumn, #handleBindVariable), so an all-INT-column predicate
        // typed the narrow half of "446_488 - 114_763L" down to I4 and the node reached the
        // backend as (i64 114763L)(i32 446488L)(-): a 4-byte immediate against an 8-byte one
        // under a single operator. QueryFuzzTest#testQueryFuzz found it on seeds
        // 274896052653843 / 1787218116926, where serialize()'s areWideLaneWidthsHarmonised()
        // assert reported the pairing.
        //
        // The rows were right - the four-lane avx2::convert() sign-extends the i32 side, and
        // the fold "446_488 - 114_763L" does not overflow INT - but which width the JAVA filter
        // reads at is the frontend's answer to give, and the NEGATED spelling of the same operand
        // already gave the right one (forceScalarOnUnharmonisedNarrowArith folds "-446_488" to an
        // I8 immediate), so the two spellings disagreed with each other.
        int options = serialize("anint >= (446_488 - 114_763L)", false, false, true);
        assertIR("anint >= (446_488 - 114_763L)",
                "(i64 114763L)(i64 446488L)(-)(i32 anint)(sx_i64)(>=)(ret)");
        assertOptionsHint("anint >= (446_488 - 114_763L)", options, OptionsHint.WIDE_LANE);

        // The control that was always harmonised: unary minus makes the operand a fold root.
        options = serialize("anint >= (-446_488 - 114_763L)", false, false, true);
        assertIR("anint >= (-446_488 - 114_763L)",
                "(i64 114763L)(i64 -446488L)(-)(i32 anint)(sx_i64)(>=)(ret)");
        assertOptionsHint("anint >= (-446_488 - 114_763L)", options, OptionsHint.WIDE_LANE);

        // A zero divisor at LONG width is deliberately NOT folded - the native int64_div is what
        // reproduces DivLong's NULL - so the operations reach the backend and the widths still
        // have to agree. This is the shape a fold-everything fix would have left behind.
        options = serialize("anint >= (446_488 / 0L)", false, false, true);
        assertIR("anint >= (446_488 / 0L)",
                "(i64 0L)(i64 446488L)(/)(i32 anint)(sx_i64)(>=)(ret)");
        assertOptionsHint("anint >= (446_488 / 0L)", options, OptionsHint.WIDE_LANE);

        // Nested: the widening has to reach every level of the chain, not just the node holding
        // the LONG literal.
        options = serialize("anint >= (446_488 - 114_763L) * 2", false, false, true);
        assertIR("anint >= (446_488 - 114_763L) * 2",
                "(i64 2L)(i64 114763L)(i64 446488L)(-)(*)(i32 anint)(sx_i64)(>=)(ret)");
        assertOptionsHint("anint >= (446_488 - 114_763L) * 2", options, OptionsHint.WIDE_LANE);

        // The IN spelling reaches markWidthSemantics through its own arm.
        options = serialize("anint in (446_488 - 114_763L, 5)", false, false, true);
        assertIR("anint in (446_488 - 114_763L, 5)",
                "(i64 5L)(i32 anint)(sx_i64)(=)(i64 114763L)(i64 446488L)(-)(i32 anint)(sx_i64)(=)(||)(ret)");
        assertOptionsHint("anint in (446_488 - 114_763L, 5)", options, OptionsHint.WIDE_LANE);

        // A NARROW arithmetic node keeps its constants at INT width, because it wraps at INT
        // width: the promotion above must not leak into the narrow-parent arm.
        options = serialize("anint >= 446_488 - 114_763", false, false, true);
        assertIR("anint >= 446_488 - 114_763", "(i32 114763L)(i32 446488L)(-)(i32 anint)(>=)(ret)");
        assertOptionsHint("anint >= 446_488 - 114_763", options, OptionsHint.SINGLE_SIZE);

        options = serialize("along > anint * 2 + 1", false, false, true);
        assertIR("along > anint * 2 + 1", "(i32 1L)(i32 2L)(i32 anint)(*)(+)(i64 along)(>)(ret)");
        assertOptionsHint("along > anint * 2 + 1", options, OptionsHint.SCALAR);
    }

    @Test
    public void testNarrowConstArithKeepsIntWidthUnderDoubleObservation() throws Exception {
        // A NARROW integer arithmetic node computes at i32 and wraps there, so its constant
        // operands have to be emitted at i32 whatever the predicate's widest observed column is.
        // serializeConstant overrode only an I8 observation, so a DOUBLE column typed them at I8
        // and the backend ran int64_sub: "0 - 2_147_483_647 - 1" came out as an ordinary
        // -2_147_483_648 instead of the INT_NULL SubInt#getInt produces, and cvt_itod's NaN
        // never happened.
        // CompiledFilterRegressionTest#testConstantFoldIntNullCollisionAtDoubleObservedWidth pins
        // the rows; this pins the widths, which is what actually changed.
        int options = serialize("adouble > (0 - 2_147_483_647 - 1)", false, false, true);
        assertIR("adouble > (0 - 2_147_483_647 - 1)",
                "(i32 1L)(i32 2147483647L)(i32 0L)(-)(-)(f64 adouble)(>)(ret)");
        // Unchanged by the fix: an eight-byte observation runs four lanes (compiler.cpp step =
        // 256 / (8 * 8)), which is exactly where avx2::convert may use cvt_itod, so the i32
        // immediates cost this filter no vectorization.
        assertOptionsHint("adouble > (0 - 2_147_483_647 - 1)", options, OptionsHint.SINGLE_SIZE);

        // The chain one operation short of the sentinel takes the same widths - the rule is about
        // the node's declared width, not about the value it lands on.
        options = serialize("adouble > (0 - 2_147_483_647)", false, false, true);
        assertIR("adouble > (0 - 2_147_483_647)", "(i32 2147483647L)(i32 0L)(-)(f64 adouble)(>)(ret)");
        assertOptionsHint("adouble > (0 - 2_147_483_647)", options, OptionsHint.SINGLE_SIZE);

        // The chain one level deeper, under a DOUBLE division rather than directly under the
        // comparison. markWidthSemantics reaches it through the F8 arithmetic node's recursion.
        options = serialize("adouble = ((-1 - 2_147_483_647) / adouble)", false, false, true);
        assertIR("adouble = ((-1 - 2_147_483_647) / adouble)",
                "(f64 adouble)(i32 2147483647L)(i32 -1L)(-)(/)(f64 adouble)(=)(ret)");
        assertOptionsHint("adouble = ((-1 - 2_147_483_647) / adouble)", options, OptionsHint.SINGLE_SIZE);

        // The LONG-observed spelling was already right and must not move: narrowKeptConstants
        // overrode the I8 observation there, and markFoldedI64ConstArith's LONG_NULL decline is
        // what keeps the operations (and the scalar force) rather than folding to one immediate.
        options = serialize("along > (0 - 2_147_483_647 - 1)", false, false, true);
        assertIR("along > (0 - 2_147_483_647 - 1)",
                "(i32 1L)(i32 2147483647L)(i32 0L)(-)(-)(i64 along)(>)(ret)");
        assertOptionsHint("along > (0 - 2_147_483_647 - 1)", options, OptionsHint.SCALAR);

        // Negative control: a bare comparison bound is not an arithmetic operand, so it keeps the
        // eight-byte width the observer gives it. Narrowing it here would break every
        // "double_col > <int literal>" filter.
        options = serialize("adouble > 5", false, false, true);
        assertIR("adouble > 5", "(i64 5L)(f64 adouble)(>)(ret)");
        assertOptionsHint("adouble > 5", options, OptionsHint.SINGLE_SIZE);

        // Negative control: a genuinely 64-bit constant chain is not narrow, so its operands are
        // not kept - descend folds it at long width and emits one I8 immediate.
        options = serialize("adouble > 5_000_000_000 - 1", false, false, true);
        assertIR("adouble > 5_000_000_000 - 1", "(i64 4999999999L)(f64 adouble)(>)(ret)");
        assertOptionsHint("adouble > 5_000_000_000 - 1", options, OptionsHint.SINGLE_SIZE);

        // Negative control: an integer constant under a DOUBLE-width arithmetic node is read at
        // f64 by the Java filter (MulDouble over IntConstant#getDouble), so it must stay at eight
        // bytes. Its parent is F8, not I4, so narrowKeptConstants never claims it.
        options = serialize("adouble > adouble * 2 + 1", false, false, true);
        assertIR("adouble > adouble * 2 + 1", "(i64 1L)(i64 2L)(f64 adouble)(*)(+)(f64 adouble)(>)(ret)");
        assertOptionsHint("adouble > adouble * 2 + 1", options, OptionsHint.SINGLE_SIZE);

        // Unchanged controls at the other observations: a FLOAT one already emitted I4 through
        // serializeNumber's I4/F4 case, and a mixed one goes through serializeUntypedNumber, which
        // has always honoured the narrow keep.
        options = serialize("afloat > (0 - 2_147_483_647 - 1)", false, false, true);
        assertIR("afloat > (0 - 2_147_483_647 - 1)",
                "(i32 1L)(i32 2147483647L)(i32 0L)(-)(-)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > (0 - 2_147_483_647 - 1)", options, OptionsHint.SINGLE_SIZE);

        options = serialize("adouble > anint * 2 + 1", false, false, true);
        assertIR("adouble > anint * 2 + 1", "(i32 1L)(i32 2L)(i32 anint)(*)(+)(f64 adouble)(>)(ret)");
        assertOptionsHint("adouble > anint * 2 + 1", options, OptionsHint.MIXED_SIZES);
    }

    @Test
    public void testNarrowConstArithFoldKeepsScalarWhereItIsLoadBearing() throws Exception {
        // A zero divisor at INT width is not folded: DivInt#getInt answers INT_NULL and the native
        // int32_div reproduces it, so the operations have to reach the backend - and with them the
        // scalar force, since they are i32 against an i64 peer.
        int options = serialize("along > 10 / 0", false, false, true);
        assertIR("along > 10 / 0", "(i32 0L)(i32 10L)(/)(i64 along)(>)(ret)");
        assertOptionsHint("along > 10 / 0", options, OptionsHint.SCALAR);

        // A chain that lands exactly on the INT NULL sentinel keeps the scalar backend too. The
        // Java filter reads IntConstant(INT_NULL) through getLong(), which is LONG_NULL, and the
        // scalar convert() is what carries that mapping.
        options = serialize("along > 2_147_483_647 + 1", false, false, true);
        assertIR("along > 2_147_483_647 + 1", "(i32 -2147483648L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 2_147_483_647 + 1", options, OptionsHint.SCALAR);

        options = serialize("along > 65_536 * 32_768", false, false, true);
        assertIR("along > 65_536 * 32_768", "(i32 -2147483648L)(i64 along)(>)(ret)");
        assertOptionsHint("along > 65_536 * 32_768", options, OptionsHint.SCALAR);

        // A narrow arithmetic subtree with a COLUMN in it is not a constant fold and keeps the
        // force: it computes at i32 and wraps per row, and SX_I64 is emitted per leaf, so its
        // RESULT cannot be sign-extended from the frontend.
        options = serialize("anint * (1000 * 1000) > along", false, false, true);
        assertIR("anint * (1000 * 1000) > along", "(i64 along)(i32 1000L)(i32 1000L)(*)(i32 anint)(*)(>)(ret)");
        assertOptionsHint("anint * (1000 * 1000) > along", options, OptionsHint.SCALAR);

        options = serialize("along + anint * 2 > 5_000_000_000", false, false, true);
        assertOptionsHint("along + anint * 2 > 5_000_000_000", options, OptionsHint.SCALAR);

        // An INT-width comparison is untouched: nothing reads the chain at 64 bits, so it keeps its
        // per-operation IR and its four-byte lanes, which is what MulInt#getInt does per row.
        options = serialize("anint > 1000 * 1000", false, false, true);
        assertIR("anint > 1000 * 1000", "(i32 1000L)(i32 1000L)(*)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 1000 * 1000", options, OptionsHint.SINGLE_SIZE);
        assertOptionsSize("anint > 1000 * 1000", options, 4);
    }

    @Test
    public void testNarrowConstArithFoldHintIsConjunctOrderIndependent() throws Exception {
        // The fold removes a forceScalarMode write, and forceScalarMode is filter-wide, so it could
        // in principle have moved a filter's mode in one conjunct order only. It does not: the
        // decision is a property of the folded subtree alone. Assert both spellings.
        int options = serialize("atimestamp > 1_000_000 * 60 and anint < along", false, false, true);
        assertIR(
                "fold conjunct first",
                "(i64 along)(i32 anint)(sx_i64)(<)(i64 60000000L)(i64 atimestamp)(>)(&&)(ret)"
        );
        assertOptionsHint("fold conjunct first", options, OptionsHint.WIDE_LANE);

        options = serialize("anint < along and atimestamp > 1_000_000 * 60", false, false, true);
        assertIR(
                "fold conjunct last",
                "(i64 60000000L)(i64 atimestamp)(>)(i64 along)(i32 anint)(sx_i64)(<)(&&)(ret)"
        );
        assertOptionsHint("fold conjunct last", options, OptionsHint.WIDE_LANE);

        options = serialize("along > 1000 * 1000 and afloat < 1.00000003", false, false, true);
        assertOptionsHint("float peer, fold conjunct first", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat < 1.00000003 and along > 1000 * 1000", false, false, true);
        assertOptionsHint("float peer, fold conjunct last", options, OptionsHint.WIDE_LANE);

        // A conjunct that keeps the force still forces the whole filter, in either order.
        options = serialize("along > 10 / 0 and anint < along", false, false, true);
        assertOptionsHint("declined fold conjunct first", options, OptionsHint.SCALAR);
        options = serialize("anint < along and along > 10 / 0", false, false, true);
        assertOptionsHint("declined fold conjunct last", options, OptionsHint.SCALAR);
    }

    @Test
    public void testEightByteArithI64ConstantHintIsConjunctOrderIndependent() throws Exception {
        // i64WidenLeaves is per-predicate but hasEmittedWideLaneConversion is filter-wide, and
        // PostOrderTreeTraversalAlgo descends node.rhs first. While the arithmetic constant joined
        // the leaf set, that made the chosen execution mode depend on which conjunct was written
        // first: the spelling whose conversion-emitting conjunct came LAST left the flag false when
        // the other conjunct closed, forcing scalar for the whole filter. Both spellings return the
        // same rows, so the only difference was a 4x swing from reordering a WHERE clause - and a
        // regression test written in one order cannot catch the other. Assert both.
        int options = serialize("along * 5_000_000_000 > 0 and anint > 16777216.0", false, false, true);
        assertIR(
                "conversion-emitting conjunct last",
                "(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(i64 0L)(i64 5000000000L)(i64 along)(*)(>)(&&)(ret)"
        );
        assertOptionsHint("conversion-emitting conjunct last", options, OptionsHint.WIDE_LANE);

        options = serialize("anint > 16777216.0 and along * 5_000_000_000 > 0", false, false, true);
        assertIR(
                "conversion-emitting conjunct first",
                "(i64 0L)(i64 5000000000L)(i64 along)(*)(>)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(&&)(ret)"
        );
        assertOptionsHint("conversion-emitting conjunct first", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testFloatArithI64ConstantHintIsConjunctOrderIndependent() throws Exception {
        // The same order dependence, on the FLOAT family the width-changing-constant force is
        // load-bearing for. A four-byte afloat predicate carrying an out-of-INT-range constant must
        // not ride an eight-lane loop, but the four-lane loop carries eight-byte lanes and is
        // welcome to it. Whether the filter reaches that loop is settled only when the traversal
        // ends, so evaluating the suppression at each predicate's exit answered from whichever
        // conjunct the traversal had reached - and PostOrderTreeTraversalAlgo descends node.rhs
        // first, so writing the afloat conjunct LAST serialized it FIRST and forced the whole
        // filter scalar. Both spellings return the same rows; the only difference was four rows per
        // YMM iteration against one.
        final String[] floatConjuncts = {
                "afloat + 5_000_000_000 > 1.5",
                "afloat * 5_000_000_000 > 1.5",
                "afloat - 5_000_000_000 > 1.5",
                "afloat + 2_147_483_648 > 1.5",
        };
        // Every peer here emits a wide-lane conversion of its own, so the filter does run the
        // four-lane loop and the suppression must lift - in both spellings.
        final String[] conversionEmittingPeers = {
                "anint < along",
                "anint < 1.00000003",
                "anint > 16777216.0",
                "anint < 5_000_000_000",
                "anint in (1, 5_000_000_000)",
                "anint + 5_000_000_000 > 1",
                "along * anint > 5_000_000_000",
                "afloat < 1.00000003",
                // A DOUBLE literal under a four-byte arithmetic node is a conversion source of its
                // own: the widened f64 immediate meets f32 lanes, which the four-lane loop closes
                // with cvt_ftod. See isNarrowLaneDoubleConstArith.
                "afloat * 1.0 > 1.00000003",
        };
        for (String glue : new String[]{" and ", " or "}) {
            for (String floatConjunct : floatConjuncts) {
                for (String peer : conversionEmittingPeers) {
                    final String floatFirst = floatConjunct + glue + peer;
                    final String peerFirst = peer + glue + floatConjunct;
                    assertOptionsHint(floatFirst, serialize(floatFirst, false, false, true), OptionsHint.WIDE_LANE);
                    assertOptionsHint(peerFirst, serialize(peerFirst, false, false, true), OptionsHint.WIDE_LANE);
                }
            }
        }

        // Control, both orders: a peer that SETS the mode but emits NO conversion leaves the filter
        // off the four-lane loop, so the force stays on whichever way round it is written.
        // adouble * 1.0 > <inexact float> is that peer - see
        // testFloatArithI64ConstantForcesScalarNotWideLane for why it sets the mode without
        // emitting - and it is the shape the deferral exists for: regressing the deferral into an
        // unconditional lift, or gating the WIDE_LANE hint on isWideLaneMode alone, shows up here
        // as WIDE_LANE in both orders. The (&&) in the IR pins that the mode really is set; a peer
        // that fails to set it runs the mixed-size detector instead and serializes as (&&_sc).
        int options = serialize("afloat + 5_000_000_000 > 1.5 and adouble * 1.0 > 1.00000003", false, false, true);
        assertIR("no-conversion peer, float conjunct first",
                "(f64 1.00000003D)(f64 1.0D)(f64 adouble)(*)(>)(f32 1.5D)(i64 5000000000L)(f32 afloat)(+)(>)(&&)(ret)");
        assertOptionsHint("no-conversion peer, float conjunct first", options, OptionsHint.SCALAR);
        options = serialize("adouble * 1.0 > 1.00000003 and afloat + 5_000_000_000 > 1.5", false, false, true);
        assertIR("no-conversion peer, float conjunct last",
                "(f32 1.5D)(i64 5000000000L)(f32 afloat)(+)(>)(f64 1.00000003D)(f64 1.0D)(f64 adouble)(*)(>)(&&)(ret)");
        assertOptionsHint("no-conversion peer, float conjunct last", options, OptionsHint.SCALAR);
    }

    @Test
    public void testFloatCmpConstWithNoExactFloat() throws Exception {
        // A FLOAT column always compares at DOUBLE width in the Java filter, so a constant with no
        // exact float must not be emitted as a 32-bit float - the nearest one selects different
        // rows. It goes to the compiled filter as a full double instead; the four-lane AVX2 mode
        // promotes the float column to double, so both filters then run the same tolerance-aware
        // double comparison. (A float bound rounded in the direction the
        // operator preserves would reproduce EXACT IEEE ordering, but QuestDB compares with a 1e-10
        // tolerance and a float ulp near 1.0 is 1.2e-7 - the rounded bound steps over the band and
        // flips rows inside it. See markFloatCmpConst.)
        int options = serialize("afloat < 1.00000003", false, false, false);
        assertIR("(f64 1.00000003D)(f32 afloat)(<)(ret)");
        assertOptionsHint("afloat < 1.00000003", options, OptionsHint.WIDE_LANE);
        serialize("afloat <= 1.00000003");
        assertIR("(f64 1.00000003D)(f32 afloat)(<=)(ret)");
        serialize("afloat > 0.99999998");
        assertIR("(f64 0.99999998D)(f32 afloat)(>)(ret)");
        serialize("afloat >= 0.99999998");
        assertIR("(f64 0.99999998D)(f32 afloat)(>=)(ret)");
        serialize("afloat = 1.00000003");
        assertIR("(f64 1.00000003D)(f32 afloat)(=)(ret)");
        serialize("afloat <> 1.00000003");
        assertIR("(f64 1.00000003D)(f32 afloat)(<>)(ret)");
        // The constant on the LEFT takes the same route (the operands serialize the other way
        // round here, the column first).
        serialize("1.00000003 > afloat");
        assertIR("(f32 afloat)(f64 1.00000003D)(>)(ret)");

        // An integer literal has no exact float above 2^24 either: (float) 16777217 is 16777216.
        // The 64-bit arm emits it exactly, as an I8 rather than a double.
        serialize("afloat < 16_777_217");
        assertIR("(i64 16777217L)(f32 afloat)(<)(ret)");

        options = serialize("afloat + 0 < 1.00000003", false, false, true);
        assertIR("(f64 1.00000003D)(i32 0L)(f32 afloat)(+)(<)(ret)");
        assertOptionsHint("float arithmetic", options, OptionsHint.WIDE_LANE);

        options = serialize("afloat IN (1.00000003, 2.5)", false, false, true);
        assertIR("(f32 2.5D)(f32 afloat)(=)(f64 1.00000003D)(f32 afloat)(=)(||)(ret)");
        assertOptionsHint("float IN", options, OptionsHint.WIDE_LANE);

        // A constant WITH an exact float is left alone: it compares the same at either width, so it
        // keeps its F4 immediate and the vectorized path - the fix costs nothing there.
        options = serialize("afloat < 1.5", false, false, false);
        assertIR("(f32 1.5D)(f32 afloat)(<)(ret)");
        assertOptionsHint("afloat < 1.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("afloat = 5.0", false, false, false);
        assertIR("(f32 5.0D)(f32 afloat)(=)(ret)");
        assertOptionsHint("afloat = 5.0", options, OptionsHint.SINGLE_SIZE);
    }

    @Test
    public void testNarrowIntCmpFloatConst() throws Exception {
        // An INT column promotes to DOUBLE in the Java filter, so a bound the JIT cannot reproduce
        // at float width has to reach the filter as a full double - and the column has to
        // sign-extend alongside it, because (i64, f64) is the pairing the backend converts in every
        // execution mode. (i32, f64) is only converted in the four-lane loop.
        int options = serialize("anint < 1.00000003", false, false, true);
        assertIR("(f64 1.00000003D)(i32 anint)(sx_i64)(<)(ret)");
        assertOptionsHint("anint < 1.00000003", options, OptionsHint.WIDE_LANE);
        serialize("anint = 1.00000003");
        assertIR("(f64 1.00000003D)(i32 anint)(sx_i64)(=)(ret)");
        serialize("anint <> 1.00000003");
        assertIR("(f64 1.00000003D)(i32 anint)(sx_i64)(<>)(ret)");

        // A constant WITH an exact float still needs the double treatment once its magnitude
        // reaches 2^24, because from there the COLUMN is the side that rounds:
        // (float) 16777217 is 16777216, so a float compare puts them equal.
        options = serialize("anint > 16777216.0", false, false, true);
        assertIR("(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(ret)");
        assertOptionsHint("anint > 16777216.0", options, OptionsHint.WIDE_LANE);

        // BYTE and SHORT take the same arm but must NOT go wide-lane: avx2::sx_i64 widens an i32
        // lane and declines anything else, so they keep the scalar fallback, where the backend
        // sign-extends i8 and i16 explicitly. These used to throw out of serializeNumber's I1/I2
        // arms and decline the filter entirely.
        options = serialize("abyte < 1.00000003", false, false, false);
        assertIR("(f64 1.00000003D)(i8 abyte)(sx_i64)(<)(ret)");
        assertOptionsHint("abyte < 1.00000003", options, OptionsHint.SCALAR);
        options = serialize("ashort < 1.00000003", false, false, false);
        assertIR("(f64 1.00000003D)(i16 ashort)(sx_i64)(<)(ret)");
        assertOptionsHint("ashort < 1.00000003", options, OptionsHint.SCALAR);

        // An INT ARITHMETIC subtree is not a leaf, so only the CONSTANT widens - the subtree keeps
        // computing at i32 and wrapping, exactly as AddInt#getInt does.
        options = serialize("anint + 0 > 16777216.0", false, false, false);
        assertIR("(f64 1.6777216E7D)(i32 0L)(i32 anint)(+)(>)(ret)");
        assertOptionsHint("anint + 0 > 16777216.0", options, OptionsHint.SCALAR);

        // An INEXACT bound below 2^24 still keeps the eight-lane path when its rounding cannot
        // reach a column value. Every INT row is a whole number, so what matters is whether an
        // integer falls between the bound and the float the filter would emit: 1.1 rounds to
        // 1.10000002384, and no integer (nor the tolerance band round one) lies in between, so the
        // f32 comparison selects exactly the same rows. 1.00000003 rounds across 1 and does not.
        options = serialize("anint > 1.1", false, false, false);
        assertIR("(f32 1.100000023841858D)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 1.1", options, OptionsHint.SINGLE_SIZE);
        options = serialize("anint > 0.1", false, false, false);
        assertIR("(f32 0.10000000149011612D)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 0.1", options, OptionsHint.SINGLE_SIZE);
        options = serialize("anint <> 2.7", false, false, false);
        assertIR("(f32 2.700000047683716D)(i32 anint)(<>)(ret)");
        assertOptionsHint("anint <> 2.7", options, OptionsHint.SINGLE_SIZE);

        // Controls: an exact-float bound below 2^24 cannot diverge - neither side rounds - so it
        // keeps its F4 immediate and the eight-lane vectorized path. The fix costs nothing there.
        options = serialize("anint < 1.5", false, false, false);
        assertIR("(f32 1.5D)(i32 anint)(<)(ret)");
        assertOptionsHint("anint < 1.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("anint > 16777215.0", false, false, false);
        assertIR("(f32 1.6777215E7D)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 16777215.0", options, OptionsHint.SINGLE_SIZE);
        // An integer-spelled literal compares at integer width on both paths and is untouched.
        options = serialize("anint > 16777216", false, false, false);
        assertIR("(i32 16777216L)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 16777216", options, OptionsHint.SINGLE_SIZE);
    }

    @Test
    public void testFloatSuppressedI64WideningNestedIntUnderLong() throws Exception {
        // A float suppresses the global narrow-i64 widening, but the inner
        // INT*INT product (anint * 100000) feeds a LONG-width multiply, so the
        // Java filter computes it at 64 bits. The serializer sign-extends the
        // inner operands while leaving along (already i64) and the leaf operand
        // of the long multiply alone.
        serialize("afloat <= along * (anint * 100000)");
        assertIR("(i32 100000L)(i32 anint)(*)(i64 along)(*)(f32 afloat)(<=)(ret)");
    }

    @Test
    public void testFloatSuppressedI64WideningSkipsLongTypedOp() throws Exception {
        // anint * 9000000000 is already i64 (the constant overflows int), so the
        // long multiply promotes anint through convert(); no sx_i64 is needed.
        serialize("afloat <= along * (anint * 9000000000)");
        assertIR("(i64 9000000000L)(i32 anint)(sx_i64)(*)(i64 along)(*)(f32 afloat)(<=)(ret)");
    }

    @Test
    public void testConstantTypes() throws Exception {
        final String[][] columns = new String[][]{
                {"abyte", "i8", "1", "1L", "i8"},
                {"abyte", "i8", "-1", "-1L", "i8"},
                {"ashort", "i16", "1", "1L", "i16"},
                {"ashort", "i16", "-1", "-1L", "i16"},
                {"anint", "i32", "1", "1L", "i32"},
                {"anint", "i32", "1.5", "1.5D", "f32"},
                {"anint", "i32", "-1", "-1L", "i32"},
                {"along", "i64", "1", "1L", "i64"},
                {"along", "i64", "1.5", "1.5D", "f64"},
                {"along", "i64", "-1", "-1L", "i64"},
                {"auuid", "i128", "'00000000-0000-0000-0000-000000000000'", "0 0L", "i128"},
                {"afloat", "f32", "1", "1L", "i32"},
                {"afloat", "f32", "1.5", "1.5D", "f32"},
                {"afloat", "f32", "-1", "-1L", "i32"},
                {"adouble", "f64", "1", "1L", "i64"},
                {"adouble", "f64", "1.5", "1.5D", "f64"},
                {"adouble", "f64", "-1", "-1L", "i64"},
        };

        for (String[] col : columns) {
            final String colName = col[0];
            final String colType = col[1];
            final String constStr = col[2];
            final String constValue = col[3];
            final String constType = col[4];
            serialize(colName + " > " + constStr);
            assertIR("different results for " + colName, "(" + constType + " " + constValue + ")(" + colType + " " + colName + ")(>)(ret)");
        }
    }

    @Test
    public void testDateLiteral() throws Exception {
        serialize("adate = '2023-02-11T11:12:22'");
        assertIR("(i64 1676113942000L)(i64 adate)(=)(ret)");
        serialize("adate >= '2023-02-11T11:12:22'");
        assertIR("(i64 1676113942000L)(i64 adate)(>=)(ret)");
        serialize("adate <= '2023-02-11T11'");
        assertIR("(i64 1676113200000L)(i64 adate)(<=)(ret)");
        serialize("adate > '2023-02-11'");
        assertIR("(i64 1676073600000L)(i64 adate)(>)(ret)");
        serialize("adate < '2023-02'");
        assertIR("(i64 1675209600000L)(i64 adate)(<)(ret)");
        serialize("adate != '2023'");
        assertIR("(i64 1672531200000L)(i64 adate)(<>)(ret)");
    }

    @Test(expected = SqlException.class)
    public void testDifferentSymbolColumnsCompare() throws Exception {
        serialize("asymbol > anothersymbol");
    }

    @Test(expected = SqlException.class)
    public void testDifferentSymbolColumnsEq() throws Exception {
        serialize("asymbol = anothersymbol");
    }

    @Test(expected = SqlException.class)
    public void testDifferentSymbolColumnsNotEq() throws Exception {
        serialize("asymbol != anothersymbol");
    }

    @Test
    public void testDirectIntLongColumnComparisonWidensAndUsesWideLane() throws Exception {
        // A bare INT-column vs LONG-column comparison (no arithmetic, no out-of-range
        // constant) must sign-extend the INT leaf so the four-lane AVX2 path compares it
        // at i64 width - the LONG column loads at full width and cmp_* dispatches on a
        // single dtype, so an un-widened INT leaf mismatches lanes.
        int options = serialize("anint < along", false, false, true);
        assertIR("(i64 along)(i32 anint)(sx_i64)(<)(ret)");
        assertOptionsHint("anint < along", options, OptionsHint.WIDE_LANE);

        // The reversed operand order widens the INT leaf the same way.
        serialize("along > anint", false, false, true);
        assertIR("(i32 anint)(sx_i64)(i64 along)(>)(ret)");

        // A widening sibling (out-of-INT-range constant) flips the whole filter to
        // four-lane; both anint leaves now carry sx_i64, so the column-vs-column
        // comparison is no longer dragged in un-widened.
        options = serialize("anint < along and anint < 5_000_000_000", false, false, true);
        assertIR("(i64 5000000000L)(i32 anint)(sx_i64)(<)"
                + "(i64 along)(i32 anint)(sx_i64)(<)(&&)(ret)");
        assertOptionsHint("column vs column AND out-of-range constant", options, OptionsHint.WIDE_LANE);
    }

    @Test(expected = SqlException.class)
    public void testEmptyIn() throws Exception {
        serialize("anint IN ()");
    }

    @Test
    public void testGeoHashConstant() throws Exception {
        String[][] columns = new String[][]{
                {"ageobyte", "i8", "##1", "1L"},
                {"ageobyte", "i8", "#s", "24L"},
                {"ageoshort", "i16", "##00000001", "1L"},
                {"ageoshort", "i16", "#sp", "789L"},
                {"ageoint", "i32", "##0000000000000001", "1L"},
                {"ageoint", "i32", "#sp05", "807941L"},
                {"ageolong", "i64", "##00000000000000000000000000000001", "1L"},
                {"ageolong", "i64", "#sp052w92p1p8", "888340623145993896L"},
        };

        for (String[] col : columns) {
            final String name = col[0];
            final String type = col[1];
            final String constant = col[2];
            final String value = col[3];
            serialize(name + " = " + constant);
            assertIR("different results for " + name, "(" + type + " " + value + ")(" + type + " " + name + ")(=)(ret)");
        }
    }

    @Test
    public void testIn() throws Exception {
        serialize("anint IN (1, 2, 3)");
        assertIR("(i32 3L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(i32 1L)(i32 anint)(=)(||)(||)(ret)");
        serialize("anint IN (1)");
        assertIR("(i32 1L)(i32 anint)(=)(ret)");
        serialize("anint IN (-1, 0, 1)");
        assertIR("(i32 1L)(i32 anint)(=)(i32 0L)(i32 anint)(=)(i32 -1L)(i32 anint)(=)(||)(||)(ret)");
        serialize("anint <> NULL AND anint IN (4, 5)");
        assertIR("(i32 5L)(i32 anint)(=)(i32 4L)(i32 anint)(=)(||)(i32 -2147483648L)(i32 anint)(<>)(&&)(ret)");
        serialize("-anint IN (-1)");
        assertIR("(i32 -1L)(i32 anint)(neg)(=)(ret)");
        serialize("anint NOT IN (1, 2, 3)");
        assertIR("(i32 3L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(i32 1L)(i32 anint)(=)(||)(||)(!)(ret)");
        serialize("atimestamp IN ('2020-01-01')");
        assertIR("(i64 1577836800000000L)(i64 atimestamp)(=)(ret)");
        serialize("atimestampns IN ('2020-01-01')");
        assertIR("(i64 1577836800000000000L)(i64 atimestampns)(=)(ret)");
    }

    @Test
    public void testInConstantKeyFollowsElementWidth() throws Exception {
        // A CONSTANT key is emitted at whatever width the predicate's type observer settled on -
        // serializeUntypedNumber picks I8 as soon as anything in the predicate is I8 - while the
        // per-element harmonisation in markWidthSemantics only fires when the pairing itself folds
        // to I8. A key that fits in an int folds to I4 against an INT element, so the element got no
        // sx_i64 and the four-lane backend compared an i64 key against four packed i32. The key now
        // takes the per-element override, so each pairing is emitted at its own width.
        int options = serialize("(0 in (anint, along)) and anint < along", false, false, false);
        assertIR("(i64 along)(i32 anint)(sx_i64)(<)(i64 along)(i64 0L)(=)(i32 anint)(sx_i64)(i64 0L)(=)(||)(&&)(ret)");
        assertOptionsHint("(0 in (anint, along)) and anint < along", options, OptionsHint.WIDE_LANE);

        // Same without the widening sibling: the pairings are harmonised either way.
        serialize("0 in (anint, along)", false, false, false);
        assertIR("(i64 along)(i64 0L)(=)(i32 anint)(sx_i64)(i64 0L)(=)(||)(ret)");

        // A key no int can hold keeps I8 for BOTH pairings, and the INT element sign-extends to
        // meet it rather than the key narrowing onto a value it cannot represent.
        serialize("3000000000 in (anint, along)", false, false, false);
        assertIR("(i64 along)(i64 3000000000L)(=)(i32 anint)(sx_i64)(i64 3000000000L)(=)(||)(ret)");
    }

    @Test
    public void testInNullElementKeepsNarrowKeyVectorized() throws Exception {
        // A NULL element used to widen the key to i64 (it is neither INT- nor LONG-typed, so the
        // per-element width fell through to I8), which emits sx_i64 - an opcode AVX2 does not
        // implement - and dropped the whole filter to the scalar path. Both widths select the same
        // rows for a plain narrow-int key: its getInt() carries INT_NULL exactly when the row is
        // null, and serializeNull emits the INT_NULL immediate. So the pairing stays at I4 and the
        // filter keeps its vectorized exec hint.
        int options = serialize("anint IN (1, 2, null)", false, false, false);
        assertIR("(i32 -2147483648L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(i32 1L)(i32 anint)(=)(||)(||)(ret)");
        assertOptionsHint("anint IN (1, 2, null)", options, OptionsHint.SINGLE_SIZE);

        // (A BYTE or SHORT key takes a different route entirely - neither type has a NULL sentinel,
        // so the pairing can never match and serializeIn folds it away. See
        // testInNullElementNonNullableNarrowKeyFoldsToNeverMatching.)

        // A genuinely wide element widens the key and selects four-lane AVX2.
        options = serialize("anint IN (1, 5_000_000_000)", false, false, false);
        assertIR("(i64 5000000000L)(i32 anint)(sx_i64)(=)(i64 1L)(i32 anint)(sx_i64)(=)(||)(ret)");
        assertOptionsHint("anint IN (1, 5_000_000_000)", options, OptionsHint.WIDE_LANE);

        // An arithmetic key wraps against the NULL element too: '=' resolves an untyped null to
        // EqInt, which reads the key with getInt(), so the key is null exactly when its getInt()
        // carries INT_NULL - the same rows a projection of the key prints null for. It therefore
        // takes I4 as well, and the whole filter stays vectorized (no sx_i64 anywhere).
        int arithOptions = serialize("anint * 2 IN (1, null)", false, false, false);
        assertIR("(i32 -2147483648L)(i32 2L)(i32 anint)(*)(=)(i32 1L)(i32 2L)(i32 anint)(*)(=)(||)(ret)");
        assertOptionsHint("anint * 2 IN (1, null)", arithOptions, OptionsHint.SINGLE_SIZE);

        // A genuinely wide element pulls the pairing to 64 bits, but the arithmetic KEY computes at
        // i32 and wraps, and SX_I64 is emitted per leaf - so its result cannot be sign-extended from
        // the frontend. That leaves an i32-against-i64 pairing, which only the scalar backend's
        // convert() reproduces, so the predicate drops out of the vectorized loop. The in-range
        // element widens with it - the list is one 64-bit pairing set - while the product stays i32.
        // This is the vectorization the one-value rule costs; see forceScalarOnUnharmonisedNarrowArith.
        arithOptions = serialize("anint * 2 IN (1, 5_000_000_000)", false, false, false);
        assertIR("(i64 5000000000L)(i32 2L)(i32 anint)(*)(=)(i64 1L)(i32 2L)(i32 anint)(*)(=)(||)(ret)");
        assertOptionsHint("anint * 2 IN (1, 5_000_000_000)", arithOptions, OptionsHint.SCALAR);

        // A NULL element beside a wide (LONG) COLUMN element takes the width the LIST settled on.
        // The along pairing lifts the key to i64 (sx_i64), and the key is one node with one emitted
        // width, so the NULL has to meet it there: LONG_NULL at I8. That is what the Java filter
        // compares against too - it reads the key through getLong(), and
        // Numbers.intToLong(INT_NULL) is LONG_NULL, so exactly the INT_NULL rows still match.
        // Emitting INT_NULL at I4 here would leave an i32 immediate against the key's i64 lanes.
        options = serialize("anint IN (along, null)", false, false, false);
        assertIR("(i64 -9223372036854775808L)(i32 anint)(sx_i64)(=)(i64 along)(i32 anint)(sx_i64)(=)(||)(ret)");
        assertOptionsHint("anint IN (along, null)", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testInNullElementNonNullableNarrowKeyFoldsToNeverMatching() throws Exception {
        // BYTE and SHORT have no NULL sentinel, so a NULL element matches nothing - which is what the
        // Java IN functions return. On master serializeNull rejected the whole filter here ("byte
        // type is not nullable"); once untyped NULLs started comparing at INT width the pairing
        // compiled instead, as an I4 immediate against a size-1 lane, and the four-lane loop tested
        // three of every four BYTE lanes against 0. serializeIn folds the pairing to a comparison
        // that is false on every row, so the filter stays vectorized and agrees with Java.
        //
        // The fold is emitted at I4 rather than the key's width: it is an all-zero FULL register,
        // which reads the same at every lane width, and the key's own width is not knowable at that
        // point for a constant key.
        int options = serialize("abyte in (null)", false, false, false);
        assertIR("(i32 0L)(i32 1L)(=)(ret)");
        assertOptionsHint("abyte in (null)", options, OptionsHint.SINGLE_SIZE);

        options = serialize("abyte in (null, 1)", false, false, false);
        assertIR("(i8 1L)(i8 abyte)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        assertOptionsHint("abyte in (null, 1)", options, OptionsHint.SINGLE_SIZE);

        options = serialize("ashort in (null, 1)", false, false, false);
        assertIR("(i16 1L)(i16 ashort)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        assertOptionsHint("ashort in (null, 1)", options, OptionsHint.SINGLE_SIZE);

        // The gate is isWidthSensitiveInKey, so every key shape it accepts folds - not just a bare
        // column. A unary minus is the one that bit: genuineArithType deliberately sees through it,
        // so "-abyte" is width-sensitive at BYTE width, and unlike a BINARY narrow arithmetic key it
        // does not set hasArithmeticOperations, so nothing forced scalar mode to cover it.
        serialize("-abyte in (null)", false, false, false);
        assertIR("(i32 0L)(i32 1L)(=)(ret)");
        serialize("-ashort in (null, 1)", false, false, false);
        assertIR("(i16 1L)(i16 ashort)(neg)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        // Narrow BINARY arithmetic reads at SHORT width and folds for the same reason.
        serialize("abyte + ashort in (null)", false, false, false);
        assertIR("(i32 0L)(i32 1L)(=)(ret)");

        // A key that widens to INT keeps the ordinary pairing - INT has a real sentinel, and both
        // sides are already I4.
        serialize("-anint in (null)", false, false, false);
        assertIR("(i32 -2147483648L)(i32 anint)(neg)(=)(ret)");
        // "abyte + 1" promotes to INT width, so it keeps the ordinary pairing too. Its widths are
        // mismatched, but a narrow BINARY arithmetic predicate forces scalar mode, where convert()
        // has the complete integer table.
        serialize("abyte + 1 in (null)", false, false, false);
        assertIR("(i32 -2147483648L)(i8 1L)(i8 abyte)(+)(=)(ret)");

        // A BIND VARIABLE key reaches the same pairing through VAR I1 / VAR I2 and needs the same
        // fold - isWidthSensitiveInKey already treats a bind variable like a column.
        bindVariableService.clear();
        bindVariableService.setByte("b", (byte) 1);
        bindVariableService.setShort("s", (short) 243);
        bindVariableService.setInt("i", 7);
        serialize(":b in (null)", false, false, false);
        assertIR("(i32 0L)(i32 1L)(=)(ret)");
        serialize(":s in (null, 1)", false, false, false);
        assertIR("(i16 1L)(i16 :0)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        // INT has a real NULL sentinel, so an INT bind variable keeps the ordinary pairing.
        serialize(":i in (null)", false, false, false);
        assertIR("(i32 -2147483648L)(i32 :0)(=)(ret)");

        // A numeric CONSTANT key is never NULL either, and it MUST fold for a second reason: the
        // CONSTANT arm of isWidthSensitiveInKey drives the NULL element to I4, which against a BYTE
        // or SHORT observer bypasses serializeNull's decline and would leave an I4 immediate against
        // the size-1 key lane - the very mismatch this fold removes.
        serialize("0 in (null, abyte)", false, false, false);
        assertIR("(i8 abyte)(i8 0L)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        serialize("0 in (null, ashort)", false, false, false);
        assertIR("(i16 ashort)(i16 0L)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        serialize("0 in (null, :b)", false, false, false);
        assertIR("(i8 :0)(i8 0L)(=)(i32 0L)(i32 1L)(=)(||)(ret)");
        serialize("0 in (null, anint)", false, false, false);
        assertIR("(i32 anint)(i32 0L)(=)(i32 0L)(i32 1L)(=)(||)(ret)");

        // A GEOBYTE shares the I1 type code but HAS a NULL at every width, so it keeps the ordinary
        // pairing and compares against its real sentinel. IPv4 and SYMBOL likewise.
        serialize("ageobyte in (null)", false, false, false);
        assertIR("(i8 -1L)(i8 ageobyte)(=)(ret)");
        serialize("anipv4 in (null)", false, false, false);
        assertIR("(i32 0L)(i32 anipv4)(=)(ret)");

        // CHAR codes as I2 but reads (char) 0 as NULL, so folding it away would drop real matches.
        // It keeps the ordinary pairing, which serializeNull still declines - as it always has, and
        // as BOOLEAN still does too.
        try {
            serialize("achar in (null)", false, false, false);
            Assert.fail("expected the CHAR pairing to decline");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "short type is not nullable");
        }
        try {
            serialize("aboolean in (null)", false, false, false);
            Assert.fail("expected the BOOLEAN pairing to decline");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "byte type is not nullable");
        }
    }

    @Test
    public void testInShortCircuit() throws Exception {
        // IN() short-circuit is enabled when:
        // 1. We're in a pure AND chain with mixed column sizes (scalar mode)
        // 2. IN() is the top-level/root predicate in the chain

        // Single value IN() in AND chain - simple equality, no special short-circuit opcodes
        serialize("along = 1 and anint IN (2)");
        assertIR("(i64 1L)(i64 along)(=)(&&_sc)(i32 2L)(i32 anint)(=)(&&_sc)(ret)");

        // Multiple value IN() in AND chain - uses BEGIN_SC(2), OR_SC(2), AND_SC(0), END_SC(2)
        // Label 0 = next_row (default for AND_SC), Label 2 = success (IN match)
        // Note: IN values are serialized in reverse order (last to first)
        serialize("along = 1 and anint IN (2, 3)");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(begin_sc 2)(i32 3L)(i32 anint)(=)(||_sc 2)(i32 2L)(i32 anint)(=)(&&_sc)(end_sc 2)(ret)"
        );

        // Three values in IN() - more OR_SC opcodes
        serialize("along = 1 and anint IN (2, 3, 4)");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(begin_sc 2)(i32 4L)(i32 anint)(=)(||_sc 2)(i32 3L)(i32 anint)(=)(||_sc 2)(i32 2L)(i32 anint)(=)(&&_sc)(end_sc 2)(ret)"
        );

        // IN() at the start of AND chain (still top-level) - sorted by priority so along comes first
        serialize("anint IN (2, 3) and along = 1");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(begin_sc 2)(i32 3L)(i32 anint)(=)(||_sc 2)(i32 2L)(i32 anint)(=)(&&_sc)(end_sc 2)(ret)"
        );

        // Decide wide-lane capability before mixed column widths select scalar short-circuit IR.
        // The LONG element widens the INT key, so this whole tree is eligible for four-lane AVX2
        // and must use lane-wise boolean operators: AND_SC/OR_SC cannot branch per SIMD lane.
        //
        // The key is ONE node with one emitted width, so the 64-bit pairing pulls the whole list
        // up: both key reads sign-extend and the in-range element widens alongside them. Widening
        // is exact for a narrow leaf, and it is what keeps the four-lane loop from comparing a
        // packed i32 half against the key's i64 lanes.
        int options = serialize("along = 1 and anint IN (2, 5_000_000_000)", false, false, false);
        assertIR(
                "(i64 5000000000L)(i32 anint)(sx_i64)(=)(i64 2L)(i32 anint)(sx_i64)(=)(||)" +
                        "(i64 1L)(i64 along)(=)(&&)(ret)"
        );
        assertOptionsHint("wide-lane IN in AND chain", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testInShortCircuitDisabledInOrChain() throws Exception {
        // IN() in an OR chain should NOT use short-circuit (uses regular || operators)
        serialize("along = 1 or anint IN (2, 3)");
        assertIR(
                "(i32 3L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(||)(||_sc)" +
                        "(i64 1L)(i64 along)(=)(ret)"
        );
    }

    @Test
    public void testInShortCircuitDisabledWhenNested() throws Exception {
        // Nested IN() (wrapped by NOT) should NOT use short-circuit for IN itself
        // The NOT wraps the IN, so IN is not the root of its predicate
        serialize("along = 1 and not anint IN (2, 3)");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(i32 3L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(||)(!)(ret)"
        );
    }

    @Test
    public void testInShortCircuitMultipleIn() throws Exception {
        // Multiple IN() predicates in same AND chain - each uses short-circuit
        // Sorted by priority: along (i64, priority 1) before anint (i32, priority 2)
        serialize("anint IN (3, 4) and along IN (1, 2)");
        assertIR(
                "(begin_sc 2)(i32 4L)(i32 anint)(=)(||_sc 2)(i32 3L)(i32 anint)(=)(&&_sc)(end_sc 2)" +
                        "(begin_sc 2)(i64 2L)(i64 along)(=)(||_sc 2)(i64 1L)(i64 along)(=)(&&_sc)(end_sc 2)(ret)"
        );
    }

    @Test
    public void testInShortCircuitNotIn() throws Exception {
        // NOT IN() in AND chain - NOT wraps IN, so IN doesn't use short-circuit internally
        // but the whole predicate participates in AND chain short-circuit
        serialize("along = 1 and anint NOT IN (2, 3)");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(i32 3L)(i32 anint)(=)(i32 2L)(i32 anint)(=)(||)(!)(ret)"
        );
    }

    @Test
    public void testInShortCircuitTwoValues() throws Exception {
        // Two-value IN() - boundary case for the args loop (args.size() = 3)
        serialize("along = 1 and anint IN (2, 3)");
        assertIR(
                "(i64 1L)(i64 along)(=)(&&_sc)" +
                        "(begin_sc 2)(i32 3L)(i32 anint)(=)(||_sc 2)(i32 2L)(i32 anint)(=)(&&_sc)(end_sc 2)(ret)"
        );
    }

    @Test
    public void testInShortCircuitWithComparison() throws Exception {
        // IN() combined with comparison operator (priority 5: OTHER)
        // Priority order: along = (priority 1), abyte > (priority 5), anint IN (priority 5)
        serialize("abyte > 0 and anint IN (1, 2) and along = 3");
        assertIR(
                "(i64 3L)(i64 along)(=)(&&_sc)(i8 0L)(i8 abyte)(>)(&&_sc)" +
                        "(begin_sc 2)(i32 2L)(i32 anint)(=)(||_sc 2)(i32 1L)(i32 anint)(=)(&&_sc)(end_sc 2)(ret)"
        );
    }

    @Test(expected = SqlException.class)
    public void testInSubSelect() throws Exception {
        serialize("asymbol in (select asymbol from tab limit 1)");
    }

    @Test
    public void testInSymbolAndCharKeysAreNotWidened() throws Exception {
        // SYMBOL and CHAR IN keys collapse onto the same narrow type codes as a genuine INT / SHORT key
        // (columnTypeCode maps SYMBOL to I4 and CHAR to I2), but they route through InSymbol / InChar,
        // not the width-sensitive InLong path, so isWidthSensitiveInKey must keep them out of the
        // per-element width override. Taking that override would read a string element as "not narrow"
        // (genuineArithType == UNDEFINED_CODE -> I8) and sign-extend the key: an SX_I64 that AVX2 does
        // not implement, dropping the whole filter onto the scalar path. Row results stay correct either
        // way - sign-extending a symbol / char key is value-preserving - so only the IR and the exec
        // hint can catch this. See CompiledFilterRegressionTest#testInOperatorSymbolAndCharKeysAreNotWidthSensitive,
        // whose own comment records that it cannot.
        int options = serialize("asymbol IN ('ABC', 'DEF')", false, false, true);
        // 'ABC' is symbol key 0 in asymbol; 'DEF' is not in its symbol table, so it emits as a bind
        // variable (see testUnknownSymbolConstant). Neither key leaf carries an sx_i64.
        assertIR("(i32 :0)(i32 asymbol)(=)(i32 0L)(i32 asymbol)(=)(||)(ret)");
        assertOptionsHint("asymbol IN ('ABC', 'DEF')", options, OptionsHint.SINGLE_SIZE);

        options = serialize("asymbol NOT IN ('ABC', 'DEF')", false, false, true);
        assertIR("(i32 :0)(i32 asymbol)(=)(i32 0L)(i32 asymbol)(=)(||)(!)(ret)");
        assertOptionsHint("asymbol NOT IN ('ABC', 'DEF')", options, OptionsHint.SINGLE_SIZE);

        options = serialize("achar IN ('x', 'z')", false, false, true);
        assertIR("(i16 122L)(i16 achar)(=)(i16 120L)(i16 achar)(=)(||)(ret)");
        assertOptionsHint("achar IN ('x', 'z')", options, OptionsHint.SINGLE_SIZE);

        options = serialize("achar NOT IN ('x', 'z')", false, false, true);
        assertIR("(i16 122L)(i16 achar)(=)(i16 120L)(i16 achar)(=)(||)(!)(ret)");
        assertOptionsHint("achar NOT IN ('x', 'z')", options, OptionsHint.SINGLE_SIZE);

        // Control: a genuine narrow-int key still wraps against narrow elements and widens against an
        // out-of-INT-range one - the width sensitivity symbol / char must not inherit.
        options = serialize("abyte IN (1, 2)", false, false, true);
        assertIR("(i8 2L)(i8 abyte)(=)(i8 1L)(i8 abyte)(=)(||)(ret)");
        assertOptionsHint("abyte IN (1, 2)", options, OptionsHint.SINGLE_SIZE);

        options = serialize("anint IN (1, 5_000_000_000)", false, false, true);
        assertIR("(i64 5000000000L)(i32 anint)(sx_i64)(=)(i64 1L)(i32 anint)(sx_i64)(=)(||)(ret)");
        assertOptionsHint("anint IN (1, 5_000_000_000)", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testInVariableBinding() throws Exception {
        bindVariableService.clear();
        bindVariableService.setInt("anint", 1);
        bindVariableService.setLong(0, 2);

        int options = serialize("anint IN (:anint, $1)", false, false, true);
        // anint is INT; the $1 (LONG) key widens the column per element via sx_i64,
        // while the :anint (INT) key needs no widening.
        assertIR("(i64 :0)(i32 anint)(sx_i64)(=)(i32 :1)(sx_i64)(i32 anint)(sx_i64)(=)(||)(ret)");
        assertOptionsHint("mixed-width IN bind variables", options, OptionsHint.WIDE_LANE);

        Assert.assertEquals(2, bindVarFunctions.size());
        Assert.assertEquals(ColumnType.LONG, bindVarFunctions.get(0).getType());
        Assert.assertEquals(ColumnType.INT, bindVarFunctions.get(1).getType());
    }

    @Test(expected = SqlException.class)
    public void testInvalidNanoTimestampLiteral() throws Exception {
        serialize("atimestampns > ''");
    }

    @Test(expected = SqlException.class)
    public void testInvalidTimestampLiteral() throws Exception {
        serialize("atimestamp > ''");
    }

    @Test(expected = SqlException.class)
    public void testInvalidUuidConstant() throws Exception {
        serialize("auuid = '111111110111101111011110111111111111'");
    }

    @Test
    public void testKnownSymbolConstant() throws Exception {
        serialize("asymbol = '" + KNOWN_SYMBOL_1 + "' or anothersymbol = '" + KNOWN_SYMBOL_2 + "'");
        assertIR("(i32 0L)(i32 anothersymbol)(=)(i32 0L)(i32 asymbol)(=)(||)(ret)");
    }

    @Test
    public void testMixedAndOrNoShortCircuit() throws Exception {
        // Mixed AND/OR is not a pure chain -> no short-circuit, just regular operators
        serialize("along = 1 and anint = 2 or ashort = 3");
        assertIR("(i16 3L)(i16 ashort)(=)(i32 2L)(i32 anint)(=)(i64 1L)(i64 along)(=)(&&)(||)(ret)");

        serialize("along = 1 or anint = 2 and ashort = 3");
        assertIR("(i16 3L)(i16 ashort)(=)(i32 2L)(i32 anint)(=)(&&)(i64 1L)(i64 along)(=)(||)(ret)");
    }

    @Test
    public void testMixedConstantColumn() throws Exception {
        serialize("anint * 3 + 42.5 + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f64 42.5D)(i32 3L)(i32 anint)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testMixedConstantColumnFloatConstant() throws Exception {
        serialize("anint * 3 + 42.5f + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f32 42.5D)(i32 3L)(i32 anint)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testMixedConstantColumnIntOverflow() throws Exception {
        serialize("anint * 2147483648 + 42.5 + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f64 42.5D)(i64 2147483648L)(i32 anint)(sx_i64)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testNanoTimestampInLiteral() throws Exception {
        serialize("atimestampns in '2020-01-01'");
        assertIR("(i64 1577836800000000000L)(i64 atimestampns)(>=)(i64 1577923199999999999L)(i64 atimestampns)(<=)(&&)(ret)");
        serialize("atimestampns in '2020-01-01;15s'");
        assertIR("(i64 1577836800000000000L)(i64 atimestampns)(>=)(i64 1577836814999999999L)(i64 atimestampns)(<=)(&&)(ret)");
        serialize("atimestampns in '2020-01-01T23:59:58;4s;-1d;3'");
        assertIR("(i64 1577750398000000000L)(i64 atimestampns)(>=)(i64 1577750401999999999L)(i64 atimestampns)(<=)(&&)" +
                "(i64 1577836798000000000L)(i64 atimestampns)(>=)(i64 1577836801999999999L)(i64 atimestampns)(<=)(&&)" +
                "(i64 1577923198000000000L)(i64 atimestampns)(>=)(i64 1577923201999999999L)(i64 atimestampns)(<=)(&&)(||)(||)(ret)");
        serialize("along = 42 and atimestampns in '2020-01-01T23:59:58;4s;-1d;3'");
        assertIR("(i64 1577750398000000000L)(i64 atimestampns)(>=)(i64 1577750401999999999L)(i64 atimestampns)(<=)(&&)" +
                "(i64 1577836798000000000L)(i64 atimestampns)(>=)(i64 1577836801999999999L)(i64 atimestampns)(<=)(&&)" +
                "(i64 1577923198000000000L)(i64 atimestampns)(>=)(i64 1577923201999999999L)(i64 atimestampns)(<=)(&&)" +
                "(||)(||)(i64 42L)(i64 along)(=)(&&)(ret)");
    }

    @Test
    public void testNanoTimestampInLiteralNull() throws Exception {
        serialize("atimestampns in null");
        assertIR("(i64 -9223372036854775808L)(i64 atimestampns)(>=)(i64 -9223372036854775808L)(i64 atimestampns)(<=)(&&)(ret)");
    }

    @Test
    public void testNanoTimestampLiteral() throws Exception {
        serialize("atimestampns = '2023-02-11T11:12:22.116234987Z'");
        assertIR("(i64 1676113942116234987L)(i64 atimestampns)(=)(ret)");
        serialize("atimestampns = '2023-02-11T11:12:22.116234Z'");
        assertIR("(i64 1676113942116234000L)(i64 atimestampns)(=)(ret)");
        serialize("atimestampns >= '2023-02-11T11:12:22'");
        assertIR("(i64 1676113942000000000L)(i64 atimestampns)(>=)(ret)");
        serialize("atimestampns <= '2023-02-11T11'");
        assertIR("(i64 1676113200000000000L)(i64 atimestampns)(<=)(ret)");
        serialize("atimestampns > '2023-02-11'");
        assertIR("(i64 1676073600000000000L)(i64 atimestampns)(>)(ret)");
        serialize("atimestampns < '2023-02'");
        assertIR("(i64 1675209600000000000L)(i64 atimestampns)(<)(ret)");
        serialize("atimestampns != '2023'");
        assertIR("(i64 1672531200000000000L)(i64 atimestampns)(<>)(ret)");
    }

    @Test
    public void testNarrowIntCmpFloatConstInMixedWidthChain() throws Exception {
        // A narrow-int leaf against a widening floating-point bound emits BOTH an SX_I64 and a
        // double constant, so it is a wide-lane conversion source exactly as an F4 leaf against
        // such a bound is. hasWideLaneConversionSource() modelled only those two pairs, so a chain
        // pairing this one with a differently-sized sibling ran the mixed-size detector, took the
        // short-circuit path, and then tripped that path's own wide-lane guard - declining JIT for
        // a filter master compiled. The sibling is 8 bytes wide on purpose: against a 4-byte one
        // the sizes match and the detector could not fire whatever the gate answered.
        int options = serialize("anint > 16777216.0 and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(&&)(ret)");
        assertOptionsHint("narrow-int float bound AND long conjunct", options, OptionsHint.WIDE_LANE);

        // OR chains ride the same gate.
        options = serialize("anint > 16777216.0 or along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(||)(ret)");
        assertOptionsHint("narrow-int float bound OR long conjunct", options, OptionsHint.WIDE_LANE);

        // A DOUBLE sibling mixes the widths the same way a LONG one does.
        options = serialize("anint < 1.00000003 and adouble > 1.5", false, false, false);
        assertIR("(f64 1.5D)(f64 adouble)(>)(f64 1.00000003D)(i32 anint)(sx_i64)(<)(&&)(ret)");
        assertOptionsHint("narrow-int float bound AND double conjunct", options, OptionsHint.WIDE_LANE);

        // Wrapping the conjunct in NOT keeps the pairing inside one predicate, so it must resolve
        // the same way.
        options = serialize("not (anint > 16777216.0) and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(!)(&&)(ret)");
        assertOptionsHint("negated narrow-int float bound AND long conjunct", options, OptionsHint.WIDE_LANE);

        // Forced scalar mode never enters wide-lane mode, so it keeps the short-circuit path - and
        // the sign extension that the scalar backend implements for every narrow width.
        options = serialize("anint > 16777216.0 and along = 5", true, false, false);
        assertIR("(i64 5L)(i64 along)(=)(&&_sc)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(ret)");
        assertOptionsHint("forced scalar narrow-int float bound", options, OptionsHint.SCALAR);

        // Control: only the CONSTANT widens for an arithmetic subtree (maybeWidenCmpConstOperand),
        // and a widened constant emits no SX_I64, so no conversion reaches the backend and the
        // chain must keep its short-circuit path and its scalar hint.
        options = serialize("anint + 0 > 16777216.0 and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(&&_sc)(f64 1.6777216E7D)(i32 0L)(i32 anint)(+)(>)(ret)");
        assertOptionsHint("narrow-int arithmetic float bound AND long conjunct", options, OptionsHint.SCALAR);

        // Control: a bound whose rounding cannot reach any INT row is not a conversion source at
        // all, so this chain must keep short-circuiting too.
        options = serialize("anint > 1.1 and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(&&_sc)(f32 1.100000023841858D)(i32 anint)(>)(ret)");
        assertOptionsHint("exact-enough float bound AND long conjunct", options, OptionsHint.MIXED_SIZES);
    }

    @Test
    public void testNarrowIntCmpFloatConstUnderNotKeepsShortCircuit() throws Exception {
        // Control first: the narrow-int leaf and the widening bound ARE the two operands of the one
        // comparison the NOT wraps, so markNarrowConstCmpWidenPair really does route them to
        // markNarrowIntCmpFloatConst and the IR carries the sx_i64 it emits. The gate must keep
        // answering true here, or the chain takes the short-circuit path and the wide-lane guard in
        // serializePredicatesAndSc declines JIT for a filter that compiles.
        int options = serialize("not (anint > 16777216.0) and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(!)(&&)(ret)");
        assertOptionsHint("paired halves under NOT", options, OptionsHint.WIDE_LANE);

        // hasWideLaneConversionSource() treats a NOT subtree as ONE predicate, so searching it for a
        // narrow-int leaf and for a widening floating-point bound INDEPENDENTLY matches anint from
        // the FIRST comparison against 1.00000003 from the SECOND. Those two are never the operands
        // of one comparison, so markNarrowConstCmpWidenPair never marks them and the filter emits no
        // conversion at all - note the absence of sx_i64 below. Suppressing the mixed-size detector
        // for that non-conversion costs the chain its short-circuit path, which evaluates every
        // conjunct on every scanned row, and its sortPredicates reordering, and buys nothing: the
        // hint stays MIXED_SIZES, and compiler.cpp runs the same scalar loop either way.
        options = serialize("not (anint > 1 and adouble > 1.00000003) and anint < 100", false, false, false);
        assertIR("(f64 1.00000003D)(f64 adouble)(>)(i32 1L)(i32 anint)(>)(&&)(!)(&&_sc)(i32 100L)(i32 anint)(<)(ret)");
        assertOptionsHint("cross-comparison halves under NOT, AND chain", options, OptionsHint.MIXED_SIZES);

        // OR chains ride the same gate.
        options = serialize("not (anint > 1 and adouble > 1.00000003) or anint < 100", false, false, false);
        assertIR("(f64 1.00000003D)(f64 adouble)(>)(i32 1L)(i32 anint)(>)(&&)(!)(||_sc)(i32 100L)(i32 anint)(<)(ret)");
        assertOptionsHint("cross-comparison halves under NOT, OR chain", options, OptionsHint.MIXED_SIZES);

        // A NOT over OR holds the two halves in one predicate exactly as a NOT over AND does.
        options = serialize("not (anint > 1 or adouble > 1.00000003) and anint < 100", false, false, false);
        assertIR("(f64 1.00000003D)(f64 adouble)(>)(i32 1L)(i32 anint)(>)(||)(!)(&&_sc)(i32 100L)(i32 anint)(<)(ret)");
        assertOptionsHint("cross-comparison halves under NOT over OR", options, OptionsHint.MIXED_SIZES);

        // An 8-byte sibling mixes the widths the same way the 4-byte one does.
        options = serialize("not (anint > 1 and adouble > 1.00000003) and along < 100", false, false, false);
        assertIR("(f64 1.00000003D)(f64 adouble)(>)(i32 1L)(i32 anint)(>)(&&)(!)(&&_sc)(i64 100L)(i64 along)(<)(ret)");
        assertOptionsHint("cross-comparison halves under NOT, long sibling", options, OptionsHint.MIXED_SIZES);

        // Forced scalar mode never enters wide-lane mode, so the gate is not consulted at all and
        // the chain keeps its short-circuit path whichever way the pairing resolves.
        options = serialize("not (anint > 1 and adouble > 1.00000003) and anint < 100", true, false, false);
        assertIR("(f64 1.00000003D)(f64 adouble)(>)(i32 1L)(i32 anint)(>)(&&)(!)(&&_sc)(i32 100L)(i32 anint)(<)(ret)");
        assertOptionsHint("cross-comparison halves under NOT, forced scalar", options, OptionsHint.SCALAR);

        // Control, the direction that is dangerous rather than merely slow: the NOT wraps a genuine
        // pair AND an unrelated sibling comparison, so the walk has to find the pair among the
        // comparisons rather than give up once the subtree holds more than one. Were the gate to
        // answer false here, the mixed widths (4-byte anint against 8-byte along / adouble) would
        // put the chain on the short-circuit path, where serializePredicatesAndSc's wide-lane guard
        // rejects the sx_i64 below and declines JIT for the whole filter.
        options = serialize("not (anint > 16777216.0 and adouble > 1.5) and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.5D)(f64 adouble)(>)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(&&)(!)(&&)(ret)");
        assertOptionsHint("paired halves under NOT beside a sibling comparison", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testNegatedArithmeticalExpression() throws Exception {
        serialize("-(anint + 42) = -10");
        assertIR("(i32 -10L)(i32 42L)(i32 anint)(+)(neg)(=)(ret)");
    }

    @Test
    public void testNegatedColumn() throws Exception {
        serialize("-ashort > 0");
        assertIR("(i16 0L)(i16 ashort)(neg)(>)(ret)");
    }

    @Test
    public void testNullConstantMixedFloatColumns() throws Exception {
        serialize("afloat + adouble <> null");
        assertIR("(f64 NaND)(f64 adouble)(f32 afloat)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstantMixedFloatIntegerColumns() throws Exception {
        serialize("afloat + anint <> null and null <> along + adouble");
        assertIR("(f32 NaND)(i32 anint)(f32 afloat)(+)(<>)(&&_sc)(f64 adouble)(i64 along)(+)(f64 NaND)(<>)(ret)");
    }

    @Test
    public void testNullConstantMixedIntegerColumns() throws Exception {
        serialize("anint + along <> null or null <> along + anint");
        // (sx_i64) widens narrow INT operands to i64 inside arithmetic predicates that also
        // have a LONG operand, so the JIT computes at long width and matches AddInt.getLong.
        assertIR("(i32 anint)(sx_i64)(i64 along)(+)(i64 -9223372036854775808L)(<>)(||_sc)(i64 -9223372036854775808L)(i64 along)(i32 anint)(sx_i64)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstantMultiplePredicates() throws Exception {
        serialize("ageoint <> null and along <> null");
        assertIR("(i32 -1L)(i32 ageoint)(<>)(&&_sc)(i64 -9223372036854775808L)(i64 along)(<>)(ret)");
    }

    @Test
    public void testNullConstantValues() throws Exception {
        String[][] columns = new String[][]{
                {"anint", "i32", Numbers.INT_NULL + "L"},
                {"along", "i64", Numbers.LONG_NULL + "L"},
                {"ageobyte", "i8", GeoHashes.BYTE_NULL + "L"},
                {"ageoshort", "i16", GeoHashes.SHORT_NULL + "L"},
                {"ageoint", "i32", GeoHashes.INT_NULL + "L"},
                {"ageolong", "i64", GeoHashes.NULL + "L"},
                {"afloat", "f32", "NaND"},
                {"adouble", "f64", "NaND"},
        };

        for (String[] col : columns) {
            final String name = col[0];
            final String type = col[1];
            final String value = col[2];
            serialize(name + " <> null");
            assertIR("different results for " + name, "(" + type + " " + value + ")(" + type + " " + name + ")(<>)(ret)");
        }
    }

    @Test
    public void testOperationPriority() throws Exception {
        // 42.5 is an operand of the SUBTRACTION, which the Java filter resolves to "-(DD)" and
        // evaluates at f64, so it emits at F8 and the INT quotient promotes through
        // int32_to_double. 0.5 is only the comparison BOUND against that f64 subtree: it has an
        // exact 32-bit float, and float_to_double carries it unchanged, so it stays F4.
        serialize("(anint + 1) / (3 * anint) - 42.5 > 0.5");
        assertIR("(f32 0.5D)(f64 42.5D)(i32 anint)(i32 3L)(*)(i32 1L)(i32 anint)(+)(/)(-)(>)(ret)");
    }

    @Test
    public void testOptionsDirectIntLongComparisonUsesWideLane() throws Exception {
        int options = serialize("anint < 5_000_000_000", false, false, true);
        assertIR("(i64 5000000000L)(i32 anint)(sx_i64)(<)(ret)");
        assertOptionsHint("anint < 5_000_000_000", options, OptionsHint.WIDE_LANE);

        options = serialize("anint = 7 and anint < 5_000_000_000", false, false, true);
        assertIR("(i64 5000000000L)(i32 anint)(sx_i64)(<)"
                + "(i32 7L)(i32 anint)(=)(&&)(ret)");
        assertOptionsHint("mixed AND", options, OptionsHint.WIDE_LANE);

        options = serialize("anint < 5_000_000_000 or anint = 7", false, false, true);
        assertIR("(i32 7L)(i32 anint)(=)"
                + "(i64 5000000000L)(i32 anint)(sx_i64)(<)(||)(ret)");
        assertOptionsHint("mixed OR", options, OptionsHint.WIDE_LANE);

        options = serialize("anint < 5_000_000_000", true, false, true);
        assertOptionsHint("forced scalar", options, OptionsHint.SCALAR);
    }

    @Test
    public void testOptionsDebugFlag() throws Exception {
        int options = serialize("abyte = 0", false, true, false);
        assertOptionsDebug(options, true);

        options = serialize("abyte = 0", false, false, false);
        assertOptionsDebug(options, false);
    }

    @Test
    public void testOptionsForcedScalarModeForByteOrShortArithmetics() throws Exception {
        Map<String, Integer> filterToOptions = new HashMap<>();
        filterToOptions.put("abyte + abyte = 0", 1);
        filterToOptions.put("ashort - ashort = 0", 2);
        filterToOptions.put("abyte * ashort = 0", 2);
        filterToOptions.put("1 * abyte / ashort = 0", 2);
        // BYTE arithmetic still requires scalar promotion when mixed with FLOAT.
        filterToOptions.put("afloat / abyte = 0", 4);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            int options = serialize(entry.getKey(), false, false, false);
            assertOptionsHint(entry.getKey(), options, OptionsHint.SCALAR);
            assertOptionsSize(entry.getKey(), options, entry.getValue());
        }
    }

    @Test
    public void testOptionsMixedSizes() throws Exception {
        Map<String, Integer> filterToOptions = new HashMap<>();
        // 2B
        filterToOptions.put("aboolean or ashort = 0", 2);
        filterToOptions.put("abyte = 0 or ashort = 0", 2);
        // 4B
        filterToOptions.put("anint = 0 or abyte = 0", 4);
        filterToOptions.put("afloat = 0 or abyte = 0", 4);
        // 8B
        filterToOptions.put("along = 0 or ashort = 0", 8);
        filterToOptions.put("adouble = 0 or ashort = 0", 8);
        filterToOptions.put("afloat = 0 or adouble = 0", 8);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            int options = serialize(entry.getKey(), false, false, false);
            assertOptionsHint(entry.getKey(), options, OptionsHint.MIXED_SIZES);
            assertOptionsSize(entry.getKey(), options, entry.getValue());
        }
    }

    @Test
    public void testOptionsNarrowI64ArithmeticUsesWideLane() throws Exception {
        for (String filter : new String[]{
                "anint * along = 0",
                "anint + along = 0"
        }) {
            int options = serialize(filter, false, false, false);
            assertOptionsHint(filter, options, OptionsHint.WIDE_LANE);
        }

        // BYTE/SHORT arithmetic still relies on scalar promotion to INT.
        for (String filter : new String[]{
                "abyte * along = 0",
                "ashort - along = 0"
        }) {
            int options = serialize(filter, false, false, false);
            assertOptionsHint(filter, options, OptionsHint.SCALAR);
        }
    }

    @Test
    public void testOptionsUnsupportedWideLaneShapesStayScalar() throws Exception {
        // A supported widening predicate must not pull an unsupported sibling into the
        // four-lane path. BYTE / SHORT arithmetic still relies on scalar INT promotion.
        for (String filter : new String[]{
                "anint < 5_000_000_000 and abyte * along = 0",
                "afloat < 1.00000003 or ashort - along = 0"
        }) {
            int options = serialize(filter, false, false, false);
            assertOptionsHint(filter, options, OptionsHint.SCALAR);
        }

        // FLOAT/LONG arithmetic and FLOAT IN bind-variable elements are not in the
        // capability allowlist. They must select a scalar execution hint.
        int options = serialize("afloat + along < 1.00000003", false, false, false);
        assertOptionsHint("FLOAT/LONG arithmetic", options, OptionsHint.SCALAR);

        bindVariableService.setDouble("d", 1.00000003);
        options = serialize("afloat in (1.00000003, :d)", false, false, false);
        assertOptionsHint("FLOAT IN bind variable", options, OptionsHint.SCALAR);

        // A non-integer element makes the whole integer IN shape ineligible even though another
        // element requires INT-to-LONG widening. The widening then emits an SX_I64 outside four-lane
        // mode, which forces scalar - so pin SCALAR, not merely "not WIDE_LANE": SINGLE_SIZE and
        // MIXED_SIZES are SIMD hints too and would satisfy the weaker assertion.
        options = serialize("anint in (1, 5_000_000_000, 1.5)", false, false, false);
        assertOptionsHint("mixed integer/float IN", options, OptionsHint.SCALAR);
    }

    @Test
    public void testOptionsNullChecksFlag() throws Exception {
        int options = serialize("abyte = 0", false, false, true);
        assertOptionsNullChecks(options, true);

        options = serialize("abyte = 0", false, false, false);
        assertOptionsNullChecks(options, false);
    }

    @Test
    public void testOptionsScalarFlag() throws Exception {
        int options = serialize("abyte = 0", true, false, false);
        assertOptionsHint(options);
    }

    @Test
    public void testOptionsSingleSize() throws Exception {
        Map<String, Integer> filterToOptions = new HashMap<>();
        // 1B
        filterToOptions.put("not aboolean", 1);
        filterToOptions.put("abyte = 0", 1);
        filterToOptions.put("ageobyte <> null", 1);
        // 2B
        filterToOptions.put("ashort = 0", 2);
        filterToOptions.put("ageoshort <> null", 2);
        filterToOptions.put("achar = 'a'", 2);
        // 4B
        filterToOptions.put("anint = 0", 4);
        filterToOptions.put("ageoint <> null", 4);
        filterToOptions.put("afloat = 0", 4);
        filterToOptions.put("asymbol <> null", 4);
        filterToOptions.put("anint / anint = 0", 4);
        filterToOptions.put("afloat = 0 or anint = 0", 4);
        // 8B
        filterToOptions.put("along = 0", 8);
        filterToOptions.put("ageolong <> null", 8);
        filterToOptions.put("adate <> null", 8);
        filterToOptions.put("atimestamp <> null", 8);
        filterToOptions.put("atimestampns <> null", 8);
        filterToOptions.put("adouble = 0", 8);
        filterToOptions.put("adouble = 0 and along = 0", 8);
        filterToOptions.put("astring = null", 8);
        filterToOptions.put("abinary = null", 8);
        filterToOptions.put("avarchar = null", 8);
        // 16B
        filterToOptions.put("auuid = '11111111-1111-1111-1111-111111111111'", 16);
        filterToOptions.put("auuid = null", 16);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            int options = serialize(entry.getKey(), false, false, false);
            assertOptionsHint(entry.getKey(), options, OptionsHint.SINGLE_SIZE);
            assertOptionsSize(entry.getKey(), options, entry.getValue());
        }
    }

    @Test
    public void testOrChainShortCircuit() throws Exception {
        // Pure OR chain with mixed sizes -> short-circuit with predicate reordering (inverted priority)
        serialize("along = 1 or anint = 2");
        assertIR("(i32 2L)(i32 anint)(=)(||_sc)(i64 1L)(i64 along)(=)(ret)");

        serialize("along = 1 or anint = 2 or ashort = 3");
        assertIR("(i16 3L)(i16 ashort)(=)(||_sc)(i32 2L)(i32 anint)(=)(||_sc)(i64 1L)(i64 along)(=)(ret)");

        // With NOT operator
        serialize("along = 1 or not anint = 2");
        assertIR("(i32 2L)(i32 anint)(=)(!)(||_sc)(i64 1L)(i64 along)(=)(ret)");
    }

    @Test
    public void testOrChainShortCircuitAllPriorities() throws Exception {
        // OR chain covering all 11 priority levels (0-10)
        // Predicates are sorted by descending (inverted) priority (higher value = evaluated first)
        // Inverted order: i128!= > i64!= > i32!= > sym!= > other!= > other_cmp > other= > sym= > i32= > i64= > i128=
        serialize(
                "auuid = '11111111-1111-1111-1111-111111111111' " + // priority 0: i128 eq
                        "or along = 1 " + // priority 1: i64 eq
                        "or anint = 2 " + // priority 2: i32 eq
                        "or asymbol = 'ABC' " + // priority 3: sym eq
                        "or ashort = 3 " + // priority 4: other eq (i16 is "other")
                        "or abyte > 0 " + // priority 5: other comparison (non-eq/neq)
                        "or achar != 'x' " + // priority 6: other neq
                        "or anothersymbol != 'DEF' " + // priority 7: sym neq
                        "or ageoint != #sp05 " + // priority 8: i32 neq
                        "or adate != '1980-01-01' " + // priority 9: i64 neq
                        "or auuid != '22222222-2222-2222-2222-222222222222'" // priority 10: i128 neq
        );
        // Expected order: priority 10 -> 9 -> 8 -> 7 -> 6 -> 5 -> 4 -> 3 -> 2 -> 1 -> 0
        assertIR(
                "(i128 2459565876494606882 2459565876494606882L)(i128 auuid)(<>)(||_sc)" + // priority 10: auuid !=
                        "(i64 315532800000L)(i64 adate)(<>)(||_sc)" + // priority 9: adate !=
                        "(i32 807941L)(i32 ageoint)(<>)(||_sc)" + // priority 8: ageoint !=
                        "(i32 0L)(i32 anothersymbol)(<>)(||_sc)" + // priority 7: anothersymbol != (key 0 for 'DEF')
                        "(i16 120L)(i16 achar)(<>)(||_sc)" + // priority 6: achar != ('x' = 120)
                        "(i8 0L)(i8 abyte)(>)(||_sc)" + // priority 5: abyte >
                        "(i16 3L)(i16 ashort)(=)(||_sc)" + // priority 4: ashort =
                        "(i32 0L)(i32 asymbol)(=)(||_sc)" + // priority 3: asymbol = (key 0 for 'ABC')
                        "(i32 2L)(i32 anint)(=)(||_sc)" + // priority 2: anint =
                        "(i64 1L)(i64 along)(=)(||_sc)" + // priority 1: along =
                        "(i128 1229782938247303441 1229782938247303441L)(i128 auuid)(=)(ret)" // priority 0: auuid =
        );
    }

    @Test
    public void testOrChainShortCircuitSamePriorityOrder() throws Exception {
        // Predicates with the same priority (same type size) should preserve their original order
        // OR chain uses inverted priority (larger sizes first for early success)
        serialize("anint = 1 or adate = '1980-01-01' or along = 3");
        // i64 columns (adate, along) first in original order, then anint (i32)
        assertIR("(i32 1L)(i32 anint)(=)(||_sc)(i64 315532800000L)(i64 adate)(=)(||_sc)(i64 3L)(i64 along)(=)(ret)");

        serialize("anipv4 = null or anint = 2 or adate = '1980-01-01' or along = 4");
        // i32 columns (anipv4, anint) in original order, then i64 columns (adate, along) first in original order
        assertIR("(i32 0L)(i32 anipv4)(=)(||_sc)(i32 2L)(i32 anint)(=)(||_sc)(i64 315532800000L)(i64 adate)(=)(||_sc)(i64 4L)(i64 along)(=)(ret)");

        // Three predicates of same size - order should be preserved
        serialize("anint = 1 or along = 2 or adate = '1980-01-01' or atimestamp = '1980-01-02'");
        assertIR("(i32 1L)(i32 anint)(=)(||_sc)(i64 2L)(i64 along)(=)(||_sc)(i64 315532800000L)(i64 adate)(=)(||_sc)(i64 315619200000000L)(i64 atimestamp)(=)(ret)");
    }

    @Test
    public void testSameSizeNoShortCircuit() throws Exception {
        // Same size columns -> SIMD possible -> no short-circuit
        serialize("along = 1 and adouble = 2.0");
        assertIR("(f64 2.0D)(f64 adouble)(=)(i64 1L)(i64 along)(=)(&&)(ret)");

        serialize("along = 1 or adouble = 2.0");
        assertIR("(f64 2.0D)(f64 adouble)(=)(i64 1L)(i64 along)(=)(||)(ret)");

        serialize("anint = 1 and afloat = 2.0");
        assertIR("(f32 2.0D)(f32 afloat)(=)(i32 1L)(i32 anint)(=)(&&)(ret)");
    }

    @Test
    public void testSingleBooleanColumn() throws Exception {
        serialize("aboolean or not aboolean");
        assertIR("(i8 1L)(i8 aboolean)(=)(!)(i8 1L)(i8 aboolean)(=)(||)(ret)");
    }

    @Test
    public void testSinglePredicateNoShortCircuit() throws Exception {
        // Single predicate doesn't need short-circuit
        serialize("along = 1");
        assertIR("(i64 1L)(i64 along)(=)(ret)");

        serialize("not along = 1");
        assertIR("(i64 1L)(i64 along)(=)(!)(ret)");
    }

    /**
     * Pins the width of every var-size header NULL sentinel at I8.
     * <p>
     * Both backends hand a var-size header back as a 64-bit value: {@code avx2::read_mem_varsize}
     * packs four i64 lanes, and the scalar {@code x86}/{@code aarch64} twin sign-extends the
     * four-byte STRING header into a 64-bit register. An I4 sentinel for STRING would make its
     * {@code IS [NOT] NULL} the only var-size comparison with mismatched operand widths, and
     * {@code avx2::convert()} would harmonise it by emitting an {@code sx_i64} - two
     * {@code vpmovsxdq}, a {@code vpcmpeqd} and a {@code vpblendvb} plus two constant-pool loads -
     * INSIDE the four-lane loop, once per iteration, for a broadcast constant that never changes.
     * That cost is invisible to a results assertion, so pin the widths instead: STRING, BINARY and
     * VARCHAR must all spell the sentinel at the same width their column's lane is read at.
     * <p>
     * The sentinel VALUES differ on purpose - STRING and BINARY compare a length against
     * {@code TableUtils.NULL_LEN}, VARCHAR compares an aux-vector header against
     * {@code VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL} - so only the type codes are asserted
     * alike here. {@link #testStringNullConstant}, {@link #testBinaryNullConstant} and
     * {@link #testVarcharNullConstant} pin the values.
     */
    @Test
    public void testVarSizeNullSentinelsShareTheI64Width() throws Exception {
        final String[][] columns = {
                {"astring", "string_header", "-1"},
                {"abinary", "binary_header", "-1"},
                {"avarchar", "varchar_header", "4"},
        };
        for (String[] col : columns) {
            final String name = col[0];
            final String operand = "(i64 " + col[2] + "L)(" + col[1] + " " + name + ")";
            serialize(name + " is null");
            assertIR(name + " is null", operand + "(=)(ret)");
            serialize(name + " is not null");
            assertIR(name + " is not null", operand + "(<>)(ret)");
        }
    }

    @Test
    public void testStringNullConstant() throws Exception {
        serialize("astring <> null");
        assertIR("(i64 -1L)(string_header astring)(<>)(ret)");
        serialize("astring is not null");
        assertIR("(i64 -1L)(string_header astring)(<>)(ret)");
        serialize("astring = null");
        assertIR("(i64 -1L)(string_header astring)(=)(ret)");
        serialize("astring is null");
        assertIR("(i64 -1L)(string_header astring)(=)(ret)");
        serialize("null <> astring");
        assertIR("(string_header astring)(i64 -1L)(<>)(ret)");
        serialize("null = astring");
        assertIR("(string_header astring)(i64 -1L)(=)(ret)");
    }

    @Test
    public void testTimestampInLiteral() throws Exception {
        serialize("atimestamp in '2020-01-01'");
        assertIR("(i64 1577836800000000L)(i64 atimestamp)(>=)(i64 1577923199999999L)(i64 atimestamp)(<=)(&&)(ret)");
        serialize("atimestamp in '2020-01-01;15s'");
        assertIR("(i64 1577836800000000L)(i64 atimestamp)(>=)(i64 1577836814999999L)(i64 atimestamp)(<=)(&&)(ret)");
        serialize("atimestamp in '2020-01-01T23:59:58;4s;-1d;3'");
        assertIR("(i64 1577750398000000L)(i64 atimestamp)(>=)(i64 1577750401999999L)(i64 atimestamp)(<=)(&&)" +
                "(i64 1577836798000000L)(i64 atimestamp)(>=)(i64 1577836801999999L)(i64 atimestamp)(<=)(&&)" +
                "(i64 1577923198000000L)(i64 atimestamp)(>=)(i64 1577923201999999L)(i64 atimestamp)(<=)(&&)(||)(||)(ret)");
        serialize("along = 42 and atimestamp in '2020-01-01T23:59:58;4s;-1d;3'");
        assertIR("(i64 1577750398000000L)(i64 atimestamp)(>=)(i64 1577750401999999L)(i64 atimestamp)(<=)(&&)" +
                "(i64 1577836798000000L)(i64 atimestamp)(>=)(i64 1577836801999999L)(i64 atimestamp)(<=)(&&)" +
                "(i64 1577923198000000L)(i64 atimestamp)(>=)(i64 1577923201999999L)(i64 atimestamp)(<=)(&&)(||)(||)" +
                "(i64 42L)(i64 along)(=)(&&)(ret)");
    }

    @Test(expected = SqlException.class)
    public void testTimestampInLiteralBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("str", "2020");
        serialize("atimestamp in :str");

        bindVariableService.clear();
        bindVariableService.setStr("str", "2020");
        serialize("atimestampns in :str");
    }

    @Test
    public void testTimestampInLiteralNull() throws Exception {
        serialize("atimestamp in null");
        assertIR("(i64 -9223372036854775808L)(i64 atimestamp)(>=)(i64 -9223372036854775808L)(i64 atimestamp)(<=)(&&)(ret)");
    }

    @Test
    public void testTimestampLiteral() throws Exception {
        serialize("atimestamp = '2023-02-11T11:12:22.116234987Z'");
        assertIR("(i64 1676113942116234L)(i64 atimestamp)(=)(ret)");
        serialize("atimestamp = '2023-02-11T11:12:22.116234Z'");
        assertIR("(i64 1676113942116234L)(i64 atimestamp)(=)(ret)");
        serialize("atimestamp >= '2023-02-11T11:12:22'");
        assertIR("(i64 1676113942000000L)(i64 atimestamp)(>=)(ret)");
        serialize("atimestamp <= '2023-02-11T11'");
        assertIR("(i64 1676113200000000L)(i64 atimestamp)(<=)(ret)");
        serialize("atimestamp > '2023-02-11'");
        assertIR("(i64 1676073600000000L)(i64 atimestamp)(>)(ret)");
        serialize("atimestamp < '2023-02'");
        assertIR("(i64 1675209600000000L)(i64 atimestamp)(<)(ret)");
        serialize("atimestamp != '2023'");
        assertIR("(i64 1672531200000000L)(i64 atimestamp)(<>)(ret)");
    }

    @Test
    public void testUnknownSymbolConstant() throws Exception {
        serialize("asymbol = '" + UNKNOWN_SYMBOL + "'");
        assertIR("(i32 :0)(i32 asymbol)(=)(ret)");

        Assert.assertEquals(1, bindVarFunctions.size());
        Assert.assertEquals(ColumnType.SYMBOL, bindVarFunctions.get(0).getType());
        Assert.assertEquals(UNKNOWN_SYMBOL, bindVarFunctions.get(0).getStrA(null));
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBinaryEquality() throws Exception {
        serialize("abinary = abinary2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBinaryInequality() throws Exception {
        serialize("abinary <> abinary2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBindVariableType1() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("astring", "foobar");
        serialize("astring = :astring");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBindVariableType2() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("avarchar", "foobar");
        serialize("avarchar = :avarchar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBitwiseOperator() throws Exception {
        serialize("~abyte <> 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBooleanColumnInNumericContext() throws Exception {
        serialize("aboolean = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedByteNullConstant() throws Exception {
        serialize("abyte = null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedCharColumnInNumericContext() throws Exception {
        serialize("achar = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedCharConstantInNumericContext() throws Exception {
        serialize("along = 'x'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedColumnType1() throws Exception {
        serialize("astring = 'a'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedColumnType2() throws Exception {
        serialize("avarchar = 'a'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedConstantPredicate() throws Exception {
        serialize("2 > 1");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedConstantPredicate2() throws Exception {
        serialize("anint = 0 or 2 > 1");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFalseConstantInNumericContext() throws Exception {
        serialize("along = false");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFloatConstantInByteContext() throws Exception {
        serialize("abyte > 1.5");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFloatConstantInShortContext() throws Exception {
        serialize("ashort > 1.5");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFunctionToken() throws Exception {
        serialize("atimestamp + now() > 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashColumnInNumericContext() throws Exception {
        serialize("ageolong = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashConstantTooFewBits() throws Exception {
        serialize("ageolong = ##10001");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashConstantTooManyChars() throws Exception {
        serialize("ageolong = #sp052w92p1p8889");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedInvalidGeoHashConstant() throws Exception {
        serialize("ageolong = ##11211");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedLong256Constant() throws Exception {
        serialize("along = 0x123");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedBooleanAndNumericColumns() throws Exception {
        serialize("aboolean = abyte");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedCharAndNumericColumns() throws Exception {
        serialize("achar = anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedGeoHashAndNumericColumns() throws Exception {
        serialize("ageoint = along");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedStringAndCharColumns() throws Exception {
        serialize("astring = achar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedStringAndVarcharColumns() throws Exception {
        serialize("astring = avarchar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedSymbolAndNumericColumns() throws Exception {
        serialize("asymbol = anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedUuidAndNumericColumns() throws Exception {
        serialize("auuid = anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedUuidAndStringColumns() throws Exception {
        serialize("auuid = astring");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedUuidAndVarcharColumns() throws Exception {
        serialize("auuid = avarchar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedVarcharAndCharColumns() throws Exception {
        serialize("avarchar = achar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedVarcharAndStringColumns() throws Exception {
        serialize("avarchar = astring");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedOperatorToken() throws Exception {
        serialize("asymbol in (select rnd_symbol('A','B','C') from long_sequence(10))");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedShortNullConstant() throws Exception {
        serialize("ashort = null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSingleConstantPredicate() throws Exception {
        serialize("true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSingleNonBooleanColumnPredicate() throws Exception {
        serialize("anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStringConstant() throws Exception {
        serialize("achar = 'abc'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStringEquality() throws Exception {
        serialize("astring = astring2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStringInequality() throws Exception {
        serialize("astring <> astring2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStringIntComparison() throws Exception {
        serialize("astring >= anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSymbolIntComparison() throws Exception {
        serialize("asymbol >= anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedTrueConstantInNumericContext() throws Exception {
        serialize("along = true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedUuidColumnInNumericContext() throws Exception {
        serialize("auuid = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedUuidConstantInNumericContext() throws Exception {
        serialize("along = '11111111-1111-1111-1111-111111111111'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedVarcharConstant() throws Exception {
        serialize("achar = 'abc'::varchar");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedVarcharEquality() throws Exception {
        serialize("avarchar = avarchar2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedVarcharInequality() throws Exception {
        serialize("avarchar <> avarchar2");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedVarcharIntComparison() throws Exception {
        serialize("avarchar >= anint");
    }

    @Test
    public void testUuidConstant() throws Exception {
        serialize("auuid = '00000000-0000-0000-0000-000000000000'");
        assertIR("(i128 0 0L)(i128 auuid)(=)(ret)");
    }

    @Test
    public void testVarcharNullConstant() throws Exception {
        serialize("avarchar <> null");
        assertIR("(i64 4L)(varchar_header avarchar)(<>)(ret)");
        serialize("avarchar is not null");
        assertIR("(i64 4L)(varchar_header avarchar)(<>)(ret)");
        serialize("avarchar = null");
        assertIR("(i64 4L)(varchar_header avarchar)(=)(ret)");
        serialize("avarchar is null");
        assertIR("(i64 4L)(varchar_header avarchar)(=)(ret)");
        serialize("null = avarchar");
        assertIR("(varchar_header avarchar)(i64 4L)(=)(ret)");
        serialize("null <> avarchar");
        assertIR("(varchar_header avarchar)(i64 4L)(<>)(ret)");
    }

    @Test
    public void testWideLanePredictionMissKeepsShortCircuit() throws Exception {
        // A wide-lane prediction that emits no conversion must not cost AND_SC short-circuiting
        // and predicate reordering: the backend runs the same scalar loop either way (compiler.cpp
        // takes avx2_loop only for the single-size and wide-lane hints), so suppressing the
        // short-circuit buys nothing and costs an evaluation of every conjunct on every row.
        int options = serialize("adouble > 1.1 and anint = 5", false, false, false);
        assertIR("(i32 5L)(i32 anint)(=)(&&_sc)(f64 1.1D)(f64 adouble)(>)(ret)");
        assertOptionsHint("DOUBLE column vs inexact constant", options, OptionsHint.MIXED_SIZES);

        // Same shape without a float in sight: the fold is read at I8 by the wide-lane trigger and
        // at I4 by the width marker, so nothing widens here either.
        options = serialize("anint = (2_000_000_000 + 2_000_000_000) and along = 7", false, false, false);
        assertIR("(i64 7L)(i64 along)(=)(&&_sc)(i32 -294967296L)(i32 anint)(=)(ret)");
        assertOptionsHint("cancelling INT fold", options, OptionsHint.MIXED_SIZES);

        // OR chains ride the same gate, so pin OR_SC too.
        options = serialize("adouble > 1.1 or anint = 5", false, false, false);
        assertIR("(f64 1.1D)(f64 adouble)(>)(||_sc)(i32 5L)(i32 anint)(=)(ret)");
        assertOptionsHint("OR chain", options, OptionsHint.MIXED_SIZES);

        // Control: an F4 leaf against a constant that no float reproduces DOES widen, so this one
        // must keep the four-lane path and its lane-wise boolean operators. The second conjunct is
        // 8 bytes wide on purpose - against a 4-byte one the sizes match, the mixed-size detector
        // could not fire whatever the gate answered, and the case would pass even with the gate
        // wired shut. Mixed widths make it bite: were the gate to under-report here, the
        // short-circuit path would run, markFloatCmpConst would still widen, and the wide-lane
        // guard in serializePredicatesAndSc would throw.
        options = serialize("afloat < 1.00000003 and along = 5", false, false, false);
        assertIR("(i64 5L)(i64 along)(=)(f64 1.00000003D)(f32 afloat)(<)(&&)(ret)");
        assertOptionsHint("genuine wide-lane conversion", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testWideLaneSourceSuppressesShortCircuitFilterWide() throws Exception {
        // In wide-lane mode a conjunct owning a conversion source suppresses the mixed-size
        // detector for the WHOLE predicate - serialize()'s gate asks hasWideLaneConversionSource()
        // about the root node - so a chain whose other conjuncts are ordinary comparisons emits
        // plain AND in traversal order where it would otherwise emit AND_SC in sortPredicates
        // order. That reach is deliberate rather than an oversight of the gate: a filter runs ONE
        // backend loop - compiler.cpp picks avx2_loop or scalar_loop once per istream - so the
        // chain either short-circuits on the scalar loop or vectorizes on the four-lane one, whole.
        // The measurement behind the choice is recorded at serialize()'s detector
        // gate; this test pins the choice itself, so a reader can see it was made, and sees it move
        // if it ever changes.
        int options = serialize("afloat * 2.0 > 1.5 and along > 5", false, false, false);
        assertIR("mixed-width chain, one conjunct owns the source",
                "(i64 5L)(i64 along)(>)(f32 1.5D)(f64 2.0D)(f32 afloat)(*)(>)(&&)(ret)");
        assertOptionsHint("mixed-width chain, one conjunct owns the source", options, OptionsHint.WIDE_LANE);

        // The short-circuit opcodes fit the shape - it is the MODE that withholds them. serialize()
        // computes isWideLaneMode as "!forceScalar && ...", so the forced-scalar pass is not in the
        // mode and the same chain emits AND_SC in priority order: byte for byte what the
        // suppression above costs.
        options = serialize("afloat * 2.0 > 1.5 and along > 5", true, false, false);
        assertIR("same chain, forced scalar",
                "(f32 1.5D)(f64 2.0D)(f32 afloat)(*)(>)(&&_sc)(i64 5L)(i64 along)(>)(ret)");
        assertOptionsHint("same chain, forced scalar", options, OptionsHint.SCALAR);

        // afloat and anint are both four bytes, so the detector would answer "not mixed" even if
        // the gate let it run: this chain had no short-circuit path to lose, and emitted plain AND
        // before the (f32, f64) pairing was admitted to the four-lane loop too.
        options = serialize("afloat * 2.0 > 1.5 and anint > 5", false, false, false);
        assertIR("uniform-width chain",
                "(i32 5L)(i32 anint)(>)(f32 1.5D)(f64 2.0D)(f32 afloat)(*)(>)(&&)(ret)");
        assertOptionsHint("uniform-width chain", options, OptionsHint.WIDE_LANE);

        // Control on the other side of the gate: neither conjunct pairs an F4 leaf with a widening
        // constant - 1.5 is exact in f32 - so hasWideLaneConversionSource() answers false, the
        // detector runs, and the mixed widths keep the chain short-circuiting. The same chain
        // shape, the same columns, the opposite answer.
        options = serialize("afloat > 1.5 and along > 5", false, false, false);
        assertIR("mixed-width chain, no conversion source",
                "(f32 1.5D)(f32 afloat)(>)(&&_sc)(i64 5L)(i64 along)(>)(ret)");
        assertOptionsHint("mixed-width chain, no conversion source", options, OptionsHint.MIXED_SIZES);

        // Control on the mode rather than the source: the conversion source is still there, but a
        // NULL comparison leaves the filter wide-lane INELIGIBLE, so isWideLaneMode is false, the
        // gate's || short-circuits before it asks about the source, the detector runs, and the
        // chain short-circuits. The gate reads that flag first, and being in the mode is not the
        // same as reaching the four-lane loop: testFloatArithI64ConstantForcesScalarNotWideLane
        // above enters the mode, emits no conversion, loses its short circuit and lands on the
        // scalar loop with a plain (&&). See the prediction note at serialize()'s detector gate.
        options = serialize("afloat * 2.0 > 1.5 and along = null", false, false, true);
        assertIR("wide-lane ineligible chain",
                "(i64 -9223372036854775808L)(i64 along)(=)(&&_sc)(f32 1.5D)(f64 2.0D)(f32 afloat)(*)(>)(ret)");
        assertOptionsHint("wide-lane ineligible chain", options, OptionsHint.SCALAR);
    }

    @Test
    public void testIntCmpFloatColumnWidensIntToI64() throws Exception {
        // QuestDB registers no (FLOAT, FLOAT) comparison factory, so `int_col <op> float_col`
        // resolves to the (DOUBLE, DOUBLE) one and reads the INT through IntFunction#getDouble:
        // the Java filter compares both operands at f64. The type observer sizes INT and FLOAT
        // alike (4 bytes each), so hasMixedSizes() answered false, the filter took a single-size
        // hint, and BOTH backends' convert() rounded the INT lane to f32 instead -
        // jit/impl/avx2.h's cvt_itof and jit/impl/x86.h's int32_to_float. Every INT above 2^24
        // then compared as a different value: (float) 16777217 is 16777216. Sign-extending the
        // INT leaf routes the pairing through the (i64, f32) arm, which promotes both sides
        // to f64 in both backends.
        //
        // That arm is four-lane only (jit/avx2.h:698-704), so the pairing also has to enter wide-lane
        // mode to reach it - isWideLaneIntCmpFloatLeafPair is what admits it. The IR below is the
        // same one this shape emitted while it was hint-forced onto the scalar loop at one row per
        // iteration; only the exec hint moves. The rows that loop returns are pinned against the
        // Java filter by CompiledFilterRegressionTest#testIntCmpFloatColumnWideLaneMatchesJavaFilter.
        final String[] ops = {"=", "<>", "<", "<=", ">", ">="};
        for (int i = 0; i < ops.length; i++) {
            final String op = ops[i];
            int options = serialize("anint " + op + " afloat", false, false, true);
            assertIR("anint " + op + " afloat", "(f32 afloat)(i32 anint)(sx_i64)(" + op + ")(ret)");
            assertOptionsHint("anint " + op + " afloat", options, OptionsHint.WIDE_LANE);

            // The reversed spelling has to widen too - the review measured `f >= i` wrong as well.
            options = serialize("afloat " + op + " anint", false, false, true);
            assertIR("afloat " + op + " anint", "(i32 anint)(sx_i64)(f32 afloat)(" + op + ")(ret)");
            assertOptionsHint("afloat " + op + " anint", options, OptionsHint.WIDE_LANE);
        }

        // A bind variable is observed exactly like a column, so both bind-variable spellings are
        // in the population and take the same widening.
        bindVariableService.clear();
        bindVariableService.setInt("iv", 16_777_217);
        bindVariableService.setFloat("fv", 16_777_216.0f);
        int options = serialize("anint > :fv", false, false, true);
        assertIR("anint > :fv", "(f32 :0)(i32 anint)(sx_i64)(>)(ret)");
        assertOptionsHint("anint > :fv", options, OptionsHint.WIDE_LANE);
        options = serialize(":iv > afloat", false, false, true);
        assertIR(":iv > afloat", "(f32 afloat)(i32 :0)(sx_i64)(>)(ret)");
        assertOptionsHint(":iv > afloat", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat > :iv", false, false, true);
        assertIR("afloat > :iv", "(i32 :0)(sx_i64)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > :iv", options, OptionsHint.WIDE_LANE);

        // An F4 arithmetic peer computes at f32 in the Java filter too - `+(FF)` exists and
        // `f * 2` types as FLOAT - so only the INT side has to move.
        options = serialize("anint > afloat * 2", false, false, true);
        assertIR("anint > afloat * 2", "(i32 2L)(f32 afloat)(*)(i32 anint)(sx_i64)(>)(ret)");
        assertOptionsHint("anint > afloat * 2", options, OptionsHint.WIDE_LANE);

        // The single-value IN form is the same pairing spelled differently, and it keeps the
        // SCALAR loop rather than following its comparison-spelled sibling onto the four-lane one:
        // isWideLaneInEligible's float arm admits an element only when it is a numeric CONSTANT or
        // NULL, so the filter is wide-lane ineligible and the emitted SX_I64 sets forceScalarMode
        // in maybeEmitI64Widening. Same IR, same rows, different loop.
        options = serialize("afloat in (anint)", false, false, true);
        assertIR("afloat in (anint)", "(i32 anint)(sx_i64)(f32 afloat)(=)(ret)");
        assertOptionsHint("afloat in (anint)", options, OptionsHint.SCALAR);

        // NOT wraps the comparison inside one predicate, so the mark still has to reach it.
        options = serialize("not (anint > afloat)", false, false, true);
        assertIR("not (anint > afloat)", "(f32 afloat)(i32 anint)(sx_i64)(>)(!)(ret)");
        assertOptionsHint("not (anint > afloat)", options, OptionsHint.WIDE_LANE);

        // Controls - every other integer width against FLOAT is already exact and must not move.
        // BYTE and SHORT span at most +-32767, so int32_to_float / cvt_itof reproduce them
        // exactly; LONG and DOUBLE peers already take a convert() arm that lands on f64.
        options = serialize("abyte > afloat", false, false, true);
        assertIR("abyte > afloat", "(f32 afloat)(i8 abyte)(>)(ret)");
        assertOptionsHint("abyte > afloat", options, OptionsHint.MIXED_SIZES);
        options = serialize("ashort > afloat", false, false, true);
        assertIR("ashort > afloat", "(f32 afloat)(i16 ashort)(>)(ret)");
        assertOptionsHint("ashort > afloat", options, OptionsHint.MIXED_SIZES);
        options = serialize("along > afloat", false, false, true);
        assertIR("along > afloat", "(f32 afloat)(i64 along)(>)(ret)");
        assertOptionsHint("along > afloat", options, OptionsHint.MIXED_SIZES);
        options = serialize("anint > adouble", false, false, true);
        assertIR("anint > adouble", "(f64 adouble)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > adouble", options, OptionsHint.MIXED_SIZES);

        // Controls - the constant pairings this PR already widens keep their four-lane loop.
        options = serialize("anint > 16777216.0", false, false, true);
        assertIR("anint > 16777216.0", "(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(ret)");
        assertOptionsHint("anint > 16777216.0", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat > 16_777_217", false, false, true);
        assertIR("afloat > 16_777_217", "(i64 16777217L)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > 16_777_217", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testNarrowIntArithCmpFloatColumnWidensSubtreeResult() throws Exception {
        // An INT-width arithmetic subtree wraps at i32 and the Java filter reads its RESULT
        // through IntFunction#getDouble, i.e. at f64. SX_I64 is a STACK opcode - both backends
        // pop the top of the value stack and sign-extend it when it is i8/i16/i32 (the
        // opcodes::Sx_I64 arm of emit_code in jit/x86.h and jit/aarch64.h) - so emitting it
        // AFTER the arithmetic operator widens the wrapped RESULT. That is exactly the Java
        // semantics: the operands stay narrow, the operation
        // still wraps at 32 bits, and only the result is promoted before the comparison.
        // Widening the OPERANDS instead would turn a wrapping int multiply into a non-wrapping
        // long one, so the SX_I64 must sit after the operator, never before it.
        final String[][] widened = {
                {"anint * 2 > afloat", "(f32 afloat)(i32 2L)(i32 anint)(*)(sx_i64)(>)(ret)"},
                {"anint + 1 > afloat", "(f32 afloat)(i32 1L)(i32 anint)(+)(sx_i64)(>)(ret)"},
                {"-anint > afloat", "(f32 afloat)(i32 anint)(neg)(sx_i64)(>)(ret)"},
                {"afloat < anint / 2", "(i32 2L)(i32 anint)(/)(sx_i64)(f32 afloat)(<)(ret)"},
                {"abyte + 2_000_000_000 > afloat", "(f32 afloat)(i32 2000000000L)(i8 abyte)(+)(sx_i64)(>)(ret)"},
                {"ashort * 100_000 > afloat", "(f32 afloat)(i32 100000L)(i16 ashort)(*)(sx_i64)(>)(ret)"},
                {"afloat in (anint + 1)", "(i32 1L)(i32 anint)(+)(sx_i64)(f32 afloat)(=)(ret)"},
        };
        for (int i = 0; i < widened.length; i++) {
            final int options = serialize(widened[i][0], false, false, true);
            assertIR(widened[i][0], widened[i][1]);
            // SX_I64 outside wide-lane mode is implemented by the scalar backend only -
            // jit/avx2.h's Sx_I64 arm bails out when wide_lane is false - so the emission has
            // to carry the filter to the scalar loop, exactly as maybeEmitI64Widening does for
            // a leaf.
            assertOptionsHint(widened[i][0], options, OptionsHint.SCALAR);
        }

        // Every operator and both operand orders take the same widening.
        final String[] ops = {"=", "<>", "<", "<=", ">", ">="};
        for (int i = 0; i < ops.length; i++) {
            final String op = ops[i];
            serialize("anint * 2 " + op + " afloat", false, false, true);
            assertIR("anint * 2 " + op + " afloat", "(f32 afloat)(i32 2L)(i32 anint)(*)(sx_i64)(" + op + ")(ret)");
            serialize("afloat " + op + " anint * 2", false, false, true);
            assertIR("afloat " + op + " anint * 2", "(i32 2L)(i32 anint)(*)(sx_i64)(f32 afloat)(" + op + ")(ret)");
        }

        // Controls - a genuinely 64-bit arithmetic peer already lands on the (i64, f32) arm, and
        // a DOUBLE peer on (i32, f64); neither loses the JIT.
        serialize("anint + along > afloat", false, false, true);
        assertIR("anint + along > afloat", "(f32 afloat)(i64 along)(i32 anint)(sx_i64)(+)(>)(ret)");
        serialize("anint * 2 > adouble", false, false, true);
        assertIR("anint * 2 > adouble", "(f64 adouble)(i32 2L)(i32 anint)(*)(>)(ret)");
        // Narrow arithmetic against a narrow peer is untouched: no FLOAT operand, no hazard.
        serialize("anint * 2 > along", false, false, true);
        assertIR("anint * 2 > along", "(i64 along)(i32 2L)(i32 anint)(*)(>)(ret)");
        // Control: a narrow subtree that cannot reach 2^24 has an exact float for every value it
        // can take, so it keeps the JIT untouched.
        serialize("abyte + 100 > afloat", false, false, true);
        assertIR("abyte + 100 > afloat", "(f32 afloat)(i32 100L)(i8 abyte)(+)(>)(ret)");
        serialize("-abyte > afloat", false, false, true);
        assertIR("-abyte > afloat", "(f32 afloat)(i8 abyte)(neg)(>)(ret)");
        serialize("1000 * 1000 > afloat", false, false, true);
        assertIR("1000 * 1000 > afloat", "(f32 afloat)(i32 1000L)(i32 1000L)(*)(>)(ret)");
        // Control: a PURE-CONSTANT chain that DOES exceed 2^24 folds to the single I8 immediate
        // the Java filter's own IntConstant carries (258_558 * -259_815 wraps to 1_542_229_966),
        // so it needs no SX_I64 - and stops rounding through cvt_itof.
        serialize("afloat <= 258_558 * -259_815", false, false, true);
        assertIR("afloat <= 258_558 * -259_815", "(i64 1542229966L)(f32 afloat)(<=)(ret)");
        // Control: a pure-constant chain whose INT-width fold lands ON the NULL sentinel keeps its
        // per-operation / immediate IR. Numbers.intToDouble(INT_NULL) is NaN on the Java side and
        // int32_to_float(INT_NULL) is NaN in both backends, so the pairing already agrees and
        // there is nothing to widen.
        int sentinelOptions = serialize("afloat <= 1_073_741_824 * 2", false, false, true);
        assertIR("afloat <= 1_073_741_824 * 2", "(i32 -2147483648L)(f32 afloat)(<=)(ret)");
        assertOptionsHint("afloat <= 1_073_741_824 * 2", sentinelOptions, OptionsHint.SINGLE_SIZE);
        // A pure-constant subtree that BOTH folds decline - a zero divisor at INT width and at LONG
        // width alike - takes the widening and the scalar loop instead, because
        // isConstantArithSubtree answers through tryFoldConstantArith, which throws the same
        // NumericException for a zero divisor as for a column. descend() emits the per-operation
        // IR, visit() reaches the node and consumes the mark, so the outstanding-mark gate never
        // fires and the rows stay right: int32_div answers INT_NULL, the SX_I64 carries it to
        // LONG_NULL and int64_to_double reads that as NaN. DivInt#getInt folds to INT_NULL on the
        // Java side and Numbers.intToDouble turns THAT into the same NaN. Pinned so a future
        // relaxation - which would put the pairing back on cvt_itof - is a deliberate act rather
        // than a side effect.
        int zeroDivisorOptions = serialize("afloat > 10 / 0", false, false, true);
        assertIR("afloat > 10 / 0", "(i32 0L)(i32 10L)(/)(sx_i64)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > 10 / 0", zeroDivisorOptions, OptionsHint.SCALAR);
        zeroDivisorOptions = serialize("afloat > 10 / (3 - 3)", false, false, true);
        assertIR("afloat > 10 / (3 - 3)", "(i32 3L)(i32 3L)(-)(i32 10L)(/)(sx_i64)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > 10 / (3 - 3)", zeroDivisorOptions, OptionsHint.SCALAR);
        // The IN spelling routes through the same marker, and its element reaches visit() too.
        zeroDivisorOptions = serialize("afloat in (10 / 0, 2.5)", false, false, true);
        assertIR("afloat in (10 / 0, 2.5)",
                "(f32 2.5D)(f32 afloat)(=)(i32 0L)(i32 10L)(/)(sx_i64)(f32 afloat)(=)(||)(ret)");
        assertOptionsHint("afloat in (10 / 0, 2.5)", zeroDivisorOptions, OptionsHint.SCALAR);
    }

    @Test
    public void testNarrowIntArithCmpFloatMagnitudeBoundPinsLeafBoundsAndDivisor() throws Exception {
        // intCmpFloatMagnitudeBound is what lets a narrow arithmetic subtree KEEP its f32 pairing:
        // markIntCmpFloatOperand skips the SX_I64 when the bound is at most 2^24, because every
        // value such a subtree can take has an exact 32-bit float. The bound therefore has to be
        // right in BOTH directions. Too LARGE only costs an unnecessary widening - the pairing
        // still answers what the Java filter answers, it just loses the vectorized loop. Too
        // SMALL drops a widening that was needed, the INT side rounds through cvt_itof, and the
        // filter returns different rows above 2^24. CompiledFilterRegressionTest's
        // "narrow int arith magnitude bound vs float column pins boundary rows" test asserts the
        // absolute rows for that direction; this one asserts the IR the decision produces.
        //
        // Every case below sits exactly one step away from flipping the decision, so a bound that
        // moves by one shows up here as a lost or a spurious SX_I64.
        final String[][] cases = {
                // The I1 leaf bound is 128, not 127: BYTE spans [-128, 127]. 128 * 131_072 IS
                // 2^24, the largest bound markIntCmpFloatOperand still calls exact, so the
                // subtree keeps its f32 pairing. A leaf bound of 129 carries the product past
                // 2^24 and would emit an SX_I64 here.
                // The same case reads from markIntCmpFloatOperand's side too: its own test is "at
                // most 2^24", not "below 2^24", so a bound landing ON the limit keeps the pairing.
                {"afloat < abyte * 131_072", "(i32 131072L)(i8 abyte)(*)(f32 afloat)(<)(ret)"},
                // One more unit of magnitude on top of that product crosses 2^24, so the same
                // shape DOES widen. A leaf bound of 127 would leave the sum at 16_646_145 and
                // drop this SX_I64 - the wrong-rows direction.
                {"afloat < abyte * 131_072 + 1",
                        "(i32 1L)(i32 131072L)(i8 abyte)(*)(+)(sx_i64)(f32 afloat)(<)(ret)"},
                // The I2 leaf bound is 32_768, not 32_767: SHORT spans [-32_768, 32_767], and
                // 32_768 * 512 IS 2^24. The same pair of cases from the same two sides.
                {"afloat < ashort * 512", "(i32 512L)(i16 ashort)(*)(f32 afloat)(<)(ret)"},
                {"afloat < ashort * 512 + 1",
                        "(i32 1L)(i32 512L)(i16 ashort)(*)(+)(sx_i64)(f32 afloat)(<)(ret)"},
                // The recursion's lhs early-out fires strictly ABOVE 2^24 too: an operand sitting
                // exactly on it still combines with its sibling instead of collapsing the whole
                // bound to Long.MAX_VALUE.
                {"afloat < abyte * 131_072 + 0",
                        "(i32 0L)(i32 131072L)(i8 abyte)(*)(+)(f32 afloat)(<)(ret)"},
                // The rhs early-out is a separate test on the same boundary, reached by putting
                // the bounded product on the right of the operator.
                {"afloat < 0 + abyte * 131_072",
                        "(i32 131072L)(i8 abyte)(*)(i32 0L)(+)(f32 afloat)(<)(ret)"},
                // The division arm. Integer division never grows the magnitude, so a non-zero
                // integer CONSTANT divisor carries the numerator's own bound through and the
                // pairing stays at f32. Without the arm the token would fall through to the
                // final Long.MAX_VALUE and every such shape would widen.
                {"afloat < abyte / 2", "(i32 2L)(i8 abyte)(/)(f32 afloat)(<)(ret)"},
                // A divisor of 1 is the smallest the arm accepts.
                {"afloat < ashort / 1", "(i32 1L)(i16 ashort)(/)(f32 afloat)(<)(ret)"},
                // constantMagnitudeBound unwraps a unary minus, so a negative divisor bounds the
                // quotient exactly as its magnitude does.
                {"afloat < abyte / -3", "(i32 -3L)(i8 abyte)(/)(f32 afloat)(<)(ret)"},
                // A numerator whose own bound is exactly 2^24 passes the lhs early-out and then
                // takes the division arm, so the two boundary tests compose.
                {"afloat < abyte * 131_072 / 1",
                        "(i32 1L)(i32 131072L)(i8 abyte)(*)(/)(f32 afloat)(<)(ret)"},
                // Controls - the two ways the divisor check declines, both of which have to
                // collapse the bound. A ZERO divisor makes DivInt#getInt answer INT_NULL, whose
                // magnitude is 2^31, so the quotient is not bounded by the numerator at all.
                {"afloat < abyte / 0", "(i32 0L)(i8 abyte)(/)(sx_i64)(f32 afloat)(<)(ret)"},
                // A divisor that is not a constant reports Long.MAX_VALUE rather than a
                // magnitude, and it can be zero at runtime, so it declines for the same reason.
                {"afloat < abyte / ashort", "(i16 ashort)(i8 abyte)(/)(sx_i64)(f32 afloat)(<)(ret)"},
        };
        for (int i = 0; i < cases.length; i++) {
            serialize(cases[i][0], false, false, true);
            assertIR(cases[i][0], cases[i][1]);
        }

        // A pure-constant numerator is the one division shape whose predicate carries no narrow
        // int COLUMN, so it is also the one where the arm buys a VECTORIZED loop rather than
        // merely one saved instruction: TypesObserver#hasNarrowInt forces every BYTE / SHORT
        // arithmetic filter above onto the scalar backend whatever the bound reports. The bound
        // check runs before markFoldedI64ConstArith, so the subtree also keeps its per-operation
        // IR instead of folding to an I8 immediate.
        final int options = serialize("afloat > 10 / 2", false, false, true);
        assertIR("afloat > 10 / 2", "(i32 2L)(i32 10L)(/)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > 10 / 2", options, OptionsHint.SINGLE_SIZE);
    }

    @Test
    public void testDoubleConstantInFourByteArithmeticEmitsAtF8() throws Exception {
        // A DOUBLE-spelled literal ('.', 'e' or 'E' in the token) makes the arithmetic node it
        // sits in a DOUBLE one in the Java filter: "+(FF)" cannot take a DOUBLE operand, so
        // FLOAT + DOUBLE resolves to "+(DD)" and the whole subtree evaluates at f64. The type
        // observer sees columns and bind variables only, so a predicate whose widest source is a
        // 4-byte INT or FLOAT column typed that literal down to F4 and the backend then ran the
        // ADD / SUB / MUL / DIV at f32 - a different computation, not merely a rounded bound.
        // Emitting the literal at F8 puts the node back on the f64 arm of convert(): the peer
        // promotes through float_to_double / int32_to_double and the operator dispatches
        // double_add and friends.
        // The third column is the loop the widened immediate may ride. An 8-byte immediate against
        // 4-byte lanes must not ride a vectorized loop that steps eight 32-bit lanes:
        // avx2::convert() DECLINES an (i32, f64) pairing outside the four-lane loop
        // (jit/avx2.h:680-686), which costs the filter its compiled backend, and
        // hasWidthChangingI64WidenConstant() carries that to getExecHint(). The FOUR-lane loop
        // is a different matter - its lanes are
        // eight bytes wide whatever the observed columns are - so
        // a shape isWideLaneEligible() admits runs there rather than falling all the way to scalar.
        // The scalar rows below are the shapes eligibility declines: an INT column under the
        // DOUBLE-width node, which needs the (i32, f64) pairing admitted to wide-lane eligibility
        // first (the same deferral SYMBOL still carries; the (i64, f32) pairing no longer does -
        // isWideLaneIntCmpFloatLeafPair admits it).
        final Object[][] widened = {
                {"afloat + 1.0 > 16777216.5", "(f64 1.67772165E7D)(f64 1.0D)(f32 afloat)(+)(>)(ret)", OptionsHint.WIDE_LANE},
                {"16777216.5 < afloat + 1.0", "(f64 1.0D)(f32 afloat)(+)(f64 1.67772165E7D)(<)(ret)", OptionsHint.WIDE_LANE},
                {"anint + 1.0 > 16777216.5", "(f64 1.67772165E7D)(f64 1.0D)(i32 anint)(+)(>)(ret)", OptionsHint.SCALAR},
                {"anint / 2.0 > 5", "(i32 5L)(f64 2.0D)(i32 anint)(/)(>)(ret)", OptionsHint.SCALAR},
                {"afloat + 1.0 > afloat", "(f32 afloat)(f64 1.0D)(f32 afloat)(+)(>)(ret)", OptionsHint.WIDE_LANE},
                {"afloat * 3.0 > 1.5", "(f32 1.5D)(f64 3.0D)(f32 afloat)(*)(>)(ret)", OptionsHint.WIDE_LANE},
                {"3 * 0.1 > afloat", "(f32 afloat)(f64 0.1D)(i32 3L)(*)(>)(ret)", OptionsHint.SCALAR},
                {"afloat + 1.0 + 1.0 > 1.5", "(f32 1.5D)(f64 1.0D)(f64 1.0D)(f32 afloat)(+)(+)(>)(ret)", OptionsHint.WIDE_LANE},
                {"afloat + -1.0 > 1.5", "(f32 1.5D)(f64 -1.0D)(f32 afloat)(+)(>)(ret)", OptionsHint.WIDE_LANE},
                {"-(afloat + 1.0) > 1.5", "(f32 1.5D)(f64 1.0D)(f32 afloat)(+)(neg)(>)(ret)", OptionsHint.WIDE_LANE},
                {"anint > afloat + 1.0", "(f64 1.0D)(f32 afloat)(+)(i32 anint)(>)(ret)", OptionsHint.SCALAR},
                {"anint * 2 > afloat * 1.0", "(f64 1.0D)(f32 afloat)(*)(i32 2L)(i32 anint)(*)(>)(ret)", OptionsHint.SCALAR},
                {"afloat + 1.0 > 5_000_000_001", "(i64 5000000001L)(f64 1.0D)(f32 afloat)(+)(>)(ret)", OptionsHint.WIDE_LANE},
                {"afloat + 1.0 in (16777216.5)", "(f64 1.67772165E7D)(f64 1.0D)(f32 afloat)(+)(=)(ret)", OptionsHint.WIDE_LANE},
        };
        for (int i = 0; i < widened.length; i++) {
            final String query = (String) widened[i][0];
            final int options = serialize(query, false, false, true);
            assertIR(query, (String) widened[i][1]);
            assertOptionsHint(query, options, (OptionsHint) widened[i][2]);
        }

        // Every operator and both operand orders take the same widening.
        final String[] ops = {"=", "<>", "<", "<=", ">", ">="};
        for (int i = 0; i < ops.length; i++) {
            final String op = ops[i];
            serialize("afloat + 1.0 " + op + " 16777216.5", false, false, true);
            assertIR("afloat + 1.0 " + op + " 16777216.5",
                    "(f64 1.67772165E7D)(f64 1.0D)(f32 afloat)(+)(" + op + ")(ret)");
            serialize("16777216.5 " + op + " afloat + 1.0", false, false, true);
            assertIR("16777216.5 " + op + " afloat + 1.0",
                    "(f64 1.0D)(f32 afloat)(+)(f64 1.67772165E7D)(" + op + ")(ret)");
        }

        // A multi-element IN over a DOUBLE-width key widens every element that has no exact float.
        serialize("afloat + 1.0 in (16777216.5, 2.5)", false, false, true);
        assertIR("afloat + 1.0 in (16777216.5, 2.5)",
                "(f32 2.5D)(f64 1.0D)(f32 afloat)(+)(=)(f64 1.67772165E7D)(f64 1.0D)(f32 afloat)(+)(=)(||)(ret)");

        // A wide-lane filter keeps the four-lane loop: its lanes are eight bytes wide whatever the
        // observed columns are, and avx2::convert() carries (f32, f64) and (i32, f64) there.
        int options = serialize("afloat + 1.0 > 1.5 and anint > 16777216.0", false, false, true);
        assertIR("afloat + 1.0 > 1.5 and anint > 16777216.0",
                "(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(f32 1.5D)(f64 1.0D)(f32 afloat)(+)(>)(&&)(ret)");
        assertOptionsHint("wide-lane conjunct pair", options, OptionsHint.WIDE_LANE);
        options = serialize("anint > 16777216.0 and afloat + 1.0 > 1.5", false, false, true);
        assertIR("anint > 16777216.0 and afloat + 1.0 > 1.5",
                "(f32 1.5D)(f64 1.0D)(f32 afloat)(+)(>)(f64 1.6777216E7D)(i32 anint)(sx_i64)(>)(&&)(ret)");
        assertOptionsHint("wide-lane conjunct pair, reversed", options, OptionsHint.WIDE_LANE);

        // The four-lane loop is what the DOUBLE-width node runs on, so a filter that reaches it
        // keeps every conjunct vectorized rather than dragging them to scalar as collateral.
        options = serialize("afloat * 2.0 > 1.5 and anint > 5", false, false, true);
        assertIR("afloat * 2.0 > 1.5 and anint > 5",
                "(i32 5L)(i32 anint)(>)(f32 1.5D)(f64 2.0D)(f32 afloat)(*)(>)(&&)(ret)");
        assertOptionsHint("afloat * 2.0 > 1.5 and anint > 5", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat * 2.0 > 1.5 or anint > 5", false, false, true);
        assertOptionsHint("afloat * 2.0 > 1.5 or anint > 5", options, OptionsHint.WIDE_LANE);
        options = serialize("not (afloat * 2.0 > 1.5)", false, false, true);
        assertOptionsHint("not (afloat * 2.0 > 1.5)", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat * 2.0 in (1.5, 2.5)", false, false, true);
        assertOptionsHint("afloat * 2.0 in (1.5, 2.5)", options, OptionsHint.WIDE_LANE);
        // A conjunct the four-lane loop cannot take keeps the whole filter scalar - eligibility is
        // conjunctive over the tree, and a SYMBOL comparison is not wide-lane eligible.
        options = serialize("afloat * 2.0 > 1.5 and asymbol = 'ABC'", false, false, true);
        assertOptionsHint("afloat * 2.0 > 1.5 and asymbol = 'ABC'", options, OptionsHint.SCALAR);

        // Controls. An F4-typed subtree is NOT widened: "+(FF)" and "*(FF)" exist, so the Java
        // filter evaluates these at f32 too, and only the comparison bound needs the double width
        // (which markFloatCmpConst already gives it).
        options = serialize("afloat + 1 > 16777216.5", false, false, true);
        assertIR("afloat + 1 > 16777216.5", "(f64 1.67772165E7D)(i32 1L)(f32 afloat)(+)(>)(ret)");
        assertOptionsHint("afloat + 1 > 16777216.5", options, OptionsHint.WIDE_LANE);
        options = serialize("afloat + 1.0f > 16777216.5", false, false, true);
        assertIR("afloat + 1.0f > 16777216.5", "(f64 1.67772165E7D)(f32 1.0D)(f32 afloat)(+)(>)(ret)");
        assertOptionsHint("afloat + 1.0f > 16777216.5", options, OptionsHint.WIDE_LANE);
        // Controls. An 8-byte source already types every constant at 8 bytes, so nothing changes
        // for it - neither the IR nor the vectorized loop it runs on.
        options = serialize("adouble + 1.0 > 16777216.5", false, false, true);
        assertIR("adouble + 1.0 > 16777216.5", "(f64 1.67772165E7D)(f64 1.0D)(f64 adouble)(+)(>)(ret)");
        assertOptionsHint("adouble + 1.0 > 16777216.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("along + 1.0 > 16777216.5", false, false, true);
        assertIR("along + 1.0 > 16777216.5", "(f64 1.67772165E7D)(f64 1.0D)(i64 along)(+)(>)(ret)");
        assertOptionsHint("along + 1.0 > 16777216.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("afloat + adouble > 16777216.5", false, false, true);
        assertIR("afloat + adouble > 16777216.5", "(f64 1.67772165E7D)(f64 adouble)(f32 afloat)(+)(>)(ret)");
        assertOptionsHint("afloat + adouble > 16777216.5", options, OptionsHint.MIXED_SIZES);
        // Control: a bare DOUBLE-spelled bound against a 4-byte column is untouched - no
        // arithmetic node computes at the wrong width and the existing rules already decide
        // whether the bound itself needs the double width.
        options = serialize("anint > 1.5", false, false, true);
        assertIR("anint > 1.5", "(f32 1.5D)(i32 anint)(>)(ret)");
        assertOptionsHint("anint > 1.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("afloat > 1.5", false, false, true);
        assertIR("afloat > 1.5", "(f32 1.5D)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > 1.5", options, OptionsHint.SINGLE_SIZE);
        options = serialize("afloat > -1.5", false, false, true);
        assertIR("afloat > -1.5", "(f32 -1.5D)(f32 afloat)(>)(ret)");
        assertOptionsHint("afloat > -1.5", options, OptionsHint.SINGLE_SIZE);
        // Control: an INT-width arithmetic node with an integer literal keeps its wrapping i32
        // operands. Only a DOUBLE-spelled literal moves.
        options = serialize("afloat * 2 > 16777216.5", false, false, true);
        assertIR("afloat * 2 > 16777216.5", "(f64 1.67772165E7D)(i32 2L)(f32 afloat)(*)(>)(ret)");
        assertOptionsHint("afloat * 2 > 16777216.5", options, OptionsHint.WIDE_LANE);
        options = serialize("anint * 2 > 16777216.5", false, false, true);
        assertIR("anint * 2 > 16777216.5", "(f64 1.67772165E7D)(i32 2L)(i32 anint)(*)(>)(ret)");
        assertOptionsHint("anint * 2 > 16777216.5", options, OptionsHint.SCALAR);
    }

    @Test
    public void testFloatArithI64AndDoubleConstChainEmitsAtF8() throws Exception {
        // The one three-way (f32, i64 IMM, f64 IMM) chain, and a boundary between two adjacent
        // spellings that nothing else in the tree crosses. The two literals settle the execution
        // mode between them:
        // - promoteArithType(F4, I8) answers F4 - the "a == F4 || b == F4" clause fires BEFORE the
        //   integer-width one - so the inner "afloat + 5_000_000_000" is still a FLOAT
        //   computation, and its out-of-INT constant emits as an I8 immediate beside f32 operands;
        // - promoteArithType(F4, F8) then makes the outer "... + 1.0" node F8, and
        //   isDoubleConst() claims its right operand;
        // - hasEightByteLeaf() counts COLUMNS and BIND VARIABLES only, so neither literal
        //   suppresses the source, isNarrowLaneDoubleConstArith() answers true and the filter
        //   takes the four-lane loop - whose lanes are eight bytes wide, which is what the widened
        //   immediate needs.
        // Drop the DOUBLE literal and the very same predicate is SCALAR, which
        // testEightByteArithI64ConstantKeepsVectorization pins from the other side.
        //
        // CompiledFilterRegressionTest#testFloatArithI64AndDoubleConstChainWidensToWideLane pins
        // the hint and the rows over a fixture: against a 1.5 bound the two spellings return the
        // SAME rows, so only the hint tells them apart.
        int options = serialize("afloat + 5_000_000_000 + 1.0 > 1.5", false, false, true);
        assertIR("afloat + 5_000_000_000 + 1.0 > 1.5",
                "(f32 1.5D)(f64 1.0D)(i64 5000000000L)(f32 afloat)(+)(+)(>)(ret)");
        assertOptionsHint("afloat + 5_000_000_000 + 1.0 > 1.5", options, OptionsHint.WIDE_LANE);

        // The two-way control, side by side: same column, same out-of-INT constant, same bound.
        options = serialize("afloat + 5_000_000_000 > 1.5", false, false, true);
        assertIR("afloat + 5_000_000_000 > 1.5", "(f32 1.5D)(i64 5000000000L)(f32 afloat)(+)(>)(ret)");
        assertOptionsHint("afloat + 5_000_000_000 > 1.5", options, OptionsHint.SCALAR);

        // The reversed comparison reaches the backend as the other pairing and takes the same
        // route.
        options = serialize("1.5 < afloat + 5_000_000_000 + 1.0", false, false, true);
        assertIR("1.5 < afloat + 5_000_000_000 + 1.0",
                "(f64 1.0D)(i64 5000000000L)(f32 afloat)(+)(+)(f32 1.5D)(<)(ret)");
        assertOptionsHint("1.5 < afloat + 5_000_000_000 + 1.0", options, OptionsHint.WIDE_LANE);
    }

    @Test
    public void testShortCircuitOpcodeConsumesOneOperandInWidthWalk() throws Exception {
        // Both backends consume ONE value at a short-circuit opcode and produce none: jit/x86.h
        // and jit/aarch64.h handle opcodes::And_Sc / Or_Sc with a bare "auto arg = values.pop()" and
        // append nothing back. A value pushed BEFORE the short circuit therefore stays live on the
        // backend's value stack and pairs with whatever the stream pushes after it.
        // hasUnharmonisedOperandWidths() models that value stack to decide the execution hint, so
        // it has to consume one value there too.
        //
        // No SQL reaches these streams. serializePredicatesAndSc() and serializeIn() emit a
        // short-circuit only at a predicate boundary, where the value it consumes is the last one
        // of a self-contained predicate and nothing of that predicate is left live behind it, so
        // the operand the walk over-pops is always a mask the pairing check skips anyway. They are
        // planted by hand for that reason, the way CompiledFilterRegressionTest#writeAbandonProbeIr
        // plants a stream the serializer never writes: what they pin is the BACKEND's contract,
        // which is what the walk's answer is about, rather than the shapes today's emitter happens
        // to produce.
        assertMemoryLeak(() -> {
            // A buffer of its own rather than the shared irMemory: the shared one is allocated
            // once for the class and its first page would land inside this block as an unbalanced
            // NATIVE_JIT allocation.
            try (
                    MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT);
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)
            ) {
                serializer.clear();
                serializer.of(ir, sqlExecutionContext, metadata, cursor, bindVarFunctions);

                // Control: the (i32, i64) pairing on its own. Pins that the walk reads these
                // widths at all, so a false answer below is about the short-circuit opcode.
                ir.truncate();
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(i32 col) > (i64 imm)", true);

                // Control: the same stream at a single width reports nothing.
                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(i64 col) > (i64 imm)", false);

                // AND_SC over a comparison mask - the shape the emitter writes - with an i32 value
                // left live underneath it. The backend pops the mask and pairs the i64 immediate
                // that follows with that live i32.
                ir.truncate();
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, EQ, 0, 0);
                putIrInstruction(ir, AND_SC, 0, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("live (i32 col) across AND_SC over a mask", true);

                // The same with OR_SC, and with a plain value rather than a mask as the operand
                // the short circuit consumes.
                ir.truncate();
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, MEM, I1_TYPE, 0);
                putIrInstruction(ir, OR_SC, 0, 1);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("live (i32 col) across OR_SC over a value", true);

                // Control: a live operand of the lane width pairs cleanly across a short circuit.
                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, MEM, I1_TYPE, 0);
                putIrInstruction(ir, AND_SC, 0, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("live (i64 col) across AND_SC", false);
            } finally {
                // The buffer above is gone; do not leave the serializer holding it. In the finally
                // so that a failing assertion above does not skip it: the static serializer outlives
                // this method, and clear() is what drops its reference to the closed buffer.
                serializer.clear();
            }
        });
    }

    @Test
    public void testExecHintDemotesUnharmonisedWidthsToScalar() throws Exception {
        // The one shape known to reach getExecHint()'s unharmonised-width demotion, and the reason
        // that arm is a live fail-safe rather than dead code.
        //
        // markWidthSemantics' IN-args loop settles the whole list at 64 bits as soon as one element
        // is out of INT range, then harmonises the key and every element to it through
        // markCmpOperandWidenedToI64. The constant key `1` is an integer constant, so it joins
        // i64WidenConstants and emits at I8 with no SX_I64 behind it; the FLOAT element is neither
        // a narrow-int leaf (so visit()'s i64WidenLeaves gate never fires) nor an integer constant,
        // and forceScalarOnUnharmonisedNarrowArith returns at once for a node that is not an
        // OPERATION, so nothing marks it at all. The resulting (f32, i64) pairing would ride a
        // four-byte, eight-lane loop, where avx2::convert declines an f32-with-i64 pairing and the
        // filter loses its compiled backend. The demotion below is what keeps it.
        final String expr = "1 in (afloat, 5_000_000_000)";
        final int options = serialize(expr, false, false, true);
        assertIR(expr, "(i64 5000000000L)(i64 1L)(=)(f32 afloat)(i64 1L)(=)(||)(ret)");
        assertOptionsSize(expr, options, 4);
        assertOptionsHint(expr, options, OptionsHint.SCALAR);
        // ... and the demotion arm is what produced that hint rather than one of the gates above
        // it: the walk reports the pairing and every earlier gate is false.
        assertUnharmonisedWidthWalk("(f32 afloat) = (i64 1L) on a four-byte lane", true);
        assertSerializerFlag("forceScalarMode", false);
        assertSerializerFlag("isWideLaneMode", false);
        assertSerializerFlag("hasEmittedWideLaneConversion", false);
        assertSerializerFlag("hasPendingWidthChangingI64Constant", false);

        // No user query reaches the arm, and this is the check that keeps it that way - not any
        // invariant of the serializer. InLongFunctionFactory ("in(LV)") admits NULL / TIMESTAMP /
        // LONG / INT / SHORT / BYTE / STRING / SYMBOL / VARCHAR / UNDEFINED elements only, so the
        // filter never reaches JIT compilation. serialize() above takes the expression tree
        // directly and so skips it, which is why the arm can be pinned at all. Should in(LV) ever
        // admit FLOAT elements, this assertion goes red and the demotion becomes a live path.
        assertException("select * from x where " + expr, 28, "cannot compare LONG with type FLOAT");
    }

    @Test
    public void testWidthWalkStopsAtAppendOffsetOverReusedMemory() throws Exception {
        // SqlCodeGenerator compiles every JIT filter of a session into ONE buffer - its jitIRMem
        // field - and hands it back with truncate() in a finally. MemoryCARWImpl#truncate() resets
        // the append offset and reallocates to a single page; it does not zero, and a realloc that
        // asks for the size the buffer already has returns the same bytes. The PREVIOUS filter's
        // IR is therefore still readable while the next one serializes into the same buffer, and a
        // buffer nothing has written yet holds whatever malloc() left there.
        //
        // hasUnharmonisedOperandWidths() bounds its walk by getAppendOffset() for that reason.
        // size() reports the MAPPED page rather than the bytes written, so a walk bounded by it
        // reads the previous filter's tail as if the current filter had emitted it. The walk
        // cannot stop on its own here: getExecHint() asks this question from
        // serializePredicatesAndSc() / serializePredicatesOrSc(), which have not emitted their RET
        // yet, so nothing terminates the stream between the current filter's last instruction and
        // the stale bytes.
        //
        // A stale read can only turn a false into a true - the walk returns at the first
        // unharmonised pairing it meets, so trailing instructions can only add pairings, never
        // remove one - and at that call site a false answer IS the "expected scalar compilation
        // mode" tripwire. So no supported SQL reaches the site with a correct answer of false, and
        // the shape is planted by hand for the same reason
        // testShortCircuitOpcodeConsumesOneOperandInWidthWalk plants its streams: what it pins is
        // the bound, not a shape today's emitter produces.
        assertMemoryLeak(() -> {
            // A buffer of its own rather than the shared irMemory: the shared one is allocated
            // once for the class and its first page would land inside this block as an unbalanced
            // NATIVE_JIT allocation.
            try (
                    MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT);
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)
            ) {
                serializer.clear();
                serializer.of(ir, sqlExecutionContext, metadata, cursor, bindVarFunctions);

                // The LONGER filter: a harmonised pairing, then an unharmonised one, then its RET.
                // Only the second pairing sits past the shorter filter's append offset, so it is
                // the one a size()-bounded walk picks up.
                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(i32 col) > (i64 imm) in the longer filter", true);

                // The reuse, spelled as SqlCodeGenerator's finally spells it.
                ir.truncate();

                // The SHORTER filter, in the state getExecHint() reads it in on a short-circuit
                // path: three instructions with no RET behind them.
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);

                // The precondition, asserted rather than assumed: the longer filter's fourth
                // instruction is still in the buffer, one instruction past this filter's append
                // offset. Should truncate() ever start zeroing, this goes red and says so instead
                // of leaving the walk assertion below quietly asserting nothing.
                final long staleOffset = 3 * IR_INSTRUCTION_SIZE;
                Assert.assertEquals("truncate() zeroed the buffer", MEM, ir.getInt(staleOffset));
                Assert.assertEquals("truncate() zeroed the buffer", I4_TYPE, ir.getInt(staleOffset + Integer.BYTES));

                // With the bound at size() the walk runs on into that stale (i32, i64) pairing and
                // answers true for a filter that emitted no such pairing.
                assertUnharmonisedWidthWalk("(i64 col) > (i64 imm) over reused memory", false);
            } finally {
                // The buffer above is gone; do not leave the serializer holding it. In the finally
                // so that a failing assertion above does not skip it: the static serializer
                // outlives this method, and clear() is what drops its reference to the closed
                // buffer.
                serializer.clear();
            }
        });
    }

    @Test
    public void testVarSizeHeaderCheckStopsAtAppendOffsetOverReusedMemory() throws Exception {
        // The sibling of testWidthWalkStopsAtAppendOffsetOverReusedMemory, over the second walk of
        // this class: ensureOnlyVarSizeHeaderChecks(). It bounded itself by memory.size(), which
        // reports the MAPPED page rather than the bytes written, so it could read the PREVIOUS
        // filter's IR out of the buffer SqlCodeGenerator reuses for every filter of a session.
        //
        // What this pins is an INVARIANT, not a bug a user could hit at HEAD, and the distinction
        // matters. All three callers - serialize(), serializePredicatesAndSc() and
        // serializePredicatesOrSc() - run the check immediately after putOperator(RET) with no
        // write in between, and the walk returns at the first RET it meets. The RET therefore sits
        // one instruction INSIDE the append offset, both bounds meet it, and neither reaches a
        // stale byte. So the test cannot drive the defect through a query; it calls the walk
        // directly, at the one point where the invariant CAN be violated - a stream that has not
        // emitted its RET yet, exactly the state getExecHint() already reads on the short-circuit
        // paths. A fourth caller of that shape is what the bound protects against.
        //
        // The rejected filter below is not a contrived leftover either: SqlCodeGenerator truncates
        // jitIRMem in a finally, so a filter this very check REJECTED leaves its offending IR in
        // the buffer for the next query to serialize over.
        assertMemoryLeak(() -> {
            // A buffer of its own rather than the shared irMemory: the shared one is allocated
            // once for the class and its first page would land inside this block as an unbalanced
            // NATIVE_JIT allocation.
            try (
                    MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT);
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)
            ) {
                serializer.clear();
                serializer.of(ir, sqlExecutionContext, metadata, cursor, bindVarFunctions);

                // Round one, the var-size arm. The LONGER filter carries a STRING header under an
                // ordering operator - what `astring > 'a'` would serialize to - so the check
                // rejects it and its IR stays in the buffer. Only the second pairing sits past the
                // shorter filter's append offset, so it is the one a size()-bounded walk picks up.
                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, MEM, STRING_HEADER_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertVarSizeHeaderChecksReject(
                        "(string header) > (i64 imm) in the rejected filter",
                        "var-size columns can only be used in NULL checks"
                );

                // The reuse, spelled as SqlCodeGenerator's finally spells it.
                ir.truncate();

                // The SHORTER filter, in the state a pre-RET caller would read it in: three
                // instructions, all of them fixed-size, with no RET behind them.
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);

                // The precondition, asserted rather than assumed: the rejected filter's fourth
                // instruction is still in the buffer, one instruction past this filter's append
                // offset. MemoryCARWImpl#truncate() reallocates to the size the buffer already has
                // and never zeroes, so the bytes survive verbatim. Should it ever start zeroing,
                // this goes red and says so instead of leaving the assertion below quietly
                // asserting nothing.
                final long staleOffset = 3 * IR_INSTRUCTION_SIZE;
                Assert.assertEquals("truncate() zeroed the buffer", MEM, ir.getInt(staleOffset));
                Assert.assertEquals(
                        "truncate() zeroed the buffer",
                        STRING_HEADER_TYPE,
                        ir.getInt(staleOffset + Integer.BYTES)
                );

                // With the bound at size() the walk runs on into that stale (string header, i64)
                // pairing and rejects a filter that reached no var-size column at all.
                assertVarSizeHeaderChecksPass("(i64 col) > (i64 imm) over reused memory");

                // Round two, the harsher arm: `case -1` throws outright. UNDEFINED_CODE is the
                // opcode serializeConstantStub() and the symbol bind-variable path write for a
                // value they backfill later; when the backfill itself throws, serialize()
                // propagates and the finally hands the buffer back with the stub still in it.
                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, IR_UNDEFINED_CODE, IR_UNDEFINED_CODE, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertVarSizeHeaderChecksReject("un-backfilled stub in the rejected filter", "invalid opcode");

                ir.truncate();
                putIrInstruction(ir, MEM, I8_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);

                Assert.assertEquals("truncate() zeroed the buffer", IR_UNDEFINED_CODE, ir.getInt(staleOffset));
                assertVarSizeHeaderChecksPass("(i64 col) > (i64 imm) over a reused stub");
            } finally {
                // The buffer above is gone; do not leave the serializer holding it. In the finally
                // so that a failing assertion above does not skip it: the static serializer
                // outlives this method, and clear() is what drops its reference to the closed
                // buffer.
                serializer.clear();
            }
        });
    }

    @Test
    public void testUnharmonisedPairingExclusionsAndWideLaneBranch() throws Exception {
        // isUnharmonisedPairing() answers a different question for each of the two loops, and the
        // suite reached only the narrower one. testShortCircuitOpcodeConsumesOneOperandInWidthWalk
        // and testExecHintDemotesUnharmonisedWidthsToScalar pin the 4/8 and 8/8 cases of the
        // single-size half; this covers the three exclusions that half carries and the wide-lane
        // half in full.
        //
        // The exclusions are what keep the walk from demoting a filter that needs no demoting: it
        // runs on every compile that reaches it, and a false positive costs the filter its
        // vectorized backend outright.
        //
        // Each stream is planted by hand, as in the two tests above: what these pin is what the
        // BACKENDS harmonise, which the emitter's current output does not enumerate.
        assertMemoryLeak(() -> {
            // A buffer of its own rather than the shared irMemory: the shared one is allocated
            // once for the class and its first page would land inside this block as an unbalanced
            // NATIVE_JIT allocation.
            try (
                    MemoryCARW ir = Vm.getCARWInstance(2_048, 1, MemoryTag.NATIVE_JIT);
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)
            ) {
                serializer.clear();
                serializer.of(ir, sqlExecutionContext, metadata, cursor, bindVarFunctions);

                // A 16-byte operand - data_type_t::i128, what a UUID or LONG128 column reads as -
                // beside an eight-byte one. The single-size half counts any byte-width mismatch, so
                // it reports; the wide-lane half counts only a NARROW INT beside an i64, and i128
                // is not one, so it does not - it reads the pairing as harmonised. avx2::convert()
                // does not agree: its i128 arm breaks out to the terminal check, which declines the
                // pairing at every lane count, four included. The disagreement is on paper only -
                // isWideLaneEligible() admits no i128 operand (its integer arm takes an I4 or I8
                // leaf, its float arm a float expression), so no wide-lane compile can put one in
                // front of the assert this half runs under. The pins below hold the walk as it
                // stands: the divergence is a rationale to record, not a behaviour to change here.
                ir.truncate();
                putIrInstruction(ir, MEM, I16_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, EQ, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(i128 col) = (i64 imm) on a narrow lane", true);
                assertUnharmonisedWidthWalkForLaneMode("(i128 col) = (i64 imm) on the four-lane loop", true, false);

                // Every var-size header observes as EIGHT bytes, so `<varsize> IS [NOT] NULL`
                // pairs same-width against the I8 sentinel serializeNull() spells for it and
                // neither half reports. The exclusion is a SIZE and not an exemption, which the
                // fourth stream below pins from the other side.
                for (int headerType : new int[]{STRING_HEADER_TYPE, BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE}) {
                    ir.truncate();
                    putIrInstruction(ir, MEM, headerType, 0);
                    putIrInstruction(ir, IMM, I8_TYPE, 0);
                    putIrInstruction(ir, EQ, 0, 0);
                    putIrInstruction(ir, RET, 0, 0);
                    assertUnharmonisedWidthWalk("(varsize header " + headerType + ") = (i64 imm)", false);
                    assertUnharmonisedWidthWalkForLaneMode("(varsize header " + headerType + ") = (i64 imm), four lanes", true, false);
                }
                ir.truncate();
                putIrInstruction(ir, MEM, STRING_HEADER_TYPE, 0);
                putIrInstruction(ir, IMM, I4_TYPE, 0);
                putIrInstruction(ir, EQ, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(string header) = (i32 imm) is a width mismatch like any other", true);

                // A comparison MASK as one half of a pairing. The walk pushes UNDEFINED_CODE for
                // one, typeSizeBytes() answers 0, and the pairing is skipped rather than read as a
                // zero-byte operand against an eight-byte one. Both operand positions, because the
                // guard has to hold on either side.
                ir.truncate();
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, IMM, I4_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(i64 imm) > (mask)", false);
                ir.truncate();
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, MEM, I4_TYPE, 0);
                putIrInstruction(ir, IMM, I4_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(mask) > (i64 imm)", false);

                // The wide-lane half, in both directions and at all three narrow widths. This is
                // the branch the assert at areWideLaneWidthsHarmonised() runs under -ea and no
                // test reached directly: avx2::sx_i64 widens an i32 lane into the low 128 bits, so
                // a narrow int beside an i64 is the one pairing the four-lane loop cannot leave as
                // it found it.
                for (int narrowType : new int[]{I1_TYPE, I2_TYPE, I4_TYPE}) {
                    ir.truncate();
                    putIrInstruction(ir, MEM, narrowType, 0);
                    putIrInstruction(ir, IMM, I8_TYPE, 0);
                    putIrInstruction(ir, GT, 0, 0);
                    putIrInstruction(ir, RET, 0, 0);
                    assertUnharmonisedWidthWalkForLaneMode("(narrow " + narrowType + ") > (i64 imm), four lanes", true, true);
                    ir.truncate();
                    putIrInstruction(ir, IMM, I8_TYPE, 0);
                    putIrInstruction(ir, MEM, narrowType, 0);
                    putIrInstruction(ir, GT, 0, 0);
                    putIrInstruction(ir, RET, 0, 0);
                    assertUnharmonisedWidthWalkForLaneMode("(i64 imm) > (narrow " + narrowType + "), four lanes", true, true);
                }

                // ... and the pairing the two halves disagree about, which is what makes the
                // wide-lane branch a branch rather than a copy: an (f32, i64) pairing is a byte
                // mismatch the single-size loop cannot harmonise, and cvt_ftod / cvt_ltod harmonise
                // it outright at four lanes. The comment on hasUnharmonisedOperandWidths() names
                // exactly this exclusion; this is the stream that holds it.
                ir.truncate();
                putIrInstruction(ir, MEM, F4_TYPE, 0);
                putIrInstruction(ir, IMM, I8_TYPE, 0);
                putIrInstruction(ir, GT, 0, 0);
                putIrInstruction(ir, RET, 0, 0);
                assertUnharmonisedWidthWalk("(f32 col) > (i64 imm) on a narrow lane", true);
                assertUnharmonisedWidthWalkForLaneMode("(f32 col) > (i64 imm) on the four-lane loop", true, false);
            } finally {
                // The buffer above is gone; do not leave the serializer holding it. In the finally
                // so that a failing assertion above does not skip it: the static serializer
                // outlives this method, and clear() is what drops its reference to the closed
                // buffer.
                serializer.clear();
            }
        });
    }

    // instruction_t in common.h: a 4-byte opcode, a 4-byte options field, then a 16-byte payload.
    private static void putIrInstruction(MemoryCARW ir, int opcode, int typeCode, long payload) {
        ir.putInt(opcode);
        ir.putInt(typeCode);
        ir.putLong(payload);
        ir.putLong(0L);
    }

    // Runs hasUnharmonisedOperandWidths() over whatever IR sits in irMemory. The walk is private
    // and this test lives in another package, so reflection is what reaches it - the alternative,
    // widening the method, would open the production class up for this test alone: both callers
    // the walk has, areWideLaneWidthsHarmonised() and getExecHint(), sit inside
    // CompiledFilterIRSerializer. false is the argument getExecHint() passes on the production
    // compile path: the single-size loop at a lane narrower than eight bytes, where the backend
    // declines a mixed-width pairing rather than promoting it.
    private static void assertUnharmonisedWidthWalk(String message, boolean expected) throws Exception {
        assertUnharmonisedWidthWalkForLaneMode(message, false, expected);
    }

    // The same walk for either loop, naming the loop in the SECOND argument, where the form above
    // carries the expectation instead - hence a name of its own rather than an overload the reader
    // has to count arguments to tell apart. true is the argument the assert at
    // areWideLaneWidthsHarmonised() passes - the four-lane loop, where avx2::convert() DOES promote
    // and only a narrow-int-with-i64 pairing counts.
    private static void assertUnharmonisedWidthWalkForLaneMode(String message, boolean isWideLane, boolean expected) throws Exception {
        final Method walk = CompiledFilterIRSerializer.class
                .getDeclaredMethod("hasUnharmonisedOperandWidths", boolean.class);
        walk.setAccessible(true);
        Assert.assertEquals(message, expected, walk.invoke(serializer, isWideLane));
    }

    // Runs ensureOnlyVarSizeHeaderChecks() over whatever IR sits in the serializer's buffer and
    // expects it to accept the stream. Same reasoning as assertUnharmonisedWidthWalk() for using
    // reflection: the check is private and every caller it has - serialize() and the two
    // short-circuit paths - sits inside CompiledFilterIRSerializer, so widening it would open the
    // production class up for this test alone.
    private static void assertVarSizeHeaderChecksPass(String message) throws Exception {
        try {
            invokeVarSizeHeaderChecks();
        } catch (InvocationTargetException e) {
            throw new AssertionError(message + " - rejected with: " + e.getCause().getMessage(), e.getCause());
        }
    }

    // The same walk, expecting a rejection carrying expectedMessage.
    private static void assertVarSizeHeaderChecksReject(String message, String expectedMessage) throws Exception {
        try {
            invokeVarSizeHeaderChecks();
            Assert.fail(message + " - expected a rejection carrying: " + expectedMessage);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            Assert.assertTrue(
                    message + " - expected an SqlException, got: " + cause,
                    cause instanceof SqlException
            );
            TestUtils.assertContains(((SqlException) cause).getFlyweightMessage(), expectedMessage);
        }
    }

    private static void invokeVarSizeHeaderChecks() throws Exception {
        final Method check = CompiledFilterIRSerializer.class.getDeclaredMethod("ensureOnlyVarSizeHeaderChecks");
        check.setAccessible(true);
        check.invoke(serializer);
    }

    // Reads one of getExecHint()'s private gate flags after a serialize() call, so a test can pin
    // WHICH arm produced an execution hint rather than only the hint itself. Same reasoning as
    // assertUnharmonisedWidthWalk(): the alternative is a seam in the production class for a flag
    // with no other reader.
    private static void assertSerializerFlag(String name, boolean expected) throws Exception {
        final Field flag = CompiledFilterIRSerializer.class.getDeclaredField(name);
        flag.setAccessible(true);
        Assert.assertEquals(name, expected, flag.getBoolean(serializer));
    }

    // Reads a private static int constant of CompiledFilterIRSerializer, so a layout number this
    // class depends on comes from production rather than from a copy that can drift. Same
    // mechanism CompiledFilterRegressionTest#serializerExecHint uses for the EXEC_HINT_* table.
    private static int serializerInt(String name) {
        try {
            final Field field = CompiledFilterIRSerializer.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.getInt(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot read CompiledFilterIRSerializer." + name, e);
        }
    }

    private void assertIR(String message, String expectedIR) {
        TestIRSerializer ser = new TestIRSerializer(irMemory, metadata);
        String actualIR = ser.serialize();
        Assert.assertEquals(message, expectedIR, actualIR);
    }

    private void assertIR(String expectedIR) {
        assertIR(null, expectedIR);
    }

    private void assertOptionsDebug(int options, boolean expectedFlag) {
        int f = options & 1;
        Assert.assertEquals(expectedFlag ? 1 : 0, f);
    }

    private void assertOptionsHint(int options) {
        assertOptionsHint(null, options, OptionsHint.SCALAR);
    }

    private void assertOptionsHint(String msg, int options, OptionsHint expectedHint) {
        int code = (options >> 4) & 0b11;
        Assert.assertEquals(msg, expectedHint.code, code);
    }

    private void assertOptionsNullChecks(int options, boolean expectedFlag) {
        int f = (options >> 6) & 1;
        Assert.assertEquals(expectedFlag ? 1 : 0, f);
    }

    private void assertOptionsSize(String msg, int options, int expectedSize) {
        int size = 1 << ((options >> 1) & 0b111);
        Assert.assertEquals(msg, expectedSize, size);
    }

    private int serialize(CharSequence seq, boolean scalar, boolean debug, boolean nullChecks) throws SqlException {
        irMemory.truncate();
        serializer.clear();
        bindVarFunctions.clear();

        ExpressionNode node = expr(seq);
        try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
            return serializer.of(irMemory, sqlExecutionContext, metadata, cursor, bindVarFunctions)
                    .serialize(node, scalar, debug, nullChecks);
        }
    }

    private void serialize(CharSequence seq) throws SqlException {
        serialize(seq, false, false, true);
    }

    private enum OptionsHint {
        SCALAR(0), SINGLE_SIZE(1), MIXED_SIZES(2), WIDE_LANE(3);

        final int code;

        OptionsHint(int code) {
            this.code = code;
        }
    }

    private static class TestIRSerializer {
        private final MemoryCARW irMem;
        private final RecordMetadata metadata;
        private long offset;
        private StringBuilder sb;

        public TestIRSerializer(MemoryCARW irMem, RecordMetadata metadata) {
            this.irMem = irMem;
            this.metadata = metadata;
        }

        public String serialize() {
            offset = 0;
            sb = new StringBuilder();
            while (offset < irMem.getAppendOffset()) {
                int opcode = irMem.getInt(offset);
                offset += Integer.BYTES;
                int type = irMem.getInt(offset);
                offset += Integer.BYTES;
                switch (opcode) {
                    // Columns
                    case MEM:
                        appendColumn(type);
                        break;
                    // Bind variables
                    case VAR:
                        appendBindVariable(type);
                        break;
                    // Constants
                    case IMM: {
                        switch (type) {
                            case F4_TYPE:
                            case F8_TYPE:
                                appendDoubleConst(type);
                                break;
                            case I16_TYPE:
                                appendLongLongConst(type);
                                break;
                            default:
                                appendLongConst(type);
                                break;
                        }
                    }
                    break;
                    // Operators
                    default:
                        appendOperator(opcode);
                        break;
                }
            }
            return sb.toString();
        }

        private void appendBindVariable(int type) {
            long index = irMem.getLong(offset);
            offset += 2 * Long.BYTES;
            sb.append("(");
            sb.append(typeName(type));
            sb.append(" :");
            sb.append(index);
            sb.append(")");
        }

        private void appendColumn(int type) {
            long index = irMem.getLong(offset);
            offset += 2 * Long.BYTES;
            sb.append("(");
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(metadata.getColumnName((int) index));
            sb.append(")");
        }

        private void appendDoubleConst(int type) {
            double value = irMem.getDouble(offset);
            offset += 2 * Double.BYTES;
            sb.append("(");
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(value);
            sb.append("D)");
        }

        private void appendLongConst(int type) {
            long value = irMem.getLong(offset);
            offset += 2 * Long.BYTES;
            sb.append("(");
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(value);
            sb.append("L)");
        }

        private void appendLongLongConst(int type) {
            long lo = irMem.getLong(offset);
            offset += Long.BYTES;
            long hi = irMem.getLong(offset);
            offset += Long.BYTES;
            sb.append("(");
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(lo);
            sb.append(" ");
            sb.append(hi);
            sb.append("L)");
        }

        private void appendOperator(int operator) {
            long payload = irMem.getLong(offset);
            offset += 2 * Long.BYTES;
            sb.append("(");
            sb.append(operatorName(operator));
            // Include label index for short-circuit opcodes when it differs from default:
            // - AND_SC default label is 0 (next_row)
            // - OR_SC default label is 1 (store_row)
            // - BEGIN_SC/END_SC always show label
            boolean showLabel = switch (operator) {
                case BEGIN_SC, END_SC -> true;
                case AND_SC -> payload != 0;
                case OR_SC -> payload != 1;
                default -> false;
            };
            if (showLabel) {
                sb.append(" ");
                sb.append(payload);
            }
            sb.append(")");
        }

        private String operatorName(int operator) {
            return switch (operator) {
                case NEG -> "neg";
                case NOT -> "!";
                case AND -> "&&";
                case OR -> "||";
                case EQ -> "=";
                case NE -> "<>";
                case LT -> "<";
                case LE -> "<=";
                case GT -> ">";
                case GE -> ">=";
                case ADD -> "+";
                case SUB -> "-";
                case MUL -> "*";
                case DIV -> "/";
                case RET -> "ret";
                case BEGIN_SC -> "begin_sc";
                case AND_SC -> "&&_sc";
                case OR_SC -> "||_sc";
                case END_SC -> "end_sc";
                case SX_I64 -> "sx_i64";
                default -> "unknown";
            };
        }

        private String typeName(int type) {
            return switch (type) {
                case I1_TYPE -> "i8";
                case I2_TYPE -> "i16";
                case I4_TYPE -> "i32";
                case I8_TYPE -> "i64";
                case F4_TYPE -> "f32";
                case F8_TYPE -> "f64";
                case I16_TYPE -> "i128";
                case STRING_HEADER_TYPE -> "string_header";
                case BINARY_HEADER_TYPE -> "binary_header";
                case VARCHAR_HEADER_TYPE -> "varchar_header";
                default -> "unknown: " + type;
            };
        }
    }
}
