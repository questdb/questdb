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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.jit.CompiledFilterIRSerializer.*;

public class CompiledFilterIRSerializerTest extends BaseFunctionFactoryTest {
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
    public void testInVariableBinding() throws Exception {
        bindVariableService.clear();
        bindVariableService.setInt("anint", 1);
        bindVariableService.setLong(0, 2);

        serialize("anint IN (:anint, $1)");
        assertIR("(i64 :0)(i32 anint)(=)(i32 :1)(i32 anint)(=)(||)(ret)");

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
        assertIR("(i32 1L)(f64 adouble)(f64 42.5D)(i64 2147483648L)(i32 anint)(*)(+)(+)(>)(ret)");
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
        assertIR("(i32 anint)(i64 along)(+)(i64 -9223372036854775808L)(<>)(||_sc)(i64 -9223372036854775808L)(i64 along)(i32 anint)(+)(<>)(ret)");
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
        serialize("(anint + 1) / (3 * anint) - 42.5 > 0.5");
        assertIR("(f32 0.5D)(f32 42.5D)(i32 anint)(i32 3L)(*)(i32 1L)(i32 anint)(+)(/)(-)(>)(ret)");
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
        filterToOptions.put("afloat / abyte = 0", 4);
        // 8B
        filterToOptions.put("along = 0 or ashort = 0", 8);
        filterToOptions.put("adouble = 0 or ashort = 0", 8);
        filterToOptions.put("afloat = 0 or adouble = 0", 8);
        filterToOptions.put("anint * along = 0", 8);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            int options = serialize(entry.getKey(), false, false, false);
            assertOptionsHint(entry.getKey(), options, OptionsHint.MIXED_SIZES);
            assertOptionsSize(entry.getKey(), options, entry.getValue());
        }
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

    @Test
    public void testStringNullConstant() throws Exception {
        serialize("astring <> null");
        assertIR("(i32 -1L)(string_header astring)(<>)(ret)");
        serialize("astring is not null");
        assertIR("(i32 -1L)(string_header astring)(<>)(ret)");
        serialize("astring = null");
        assertIR("(i32 -1L)(string_header astring)(=)(ret)");
        serialize("astring is null");
        assertIR("(i32 -1L)(string_header astring)(=)(ret)");
        serialize("null <> astring");
        assertIR("(string_header astring)(i32 -1L)(<>)(ret)");
        serialize("null = astring");
        assertIR("(string_header astring)(i32 -1L)(=)(ret)");
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
        SCALAR(0), SINGLE_SIZE(1), MIXED_SIZES(2);

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
