/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.jit;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.BaseFunctionFactoryTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

public class CompiledFilterIRSerializerTest extends BaseFunctionFactoryTest {

    private static final String KNOWN_SYMBOL_1 = "ABC";
    private static final String KNOWN_SYMBOL_2 = "DEF";
    private static final String UNKNOWN_SYMBOL = "XYZ";

    private static TableReader reader;
    private static RecordMetadata metadata;
    private static IntList columnIndexes;
    private static ObjList<Function> bindVarFunctions;
    private static MemoryCARW irMemory;
    private static CompiledFilterIRSerializer serializer;

    @BeforeClass
    public static void setUp2() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model.col("aboolean", ColumnType.BOOLEAN)
                    .col("abyte", ColumnType.BYTE)
                    .col("ageobyte", ColumnType.GEOBYTE)
                    .col("ashort", ColumnType.SHORT)
                    .col("ageoshort", ColumnType.GEOSHORT)
                    .col("achar", ColumnType.CHAR)
                    .col("anint", ColumnType.INT)
                    .col("ageoint", ColumnType.GEOINT)
                    .col("asymbol", ColumnType.SYMBOL)
                    .col("anothersymbol", ColumnType.SYMBOL)
                    .col("afloat", ColumnType.FLOAT)
                    .col("along", ColumnType.LONG)
                    .col("ageolong", ColumnType.GEOLONG)
                    .col("adate", ColumnType.DATE)
                    .col("atimestamp", ColumnType.TIMESTAMP)
                    .col("adouble", ColumnType.DOUBLE)
                    .col("astring", ColumnType.STRING)
                    .timestamp();
            CairoTestUtils.create(model);
        }

        try (TableWriter writer = new TableWriter(configuration, "x")) {
            TableWriter.Row row = writer.newRow();
            row.putSym(writer.getColumnIndex("asymbol"), KNOWN_SYMBOL_1);
            row.putSym(writer.getColumnIndex("anothersymbol"), KNOWN_SYMBOL_2);
            row.append();
            writer.commit();
        }

        reader = new TableReader(configuration, "x");
        metadata = reader.getMetadata();
        columnIndexes = new IntList();
        bindVarFunctions = new ObjList<>();
        for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
            columnIndexes.add(i);
        }
        irMemory = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_DEFAULT);
        serializer = new CompiledFilterIRSerializer();
    }

    @AfterClass
    public static void tearDown2() {
        reader.close();
        irMemory.close();
    }

    @Before
    public void setUp1() {
        irMemory.truncate();
        serializer.clear();
        bindVarFunctions.clear();
    }

    @Test
    public void testColumnTypes() throws Exception {
        Map<String, String[]> typeToColumn = new HashMap<>();
        typeToColumn.put("i8", new String[]{"aboolean", "abyte", "ageobyte"});
        typeToColumn.put("i16", new String[]{"ashort", "ageoshort", "achar"});
        typeToColumn.put("i32", new String[]{"anint", "ageoint", "asymbol"});
        typeToColumn.put("i64", new String[]{"along", "ageolong", "adate", "atimestamp"});
        typeToColumn.put("f32", new String[]{"afloat"});
        typeToColumn.put("f64", new String[]{"adouble"});

        for (String type : typeToColumn.keySet()) {
            for (String col : typeToColumn.get(type)) {
                setUp1();
                serialize(col + " < " + col);
                assertIR("different results for " + type, "(" + type + " " + col + ")(" + type + " " + col + ")(<)(ret)");
            }
        }
    }

    @Test
    public void testSingleBooleanColumn() throws Exception {
        serialize("aboolean or not aboolean");
        assertIR("(i8 1L)(i8 aboolean)(=)(!)(i8 1L)(i8 aboolean)(=)(||)(ret)");
    }

    @Test
    public void testBooleanOperators() throws Exception {
        serialize("anint = 0 and not (abyte = 0) or along = 0");
        assertIR("(i64 0L)(i64 along)(=)(i8 0L)(i8 abyte)(=)(!)(i32 0L)(i32 anint)(=)(&&)(||)(ret)");
    }

    @Test
    public void testComparisonOperators() throws Exception {
        for (String op : new String[]{"<", "<=", ">", ">=", "<>", "="}) {
            setUp1();
            serialize("along " + op + " 0");
            assertIR("(i64 0L)(i64 along)(" + op + ")(ret)");
        }
    }

    @Test
    public void testArithmeticOperators() throws Exception {
        for (String op : new String[]{"+", "-", "*", "/"}) {
            setUp1();
            serialize("along " + op + " 42 != -1");
            assertIR("(i64 -1L)(i64 42L)(i64 along)(" + op + ")(<>)(ret)");
        }
    }

    @Test
    public void testMixedConstantColumn() throws Exception {
        serialize("anint * 3 + 42.5 + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f64 42.5D)(i32 3L)(i32 anint)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testMixedConstantColumnIntOverflow() throws Exception {
        serialize("anint * 2147483648 + 42.5 + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f64 42.5D)(i64 2147483648L)(i32 anint)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testMixedConstantColumnFloatConstant() throws Exception {
        serialize("anint * 3 + 42.5f + adouble > 1");
        assertIR("(i32 1L)(f64 adouble)(f32 42.5D)(i32 3L)(i32 anint)(*)(+)(+)(>)(ret)");
    }

    @Test
    public void testNullConstantValues() throws Exception {
        String[][] columns = new String[][]{
                {"anint", "i32", Numbers.INT_NaN + "L"},
                {"along", "i64", Numbers.LONG_NaN + "L"},
                {"ageobyte", "i8", GeoHashes.BYTE_NULL + "L"},
                {"ageoshort", "i16", GeoHashes.SHORT_NULL + "L"},
                {"ageoint", "i32", GeoHashes.INT_NULL + "L"},
                {"ageolong", "i64", GeoHashes.NULL + "L"},
                {"afloat", "f32", "NaND"},
                {"adouble", "f64", "NaND"},
        };

        for (String[] col : columns) {
            setUp1();
            final String name = col[0];
            final String type = col[1];
            final String value = col[2];
            serialize(name + " <> null");
            assertIR("different results for " + name, "(" + type + " " + value + ")(" + type + " " + name + ")(<>)(ret)");
        }
    }

    @Test
    public void testNullConstantMixedFloatColumns() throws Exception {
        serialize("afloat + adouble <> null");
        assertIR("(f64 NaND)(f64 adouble)(f32 afloat)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstantMixedIntegerColumns() throws Exception {
        serialize("anint + along <> null or null <> along + anint");
        assertIR("(i32 anint)(i64 along)(+)(i64 " + Numbers.LONG_NaN + "L)(<>)" +
                "(i64 " + Numbers.LONG_NaN + "L)(i64 along)(i32 anint)(+)(<>)" +
                "(||)(ret)");
    }

    @Test
    public void testNullConstantMixedFloatIntegerColumns() throws Exception {
        serialize("afloat + along <> null and null <> along + afloat");
        assertIR("(f32 afloat)(i64 along)(+)(f64 NaND)(<>)" +
                "(f64 NaND)(i64 along)(f32 afloat)(+)(<>)" +
                "(&&)(ret)");
    }

    @Test
    public void testNullConstantMultipleExpressions() throws Exception {
        serialize("ageoint <> null and along <> null");
        assertIR("(i64 -9223372036854775808L)(i64 along)(<>)(i32 -1L)(i32 ageoint)(<>)(&&)(ret)");
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
                {"afloat", "f32", "1", "1L", "i32"},
                {"afloat", "f32", "1.5", "1.5D", "f32"},
                {"afloat", "f32", "-1", "-1L", "i32"},
                {"adouble", "f64", "1", "1L", "i64"},
                {"adouble", "f64", "1.5", "1.5D", "f64"},
                {"adouble", "f64", "-1", "-1L", "i64"},
        };

        for (String[] col : columns) {
            setUp1();
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
    public void testBooleanConstant() throws Exception {
        serialize("aboolean = true or not aboolean = not false");
        assertIR("(i8 0L)(!)(i8 aboolean)(=)(!)(i8 1L)(i8 aboolean)(=)(||)(ret)");
    }

    @Test
    public void testCharConstant() throws Exception {
        serialize("achar = 'a'");
        assertIR("(i16 97L)(i16 achar)(=)(ret)");
    }

    @Test
    public void testKnownSymbolConstant() throws Exception {
        serialize("asymbol = '" + KNOWN_SYMBOL_1 + "' or anothersymbol = '" + KNOWN_SYMBOL_2 + "'");
        assertIR("(i32 0L)(i32 anothersymbol)(=)(i32 0L)(i32 asymbol)(=)(||)(ret)");
    }

    @Test
    public void testUnknownSymbolConstant() throws Exception {
        serialize("asymbol = '" + UNKNOWN_SYMBOL + "'");
        assertIR("(i32 :0)(i32 asymbol)(=)(ret)");

        Assert.assertEquals(1, bindVarFunctions.size());
        Assert.assertEquals(ColumnType.SYMBOL, bindVarFunctions.get(0).getType());
        Assert.assertEquals(UNKNOWN_SYMBOL, bindVarFunctions.get(0).getStr(null));
    }

    @Test
    public void testNegatedColumn() throws Exception {
        serialize("-ashort > 0");
        assertIR("(i16 0L)(i16 ashort)(neg)(>)(ret)");
    }

    @Test
    public void testNegatedExpression() throws Exception {
        serialize("-(anint + 42) = -10");
        assertIR("(i32 -10L)(i32 42L)(i32 anint)(+)(neg)(=)(ret)");
    }

    @Test
    public void testNotNullColumns() throws Exception {
        RecordMetadata originalMetadata = metadata;
        TestRecordMetadata metadataWrapper = new TestRecordMetadata(metadata);
        metadataWrapper.setNotNullColumn("afloat");
        metadataWrapper.setNotNullColumn("along");
        metadata = metadataWrapper;

        try {
            serialize("abyte + afloat = -1 or along - anint  = 1");
            assertIR("(i32 1L)(i32 anint)(i64 not_null along)(-)(=)" +
                    "(i32 -1L)(f32 not_null afloat)(i8 abyte)(+)(=)" +
                    "(||)(ret)");
        } finally {
            metadata = originalMetadata;
        }
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
            setUp1();
            final String name = col[0];
            final String type = col[1];
            final String constant = col[2];
            final String value = col[3];
            serialize(name + " = " + constant);
            assertIR("different results for " + name, "(" + type + " " + value + ")(" + type + " " + name + ")(=)(ret)");
        }
    }

    @Test
    public void testOptionsDebugFlag() throws Exception {
        int options = serialize("abyte = 0", false, true, false);
        Assert.assertEquals(0b00001001, options);
    }

    @Test
    public void testOptionsNullChecksFlag() throws Exception {
        int options = serialize("abyte = 0", false, true, true);
        Assert.assertEquals(0b00101001, options);
    }

    @Test
    public void testOptionsScalarFlag() throws Exception {
        int options = serialize("abyte = 0", true, false, false);
        Assert.assertEquals(0b00000000, options);
    }

    @Test
    public void testOptionsSingleSize() throws Exception {
        Map<String, Integer> filterToOptions = new HashMap<>();
        // 1B
        filterToOptions.put("not aboolean", 0b00001000);
        filterToOptions.put("abyte = 0", 0b00001000);
        filterToOptions.put("ageobyte <> null", 0b00001000);
        filterToOptions.put("abyte + abyte = 0", 0b00001000);
        // 2B
        filterToOptions.put("ashort = 0", 0b00001010);
        filterToOptions.put("ageoshort <> null", 0b00001010);
        filterToOptions.put("achar = 'a'", 0b00001010);
        filterToOptions.put("ashort * ashort = 0", 0b00001010);
        // 4B
        filterToOptions.put("anint = 0", 0b00001100);
        filterToOptions.put("ageoint <> null", 0b00001100);
        filterToOptions.put("afloat = 0", 0b00001100);
        filterToOptions.put("asymbol <> null", 0b00001100);
        filterToOptions.put("anint / anint = 0", 0b00001100);
        filterToOptions.put("afloat = 0 or anint = 0", 0b00001100);
        // 8B
        filterToOptions.put("along = 0", 0b00001110);
        filterToOptions.put("ageolong <> null", 0b00001110);
        filterToOptions.put("adate <> null", 0b00001110);
        filterToOptions.put("atimestamp <> null", 0b00001110);
        filterToOptions.put("adouble = 0", 0b00001110);
        filterToOptions.put("adouble = 0 and along = 0", 0b00001110);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            setUp1();
            int options = serialize(entry.getKey(), false, false, false);
            Assert.assertEquals("options mismatch for filter: " + entry.getKey(), (int) entry.getValue(), options);
        }
    }

    @Test
    public void testOptionsMixedSizes() throws Exception {
        Map<String, Integer> filterToOptions = new HashMap<>();
        // 2B
        filterToOptions.put("aboolean or ashort = 0", 0b00010010);
        filterToOptions.put("abyte = 0 or ashort = 0", 0b00010010);
        filterToOptions.put("abyte + ashort = 0", 0b00010010);
        // 4B
        filterToOptions.put("anint = 0 or abyte = 0", 0b00010100);
        filterToOptions.put("afloat = 0 or abyte = 0", 0b00010100);
        filterToOptions.put("afloat / abyte = 0", 0b00010100);
        // 8B
        filterToOptions.put("along = 0 or ashort = 0", 0b00010110);
        filterToOptions.put("adouble = 0 or ashort = 0", 0b00010110);
        filterToOptions.put("afloat = 0 or adouble = 0", 0b00010110);
        filterToOptions.put("anint * along = 0", 0b00010110);

        for (Map.Entry<String, Integer> entry : filterToOptions.entrySet()) {
            setUp1();
            int options = serialize(entry.getKey(), false, false, false);
            Assert.assertEquals("options mismatch for filter: " + entry.getKey(), (int) entry.getValue(), options);
        }
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

        serialize(
                "aboolean = :aboolean or abyte = :abyte or ageobyte = :ageobyte" + // i8
                        " or ashort = :ashort or ageoshort = :ageoshort or achar = :achar" + // i16
                        " or anint = :anint or ageoint = :ageoint or asymbol = :asymbol" + // i32
                        " or along = :along or adate = :adate or ageolong = :ageolong or atimestamp = :atimestamp" + // i64
                        " or afloat = :afloat" + // f32
                        " or adouble = :adouble" // f64
        );
        assertIR("(f64 :0)(f64 adouble)" +
                "(=)(f32 :1)(f32 afloat)(=)" +
                "(i64 :2)(i64 atimestamp)(=)(i64 :3)(i64 ageolong)(=)(i64 :4)(i64 adate)(=)(i64 :5)(i64 along)(=)" +
                "(i32 :6)(i32 asymbol)(=)(i32 :7)(i32 ageoint)(=)(i32 :8)(i32 anint)(=)" +
                "(i16 :9)(i16 achar)(=)(i16 :10)(i16 ageoshort)(=)(i16 :11)(i16 ashort)(=)" +
                "(i8 :12)(i8 ageobyte)(=)(i8 :13)(i8 abyte)(=)(i8 :14)(i8 aboolean)(=)" +
                "(||)(||)(||)(||)(||)(||)(||)(||)(||)(||)(||)(||)(||)(||)(ret)");

        Assert.assertEquals(15, bindVarFunctions.size());
    }

    @Test
    public void testBindVariablesMixed() throws Exception {
        bindVariableService.clear();
        bindVariableService.setInt("anint", 1);
        bindVariableService.setLong(0, 2);

        serialize("anint = :anint or along = $1");
        assertIR("(i64 :0)(i64 along)(=)(i32 :1)(i32 anint)(=)(||)(ret)");

        Assert.assertEquals(2, bindVarFunctions.size());
        Assert.assertEquals(ColumnType.LONG, bindVarFunctions.get(0).getType());
        Assert.assertEquals(ColumnType.INT, bindVarFunctions.get(1).getType());
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSingleConstantExpression() throws Exception {
        serialize("true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSingleNonBooleanColumnExpression() throws Exception {
        serialize("anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedConstantExpression() throws Exception {
        serialize("2 > 1");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedConstantSubexpression() throws Exception {
        serialize("anint = 0 or 2 > 1");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedColumnType() throws Exception {
        serialize("astring = 'a'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBitwiseOperator() throws Exception {
        serialize("~abyte <> 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFunctionToken() throws Exception {
        serialize("atimestamp + now() > 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedOperatorToken() throws Exception {
        serialize("asymbol in (select rnd_symbol('A','B','C') from long_sequence(10))");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStringConstant() throws Exception {
        serialize("achar = 'abc'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBindVariableType() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("astring", "foobar");
        serialize("astring = :astring");
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
    public void testUnsupportedMixedSymbolAndNumericColumns() throws Exception {
        serialize("asymbol = anint");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedMixedGeoHashAndNumericColumns() throws Exception {
        serialize("ageoint = along");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedByteNullConstant() throws Exception {
        serialize("abyte = null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedShortNullConstant() throws Exception {
        serialize("ashort = null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedNullType() throws Exception {
        serialize("astring <> null");
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
    public void testUnsupportedTrueConstantInNumericContext() throws Exception {
        serialize("along = true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFalseConstantInNumericContext() throws Exception {
        serialize("along = false");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedCharConstantInNumericContext() throws Exception {
        serialize("along = 'x'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBooleanColumnInNumericContext() throws Exception {
        serialize("aboolean = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedCharColumnInNumericContext() throws Exception {
        serialize("achar = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashColumnInNumericContext() throws Exception {
        serialize("ageolong = 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedInvalidGeoHashConstant() throws Exception {
        serialize("ageolong = ##11211");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashConstantTooFewBits() throws Exception {
        serialize("ageolong = ##10001");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedGeoHashConstantTooManyChars() throws Exception {
        serialize("ageolong = #sp052w92p1p8889");
    }

    private void serialize(CharSequence seq) throws SqlException {
        serialize(seq, false, false, true);
    }

    private int serialize(CharSequence seq, boolean scalar, boolean debug, boolean nullChecks) throws SqlException {
        ExpressionNode node = expr(seq);
        return serializer.of(irMemory, sqlExecutionContext, metadata, reader, columnIndexes, bindVarFunctions)
                .serialize(node, scalar, debug, nullChecks);
    }

    private void assertIR(String message, String expectedIR) {
        TestIRSerializer ser = new TestIRSerializer(irMemory, metadata);
        String actualIR = ser.serialize();
        Assert.assertEquals(message, expectedIR, actualIR);
    }

    private void assertIR(String expectedIR) {
        assertIR(null, expectedIR);
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
                byte b = irMem.getByte(offset);
                offset += Byte.BYTES;
                switch (b) {
                    // Columns
                    case MEM_I1:
                    case MEM_I2:
                    case MEM_I4:
                    case MEM_I8:
                    case MEM_F4:
                    case MEM_F8:
                        appendColumn(b);
                        break;
                    // Bind variables
                    case VAR_I1:
                    case VAR_I2:
                    case VAR_I4:
                    case VAR_I8:
                    case VAR_F4:
                    case VAR_F8:
                        appendBindVariable(b);
                        break;
                    // Constants
                    case IMM_I1:
                    case IMM_I2:
                    case IMM_I4:
                    case IMM_I8:
                        appendLongConst(b);
                        break;
                    case IMM_F4:
                    case IMM_F8:
                        appendDoubleConst(b);
                        break;
                    // Operators
                    default:
                        appendOperator(b);
                        break;
                }
            }
            return sb.toString();
        }

        private void appendColumn(byte type) {
            sb.append("(");
            long index = irMem.getLong(offset);
            offset += Long.BYTES;
            sb.append(typeName(type));
            sb.append(" ");
            if ((NOT_NULL_COLUMN_MASK & index) != 0) {
                sb.append("not_null");
                sb.append(" ");
                index &= ~NOT_NULL_COLUMN_MASK;
            }
            sb.append(metadata.getColumnName((int) index));
            sb.append(")");
        }

        private void appendBindVariable(byte type) {
            sb.append("(");
            long index = irMem.getLong(offset);
            offset += Long.BYTES;
            sb.append(typeName(type));
            sb.append(" :");
            sb.append(index);
            sb.append(")");
        }

        private void appendLongConst(byte type) {
            sb.append("(");
            long value = irMem.getLong(offset);
            offset += Long.BYTES;
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(value);
            sb.append("L)");
        }

        private void appendDoubleConst(byte type) {
            sb.append("(");
            double value = irMem.getDouble(offset);
            offset += Double.BYTES;
            sb.append(typeName(type));
            sb.append(" ");
            sb.append(value);
            sb.append("D)");
        }

        private void appendOperator(byte operator) {
            sb.append("(");
            sb.append(operatorName(operator));
            sb.append(")");
        }

        private String typeName(byte type) {
            switch (type) {
                case MEM_I1:
                case IMM_I1:
                case VAR_I1:
                    return "i8";
                case MEM_I2:
                case IMM_I2:
                case VAR_I2:
                    return "i16";
                case MEM_I4:
                case IMM_I4:
                case VAR_I4:
                    return "i32";
                case MEM_I8:
                case IMM_I8:
                case VAR_I8:
                    return "i64";
                case MEM_F4:
                case IMM_F4:
                case VAR_F4:
                    return "f32";
                case MEM_F8:
                case IMM_F8:
                case VAR_F8:
                    return "f64";
                default:
                    return "unknown";
            }
        }

        private String operatorName(byte operator) {
            switch (operator) {
                case NEG:
                    return "neg";
                case NOT:
                    return "!";
                case AND:
                    return "&&";
                case OR:
                    return "||";
                case EQ:
                    return "=";
                case NE:
                    return "<>";
                case LT:
                    return "<";
                case LE:
                    return "<=";
                case GT:
                    return ">";
                case GE:
                    return ">=";
                case ADD:
                    return "+";
                case SUB:
                    return "-";
                case MUL:
                    return "*";
                case DIV:
                    return "/";
                case RET:
                    return "ret";
                default:
                    return "unknown";
            }
        }
    }

    private static class TestRecordMetadata implements RecordMetadata {

        private final RecordMetadata delegate;
        private final boolean[] nullableColumns;

        public TestRecordMetadata(RecordMetadata delegate) {
            this.delegate = delegate;
            nullableColumns = new boolean[delegate.getColumnCount()];
            Arrays.fill(nullableColumns, true);
        }

        public void setNotNullColumn(CharSequence columnName) {
            nullableColumns[delegate.getColumnIndex(columnName)] = false;
        }

        @Override
        public boolean isColumnNullable(int columnIndex) {
            return nullableColumns[columnIndex];
        }

        @Override
        public int getColumnCount() {
            return delegate.getColumnCount();
        }

        @Override
        public int getColumnType(int columnIndex) {
            return delegate.getColumnType(columnIndex);
        }

        @Override
        public int getColumnIndex(CharSequence columnName) {
            return delegate.getColumnIndex(columnName);
        }

        @Override
        public int getColumnIndexQuiet(CharSequence columnName) {
            return delegate.getColumnIndexQuiet(columnName);
        }

        @Override
        public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
            return delegate.getColumnIndexQuiet(columnName, lo, hi);
        }

        @Override
        public String getColumnName(int columnIndex) {
            return delegate.getColumnName(columnIndex);
        }

        @Override
        public long getColumnHash(int columnIndex) {
            return delegate.getColumnHash(columnIndex);
        }

        @Override
        public int getColumnType(CharSequence columnName) {
            return delegate.getColumnType(columnName);
        }

        @Override
        public int getIndexValueBlockCapacity(int columnIndex) {
            return delegate.getIndexValueBlockCapacity(columnIndex);
        }

        @Override
        public int getIndexValueBlockCapacity(CharSequence columnName) {
            return delegate.getIndexValueBlockCapacity(columnName);
        }

        @Override
        public int getTimestampIndex() {
            return delegate.getTimestampIndex();
        }

        @Override
        public boolean isSymbolTableStatic(CharSequence columnName) {
            return delegate.isSymbolTableStatic(columnName);
        }

        @Override
        public RecordMetadata getMetadata(int columnIndex) {
            return delegate.getMetadata(columnIndex);
        }

        @Override
        public boolean isColumnIndexed(int columnIndex) {
            return delegate.isColumnIndexed(columnIndex);
        }

        @Override
        public boolean isSymbolTableStatic(int columnIndex) {
            return delegate.isSymbolTableStatic(columnIndex);
        }

        @Override
        public void toJson(CharSink sink) {
            delegate.toJson(sink);
        }
    }

}
