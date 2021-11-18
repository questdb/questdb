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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.BaseFunctionFactoryTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static io.questdb.jit.FilterExprIRSerializer.*;

public class FilterExprIRSerializerTest extends BaseFunctionFactoryTest {

    private static TableReader reader;
    private static RecordMetadata metadata;
    private static MemoryCARWImpl irMem;
    private static FilterExprIRSerializer serializer;

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

        reader = new TableReader(configuration, "x");
        metadata = reader.getMetadata();
        irMem = new MemoryCARWImpl(1024, 1, MemoryTag.NATIVE_DEFAULT);
        serializer = new FilterExprIRSerializer();
    }

    @AfterClass
    public static void tearDown2() {
        reader.close();
        irMem.close();
    }

    @Before
    public void setUp1() {
        irMem.truncate();
        serializer.clear();
    }

    @Test
    public void testColumnTypes() throws Exception {
        Map<String, String[]> expectedTypeToColumn = new HashMap<>();
        expectedTypeToColumn.put("i8", new String[]{"aboolean", "abyte", "ageobyte"});
        expectedTypeToColumn.put("i16", new String[]{"ashort", "ageoshort", "achar"});
        expectedTypeToColumn.put("i32", new String[]{"anint", "ageoint", "asymbol"});
        expectedTypeToColumn.put("i64", new String[]{"along", "ageolong", "adate", "atimestamp"});
        expectedTypeToColumn.put("f32", new String[]{"afloat"});
        expectedTypeToColumn.put("f64", new String[]{"adouble"});

        for (String type : expectedTypeToColumn.keySet()) {
            for (String col : expectedTypeToColumn.get(type)) {
                setUp1();
                serialize(col + " < " + col);
                assertIR("(" + type + " " + col + ")(" + type + " " + col + ")(<)(ret)");
            }
        }
    }

    @Test
    public void testBooleanOperators() throws Exception {
        serialize("aboolean and not aboolean or aboolean");
        assertIR("(i8 aboolean)(i8 aboolean)(!)(i8 aboolean)(&&)(||)(ret)");
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
    public void testNullConstValues() throws Exception {
        String[][] columns = new String[][]{
                {"abyte", "i8", "0L"},
                {"ashort", "i16", "0L"},
                {"anint", "i32", Numbers.INT_NaN + "L"},
                {"along", "i64", Numbers.LONG_NaN + "L"},
                {"ageobyte", "i8", "-1L"},
                {"ageoshort", "i16", "-1L"},
                {"ageoint", "i32", "-1L"},
                {"ageolong", "i64", "-1L"},
                {"afloat", "f32", "NaND"},
                {"adouble", "f64", "NaND"},
        };

        for (String[] col : columns) {
            setUp1();
            String name = col[0];
            String type = col[1];
            String value = col[2];
            serialize(name + " <> null");
            assertIR("(" + type + " " + value + ")(" + type + " " + name + ")(<>)(ret)");
        }
    }

    @Test
    public void testNullConstMixedFloatValues() throws Exception {
        serialize("afloat + adouble <> null");
        assertIR("(f64 NaND)(f64 adouble)(f32 afloat)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstMixedLongValues() throws Exception {
        serialize("anint + along <> null");
        assertIR("(i64 " + Numbers.LONG_NaN + "L)(i64 along)(i32 anint)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstMixedFloatLongValues() throws Exception {
        serialize("afloat + along <> null");
        assertIR("(f64 NaND)(i64 along)(f32 afloat)(+)(<>)(ret)");
    }

    @Test
    public void testNullConstMultipleExpressions() throws Exception {
        serialize("ageoint <> null and abyte <> null");
        assertIR("(i8 0L)(i8 abyte)(<>)(i32 -1L)(i32 ageoint)(<>)(&&)(ret)");
    }

    @Test
    public void testPositiveConstTypes() throws Exception {
        String[][] columns = new String[][]{
                {"abyte", "i8", "1L"},
                {"ashort", "i16", "1L"},
                {"anint", "i32", "1L"},
                {"along", "i64", "1L"},
                {"afloat", "f32", "1.0D"},
                {"adouble", "f64", "1.0D"},
        };

        for (String[] col : columns) {
            setUp1();
            String name = col[0];
            String type = col[1];
            String value = col[2];
            serialize(name + " > 1");
            assertIR("(" + type + " " + value + ")(" + type + " " + name + ")(>)(ret)");
        }
    }

    @Test
    public void testNegativeConstTypes() throws Exception {
        String[][] columns = new String[][]{
                {"abyte", "i8", "-1L"},
                {"ashort", "i16", "-1L"},
                {"anint", "i32", "-1L"},
                {"along", "i64", "-1L"},
                {"afloat", "f32", "-1.0D"},
                {"adouble", "f64", "-1.0D"},
        };

        for (String[] col : columns) {
            setUp1();
            String name = col[0];
            String type = col[1];
            String value = col[2];
            serialize(name + " < -1");
            assertIR("(" + type + " " + value + ")(" + type + " " + name + ")(<)(ret)");
        }
    }

    @Test
    public void testNegatedColumn() throws Exception {
        serialize("-ashort > 0");
        assertIR("(i16 0L)(i16 ashort)(neg)(>)(ret)");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedSingleConst() throws Exception {
        serialize("true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedConstExpression() throws Exception {
        serialize("2 > 1");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedColumnType() throws Exception {
        serialize("astring <> null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBitwiseOperator() throws Exception {
        serialize("~abyte <> 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFunctionToken() throws Exception {
        serialize("now() > 0");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedStrConst() throws Exception {
        serialize("achar = 'abc'");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedNonNumericNullType() throws Exception {
        serialize("astring <> null");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedFloatConstInIntContext() throws Exception {
        serialize("anint > 1.5");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedBooleanConstInNumericContext() throws Exception {
        serialize("along = true");
    }

    @Test(expected = SqlException.class)
    public void testUnsupportedCharConstInNumericContext() throws Exception {
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

    private void serialize(CharSequence seq) throws SqlException {
        ExpressionNode node = expr(seq);
        serializer.of(irMem, metadata).serialize(node);
    }

    private void assertIR(String expectedIR) {
        IRToStringSerializer ser = new IRToStringSerializer(irMem, metadata);
        String actualIR = ser.serialize();
        Assert.assertEquals(expectedIR, actualIR);
    }

    private static class IRToStringSerializer {

        private final MemoryCARWImpl irMem;
        private final RecordMetadata metadata;
        private long offset;
        private StringBuilder sb;

        public IRToStringSerializer(MemoryCARWImpl irMem, RecordMetadata metadata) {
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
                    // Variables
                    case MEM_I1:
                    case MEM_I2:
                    case MEM_I4:
                    case MEM_I8:
                    case MEM_F4:
                    case MEM_F8:
                        appendColumn(b);
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
            sb.append(metadata.getColumnName((int) index));
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
                    return "i8";
                case MEM_I2:
                case IMM_I2:
                    return "i16";
                case MEM_I4:
                case IMM_I4:
                    return "i32";
                case MEM_I8:
                case IMM_I8:
                    return "i64";
                case MEM_F4:
                case IMM_F4:
                    return "f32";
                case MEM_F8:
                case IMM_F8:
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

}
