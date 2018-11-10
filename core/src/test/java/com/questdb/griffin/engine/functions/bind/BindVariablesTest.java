/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.bind;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.BaseFunctionFactoryTest;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.TestBinarySequence;
import com.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import com.questdb.griffin.engine.functions.math.*;
import com.questdb.griffin.engine.functions.str.*;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.Rnd;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BindVariablesTest extends BaseFunctionFactoryTest {

    private static final FunctionBuilder builder = new FunctionBuilder();

    private static final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();

    @Test
    public void testAmbiguousCall() throws SqlException {

        bindVariableService.setDate("xyz", 0);

        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharDateFunctionFactory())
                .withFunction(new ToCharTimestampFunctionFactory())
                .$();

        func.init(null, bindVariableService);
        TestUtils.assertEquals("1970-01", func.getStr(builder.getRecord()));
    }

    @Test
    public void testBin() throws SqlException {
        Rnd rnd = new Rnd();
        TestBinarySequence sequence = new TestBinarySequence();
        sequence.of(rnd.nextBytes(256));
        bindVariableService.setBin("x", sequence);

        Function func = expr("to_char(:x)")
                .withFunction(new ToCharBinFunctionFactory())
                .withFunction(new ToCharTimestampFunctionFactory())
                .$();

        func.init(null, bindVariableService);

        Function func2 = expr("length(:x)")
                .withFunction(new LengthBinFunctionFactory())
                .$();

        func2.init(null, bindVariableService);

        TestUtils.assertEquals("00000000 56 54 4a 57 43 50 53 57 48 59 52 58 50 45 48 4e\n" +
                        "00000010 52 58 47 5a 53 58 55 58 49 42 42 54 47 50 47 57\n" +
                        "00000020 46 46 59 55 44 45 59 59 51 45 48 42 48 46 4f 57\n" +
                        "00000030 4c 50 44 58 59 53 42 45 4f 55 4f 4a 53 48 52 55\n" +
                        "00000040 45 44 52 51 51 55 4c 4f 46 4a 47 45 54 4a 52 53\n" +
                        "00000050 5a 53 52 59 52 46 42 56 54 4d 48 47 4f 4f 5a 5a\n" +
                        "00000060 56 44 5a 4a 4d 59 49 43 43 58 5a 4f 55 49 43 57\n" +
                        "00000070 45 4b 47 48 56 55 56 53 44 4f 54 53 45 44 59 59\n" +
                        "00000080 43 54 47 51 4f 4c 59 58 57 43 4b 59 4c 53 55 57\n" +
                        "00000090 44 53 57 55 47 53 48 4f 4c 4e 56 54 49 51 42 5a\n" +
                        "000000a0 58 49 4f 56 49 4b 4a 53 4d 53 53 55 51 53 52 4c\n" +
                        "000000b0 54 4b 56 56 53 4a 4f 4a 49 50 48 5a 45 50 49 48\n" +
                        "000000c0 56 4c 54 4f 56 4c 4a 55 4d 4c 47 4c 48 4d 4c 4c\n" +
                        "000000d0 45 4f 59 50 48 52 49 50 5a 49 4d 4e 5a 5a 52 4d\n" +
                        "000000e0 46 4d 42 45 5a 47 48 57 56 44 4b 46 4c 4f 50 4a\n" +
                        "000000f0 4f 58 50 4b 52 47 49 49 48 59 48 42 4f 51 4d 59",
                func.getStr(builder.getRecord()));

        // check that bin bind variable length is accurate
        Assert.assertEquals(256, func2.getLong(builder.getRecord()));

        sequence.of(rnd.nextBytes(16));

        bindVariableService.setBin("x", sequence);

        TestUtils.assertEquals("00000000 53 53 4d 50 47 4c 55 4f 48 4e 5a 48 5a 53 51 4c",
                func.getStr(builder.getRecord()));

        bindVariableService.setBin("x", new TestBinarySequence().of(rnd.nextBytes(24)));

        TestUtils.assertEquals("00000000 44 47 4c 4f 47 49 46 4f 55 53 5a 4d 5a 56 51 45\n" +
                        "00000010 42 4e 44 43 51 43 45 48",
                func.getStr(builder.getRecord()));
    }

    @Test
    public void testBoolean() throws SqlException {
        bindVariableService.setBoolean("xyz", false);
        Function func = expr("not :xyz")
                .withFunction(new NotFunctionFactory())
                .$();

        func.init(null, bindVariableService);
        Assert.assertTrue(func.getBool(builder.getRecord()));

        bindVariableService.setBoolean("xyz", true);
        Assert.assertFalse(func.getBool(builder.getRecord()));
    }

    @Test
    public void testByte() throws SqlException {
        bindVariableService.setByte("xyz", (byte) 8);
        Function func = expr("b + :xyz")
                .withFunction(new AddByteFunctionFactory())
                .withColumn("b", ColumnType.BYTE, (byte) 22)
                .$();

        func.init(null, bindVariableService);
        Assert.assertEquals(30, func.getByte(builder.getRecord()));

        bindVariableService.setByte("xyz", (byte) 10);
        Assert.assertEquals(32, func.getByte(builder.getRecord()));
    }

    @Test
    public void testDate() throws SqlException, NumericException {
        bindVariableService.setDate("xyz", DateFormatUtils.parseDateTime("2015-04-10T10:00:00.000Z"));
        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharDateFunctionFactory())
                .$();

        func.init(null, bindVariableService);
        TestUtils.assertEquals("2015-04", func.getStr(builder.getRecord()));

        bindVariableService.setDate("xyz", DateFormatUtils.parseDateTime("2015-08-10T10:00:00.000Z"));
        TestUtils.assertEquals("2015-08", func.getStr(builder.getRecord()));
    }

    @Test
    public void testDouble() throws SqlException {
        bindVariableService.setDouble("xyz", 7.98821);
        Function func = expr("a + :xyz")
                .withFunction(new AddDoubleFunctionFactory())
                .withColumn("a", ColumnType.DOUBLE, 25.1)
                .$();

        func.init(null, bindVariableService);
        Assert.assertEquals(33.08821, func.getDouble(builder.getRecord()), 0.00001);

        bindVariableService.setDouble("xyz", 0.12311);
        Assert.assertEquals(25.22311, func.getDouble(builder.getRecord()), 0.00001);
    }

    @Test
    public void testFloat() throws SqlException {
        bindVariableService.setFloat("xyz", 7.6f);

        Function func = expr("a + :xyz")
                .withFunction(new AddFloatFunctionFactory())
                .withColumn("a", ColumnType.FLOAT, 25.1f)
                .$();

        func.init(null, bindVariableService);
        Assert.assertEquals(32.7f, func.getFloat(builder.getRecord()), 0.001f);

        bindVariableService.setFloat("xyz", 0.78f);

        func.init(null, bindVariableService);

        Assert.assertEquals(25.88f, func.getFloat(builder.getRecord()), 0.001f);

        func.close();
    }

    @Test
    public void testInt() throws SqlException {
        bindVariableService.setInt("xyz", 10);
        bindVariableService.setInt("zz", 5);

        Function func = expr("a + :xyz + :xyz - :zz")
                .withFunction(new AddIntFunctionFactory())
                .withFunction(new SubIntFunctionFactory())
                .withColumn("a", ColumnType.INT, 22)
                .$();

        func.init(null, bindVariableService);

        Assert.assertEquals(37, func.getInt(builder.getRecord()));

        bindVariableService.setInt("zz", 8);
        Assert.assertEquals(34, func.getInt(builder.getRecord()));
    }

    @Test
    public void testLong() throws SqlException {
        bindVariableService.setLong("xyz", 9);
        Function func = expr("a + :xyz")
                .withFunction(new AddLongFunctionFactory())
                .withColumn("a", ColumnType.LONG, 22L)
                .$();

        func.init(null, bindVariableService);

        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        bindVariableService.setLong("xyz", 11);
        Assert.assertEquals(33L, func.getLong(builder.getRecord()));
    }

    @Test
    public void testShort() throws SqlException {
        bindVariableService.setShort("xyz", (short) 8);
        Function func = expr("b + :xyz")
                .withFunction(new AddShortFunctionFactory())
                .withColumn("b", ColumnType.SHORT, (short) 22)
                .$();

        func.init(null, bindVariableService);
        Assert.assertEquals(30, func.getShort(builder.getRecord()));

        bindVariableService.setShort("xyz", (short) 33);
        Assert.assertEquals(55, func.getShort(builder.getRecord()));
    }

    @Test
    public void testStr() throws SqlException {
        bindVariableService.setStr("str", "abc");
        Function func = expr("length(:str)")
                .withFunction(new LengthStrFunctionFactory())
                .$();

        func.init(null, bindVariableService);
        Assert.assertEquals(3, func.getInt(builder.getRecord()));

        bindVariableService.setStr("str", "hello");
        Assert.assertEquals(5, func.getInt(builder.getRecord()));
    }

    @Test
    public void testStr2() throws SqlException {
        bindVariableService.setStr("str", "abcd");
        bindVariableService.setInt("start", 1);

        Function func = expr("substr(:str, :start)")
                .withFunction(new SubStrFunctionFactory())
                .$();

        func.init(null, bindVariableService);

        TestUtils.assertEquals("bcd", func.getStr(builder.getRecord()));
    }

    @Test
    public void testTimestamp() throws SqlException, NumericException {
        bindVariableService.setTimestamp("xyz", com.questdb.std.microtime.DateFormatUtils.parseDateTime("2015-04-10T10:00:00.000Z"));

        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharTimestampFunctionFactory())
                .$();

        func.init(null, bindVariableService);
        TestUtils.assertEquals("2015-04", func.getStr(builder.getRecord()));

        bindVariableService.setTimestamp("xyz", com.questdb.std.microtime.DateFormatUtils.parseDateTime("2015-08-10T10:00:00.000Z"));
        TestUtils.assertEquals("2015-08", func.getStr(builder.getRecord()));
    }

    @Test
    public void testUndefined() {
        try {
            expr("to_char(:xyz, 'yyyy-MM')")
                    .withFunction(new ToCharDateFunctionFactory())
                    .withFunction(new ToCharTimestampFunctionFactory())
                    .$();
        } catch (SqlException e) {
            Assert.assertEquals(8, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "undefined bind variable: :xyz");
        }
    }

    private FunctionBuilder expr(String expression) {
        return builder.withExpression(expression);
    }

    private static class FunctionBuilder {
        final ObjList<Object> columnValues = new ObjList<>();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final Record record = new Record() {
            @Override
            public byte getByte(int col) {
                return (byte) columnValues.get(col);
            }

            @Override
            public double getDouble(int col) {
                return (double) columnValues.get(col);
            }

            @Override
            public float getFloat(int col) {
                return (float) columnValues.get(col);
            }

            @Override
            public int getInt(int col) {
                return (int) columnValues.get(col);
            }

            @Override
            public long getLong(int col) {
                return (long) columnValues.get(col);
            }

            @Override
            public short getShort(int col) {
                return (short) columnValues.get(col);
            }
        };
        String expression;

        private Function $() throws SqlException {
            return parseFunction(expression, metadata, new FunctionParser(configuration, functions));
        }

        Record getRecord() {
            return record;
        }

        private FunctionBuilder withColumn(String name, int type, Object value) {
            metadata.add(new TableColumnMetadata(name, type));
            columnValues.add(value);
            return this;
        }

        private FunctionBuilder withExpression(String expression) {
            BindVariablesTest.functions.clear();
            this.expression = expression;
            columnValues.clear();
            this.metadata.clear();
            return this;
        }

        private FunctionBuilder withFunction(FunctionFactory functionFactory) {
            BindVariablesTest.functions.add(functionFactory);
            return this;
        }
    }
}