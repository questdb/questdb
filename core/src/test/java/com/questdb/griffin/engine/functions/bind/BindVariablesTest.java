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

import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.griffin.*;
import com.questdb.griffin.engine.functions.bool.NotVFunctionFactory;
import com.questdb.griffin.engine.functions.math.*;
import com.questdb.griffin.engine.functions.str.LengthStrVFunctionFactory;
import com.questdb.griffin.engine.functions.str.SubStrVVFunctionFactory;
import com.questdb.griffin.engine.functions.str.ToCharDateVCFunctionFactory;
import com.questdb.griffin.engine.functions.str.ToCharTimestampVCFunctionFactory;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BindVariablesTest extends BaseFunctionFactoryTest {

    private static FunctionBuilder builder = new FunctionBuilder();

    @Test
    public void testAmbiguousCall() throws SqlException {

        bindVariableService.setDate("xyz", 0);

        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharDateVCFunctionFactory())
                .withFunction(new ToCharTimestampVCFunctionFactory())
                .$();

        TestUtils.assertEquals("1970-01", func.getStr(builder.getRecord()));
    }

    @Test
    public void testBoolean() throws SqlException {
        bindVariableService.setBoolean("xyz", false);
        Function func = expr("not :xyz")
                .withFunction(new NotVFunctionFactory())
                .$();

        Assert.assertTrue(func.getBool(builder.getRecord()));

        bindVariableService.setBoolean("xyz", true);
        Assert.assertFalse(func.getBool(builder.getRecord()));
    }

    @Test
    public void testByte() throws SqlException {
        bindVariableService.setByte("xyz", (byte) 8);
        Function func = expr("b + :xyz")
                .withFunction(new AddByteVVFunctionFactory())
                .withColumn("b", ColumnType.BYTE, (byte) 22)
                .$();

        Assert.assertEquals(30, func.getByte(builder.getRecord()));

        bindVariableService.setByte("xyz", (byte) 10);
        Assert.assertEquals(32, func.getByte(builder.getRecord()));
    }

    @Test
    public void testDate() throws SqlException, NumericException {
        bindVariableService.setDate("xyz", DateFormatUtils.parseDateTime("2015-04-10T10:00:00.000Z"));
        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharDateVCFunctionFactory())
                .$();

        TestUtils.assertEquals("2015-04", func.getStr(builder.getRecord()));

        bindVariableService.setDate("xyz", DateFormatUtils.parseDateTime("2015-08-10T10:00:00.000Z"));
        TestUtils.assertEquals("2015-08", func.getStr(builder.getRecord()));
    }

    @Test
    public void testDouble() throws SqlException {
        bindVariableService.setDouble("xyz", 7.98821);
        Function func = expr("a + :xyz")
                .withFunction(new AddDoubleVVFunctionFactory())
                .withColumn("a", ColumnType.DOUBLE, 25.1)
                .$();

        Assert.assertEquals(33.08821, func.getDouble(builder.getRecord()), 0.00001);

        bindVariableService.setDouble("xyz", 0.12311);
        Assert.assertEquals(25.22311, func.getDouble(builder.getRecord()), 0.00001);
    }

    @Test
    public void testFloat() throws SqlException {
        bindVariableService.setFloat("xyz", 7.6f);

        Function func = expr("a + :xyz")
                .withFunction(new AddFloatVVFunctionFactory())
                .withColumn("a", ColumnType.FLOAT, 25.1f)
                .$();


        Assert.assertEquals(32.7f, func.getFloat(builder.getRecord()), 0.001f);

        bindVariableService.setFloat("xyz", 0.78f);
        Assert.assertEquals(25.88f, func.getFloat(builder.getRecord()), 0.001f);
    }

    @Test
    public void testInt() throws SqlException {
        bindVariableService.setInt("xyz", 10);
        bindVariableService.setInt("zz", 5);

        Function func = expr("a + :xyz + :xyz - :zz")
                .withFunction(new AddIntVVFunctionFactory())
                .withFunction(new SubtractIntVVFunctionFactory())
                .withColumn("a", ColumnType.INT, 22)
                .$();

        Assert.assertEquals(37, func.getInt(builder.getRecord()));

        bindVariableService.setInt("zz", 8);
        Assert.assertEquals(34, func.getInt(builder.getRecord()));
    }

    @Test
    public void testLong() throws SqlException {
        bindVariableService.setLong("xyz", 9);
        Function func = expr("a + :xyz")
                .withFunction(new AddLongVVFunctionFactory())
                .withColumn("a", ColumnType.LONG, 22L)
                .$();

        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        bindVariableService.setLong("xyz", 11);
        Assert.assertEquals(33L, func.getLong(builder.getRecord()));
    }

    @Test
    public void testShort() throws SqlException {
        bindVariableService.setShort("xyz", (short) 8);
        Function func = expr("b + :xyz")
                .withFunction(new AddShortVVFunctionFactory())
                .withColumn("b", ColumnType.SHORT, (short) 22)
                .$();

        Assert.assertEquals(30, func.getShort(builder.getRecord()));

        bindVariableService.setShort("xyz", (short) 33);
        Assert.assertEquals(55, func.getShort(builder.getRecord()));
    }

    @Test
    public void testStr() throws SqlException {
        bindVariableService.setStr("str", "abc");
        Function func = expr("length(:str)")
                .withFunction(new LengthStrVFunctionFactory())
                .$();

        Assert.assertEquals(3, func.getInt(builder.getRecord()));

        bindVariableService.setStr("str", "hello");
        Assert.assertEquals(5, func.getInt(builder.getRecord()));
    }

    @Test
    public void testStr2() throws SqlException {
        bindVariableService.setStr("str", "abcd");
        bindVariableService.setInt("start", 1);

        Function func = expr("substr(:str, :start)")
                .withFunction(new SubStrVVFunctionFactory())
                .$();

        TestUtils.assertEquals("bcd", func.getStr(builder.getRecord()));
    }

    @Test
    public void testTimestamp() throws SqlException, NumericException {
        bindVariableService.setTimestamp("xyz", com.questdb.std.microtime.DateFormatUtils.parseDateTime("2015-04-10T10:00:00.000Z"));

        Function func = expr("to_char(:xyz, 'yyyy-MM')")
                .withFunction(new ToCharTimestampVCFunctionFactory())
                .$();

        TestUtils.assertEquals("2015-04", func.getStr(builder.getRecord()));

        bindVariableService.setTimestamp("xyz", com.questdb.std.microtime.DateFormatUtils.parseDateTime("2015-08-10T10:00:00.000Z"));
        TestUtils.assertEquals("2015-08", func.getStr(builder.getRecord()));
    }

    @Test
    public void testUndefined() {
        try {
            expr("to_char(:xyz, 'yyyy-MM')")
                    .withFunction(new ToCharDateVCFunctionFactory())
                    .withFunction(new ToCharTimestampVCFunctionFactory())
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
        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
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
            metadata.add(new TestColumnMetadata(name, type));
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