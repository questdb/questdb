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

package com.questdb.griffin.engine.params;

import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.griffin.*;
import com.questdb.griffin.engine.functions.math.*;
import com.questdb.griffin.engine.functions.str.LengthStrVFunctionFactory;
import com.questdb.griffin.engine.functions.str.SubStrVVFunctionFactory;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.std.ObjList;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ParameterTest extends BaseFunctionFactoryTest {

    private static FunctionBuilder builder = new FunctionBuilder();

    @Test
    public void testByte() throws SqlException {
        Function func = expr("b + :xyz")
                .withFunction(new AddByteVVFunctionFactory())
                .withColumn("b", ColumnType.BYTE, (byte) 22)
                .$();

        param(":xyz").setByte((byte) 8);
        Assert.assertEquals(30, func.getByte(builder.getRecord()));

        param(":xyz").setByte((byte) 10);
        Assert.assertEquals(32, func.getByte(builder.getRecord()));

        try {
            param(":xyz").setShort((short) 8);
            func.getByte(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=BYTE, actual=SHORT]");
        }


        try {
            param(":xyz").setStr("abc");
            func.getByte(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "[required=BYTE, actual=STRING]");
        }
    }

    @Test
    public void testDouble() throws SqlException {
        Function func = expr("a + :xyz")
                .withFunction(new AddDoubleVVFunctionFactory())
                .withColumn("a", ColumnType.DOUBLE, 25.1)
                .$();

        param(":xyz").setLong(9);
        Assert.assertEquals(34.1, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setInt(9);
        Assert.assertEquals(34.1, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setShort((short) 9);
        Assert.assertEquals(34.1, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setByte((byte) 9);
        Assert.assertEquals(34.1, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setFloat(7.6f);
        Assert.assertEquals(32.7, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setDouble(7.98821);
        Assert.assertEquals(33.08821, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setDouble(0.12311);
        Assert.assertEquals(25.22311, func.getDouble(builder.getRecord()), 0.00001);

        param(":xyz").setStr("abc");
        try {
            func.getDouble(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=DOUBLE, actual=STRING]");
        }
    }

    @Test
    public void testFloat() throws SqlException {
        Function func = expr("a + :xyz")
                .withFunction(new AddFloatVVFunctionFactory())
                .withColumn("a", ColumnType.FLOAT, 25.1f)
                .$();

        param(":xyz").setLong(9);
        Assert.assertEquals(34.1f, func.getFloat(builder.getRecord()), 0.001f);

        param(":xyz").setInt(9);
        Assert.assertEquals(34.1f, func.getFloat(builder.getRecord()), 0.001f);

        param(":xyz").setShort((short) 9);
        Assert.assertEquals(34.1f, func.getFloat(builder.getRecord()), 0.001f);

        param(":xyz").setByte((byte) 9);
        Assert.assertEquals(34.1f, func.getFloat(builder.getRecord()), 0.001f);

        param(":xyz").setFloat(7.6f);
        Assert.assertEquals(32.7f, func.getFloat(builder.getRecord()), 0.001f);

        param(":xyz").setFloat(0.78f);
        Assert.assertEquals(25.88f, func.getFloat(builder.getRecord()), 0.001f);

        try {
            param(":xyz").setDouble(0.78);
            func.getFloat(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=FLOAT, actual=DOUBLE]");
        }
    }

    @Test
    public void testInt() throws SqlException {
        Function func = expr("a + :xyz + :xyz - :zz")
                .withFunction(new AddIntVVFunctionFactory())
                .withFunction(new SubtractIntVVFunctionFactory())
                .withColumn("a", ColumnType.INT, 22)
                .$();

        param(":xyz").setInt(10);
        param(":zz").setInt(5);
        Assert.assertEquals(37, func.getInt(builder.getRecord()));

        param(":zz").setInt(8);
        Assert.assertEquals(34, func.getInt(builder.getRecord()));

        param(":zz").setShort((short) 8);
        Assert.assertEquals(34, func.getInt(builder.getRecord()));

        param(":zz").setByte((byte) 8);
        Assert.assertEquals(34, func.getInt(builder.getRecord()));

        try {
            param(":zz").setLong(8);
            func.getInt(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(18, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=INT, actual=LONG]");
        }
    }

    @Test
    public void testLong() throws SqlException {
        Function func = expr("a + :xyz")
                .withFunction(new AddLongVVFunctionFactory())
                .withColumn("a", ColumnType.LONG, 22L)
                .$();

        param(":xyz").setLong(9);
        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        param(":xyz").setLong(11);
        Assert.assertEquals(33L, func.getLong(builder.getRecord()));

        param(":xyz").setInt(9);
        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        param(":xyz").setShort((short) 9);
        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        param(":xyz").setByte((byte) 9);
        Assert.assertEquals(31L, func.getLong(builder.getRecord()));

        try {
            param(":xyz").setFloat(9f);
            func.getLong(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=LONG, actual=FLOAT]");
        }
    }

    @Test
    public void testShort() throws SqlException {
        Function func = expr("b + :xyz")
                .withFunction(new AddShortVVFunctionFactory())
                .withColumn("b", ColumnType.SHORT, (short) 22)
                .$();

        param(":xyz").setShort((short) 8);
        Assert.assertEquals(30, func.getShort(builder.getRecord()));

        param(":xyz").setShort((short) 33);
        Assert.assertEquals(55, func.getShort(builder.getRecord()));

        param(":xyz").setByte((byte) 8);
        Assert.assertEquals(30, func.getShort(builder.getRecord()));

        try {
            param(":xyz").setInt(8);
            func.getShort(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(4, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=SHORT, actual=INT]");
        }
    }

    @Test
    public void testStr() throws SqlException {
        Function func = expr("length(:str)")
                .withFunction(new LengthStrVFunctionFactory())
                .$();

        param(":str").setStr("abc");
        Assert.assertEquals(3, func.getInt(builder.getRecord()));
        assertStrParam("abc", param(":str"));

        param(":str").setStr("efghk");
        Assert.assertEquals(5, func.getInt(builder.getRecord()));
        assertStrParam("efghk", param(":str"));

        param(":str").setStr(null);
        Assert.assertEquals(-1, func.getInt(builder.getRecord()));
        assertStrParam(null, param(":str"));


        try {
            param(":str").setByte((byte) 12);
            func.getInt(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(7, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=STRING, actual=BYTE]");
        }
    }

    @Test
    public void testStr2() throws SqlException {
        Function func = expr("substr(:str, :start)")
                .withFunction(new SubStrVVFunctionFactory())
                .$();

        param(":str").setStr("abcd");
        param(":start").setInt(1);
        TestUtils.assertEquals("bcd", func.getStr(builder.getRecord()));

        param(":str").setInt(123);

        try {
            func.getStr(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(7, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=STRING, actual=INT]");
        }

        try {
            func.getStrB(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(7, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=STRING, actual=INT]");
        }

        try {
            func.getStrLen(builder.getRecord());
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(7, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=STRING, actual=INT]");
        }

        try {
            func.getStr(builder.getRecord(), sink);
            Assert.fail();
        } catch (ParameterException e) {
            Assert.assertEquals(7, e.getPosition());
            TestUtils.assertContains(e.getMessage(), "invalid parameter type [required=STRING, actual=INT]");
        }


    }

    private void assertStrParam(String expected, Parameter param) {
        sink.clear();
        param.getStr(null, sink);

        TestUtils.assertEquals(expected, param.getStr(null));
        TestUtils.assertEquals(expected, param.getStrB(null));
        if (expected == null) {
            Assert.assertEquals(-1, param.getStrLen(null));
            Assert.assertEquals(0, sink.length());
        } else {
            Assert.assertEquals(expected.length(), param.getStrLen(null));
            TestUtils.assertEquals(expected, sink);
        }

    }

    private FunctionBuilder expr(String expression) {
        return builder.withExpression(expression);
    }

    private Parameter param(CharSequence name) {
        Parameter parameter = params.get(name);
        Assert.assertNotNull("parameter " + name + " not found", parameter);
        return parameter;
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
            ParameterTest.functions.clear();
            this.expression = expression;
            columnValues.clear();
            this.metadata.clear();
            return this;
        }

        private FunctionBuilder withFunction(FunctionFactory functionFactory) {
            ParameterTest.functions.add(functionFactory);
            return this;
        }
    }
}