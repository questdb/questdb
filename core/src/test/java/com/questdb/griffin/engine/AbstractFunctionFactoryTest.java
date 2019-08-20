/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.*;
import com.questdb.griffin.engine.functions.cast.ToByteIntFunctionFactory;
import com.questdb.griffin.engine.functions.cast.ToDateLongFunctionFactory;
import com.questdb.griffin.engine.functions.cast.ToShortIntFunctionFactory;
import com.questdb.griffin.engine.functions.cast.ToTimestampLongFunctionFactory;
import com.questdb.std.BinarySequence;
import com.questdb.std.Numbers;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;

public abstract class AbstractFunctionFactoryTest extends BaseFunctionFactoryTest {
    private static int toTimestampRefs = 0;
    private static int toDateRefs = 0;
    private static int toShortRefs = 0;
    private static int toByteRefs = 0;
    private FunctionFactory factory;

    public void assertFailure(int expectedPosition, CharSequence expectedMsg, Object... args) {
        assertFailure(false, expectedPosition, expectedMsg, args);
    }

    public void assertFailure(boolean forceConstant, int expectedPosition, CharSequence expectedMsg, Object... args) {
        try {
            callCustomised(forceConstant, true, args);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(expectedPosition, e.getPosition());
            TestUtils.assertContains(e.getMessage(), expectedMsg);
        }
    }

    protected void addExtraFunctions() {
    }

    protected Invocation call(Object... args) throws SqlException {
        return callCustomised(false, true, args);
    }

    protected Invocation callCustomised(boolean forceConstant, boolean argTypeFromSig, Object... args) throws SqlException {
        setUp2();
        toShortRefs = 0;
        toByteRefs = 0;
        toTimestampRefs = 0;
        toDateRefs = 0;

        final FunctionFactory functionFactory = getFactory0();
        final String signature = functionFactory.getSignature();

        // validate signature first
        final int pos = FunctionParser.validateSignatureAndGetNameSeparator(signature);

        // create metadata

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final String name = signature.substring(0, pos);
        final int argCount;
        final boolean hasVarArg;
        final boolean constVarArg;

        if (signature.indexOf('v', pos) != -1) {
            hasVarArg = true;
            constVarArg = true;
        } else if (signature.indexOf('V', pos) != -1) {
            hasVarArg = true;
            constVarArg = false;
        } else {
            hasVarArg = false;
            constVarArg = false;
        }

        if (hasVarArg) {
            argCount = signature.length() - pos - 3;
            Assert.assertTrue(args.length >= argCount);
        } else {
            argCount = signature.length() - pos - 2;
            Assert.assertEquals("Invalid number of arguments", argCount, args.length);
        }

        final StringSink expression1 = new StringSink();
        final StringSink expression2 = new StringSink();

        final boolean setOperation = OperatorExpression.getOperatorType(name) == OperatorExpression.SET;
        final boolean operator = OperatorExpression.isOperator(name);

        if (operator && !setOperation) {
            switch (argCount) {
                case 0:
                    expression1.put(name);
                    expression2.put(name);
                    break;
                case 1:
                    expression1.put(name).put(' ');
                    expression2.put(name).put(' ');
                    printArgument(
                            signature,
                            pos,
                            forceConstant,
                            metadata,
                            argTypeFromSig,
                            constVarArg,
                            expression1,
                            expression2,
                            0,
                            args[0]);
                    break;
                default:
                    // two args
                    printArgument(
                            signature,
                            pos,
                            forceConstant,
                            metadata,
                            argTypeFromSig,
                            constVarArg,
                            expression1,
                            expression2,
                            0,
                            args[0]);

                    expression1.put(' ').put(name).put(' ');
                    expression2.put(' ').put(name).put(' ');

                    printArgument(
                            signature,
                            pos,
                            forceConstant,
                            metadata,
                            argTypeFromSig,
                            constVarArg,
                            expression1,
                            expression2,
                            1,
                            args[1]);
                    break;
            }
        } else {

            if (!setOperation) {
                expression1.put(name).put('(');
                expression2.put(name).put('(');
            }

            for (int i = 0, n = args.length; i < n; i++) {

                if ((setOperation && i > 1) || (!setOperation && i > 0)) {
                    expression1.put(',');
                    expression2.put(',');
                }

                printArgument(
                        signature,
                        pos,
                        forceConstant,
                        metadata,
                        i < argCount,
                        constVarArg,
                        expression1,
                        expression2,
                        i,
                        args[i]);

                if (i == 0 && setOperation) {
                    expression1.put(' ').put(name).put(' ').put('(');
                    expression2.put(' ').put(name).put(' ').put('(');
                }
            }
            expression1.put(')');
            expression2.put(')');
        }

        functions.add(functionFactory);
        if (toTimestampRefs > 0) {
            functions.add(new ToTimestampLongFunctionFactory());
        }
        if (toDateRefs > 0) {
            functions.add(new ToDateLongFunctionFactory());
        }

        if (toShortRefs > 0) {
            functions.add(new ToShortIntFunctionFactory());
        }

        if (toByteRefs > 0) {
            functions.add(new ToByteIntFunctionFactory());
        }

        addExtraFunctions();

        FunctionParser functionParser = new FunctionParser(configuration, functions);
        return new Invocation(
                parseFunction(expression1, metadata, functionParser),
                parseFunction(expression2, metadata, functionParser),
                new TestRecord(args));
    }

    private int getArgType(Object arg) {
        if (arg == null) {
            return ColumnType.STRING;
        }

        if (arg instanceof CharSequence) {
            return ColumnType.STRING;
        }

        if (arg instanceof Integer) {
            return ColumnType.INT;
        }

        if (arg instanceof Double) {
            return ColumnType.DOUBLE;
        }

        if (arg instanceof Long) {
            return ColumnType.LONG;
        }

        if (arg instanceof Float) {
            return ColumnType.FLOAT;
        }

        if (arg instanceof Character) {
            return ColumnType.CHAR;
        }

        Assert.fail("Unsupported type: " + arg.getClass());
        return -1;
    }

    private FunctionFactory getFactory0() {
        if (factory == null) {
            factory = getFunctionFactory();
        }
        return factory;
    }

    protected abstract FunctionFactory getFunctionFactory();

    private boolean isNegative(int argType, Object arg) {
        switch (argType) {
            case ColumnType.INT:
                return (int) arg < 0 && (int) arg != Numbers.INT_NaN;
            case ColumnType.LONG:
                return (long) arg < 0 && (long) arg != Numbers.LONG_NaN;
            case ColumnType.SHORT:
            case ColumnType.BYTE:
                // byte is passed as int
                // short is passed as int
                return (int) arg < 0;
            case ColumnType.DOUBLE:
                // double can be auto-overloaded, e.g. lesser types passed
                // into this method. Even though method accepts double we could
                // have byte, short, int, long, float, timestamp and date
                if (arg instanceof Integer) {
                    return (Integer) arg < 0;
                }

                if (arg instanceof Long) {
                    return (Long) arg < 0;
                }
                return (double) arg < 0;
            case ColumnType.FLOAT:
                return (float) arg < 0;
            default:
                return false;
        }
    }

    private void printArgument(CharSequence signature,
                               int signatureTypeOffset,
                               boolean forceConstant,
                               GenericRecordMetadata metadata,
                               boolean b,
                               boolean constVarArg,
                               StringSink expression1,
                               StringSink expression2,
                               int i,
                               Object arg) {
        final String columnName = "f" + i;
        final boolean constantArg;
        final int argType;


        if (b) {
            final char typeChar = signature.charAt(signatureTypeOffset + i + 1);
            constantArg = Character.isLowerCase(typeChar);
            argType = FunctionParser.getArgType(typeChar);
        } else {
            constantArg = constVarArg;
            argType = getArgType(arg);
        }

        metadata.add(new TableColumnMetadata(columnName, argType));

        if (constantArg || forceConstant) {
            printConstant(argType, expression1, arg);
            printConstant(argType, expression2, arg);
        } else {
            expression1.put(columnName);
            if (argType == ColumnType.SYMBOL || argType == ColumnType.BINARY || isNegative(argType, arg)) {
                // above types cannot be expressed as constant in SQL
                expression2.put(columnName);
            } else {
                printConstant(argType, expression2, arg);
            }
        }
    }

    private void printConstant(int type, StringSink sink, Object value) {
        switch (type) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                if (value == null) {
                    sink.put("null");
                } else {
                    sink.put('\'');
                    sink.put((CharSequence) value);
                    sink.put('\'');
                }
                break;
            case ColumnType.INT:
                sink.put((Integer) value);
                break;
            case ColumnType.BOOLEAN:
                sink.put((Boolean) value);
                break;
            case ColumnType.DOUBLE:
                if (value instanceof Integer) {
                    sink.put((Integer) value);
                } else if (value instanceof Long) {
                    sink.put((Long) value);
                } else {
                    sink.put((Double) value, 5);
                }
                break;
            case ColumnType.FLOAT:
                sink.put((Float) value, 5);
                break;
            case ColumnType.LONG:
                sink.put((Long) value);
                break;
            case ColumnType.DATE:
                sink.put("to_date(").put((Long) value).put(")");
                toDateRefs++;
                break;
            case ColumnType.TIMESTAMP:
                sink.put("to_timestamp(").put((Long) value).put(")");
                toTimestampRefs++;
                break;
            case ColumnType.SHORT:
                sink.put("to_short(").put((Integer) value).put(")");
                toShortRefs++;
                break;
            case ColumnType.CHAR:
                sink.put('\'').put((char) value).put('\'');
                break;
            default:
                // byte
                sink.put("to_byte(").put((Integer) value).put(")");
                toByteRefs++;
                break;
        }
    }

    public static class Invocation {
        private final Function function1;
        private final Function function2;
        private final Record record;

        public Invocation(Function function1, Function function2, Record record) {
            this.function1 = function1;
            this.function2 = function2;
            this.record = record;
        }

        public void andAssert(boolean expected) {
            Assert.assertEquals(expected, function1.getBool(record));
            Assert.assertEquals(expected, function2.getBool(record));
        }

        public void andAssert(CharSequence expected) {
            if (function1.getType() == ColumnType.STRING) {
                assertString(function1, expected);
                assertString(function2, expected);
            }
            closeFunctions();
        }

        public void andAssert(int expected) {
            Assert.assertEquals(expected, function1.getInt(record));
            Assert.assertEquals(expected, function2.getInt(record));
            closeFunctions();
        }

        public void andAssert(byte expected) {
            Assert.assertEquals(expected, function1.getByte(record));
            Assert.assertEquals(expected, function2.getByte(record));
            closeFunctions();
        }

        public void andAssert(short expected) {
            Assert.assertEquals(expected, function1.getShort(record));
            Assert.assertEquals(expected, function2.getShort(record));
            closeFunctions();
        }

        public void andAssert(long expected) {
            Assert.assertEquals(expected, function1.getLong(record));
            Assert.assertEquals(expected, function2.getLong(record));
            closeFunctions();
        }

        public void andAssert(double expected, double delta) {
            Assert.assertEquals(expected, function1.getDouble(record), delta);
            Assert.assertEquals(expected, function2.getDouble(record), delta);
            closeFunctions();
        }

        public void andAssertDate(long expected) {
            Assert.assertEquals(expected, function1.getDate(record));
            Assert.assertEquals(expected, function2.getDate(record));
            closeFunctions();
        }

        public void andAssertOnlyColumnValues(boolean expected) {
            Assert.assertEquals(expected, function2.getBool(record));
        }

        public void andAssertTimestamp(long expected) {
            Assert.assertEquals(expected, function1.getTimestamp(record));
            Assert.assertEquals(expected, function2.getTimestamp(record));
            closeFunctions();
        }

        public Function getFunction1() {
            return function1;
        }

        public Function getFunction2() {
            return function2;
        }

        public Record getRecord() {
            return record;
        }

        private void assertString(Function func, CharSequence expected) {
            if (expected == null) {
                Assert.assertNull(func.getStr(record));
                Assert.assertNull(func.getStrB(record));
                Assert.assertEquals(-1, func.getStrLen(record));
                sink.clear();
                func.getStr(record, sink);
                Assert.assertEquals(0, sink.length());
            } else {
                CharSequence a = func.getStr(record);
                CharSequence b = func.getStrB(record);
                if (!func.isConstant() && (!(a instanceof String) || !(b instanceof String))) {
                    Assert.assertNotSame(a, b);
                }
                TestUtils.assertEquals(expected, a);
                TestUtils.assertEquals(expected, b);

                // repeat call to make sure there is correct object reuse
                TestUtils.assertEquals(expected, func.getStr(record));
                TestUtils.assertEquals(expected, func.getStrB(record));

                sink.clear();
                func.getStr(record, sink);
                TestUtils.assertEquals(expected, sink);
                Assert.assertEquals(expected.length(), func.getStrLen(record));
            }
        }

        private void closeFunctions() {
            function1.close();
            function2.close();
        }
    }

    private static class TestRecord implements Record {
        private final Object[] args;
        private final TestBinarySequence byteSequence = new TestBinarySequence();

        public TestRecord(Object[] args) {
            this.args = args;
        }

        @Override
        public BinarySequence getBin(int col) {
            Object o = args[col];
            if (o == null) {
                return null;
            }
            return byteSequence.of((byte[]) o);
        }

        @Override
        public boolean getBool(int col) {
            return (boolean) args[col];
        }

        @Override
        public byte getByte(int col) {
            return (byte) (int) args[col];
        }

        @Override
        public double getDouble(int col) {
            Object value = args[col];
            if (value instanceof Integer) {
                return ((Integer) value).doubleValue();
            }

            if (value instanceof Long) {
                return ((Long) value).doubleValue();
            }

            return (double) args[col];
        }

        @Override
        public float getFloat(int col) {
            return (float) args[col];
        }

        @Override
        public int getInt(int col) {
            return (int) args[col];
        }

        @Override
        public long getLong(int col) {
            return (long) args[col];
        }

        @Override
        public short getShort(int col) {
            return (short) (int) args[col];
        }

        @Override
        public char getChar(int col) {
            return (char) args[col];
        }

        @Override
        public CharSequence getStr(int col) {
            return (CharSequence) args[col];
        }

        @Override
        public CharSequence getStrB(int col) {
            return (CharSequence) args[col];
        }

        @Override
        public int getStrLen(int col) {
            final Object o = args[col];
            return o != null ? ((CharSequence) o).length() : -1;
        }

        @Override
        public CharSequence getSym(int col) {
            return (CharSequence) args[col];
        }
    }
}