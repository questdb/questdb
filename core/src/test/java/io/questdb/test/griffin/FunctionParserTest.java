/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.array.ArrayCreateFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.CursorDereferenceFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.date.SysdateFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToStrDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToStrTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDateFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLong256GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.MinTimestampGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.math.AbsShortFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegShortFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubIntFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToCharBinFunctionFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;

import static io.questdb.cairo.ColumnType.OVERLOAD_NONE;
import static org.junit.Assert.*;

public class FunctionParserTest extends BaseFunctionFactoryTest {

    @Test
    public void overloadFromCharToDoubleDoesNotExist() {
        assertEquals(7, ColumnType.overloadDistance(ColumnType.CHAR, ColumnType.DOUBLE));
    }

    @Test
    public void overloadFromShortToIntIsLikelyThanToDouble() {
        assertTrue(ColumnType.overloadDistance(ColumnType.SHORT, ColumnType.INT) <
                ColumnType.overloadDistance(ColumnType.SHORT, ColumnType.DOUBLE));
    }

    @Test
    public void overloadToUndefinedDoesNotExist() {
        boolean assertsEnabled = false;
        //noinspection AssertWithSideEffects
        assert assertsEnabled = true;

        // test asserts that an assert statement in production code will fail
        Assume.assumeTrue(assertsEnabled);

        try {
            ColumnType.overloadDistance(ColumnType.INT, ColumnType.UNDEFINED);
            fail();
        } catch (AssertionError e) {
            TestUtils.assertContains(e.getMessage(), "Undefined not supported in overloads");
        }
    }

    @Test
    public void testAmbiguousFunctionInvocation() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(DD)"; // double,double
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return new DoubleFunction() {
                    @Override
                    public double getDouble(Record rec) {
                        return 123.123;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }

                };
            }
        });
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(FF)"; // float,float
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return new FloatFunction() {
                    @Override
                    public float getFloat(Record rec) {
                        return 123.123f;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BYTE));
        metadata.add(new TableColumnMetadata("c", ColumnType.SHORT));
        FunctionParser functionParser = createFunctionParser();
        Function f = parseFunction("a + c", metadata, functionParser);
        assertEquals(123.123f, f.getFloat(null), 0.0001);
    }

    @Test
    public void testArrayFunctionInvalid() {
        functions.add(new ArrayCreateFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        FunctionParser functionParser = createFunctionParser();
        try (Function f = parseFunction("ARRAY[[1.0], 2]", metadata, functionParser)) {
            f.getArray(null);
            fail();
        } catch (SqlException e) {
            assertEquals("[13] mixed array and non-array elements", e.getMessage());
        }
        try (Function f = parseFunction("ARRAY[1.0, [2.0]]", metadata, functionParser)) {
            f.getArray(null);
            fail();
        } catch (SqlException e) {
            assertEquals("[11] mixed array and non-array elements", e.getMessage());
        }
        try (Function f = parseFunction("ARRAY[[1.0], [[2.0]]]", metadata, functionParser)) {
            f.getArray(null);
            fail();
        } catch (SqlException e) {
            assertEquals("[13] sub-arrays don't match in number of dimensions", e.getMessage());
        }
    }

    @Test
    public void testArrayFunctionWithLiterals() throws SqlException {
        functions.add(new ArrayCreateFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        FunctionParser functionParser = createFunctionParser();
        //TODO: Add more types as we support them
        try (Function f = parseFunction("ARRAY[1.0, 2]", metadata, functionParser)) {
            ArrayView array = f.getArray(null);
            assertEquals(ColumnType.DOUBLE, array.getElemType());
            assertEquals(2, array.getFlatViewLength());
            assertEquals(1, array.getDimCount());
            assertEquals(1.0, array.getDouble(0), 0.0001);
            assertEquals(2.0, array.getDouble(1), 0.0001);
        }
        try (Function f = parseFunction("ARRAY[[1.0], [2.0]]", metadata, functionParser)) {
            ArrayView array = f.getArray(null);
            assertEquals(ColumnType.DOUBLE, array.getElemType());
            assertEquals(2, array.getDimLen(0));
            assertEquals(1, array.getDimLen(1));
            assertEquals(2, array.getFlatViewLength());
            assertEquals(1.0, array.getDouble(0), 0.0001);
            assertEquals(2.0, array.getDouble(1), 0.0001);
        }
    }

    @Test
    public void testArrayFunctionWithRecord() throws SqlException {
        functions.add(new ArrayCreateFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final Record record = new Record() {
            @Override
            public double getDouble(int col) {
                return col + 1;
            }
        };

        metadata.add(new TableColumnMetadata("a", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("b", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("c", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("d", ColumnType.DOUBLE));
        FunctionParser functionParser = createFunctionParser();
        try (Function f = parseFunction("ARRAY[a]", metadata, functionParser)) {
            ArrayView array = f.getArray(record);
            assertEquals(1, array.getDimCount());
            assertEquals(1, array.getFlatViewLength());
            assertEquals(1.0, array.getDouble(0), 1e-11);
        }
        try (Function f = parseFunction("ARRAY[a, b]", metadata, functionParser)) {
            ArrayView array = f.getArray(record);
            assertEquals(2, array.getFlatViewLength());
            assertEquals(1, array.getDimCount());
            assertEquals(1.0, array.getDouble(0), 1e-11);
            assertEquals(2.0, array.getDouble(1), 1e-11);
        }
        try (Function f = parseFunction("ARRAY[[a]]", metadata, functionParser)) {
            ArrayView array = f.getArray(record);
            assertEquals(2, array.getDimCount());
            assertEquals(1, array.getDimLen(0));
            assertEquals(1, array.getDimLen(1));
            assertEquals(1, array.getFlatViewLength());
            assertEquals(1.0, array.getDouble(0), 1e-11);
        }
        try (Function f = parseFunction("ARRAY[[a], [b]]", metadata, functionParser)) {
            ArrayView array = f.getArray(record);
            assertEquals(2, array.getDimLen(0));
            assertEquals(1, array.getDimLen(1));
            assertEquals(2, array.getFlatViewLength());
            assertEquals(1.0, array.getDouble(0), 1e-11);
            assertEquals(2.0, array.getDouble(1), 1e-11);
        }
        try (Function f = parseFunction("ARRAY[[a, b], [c, d]]", metadata, functionParser)) {
            ArrayView array = f.getArray(record);
            assertEquals(2, array.getDimLen(0));
            assertEquals(2, array.getDimLen(1));
            assertEquals(4, array.getFlatViewLength());
            assertEquals(1.0, array.getDouble(0), 1e-11);
            assertEquals(2.0, array.getDouble(1), 1e-11);
            assertEquals(3.0, array.getDouble(2), 1e-11);
            assertEquals(4.0, array.getDouble(3), 1e-11);
        }
    }

    @Test
    public void testBooleanConstants() throws SqlException {
        functions.add(new NotFunctionFactory());
        functions.add(new OrFunctionFactory());

        final Record record = new Record() {
            @Override
            public boolean getBool(int col) {
                // col0 = false
                return false;
            }
        };

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a or not false", metadata, functionParser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertTrue(function.getBool(record));

        Function function2 = parseFunction("a or true", metadata, functionParser);
        assertTrue(function2.getBool(record));
    }

    @Test
    public void testBooleanFunctions() throws SqlException {

        functions.add(new NotFunctionFactory());
        functions.add(new OrFunctionFactory());

        final Record record = new Record() {
            @Override
            public boolean getBool(int col) {
                // col0 = false
                // col1 = true
                return col != 0;
            }
        };

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("b", ColumnType.BOOLEAN));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a or not b", metadata, functionParser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertFalse(function.getBool(record));
    }

    @Test
    public void testByteAndLongToDoubleCast() throws SqlException {
        assertCastToDouble(363, ColumnType.BYTE, ColumnType.LONG, new Record() {

            @Override
            public byte getByte(int col) {
                return 18;
            }

            @Override
            public long getLong(int col) {
                return 345;
            }
        });
    }

    @Test
    public void testByteAndShortToFloatCast() throws SqlException {
        assertCastToFloat(new Record() {
            @Override
            public byte getByte(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testByteAndShortToIntCast() throws SqlException {
        functions.add(new AddIntFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BYTE));
        metadata.add(new TableColumnMetadata("b", ColumnType.SHORT));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        assertEquals(ColumnType.INT, function.getType());
        assertEquals(33, function.getInt(new Record() {
            @Override
            public byte getByte(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        }));
    }

    @Test
    public void testByteToDoubleCast() throws SqlException {
        assertCastToDouble(131, ColumnType.BYTE, ColumnType.BYTE, new Record() {
            @Override
            public byte getByte(int col) {
                if (col == 0) {
                    return 41;
                }
                return 90;
            }
        });
    }

    @Test
    public void testByteToLongCast() throws SqlException {
        assertCastToLong(131, ColumnType.BYTE, ColumnType.BYTE, new Record() {
            @Override
            public byte getByte(int col) {
                if (col == 0) {
                    return 41;
                }
                return 90;
            }
        });
    }

    @Test
    public void testByteToShortCast() throws SqlException {
        functions.add(new NegShortFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BYTE));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("-a", metadata, functionParser);
        assertEquals(ColumnType.SHORT, function.getType());
        assertEquals(-90, function.getShort(new Record() {
            @Override
            public byte getByte(int col) {
                return 90;
            }
        }));
    }

    @Test
    public void testConstGeoHashFunctionConvertedByte() throws SqlException {
        int type = ColumnType.getGeoHashTypeWithBits(5);
        assertGeoConstFunctionTypeAndValue(
                dummyGeoHashFunctionFactory(type, 1),
                "io.questdb.griffin.engine.functions.constants.GeoByteConstant",
                type,
                1
        );
    }

    @Test
    public void testConstGeoHashFunctionConvertedInt() throws SqlException {
        int type = ColumnType.getGeoHashTypeWithBits(22);
        assertGeoConstFunctionTypeAndValue(
                dummyGeoHashFunctionFactory(type, 3),
                "io.questdb.griffin.engine.functions.constants.GeoIntConstant",
                type,
                3
        );
    }

    @Test
    public void testConstGeoHashFunctionConvertedLong() throws SqlException {
        int type = ColumnType.getGeoHashTypeWithBits(43);
        assertGeoConstFunctionTypeAndValue(
                dummyGeoHashFunctionFactory(type, 4),
                "io.questdb.griffin.engine.functions.constants.GeoLongConstant",
                type,
                4
        );
    }

    @Test
    public void testConstGeoHashFunctionConvertedShort() throws SqlException {
        int type = ColumnType.getGeoHashTypeWithBits(10);
        assertGeoConstFunctionTypeAndValue(
                dummyGeoHashFunctionFactory(type, 2),
                "io.questdb.griffin.engine.functions.constants.GeoShortConstant",
                type,
                2
        );
    }

    @Test
    public void testConstStrToDateCast() throws SqlException {
        functions.add(new EqDateFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.DATE));

        FunctionParser parser = createFunctionParser();
        Function function = parseFunction("a='2020-01-01'", metadata, parser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertTrue(function.getBool(new Record() {
            @Override
            public long getDate(int col) {
                return 1577836800000L;
            }
        }));

        function = parseFunction("'2020-01-01'=a", metadata, parser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertTrue(function.getBool(new Record() {
            @Override
            public long getDate(int col) {
                return 1577836800000L;
            }
        }));
    }

    @Test
    public void testConstVarArgFunction() throws SqlException {
        functions.add(new InStrFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a in ('xu', 'yk')", metadata, functionParser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertTrue(function.getBool(new Record() {
            @Override
            public CharSequence getStrA(int col) {
                return "yk";
            }
        }));
    }

    @Test
    public void testCountUpperCase() throws SqlException {
        functions.add(new CountGroupByFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("COUNT()", metadata, functionParser);
        assertEquals(ColumnType.LONG, function.getType());
        assertEquals(CountLongConstGroupByFunction.class, function.getClass());
    }

    @Test
    public void testExplicitConstantBoolean() throws SqlException {
        testConstantPassThru(BooleanConstant.TRUE);
    }

    @Test
    public void testExplicitConstantByte() throws SqlException {
        testConstantPassThru(new ByteConstant((byte) 200));
    }

    @Test
    public void testExplicitConstantDate() throws SqlException {
        testConstantPassThru(new DateConstant(123));
    }

    @Test
    public void testExplicitConstantDouble() throws SqlException {
        testConstantPassThru(new DoubleConstant(200));
    }

    @Test
    public void testExplicitConstantFloat() throws SqlException {
        testConstantPassThru(new FloatConstant(200));
    }

    @Test
    public void testExplicitConstantGeoHash() throws SqlException, NumericException {
        int bits = 6 * 5;
        int hash = (int) GeoHashes.fromCoordinatesDeg(39.9830487269087, 0.02405432769681642, bits);
        testConstantPassThru(new GeoIntConstant(hash, ColumnType.getGeoHashTypeWithBits(bits)));
        functions.clear();
        sink.clear();
        GeoHashes.appendChars(hash, 6, sink);
    }

    @Test
    public void testExplicitConstantInt() throws SqlException {
        testConstantPassThru(new IntConstant(200));
    }

    @Test
    public void testExplicitConstantLong() throws SqlException {
        testConstantPassThru(new LongConstant(200));
    }

    @Test
    public void testExplicitConstantLong256() throws SqlException {
        CharSequence tok = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        testConstantPassThru(new Long256Constant(Numbers.parseLong256(tok, new Long256Impl())));
    }

    @Test
    public void testExplicitConstantNull() throws SqlException {
        testConstantPassThru(NullConstant.NULL);
    }

    @Test
    public void testExplicitConstantShort() throws SqlException {
        testConstantPassThru(new ShortConstant((short) 200));
    }

    @Test
    public void testExplicitConstantStr() throws SqlException {
        testConstantPassThru(new StrConstant("abc"));
    }

    @Test
    public void testExplicitConstantTimestamp() throws SqlException {
        testConstantPassThru(new TimestampConstant(123, ColumnType.TIMESTAMP_MICRO));
    }

    @Test
    public void testFloatAndLongToDoubleCast() throws SqlException {
        assertCastToDouble(468.3, ColumnType.FLOAT, ColumnType.LONG, new Record() {
            @Override
            public float getFloat(int col) {
                return 123.3f;
            }

            @Override
            public long getLong(int col) {
                return 345;
            }
        });
    }

    @Test
    public void testFunctionDoesNotExist() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("c", ColumnType.SYMBOL, false, 0, false, null));
        assertFail(5, "unknown function name: xyz(BOOLEAN,SYMBOL)", "a or xyz(a,c)", metadata);
    }

    @Test
    public void testFunctionFactoryException() {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                throw new RuntimeException("oops");
            }
        });
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        assertFail(0, "exception in function factory", "x()", metadata);
    }

    @Test
    public void testFunctionFactoryNullFunction() {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return null;
            }
        });
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        assertFail(0, "bad function factory (NULL), check log", "x()", metadata);
    }

    @Test
    public void testFunctionFactoryNullSignature() {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return null;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return new IntConstant(0);
            }
        });
        FunctionParser parser = createFunctionParser();
        assertEquals(0, parser.getFunctionFactoryCache().getFunctionCount());
    }

    @Test
    public void testFunctionOverload() throws SqlException {
        functions.add(new ToStrDateFunctionFactory());
        functions.add(new ToStrTimestampFunctionFactory());
        functions.add(new ToCharBinFunctionFactory());

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.DATE));
        metadata.add(new TableColumnMetadata("b", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("c", ColumnType.BINARY));

        FunctionParser functionParser = createFunctionParser();
        Record record = new TestRecord();

        Function function = parseFunction("to_str(a, 'EE, dd-MMM-yyyy hh:mm:ss')", metadata, functionParser);
        assertEquals(ColumnType.STRING, function.getType());
        TestUtils.assertEquals("Thursday, 03-Apr-150577 02:54:03", function.getStrA(record));

        Function function2 = parseFunction("to_str(b, 'EE, dd-MMM-yyyy hh:mm:ss')", metadata, functionParser);
        assertEquals(ColumnType.STRING, function2.getType());
        TestUtils.assertEquals("Tuesday, 21-Nov-2119 07:50:58", function2.getStrA(record));

        Function function3 = parseFunction("to_char(c)", metadata, functionParser);

        String expectedBin = "00000000 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4 91 3b\n" +
                "00000010 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68 61 26\n" +
                "00000020 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e 38\n" +
                "00000030 8d 1b 9e f4 c8 39 09 fe d8 9d 30 78 36 6a 32 de\n" +
                "00000040 e4 7c d2 35 07 42 fc 31 79 5f 8b 81 2b 93 4d 1a\n" +
                "00000050 8e 78 b5 b9 11 53 d0 fb 64 bb 1a d4 f0 2d 40 e2\n" +
                "00000060 4b b1 3e e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82\n" +
                "00000070 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64 0e\n" +
                "00000080 2c 7f d7 6f b8 c9 ae 28 c7 84 47 dc d2 85 7f a5\n" +
                "00000090 b8 7b 4a 9d 46 7c 8d dd 93 e6 d0 b3 2b 07 98 cc\n" +
                "000000a0 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac a8 3b\n" +
                "000000b0 a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a e7 0c 89\n" +
                "000000c0 14 58 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9\n" +
                "000000d0 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34 04 23 8d\n" +
                "000000e0 d8 57 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb 67\n" +
                "000000f0 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\n" +
                "00000100 0b 92 58 7d 24 bc 2e 60 6a 1c 0b 20 a2 86 89 37\n" +
                "00000110 11 2c 14 0c 2d 20 84 52 d9 6f 04 ab 27 47 8f 23\n" +
                "00000120 3f ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d b3\n" +
                "00000130 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c\n" +
                "00000140 65 ff 27 67 77 12 54 52 d0 29 26 c5 aa da 18 ce\n" +
                "00000150 5f b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3 14\n" +
                "00000160 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                "00000170 61 52 8b 0b 93 e5 57 a5 db a1 76 1c 1c 26 fb 2e\n" +
                "00000180 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a ef\n" +
                "00000190 88 cb 4b a1 cf cf 41 7d a6 d1 3e b4 48 d4 41 9d\n" +
                "000001a0 fb 49 40 44 49 96 cf 2b b3 71 a7 d5 af 11 96 37\n" +
                "000001b0 08 dd 98 ef 54 88 2a a2 ad e7 d4 62 e1 4e d6 b2\n" +
                "000001c0 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44\n" +
                "000001d0 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13 5d b2 15\n" +
                "000001e0 e7 b8 35 67 9c 94 b9 8e 28 b6 a9 17 ec 0e 01 c4\n" +
                "000001f0 eb 9f 13 8f bb 2a 4b af 8f 89 df 35 8f da fe 33\n" +
                "00000200 98 80 85 20 53 3b 51 9d 5d 28 ac 02 2e fe 05 3b\n" +
                "00000210 94 5f ec d3 dc f8 43 b2 e3 75 62 60 af 6d 8c d8\n" +
                "00000220 ac c8 46 3b 47 3c e1 72 3b 9d ef c4 4a c9 cf fb\n" +
                "00000230 9d 63 ca 94 00 6b dd 18 fe 71 76 bc 45 24 cd 13\n" +
                "00000240 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35 15 29 83\n" +
                "00000250 1f c3 2f ed b0 ba 08 e0 2c ee 41 de b6 81 df b7\n" +
                "00000260 6c 4b fb 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4\n" +
                "00000270 e9 19 47 8d ad 11 bc fe b9 52 dd 4d f3 f9 76 f6\n" +
                "00000280 85 ab a3 ab ee 6d 54 75 10 b3 4c 0e 8f f1 0c c5\n" +
                "00000290 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1 00\n" +
                "000002a0 f8 42 23 37 03 a1 8c 47 64 59 1a d4 ab be 30 fa\n" +
                "000002b0 8d ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6 2c\n" +
                "000002c0 23 45 a3 76 60 15 c1 8c d9 11 69 94 3f 7d ef 3b\n" +
                "000002d0 b8 be f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0\n" +
                "000002e0 35 d8 ab 3f a1 f5 4b ea 01 c9 63 b4 fc 92 60 1f\n" +
                "000002f0 df 41 ec 2c 38 88 88 e7 59 40 10 20 81 c6 3d bc\n" +
                "00000300 b5 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79\n" +
                "00000310 87 fc 92 83 fc 88 f3 32 27 70 c8 01 b0 dc c9 3a\n" +
                "00000320 5b 7e 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6\n" +
                "00000330 78 09 1c 5d 88 f5 52 fd 36 02 50 d9 a0 b5 90 6c\n" +
                "00000340 9c 23 22 89 99 ad f7 fe 9a 9e 1b fd a9 d7 0e 39\n" +
                "00000350 5a 28 ed 97 99 d8 77 33 3f b2 67 da 98 47 47 bf\n" +
                "00000360 4f ea 5f 48 ed f6 bb 28 a2 3c d0 65 5e b7 95 2e\n" +
                "00000370 4a af c6 d0 19 6a de 46 04 d3 81 e7 a2 16 22 35\n" +
                "00000380 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19 a1 ec\n" +
                "00000390 74 f8 10 fc 6e 23 3d e0 2d 04 86 e7 ca 29 98 07\n" +
                "000003a0 69 ca 5b d6 cf 09 69 01 b1 55 38 ad b2 4a 4e 7d\n" +
                "000003b0 85 f9 39 25 42 67 78 47 b3 80 69 b9 14 d6 fc ee\n" +
                "000003c0 03 22 81 b8 06 c4 06 af 38 71 1f e1 e4 91 7d e9\n" +
                "000003d0 5d 4b 6a cd 4e f9 17 9e cf 6a 34 2c 37 a3 6f 2a\n" +
                "000003e0 12 61 3a 9a ad 98 2e 75 52 ad 62 87 88 45 b9 9d\n" +
                "000003f0 20 13 51 c0 e0 b7 a4 24 40 4d 50 b1 8c 4d 66 e8";

        TestUtils.assertEquals(expectedBin, function3.getStrA(record));
    }

    @Test
    public void testGeoHashFunction() throws SqlException {
        functions.add(new CastStrToGeoHashFunctionFactory());

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("gh", ColumnType.getGeoHashTypeWithBits(25)));

        FunctionParser functionParser = createFunctionParser();
        Record record = new Record() {
            @Override
            public int getGeoInt(int col) {
                return (int) getLong(col);
            }

            @Override
            public long getLong(int col) {
                return 847187636514L;
            }
        };

        Function function = parseFunction("cast('sp052w92' as geohash(5c))", metadata, functionParser);
        assertEquals(ColumnType.getGeoHashTypeWithBits(25), function.getType());
        assertEquals(25854114, function.getGeoInt(record));
    }

    @Test
    public void testImplicitConstantBin() throws SqlException {
        BinFunction function = new BinFunction() {
            @Override
            public BinarySequence getBin(Record rec) {
                return null;
            }

            @Override
            public long getBinLen(Record rec) {
                return -1;
            }

            @Override
            public boolean isConstant() {
                return true;
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        };

        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return function;
            }
        });

        assertSame(function, parseFunction("x()", new GenericRecordMetadata(), createFunctionParser()));
        assertTrue(function.isConstant());
    }

    @Test
    public void testImplicitConstantBoolean() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new BooleanFunction() {
                    @Override
                    public boolean getBool(Record rec) {
                        return false;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof BooleanConstant);
    }

    @Test
    public void testImplicitConstantByte() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new ByteFunction() {
                    @Override
                    public byte getByte(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof ByteConstant);
    }

    @Test
    public void testImplicitConstantDate() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new DateFunction() {
                    @Override
                    public long getDate(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof DateConstant);
    }

    @Test
    public void testImplicitConstantDouble() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new DoubleFunction() {
                    @Override
                    public double getDouble(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof DoubleConstant);
    }

    @Test
    public void testImplicitConstantFloat() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new FloatFunction() {
                    @Override
                    public float getFloat(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof FloatConstant);
    }

    @Test
    public void testImplicitConstantInt() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new IntFunction() {
                    @Override
                    public int getInt(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof IntConstant);
    }

    @Test
    public void testImplicitConstantLong() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new LongFunction() {
                    @Override
                    public long getLong(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof LongConstant);
    }

    @Test
    public void testImplicitConstantNull() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new StrFunction() {
                    @Override
                    public CharSequence getStrA(Record rec) {
                        return null;
                    }

                    @Override
                    public CharSequence getStrB(Record rec) {
                        return null;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertSame(StrConstant.NULL, function);
    }

    @Test
    public void testImplicitConstantNullSymbol() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new SymbolConstant(null, SymbolTable.VALUE_IS_NULL);
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof SymbolConstant);
    }

    @Test
    public void testImplicitConstantShort() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new ShortFunction() {
                    @Override
                    public short getShort(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof ShortConstant);
    }

    @Test
    public void testImplicitConstantStr() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new StrFunction() {
                    private final String x = "abc";

                    @Override
                    public CharSequence getStrA(Record rec) {
                        return x;
                    }

                    @Override
                    public CharSequence getStrB(Record rec) {
                        return x;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof StrConstant);
    }

    @Test
    public void testImplicitConstantSymbol() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new SymbolConstant("xyz", 0);
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof SymbolConstant);
    }

    @Test
    public void testImplicitConstantTimestamp() throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration1, SqlExecutionContext sqlExecutionContext) {
                return new TimestampFunction(ColumnType.TIMESTAMP_MICRO) {
                    @Override
                    public long getTimestamp(Record rec) {
                        return 0;
                    }

                    @Override
                    public boolean isConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });

        Function function = parseFunction("x()", new GenericRecordMetadata(), createFunctionParser());
        assertTrue(function instanceof TimestampConstant);
    }

    @Test
    public void testIntAndShortToDoubleCast() throws SqlException {
        assertCastToDouble(33, ColumnType.INT, ColumnType.SHORT, new Record() {
            @Override
            public int getInt(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testIntAndShortToLongCast() throws SqlException {
        assertCastToLong(33, ColumnType.INT, ColumnType.SHORT, new Record() {
            @Override
            public int getInt(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testInvalidColumn() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("c", ColumnType.SHORT));
        assertFail(4, "Invalid column: d", "a + d", metadata);
    }

    @Test
    public void testInvalidConstant() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("c", ColumnType.SYMBOL, false, 0, true, null));
        assertFail(4, "invalid constant: 1c", "a + 1c", metadata);
    }

    @Test
    public void testNoArgFunction() throws SqlException {
        functions.add(new SysdateFunctionFactory());
        functions.add(new ToStrDateFunctionFactory());
        FunctionParser functionParser = new FunctionParser(
                new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull MillisecondClock getMillisecondClock() {
                        return () -> {
                            try {
                                return DateFormatUtils.parseUTCDate("2018-03-04T21:40:00.000Z");
                            } catch (NumericException e) {
                                fail();
                            }
                            return 0;
                        };
                    }
                },
                new FunctionFactoryCache(configuration, functions));

        Function function = parseFunction("to_str(sysdate(), 'EE, dd-MMM-yyyy HH:mm:ss')",
                new GenericRecordMetadata(),
                functionParser
        );
        TestUtils.assertEquals("Sunday, 04-Mar-2018 21:40:00", function.getStrA(null));
    }

    @Test
    public void testNoArgFunctionDoesNotExist() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        assertFail(5, "unknown function name", "a or xyz()", metadata);
    }

    @Test
    public void testNoArgFunctionWrongSignature() {
        functions.add(new SysdateFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        assertFail(7, "wrong number of arguments for function `sysdate`; expected: 0, provided: 1", "a or   sysdate(a)", metadata);
    }

    @Test
    public void testOverloadBetweenNullAndAnyType() {
        for (short type = ColumnType.BOOLEAN; type < ColumnType.NULL; type++) {
            String msg = "type: " + ColumnType.nameOf(type) + "(" + type + ")";
            if (type == ColumnType.STRING || type == ColumnType.SYMBOL) {
                assertEquals(msg, -1, ColumnType.overloadDistance(ColumnType.NULL, type));
            } else {
                assertEquals(msg, 0, ColumnType.overloadDistance(ColumnType.NULL, type));
            }
            assertEquals(msg, OVERLOAD_NONE, ColumnType.overloadDistance(type, ColumnType.NULL));
        }
    }

    @Test
    public void testPassColumnToConstVarArgFunction() {
        functions.add(new InStrFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("b", ColumnType.STRING));
        assertFail(6, "constant expected", "a in (b, 'y')", metadata);
    }

    @Test
    public void testPassNaNAsShort() {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x(E)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return null;
            }
        });

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        try {
            parseFunction("x(NaN)", metadata, createFunctionParser());
            fail();
        } catch (SqlException e) {
            assertEquals(0, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "bad function factory (NULL), check log");
        }
    }

    @Test
    public void testPassVarToConstArg() {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x(i)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return null;
            }
        });

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.INT));
        try {
            parseFunction("x(a)", metadata, createFunctionParser());
            fail();
        } catch (SqlException e) {
            assertEquals(2, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "argument type mismatch for function `x` at #1 expected: INT constant, actual: INT");
        }
    }

    @Test
    public void testSignatureBeginsWithDigit() throws SqlException {
        assertSignatureFailure("1x()");
    }

    @Test
    public void testSignatureEmptyFunctionName() throws SqlException {
        assertSignatureFailure("(B)");
    }

    @Test
    public void testSignatureIllegalArgumentType() throws SqlException {
        assertSignatureFailure("x(By)");
    }

    @Test
    public void testSignatureIllegalCharacter() throws SqlException {
        assertSignatureFailure("x'x()");
    }

    @Test
    public void testSignatureIllegalName1() throws SqlException {
        assertSignatureFailure("/*()");
    }

    @Test
    public void testSignatureIllegalName2() throws SqlException {
        assertSignatureFailure("--()");
    }

    @Test
    public void testSignatureMissingCloseBrace() throws SqlException {
        assertSignatureFailure("a(");
    }

    @Test
    public void testSignatureMissingOpenBrace() throws SqlException {
        assertSignatureFailure("x");
    }

    @Test
    public void testSimpleFunction() throws SqlException {
        assertCastToDouble(13.1, ColumnType.DOUBLE, ColumnType.DOUBLE, new Record() {
            @Override
            public double getDouble(int col) {
                if (col == 0) {
                    return 5.3;
                }
                return 7.8;
            }
        });
    }

    @Test
    public void testSymbolFunction() throws SqlException {
        functions.add(new LengthStrFunctionFactory());
        functions.add(new LengthSymbolFunctionFactory());
        functions.add(new SubIntFunctionFactory());

        FunctionParser functionParser = createFunctionParser();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("b", ColumnType.SYMBOL, false, 0, false, null));

        final Function function = parseFunction("length(b) - length(a)",
                metadata,
                functionParser
        );

        String symbolValue = "EFGHT";

        Record record = new Record() {
            @Override
            public int getInt(int col) {
                return 0;
            }

            @Override
            public CharSequence getStrA(int col) {
                return "ABC";
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }

            @Override
            public CharSequence getSymA(int col) {
                return symbolValue;
            }
        };

        function.init(new SymbolTableSource() {
                          @Override
                          public SymbolTable getSymbolTable(int columnIndex) {
                              return new SymbolTable() {
                                  @Override
                                  public CharSequence valueBOf(int key) {
                                      return symbolValue;
                                  }

                                  @Override
                                  public CharSequence valueOf(int key) {
                                      return symbolValue;
                                  }
                              };
                          }

                          @Override
                          public SymbolTable newSymbolTable(int columnIndex) {
                              return getSymbolTable(columnIndex);
                          }
                      }
                , sqlExecutionContext);

        assertEquals(2, function.getInt(record));

        Function function1 = parseFunction("length(null)", metadata, functionParser);
        assertEquals(-1, function1.getInt(record));

        Function function2 = parseFunction("length(NULL)", metadata, functionParser);
        assertEquals(-1, function2.getInt(record));
    }

    @Test
    public void testTooFewArguments() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.SHORT));
        assertFail(2, "too few arguments for '+' [found=1,expected=2]", "a + ", metadata);
    }

    @Test
    public void testUndefinedBindVariableAmbiguouslyMatched() throws SqlException {
        bindVariableService.clear();
        functions.add(new EqIntFunctionFactory());
        functions.add(new EqDoubleFunctionFactory());
        functions.add(new EqLongFunctionFactory());
        try (Function f = parseFunction("$2 = $1", null, createFunctionParser())) {
            TestUtils.assertContains(f.getClass().getCanonicalName(), "io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory.Func");
        }
        assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(0).getType());
        assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(1).getType());
    }

    @Test
    public void testUndefinedBindVariableDefineBinary() throws SqlException {
        assertBindVariableTypes(
                "to_char($1)",
                new ToCharBinFunctionFactory(),
                "io.questdb.griffin.engine.functions.str.ToCharBinFunctionFactory.ToCharBinFunc",
                ColumnType.BINARY
        );
    }

    @Test
    public void testUndefinedBindVariableDefineBoolean() throws SqlException {
        assertBindVariableTypes(
                "not($1)",
                new NotFunctionFactory(),
                "io.questdb.griffin.engine.functions.bool.NotFunctionFactory.Func",
                ColumnType.BOOLEAN
        );
    }

    @Test
    public void testUndefinedBindVariableDefineCursor() {
        bindVariableService.clear();
        functions.add(new CursorDereferenceFunctionFactory());
        try (Function ignored = parseFunction("($1).n", null, createFunctionParser())) {
            fail();
        } catch (SqlException e) {
            assertEquals(1, e.getPosition());
        }
    }

    @Test
    public void testUndefinedBindVariableDefineDate() throws SqlException {
        assertBindVariableTypes(
                "min($1)",
                new MinDateGroupByFunctionFactory(),
                "io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunction",
                ColumnType.DATE
        );
    }

    @Test
    public void testUndefinedBindVariableDefineFloat() throws SqlException {
        assertBindVariableTypes(
                "min($1)",
                new MinFloatGroupByFunctionFactory(),
                "io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunction",
                ColumnType.FLOAT
        );
    }

    @Test
    public void testUndefinedBindVariableDefineInt() throws SqlException {
        assertBindVariableTypes(
                "$2 = $1",
                new EqIntFunctionFactory(),
                "io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory.Func",
                ColumnType.INT,
                ColumnType.INT
        );
    }

    @Test
    public void testUndefinedBindVariableDefineLong256() throws SqlException {
        assertBindVariableTypes(
                "count_distinct($1)",
                new CountDistinctLong256GroupByFunctionFactory(),
                "io.questdb.griffin.engine.functions.groupby.CountDistinctLong256GroupByFunction",
                ColumnType.LONG256
        );

        assertBindVariableTypes(
                "count(distinct $1)",
                new CountDistinctLong256GroupByFunctionFactory(),
                "io.questdb.griffin.engine.functions.groupby.CountDistinctLong256GroupByFunction",
                ColumnType.LONG256
        );

    }

    @Test
    public void testUndefinedBindVariableDefineShort() throws SqlException {
        assertBindVariableTypes(
                "abs($1)",
                new AbsShortFunctionFactory(),
                "io.questdb.griffin.engine.functions.math.AbsShortFunctionFactory.AbsFunction",
                ColumnType.SHORT
        );
    }

    @Test
    public void testUndefinedBindVariableDefineStr() throws SqlException {
        assertBindVariableTypes(
                "length($1)",
                new LengthStrFunctionFactory(),
                "io.questdb.griffin.engine.functions.str.LengthStrFunctionFactory.Func",
                ColumnType.STRING
        );
    }

    @Test
    public void testUndefinedBindVariableDefineSymbol() throws SqlException {
        assertBindVariableTypes(
                "length($1)",
                new LengthSymbolFunctionFactory(),
                "io.questdb.griffin.engine.functions.str.LengthSymbolFunctionFactory.LengthSymbolVFunc",
                ColumnType.STRING
        );
    }

    @Test
    public void testUndefinedBindVariableDefineTimestamp() throws SqlException {
        assertBindVariableTypes(
                "min($1)",
                new MinTimestampGroupByFunctionFactory(),
                "io.questdb.griffin.engine.functions.groupby.MinTimestampGroupByFunction",
                ColumnType.TIMESTAMP
        );
    }

    @Test
    public void testUndefinedBindVariableDefineVarArg() {
        // not defined
        bindVariableService.clear();
        functions.add(new SwitchFunctionFactory());
        try {
            parseFunction("case $1 when 'A' then $3 else $4 end", null, createFunctionParser());
            fail();
        } catch (SqlException e) {
            assertEquals(5, e.getPosition());
            TestUtils.assertContains("bind variable is not supported here, please use column instead", e.getFlyweightMessage());
        }
    }

    @Test
    public void testUndefinedBindVariableExactlyMatched() throws SqlException {
        bindVariableService.clear();
        functions.add(new EqIntFunctionFactory());
        functions.add(new EqDoubleFunctionFactory());
        functions.add(new EqLongFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.LONG));
        try (Function f = parseFunction("a = $1", metadata, createFunctionParser())) {
            TestUtils.assertContains(f.getClass().getCanonicalName(), "io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory.Func");
        }
        assertEquals(ColumnType.LONG, bindVariableService.getFunction(0).getType());
    }

    @Test
    public void testUndefinedBindVariableFuzzyMatched() throws SqlException {
        bindVariableService.clear();
        functions.add(new EqIntFunctionFactory());
        functions.add(new EqDoubleFunctionFactory());
        functions.add(new EqLongFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.FLOAT));
        try (Function f = parseFunction("a = $1", metadata, createFunctionParser())) {
            TestUtils.assertContains(f.getClass().getCanonicalName(), "io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory.Func");
        }
        assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(0).getType());
    }

    @Test
    public void testVarArgFunction() throws SqlException {
        functions.add(new InStrFunctionFactory());

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));

        FunctionParser functionParser = createFunctionParser();
        Record record = new Record() {
            @Override
            public CharSequence getStrA(int col) {
                return "YZ";
            }
        };

        Function function = parseFunction("a in ('XY', 'YZ')", metadata, functionParser);
        assertEquals(ColumnType.BOOLEAN, function.getType());
        assertTrue(function.getBool(record));
    }

    @Test
    public void testVarArgFunctionNoArg() {
        functions.add(new InStrFunctionFactory());

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.STRING));

        FunctionParser functionParser = createFunctionParser();

        try {
            parseFunction("a in ()", metadata, functionParser);
            fail();
        } catch (SqlException e) {
            assertEquals("[2] too few arguments for 'in'", e.getMessage());
        }
    }

    private void assertBindVariableTypes(
            String expr,
            FunctionFactory factory,
            CharSequence expectedFunctionClass,
            int... expectedTypes
    ) throws SqlException {
        bindVariableService.clear();
        functions.add(factory);
        try (Function f = parseFunction(expr, null, createFunctionParser())) {
            TestUtils.assertContains(f.getClass().getCanonicalName(), expectedFunctionClass);
        }

        for (int i = 0, n = expectedTypes.length; i < n; i++) {
            final int expected = expectedTypes[i];
            if (expected > -1) {
                final int actual = bindVariableService.getFunction(i).getType();
                if (expected != actual) {
                    fail("type mismatch [expected=" + ColumnType.nameOf(expected) + ", actual=" + ColumnType.nameOf(actual) + ", i=" + i + "]");
                }
            }
        }
    }

    private void assertCastToDouble(double expected, int type1, int type2, Record record) throws SqlException {
        functions.add(new AddDoubleFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", type1));
        metadata.add(new TableColumnMetadata("b", type2));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        assertEquals(ColumnType.DOUBLE, function.getType());
        assertEquals(expected, function.getDouble(record), 0.00001);
    }

    private void assertCastToFloat(Record record) throws SqlException {
        functions.add(new AddFloatFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BYTE));
        metadata.add(new TableColumnMetadata("b", ColumnType.SHORT));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        assertEquals(ColumnType.FLOAT, function.getType());
        assertEquals((float) 33, function.getFloat(record), 0.00001);
    }

    private void assertCastToLong(long expected, int type1, int type2, Record record) throws SqlException {
        functions.add(new AddLongFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", type1));
        metadata.add(new TableColumnMetadata("b", type2));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        assertEquals(ColumnType.LONG, function.getType());
        assertEquals(expected, function.getLong(record));
    }

    private void assertFail(int expectedPos, String expectedMessage, String expression, GenericRecordMetadata metadata) {
        FunctionParser functionParser = createFunctionParser();
        try {
            parseFunction(expression, metadata, functionParser);
            fail();
        } catch (SqlException e) {
            assertEquals(expectedPos, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    private void assertGeoConstFunctionTypeAndValue(
            FunctionFactory factory,
            CharSequence expectedFunctionClass,
            int expectedType,
            int expectedValue
    ) throws SqlException {
        bindVariableService.clear();
        functions.add(factory);
        try (Function f = parseFunction("geohash_func()", null, createFunctionParser())) {
            TestUtils.assertContains(f.getClass().getCanonicalName(), expectedFunctionClass);
            assertEquals(expectedType, f.getType());
            switch (ColumnType.tagOf(expectedType)) {
                case ColumnType.GEOBYTE:
                    assertEquals(expectedValue, f.getGeoByte(null));
                    break;
                case ColumnType.GEOSHORT:
                    assertEquals(expectedValue, f.getGeoShort(null));
                    break;
                case ColumnType.GEOINT:
                    assertEquals(expectedValue, f.getGeoInt(null));
                    break;
                default:
                    assertEquals(expectedValue, f.getGeoLong(null));
                    break;
            }
        }
    }

    private void assertSignatureFailure(String signature) throws SqlException {
        functions.add(new OrFunctionFactory());
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return signature;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return null;
            }
        });
        functions.add(new NotFunctionFactory());
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("a", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("b", ColumnType.BOOLEAN));
        FunctionParser functionParser = createFunctionParser();
        assertNotNull(parseFunction("a or not b", metadata, functionParser));
        assertEquals(2, functionParser.getFunctionFactoryCache().getFunctionCount());
    }

    @NotNull
    private FunctionFactory dummyGeoHashFunctionFactory(final int type, long hash) {
        return new FunctionFactory() {
            @Override
            public String getSignature() {
                return "geohash_func()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return Constants.getGeoHashConstantWithType(hash, type);
            }
        };
    }

    private void testConstantPassThru(Function constant) throws SqlException {
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "x()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return constant;
            }
        });
        assertSame(constant, parseFunction("x()", new GenericRecordMetadata(), createFunctionParser()));
    }
}
