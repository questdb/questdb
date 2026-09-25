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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.DecimalTypeDriver;
import io.questdb.cairo.FixedSizeTypeDriver;
import io.questdb.cairo.GeoHashTypeDriver;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.IntervalTypeDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampTypeDriver;
import io.questdb.cairo.TypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.Long128Constant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullArrayConstant;
import io.questdb.griffin.engine.functions.constants.NullBinConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TypeDriverTest {
    // tags that resolve overloads or mark parser state; none of their values is stored or computed
    private static final Set<ColumnTypeTag> PSEUDO_TAGS = EnumSet.of(
            ColumnTypeTag.UNDEFINED, ColumnTypeTag.CURSOR, ColumnTypeTag.VAR_ARG, ColumnTypeTag.RECORD,
            ColumnTypeTag.GEOHASH, ColumnTypeTag.DECIMAL, ColumnTypeTag.REGCLASS, ColumnTypeTag.REGPROCEDURE,
            ColumnTypeTag.ARRAY_STRING, ColumnTypeTag.PARAMETER, ColumnTypeTag.NULL
    );
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testClassInitOrder() throws Exception {
        // ColumnTypeTag, the leaves, TypeDrivers and ColumnType must agree whichever initialises first
        final String[][] orders = {
                {"type", "tag", "drivers", "leaf"},
                {"leaf", "type", "tag", "drivers"},
                {"drivers", "leaf", "tag", "type"},
                {"tag", "leaf", "drivers", "type"},
        };
        for (String[] order : orders) {
            runInFreshJvm(order);
        }
    }

    @Test
    public void testEncodedTypesResolveToTheTagDriver() {
        Assert.assertSame(GeoHashTypeDriver.GEOBYTE, ColumnType.getTypeDriver(ColumnType.getGeoHashTypeWithBits(5)));
        Assert.assertSame(GeoHashTypeDriver.GEOSHORT, ColumnType.getTypeDriver(ColumnType.getGeoHashTypeWithBits(8)));
        Assert.assertSame(GeoHashTypeDriver.GEOINT, ColumnType.getTypeDriver(ColumnType.getGeoHashTypeWithBits(31)));
        Assert.assertSame(GeoHashTypeDriver.GEOLONG, ColumnType.getTypeDriver(ColumnType.getGeoHashTypeWithBits(60)));
        Assert.assertSame(DecimalTypeDriver.DECIMAL32, ColumnType.getTypeDriver(ColumnType.getDecimalType(5, 2)));
        Assert.assertSame(DecimalTypeDriver.DECIMAL64, ColumnType.getTypeDriver(ColumnType.getDecimalType(18, 3)));
        Assert.assertSame(ArrayTypeDriver.INSTANCE, ColumnType.getTypeDriver(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2)));
        Assert.assertSame(TimestampTypeDriver.INSTANCE, ColumnType.getTypeDriver(ColumnType.TIMESTAMP_NANO));
        Assert.assertSame(TimestampTypeDriver.INSTANCE, ColumnType.getTypeDriver(ColumnType.setDesignatedTimestampBit(ColumnType.TIMESTAMP_MICRO, true)));
        Assert.assertSame(IntervalTypeDriver.INSTANCE, ColumnType.getTypeDriver(ColumnType.INTERVAL_TIMESTAMP_NANO));
        Assert.assertSame(ColumnType.getDriver(ColumnType.VARCHAR_SLICE), ColumnType.getTypeDriver(ColumnType.VARCHAR_SLICE));
    }

    @Test
    public void testEveryNonPseudoTagHasItsOwnDriver() {
        final List<TypeDriver> seen = new ArrayList<>();
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            final ColumnTypeTag enumTag = ColumnTypeTag.of(tag);
            if (PSEUDO_TAGS.contains(enumTag)) {
                try {
                    ColumnType.getTypeDriver(tag);
                    Assert.fail("pseudo tag " + enumTag + " must have no driver");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("no type driver for type: " + tag));
                }
                continue;
            }
            final TypeDriver driver = ColumnType.getTypeDriver(tag);
            Assert.assertNotNull(enumTag.name(), driver);
            if (enumTag == ColumnTypeTag.VARCHAR_SLICE) {
                // the slice is a transient view of a varchar and shares its driver
                Assert.assertSame(ColumnType.getTypeDriver(ColumnType.VARCHAR), driver);
                continue;
            }
            Assert.assertSame(enumTag.name(), enumTag, driver.getTag());
            Assert.assertEquals(enumTag.name(), driver.getTypeName());
            for (TypeDriver other : seen) {
                Assert.assertNotSame("one instance per tag: " + enumTag, other, driver);
            }
            seen.add(driver);
        }
        Assert.assertEquals(ColumnType.MAX_TAG + 1 - PSEUDO_TAGS.size() - 1, seen.size());
    }

    @Test
    public void testFixedSizeDriverWidthsMatchColumnType() {
        final IntList fixedWidthTags = new IntList();
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            if (PSEUDO_TAGS.contains(ColumnTypeTag.of(tag))) {
                continue;
            }
            final TypeDriver driver = ColumnType.getTypeDriver(tag);
            if (driver instanceof FixedSizeTypeDriver fixed) {
                Assert.assertEquals(ColumnType.nameOf(tag), ColumnType.sizeOf(tag), fixed.getWidth());
                Assert.assertEquals(ColumnType.nameOf(tag), ColumnType.pow2SizeOf(tag), fixed.getPow2Width());
                fixedWidthTags.add(tag);
            } else {
                Assert.assertTrue(ColumnType.nameOf(tag), driver instanceof ColumnTypeDriver);
                Assert.assertSame(ColumnType.nameOf(tag), ColumnType.getDriver(tag), driver);
                Assert.assertTrue(ColumnType.nameOf(tag), ColumnType.isVarSize(tag));
            }
        }
        // the fixed-width drivers are the isFixedSize tags plus SYMBOL and INTERVAL, which
        // isFixedSize reports as not fixed-size while their data vectors have a fixed width
        final IntList expected = new IntList();
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            if (ColumnType.isFixedSize(tag) || tag == ColumnType.SYMBOL || tag == ColumnType.INTERVAL) {
                expected.add(tag);
            }
        }
        Assert.assertEquals(expected, fixedWidthTags);
        Assert.assertEquals(26, fixedWidthTags.size());
    }

    @Test
    public void testNullAppenderWritesTheSameBytesAsSetNull() {
        // the per-row appender and the batch fill must agree, for every driver
        try (MemoryCARW mem = Vm.getCARWInstance(4096, 1, MemoryTag.NATIVE_DEFAULT)) {
            final long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
                    if (PSEUDO_TAGS.contains(ColumnTypeTag.of(tag)) || ColumnType.isVarSize(tag)) {
                        continue;
                    }
                    final FixedSizeTypeDriver driver = (FixedSizeTypeDriver) ColumnType.getTypeDriver(tag);
                    mem.truncate();
                    driver.newNullAppender(mem, null).run();
                    Assert.assertEquals(driver.getTypeName(), driver.getWidth(), mem.getAppendOffset());
                    driver.setNull(buf, 1);
                    for (int b = 0; b < driver.getWidth(); b++) {
                        Assert.assertEquals(driver.getTypeName() + " byte " + b, Unsafe.getByte(buf + b), mem.getByte(b));
                    }
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testColumnFunctionsPerType() {
        // the classes FunctionParser.createColumn and GroupByUtils.createColumnFunction used to name
        final Object[][] expected = {
                {ColumnType.BOOLEAN, "BooleanColumn"},
                {ColumnType.BYTE, "ByteColumn"},
                {ColumnType.SHORT, "ShortColumn"},
                {ColumnType.CHAR, "CharColumn"},
                {ColumnType.INT, "IntColumn"},
                {ColumnType.LONG, "LongColumn"},
                {ColumnType.DATE, "DateColumn"},
                {ColumnType.TIMESTAMP_MICRO, "TimestampColumn"},
                {ColumnType.TIMESTAMP_NANO, "TimestampColumn"},
                {ColumnType.FLOAT, "FloatColumn"},
                {ColumnType.DOUBLE, "DoubleColumn"},
                {ColumnType.STRING, "StrColumn"},
                {ColumnType.LONG256, "Long256Column"},
                {ColumnType.getGeoHashTypeWithBits(5), "GeoByteColumn"},
                {ColumnType.getGeoHashTypeWithBits(12), "GeoShortColumn"},
                {ColumnType.getGeoHashTypeWithBits(30), "GeoIntColumn"},
                {ColumnType.getGeoHashTypeWithBits(60), "GeoLongColumn"},
                {ColumnType.BINARY, "BinColumn"},
                {ColumnType.UUID, "UuidColumn"},
                {ColumnType.LONG128, "Long128Column"},
                {ColumnType.IPv4, "IPv4Column"},
                {ColumnType.VARCHAR, "VarcharColumn"},
                {ColumnType.VARCHAR_SLICE, "VarcharColumn"},
                {ColumnType.encodeArrayType(ColumnType.DOUBLE, 2), "ArrayColumn"},
                {ColumnType.getDecimalType(2, 1), "DecimalColumn"},
                {ColumnType.getDecimalType(4, 1), "DecimalColumn"},
                {ColumnType.getDecimalType(9, 2), "DecimalColumn"},
                {ColumnType.getDecimalType(18, 3), "DecimalColumn"},
                {ColumnType.getDecimalType(38, 4), "DecimalColumn"},
                {ColumnType.getDecimalType(76, 5), "DecimalColumn"},
                {ColumnType.INTERVAL_TIMESTAMP_MICRO, "IntervalColumn"},
                {ColumnType.INTERVAL_TIMESTAMP_NANO, "IntervalColumn"},
                {ColumnType.INTERVAL_RAW, "IntervalColumn"},
        };
        final Set<ColumnTypeTag> covered = EnumSet.noneOf(ColumnTypeTag.class);
        for (Object[] row : expected) {
            final int type = ((Number) row[0]).intValue();
            final String name = ColumnType.nameOf(type);
            for (int index : new int[]{0, 3, 1000}) {
                final Function func = ColumnType.getTypeDriver(type).newColumnFunction(index, type);
                Assert.assertEquals(name, row[1], func.getClass().getSimpleName());
                // IntervalColumn is the one column function that does not implement ColumnFunction
                if (func instanceof ColumnFunction columnFunction) {
                    Assert.assertEquals(name, index, columnFunction.getColumnIndex());
                }
                final int expectedType = ColumnType.tagOf(type) == ColumnType.VARCHAR_SLICE ? ColumnType.VARCHAR : type;
                Assert.assertEquals(name, expectedType, func.getType());
            }
            covered.add(ColumnTypeTag.of(type));
        }
        try {
            ColumnType.getTypeDriver(ColumnType.SYMBOL).newColumnFunction(0, ColumnType.SYMBOL);
            Assert.fail("SYMBOL column functions need the symbol table; the callers build them");
        } catch (UnsupportedOperationException ignore) {
        }
        covered.add(ColumnTypeTag.SYMBOL);
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            final ColumnTypeTag enumTag = ColumnTypeTag.of(tag);
            Assert.assertTrue(enumTag.name(), PSEUDO_TAGS.contains(enumTag) || covered.contains(enumTag));
        }
    }

    @Test
    public void testNullAsLongIsTheDeletedLongNullUtilsTable() {
        // the values the deleted LongNullUtils table held, per tag; a widening read of the storage NULL
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.BOOLEAN).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.BYTE).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.SHORT).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.CHAR).getNullAsLong());
        Assert.assertEquals(Numbers.INT_NULL, ColumnType.getTypeDriver(ColumnType.INT).getNullAsLong());
        Assert.assertEquals(Numbers.LONG_NULL, ColumnType.getTypeDriver(ColumnType.LONG).getNullAsLong());
        Assert.assertEquals(Numbers.LONG_NULL, ColumnType.getTypeDriver(ColumnType.DATE).getNullAsLong());
        Assert.assertEquals(Numbers.LONG_NULL, ColumnType.getTypeDriver(ColumnType.TIMESTAMP).getNullAsLong());
        Assert.assertEquals(Float.floatToIntBits(Float.NaN), ColumnType.getTypeDriver(ColumnType.FLOAT).getNullAsLong());
        Assert.assertEquals(Double.doubleToLongBits(Double.NaN), ColumnType.getTypeDriver(ColumnType.DOUBLE).getNullAsLong());
        // the query engine has always parked a missing SYMBOL as INT_NULL, not as VALUE_IS_NULL
        Assert.assertEquals(Numbers.INT_NULL, ColumnType.getTypeDriver(ColumnType.SYMBOL).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.LONG256).getNullAsLong());
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOBYTE).getNullAsLong());
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOSHORT).getNullAsLong());
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOINT).getNullAsLong());
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOLONG).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.UUID).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.LONG128).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.IPv4).getNullAsLong());
        Assert.assertEquals(Decimals.DECIMAL8_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL8).getNullAsLong());
        Assert.assertEquals(Decimals.DECIMAL16_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL16).getNullAsLong());
        Assert.assertEquals(Decimals.DECIMAL32_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL32).getNullAsLong());
        Assert.assertEquals(Decimals.DECIMAL64_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL64).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.DECIMAL128).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.DECIMAL256).getNullAsLong());
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.INTERVAL).getNullAsLong());
        for (int tag : new int[]{ColumnType.STRING, ColumnType.BINARY, ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE, ColumnType.ARRAY}) {
            Assert.assertEquals(ColumnType.nameOf(tag), 0L, ColumnType.getTypeDriver(tag).getNullAsLong());
        }
        // and, for every fixed driver, it is the low width bytes of the storage NULL, sign-extended
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            if (PSEUDO_TAGS.contains(ColumnTypeTag.of(tag)) || ColumnType.isVarSize(tag)) {
                continue;
            }
            final FixedSizeTypeDriver driver = (FixedSizeTypeDriver) ColumnType.getTypeDriver(tag);
            final long expected = switch (driver.getPow2Width()) {
                case 0 -> (byte) driver.getNullLong(0);
                case 1 -> (short) driver.getNullLong(0);
                case 2 -> (int) driver.getNullLong(0);
                case 3 -> driver.getNullLong(0);
                default -> 0L;
            };
            if (tag != ColumnType.SYMBOL) {
                Assert.assertEquals(ColumnType.nameOf(tag), expected, driver.getNullAsLong());
            }
        }
    }

    @Test
    public void testNullConstantsAreTheDeletedPreFill() {
        // the instances the deleted Constants.nullConstants pre-fill held, per tag
        final Object[][] expected = {
                {ColumnType.BOOLEAN, BooleanConstant.FALSE},
                {ColumnType.BYTE, ByteConstant.ZERO},
                {ColumnType.SHORT, ShortConstant.ZERO},
                {ColumnType.CHAR, CharConstant.ZERO},
                {ColumnType.INT, IntConstant.NULL},
                {ColumnType.LONG, LongConstant.NULL},
                {ColumnType.DATE, DateConstant.NULL},
                {ColumnType.TIMESTAMP_MICRO, ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO).getTimestampConstantNull()},
                {ColumnType.TIMESTAMP_NANO, ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_NANO).getTimestampConstantNull()},
                {ColumnType.FLOAT, FloatConstant.NULL},
                {ColumnType.DOUBLE, DoubleConstant.NULL},
                {ColumnType.STRING, StrConstant.NULL},
                {ColumnType.SYMBOL, SymbolConstant.NULL},
                {ColumnType.LONG256, Long256NullConstant.INSTANCE},
                {ColumnType.GEOBYTE, GeoByteConstant.NULL},
                {ColumnType.GEOSHORT, GeoShortConstant.NULL},
                {ColumnType.GEOINT, GeoIntConstant.NULL},
                {ColumnType.GEOLONG, GeoLongConstant.NULL},
                {ColumnType.BINARY, NullBinConstant.INSTANCE},
                {ColumnType.UUID, UuidConstant.NULL},
                {ColumnType.LONG128, Long128Constant.NULL},
                {ColumnType.IPv4, IPv4Constant.NULL},
                {ColumnType.VARCHAR, VarcharConstant.NULL},
                {ColumnType.INTERVAL, IntervalConstant.RAW_NULL},
                {ColumnType.INTERVAL_RAW, IntervalConstant.RAW_NULL},
                {ColumnType.INTERVAL_TIMESTAMP_MICRO, IntervalConstant.TIMESTAMP_MICRO_NULL},
                {ColumnType.INTERVAL_TIMESTAMP_NANO, IntervalConstant.TIMESTAMP_NANO_NULL},
                // pseudo tags, and VARCHAR_SLICE, have always yielded the untyped NULL
                {ColumnType.UNDEFINED, NullConstant.NULL},
                {ColumnType.CURSOR, NullConstant.NULL},
                {ColumnType.VAR_ARG, NullConstant.NULL},
                {ColumnType.RECORD, NullConstant.NULL},
                {ColumnType.GEOHASH, NullConstant.NULL},
                {ColumnType.DECIMAL, NullConstant.NULL},
                {ColumnType.REGCLASS, NullConstant.NULL},
                {ColumnType.REGPROCEDURE, NullConstant.NULL},
                {ColumnType.ARRAY_STRING, NullConstant.NULL},
                {ColumnType.PARAMETER, NullConstant.NULL},
                {ColumnType.VARCHAR_SLICE, NullConstant.NULL},
                {ColumnType.NULL, NullConstant.NULL},
        };
        final Set<ColumnTypeTag> covered = EnumSet.noneOf(ColumnTypeTag.class);
        for (Object[] row : expected) {
            final int type = ((Number) row[0]).intValue();
            Assert.assertSame(ColumnType.nameOf(type), row[1], Constants.getNullConstant(type));
            covered.add(ColumnTypeTag.of(type));
        }
        // encoded types: typed NULLs, cached where they always were
        for (int bits = 1; bits <= ColumnType.GEOLONG_MAX_BITS; bits++) {
            final int type = ColumnType.getGeoHashTypeWithBits(bits);
            final ConstantFunction c = Constants.getNullConstant(type);
            Assert.assertSame("bits " + bits, c, Constants.getNullConstant(type));
            Assert.assertEquals("bits " + bits, type, c.getType());
            Assert.assertTrue("bits " + bits, c.isNullConstant());
        }
        for (int dims = 1; dims <= 12; dims++) {
            final int type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dims);
            final ConstantFunction c = Constants.getNullConstant(type);
            Assert.assertEquals("dims " + dims, type, c.getType());
            Assert.assertTrue("dims " + dims, c instanceof NullArrayConstant);
            if (dims <= 10) {
                Assert.assertSame("dims " + dims, c, Constants.getNullConstant(type));
            }
        }
        for (int[] ps : new int[][]{{2, 1}, {4, 1}, {9, 2}, {18, 3}, {38, 4}, {76, 5}}) {
            final int type = ColumnType.getDecimalType(ps[0], ps[1]);
            final ConstantFunction c = Constants.getNullConstant(type);
            Assert.assertEquals(ColumnType.nameOf(type), type, c.getType());
            Assert.assertTrue(ColumnType.nameOf(type), c.isNullConstant());
            covered.add(ColumnTypeTag.of(type));
        }
        covered.add(ColumnTypeTag.ARRAY);
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            Assert.assertTrue(ColumnTypeTag.of(tag).name(), covered.contains(ColumnTypeTag.of(tag)));
        }
    }

    @Test
    public void testNullSentinelsAreTheDocumentedValues() {
        // the values the deleted TableUtils.setNull / getNullLong switches produced, per type
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.BOOLEAN).getNullLong(0));
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.BYTE).getNullLong(0));
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.SHORT).getNullLong(0));
        Assert.assertEquals(0L, ColumnType.getTypeDriver(ColumnType.CHAR).getNullLong(0));
        Assert.assertEquals(Numbers.encodeLowHighInts(Numbers.INT_NULL, Numbers.INT_NULL), ColumnType.getTypeDriver(ColumnType.INT).getNullLong(0));
        Assert.assertEquals(Numbers.encodeLowHighInts(SymbolTable.VALUE_IS_NULL, SymbolTable.VALUE_IS_NULL), ColumnType.getTypeDriver(ColumnType.SYMBOL).getNullLong(0));
        Assert.assertEquals(Numbers.encodeLowHighInts(Float.floatToIntBits(Float.NaN), Float.floatToIntBits(Float.NaN)), ColumnType.getTypeDriver(ColumnType.FLOAT).getNullLong(0));
        Assert.assertEquals(Double.doubleToLongBits(Double.NaN), ColumnType.getTypeDriver(ColumnType.DOUBLE).getNullLong(0));
        Assert.assertEquals(Numbers.IPv4_NULL, ColumnType.getTypeDriver(ColumnType.IPv4).getNullLong(0));
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOBYTE).getNullLong(0));
        Assert.assertEquals(GeoHashes.NULL, ColumnType.getTypeDriver(ColumnType.GEOLONG).getNullLong(0));
        for (int tag : new int[]{ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.LONG256, ColumnType.LONG128, ColumnType.UUID, ColumnType.INTERVAL}) {
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(ColumnType.nameOf(tag), Numbers.LONG_NULL, ColumnType.getTypeDriver(tag).getNullLong(i));
            }
        }
        Assert.assertEquals(Decimals.DECIMAL8_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL8).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL16_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL16).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL32_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL32).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL64_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL64).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL128_HI_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL128).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL128_LO_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL128).getNullLong(1));
        Assert.assertEquals(Decimals.DECIMAL256_HH_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL256).getNullLong(0));
        Assert.assertEquals(Decimals.DECIMAL256_HL_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL256).getNullLong(1));
        Assert.assertEquals(Decimals.DECIMAL256_LH_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL256).getNullLong(2));
        Assert.assertEquals(Decimals.DECIMAL256_LL_NULL, ColumnType.getTypeDriver(ColumnType.DECIMAL256).getNullLong(3));
        // var-size: the aux entry of a NULL
        Assert.assertEquals(Numbers.encodeLowHighInts(TableUtils.NULL_LEN, TableUtils.NULL_LEN), ColumnType.getTypeDriver(ColumnType.STRING).getNullLong(0));
        Assert.assertEquals(TableUtils.NULL_LEN, ColumnType.getTypeDriver(ColumnType.BINARY).getNullLong(0));
        Assert.assertEquals(TableUtils.NULL_LEN, ColumnType.getTypeDriver(ColumnType.VARCHAR).getNullLong(0));
        Assert.assertEquals(TableUtils.NULL_LEN, ColumnType.getTypeDriver(ColumnType.ARRAY).getNullLong(0));

        // only the four value-only types have no sentinel
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            if (PSEUDO_TAGS.contains(ColumnTypeTag.of(tag))) {
                continue;
            }
            final boolean isValueOnly = tag == ColumnType.BOOLEAN || tag == ColumnType.BYTE || tag == ColumnType.SHORT || tag == ColumnType.CHAR;
            Assert.assertEquals(ColumnType.nameOf(tag), !isValueOnly, ColumnType.getTypeDriver(tag).hasNullSentinel());
        }
    }

    @Test
    public void testSetNullWritesTheNullLongs() {
        // the batch fill and the per-long NULL description agree byte for byte, for every fixed type
        final long mem1 = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        final long mem2 = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
                if (PSEUDO_TAGS.contains(ColumnTypeTag.of(tag)) || ColumnType.isVarSize(tag)) {
                    continue;
                }
                final TypeDriver driver = ColumnType.getTypeDriver(tag);
                final int size = ColumnType.sizeOf(tag);
                Assert.assertTrue(size > 0);
                driver.setNull(mem2, 1);
                Unsafe.putLong(mem1, driver.getNullLong(0));
                Unsafe.putLong(mem1 + 8, driver.getNullLong(1));
                Unsafe.putLong(mem1 + 16, driver.getNullLong(2));
                Unsafe.putLong(mem1 + 24, driver.getNullLong(3));
                for (int b = 0; b < size; b++) {
                    Assert.assertEquals(ColumnType.nameOf(tag) + " byte " + b, Unsafe.getByte(mem1 + b), Unsafe.getByte(mem2 + b));
                }
            }
        } finally {
            Unsafe.free(mem1, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(mem2, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOfMatchesTagOfForEveryEncodableType() {
        final IntList types = new IntList();
        for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            types.add(tag);
        }
        for (int bits = 1; bits <= ColumnType.GEOLONG_MAX_BITS; bits++) {
            types.add(ColumnType.getGeoHashTypeWithBits(bits));
        }
        for (int precision = 1; precision <= Decimals.MAX_PRECISION; precision++) {
            for (int scale = 0; scale <= Math.min(precision, Decimals.MAX_SCALE); scale++) {
                types.add(ColumnType.getDecimalType(precision, scale));
            }
        }
        for (int dims = 1; dims <= ColumnType.ARRAY_NDIMS_LIMIT; dims++) {
            types.add(ColumnType.encodeArrayType(ColumnType.DOUBLE, dims));
        }
        types.add(ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true));
        types.add(ColumnType.TIMESTAMP_MICRO);
        types.add(ColumnType.TIMESTAMP_NANO);
        types.add(ColumnType.setDesignatedTimestampBit(ColumnType.TIMESTAMP_MICRO, true));
        types.add(ColumnType.setDesignatedTimestampBit(ColumnType.TIMESTAMP_NANO, true));
        types.add(ColumnType.INTERVAL_RAW);
        types.add(ColumnType.INTERVAL_TIMESTAMP_MICRO);
        types.add(ColumnType.INTERVAL_TIMESTAMP_NANO);
        for (int i = 0, n = types.size(); i < n; i++) {
            final int type = types.getQuick(i);
            final ColumnTypeTag tag = ColumnTypeTag.of(type);
            Assert.assertNotEquals(Integer.toHexString(type), ColumnTypeTag.UNKNOWN, tag);
            Assert.assertEquals(Integer.toHexString(type), ColumnType.tagOf(type), tag.code());
        }
        // and nothing else is a tag
        Assert.assertEquals(ColumnTypeTag.UNKNOWN, ColumnTypeTag.of(-1));
        for (int code = ColumnType.MAX_TAG + 1; code < 256; code++) {
            Assert.assertEquals(ColumnTypeTag.UNKNOWN, ColumnTypeTag.of(code));
        }
    }

    @Test
    public void testTagEnumMirrorsColumnTypeConstants() throws Exception {
        // every ColumnType tag constant has an enum constant of the same name and number
        int constants = 0;
        for (Field field : ColumnType.class.getFields()) {
            final int mods = field.getModifiers();
            if (field.getType() != short.class || !Modifier.isStatic(mods) || !Modifier.isFinal(mods) || "MAX_TAG".equals(field.getName())) {
                continue;
            }
            final short code = field.getShort(null);
            if (code < 0 || code > ColumnType.MAX_TAG) {
                continue; // OVERLOAD_FULL, OVERLOAD_NONE
            }
            Assert.assertEquals(field.getName(), code, ColumnTypeTag.valueOf(field.getName()).code());
            constants++;
        }
        // and every enum constant but UNKNOWN is such a constant
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (tag == ColumnTypeTag.UNKNOWN) {
                Assert.assertEquals(-1, tag.code());
                continue;
            }
            Assert.assertEquals(tag.name(), tag.code(), ColumnType.class.getField(tag.name()).getShort(null));
        }
        Assert.assertEquals(ColumnType.MAX_TAG + 1, constants);
        Assert.assertEquals(ColumnType.MAX_TAG + 2, ColumnTypeTag.values().length);
    }

    private void runInFreshJvm(String[] order) throws Exception {
        File javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java");
        if (!javaExecutable.exists()) {
            javaExecutable = new File(javaExecutable.getPath() + ".exe");
        }
        final String classPath = Paths.get(
                TypeDriverInitOrderMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()
        ) + File.pathSeparator + Paths.get(
                ColumnType.class.getProtectionDomain().getCodeSource().getLocation().toURI()
        );
        final File outputFile = temp.newFile("type-driver-init-" + String.join("-", order) + ".out");
        final List<String> command = new ArrayList<>();
        command.add(javaExecutable.getAbsolutePath());
        command.add("-ea");
        command.add("--enable-native-access=ALL-UNNAMED");
        command.add("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED");
        command.add("-cp");
        command.add(classPath);
        command.add(TypeDriverInitOrderMain.class.getName());
        command.addAll(List.of(order));
        final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile).start();
        try {
            process.getOutputStream().close();
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor();
                Assert.fail("init order process timed out:\n" + Files.readString(outputFile.toPath(), StandardCharsets.UTF_8));
            }
            final String output = Files.readString(outputFile.toPath(), StandardCharsets.UTF_8);
            Assert.assertEquals(output, 0, process.exitValue());
            Assert.assertTrue(output, output.trim().endsWith("OK " + String.join(",", order)));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().onExit().join();
            }
        }
    }
}
