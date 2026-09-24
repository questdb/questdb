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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
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
