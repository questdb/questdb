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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnTypeTest {
    // Every tag number, pseudo tags included; the index is the number. Inserting a tag renumbers
    // every tag after it, so the insertion must also edit this table, column_type.h and
    // col_type.rs (the two file tests below compare those against this table).
    private static final String[] PINNED_TAG_NAMES = {
            "UNDEFINED",     // 0
            "BOOLEAN",       // 1
            "BYTE",          // 2
            "SHORT",         // 3
            "CHAR",          // 4
            "INT",           // 5
            "LONG",          // 6
            "DATE",          // 7
            "TIMESTAMP",     // 8
            "FLOAT",         // 9
            "DOUBLE",        // 10
            "STRING",        // 11
            "SYMBOL",        // 12
            "LONG256",       // 13
            "GEOBYTE",       // 14
            "GEOSHORT",      // 15
            "GEOINT",        // 16
            "GEOLONG",       // 17
            "BINARY",        // 18
            "UUID",          // 19
            "CURSOR",        // 20
            "VAR_ARG",       // 21
            "RECORD",        // 22
            "GEOHASH",       // 23
            "LONG128",       // 24
            "IPv4",          // 25
            "VARCHAR",       // 26
            "ARRAY",         // 27
            "DECIMAL8",      // 28
            "DECIMAL16",     // 29
            "DECIMAL32",     // 30
            "DECIMAL64",     // 31
            "DECIMAL128",    // 32
            "DECIMAL256",    // 33
            "DECIMAL",       // 34
            "REGCLASS",      // 35
            "REGPROCEDURE",  // 36
            "ARRAY_STRING",  // 37
            "PARAMETER",     // 38
            "INTERVAL",      // 39
            "VARCHAR_SLICE", // 40
            "NULL",          // 41
    };
    // Tags that qdb_core::ColumnTypeTag (col_type.rs) deliberately does not carry: the Rust side
    // only sees types that reach disk, plus VARCHAR_SLICE.
    private static final Set<String> TAGS_ABSENT_FROM_RUST = Set.of(
            "UNDEFINED", "CURSOR", "VAR_ARG", "RECORD", "GEOHASH", "DECIMAL",
            "REGCLASS", "REGPROCEDURE", "ARRAY_STRING", "PARAMETER", "INTERVAL", "NULL"
    );

    public short getExpectedTag(int precision) {
        int size = Decimals.getStorageSizePow2(precision);
        switch (size) {
            case 0:
                return ColumnType.DECIMAL8;
            case 1:
                return ColumnType.DECIMAL16;
            case 2:
                return ColumnType.DECIMAL32;
            case 3:
                return ColumnType.DECIMAL64;
            case 4:
                return ColumnType.DECIMAL128;
            default:
                return ColumnType.DECIMAL256;
        }
    }

    @Test
    public void testArrayWithWeakDims() {
        int arrayType = ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertTrue(ColumnType.isArrayWithWeakDims(arrayType));
        // arrays with weak dimensions are considered undefined
        Assert.assertTrue(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.DOUBLE, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(-1, ColumnType.decodeWeakArrayDimensionality(arrayType));

        arrayType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 5);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertFalse(ColumnType.isArrayWithWeakDims(arrayType));
        Assert.assertFalse(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.DOUBLE, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(5, ColumnType.decodeWeakArrayDimensionality(arrayType));

        arrayType = ColumnType.encodeArrayType(ColumnType.LONG, 7, false);
        Assert.assertTrue(ColumnType.isArray(arrayType));
        Assert.assertFalse(ColumnType.isArrayWithWeakDims(arrayType));
        Assert.assertFalse(ColumnType.isUndefined(arrayType));
        Assert.assertEquals(ColumnType.LONG, ColumnType.decodeArrayElementType(arrayType));
        Assert.assertEquals(7, ColumnType.decodeWeakArrayDimensionality(arrayType));
    }

    @Test
    public void testDecimalDefaultType() {
        Assert.assertEquals(ColumnType.DECIMAL_DEFAULT_TYPE_TAG, ColumnType.tagOf(ColumnType.DECIMAL_DEFAULT_TYPE));
    }

    @Test
    public void testGetDecimalTypeCombinatorics() {
        // Combinations of precision, scale -> expected type
        int[][] combinations = {
                {1, 0, 0x00000100 | (int) ColumnType.DECIMAL8},
                {1, 1, 0x00040100 | (int) ColumnType.DECIMAL8},
                {3, 2, 0x00080300 | (int) ColumnType.DECIMAL16},
                {6, 0, 0x00000600 | (int) ColumnType.DECIMAL32},
                {7, 4, 0x00100700 | (int) ColumnType.DECIMAL32},
                {12, 12, 0x00300C00 | (int) ColumnType.DECIMAL64},
                {18, 0, 0x00001200 | (int) ColumnType.DECIMAL64},
                {18, 18, 0x00481200 | (int) ColumnType.DECIMAL64},
                {30, 4, 0x00101E00 | (int) ColumnType.DECIMAL128},
                {30, 30, 0x00781E00 | (int) ColumnType.DECIMAL128},
                {42, 4, 0x00102A00 | (int) ColumnType.DECIMAL256},
                {55, 45, 0x00B43700 | (int) ColumnType.DECIMAL256},
                {76, 10, 0x00284C00 | (int) ColumnType.DECIMAL256},
        };

        for (int[] combination : combinations) {
            int precision = combination[0];
            int scale = combination[1];
            int expectedType = combination[2];
            int type = ColumnType.getDecimalType(precision, scale);
            Assert.assertEquals(String.format("Failure with precision: %d and scale: %d. Expected 0x%08x but was 0x%08x", precision, scale, expectedType, type), expectedType, type);
        }
    }

    @Test
    public void testGetDecimalTypeFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        final int iterations = 1_000;
        for (int i = 0; i < iterations; i++) {
            int precision = rnd.nextInt(Decimals.MAX_PRECISION - 1) + 1;
            int scale = rnd.nextInt(Decimals.MAX_SCALE - 1) + 1;
            int type = ColumnType.getDecimalType(precision, scale);

            int p = ColumnType.getDecimalPrecision(type);
            Assert.assertEquals(String.format("Failed at iteration %d, expected precision to be %d not %d", i, precision, p), precision, p);

            int s = ColumnType.getDecimalScale(type);
            Assert.assertEquals(String.format("Failed at iteration %d, expected scale to be %d not %d", i, scale, s), scale, s);

            short tag = ColumnType.tagOf(type);
            short expectedTag = getExpectedTag(precision);
            Assert.assertEquals(String.format("Failed at iteration %d, expected tag to be %d not %d", i, expectedTag, tag), expectedTag, tag);

            Assert.assertTrue(ColumnType.isDecimal(type));
        }
    }

    @Test
    public void testIsBuiltInWideningCastContract() {
        // Contract: if isBuiltInWideningCast(from, to) returns true,
        // then calling the corresponding getter on a Function of type 'from'
        // to retrieve value as type 'to' must NOT throw UnsupportedOperationException

        Set<Short> unsupportedTypes = Set.of(
                ColumnType.UNDEFINED,
                ColumnType.VAR_ARG, // special marker, not really a type
                ColumnType.RECORD,
                ColumnType.CURSOR,
                ColumnType.REGCLASS,
                ColumnType.REGPROCEDURE,
                ColumnType.ARRAY_STRING,
                ColumnType.PARAMETER
        );

        short allTypesLowerBoundInc = ColumnType.UNDEFINED + 1;
        short allTypesUpperBoundEx = ColumnType.NULL;

        int violations = 0;
        int unexpectedlySupported = 0;
        StringBuilder violationDetails = new StringBuilder();
        StringBuilder unexpectedlySupportedDetails = new StringBuilder();

        for (short fromType = allTypesLowerBoundInc; fromType < allTypesUpperBoundEx; fromType++) {
            if (unsupportedTypes.contains(fromType)) {
                continue;
            }
            for (short toType = allTypesLowerBoundInc; toType < allTypesUpperBoundEx; toType++) {
                if (unsupportedTypes.contains(toType)) {
                    continue;
                }

                boolean isBuiltInWidening = ColumnType.isBuiltInWideningCast(fromType, toType);
                Function testFunc = createTestFunction(fromType);

                boolean throwsUnsupported = false;
                String exceptionMessage = null;

                try {
                    callGetterForType(testFunc, toType);
                } catch (ImplicitCastException e) {
                    // ImplicitCastException means the conversion is supported but the value failed
                    // This is acceptable - types are compatible, just this specific value can't convert
                    // Example: CHAR 'A' -> BYTE throws ImplicitCastException, but CHAR -> BYTE is supported
                } catch (UnsupportedOperationException e) {
                    // UnsupportedOperationException means the types are fundamentally incompatible
                    throwsUnsupported = true;
                    exceptionMessage = e.getMessage();
                }

                // Check contract violation: isBuiltInWidening claims true but getter throws
                if (isBuiltInWidening && throwsUnsupported) {
                    violations++;
                    violationDetails.append(String.format(
                            "\n  VIOLATION: isBuiltInWideningCast(%s, %s) = true, but getter throws UnsupportedOperationException: %s",
                            ColumnType.nameOf(fromType),
                            ColumnType.nameOf(toType),
                            exceptionMessage
                    ));
                }

                // Check inverse: getter works but isBuiltInWidening returns false
                // This is informational - might indicate missing optimization or intentional design
                if (!isBuiltInWidening && !throwsUnsupported && fromType != toType) {
                    unexpectedlySupported++;
                    unexpectedlySupportedDetails.append(String.format(
                            "\n  INFO: isBuiltInWideningCast(%s, %s) = false, but getter works without UnsupportedOperationException",
                            ColumnType.nameOf(fromType),
                            ColumnType.nameOf(toType)
                    ));
                }
            }
        }

        if (violations > 0) {
            Assert.fail("Found " + violations + " contract violations:" + violationDetails);
        }

        // Print informational findings
        if (unexpectedlySupported > 0) {
            System.out.println("\n=== Informational: Getters that work but aren't marked as isBuiltInWideningCast ===");
            System.out.println("Found " + unexpectedlySupported + " cases:" + unexpectedlySupportedDetails);
            System.out.println("\nThese conversions work but may require cast wrappers or are intentionally not optimized.");
        }
    }

    @Test
    public void testArrayElementTagsFitTheElementTypeField() {
        // encodeArrayType() stores the element tag in a 6-bit field (ARRAY_ELEMTYPE_FIELD_MASK = 0x3F).
        // Every tag that can be an array element, or that has an array type name registered, must
        // survive the round trip; a tag numbered 64 or above cannot.
        for (short tag = ColumnType.UNDEFINED; tag <= ColumnType.MAX_TAG; tag++) {
            final int arrayType = ColumnType.encodeArrayType(tag, 1, false);
            final boolean isElementTag = ColumnType.isSupportedArrayElementType(tag)
                    || !"unknown".equals(ColumnType.nameOf(arrayType));
            if (isElementTag) {
                Assert.assertTrue("array element tag " + ColumnType.nameOf(tag) + " = " + tag + " does not fit 6 bits", tag < 64);
                Assert.assertEquals(ColumnType.nameOf(tag), tag, ColumnType.decodeArrayElementType(arrayType));
            }
        }
    }

    @Test
    public void testColumnTypeHeaderMatchesJava() throws IOException {
        // core/src/main/c/share/column_type.h mirrors the Java tag numbers by hand.
        final Path header = sourceFile("src/main/c/share/column_type.h");
        final Pattern entry = Pattern.compile("^\\s*([A-Z0-9_]+)\\s*=\\s*(\\d+)\\s*,");
        final Map<String, Integer> parsed = new HashMap<>();
        boolean isInEnum = false;
        for (String line : Files.readAllLines(header, StandardCharsets.UTF_8)) {
            if (line.startsWith("enum class ColumnType")) {
                isInEnum = true;
                continue;
            }
            if (!isInEnum) {
                continue;
            }
            if (line.startsWith("}")) {
                break;
            }
            final Matcher m = entry.matcher(line);
            if (m.find()) {
                // NULL_ avoids the C macro; TIMESTAMP_MICRO is the header's name for the TIMESTAMP tag
                String name = m.group(1);
                name = name.endsWith("_") ? name.substring(0, name.length() - 1) : name;
                name = "TIMESTAMP_MICRO".equals(name) ? "TIMESTAMP" : name;
                Assert.assertNull("duplicate entry " + name + " in " + header, parsed.put(name, Integer.parseInt(m.group(2))));
            }
        }
        Assert.assertFalse("no enum entries found in " + header, parsed.isEmpty());

        final Map<String, Integer> pinned = pinnedTagsByUpperCaseName();
        for (Map.Entry<String, Integer> e : parsed.entrySet()) {
            final Integer javaTag = pinned.get(e.getKey());
            Assert.assertNotNull("column_type.h names a tag Java does not have: " + e.getKey(), javaTag);
            Assert.assertEquals("column_type.h disagrees with Java on " + e.getKey(), javaTag, e.getValue());
        }
        for (String name : pinned.keySet()) {
            Assert.assertTrue("column_type.h is missing tag " + name, parsed.containsKey(name));
        }
    }

    @Test
    public void testGetDriverVarcharSlice() {
        // VARCHAR_SLICE is a transient in-memory type from read_parquet().
        // getDriver() must return the same VarcharTypeDriver as for VARCHAR.
        Assert.assertSame(
                ColumnType.getDriver(ColumnType.VARCHAR),
                ColumnType.getDriver(ColumnType.VARCHAR_SLICE)
        );
    }

    @Test
    public void testIsDecimalInvalid() {
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.BOOLEAN));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.DOUBLE));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.VARCHAR));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.INTERVAL));
        Assert.assertFalse(ColumnType.isDecimal(ColumnType.GEOHASH));
    }

    @Test
    public void testMaxTagFitsTheTagField() {
        // The tag is an 8-bit field; keep the top bit clear so a tag never reads as negative when
        // narrowed to a signed byte.
        Assert.assertTrue(ColumnType.MAX_TAG < 128);
        Assert.assertEquals(ColumnType.NULL, ColumnType.MAX_TAG);
    }

    @Test
    public void testRustColumnTypeTagMatchesJava() throws IOException {
        // core/rust/qdb-core/src/col_type.rs hand-numbers ColumnTypeTag, repeats the numbers in
        // TryFrom<u8>, and counts the variants in VALUES; ENT Rust depends on all three.
        final Path source = sourceFile("rust/qdb-core/src/col_type.rs");
        final List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
        final Pattern variant = Pattern.compile("^\\s*([A-Za-z0-9]+)\\s*=\\s*(\\d+)\\s*,");
        final Pattern tryFromArm = Pattern.compile("^\\s*(\\d+)\\s*=>\\s*Ok\\(ColumnTypeTag::([A-Za-z0-9]+)\\)");
        final Pattern valuesLen = Pattern.compile("const VALUES: \\[Self; (\\d+)]");
        final Map<String, Integer> variants = new HashMap<>();
        final Map<String, Integer> tryFromArms = new HashMap<>();
        int valuesCount = -1;
        boolean isInEnum = false;
        for (String line : lines) {
            if (line.startsWith("pub enum ColumnTypeTag")) {
                isInEnum = true;
                continue;
            }
            if (isInEnum) {
                if (line.startsWith("}")) {
                    isInEnum = false;
                    continue;
                }
                final Matcher m = variant.matcher(line);
                if (m.find()) {
                    Assert.assertNull("duplicate variant " + m.group(1), variants.put(m.group(1), Integer.parseInt(m.group(2))));
                }
                continue;
            }
            Matcher m = tryFromArm.matcher(line);
            if (m.find()) {
                Assert.assertNull("duplicate TryFrom arm for " + m.group(2), tryFromArms.put(m.group(2), Integer.parseInt(m.group(1))));
                continue;
            }
            m = valuesLen.matcher(line);
            if (m.find()) {
                valuesCount = Integer.parseInt(m.group(1));
            }
        }
        Assert.assertFalse("no ColumnTypeTag variants found in " + source, variants.isEmpty());
        Assert.assertEquals("VALUES length in " + source, variants.size(), valuesCount);
        Assert.assertEquals("TryFrom<u8> arms in " + source, variants, tryFromArms);

        // Rust spells tags in CamelCase: GeoByte, VarcharSlice, IPv4. Compare case-insensitively
        // with the underscores removed.
        final Map<String, Integer> pinned = new HashMap<>();
        for (int tag = 0; tag < PINNED_TAG_NAMES.length; tag++) {
            pinned.put(PINNED_TAG_NAMES[tag].replace("_", "").toLowerCase(), tag);
        }
        final Set<String> expectedInRust = new HashSet<>();
        for (String name : PINNED_TAG_NAMES) {
            if (!TAGS_ABSENT_FROM_RUST.contains(name)) {
                expectedInRust.add(name.replace("_", "").toLowerCase());
            }
        }
        final Set<String> foundInRust = new HashSet<>();
        for (Map.Entry<String, Integer> e : variants.entrySet()) {
            final String key = e.getKey().toLowerCase();
            final Integer javaTag = pinned.get(key);
            Assert.assertNotNull("col_type.rs names a tag Java does not have: " + e.getKey(), javaTag);
            Assert.assertEquals("col_type.rs disagrees with Java on " + e.getKey(), javaTag, e.getValue());
            foundInRust.add(key);
        }
        Assert.assertEquals("col_type.rs variant set (update TAGS_ABSENT_FROM_RUST if the omission is deliberate)", expectedInRust, foundInRust);
    }

    @Test
    public void testTagNumbersArePinned() throws Exception {
        Assert.assertEquals("MAX_TAG must be the last pinned tag", PINNED_TAG_NAMES.length - 1, ColumnType.MAX_TAG);
        for (int tag = 0; tag < PINNED_TAG_NAMES.length; tag++) {
            final Field field = ColumnType.class.getField(PINNED_TAG_NAMES[tag]);
            Assert.assertEquals("ColumnType." + PINNED_TAG_NAMES[tag], tag, field.getShort(null));
        }
        // Every public short constant in the tag range must be one of the pinned names, so a new
        // tag cannot be added without extending the table, and no two tags share a number.
        for (Field field : ColumnType.class.getFields()) {
            final int mods = field.getModifiers();
            if (field.getType() != short.class || !Modifier.isStatic(mods) || !Modifier.isFinal(mods) || "MAX_TAG".equals(field.getName())) {
                continue;
            }
            final short value = field.getShort(null);
            if (value >= 0 && value <= ColumnType.MAX_TAG) {
                Assert.assertEquals("unpinned tag constant ColumnType." + field.getName(), PINNED_TAG_NAMES[value], field.getName());
            }
        }
    }

    private static Map<String, Integer> pinnedTagsByUpperCaseName() {
        final Map<String, Integer> pinned = new HashMap<>();
        for (int tag = 0; tag < PINNED_TAG_NAMES.length; tag++) {
            pinned.put(PINNED_TAG_NAMES[tag].toUpperCase(), tag);
        }
        return pinned;
    }

    // Surefire runs with core/ as the working directory; fall back to the repository root.
    private static Path sourceFile(String relativeToCore) {
        final Path inCore = Paths.get(relativeToCore);
        if (Files.exists(inCore)) {
            return inCore;
        }
        final Path inRoot = Paths.get("core", relativeToCore);
        Assert.assertTrue("source file not found: " + inCore.toAbsolutePath(), Files.exists(inRoot));
        return inRoot;
    }

    private void callGetterForType(Function func, short type) {
        switch (type) {
            case ColumnType.BOOLEAN -> func.getBool(null);
            case ColumnType.BYTE -> func.getByte(null);
            case ColumnType.SHORT -> func.getShort(null);
            case ColumnType.CHAR -> func.getChar(null);
            case ColumnType.INT -> func.getInt(null);
            case ColumnType.LONG -> func.getLong(null);
            case ColumnType.DATE -> func.getDate(null);
            case ColumnType.TIMESTAMP -> func.getTimestamp(null);
            case ColumnType.FLOAT -> func.getFloat(null);
            case ColumnType.DOUBLE -> func.getDouble(null);
            case ColumnType.STRING -> func.getStrA(null);
            case ColumnType.SYMBOL -> func.getSymbol(null);
            case ColumnType.LONG256 -> func.getLong256A(null);
            case ColumnType.GEOBYTE -> func.getGeoByte(null);
            case ColumnType.GEOSHORT -> func.getGeoShort(null);
            case ColumnType.GEOINT -> func.getGeoInt(null);
            case ColumnType.GEOLONG -> func.getGeoLong(null);
            case ColumnType.BINARY -> func.getBin(null);
            case ColumnType.UUID, ColumnType.LONG128 -> func.getLong128Lo(null);
            case ColumnType.GEOHASH -> func.getGeoLong(null);
            case ColumnType.IPv4 -> func.getIPv4(null);
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> func.getVarcharA(null);
            case ColumnType.ARRAY -> func.getArray(null);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL -> func.getDecimal8(null);
            case ColumnType.DECIMAL16 -> func.getDecimal16(null);
            case ColumnType.DECIMAL32 -> func.getDecimal32(null);
            case ColumnType.DECIMAL64 -> func.getDecimal64(null);
            case ColumnType.DECIMAL128 -> func.getDecimal128(null, new Decimal128());
            case ColumnType.DECIMAL256 -> func.getDecimal256(null, new Decimal256());
            case ColumnType.INTERVAL -> func.getInterval(null);
            default ->
                    throw new AssertionError("Unexpected type [type=" + ColumnType.nameOf(type) + ", id=" + type + ']');
        }
    }

    private Function createTestFunction(short type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN -> BooleanConstant.TRUE;
            case ColumnType.BYTE -> new ByteConstant((byte) 42);
            case ColumnType.SHORT -> new ShortConstant((short) 42);
            case ColumnType.CHAR -> new CharConstant('A');
            case ColumnType.INT -> new IntConstant(42);
            case ColumnType.LONG -> new LongConstant(42L);
            case ColumnType.DATE -> new DateConstant(42L);
            case ColumnType.TIMESTAMP -> new TimestampConstant(42L, ColumnType.TIMESTAMP_MICRO);
            case ColumnType.FLOAT -> new FloatConstant(42.0f);
            case ColumnType.DOUBLE -> new DoubleConstant(42.0);
            case ColumnType.STRING -> new StrConstant("42");
            case ColumnType.NULL -> NullConstant.NULL;
            case ColumnType.SYMBOL -> new SymbolConstant("sym", 0);
            case ColumnType.LONG256 -> new Long256Constant(0, 0, 0, 0);
            case ColumnType.GEOBYTE -> GeoByteConstant.NULL;
            case ColumnType.GEOSHORT -> GeoShortConstant.NULL;
            case ColumnType.GEOINT -> GeoIntConstant.NULL;
            case ColumnType.GEOLONG -> GeoLongConstant.NULL;
            case ColumnType.BINARY -> NullBinConstant.INSTANCE;
            case ColumnType.UUID, ColumnType.LONG128 -> Long128Constant.NULL;
            case ColumnType.GEOHASH -> GeoLongConstant.NULL;
            case ColumnType.IPv4 -> IPv4Constant.NULL;
            case ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> new VarcharConstant("42");
            case ColumnType.ARRAY -> new NullArrayConstant(ColumnType.DOUBLE);
            case ColumnType.DECIMAL8, ColumnType.DECIMAL ->
                    new Decimal8Constant((byte) 0, ColumnType.getDecimalType(2, 0));
            case ColumnType.DECIMAL16 -> new Decimal16Constant((short) 0, ColumnType.getDecimalType(4, 0));
            case ColumnType.DECIMAL32 -> new Decimal32Constant(0, ColumnType.getDecimalType(8, 0));
            case ColumnType.DECIMAL64 -> new Decimal64Constant(0, ColumnType.getDecimalType(16, 0));
            case ColumnType.DECIMAL128 -> new Decimal128Constant(0, 0, ColumnType.getDecimalType(34, 0));
            case ColumnType.DECIMAL256 -> new Decimal256Constant(0, 0, 0, 0, ColumnType.getDecimalType(76, 0));
            case ColumnType.INTERVAL -> IntervalConstant.TIMESTAMP_MICRO_NULL;
            default ->
                    throw new AssertionError("Unexpected type [type=" + ColumnType.nameOf(type) + ", id=" + type + ']');
        };
    }
}
