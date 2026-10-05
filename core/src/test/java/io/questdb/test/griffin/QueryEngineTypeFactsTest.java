/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.WhereClauseParser;
import io.questdb.jit.CompiledFilterIRSerializer;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * The per-type facts the query engine reads from the type drivers: integers and numbers, WHERE key
 * columns and timestamp bounds, the compiled filter's integer lanes; and the FILL(PREV) type match,
 * which compares whole types. Each expected list pins the types the engine treats that way, so a
 * type that answers differently is a behaviour change. The sweep covers every tag and the encoded
 * types that carry parameters (timestamp unit, geohash bits, decimal precision and scale, array
 * dimensions, interval unit); a new tag joins the sweep and answers here.
 */
public class QueryEngineTypeFactsTest {
    // the type sweep, shared with the coverage tests of the per-type answers the relation rules
    // leave outside (bind-variable setters, DecimalUtil.load, a CAST target's bind type): every tag,
    // labelled by its ColumnType constant, then the encoded types
    public static final String[] LABELS;
    public static final int[] TYPES;

    @Test
    public void testFillPrevMatchesTheWholeType() {
        // FILL(PREV(col)) admits a source column whose type equals the target's. `listed` is a
        // per-type rule: the whole type for DECIMAL, GEOHASH, ARRAY, TIMESTAMP and INTERVAL, the
        // tag for every other type. The two agree for every pair, because an encoding without
        // parameters is its tag, except a bare GEOBYTE..GEOLONG tag, which takes tag equality; no
        // column has such a type, because every geohash type carries its bits.
        final StringBuilder mismatches = new StringBuilder();
        for (int t = 0; t < TYPES.length; t++) {
            for (int s = 0; s < TYPES.length; s++) {
                final int target = TYPES[t];
                final int source = TYPES[s];
                final short targetTag = ColumnType.tagOf(target);
                final boolean isExact = ColumnType.isDecimal(target)
                        || ColumnType.isGeoHash(target)
                        || targetTag == ColumnType.ARRAY
                        || targetTag == ColumnType.TIMESTAMP
                        || targetTag == ColumnType.INTERVAL;
                final boolean listed = isExact ? target == source : targetTag == ColumnType.tagOf(source);
                if (listed != (target == source)) {
                    mismatches.append(LABELS[t]).append(" <- ").append(LABELS[s]).append('\n');
                }
            }
        }
        Assert.assertEquals(
                """
                        GEOBYTE <- GEOHASH(1c)
                        GEOSHORT <- GEOHASH(8b)
                        GEOINT <- GEOHASH(31b)
                        GEOLONG <- GEOHASH(12c)
                        """,
                mismatches.toString()
        );
    }

    @Test
    public void testIntegral() {
        // SqlOptimiser's cadence seed and simple integer column, SubsampleValidator's stride and
        // target point count, WhereClauseParser's integer bound
        assertTypes("BYTE SHORT INT LONG", ColumnType::isIntegral);
    }

    @Test
    public void testIntegralOrFloat() {
        // SqlOptimiser's sdt compdev, SubsampleValidator's value column
        assertTypes("BYTE SHORT INT LONG FLOAT DOUBLE", ColumnType::isIntegralOrFloat);
    }

    @Test
    public void testJitGenuineIntegerLeaf() throws Exception {
        assertTypes("INT LONG DATE TIMESTAMP TIMESTAMP_NS", fact(CompiledFilterIRSerializer.class, "isGenuineIntegerType"));
    }

    @Test
    public void testJitWidthSensitiveInKey() throws Exception {
        assertTypes("BYTE SHORT INT", fact(CompiledFilterIRSerializer.class, "isWidthSensitiveType"));
    }

    @Test
    public void testRostiKeyNull() {
        // the vectorized GROUP BY key slot starts as the key's NULL: INT_NULL for an INT key,
        // VALUE_IS_NULL for a SYMBOL key
        Assert.assertEquals(Numbers.INT_NULL, (int) ColumnType.getTypeDriver(ColumnType.INT).getNullAsLong());
        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, (int) ColumnType.getTypeDriver(ColumnType.SYMBOL).getNullAsLong());
        Assert.assertEquals(NullPolicy.SENTINEL, ColumnType.getTypeDriver(ColumnType.INT).getNullPolicy());
        Assert.assertEquals(NullPolicy.SENTINEL, ColumnType.getTypeDriver(ColumnType.SYMBOL).getNullPolicy());
    }

    @Test
    public void testSumRewriteCountsTheColumn() {
        // sum(x + c) becomes sum(x) + count(x) * c for an integer x with NULLs, and
        // sum(x) + count(*) * c for one without
        assertTypes(
                "INT LONG",
                type -> ColumnType.isIntegral(type) && ColumnType.getTypeDriver(type).getNullPolicy() == NullPolicy.SENTINEL
        );
    }

    @Test
    public void testWhereKeyColumn() throws Exception {
        assertTypes("INT LONG STRING SYMBOL VARCHAR", fact(WhereClauseParser.class, "isKeyColumnType"));
    }

    @Test
    public void testWhereTimestampBound() throws Exception {
        assertTypes(
                "INT LONG DATE TIMESTAMP STRING SYMBOL VARCHAR TIMESTAMP_NS",
                fact(WhereClauseParser.class, "canCastToTimestamp")
        );
    }

    private static void assertTypes(String expected, Fact fact) {
        final StringBuilder actual = new StringBuilder();
        for (int i = 0; i < TYPES.length; i++) {
            final boolean answer;
            try {
                answer = fact.test(TYPES[i]);
            } catch (Exception e) {
                throw new AssertionError(LABELS[i] + " threw", e);
            }
            if (answer) {
                if (!actual.isEmpty()) {
                    actual.append(' ');
                }
                actual.append(LABELS[i]);
            }
        }
        Assert.assertEquals(expected, actual.toString());
    }

    private static Fact fact(Class<?> owner, String name) throws NoSuchMethodException {
        final Method method = owner.getDeclaredMethod(name, int.class);
        method.setAccessible(true);
        return type -> (boolean) method.invoke(null, type);
    }

    @FunctionalInterface
    private interface Fact {
        boolean test(int type) throws Exception;
    }

    static {
        final int[] extraTypes = {
                ColumnType.TIMESTAMP_NANO,
                ColumnType.getGeoHashTypeWithBits(5),
                ColumnType.getGeoHashTypeWithBits(8),
                ColumnType.getGeoHashTypeWithBits(31),
                ColumnType.getGeoHashTypeWithBits(60),
                ColumnType.getDecimalType(5, 2),
                ColumnType.getDecimalType(18, 3),
                ColumnType.encodeArrayType(ColumnType.DOUBLE, 1),
                ColumnType.encodeArrayType(ColumnType.DOUBLE, 2),
                ColumnType.INTERVAL_TIMESTAMP_MICRO,
                ColumnType.INTERVAL_TIMESTAMP_NANO,
        };
        final String[] extraLabels = {
                "TIMESTAMP_NS",
                "GEOHASH(1c)",
                "GEOHASH(8b)",
                "GEOHASH(31b)",
                "GEOHASH(12c)",
                "DECIMAL(5,2)",
                "DECIMAL(18,3)",
                "DOUBLE[]",
                "DOUBLE[][]",
                "INTERVAL(us)",
                "INTERVAL(ns)",
        };
        TYPES = new int[ColumnType.MAX_TAG + 1 + extraTypes.length];
        LABELS = new String[TYPES.length];
        // tags are labelled by their ColumnType constant name: nameOf() says "unknown" for
        // UNDEFINED, the four GEO* tags and the six DECIMAL<n> tags
        for (Field field : ColumnType.class.getFields()) {
            final int mods = field.getModifiers();
            if (field.getType() != short.class || !Modifier.isStatic(mods) || !Modifier.isFinal(mods) || "MAX_TAG".equals(field.getName())) {
                continue;
            }
            try {
                final short tag = field.getShort(null);
                if (tag >= 0 && tag <= ColumnType.MAX_TAG) {
                    TYPES[tag] = tag;
                    LABELS[tag] = field.getName();
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
        for (int i = 0; i < extraTypes.length; i++) {
            TYPES[ColumnType.MAX_TAG + 1 + i] = extraTypes[i];
            LABELS[ColumnType.MAX_TAG + 1 + i] = extraLabels[i];
        }
    }
}
