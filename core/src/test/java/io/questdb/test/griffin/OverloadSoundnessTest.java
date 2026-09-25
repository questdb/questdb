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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Overload resolution is sound when a column of any type reaches a function only through a
 * signature that names its type, or through a row of {@code ColumnType}'s overload priority
 * table that declares the widening, and the factory then receives either the column itself
 * (whose getter for the wider type is implemented) or an explicit cast the parser inserted.
 * Anything else would be a fuzzy pass-through: a value of one type handed to a function
 * written for another, with no declaration anywhere that this is intended.
 * <p>
 * The tests register one single-argument factory per signature character and resolve
 * {@code f(x)} for a column {@code x} of every column type, once with every factory present
 * and once with the type's own factory withheld. The second run is the one that matters for a
 * type that has no functions of its own yet: it must resolve by a declared row or fail, never
 * land somewhere by accident.
 */
public class OverloadSoundnessTest extends BaseFunctionFactoryTest {
    private static final String ARRAY_SIGNATURE = "D[]";
    private static int lastArgType;

    @Test
    public void testDeclaredWideningRowsAreImplemented() throws Exception {
        // for every (from, to) the overload table admits and a signature can name, the getter a
        // factory declared for `to` calls must be implemented by a function of type `from`; the
        // parser passes the function through unchanged unless it inserts one of its explicit
        // casts, which are pinned in testResolutionWithoutOwnSignature
        assertMemoryLeak(() -> {
            final StringSink unimplemented = new StringSink();
            for (ColumnTypeTag from : ColumnTypeTag.values()) {
                final int fromType = columnTypeOf(from);
                if (fromType == -1) {
                    continue;
                }
                final Function fromFunction = Constants.getNullConstant(fromType);
                for (ColumnTypeTag to : ColumnTypeTag.values()) {
                    if (to == from
                            || FunctionFactoryDescriptor.signatureChar(to) == FunctionFactoryDescriptor.NO_SIGNATURE_CHAR
                            || ColumnType.overloadDistance(from.code(), to.code()) == ColumnType.OVERLOAD_NONE
                            || isExplicitCast(from, to)
                            || isFamilySignature(from, to)
                    ) {
                        continue;
                    }
                    try {
                        callGetter(to, fromFunction);
                    } catch (UnsupportedOperationException e) {
                        unimplemented.put(from.name()).put(" -> ").put(to.name()).put('\n');
                    } catch (ImplicitCastException ignore) {
                        // the getter converts and rejected the NULL value (CHAR to a number): implemented
                    }
                }
            }
            // IPv4Function.getVarcharA throws; the parser casts IPv4 only for a STRING signature,
            // so a function with a VARCHAR overload and no STRING or IPv4 one would fail at
            // runtime. No such single-argument function exists today. Pre-existing; see
            // issues/ipv4-varchar-overload-row-unimplemented.md
            TestUtils.assertEquals("IPv4 -> VARCHAR\n", unimplemented);
        });
    }

    @Test
    public void testOwnSignatureWins() throws Exception {
        assertMemoryLeak(() -> {
            for (ColumnTypeTag tag : ColumnTypeTag.values()) {
                final int columnType = columnTypeOf(tag);
                if (columnType == -1) {
                    continue;
                }
                functions.clear();
                addFactories(null);
                final FunctionParser parser = createFunctionParser();
                try (Function f = parseFunction("f(x)", metadataOf(columnType), parser)) {
                    Assert.assertEquals(tag.name(), ownSignatureTag(tag), f.getLong(null));
                    Assert.assertEquals(tag.name(), columnType, lastArgType);
                }
            }
        });
    }

    @Test
    public void testResolutionWithoutOwnSignature() throws Exception {
        // Each line: the column type, the signature it resolved to when its own was withheld and
        // the type of the argument the factory received, or the error. The property: a signature
        // it resolved to is in the column type's overload row.
        assertMemoryLeak(() -> {
            final StringSink table = new StringSink();
            for (ColumnTypeTag tag : ColumnTypeTag.values()) {
                final int columnType = columnTypeOf(tag);
                if (columnType == -1) {
                    continue;
                }
                functions.clear();
                addFactories(tag);
                final FunctionParser parser = createFunctionParser();
                table.put(tag.name()).put(" -> ");
                try (Function f = parseFunction("f(x)", metadataOf(columnType), parser)) {
                    final ColumnTypeTag to = ColumnTypeTag.of((int) f.getLong(null));
                    table.put(to.name()).put(" (arg ").put(ColumnType.nameOf(lastArgType)).put(")\n");
                    Assert.assertNotEquals(
                            tag + " resolved to " + to + " without a declared overload row",
                            ColumnType.OVERLOAD_NONE,
                            ColumnType.overloadDistance(tag.code(), to.code())
                    );
                    Assert.assertEquals(
                            tag + " -> " + to + ": argument neither passed through nor cast to the signature type",
                            isExplicitCast(tag, to) ? to.code() : columnType,
                            isExplicitCast(tag, to) ? ColumnType.tagOf(lastArgType) : lastArgType
                    );
                } catch (SqlException e) {
                    table.put("! ").put(e.getFlyweightMessage()).put('\n');
                }
            }
            TestUtils.assertEquals(
                    """
                            BOOLEAN -> ! there is no matching function `f` with the argument types: (BOOLEAN)
                            BYTE -> SHORT (arg BYTE)
                            SHORT -> INT (arg SHORT)
                            CHAR -> STRING (arg CHAR)
                            INT -> LONG (arg INT)
                            LONG -> DOUBLE (arg LONG)
                            DATE -> TIMESTAMP (arg DATE)
                            TIMESTAMP -> LONG (arg TIMESTAMP)
                            FLOAT -> DOUBLE (arg FLOAT)
                            DOUBLE -> ! there is no matching function `f` with the argument types: (DOUBLE)
                            STRING -> VARCHAR (arg STRING)
                            SYMBOL -> STRING (arg SYMBOL)
                            LONG256 -> LONG (arg LONG256)
                            GEOBYTE -> ! there is no matching function `f` with the argument types: (GEOHASH(1c))
                            GEOSHORT -> ! there is no matching function `f` with the argument types: (GEOHASH(3c))
                            GEOINT -> ! there is no matching function `f` with the argument types: (GEOHASH(6c))
                            GEOLONG -> ! there is no matching function `f` with the argument types: (GEOHASH(12c))
                            BINARY -> ! there is no matching function `f` with the argument types: (BINARY)
                            UUID -> STRING (arg STRING)
                            LONG128 -> ! there is no matching function `f` with the argument types: (LONG128)
                            IPv4 -> STRING (arg STRING)
                            VARCHAR -> STRING (arg VARCHAR)
                            ARRAY -> ! there is no matching function `f` with the argument types: (DOUBLE[])
                            DECIMAL8 -> ! there is no matching function `f` with the argument types: (DECIMAL(2,0))
                            DECIMAL16 -> ! there is no matching function `f` with the argument types: (DECIMAL(4,1))
                            DECIMAL32 -> ! there is no matching function `f` with the argument types: (DECIMAL(9,2))
                            DECIMAL64 -> ! there is no matching function `f` with the argument types: (DECIMAL(18,3))
                            DECIMAL128 -> ! there is no matching function `f` with the argument types: (DECIMAL(38,4))
                            DECIMAL256 -> ! there is no matching function `f` with the argument types: (DECIMAL(76,5))
                            INTERVAL -> STRING (arg STRING)
                            """,
                    table
            );
        });
    }

    /**
     * Registers {@code f(<C>)} for every signature character and {@code f(D[])} for arrays,
     * except the signature that names {@code withheld}; each factory yields the tag of its
     * signature and records the type of the argument it received.
     */
    private static void addFactories(ColumnTypeTag withheld) {
        final long withheldSignatureTag = withheld == null ? -1 : ownSignatureTag(withheld);
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            final char c = FunctionFactoryDescriptor.signatureChar(tag);
            if (c == FunctionFactoryDescriptor.NO_SIGNATURE_CHAR || tag == ColumnTypeTag.VAR_ARG || tag.code() == withheldSignatureTag) {
                continue;
            }
            functions.add(factoryOf("f(" + Character.toUpperCase(c) + ")", tag.code()));
        }
        if (withheldSignatureTag != ColumnType.ARRAY) {
            functions.add(factoryOf("f(" + ARRAY_SIGNATURE + ")", ColumnType.ARRAY));
        }
    }

    private static void callGetter(ColumnTypeTag to, Function function) {
        // how a factory declared for `to` reads its argument; only tags with a signature character.
        // A switch expression, so that javac demands an arm for a new tag; the value is not used.
        final Object ignore = switch (to) {
            case BOOLEAN -> function.getBool(null);
            case BYTE -> function.getByte(null);
            case SHORT -> function.getShort(null);
            case CHAR -> function.getChar(null);
            case INT -> function.getInt(null);
            case LONG -> function.getLong(null);
            case DATE -> function.getDate(null);
            case TIMESTAMP -> function.getTimestamp(null);
            case FLOAT -> function.getFloat(null);
            case DOUBLE -> function.getDouble(null);
            case STRING -> function.getStrA(null);
            case SYMBOL -> function.getSymbol(null);
            case LONG256 -> function.getLong256A(null);
            case BINARY -> function.getBin(null);
            case UUID, LONG128 -> function.getLong128Lo(null) + function.getLong128Hi(null);
            case IPv4 -> function.getIPv4(null);
            case VARCHAR -> function.getVarcharA(null);
            case INTERVAL -> function.getInterval(null);
            case CURSOR -> function.getRecordCursorFactory();
            // GEOHASH and DECIMAL signatures are family matches, read by the argument's own width
            case GEOHASH, DECIMAL, RECORD, NULL, REGCLASS, REGPROCEDURE, ARRAY_STRING, VAR_ARG, UNDEFINED, GEOBYTE,
                 GEOSHORT, GEOINT, GEOLONG, ARRAY, DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256,
                 PARAMETER, VARCHAR_SLICE, UNKNOWN ->
                    throw new IllegalArgumentException("no widening row leads to " + to);
        };
    }

    /**
     * A representative column type per tag, or -1 for a tag that is not a column type.
     */
    private static int columnTypeOf(ColumnTypeTag tag) {
        return switch (tag) {
            case BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, FLOAT, DOUBLE, STRING, SYMBOL, LONG256, BINARY, UUID,
                 LONG128, IPv4, VARCHAR -> tag.code();
            case TIMESTAMP -> ColumnType.TIMESTAMP_MICRO;
            case GEOBYTE -> ColumnType.getGeoHashTypeWithBits(5);
            case GEOSHORT -> ColumnType.getGeoHashTypeWithBits(15);
            case GEOINT -> ColumnType.getGeoHashTypeWithBits(30);
            case GEOLONG -> ColumnType.getGeoHashTypeWithBits(60);
            case ARRAY -> ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
            case DECIMAL8 -> ColumnType.getDecimalType(2, 0);
            case DECIMAL16 -> ColumnType.getDecimalType(4, 1);
            case DECIMAL32 -> ColumnType.getDecimalType(9, 2);
            case DECIMAL64 -> ColumnType.getDecimalType(18, 3);
            case DECIMAL128 -> ColumnType.getDecimalType(38, 4);
            case DECIMAL256 -> ColumnType.getDecimalType(76, 5);
            case INTERVAL -> ColumnType.INTERVAL_TIMESTAMP_MICRO;
            case UNDEFINED, CURSOR, VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER,
                 VARCHAR_SLICE, NULL, UNKNOWN -> -1;
        };
    }

    private static FunctionFactory factoryOf(String signature, long signatureTag) {
        return new FunctionFactory() {
            @Override
            public String getSignature() {
                return signature;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                lastArgType = args.getQuick(0).getType();
                return LongConstant.newInstance(signatureTag);
            }
        };
    }

    /**
     * The (from, to) pairs for which {@code FunctionParser.createFunction} replaces a
     * non-constant argument by a cast to the signature type instead of passing it through.
     */
    private static boolean isExplicitCast(ColumnTypeTag from, ColumnTypeTag to) {
        return switch (from) {
            case UUID, IPv4, INTERVAL -> to == ColumnTypeTag.STRING;
            case BYTE, SHORT, INT, LONG -> to == ColumnTypeTag.DECIMAL;
            case UNDEFINED, BOOLEAN, CHAR, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL, LONG256, GEOBYTE, GEOSHORT,
                 GEOINT, GEOLONG, BINARY, CURSOR, VAR_ARG, RECORD, GEOHASH, LONG128, VARCHAR, ARRAY, DECIMAL8,
                 DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL, REGCLASS, REGPROCEDURE,
                 ARRAY_STRING, PARAMETER, VARCHAR_SLICE, NULL, UNKNOWN -> false;
        };
    }

    /**
     * A geohash or decimal storage tag matches its family's signature character exactly, and
     * the factory then reads the argument by its actual width; not a widening.
     */
    private static boolean isFamilySignature(ColumnTypeTag from, ColumnTypeTag to) {
        return switch (from) {
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG -> to == ColumnTypeTag.GEOHASH;
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 -> to == ColumnTypeTag.DECIMAL;
            case UNDEFINED, BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL,
                 LONG256, BINARY, UUID, CURSOR, VAR_ARG, RECORD, GEOHASH, LONG128, IPv4, VARCHAR, ARRAY, DECIMAL,
                 REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER, INTERVAL, VARCHAR_SLICE, NULL, UNKNOWN -> false;
        };
    }

    private static GenericRecordMetadata metadataOf(int columnType) {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        if (ColumnType.isSymbol(columnType)) {
            metadata.add(new TableColumnMetadata("x", columnType, IndexType.NONE, 0, true, null));
        } else {
            metadata.add(new TableColumnMetadata("x", columnType));
        }
        return metadata;
    }

    /**
     * The tag of the signature that names a column of {@code tag}: its own character, the
     * family character for geohash and decimal storage tags, {@code D[]} for arrays.
     */
    private static long ownSignatureTag(ColumnTypeTag tag) {
        return switch (tag) {
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG -> ColumnType.GEOHASH;
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 -> ColumnType.DECIMAL;
            case ARRAY -> ColumnType.ARRAY;
            case BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL, LONG256, BINARY,
                 UUID, LONG128, IPv4, VARCHAR, INTERVAL, CURSOR, VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS,
                 REGPROCEDURE, ARRAY_STRING, NULL -> tag.code();
            case UNDEFINED, PARAMETER, VARCHAR_SLICE, UNKNOWN ->
                    throw new IllegalArgumentException("no signature: " + tag);
        };
    }
}
