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
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.UpdateOperatorImpl;
import io.questdb.griffin.engine.functions.conditional.CaseCommon;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByColumnSink;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Golden truth tables for the pairwise relations between column types. Each table was
 * generated once from the implementation and committed as the expected value; the tests
 * compare the current output against it character by character.
 * <p>
 * The tables freeze today's behaviour, including its quirks, so that a refactor of the
 * relation code can be checked for behaviour preservation. A diff in any table is a
 * behaviour change to be explained, not a golden to be regenerated. Cells that encode a
 * known pre-existing bug carry the bug id in a comment next to the table (PB6 below).
 * <p>
 * The type set is every tag from 0 to {@link ColumnType#MAX_TAG} in tag order, followed by a
 * few encoded types (timestamp precision, geohash bits, decimal precision and scale, array
 * dimensionality, interval kind) that the relations inspect beyond the tag. Row labels double
 * as the column legend: column {@code k} is the type on row {@code k}. Adding a tag grows the
 * set, so every table then reports the new row and column.
 * <p>
 * Cell notation: {@code X} true, {@code .} false, {@code !} the call threw, an integer the
 * index into the type set of the returned type, {@code -} a returned -1, {@code #} a returned
 * type outside the set, spelled out in a note under the table. The overload table is sparse:
 * each row lists {@code name=distance} for every cell that is not {@code OVERLOAD_NONE},
 * where -1 is {@code OVERLOAD_FULL}. The CASE cast table is sparse the same way: each row
 * lists {@code name=factory} for every cell with a cast factory.
 */
public class TypeRelationGoldenTest {
    private static final String[] LABELS;
    private static final int[] TYPES;

    @Test
    public void testColumnConversionSupport() throws Exception {
        // ALTER TABLE ... ALTER COLUMN ... TYPE: SqlCompilerImpl.isCompatibleColumnTypeChange
        final Field field = SqlCompilerImpl.class.getDeclaredField("columnConversionSupport");
        field.setAccessible(true);
        final boolean[][] support = (boolean[][]) field.get(null);
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     .....................................................
                         1 BOOLEAN       .XXX.XXXXXXXX.............X...............X..........
                         2 BYTE          .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                         3 SHORT         .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                         4 CHAR          ...........XX.............X..........................
                         5 INT           .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                         6 LONG          .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                         7 DATE          .XXX.XXXXXXXX.............X...............X..........
                         8 TIMESTAMP     .XXX.XXXXXXXX.............X...............X..........
                         9 FLOAT         .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                        10 DOUBLE        .XXX.XXXXXXXX.............X.XXXXXX........X....XX....
                        11 STRING        .XXXXXXXXXXXX......X.....XX.XXXXXX........X....XX....
                        12 SYMBOL        .XXXXXXXXXXXX......X.....XX...............X..........
                        13 LONG256       .....................................................
                        14 GEOBYTE       .....................................................
                        15 GEOSHORT      .....................................................
                        16 GEOINT        .....................................................
                        17 GEOLONG       .....................................................
                        18 BINARY        .....................................................
                        19 UUID          ...........XX.............X..........................
                        20 CURSOR        .....................................................
                        21 VAR_ARG       .....................................................
                        22 RECORD        .....................................................
                        23 GEOHASH       .....................................................
                        24 LONG128       .....................................................
                        25 IPv4          ...........XX.............X..........................
                        26 VARCHAR       .XXXXXXXXXXXX......X.....XX.XXXXXX........X....XX....
                        27 ARRAY         .....................................................
                        28 DECIMAL8      .........XXX..............X.XXXXXX.............XX....
                        29 DECIMAL16     .........XXX..............X.XXXXXX.............XX....
                        30 DECIMAL32     .........XXX..............X.XXXXXX.............XX....
                        31 DECIMAL64     .........XXX..............X.XXXXXX.............XX....
                        32 DECIMAL128    .........XXX..............X.XXXXXX.............XX....
                        33 DECIMAL256    .........XXX..............X.XXXXXX.............XX....
                        34 DECIMAL       .....................................................
                        35 REGCLASS      .....................................................
                        36 REGPROCEDURE  .....................................................
                        37 ARRAY_STRING  .....................................................
                        38 PARAMETER     .....................................................
                        39 INTERVAL      .....................................................
                        40 VARCHAR_SLICE .....................................................
                        41 NULL          .....................................................
                        42 TIMESTAMP_NS  .XXX.XXXXXXXX.............X...............X..........
                        43 GEOHASH(1c)   .....................................................
                        44 GEOHASH(8b)   .....................................................
                        45 GEOHASH(31b)  .....................................................
                        46 GEOHASH(12c)  .....................................................
                        47 DECIMAL(5,2)  .........XXX..............X.XXXXXX.............XX....
                        48 DECIMAL(18,3) .........XXX..............X.XXXXXX.............XX....
                        49 DOUBLE[]      .....................................................
                        50 DOUBLE[][]    .....................................................
                        51 INTERVAL(us)  .....................................................
                        52 INTERVAL(ns)  .....................................................
                        """,
                renderBoolean((from, to) -> support[ColumnType.tagOf(from)][ColumnType.tagOf(to)])
        );
    }

    @Test
    public void testCaseCastFactory() {
        // CASE / SWITCH / COALESCE: the factory that wraps a branch of the row type so that it
        // reads as the column type; a blank cell hands the branch back unchanged. Keyed by
        // encoded type, so the TIMESTAMP_NS row is empty and TIMESTAMP_NS is not a target.
        assertGolden(
                """
                         0 UNDEFINED
                         1 BOOLEAN        LONG256=CastBooleanToLong256FunctionFactory
                         2 BYTE           CHAR=CastByteToCharFunctionFactory DATE=CastByteToDateFunctionFactory TIMESTAMP=CastByteToTimestampFunctionFactory STRING=CastByteToStrFunctionFactory SYMBOL=CastByteToSymbolFunctionFactory LONG256=CastByteToLong256FunctionFactory VARCHAR=CastByteToVarcharFunctionFactory
                         3 SHORT          DATE=CastShortToDateFunctionFactory TIMESTAMP=CastShortToTimestampFunctionFactory STRING=CastShortToStrFunctionFactory SYMBOL=CastShortToSymbolFunctionFactory LONG256=CastShortToLong256FunctionFactory VARCHAR=CastShortToVarcharFunctionFactory
                         4 CHAR           DATE=CastCharToDateFunctionFactory TIMESTAMP=CastCharToTimestampFunctionFactory STRING=CastCharToStrFunctionFactory SYMBOL=CastCharToSymbolFunctionFactory LONG256=CastCharToLong256FunctionFactory VARCHAR=CastCharToVarcharFunctionFactory
                         5 INT            BYTE=CastIntToByteFunctionFactory SHORT=CastIntToShortFunctionFactory STRING=CastIntToStrFunctionFactory SYMBOL=CastIntToSymbolFunctionFactory LONG256=CastIntToLong256FunctionFactory IPv4=CastIntToIPv4FunctionFactory VARCHAR=CastIntToVarcharFunctionFactory
                         6 LONG           BYTE=CastLongToByteFunctionFactory SHORT=CastLongToShortFunctionFactory INT=CastLongToIntFunctionFactory STRING=CastLongToStrFunctionFactory SYMBOL=CastLongToSymbolFunctionFactory LONG256=CastLongToLong256FunctionFactory VARCHAR=CastLongToVarcharFunctionFactory
                         7 DATE           STRING=CastDateToStrFunctionFactory SYMBOL=CastDateToSymbolFunctionFactory LONG256=CastDateToLong256FunctionFactory VARCHAR=CastDateToVarcharFunctionFactory
                         8 TIMESTAMP      STRING=CastTimestampToStrFunctionFactory SYMBOL=CastTimestampToSymbolFunctionFactory LONG256=CastTimestampToLong256FunctionFactory VARCHAR=CastTimestampToVarcharFunctionFactory
                         9 FLOAT          DATE=CastFloatToDateFunctionFactory STRING=CastFloatToStrFunctionFactory SYMBOL=CastFloatToSymbolFunctionFactory LONG256=CastFloatToLong256FunctionFactory VARCHAR=CastFloatToVarcharFunctionFactory
                        10 DOUBLE         STRING=CastDoubleToStrFunctionFactory SYMBOL=CastDoubleToSymbolFunctionFactory LONG256=CastDoubleToLong256FunctionFactory VARCHAR=CastDoubleToVarcharFunctionFactory
                        11 STRING         UUID=CastStrToUuidFunctionFactory IPv4=CastStrToIPv4FunctionFactory
                        12 SYMBOL
                        13 LONG256        STRING=CastLong256ToStrFunctionFactory SYMBOL=CastLong256ToSymbolFunctionFactory VARCHAR=CastLong256ToVarcharFunctionFactory
                        14 GEOBYTE
                        15 GEOSHORT
                        16 GEOINT
                        17 GEOLONG
                        18 BINARY
                        19 UUID           STRING=CastUuidToStrFunctionFactory VARCHAR=CastUuidToVarcharFunctionFactory
                        20 CURSOR
                        21 VAR_ARG
                        22 RECORD
                        23 GEOHASH
                        24 LONG128
                        25 IPv4           INT=CastIPv4ToIntFunctionFactory STRING=CastIPv4ToStrFunctionFactory VARCHAR=CastIPv4ToVarcharFunctionFactory
                        26 VARCHAR        UUID=CastVarcharToUuidFunctionFactory IPv4=CastVarcharToIPv4FunctionFactory
                        27 ARRAY
                        28 DECIMAL8
                        29 DECIMAL16
                        30 DECIMAL32
                        31 DECIMAL64
                        32 DECIMAL128
                        33 DECIMAL256
                        34 DECIMAL
                        35 REGCLASS
                        36 REGPROCEDURE
                        37 ARRAY_STRING
                        38 PARAMETER
                        39 INTERVAL
                        40 VARCHAR_SLICE
                        41 NULL
                        42 TIMESTAMP_NS
                        43 GEOHASH(1c)
                        44 GEOHASH(8b)
                        45 GEOHASH(31b)
                        46 GEOHASH(12c)
                        47 DECIMAL(5,2)
                        48 DECIMAL(18,3)
                        49 DOUBLE[]
                        50 DOUBLE[][]
                        51 INTERVAL(us)
                        52 INTERVAL(ns)
                        """,
                renderSparse((from, to) -> {
                    final FunctionFactory factory = CaseCommon.getCastFactory(from, to);
                    return factory == null ? null : factory.getClass().getSimpleName();
                })
        );
    }

    @Test
    public void testCaseCommonType() {
        // CASE / SWITCH / COALESCE: the type the expression takes when the branches so far
        // have the row type and the next branch has the column type; ! is "inconvertible"
        assertGolden(
                """
                                                                         1  1  1  1  1  1  1  1  1  1  2  2  2  2  2  2  2  2  2  2  3  3  3  3  3  3  3  3  3  3  4  4  4  4  4  4  4  4  4  4  5  5  5
                                           0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2
                         0 UNDEFINED       !  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26  ! 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52
                         1 BOOLEAN         !  1  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  1  !  !  !  !  !  !  !  !  !  !  !
                         2 BYTE            !  !  2  3  !  5  6  !  !  9 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  #  #  #  #  #  #  #  !  !  !  !  !  !  2  !  !  !  !  ! 47 48  !  !  !  !
                         3 SHORT           !  !  3  3  !  5  6  !  !  9 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  #  #  #  #  #  #  #  !  !  !  !  !  !  3  !  !  !  !  !  # 48  !  !  !  !
                         4 CHAR            !  !  !  !  4  !  !  !  !  !  ! 11 12  !  !  !  !  !  !  !  !  !  !  !  !  ! 26  !  !  !  !  !  !  !  !  !  !  !  !  !  !  4  !  !  !  !  !  !  !  !  !  !  !
                         5 INT             !  !  5  5  !  5  6  !  !  9 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  #  #  #  #  #  #  #  !  !  !  !  !  !  5  !  !  !  !  !  # 48  !  !  !  !
                         6 LONG            !  !  6  6  !  6  6  !  !  9 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  #  #  #  #  #  #  #  !  !  !  !  !  !  6  !  !  !  !  !  #  #  !  !  !  !
                         7 DATE            !  !  !  !  !  !  !  7  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  7  !  !  !  !  !  !  !  !  !  !  !
                         8 TIMESTAMP       !  !  !  !  !  !  !  !  8  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  8 42  !  !  !  !  !  !  !  !  !  !
                         9 FLOAT           !  !  9  9  !  9  9  !  !  9 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  9  !  !  !  !  !  !  !  !  !  !  !
                        10 DOUBLE          !  ! 10 10  ! 10 10  !  ! 10 10  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 10  !  !  !  !  !  !  !  !  !  !  !
                        11 STRING          !  !  !  ! 11  !  !  !  !  !  ! 11 11  !  !  !  !  !  ! 19  !  !  !  !  ! 25 26  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 11  !  !  !  !  !  !  !  !  !  !  !
                        12 SYMBOL          !  !  !  ! 11  !  !  !  !  !  ! 11 12  !  !  !  !  !  !  !  !  !  !  !  !  ! 26  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 12  !  !  !  !  !  !  !  !  !  !  !
                        13 LONG256         !  !  !  !  !  !  !  !  !  !  !  !  ! 13  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 13  !  !  !  !  !  !  !  !  !  !  !
                        14 GEOBYTE         !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 14  !  !  !  !  !  !  !  !  !  !  !
                        15 GEOSHORT        !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 15  !  !  !  !  !  !  !  !  !  !  !
                        16 GEOINT          !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 16  !  !  !  !  !  !  !  !  !  !  !
                        17 GEOLONG         !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 17  !  !  !  !  !  !  !  !  !  !  !
                        18 BINARY          !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 18  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 18  !  !  !  !  !  !  !  !  !  !  !
                        19 UUID            !  !  !  !  !  !  !  !  !  !  ! 19  !  !  !  !  !  !  ! 19  !  !  !  !  !  ! 19  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 19  !  !  !  !  !  !  !  !  !  !  !
                        20 CURSOR          !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 20  !  !  !  !  !  !  !  !  !  !  !
                        21 VAR_ARG         !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 21  !  !  !  !  !  !  !  !  !  !  !
                        22 RECORD          !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 22  !  !  !  !  !  !  !  !  !  !  !
                        23 GEOHASH         !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 23  !  !  !  !  !  !  !  !  !  !  !
                        24 LONG128         !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 24  !  !  !  !  !  !  !  !  !  !  !
                        25 IPv4            !  !  !  !  !  !  !  !  !  !  ! 25  !  !  !  !  !  !  !  !  !  !  !  !  ! 25 25  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 25  !  !  !  !  !  !  !  !  !  !  !
                        26 VARCHAR         !  !  !  ! 26  !  !  !  !  !  ! 26 26  !  !  !  !  !  ! 19  !  !  !  !  ! 25 26  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 26  !  !  !  !  !  !  !  !  !  !  !
                        27 ARRAY           !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 27  !  !  !  !  !  !  !  !  !  !  !
                        28 DECIMAL8        !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 28  !  !  !  !  !  !  !  !  !  !  !  ! 28  !  !  !  !  ! 47 48  !  !  !  !
                        29 DECIMAL16       !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 29  !  !  !  !  !  !  !  !  !  !  ! 29  !  !  !  !  ! 47 48  !  !  !  !
                        30 DECIMAL32       !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 30  !  !  !  !  !  !  !  !  !  ! 30  !  !  !  !  ! 47 48  !  !  !  !
                        31 DECIMAL64       !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 31  !  !  !  !  !  !  !  !  ! 31  !  !  !  !  ! 47 48  !  !  !  !
                        32 DECIMAL128      !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 32  !  !  !  !  !  !  !  ! 32  !  !  !  !  ! 47 48  !  !  !  !
                        33 DECIMAL256      !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 33  !  !  !  !  !  !  ! 33  !  !  !  !  ! 47 48  !  !  !  !
                        34 DECIMAL         !  !  #  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 34  !  !  !  !  !  ! 34  !  !  !  !  ! 47 48  !  !  !  !
                        35 REGCLASS        !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 35  !  !  !  !  !  !  !  !  !  !  !
                        36 REGPROCEDURE    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 36  !  !  !  !  !  !  !  !  !  !  !
                        37 ARRAY_STRING    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 37  !  !  !  !  !  !  !  !  !  !  !
                        38 PARAMETER       !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 38  !  !  !  !  !  !  !  !  !  !  !
                        39 INTERVAL        !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 39  !  !  !  !  !  !  !  !  !  !  !
                        40 VARCHAR_SLICE   !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 40  !  !  !  !  !  !  !  !  !  !  !
                        41 NULL            !  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26  ! 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52
                        42 TIMESTAMP_NS    !  !  !  !  !  !  !  ! 42  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 42 42  !  !  !  !  !  !  !  !  !  !
                        43 GEOHASH(1c)     !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 43  !  !  !  !  !  !  !  !  !  !  !
                        44 GEOHASH(8b)     !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 44  !  !  !  !  !  !  !  !  !  !  !
                        45 GEOHASH(31b)    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 45  !  !  !  !  !  !  !  !  !  !  !
                        46 GEOHASH(12c)    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 46  !  !  !  !  !  !  !  !  !  !  !
                        47 DECIMAL(5,2)    !  ! 47  #  !  #  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 47 47 47 47 47 47 47  !  !  !  !  !  ! 47  !  !  !  !  ! 47 48  !  !  !  !
                        48 DECIMAL(18,3)   !  ! 48 48  ! 48  #  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 48 48 48 48 48 48 48  !  !  !  !  !  ! 48  !  !  !  !  ! 48 48  !  !  !  !
                        49 DOUBLE[]        !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 49  !  !  !  !  !  !  ! 49  !  !  !
                        50 DOUBLE[][]      !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 50  !  !  !  !  !  !  !  ! 50  !  !
                        51 INTERVAL(us)    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 51  !  !  !  !  !  !  !  !  !  !  !
                        52 INTERVAL(ns)    !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  !  ! 52  !  !  !  !  !  !  !  !  !  !  !
                        # BYTE x DECIMAL8 -> DECIMAL(3,0)
                        # BYTE x DECIMAL16 -> DECIMAL(3,0)
                        # BYTE x DECIMAL32 -> DECIMAL(3,0)
                        # BYTE x DECIMAL64 -> DECIMAL(3,0)
                        # BYTE x DECIMAL128 -> DECIMAL(3,0)
                        # BYTE x DECIMAL256 -> DECIMAL(3,0)
                        # BYTE x DECIMAL -> DECIMAL(3,0)
                        # SHORT x DECIMAL8 -> DECIMAL(5,0)
                        # SHORT x DECIMAL16 -> DECIMAL(5,0)
                        # SHORT x DECIMAL32 -> DECIMAL(5,0)
                        # SHORT x DECIMAL64 -> DECIMAL(5,0)
                        # SHORT x DECIMAL128 -> DECIMAL(5,0)
                        # SHORT x DECIMAL256 -> DECIMAL(5,0)
                        # SHORT x DECIMAL -> DECIMAL(5,0)
                        # SHORT x DECIMAL(5,2) -> DECIMAL(7,2)
                        # INT x DECIMAL8 -> DECIMAL(10,0)
                        # INT x DECIMAL16 -> DECIMAL(10,0)
                        # INT x DECIMAL32 -> DECIMAL(10,0)
                        # INT x DECIMAL64 -> DECIMAL(10,0)
                        # INT x DECIMAL128 -> DECIMAL(10,0)
                        # INT x DECIMAL256 -> DECIMAL(10,0)
                        # INT x DECIMAL -> DECIMAL(10,0)
                        # INT x DECIMAL(5,2) -> DECIMAL(12,2)
                        # LONG x DECIMAL8 -> DECIMAL(19,0)
                        # LONG x DECIMAL16 -> DECIMAL(19,0)
                        # LONG x DECIMAL32 -> DECIMAL(19,0)
                        # LONG x DECIMAL64 -> DECIMAL(19,0)
                        # LONG x DECIMAL128 -> DECIMAL(19,0)
                        # LONG x DECIMAL256 -> DECIMAL(19,0)
                        # LONG x DECIMAL -> DECIMAL(19,0)
                        # LONG x DECIMAL(5,2) -> DECIMAL(21,2)
                        # LONG x DECIMAL(18,3) -> DECIMAL(22,3)
                        # DECIMAL8 x BYTE -> DECIMAL(3,0)
                        # DECIMAL8 x SHORT -> DECIMAL(5,0)
                        # DECIMAL8 x INT -> DECIMAL(10,0)
                        # DECIMAL8 x LONG -> DECIMAL(19,0)
                        # DECIMAL16 x BYTE -> DECIMAL(3,0)
                        # DECIMAL16 x SHORT -> DECIMAL(5,0)
                        # DECIMAL16 x INT -> DECIMAL(10,0)
                        # DECIMAL16 x LONG -> DECIMAL(19,0)
                        # DECIMAL32 x BYTE -> DECIMAL(3,0)
                        # DECIMAL32 x SHORT -> DECIMAL(5,0)
                        # DECIMAL32 x INT -> DECIMAL(10,0)
                        # DECIMAL32 x LONG -> DECIMAL(19,0)
                        # DECIMAL64 x BYTE -> DECIMAL(3,0)
                        # DECIMAL64 x SHORT -> DECIMAL(5,0)
                        # DECIMAL64 x INT -> DECIMAL(10,0)
                        # DECIMAL64 x LONG -> DECIMAL(19,0)
                        # DECIMAL128 x BYTE -> DECIMAL(3,0)
                        # DECIMAL128 x SHORT -> DECIMAL(5,0)
                        # DECIMAL128 x INT -> DECIMAL(10,0)
                        # DECIMAL128 x LONG -> DECIMAL(19,0)
                        # DECIMAL256 x BYTE -> DECIMAL(3,0)
                        # DECIMAL256 x SHORT -> DECIMAL(5,0)
                        # DECIMAL256 x INT -> DECIMAL(10,0)
                        # DECIMAL256 x LONG -> DECIMAL(19,0)
                        # DECIMAL x BYTE -> DECIMAL(3,0)
                        # DECIMAL x SHORT -> DECIMAL(5,0)
                        # DECIMAL x INT -> DECIMAL(10,0)
                        # DECIMAL x LONG -> DECIMAL(19,0)
                        # DECIMAL(5,2) x SHORT -> DECIMAL(7,2)
                        # DECIMAL(5,2) x INT -> DECIMAL(12,2)
                        # DECIMAL(5,2) x LONG -> DECIMAL(21,2)
                        # DECIMAL(18,3) x LONG -> DECIMAL(22,3)
                        """,
                renderType((from, to) -> CaseCommon.getCommonType(from, to, 0, "undefined"))
        );
    }

    @Test
    public void testCommonWideningType() {
        assertGolden(
                """
                                                                         1  1  1  1  1  1  1  1  1  1  2  2  2  2  2  2  2  2  2  2  3  3  3  3  3  3  3  3  3  3  4  4  4  4  4  4  4  4  4  4  5  5  5
                                           0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2
                         0 UNDEFINED       0 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  0 11 11 11 11 11 11 11 11 11 11 11
                         1 BOOLEAN        11  1 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  1 11 11 11 11 11 11 11 11 11 11 11
                         2 BYTE           11 11  2  3 11  5  6  7  8  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  2 42 11 11 11 11 11 11 11 11 11 11
                         3 SHORT          11 11  3  3  3  5  6  7  8  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  3 42 11 11 11 11 11 11 11 11 11 11
                         4 CHAR           11 11 11  3  4  5  6  7  8  9 10 11 11 11 14 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 42 43 11 11 11 11 11 11 11 11 11
                         5 INT            11 11  5  5  5  5  6  7  8  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  5 42 11 11 11 11 11 11 11 11 11 11
                         6 LONG           11 11  6  6  6  6  6  7  8  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  6 42 11 11 11 11 11 11 11 11 11 11
                         7 DATE           11 11  7  7  7  7  7  7  8  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  7 42 11 11 11 11 11 11 11 11 11 11
                         8 TIMESTAMP      11 11  8  8  8  8  8  8  8  9 10 11  8 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  8 42 11 11 11 11 11 11 11 11 11 11
                         9 FLOAT          11 11  9  9  9  9  9  9  9  9 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26  9  9 11 11 11 11 11 11 11 11 11 11
                        10 DOUBLE         11 11 10 10 10 10 10 10 10 10 10 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 10 10 11 11 11 11 11 11 11 11 11 11
                        11 STRING         11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11
                        12 SYMBOL         11 11 11 11 11 11 11 11  8 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 42 11 11 11 11 11 11 11 11 11 11
                        13 LONG256        11 11 11 11 11 11 11 11 11 11 11 11 11 13 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 13 11 11 11 11 11 11 11 11 11 11 11
                        14 GEOBYTE        11 11 11 11 14 11 11 11 11 11 11 11 11 11 14 14 14 14 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 14 11 14 14 14 14 11 11 11 11 11 11
                        15 GEOSHORT       11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 15 15 15 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 15 11 43 15 15 15 11 11 11 11 11 11
                        16 GEOINT         11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 15 16 16 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 16 11 43 44 16 16 11 11 11 11 11 11
                        17 GEOLONG        11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 15 16 17 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 17 11 43 44 45 17 11 11 11 11 11 11
                        18 BINARY         11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 18 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 18 11 11 11 11 11 11 11 11 11 11 11
                        19 UUID           11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 19 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 19 11 11 11 11 11 11 11 11 11 11 11
                        20 CURSOR         11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 20 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 20 11 11 11 11 11 11 11 11 11 11 11
                        21 VAR_ARG        11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 21 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 21 11 11 11 11 11 11 11 11 11 11 11
                        22 RECORD         11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 22 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 22 11 11 11 11 11 11 11 11 11 11 11
                        23 GEOHASH        11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 23 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 23 11 11 11 11 11 11 11 11 11 11 11
                        24 LONG128        11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 24 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 24 11 11 11 11 11 11 11 11 11 11 11
                        25 IPv4           11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 25 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 25 11 11 11 11 11 11 11 11 11 11 11
                        26 VARCHAR        26 26 26 26 26 26 26 26 26 26 26 11 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26
                        27 ARRAY          11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 27 11 11 11 11 11 11 11 11 11 11 11 11 26 27 11 11 11 11 11 11 11 11 11 11 11
                        28 DECIMAL8       11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 28 28 28 28 28 28 11 11 11 11 11 11 26 28 11 11 11 11 11 28 28 11 11 11 11
                        29 DECIMAL16      11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 29 29 29 29 29 29 11 11 11 11 11 11 26 29 11 11 11 11 11 29 29 11 11 11 11
                        30 DECIMAL32      11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 30 30 30 30 30 30 11 11 11 11 11 11 26 30 11 11 11 11 11 30 30 11 11 11 11
                        31 DECIMAL64      11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 31 31 31 31 31 31 11 11 11 11 11 11 26 31 11 11 11 11 11 31 31 11 11 11 11
                        32 DECIMAL128     11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 32 32 32 32 32 32 11 11 11 11 11 11 26 32 11 11 11 11 11 32 32 11 11 11 11
                        33 DECIMAL256     11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 33 33 33 33 33 33 11 11 11 11 11 11 26 33 11 11 11 11 11 33 33 11 11 11 11
                        34 DECIMAL        11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 34 11 11 11 11 11 26 34 11 11 11 11 11 11 11 11 11 11 11
                        35 REGCLASS       11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 35 11 11 11 11 26 35 11 11 11 11 11 11 11 11 11 11 11
                        36 REGPROCEDURE   11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 36 11 11 11 26 36 11 11 11 11 11 11 11 11 11 11 11
                        37 ARRAY_STRING   11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 37 11 11 26 37 11 11 11 11 11 11 11 11 11 11 11
                        38 PARAMETER      11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 38 11 26 38 11 11 11 11 11 11 11 11 11 11 11
                        39 INTERVAL       11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 39 26 39 11 11 11 11 11 11 11 11 11 39 39
                        40 VARCHAR_SLICE  26 26 26 26 26 26 26 26 26 26 26 11 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26
                        41 NULL            0  1  2  3 11  5  6  7  8  9 10 11 11 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 26 41 42 43 44 45 46 47 48 49 50 51 52
                        42 TIMESTAMP_NS   11 11 42 42 42 42 42 42 42  9 10 11 42 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 42 42 11 11 11 11 11 11 11 11 11 11
                        43 GEOHASH(1c)    11 11 11 11 43 11 11 11 11 11 11 11 11 11 43 43 43 43 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 43 11 43 43 43 43 11 11 11 11 11 11
                        44 GEOHASH(8b)    11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 44 44 44 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 44 11 43 44 44 44 11 11 11 11 11 11
                        45 GEOHASH(31b)   11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 15 45 45 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 45 11 43 44 45 45 11 11 11 11 11 11
                        46 GEOHASH(12c)   11 11 11 11 11 11 11 11 11 11 11 11 11 11 14 15 16 46 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 46 11 43 44 45 46 11 11 11 11 11 11
                        47 DECIMAL(5,2)   11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 47 47 47 47 47 47 11 11 11 11 11 11 26 47 11 11 11 11 11 47 47 11 11 11 11
                        48 DECIMAL(18,3)  11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 48 48 48 48 48 48 11 11 11 11 11 11 26 48 11 11 11 11 11 48 48 11 11 11 11
                        49 DOUBLE[]       11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 49 11 11 11 11 11 11 11 49 11 11 11
                        50 DOUBLE[][]     11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 11 26 50 11 11 11 11 11 11 11 11 50 11 11
                        51 INTERVAL(us)   11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 51 26 51 11 11 11 11 11 11 11 11 11 51 51
                        52 INTERVAL(ns)   11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 26 11 11 11 11 11 11 11 11 11 11 11 11 52 26 52 11 11 11 11 11 11 11 11 11 52 52
                        """,
                renderType(ColumnType::commonWideningType)
        );
    }

    @Test
    public void testCopierArms() throws Exception {
        // the pairs the three RecordToRowCopier implementations have an arm for; a NULL source
        // reads through the target's arm, a VARCHAR_SLICE source through VARCHAR's
        final Method method = RecordToRowCopierUtils.class.getDeclaredMethod("copyOpcode", int.class, int.class);
        method.setAccessible(true);
        final Field field = RecordToRowCopierUtils.class.getDeclaredField("COPY_NONE");
        field.setAccessible(true);
        final int none = field.getInt(null);
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     .....................................................
                         1 BOOLEAN       .X...................................................
                         2 BYTE          .XXX.XXXXXX.................XXXXXX........X....XX....
                         3 SHORT         ..XX.XXXXXX.................XXXXXX........X....XX....
                         4 CHAR          ..XXXXXXXXXXX.X...........X.XXXXXX........XX...XX....
                         5 INT           ..XX.XXXXXX.................XXXXXX........X....XX....
                         6 LONG          ..XX.XXXXXX.................XXXXXX........X....XX....
                         7 DATE          ..XX.XXXXXX...............................X..........
                         8 TIMESTAMP     ..XX.XXXXXX...............................X..........
                         9 FLOAT         ..XX.XXXXXX...............................X..........
                        10 DOUBLE        ..XX.XXXXXX...............................X..........
                        11 STRING        ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXX........XXXXXXXXX..
                        12 SYMBOL        ...........XX.............X..........................
                        13 LONG256       .............X.......................................
                        14 GEOBYTE       ..............X............................X.........
                        15 GEOSHORT      ..............XX...........................XX........
                        16 GEOINT        ..............XXX..........................XXX.......
                        17 GEOLONG       ..............XXXX.........................XXXX......
                        18 BINARY        ..................X..................................
                        19 UUID          ...........X.......X....X.X..........................
                        20 CURSOR        .....................................................
                        21 VAR_ARG       .....................................................
                        22 RECORD        .....................................................
                        23 GEOHASH       .....................................................
                        24 LONG128       ...........X.......X....X.X..........................
                        25 IPv4          .........................X...........................
                        26 VARCHAR       ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXX........XXXXXXXXX..
                        27 ARRAY         ...........................X.....................XX..
                        28 DECIMAL8      ............................XXXXXX.............XX....
                        29 DECIMAL16     ............................XXXXXX.............XX....
                        30 DECIMAL32     ............................XXXXXX.............XX....
                        31 DECIMAL64     ............................XXXXXX.............XX....
                        32 DECIMAL128    ............................XXXXXX.............XX....
                        33 DECIMAL256    ............................XXXXXX.............XX....
                        34 DECIMAL       .....................................................
                        35 REGCLASS      .....................................................
                        36 REGPROCEDURE  .....................................................
                        37 ARRAY_STRING  .....................................................
                        38 PARAMETER     .....................................................
                        39 INTERVAL      .....................................................
                        40 VARCHAR_SLICE ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXX........XXXXXXXXX..
                        41 NULL          .XXXXXXXXXXXXXXXXXXX....XXXXXXXXXX........XXXXXXXXX..
                        42 TIMESTAMP_NS  ..XX.XXXXXX...............................X..........
                        43 GEOHASH(1c)   ..............X............................X.........
                        44 GEOHASH(8b)   ..............XX...........................XX........
                        45 GEOHASH(31b)  ..............XXX..........................XXX.......
                        46 GEOHASH(12c)  ..............XXXX.........................XXXX......
                        47 DECIMAL(5,2)  ............................XXXXXX.............XX....
                        48 DECIMAL(18,3) ............................XXXXXX.............XX....
                        49 DOUBLE[]      ...........................X.....................XX..
                        50 DOUBLE[][]    ...........................X.....................XX..
                        51 INTERVAL(us)  .....................................................
                        52 INTERVAL(ns)  .....................................................
                        """,
                renderBoolean((from, to) -> (int) method.invoke(null, from, to) != none)
        );
    }

    @Test
    public void testCopierGaps() throws Exception {
        // the pairs INSERT admits (isConvertibleFrom, into a tag a table column can have) that no
        // copier has an arm for: the copiers write nothing and the column stays NULL; see
        // issues/copier-admitted-pairs-without-arm. GEOHASH and DECIMAL are the two pseudo tags a
        // DDL resolves to a sized tag before a column exists.
        final Method method = RecordToRowCopierUtils.class.getDeclaredMethod("copyOpcode", int.class, int.class);
        method.setAccessible(true);
        final Field field = RecordToRowCopierUtils.class.getDeclaredField("COPY_NONE");
        field.setAccessible(true);
        final int none = field.getInt(null);
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     .....................................................
                         1 BOOLEAN       .....................................................
                         2 BYTE          ....X................................................
                         3 SHORT         ....X................................................
                         4 CHAR          .....................................................
                         5 INT           .....................................................
                         6 LONG          ....X................................................
                         7 DATE          ....X................................................
                         8 TIMESTAMP     ....X................................................
                         9 FLOAT         ....X................................................
                        10 DOUBLE        ....X................................................
                        11 STRING        .....................................................
                        12 SYMBOL        ........X.................................X..........
                        13 LONG256       .....................................................
                        14 GEOBYTE       .....................................................
                        15 GEOSHORT      .....................................................
                        16 GEOINT        .....................................................
                        17 GEOLONG       .....................................................
                        18 BINARY        .....................................................
                        19 UUID          .....................................................
                        20 CURSOR        .....................................................
                        21 VAR_ARG       .....................................................
                        22 RECORD        .....................................................
                        23 GEOHASH       .....................................................
                        24 LONG128       .....................................................
                        25 IPv4          .....................................................
                        26 VARCHAR       .....................................................
                        27 ARRAY         .....................................................
                        28 DECIMAL8      .....................................................
                        29 DECIMAL16     .....................................................
                        30 DECIMAL32     .....................................................
                        31 DECIMAL64     .....................................................
                        32 DECIMAL128    .....................................................
                        33 DECIMAL256    .....................................................
                        34 DECIMAL       .....................................................
                        35 REGCLASS      .....................................................
                        36 REGPROCEDURE  .....................................................
                        37 ARRAY_STRING  .....................................................
                        38 PARAMETER     .....................................................
                        39 INTERVAL      .....................................................
                        40 VARCHAR_SLICE .....................................................
                        41 NULL          .....................................................
                        42 TIMESTAMP_NS  ....X................................................
                        43 GEOHASH(1c)   .....................................................
                        44 GEOHASH(8b)   .....................................................
                        45 GEOHASH(31b)  .....................................................
                        46 GEOHASH(12c)  .....................................................
                        47 DECIMAL(5,2)  .....................................................
                        48 DECIMAL(18,3) .....................................................
                        49 DOUBLE[]      .....................................................
                        50 DOUBLE[][]    .....................................................
                        51 INTERVAL(us)  .....................................................
                        52 INTERVAL(ns)  .....................................................
                        """,
                renderBoolean((from, to) -> ColumnType.isPersisted(ColumnType.tagOf(to))
                        && ColumnType.tagOf(to) != ColumnType.GEOHASH && ColumnType.tagOf(to) != ColumnType.DECIMAL
                        && ColumnType.isConvertibleFrom(from, to)
                        && (int) method.invoke(null, from, to) == none)
        );
    }

    @Test
    public void testIsBuiltInWideningCast() {
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     .....................................................
                         1 BOOLEAN       .....................................................
                         2 BYTE          ...X.XX..XX..........................................
                         3 SHORT         ....XXX..XX..........................................
                         4 CHAR          ...X.XX..XX..........................................
                         5 INT           ......XXXXX...............................X..........
                         6 LONG          .......XXXX...............................X..........
                         7 DATE          ......X.XXX...............................X..........
                         8 TIMESTAMP     ......X..XX..........................................
                         9 FLOAT         ..........X..........................................
                        10 DOUBLE        .....................................................
                        11 STRING        ..XXXXXXXXX...............................X..........
                        12 SYMBOL        .....................................................
                        13 LONG256       .....................................................
                        14 GEOBYTE       .....................................................
                        15 GEOSHORT      .....................................................
                        16 GEOINT        .....................................................
                        17 GEOLONG       .....................................................
                        18 BINARY        .....................................................
                        19 UUID          .....................................................
                        20 CURSOR        .....................................................
                        21 VAR_ARG       .....................................................
                        22 RECORD        .....................................................
                        23 GEOHASH       .....................................................
                        24 LONG128       .....................................................
                        25 IPv4          .....................................................
                        26 VARCHAR       ..XXXXXXXXX...............................X..........
                        27 ARRAY         .....................................................
                        28 DECIMAL8      .....................................................
                        29 DECIMAL16     .....................................................
                        30 DECIMAL32     .....................................................
                        31 DECIMAL64     .....................................................
                        32 DECIMAL128    .....................................................
                        33 DECIMAL256    .....................................................
                        34 DECIMAL       .....................................................
                        35 REGCLASS      .....................................................
                        36 REGPROCEDURE  .....................................................
                        37 ARRAY_STRING  .....................................................
                        38 PARAMETER     .....................................................
                        39 INTERVAL      .....................................................
                        40 VARCHAR_SLICE ..XXXXXXXXX...............................X..........
                        41 NULL          XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                        42 TIMESTAMP_NS  ......X..XX..........................................
                        43 GEOHASH(1c)   .....................................................
                        44 GEOHASH(8b)   .....................................................
                        45 GEOHASH(31b)  .....................................................
                        46 GEOHASH(12c)  .....................................................
                        47 DECIMAL(5,2)  .....................................................
                        48 DECIMAL(18,3) .....................................................
                        49 DOUBLE[]      .....................................................
                        50 DOUBLE[][]    .....................................................
                        51 INTERVAL(us)  .....................................................
                        52 INTERVAL(ns)  .....................................................
                        """,
                renderBoolean(ColumnType::isBuiltInWideningCast)
        );
    }

    @Test
    public void testIsCompatibleCast() throws Exception {
        // CREATE TABLE ... AS (SELECT ...), CAST(col AS type): CreateTableOperationBuilderImpl.isCompatibleCast
        final Method method = CreateTableOperationBuilderImpl.class.getDeclaredMethod("isCompatibleCast", int.class, int.class);
        method.setAccessible(true);
        // PB6: castGroups has no entry for UNDEFINED, LONG256, GEOBYTE..GEOLONG, UUID, CURSOR,
        // VAR_ARG, RECORD, GEOHASH, LONG128 and IPv4, so they all read group 0 and every pair
        // among them is compatible: rows 0, 13-17, 19-25 and the encoded geohash rows 43-46.
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     X............XXXXX.XXXXXXX.................XXXX......
                         1 BOOLEAN       .X...................................................
                         2 BYTE          ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         3 SHORT         ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         4 CHAR          ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         5 INT           ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         6 LONG          ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         7 DATE          ..XXXXXXXXX...............................X..........
                         8 TIMESTAMP     ..XXXXXXXXX...............................X..........
                         9 FLOAT         ..XXXXXXXXX...............................X..........
                        10 DOUBLE        ..XXXXXXXXX...............................X..........
                        11 STRING        ...........XX............XXXXXXXXXX............XXXX..
                        12 SYMBOL        ...........XX.............X..........................
                        13 LONG256       X............XXXXX.XXXXXXX.................XXXX......
                        14 GEOBYTE       X............XXXXX.XXXXXXX.................XXXX......
                        15 GEOSHORT      X............XXXXX.XXXXXXX.................XXXX......
                        16 GEOINT        X............XXXXX.XXXXXXX.................XXXX......
                        17 GEOLONG       X............XXXXX.XXXXXXX.................XXXX......
                        18 BINARY        ..................X..................................
                        19 UUID          X............XXXXX.XXXXXXX.................XXXX......
                        20 CURSOR        X............XXXXX.XXXXXXX.................XXXX......
                        21 VAR_ARG       X............XXXXX.XXXXXXX.................XXXX......
                        22 RECORD        X............XXXXX.XXXXXXX.................XXXX......
                        23 GEOHASH       X............XXXXX.XXXXXXX.................XXXX......
                        24 LONG128       X............XXXXX.XXXXXXX.................XXXX......
                        25 IPv4          X............XXXXX.XXXXXXX.................XXXX......
                        26 VARCHAR       ...........XX............XXXXXXXXXX............XXXX..
                        27 ARRAY         ...........................X.........................
                        28 DECIMAL8      ............................XXXXXX.............XX....
                        29 DECIMAL16     ............................XXXXXX.............XX....
                        30 DECIMAL32     ............................XXXXXX.............XX....
                        31 DECIMAL64     ............................XXXXXX.............XX....
                        32 DECIMAL128    ............................XXXXXX.............XX....
                        33 DECIMAL256    ............................XXXXXX.............XX....
                        34 DECIMAL       ..................................X..................
                        35 REGCLASS      ...................................X.................
                        36 REGPROCEDURE  ....................................X................
                        37 ARRAY_STRING  .....................................X...............
                        38 PARAMETER     ......................................X..............
                        39 INTERVAL      .......................................X...........XX
                        40 VARCHAR_SLICE ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXXX.....X.XXXXXXXXX..
                        41 NULL          XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                        42 TIMESTAMP_NS  ..XXXXXXXXX...............................X..........
                        43 GEOHASH(1c)   X............XXXXX.XXXXXXX.................XXXX......
                        44 GEOHASH(8b)   X............XXXXX.XXXXXXX.................XXXX......
                        45 GEOHASH(31b)  X............XXXXX.XXXXXXX.................XXXX......
                        46 GEOHASH(12c)  X............XXXXX.XXXXXXX.................XXXX......
                        47 DECIMAL(5,2)  ............................XXXXXX.............XX....
                        48 DECIMAL(18,3) ............................XXXXXX.............XX....
                        49 DOUBLE[]      .................................................X...
                        50 DOUBLE[][]    ..................................................X..
                        51 INTERVAL(us)  .......................................X...........XX
                        52 INTERVAL(ns)  .......................................X...........XX
                        """,
                renderBoolean((from, to) -> (boolean) method.invoke(null, from, to))
        );
    }

    @Test
    public void testIsCompatibleCastZeroSlotQuirk() throws Exception {
        // PB6, pre-existing-bugs.md: the cells the golden above freezes for the unset castGroups slots
        final Method method = CreateTableOperationBuilderImpl.class.getDeclaredMethod("isCompatibleCast", int.class, int.class);
        method.setAccessible(true);
        Assert.assertTrue((boolean) method.invoke(null, ColumnType.UUID, ColumnType.IPv4)); // PB6
        Assert.assertTrue((boolean) method.invoke(null, ColumnType.LONG256, ColumnType.CURSOR)); // PB6
        Assert.assertTrue((boolean) method.invoke(null, ColumnType.GEOBYTE, ColumnType.LONG128)); // PB6
        Assert.assertTrue((boolean) method.invoke(null, ColumnType.getGeoHashTypeWithBits(5), ColumnType.UNDEFINED)); // PB6
        // the same slots are not compatible with a type that does have a group
        Assert.assertFalse((boolean) method.invoke(null, ColumnType.UUID, ColumnType.LONG));
        Assert.assertFalse((boolean) method.invoke(null, ColumnType.LONG256, ColumnType.STRING));
    }

    @Test
    public void testIsConvertibleFrom() {
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     X....................................................
                         1 BOOLEAN       .X...................................................
                         2 BYTE          ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         3 SHORT         ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         4 CHAR          ..XXXXXXXXXXX.X...........X.XXXXXXX.......XX...XX....
                         5 INT           ..XX.XXXXXX.................XXXXXXX.......X....XX....
                         6 LONG          ..XXXXXXXXX.................XXXXXXX.......X....XX....
                         7 DATE          ..XXXXXXXXX...............................X..........
                         8 TIMESTAMP     ..XXXXXXXXX...............................X..........
                         9 FLOAT         ..XXXXXXXXX...............................X..........
                        10 DOUBLE        ..XXXXX..XX..........................................
                        11 STRING        ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXXX.......XXXXXXXXX..
                        12 SYMBOL        ........X..XX.............X...............X..........
                        13 LONG256       .............X.......................................
                        14 GEOBYTE       ..............X............................X.........
                        15 GEOSHORT      ..............XX...........................XX........
                        16 GEOINT        ..............XXX..........................XXX.......
                        17 GEOLONG       ..............XXXX.........................XXXX......
                        18 BINARY        ..................X..................................
                        19 UUID          ...........X.......X......X..........................
                        20 CURSOR        ....................X................................
                        21 VAR_ARG       .....................X...............................
                        22 RECORD        ......................X..............................
                        23 GEOHASH       .......................X.............................
                        24 LONG128       ........................X............................
                        25 IPv4          .........................X...........................
                        26 VARCHAR       ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXXX.......XXXXXXXXX..
                        27 ARRAY         ...........................X.........................
                        28 DECIMAL8      ............................XXXXXX.............XX....
                        29 DECIMAL16     ............................XXXXXX.............XX....
                        30 DECIMAL32     ............................XXXXXX.............XX....
                        31 DECIMAL64     ............................XXXXXX.............XX....
                        32 DECIMAL128    ............................XXXXXX.............XX....
                        33 DECIMAL256    ............................XXXXXX.............XX....
                        34 DECIMAL       ..................................X..................
                        35 REGCLASS      ...................................X.................
                        36 REGPROCEDURE  ....................................X................
                        37 ARRAY_STRING  .....................................X...............
                        38 PARAMETER     ......................................X..............
                        39 INTERVAL      .......................................X...........XX
                        40 VARCHAR_SLICE ..XXXXXXXXXXXXXXXX.X.....XXXXXXXXXX.....X.XXXXXXXXX..
                        41 NULL          XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                        42 TIMESTAMP_NS  ..XXXXXXXXX...............................X..........
                        43 GEOHASH(1c)   ..............X............................X.........
                        44 GEOHASH(8b)   ..............XX...........................XX........
                        45 GEOHASH(31b)  ..............XXX..........................XXX.......
                        46 GEOHASH(12c)  ..............XXXX.........................XXXX......
                        47 DECIMAL(5,2)  ............................XXXXXX.............XX....
                        48 DECIMAL(18,3) ............................XXXXXX.............XX....
                        49 DOUBLE[]      .................................................X...
                        50 DOUBLE[][]    ..................................................X..
                        51 INTERVAL(us)  .......................................X...........XX
                        52 INTERVAL(ns)  .......................................X...........XX
                        """,
                renderBoolean(ColumnType::isConvertibleFrom)
        );
    }

    @Test
    public void testIsNarrowingCast() throws Exception {
        final Method method = ColumnType.class.getDeclaredMethod("isNarrowingCast", int.class, int.class);
        method.setAccessible(true);
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     .....................................................
                         1 BOOLEAN       .....................................................
                         2 BYTE          ............................XXXXXXX............XX....
                         3 SHORT         ..X.........................XXXXXXX............XX....
                         4 CHAR          ..X.........................XXXXXXX............XX....
                         5 INT           ..XX........................XXXXXXX............XX....
                         6 LONG          ..XXXX......................XXXXXXX............XX....
                         7 DATE          ..XXXX...............................................
                         8 TIMESTAMP     ..XXXX.X.............................................
                         9 FLOAT         ..XXXXXXX.................................X..........
                        10 DOUBLE        ..XXXXX..X...........................................
                        11 STRING        ..XXXXXXXXX........X.......XXXXXXXX.......X....XXXX..
                        12 SYMBOL        .....................................................
                        13 LONG256       .....................................................
                        14 GEOBYTE       .....................................................
                        15 GEOSHORT      .....................................................
                        16 GEOINT        .....................................................
                        17 GEOLONG       .....................................................
                        18 BINARY        .....................................................
                        19 UUID          .....................................................
                        20 CURSOR        .....................................................
                        21 VAR_ARG       .....................................................
                        22 RECORD        .....................................................
                        23 GEOHASH       .....................................................
                        24 LONG128       .....................................................
                        25 IPv4          .....................................................
                        26 VARCHAR       ..XXXXXXXXX........X.......XXXXXXXX.......X....XXXX..
                        27 ARRAY         .....................................................
                        28 DECIMAL8      .....................................................
                        29 DECIMAL16     .....................................................
                        30 DECIMAL32     .....................................................
                        31 DECIMAL64     .....................................................
                        32 DECIMAL128    .....................................................
                        33 DECIMAL256    .....................................................
                        34 DECIMAL       .....................................................
                        35 REGCLASS      .....................................................
                        36 REGPROCEDURE  .....................................................
                        37 ARRAY_STRING  .....................................................
                        38 PARAMETER     .....................................................
                        39 INTERVAL      .....................................................
                        40 VARCHAR_SLICE ..XXXXXXXXX........X.......XXXXXXXX.......X....XXXX..
                        41 NULL          .....................................................
                        42 TIMESTAMP_NS  ..XXXX.X.............................................
                        43 GEOHASH(1c)   .....................................................
                        44 GEOHASH(8b)   .....................................................
                        45 GEOHASH(31b)  .....................................................
                        46 GEOHASH(12c)  .....................................................
                        47 DECIMAL(5,2)  .....................................................
                        48 DECIMAL(18,3) .....................................................
                        49 DOUBLE[]      .....................................................
                        50 DOUBLE[][]    .....................................................
                        51 INTERVAL(us)  .....................................................
                        52 INTERVAL(ns)  .....................................................
                        """,
                renderBoolean((from, to) -> (boolean) method.invoke(null, from, to))
        );
    }

    @Test
    public void testIsToSameOrWider() {
        assertGolden(
                """
                                                   1111111111222222222233333333334444444444555
                                         01234567890123456789012345678901234567890123456789012
                         0 UNDEFINED     X....................................................
                         1 BOOLEAN       .X...................................................
                         2 BYTE          ..XXXXXXXXX...............................X..........
                         3 SHORT         ...XXXXXXXX...............................X..........
                         4 CHAR          ...XXXXXXXXXX.X...........X...............XX.........
                         5 INT           .....XXXXXX...............................X..........
                         6 LONG          ......XXXXX...............................X..........
                         7 DATE          ......XXXXX...............................X..........
                         8 TIMESTAMP     ......X.XXX...............................X..........
                         9 FLOAT         .........XX..........................................
                        10 DOUBLE        ..........X..........................................
                        11 STRING        ..XXXXXXXXXXXXXXXX.......XX...............XXXXX......
                        12 SYMBOL        ........X..XX.............X...............X..........
                        13 LONG256       .............X.......................................
                        14 GEOBYTE       ..............X............................X.........
                        15 GEOSHORT      ..............XX...........................XX........
                        16 GEOINT        ..............XXX..........................XXX.......
                        17 GEOLONG       ..............XXXX.........................XXXX......
                        18 BINARY        ..................X..................................
                        19 UUID          ...........X.......X......X..........................
                        20 CURSOR        ....................X................................
                        21 VAR_ARG       .....................X...............................
                        22 RECORD        ......................X..............................
                        23 GEOHASH       .......................X.............................
                        24 LONG128       ........................X............................
                        25 IPv4          .........................X...........................
                        26 VARCHAR       ..XXXXXXXXXXXXXXXX.......XX...............XXXXX......
                        27 ARRAY         ...........................X.........................
                        28 DECIMAL8      ............................XXXXXX.............XX....
                        29 DECIMAL16     ............................XXXXXX.............XX....
                        30 DECIMAL32     ............................XXXXXX.............XX....
                        31 DECIMAL64     ............................XXXXXX.............XX....
                        32 DECIMAL128    ............................XXXXXX.............XX....
                        33 DECIMAL256    ............................XXXXXX.............XX....
                        34 DECIMAL       ..................................X..................
                        35 REGCLASS      ...................................X.................
                        36 REGPROCEDURE  ....................................X................
                        37 ARRAY_STRING  .....................................X...............
                        38 PARAMETER     ......................................X..............
                        39 INTERVAL      .......................................X...........XX
                        40 VARCHAR_SLICE ..XXXXXXXXXXXXXXXX.......XX.............X.XXXXX......
                        41 NULL          XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                        42 TIMESTAMP_NS  ......X.XXX...............................X..........
                        43 GEOHASH(1c)   ..............X............................X.........
                        44 GEOHASH(8b)   ..............XX...........................XX........
                        45 GEOHASH(31b)  ..............XXX..........................XXX.......
                        46 GEOHASH(12c)  ..............XXXX.........................XXXX......
                        47 DECIMAL(5,2)  ............................XXXXXX.............XX....
                        48 DECIMAL(18,3) ............................XXXXXX.............XX....
                        49 DOUBLE[]      .................................................X...
                        50 DOUBLE[][]    ..................................................X..
                        51 INTERVAL(us)  .......................................X...........XX
                        52 INTERVAL(ns)  .......................................X...........XX
                        """,
                renderBoolean(ColumnType::isToSameOrWider)
        );
    }

    @Test
    public void testNameOf() {
        assertGolden(
                """
                         0 UNDEFINED     unknown
                         1 BOOLEAN       BOOLEAN
                         2 BYTE          BYTE
                         3 SHORT         SHORT
                         4 CHAR          CHAR
                         5 INT           INT
                         6 LONG          LONG
                         7 DATE          DATE
                         8 TIMESTAMP     TIMESTAMP
                         9 FLOAT         FLOAT
                        10 DOUBLE        DOUBLE
                        11 STRING        STRING
                        12 SYMBOL        SYMBOL
                        13 LONG256       LONG256
                        14 GEOBYTE       unknown
                        15 GEOSHORT      unknown
                        16 GEOINT        unknown
                        17 GEOLONG       unknown
                        18 BINARY        BINARY
                        19 UUID          UUID
                        20 CURSOR        CURSOR
                        21 VAR_ARG       VARARG
                        22 RECORD        RECORD
                        23 GEOHASH       GEOHASH
                        24 LONG128       LONG128
                        25 IPv4          IPv4
                        26 VARCHAR       VARCHAR
                        27 ARRAY         ARRAY
                        28 DECIMAL8      unknown
                        29 DECIMAL16     unknown
                        30 DECIMAL32     unknown
                        31 DECIMAL64     unknown
                        32 DECIMAL128    unknown
                        33 DECIMAL256    unknown
                        34 DECIMAL       DECIMAL
                        35 REGCLASS      regclass
                        36 REGPROCEDURE  regprocedure
                        37 ARRAY_STRING  text[]
                        38 PARAMETER     PARAMETER
                        39 INTERVAL      INTERVAL
                        40 VARCHAR_SLICE VARCHAR_SLICE
                        41 NULL          NULL
                        42 TIMESTAMP_NS  TIMESTAMP_NS
                        43 GEOHASH(1c)   GEOHASH(1c)
                        44 GEOHASH(8b)   GEOHASH(8b)
                        45 GEOHASH(31b)  GEOHASH(31b)
                        46 GEOHASH(12c)  GEOHASH(12c)
                        47 DECIMAL(5,2)  DECIMAL(5,2)
                        48 DECIMAL(18,3) DECIMAL(18,3)
                        49 DOUBLE[]      DOUBLE[]
                        50 DOUBLE[][]    DOUBLE[][]
                        51 INTERVAL(us)  INTERVAL
                        52 INTERVAL(ns)  INTERVAL
                        """,
                renderPerType(ColumnType::nameOf)
        );
    }

    @Test
    public void testNullConstants() {
        assertGolden(
                """
                         0 UNDEFINED     NullConstant NULL
                         1 BOOLEAN       BooleanConstant BOOLEAN
                         2 BYTE          ByteConstant BYTE
                         3 SHORT         ShortConstant SHORT
                         4 CHAR          CharConstant CHAR
                         5 INT           IntConstant INT
                         6 LONG          LongConstant LONG
                         7 DATE          DateConstant DATE
                         8 TIMESTAMP     TimestampConstant TIMESTAMP
                         9 FLOAT         FloatConstant FLOAT
                        10 DOUBLE        DoubleConstant DOUBLE
                        11 STRING        StrConstant STRING
                        12 SYMBOL        SymbolConstant SYMBOL
                        13 LONG256       Long256NullConstant LONG256
                        14 GEOBYTE       GeoByteConstant GEOBYTE
                        15 GEOSHORT      GeoShortConstant GEOSHORT
                        16 GEOINT        GeoIntConstant GEOINT
                        17 GEOLONG       GeoLongConstant GEOLONG
                        18 BINARY        NullBinConstant BINARY
                        19 UUID          UuidConstant UUID
                        20 CURSOR        NullConstant NULL
                        21 VAR_ARG       NullConstant NULL
                        22 RECORD        NullConstant NULL
                        23 GEOHASH       NullConstant NULL
                        24 LONG128       Long128Constant LONG128
                        25 IPv4          IPv4Constant IPv4
                        26 VARCHAR       VarcharConstant VARCHAR
                        27 ARRAY         NullArrayConstant DOUBLE[]
                        28 DECIMAL8      !
                        29 DECIMAL16     !
                        30 DECIMAL32     !
                        31 DECIMAL64     !
                        32 DECIMAL128    !
                        33 DECIMAL256    !
                        34 DECIMAL       NullConstant NULL
                        35 REGCLASS      NullConstant NULL
                        36 REGPROCEDURE  NullConstant NULL
                        37 ARRAY_STRING  NullConstant NULL
                        38 PARAMETER     NullConstant NULL
                        39 INTERVAL      IntervalConstant INTERVAL
                        40 VARCHAR_SLICE NullConstant NULL
                        41 NULL          NullConstant NULL
                        42 TIMESTAMP_NS  TimestampConstant TIMESTAMP_NS
                        43 GEOHASH(1c)   GeoByteConstant GEOHASH(1c)
                        44 GEOHASH(8b)   GeoShortConstant GEOHASH(8b)
                        45 GEOHASH(31b)  GeoIntConstant GEOHASH(31b)
                        46 GEOHASH(12c)  GeoLongConstant GEOHASH(12c)
                        47 DECIMAL(5,2)  Decimal32Constant DECIMAL(5,2)
                        48 DECIMAL(18,3) Decimal64Constant DECIMAL(18,3)
                        49 DOUBLE[]      NullArrayConstant DOUBLE[]
                        50 DOUBLE[][]    NullArrayConstant DOUBLE[][]
                        51 INTERVAL(us)  IntervalConstant INTERVAL(us)
                        52 INTERVAL(ns)  IntervalConstant INTERVAL(ns)
                        """,
                renderPerType(type -> {
                    final Function fn = Constants.getNullConstant(type);
                    return fn.getClass().getSimpleName() + " " + typeLabel(fn.getType());
                })
        );
    }

    @Test
    public void testOverloadDistance() {
        assertGolden(
                """
                         0 UNDEFINED      DOUBLE=0 FLOAT=1 STRING=2 VARCHAR=3 LONG=4 TIMESTAMP=5 DATE=6 INT=7 CHAR=8 SHORT=9 BYTE=10 BOOLEAN=11
                         1 BOOLEAN        BOOLEAN=0
                         2 BYTE           BYTE=0 SHORT=1 INT=2 LONG=3 FLOAT=4 DOUBLE=5 DECIMAL=6
                         3 SHORT          SHORT=0 INT=1 LONG=2 FLOAT=3 DOUBLE=4 CHAR=5 DECIMAL=6
                         4 CHAR           CHAR=0 STRING=1 VARCHAR=2 SHORT=3 INT=4 LONG=5 FLOAT=6 DOUBLE=7
                         5 INT            INT=0 LONG=1 FLOAT=2 DOUBLE=3 TIMESTAMP=4 DATE=5 DECIMAL=6
                         6 LONG           LONG=0 DOUBLE=1 TIMESTAMP=2 DATE=3 DECIMAL=4
                         7 DATE           DATE=0 TIMESTAMP=1 LONG=2 DOUBLE=3
                         8 TIMESTAMP      TIMESTAMP=0 LONG=1 DATE=2 DOUBLE=3
                         9 FLOAT          FLOAT=0 DOUBLE=1
                        10 DOUBLE         DOUBLE=0
                        11 STRING         STRING=0 VARCHAR=1 CHAR=2 DOUBLE=3 LONG=4 INT=5 FLOAT=6 SHORT=7 BYTE=8 TIMESTAMP=9 DATE=10 SYMBOL=11 IPv4=12
                        12 SYMBOL         SYMBOL=0 STRING=1 VARCHAR=2 CHAR=3 INT=4 TIMESTAMP=5
                        13 LONG256        LONG256=0 LONG=1
                        14 GEOBYTE        GEOBYTE=0 GEOSHORT=1 GEOINT=2 GEOLONG=3 GEOHASH=4
                        15 GEOSHORT       GEOSHORT=0 GEOINT=1 GEOLONG=2 GEOHASH=3
                        16 GEOINT         GEOINT=0 GEOLONG=1 GEOHASH=2
                        17 GEOLONG        GEOLONG=0 GEOHASH=1
                        18 BINARY         BINARY=0
                        19 UUID           UUID=0 STRING=1
                        20 CURSOR         CURSOR=0
                        21 VAR_ARG
                        22 RECORD
                        23 GEOHASH
                        24 LONG128        LONG128=0
                        25 IPv4           IPv4=0 STRING=1 VARCHAR=2
                        26 VARCHAR        VARCHAR=0 STRING=1 CHAR=2 DOUBLE=3 LONG=4 INT=5 FLOAT=6 SHORT=7 BYTE=8 TIMESTAMP=9 DATE=10 SYMBOL=11 IPv4=12
                        27 ARRAY          ARRAY=0
                        28 DECIMAL8       DECIMAL8=0 DECIMAL16=1 DECIMAL32=2 DECIMAL64=3 DECIMAL128=4 DECIMAL256=5 DECIMAL=6
                        29 DECIMAL16      DECIMAL16=0 DECIMAL32=1 DECIMAL64=2 DECIMAL128=3 DECIMAL256=4 DECIMAL=5
                        30 DECIMAL32      DECIMAL32=0 DECIMAL64=1 DECIMAL128=2 DECIMAL256=3 DECIMAL=4
                        31 DECIMAL64      DECIMAL64=0 DECIMAL128=1 DECIMAL256=2 DECIMAL=3
                        32 DECIMAL128     DECIMAL128=0 DECIMAL256=1 DECIMAL=2
                        33 DECIMAL256     DECIMAL256=0 DECIMAL=1
                        34 DECIMAL
                        35 REGCLASS
                        36 REGPROCEDURE
                        37 ARRAY_STRING
                        38 PARAMETER
                        39 INTERVAL       INTERVAL=0 STRING=1
                        40 VARCHAR_SLICE  VARCHAR=0 STRING=1 CHAR=2 DOUBLE=3 LONG=4 INT=5 FLOAT=6 SHORT=7 BYTE=8 TIMESTAMP=9 DATE=10 SYMBOL=11 IPv4=12
                        41 NULL           STRING=-1 SYMBOL=-1 BOOLEAN=0 BYTE=0 SHORT=0 CHAR=0 INT=0 LONG=0 DATE=0 TIMESTAMP=0 FLOAT=0 DOUBLE=0 LONG256=0 GEOBYTE=0 GEOSHORT=0 GEOINT=0 GEOLONG=0 BINARY=0 UUID=0 VAR_ARG=0 RECORD=0 GEOHASH=0 LONG128=0 IPv4=0 VARCHAR=0 ARRAY=0 DECIMAL8=0 DECIMAL16=0 DECIMAL32=0 DECIMAL64=0 DECIMAL128=0 DECIMAL256=0 DECIMAL=0 REGCLASS=0 REGPROCEDURE=0 ARRAY_STRING=0 PARAMETER=0 INTERVAL=0 VARCHAR_SLICE=0 NULL=0
                        """,
                renderOverloadDistance()
        );
    }

    @Test
    public void testPerRowSinkArms() throws Exception {
        // the unary relations behind the per-row sinks, sorts and updates: which arm a type takes
        // (its own tag), that the site writes nothing for it (none), or that the site rejects it (!).
        // sink: RecordSinkFactory and LoopingRecordSink; vsink: RecordValueSinkFactory; cmp:
        // RecordComparatorCompiler; key: SortKeyEncoder kind/width (signed, unsigned, float,
        // double, wide, symbol, variable); mat: SortKeyMaterializingRecordCursor; agg:
        // GroupByColumnSink (none = the sink appends nothing, PB5); upd: UpdateOperatorImpl (none =
        // rejected at the first row); map: the single-column key eligibility of Unordered4/8Map
        final Method sink = method(RecordSinkFactory.class, "sinkOpcode", int.class, String.class);
        final Method vsink = method(RecordValueSinkFactory.class, "isSupportedColumnType", int.class);
        final Method cmp = method(RecordComparatorCompiler.class, "comparatorOpcode", int.class);
        final Method kind = method(SortKeyEncoder.class, "keyKind", int.class);
        final Method width = method(SortKeyEncoder.class, "fixedColumnByteWidth", int.class);
        final Method mat = method(Class.forName("io.questdb.griffin.engine.orderby.SortKeyMaterializingRecordCursor"), "materializeOpcode", int.class);
        final Method agg = method(GroupByColumnSink.class, "argTag", int.class);
        final Method upd = method(UpdateOperatorImpl.class, "updateOpcode", int.class);
        final Method map4 = method(Unordered4Map.class, "isSupportedKeyType", int.class);
        final Method map8 = method(Unordered8Map.class, "isSupportedKeyType", int.class);
        final String[] kinds = {"signed", "unsigned", "float", "double", "wide", "symbol", "variable"};
        // TIMESTAMP_NS map=. is PB3: only the plain TIMESTAMP type takes the 8-byte map
        assertGolden(
                """
                         0 UNDEFINED     sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                         1 BOOLEAN       sink=X vsink=X cmp=X key=unsigned/1 mat=X agg=X upd=X map=.
                         2 BYTE          sink=X vsink=X cmp=X key=signed/1 mat=X agg=X upd=X map=.
                         3 SHORT         sink=X vsink=X cmp=X key=signed/2 mat=X agg=X upd=X map=.
                         4 CHAR          sink=X vsink=X cmp=X key=unsigned/2 mat=X agg=X upd=X map=.
                         5 INT           sink=X vsink=X cmp=X key=signed/4 mat=X agg=X upd=X map=4
                         6 LONG          sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=8
                         7 DATE          sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=8
                         8 TIMESTAMP     sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=8
                         9 FLOAT         sink=X vsink=X cmp=X key=float/4 mat=X agg=X upd=X map=.
                        10 DOUBLE        sink=X vsink=X cmp=X key=double/8 mat=X agg=X upd=X map=.
                        11 STRING        sink=X vsink=. cmp=X key=variable/-1 mat=! agg=none upd=X map=.
                        12 SYMBOL        sink=X vsink=X cmp=X key=symbol/-1 mat=! agg=none upd=X map=4
                        13 LONG256       sink=X vsink=X cmp=X key=wide/32 mat=! agg=none upd=none map=.
                        14 GEOBYTE       sink=X vsink=X cmp=X key=signed/1 mat=X agg=X upd=X map=.
                        15 GEOSHORT      sink=X vsink=X cmp=X key=signed/2 mat=X agg=X upd=X map=.
                        16 GEOINT        sink=X vsink=X cmp=X key=signed/4 mat=X agg=X upd=X map=.
                        17 GEOLONG       sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=.
                        18 BINARY        sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=X map=.
                        19 UUID          sink=X vsink=X cmp=X key=wide/16 mat=! agg=X upd=X map=.
                        20 CURSOR        sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        21 VAR_ARG       sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        22 RECORD        sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        23 GEOHASH       sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        24 LONG128       sink=X vsink=X cmp=X key=wide/16 mat=! agg=X upd=X map=.
                        25 IPv4          sink=X vsink=X cmp=X key=unsigned/4 mat=! agg=X upd=X map=4
                        26 VARCHAR       sink=X vsink=. cmp=X key=variable/-1 mat=! agg=none upd=X map=.
                        27 ARRAY         sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=X map=.
                        28 DECIMAL8      sink=X vsink=X cmp=X key=signed/1 mat=X agg=X upd=X map=.
                        29 DECIMAL16     sink=X vsink=X cmp=X key=signed/2 mat=X agg=X upd=X map=.
                        30 DECIMAL32     sink=X vsink=X cmp=X key=signed/4 mat=X agg=X upd=X map=.
                        31 DECIMAL64     sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=.
                        32 DECIMAL128    sink=X vsink=X cmp=X key=wide/16 mat=X agg=X upd=X map=.
                        33 DECIMAL256    sink=X vsink=X cmp=X key=wide/32 mat=X agg=X upd=X map=.
                        34 DECIMAL       sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        35 REGCLASS      sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        36 REGPROCEDURE  sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        37 ARRAY_STRING  sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        38 PARAMETER     sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        39 INTERVAL      sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        40 VARCHAR_SLICE sink=! vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        41 NULL          sink=none vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        42 TIMESTAMP_NS  sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=.
                        43 GEOHASH(1c)   sink=X vsink=X cmp=X key=signed/1 mat=X agg=X upd=X map=.
                        44 GEOHASH(8b)   sink=X vsink=X cmp=X key=signed/2 mat=X agg=X upd=X map=.
                        45 GEOHASH(31b)  sink=X vsink=X cmp=X key=signed/4 mat=X agg=X upd=X map=.
                        46 GEOHASH(12c)  sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=.
                        47 DECIMAL(5,2)  sink=X vsink=X cmp=X key=signed/4 mat=X agg=X upd=X map=.
                        48 DECIMAL(18,3) sink=X vsink=X cmp=X key=signed/8 mat=X agg=X upd=X map=.
                        49 DOUBLE[]      sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=X map=.
                        50 DOUBLE[][]    sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=X map=.
                        51 INTERVAL(us)  sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        52 INTERVAL(ns)  sink=X vsink=. cmp=! key=./-1 mat=! agg=none upd=none map=.
                        """,
                renderPerType(type -> {
                    final int tag = ColumnType.tagOf(type);
                    final StringSink row = new StringSink();
                    row.put("sink=").put(arm(sink, tag, type, "column"));
                    row.put(" vsink=").put((boolean) vsink.invoke(null, type) ? "X" : ".");
                    row.put(" cmp=").put(arm(cmp, tag, type));
                    final int k = (int) kind.invoke(null, type);
                    row.put(" key=").put(k < 0 ? "." : kinds[k]).put('/').put((int) width.invoke(null, type));
                    row.put(" mat=").put(arm(mat, tag, type));
                    row.put(" agg=").put(sizeArm(agg, tag, type));
                    row.put(" upd=").put(arm(upd, tag, type));
                    row.put(" map=").put((boolean) map4.invoke(null, type) ? "4" : (boolean) map8.invoke(null, type) ? "8" : ".");
                    return row.toString();
                })
        );
    }

    @Test
    public void testSizes() {
        assertGolden(
                """
                         0 UNDEFINED     size=-1 pow2=-1
                         1 BOOLEAN       size=1 pow2=0 fixed persisted
                         2 BYTE          size=1 pow2=0 fixed persisted
                         3 SHORT         size=2 pow2=1 fixed persisted
                         4 CHAR          size=2 pow2=1 fixed persisted
                         5 INT           size=4 pow2=2 fixed persisted
                         6 LONG          size=8 pow2=3 fixed persisted
                         7 DATE          size=8 pow2=3 fixed persisted
                         8 TIMESTAMP     size=8 pow2=3 fixed persisted
                         9 FLOAT         size=4 pow2=2 fixed persisted
                        10 DOUBLE        size=8 pow2=3 fixed persisted
                        11 STRING        size=0 pow2=-1 var persisted
                        12 SYMBOL        size=4 pow2=2 persisted
                        13 LONG256       size=32 pow2=5 fixed persisted
                        14 GEOBYTE       size=1 pow2=0 fixed persisted
                        15 GEOSHORT      size=2 pow2=1 fixed persisted
                        16 GEOINT        size=4 pow2=2 fixed persisted
                        17 GEOLONG       size=8 pow2=3 fixed persisted
                        18 BINARY        size=0 pow2=-1 var persisted
                        19 UUID          size=16 pow2=4 fixed persisted
                        20 CURSOR        size=-1 pow2=-1
                        21 VAR_ARG       size=-1 pow2=-1
                        22 RECORD        size=-1 pow2=-1
                        23 GEOHASH       size=0 pow2=0 persisted
                        24 LONG128       size=16 pow2=4 fixed persisted
                        25 IPv4          size=4 pow2=2 fixed persisted
                        26 VARCHAR       size=0 pow2=-1 var persisted
                        27 ARRAY         size=0 pow2=-1 var persisted
                        28 DECIMAL8      size=1 pow2=0 fixed persisted
                        29 DECIMAL16     size=2 pow2=1 fixed persisted
                        30 DECIMAL32     size=4 pow2=2 fixed persisted
                        31 DECIMAL64     size=8 pow2=3 fixed persisted
                        32 DECIMAL128    size=16 pow2=4 fixed persisted
                        33 DECIMAL256    size=32 pow2=5 fixed persisted
                        34 DECIMAL       size=0 pow2=0 persisted
                        35 REGCLASS      size=0 pow2=0
                        36 REGPROCEDURE  size=0 pow2=0
                        37 ARRAY_STRING  size=0 pow2=0
                        38 PARAMETER     size=-1 pow2=-1
                        39 INTERVAL      size=16 pow2=4
                        40 VARCHAR_SLICE size=0 pow2=4 var
                        41 NULL          size=0 pow2=-1
                        42 TIMESTAMP_NS  size=8 pow2=3 fixed persisted
                        43 GEOHASH(1c)   size=1 pow2=0 persisted
                        44 GEOHASH(8b)   size=2 pow2=1 persisted
                        45 GEOHASH(31b)  size=4 pow2=2 persisted
                        46 GEOHASH(12c)  size=8 pow2=3 persisted
                        47 DECIMAL(5,2)  size=4 pow2=2 persisted
                        48 DECIMAL(18,3) size=8 pow2=3 persisted
                        49 DOUBLE[]      size=0 pow2=-1 var persisted
                        50 DOUBLE[][]    size=0 pow2=-1 var persisted
                        51 INTERVAL(us)  size=16 pow2=4 persisted
                        52 INTERVAL(ns)  size=16 pow2=4 persisted
                        """,
                renderPerType(type -> "size=" + ColumnType.sizeOf(type)
                        + " pow2=" + ColumnType.pow2SizeOf(type)
                        + (ColumnType.isFixedSize(type) ? " fixed" : "")
                        + (ColumnType.isVarSize(type) ? " var" : "")
                        + (ColumnType.isPersisted(type) ? " persisted" : ""))
        );
    }

    @Test
    public void testTypeSetStartsWithEveryTag() {
        Assert.assertTrue(TYPES.length > ColumnType.MAX_TAG + 1);
        for (int tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
            Assert.assertEquals(tag, TYPES[tag]);
            Assert.assertNotNull("tag " + tag + " has no ColumnType constant", LABELS[tag]);
        }
    }

    @Test
    public void testUnionCastType() {
        assertGolden(
                """
                                                                         1  1  1  1  1  1  1  1  1  1  2  2  2  2  2  2  2  2  2  2  3  3  3  3  3  3  3  3  3  3  4  4  4  4  4  4  4  4  4  4  5  5  5
                                           0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5  6  7  8  9  0  1  2
                         0 UNDEFINED       0 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  0 11 11 11 11 11 11 11 26 26 11 11
                         1 BOOLEAN        11  1 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  1 11 11 11 11 11 11 11 26 26 11 11
                         2 BYTE           11 11  2  3 11  5  6  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  2 42 11 11 11 11 47 48 26 26 11 11
                         3 SHORT          11 11  3  3  3  5  6  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  3 42 11 11 11 11  # 48 26 26 11 11
                         4 CHAR           11 11 11  3  4  5  6  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 11 42 11 11 11 11 11 11 26 26 11 11
                         5 INT            11 11  5  5  5  5  6  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  5 42 11 11 11 11  # 48 26 26 11 11
                         6 LONG           11 11  6  6  6  6  6  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  6 42 11 11 11 11  #  # 26 26 11 11
                         7 DATE           11 11  7  7  7  7  7  7  8  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  7 42 11 11 11 11  #  # 26 26 11 11
                         8 TIMESTAMP      11 11  8  8  8  8  8  8  8  9 10 11  8 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  8 42 11 11 11 11  #  # 26 26 11 11
                         9 FLOAT          11 11  9  9  9  9  9  9  9  9 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26  9  9 11 11 11 11 11 11 26 26 11 11
                        10 DOUBLE         11 11 10 10 10 10 10 10 10 10 10 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 27 11 11 11 11 11 11 11 11 11 11 11 11 26 10 10 11 11 11 11 11 11 49 50 11 11
                        11 STRING         11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 11 11 28 29 30 31 32 33 11 11 11 11 11 11 11 11 11 43 44 45 46 47 48 11 11 11 11
                        12 SYMBOL         11 11 11 11 11 11 11 11  8 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 11 42 11 11 11 11 11 11 26 26 11 11
                        13 LONG256        11 11 11 11 11 11 11 11 11 11 11 11 11 13  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 13 11 11 11 11 11 11 11 26 26 11 11
                        14 GEOBYTE         -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 26 11 11 11 11 11 11  -  -  -  -  -  -  -  -  - 11 11 11 11 11 11 26 26  -  -
                        15 GEOSHORT        -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 26 11 11 11 11 11 11  -  -  -  -  -  -  -  -  - 11 11 11 11 11 11 26 26  -  -
                        16 GEOINT          -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 26 11 11 11 11 11 11  -  -  -  -  -  -  -  -  - 11 11 11 11 11 11 26 26  -  -
                        17 GEOLONG         -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  - 26 11 11 11 11 11 11  -  -  -  -  -  -  -  -  - 11 11 11 11 11 11 26 26  -  -
                        18 BINARY         11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 18 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 18 11 11 11 11 11 11 11 26 26 11 11
                        19 UUID           11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 19 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 19 11 11 11 11 11 11 11 26 26 11 11
                        20 CURSOR         11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 20 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 20 11 11 11 11 11 11 11 26 26 11 11
                        21 VAR_ARG        11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 21 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 21 11 11 11 11 11 11 11 26 26 11 11
                        22 RECORD         11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 22 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 22 11 11 11 11 11 11 11 26 26 11 11
                        23 GEOHASH        11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 23 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 23 11 11 11 11 11 11 11 26 26 11 11
                        24 LONG128        11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 24 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 24 11 11 11 11 11 11 11 26 26 11 11
                        25 IPv4           11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 25 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 25 11 11 11 11 11 11 11 26 26 11 11
                        26 VARCHAR        26 26 26 26 26 26 26 26 26 26 26 11 26 26  -  -  -  - 26 26 26 26 26 26 26 26 26 26 28 29 30 31 32 33 26 26 26 26 26 26 26 26 26 43 44 45 46 47 48 26 26 26 26
                        27 ARRAY          26 26 26 26 26 26 26 26 26 26 27 11 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 27 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26
                        28 DECIMAL8       11 11 11 11 11 11 11 11 11 11 11 28 11 11 11 11 11 11 11 11 11 11 11 11 11 11 28 26 11 11 11 11 11 11 11 11 11 11 11 11 28 11 11 11 11 11 11 11 11 26 26 11 11
                        29 DECIMAL16      11 11 11 11 11 11 11 11 11 11 11 29 11 11 11 11 11 11 11 11 11 11 11 11 11 11 29 26 11 11 11 11 11 11 11 11 11 11 11 11 29 11 11 11 11 11 11 11 11 26 26 11 11
                        30 DECIMAL32      11 11 11 11 11 11 11 11 11 11 11 30 11 11 11 11 11 11 11 11 11 11 11 11 11 11 30 26 11 11 11 11 11 11 11 11 11 11 11 11 30 11 11 11 11 11 11 11 11 26 26 11 11
                        31 DECIMAL64      11 11 11 11 11 11 11 11 11 11 11 31 11 11 11 11 11 11 11 11 11 11 11 11 11 11 31 26 11 11 11 11 11 11 11 11 11 11 11 11 31 11 11 11 11 11 11 11 11 26 26 11 11
                        32 DECIMAL128     11 11 11 11 11 11 11 11 11 11 11 32 11 11 11 11 11 11 11 11 11 11 11 11 11 11 32 26 11 11 11 11 11 11 11 11 11 11 11 11 32 11 11 11 11 11 11 11 11 26 26 11 11
                        33 DECIMAL256     11 11 11 11 11 11 11 11 11 11 11 33 11 11 11 11 11 11 11 11 11 11 11 11 11 11 33 26 11 11 11 11 11 11 11 11 11 11 11 11 33 11 11 11 11 11 11 11 11 26 26 11 11
                        34 DECIMAL        11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 34 11 11 11 11 11 26 34 11 11 11 11 11 11 11 26 26 11 11
                        35 REGCLASS       11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 35 11 11 11 11 26 35 11 11 11 11 11 11 11 26 26 11 11
                        36 REGPROCEDURE   11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 36 11 11 11 26 36 11 11 11 11 11 11 11 26 26 11 11
                        37 ARRAY_STRING   11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 37 11 11 26 37 11 11 11 11 11 11 11 26 26 11 11
                        38 PARAMETER      11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 38 11 26 38 11 11 11 11 11 11 11 26 26 11 11
                        39 INTERVAL       11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 39 26 39 11 11 11 11 11 11 11 26 26 51 52
                        40 VARCHAR_SLICE  26 26 26 26 26 26 26 26 26 26 26 11 26 26  -  -  -  - 26 26 26 26 26 26 26 26 26 26 28 29 30 31 32 33 26 26 26 26 26 26 26 26 26 43 44 45 46 47 48 26 26 26 26
                        41 NULL            0  1  2  3 11  5  6  7  8  9 10 11 11 13  -  -  -  - 18 19 20 21 22 23 24 25 26 26 11 11 11 11 11 11 34 35 36 37 38 39 26 41 42 11 11 11 11 11 11 26 26 51 52
                        42 TIMESTAMP_NS   11 11 42 42 42 42 42 42 42  9 10 11 42 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 11 26 42 42 11 11 11 11  #  # 26 26 11 11
                        43 GEOHASH(1c)    11 11 11 11 11 11 11 11 11 11 11 43 11 11 11 11 11 11 11 11 11 11 11 11 11 11 43 26 11 11 11 11 11 11 11 11 11 11 11 11 43 11 11 43 43 43 43 11 11 26 26 11 11
                        44 GEOHASH(8b)    11 11 11 11 11 11 11 11 11 11 11 44 11 11 11 11 11 11 11 11 11 11 11 11 11 11 44 26 11 11 11 11 11 11 11 11 11 11 11 11 44 11 11 43 44 44 44 11 11 26 26 11 11
                        45 GEOHASH(31b)   11 11 11 11 11 11 11 11 11 11 11 45 11 11 11 11 11 11 11 11 11 11 11 11 11 11 45 26 11 11 11 11 11 11 11 11 11 11 11 11 45 11 11 43 44 45 45 11 11 26 26 11 11
                        46 GEOHASH(12c)   11 11 11 11 11 11 11 11 11 11 11 46 11 11 11 11 11 11 11 11 11 11 11 11 11 11 46 26 11 11 11 11 11 11 11 11 11 11 11 11 46 11 11 43 44 45 46 11 11 26 26 11 11
                        47 DECIMAL(5,2)   11 11 47  # 11  #  #  #  # 11 11 47 11 11 11 11 11 11 11 11 11 11 11 11 11 11 47 26 11 11 11 11 11 11 11 11 11 11 11 11 47 11  # 11 11 11 11 47 48 26 26 11 11
                        48 DECIMAL(18,3)  11 11 48 48 11 48  #  #  # 11 11 48 11 11 11 11 11 11 11 11 11 11 11 11 11 11 48 26 11 11 11 11 11 11 11 11 11 11 11 11 48 11  # 11 11 11 11 48 48 26 26 11 11
                        49 DOUBLE[]       26 26 26 26 26 26 26 26 26 26 49 11 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 49 50 26 26
                        50 DOUBLE[][]     26 26 26 26 26 26 26 26 26 26 50 11 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 26 50 50 26 26
                        51 INTERVAL(us)   11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 51 26 51 11 11 11 11 11 11 11 26 26 51 52
                        52 INTERVAL(ns)   11 11 11 11 11 11 11 11 11 11 11 11 11 11  -  -  -  - 11 11 11 11 11 11 11 11 26 26 11 11 11 11 11 11 11 11 11 11 11 52 26 52 11 11 11 11 11 11 11 26 26 52 52
                        # SHORT x DECIMAL(5,2) -> DECIMAL(7,2)
                        # INT x DECIMAL(5,2) -> DECIMAL(12,2)
                        # LONG x DECIMAL(5,2) -> DECIMAL(21,2)
                        # LONG x DECIMAL(18,3) -> DECIMAL(22,3)
                        # DATE x DECIMAL(5,2) -> DECIMAL(21,2)
                        # DATE x DECIMAL(18,3) -> DECIMAL(22,3)
                        # TIMESTAMP x DECIMAL(5,2) -> DECIMAL(21,2)
                        # TIMESTAMP x DECIMAL(18,3) -> DECIMAL(22,3)
                        # TIMESTAMP_NS x DECIMAL(5,2) -> DECIMAL(21,2)
                        # TIMESTAMP_NS x DECIMAL(18,3) -> DECIMAL(22,3)
                        # DECIMAL(5,2) x SHORT -> DECIMAL(7,2)
                        # DECIMAL(5,2) x INT -> DECIMAL(12,2)
                        # DECIMAL(5,2) x LONG -> DECIMAL(21,2)
                        # DECIMAL(5,2) x DATE -> DECIMAL(21,2)
                        # DECIMAL(5,2) x TIMESTAMP -> DECIMAL(21,2)
                        # DECIMAL(5,2) x TIMESTAMP_NS -> DECIMAL(21,2)
                        # DECIMAL(18,3) x LONG -> DECIMAL(22,3)
                        # DECIMAL(18,3) x DATE -> DECIMAL(22,3)
                        # DECIMAL(18,3) x TIMESTAMP -> DECIMAL(22,3)
                        # DECIMAL(18,3) x TIMESTAMP_NS -> DECIMAL(22,3)
                        """,
                renderType(SqlCodeGenerator::getUnionCastType)
        );
    }

    /**
     * Renders a unary tag relation: {@code X} when the relation yields the type's own tag,
     * {@code none} when it yields a negative "no arm" opcode, {@code !} when it throws.
     */
    private static String arm(Method relation, int tag, Object... args) {
        try {
            final int opcode = ((Number) relation.invoke(null, args)).intValue();
            return opcode == tag ? "X" : opcode < 0 ? "none" : "#" + opcode;
        } catch (InvocationTargetException e) {
            return "!";
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertGolden(String expected, String actual) {
        TestUtils.assertEquals(expected, actual);
    }

    /**
     * Renders GroupByColumnSink's relation by what {@link GroupByColumnSink#put} does with the
     * tag {@link GroupByColumnSink#argTag} yields: {@code X} when it appends the type's width,
     * {@code none} when it appends nothing.
     */
    private static String sizeArm(Method argTag, int tag, int type) throws Exception {
        final short argType = (short) argTag.invoke(null, type);
        Assert.assertEquals(tag, argType);
        final GroupByColumnSink columnSink = new GroupByColumnSink(64);
        try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB)) {
            columnSink.setAllocator(allocator);
            columnSink.of(0);
            columnSink.put(null, NullConstant.NULL, argType);
            final int appended = columnSink.size();
            if (appended == 0) {
                return "none";
            }
            Assert.assertEquals(ColumnType.sizeOf(type), appended);
            return "X";
        }
    }

    private static Method method(Class<?> clazz, String name, Class<?>... parameterTypes) throws NoSuchMethodException {
        final Method method = clazz.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private static String finish(StringSink sink) {
        // no trailing spaces: Java text blocks strip them from the expected value
        return sink.toString().replaceAll(" +\n", "\n");
    }

    private static int indexOf(int type) {
        for (int i = 0; i < TYPES.length; i++) {
            if (TYPES[i] == type) {
                return i;
            }
        }
        return -1;
    }

    private static String renderBoolean(Relation<Boolean> relation) {
        final StringSink sink = new StringSink();
        renderHeader(sink, 1);
        for (int from = 0; from < TYPES.length; from++) {
            renderRowLabel(sink, from);
            for (int to = 0; to < TYPES.length; to++) {
                try {
                    sink.put(relation.apply(TYPES[from], TYPES[to]) ? 'X' : '.');
                } catch (Throwable e) {
                    sink.put('!');
                }
            }
            sink.put('\n');
        }
        return finish(sink);
    }

    private static void renderHeader(StringSink sink, int cellWidth) {
        // two header lines: tens and ones of the column index
        for (int digitLine = 0; digitLine < 2; digitLine++) {
            sink.put("                 ");
            for (int col = 0; col < TYPES.length; col++) {
                for (int pad = 1; pad < cellWidth; pad++) {
                    sink.put(' ');
                }
                final int digit = digitLine == 0 ? col / 10 : col % 10;
                sink.put(digitLine == 0 && digit == 0 ? ' ' : (char) ('0' + digit));
            }
            sink.put('\n');
        }
    }

    private static String renderOverloadDistance() {
        // overloadDistance takes tags, not encoded types; a row lists every signature type the
        // row type may be passed as, closest first, as name=distance; unlisted cells are OVERLOAD_NONE
        final StringSink sink = new StringSink();
        for (short from = 0; from <= ColumnType.MAX_TAG; from++) {
            renderRowLabel(sink, from);
            for (int distance = ColumnType.OVERLOAD_FULL; distance < ColumnType.OVERLOAD_NONE; distance++) {
                boolean anyLeft = false;
                for (short to = 1; to <= ColumnType.MAX_TAG; to++) {
                    final int d = ColumnType.overloadDistance(from, to);
                    if (d == distance) {
                        sink.put(' ').put(LABELS[to]).put('=').put(d);
                    } else if (d > distance && d != ColumnType.OVERLOAD_NONE) {
                        anyLeft = true;
                    }
                }
                if (!anyLeft) {
                    break;
                }
            }
            sink.put('\n');
        }
        return finish(sink);
    }

    private static String renderPerType(Relation1<String> relation) {
        final StringSink sink = new StringSink();
        for (int i = 0; i < TYPES.length; i++) {
            renderRowLabel(sink, i);
            try {
                sink.put(relation.apply(TYPES[i]));
            } catch (Throwable e) {
                sink.put('!');
            }
            sink.put('\n');
        }
        return finish(sink);
    }

    private static String renderSparse(Relation<String> relation) {
        // a row lists name=value for every cell with a value, in type set order
        final StringSink sink = new StringSink();
        for (int from = 0; from < TYPES.length; from++) {
            renderRowLabel(sink, from);
            for (int to = 0; to < TYPES.length; to++) {
                String value;
                try {
                    value = relation.apply(TYPES[from], TYPES[to]);
                } catch (Throwable e) {
                    value = "!";
                }
                if (value != null) {
                    sink.put(' ').put(LABELS[to]).put('=').put(value);
                }
            }
            sink.put('\n');
        }
        return finish(sink);
    }

    private static void renderRowLabel(StringSink sink, int index) {
        if (index < 10) {
            sink.put(' ');
        }
        sink.put(index).put(' ').put(LABELS[index]);
        for (int i = LABELS[index].length(); i < 14; i++) {
            sink.put(' ');
        }
    }

    private static String renderType(Relation<Integer> relation) {
        final StringSink sink = new StringSink();
        final StringSink notes = new StringSink();
        renderHeader(sink, 3);
        for (int from = 0; from < TYPES.length; from++) {
            renderRowLabel(sink, from);
            for (int to = 0; to < TYPES.length; to++) {
                String cell;
                try {
                    final int type = relation.apply(TYPES[from], TYPES[to]);
                    final int index = indexOf(type);
                    if (index > -1) {
                        cell = Integer.toString(index);
                    } else if (type == -1) {
                        cell = "-";
                    } else {
                        cell = "#";
                        notes.put("# ").put(LABELS[from]).put(" x ").put(LABELS[to]).put(" -> ").put(typeLabel(type)).put('\n');
                    }
                } catch (Throwable e) {
                    cell = "!";
                }
                for (int pad = cell.length(); pad < 3; pad++) {
                    sink.put(' ');
                }
                sink.put(cell);
            }
            sink.put('\n');
        }
        sink.put(notes);
        return finish(sink);
    }

    private static String typeLabel(int type) {
        final int index = indexOf(type);
        if (index > -1) {
            return LABELS[index];
        }
        final String name = ColumnType.nameOf(type);
        return "unknown".equals(name) ? "0x" + Integer.toHexString(type) : name;
    }

    @FunctionalInterface
    private interface Relation<T> {
        T apply(int from, int to) throws Exception;
    }

    @FunctionalInterface
    private interface Relation1<T> {
        T apply(int type) throws Exception;
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
            // the label must name the type the way nameOf does, except where nameOf is ambiguous
            final String name = ColumnType.nameOf(extraTypes[i]);
            if (!name.equals(extraLabels[i]) && !extraLabels[i].startsWith(name + "(")) {
                throw new IllegalStateException("label " + extraLabels[i] + " does not match nameOf " + name);
            }
        }
    }
}
