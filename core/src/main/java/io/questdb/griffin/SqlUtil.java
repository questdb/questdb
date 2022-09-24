/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.fastdouble.FastFloatParser;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.millitime.DateFormatUtils.*;

public class SqlUtil {

    static final CharSequenceHashSet disallowedAliases = new CharSequenceHashSet();
    private static final DateFormat[] DATE_FORMATS;
    private static final int DATE_FORMATS_SIZE;
    private static final DateFormat[] DATE_FORMATS_FOR_TIMESTAMP;
    private static final int DATE_FORMATS_FOR_TIMESTAMP_SIZE;

    public static void addSelectStar(
            QueryModel model,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        model.addBottomUpColumn(nextColumn(queryColumnPool, expressionNodePool, "*", "*"));
        model.setArtificialStar(true);
    }

    // used by Copier assembler
    @SuppressWarnings("unused")
    public static long dateToTimestamp(long millis) {
        return millis != Numbers.LONG_NaN ? millis * 1000L : millis;
    }

    /**
     * Fetches next non-whitespace token that's not part of single or multiline comment.
     *
     * @param lexer input lexer
     * @return with next valid token or null if end of input is reached .
     */
    public static CharSequence fetchNext(GenericLexer lexer) {
        int blockCount = 0;
        boolean lineComment = false;
        while (lexer.hasNext()) {
            CharSequence cs = lexer.next();

            if (lineComment) {
                if (Chars.equals(cs, '\n') || Chars.equals(cs, '\r')) {
                    lineComment = false;
                }
                continue;
            }

            if (Chars.equals("--", cs)) {
                lineComment = true;
                continue;
            }

            if (Chars.equals("/*", cs)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("*/", cs) && blockCount > 0) {
                blockCount--;
                continue;
            }

            if (blockCount == 0 && GenericLexer.WHITESPACE.excludes(cs)) {
                return cs;
            }
        }
        return null;
    }

    // used by bytecode assembler
    @SuppressWarnings("unused")
    public static byte implicitCastCharAsByte(char value, int toType) {
        return implicitCastCharAsType(value, 0, toType);
    }

    @SuppressWarnings("unused")
    // used by copier bytecode assembler
    public static byte implicitCastCharAsGeoHash(char value, int toType) {
        int v;
        // '0' .. '9' and 'A-Z', excl 'A', 'I', 'L', 'O'
        if ((value >= '0' && value <= '9') || ((v = value | 32) > 'a' && v <= 'z' && v != 'i' && v != 'l' && v != 'o')) {
            int toBits = ColumnType.getGeoHashBits(toType);
            if (toBits < 5) {
                // widening
                return (byte) GeoHashes.widen(GeoHashes.encodeChar(value), 5, toBits);
            }

            if (toBits == 5) {
                return GeoHashes.encodeChar(value);
            }
        }
        throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.CHAR, toType);
    }

    public static byte implicitCastCharAsType(char value, int tupleIndex, int toType) {
        byte v = (byte) (value - '0');
        if (v > -1 && v < 10) {
            return v;
        }
        throw ImplicitCastException.inconvertibleValue(tupleIndex, value, ColumnType.CHAR, toType);
    }

    public static long implicitCastGeoHashAsGeoHash(long value, int fromType, int toType) {
        final int fromBits = ColumnType.getGeoHashBits(fromType);
        final int toBits = ColumnType.getGeoHashBits(toType);
        assert fromBits >= toBits;
        return GeoHashes.widen(value, fromBits, toBits);
    }

    public static byte implicitCastStrAsByte(CharSequence value) {
        if (value != null) {
            try {
                int res = Numbers.parseInt(value);
                if (res >= Byte.MIN_VALUE && res <= Byte.MAX_VALUE) {
                    return (byte) res;
                }
            } catch (NumericException ignore) {
            }
            throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.BYTE);
        }
        return 0;
    }

    public static char implicitCastStrAsChar(CharSequence value) {
        if (value == null || value.length() == 0) {
            return 0;
        }

        if (value.length() == 1) {
            return value.charAt(0);
        }

        throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.CHAR);
    }

    public static long implicitCastStrAsDate(CharSequence value) {
        if (value != null) {
            final int hi = value.length();
            for (int i = 0; i < DATE_FORMATS_SIZE; i++) {
                try {
                    return DATE_FORMATS[i].parse(value, 0, hi, DateFormatUtils.enLocale);
                } catch (NumericException ignore) {
                }
            }
            try {
                return Numbers.parseLong(value, 0, hi);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.DATE);
            }
        }
        return Numbers.LONG_NaN;
    }

    public static double implicitCastStrAsDouble(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseDouble(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.DOUBLE);
            }
        }
        return Double.NaN;
    }

    public static float implicitCastStrAsFloat(CharSequence value) {
        if (value != null) {
            try {
                return FastFloatParser.parseFloat(value, 0, value.length(), true);
            } catch (NumericException ignored) {
                throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.FLOAT);
            }
        }
        return Float.NaN;
    }

    public static int implicitCastStrAsInt(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.INT);
            }
        }
        return Numbers.INT_NaN;
    }

    public static long implicitCastStrAsLong(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NaN;
    }

    public static void implicitCastStrAsLong256(CharSequence value, Long256Acceptor long256Acceptor) {
        try {
            if (value != null) {
                Long256FromCharSequenceDecoder.decode(value, 0, value.length(), long256Acceptor);
            } else {
                long256Acceptor.setAll(
                        Long256Impl.NULL_LONG256.getLong0(),
                        Long256Impl.NULL_LONG256.getLong1(),
                        Long256Impl.NULL_LONG256.getLong2(),
                        Long256Impl.NULL_LONG256.getLong3()
                );
            }
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.LONG256);
        }
    }

    public static short implicitCastStrAsShort(@Nullable CharSequence value) {
        try {
            return value != null ? Numbers.parseShort(value) : 0;
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.SHORT);
        }
    }

    public static long implicitCastStrAsTimestamp(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }

            // Parse as ISO with variable length.
            try {
                return IntervalUtils.parseFloorPartialTimestamp(value);
            } catch (NumericException ignore) {
            }

            final int hi = value.length();
            for (int i = 0; i < DATE_FORMATS_FOR_TIMESTAMP_SIZE; i++) {
                try {
                    //
                    return DATE_FORMATS_FOR_TIMESTAMP[i].parse(value, 0, hi, enLocale) * 1000L;
                } catch (NumericException ignore) {
                }
            }

            throw ImplicitCastException.inconvertibleValue(0, value, ColumnType.STRING, ColumnType.TIMESTAMP);
        }
        return Numbers.LONG_NaN;
    }

    /**
     * Parses partial representation of timestamp with time zone.
     *
     * @param value      the characters representing timestamp
     * @param tupleIndex the tuple index for insert SQL, which inserts multiple rows at once
     * @param columnType the target column type, which might be different from timestamp
     * @return epoch offset
     * @throws ImplicitCastException inconvertible type error.
     */
    public static long parseFloorPartialTimestamp(CharSequence value, int tupleIndex, int columnType) {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(tupleIndex, value, ColumnType.STRING, columnType);
        }
    }

    static ExpressionNode nextLiteral(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return pool.next().of(ExpressionNode.LITERAL, token, 0, position);
    }

    static CharSequence createColumnAlias(
            CharacterStore store,
            CharSequence base,
            int indexOfDot,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap
    ) {
        return createColumnAlias(store, base, indexOfDot, aliasToColumnMap, false);
    }

    static CharSequence createColumnAlias(
            CharacterStore store,
            CharSequence base,
            int indexOfDot,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap,
            boolean cleanColumnNames
    ) {
        final boolean disallowed = cleanColumnNames && disallowedAliases.contains(base);

        // short and sweet version
        if (indexOfDot == -1 && !disallowed && aliasToColumnMap.excludes(base)) {
            return base;
        }

        final CharacterStoreEntry characterStoreEntry = store.newEntry();

        if (indexOfDot == -1) {
            if (disallowed) {
                characterStoreEntry.put("column");
            } else {
                characterStoreEntry.put(base);
            }
        } else {
            if (indexOfDot + 1 == base.length()) {
                characterStoreEntry.put("column");
            } else {
                characterStoreEntry.put(base, indexOfDot + 1, base.length());
            }
        }

        int len = characterStoreEntry.length();
        int sequence = 0;
        while (true) {
            if (sequence > 0) {
                characterStoreEntry.trimTo(len);
                characterStoreEntry.put(sequence);
            }
            sequence++;
            CharSequence alias = characterStoreEntry.toImmutable();
            if (aliasToColumnMap.excludes(alias)) {
                return alias;
            }
        }
    }

    static QueryColumn nextColumn(
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> sqlNodePool,
            CharSequence alias,
            CharSequence column
    ) {
        return queryColumnPool.next().of(alias, nextLiteral(sqlNodePool, column, 0));
    }

    static long expectMicros(CharSequence tok, int position) throws SqlException {
        int k = -1;

        final int len = tok.length();

        // look for end of digits
        for (int i = 0; i < len; i++) {
            char c = tok.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            throw SqlException.$(position + len, "expected interval qualifier in ").put(tok);
        }

        try {
            long interval = Numbers.parseLong(tok, 0, k);
            int nChars = len - k;
            if (nChars > 2) {
                throw SqlException.$(position + k, "expected 1/2 letter interval qualifier in ").put(tok);
            }

            switch (tok.charAt(k)) {
                case 's':
                    if (nChars == 1) {
                        // seconds
                        return interval * Timestamps.SECOND_MICROS;
                    }
                    break;
                case 'm':
                    if (nChars == 1) {
                        // minutes
                        return interval * Timestamps.MINUTE_MICROS;
                    } else {
                        if (tok.charAt(k + 1) == 's') {
                            // millis
                            return interval * Timestamps.MILLI_MICROS;
                        }
                    }
                    break;
                case 'h':
                    if (nChars == 1) {
                        // hours
                        return interval * Timestamps.HOUR_MICROS;
                    }
                    break;
                case 'd':
                    if (nChars == 1) {
                        // days
                        return interval * Timestamps.DAY_MICROS;
                    }
                    break;
                case 'u':
                    if (nChars == 2 && tok.charAt(k + 1) == 's') {
                        return interval;
                    }
                    break;
                default:
                    break;
            }
        } catch (NumericException ex) {
            // Ignored
        }

        throw SqlException.$(position + len, "invalid interval qualifier ").put(tok);
    }

    static {
        for (int i = 0, n = OperatorExpression.operators.size(); i < n; i++) {
            SqlUtil.disallowedAliases.add(OperatorExpression.operators.getQuick(i).token);
        }

        final DateFormatCompiler milliCompiler = new DateFormatCompiler();
        final DateFormat pgDateTimeFormat = milliCompiler.compile("y-MM-dd HH:mm:ssz");

        DATE_FORMATS = new DateFormat[]{
                pgDateTimeFormat,
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                UTC_FORMAT
        };

        DATE_FORMATS_SIZE = DATE_FORMATS.length;

        // we are using "millis" compiler deliberately because clients encode millis into strings
        DATE_FORMATS_FOR_TIMESTAMP = new DateFormat[]{
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                pgDateTimeFormat
        };

        DATE_FORMATS_FOR_TIMESTAMP_SIZE = DATE_FORMATS_FOR_TIMESTAMP.length;

    }
}
