/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.fastdouble.FastFloatParser;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.millitime.DateFormatUtils.*;

public class SqlUtil {

    static final CharSequenceHashSet disallowedAliases = new CharSequenceHashSet();
    private static final DateFormat[] DATE_FORMATS_FOR_TIMESTAMP;
    private static final int DATE_FORMATS_FOR_TIMESTAMP_SIZE;
    private static final ThreadLocal<Long256ConstantFactory> LONG256_FACTORY = new ThreadLocal<>(Long256ConstantFactory::new);

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

    public static long expectMicros(CharSequence tok, int position) throws SqlException {
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

    public static long expectSeconds(CharSequence tok, int position) throws SqlException {
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
            if (nChars > 1) {
                throw SqlException.$(position + k, "expected single letter interval qualifier in ").put(tok);
            }

            switch (tok.charAt(k)) {
                case 's': // seconds
                    return interval;
                case 'm': // minutes
                    return interval * Timestamps.MINUTE_SECONDS;
                case 'h': // hours
                    return interval * Timestamps.HOUR_SECONDS;
                case 'd': // days
                    return interval * Timestamps.DAY_SECONDS;
                default:
                    break;
            }
        } catch (NumericException ex) {
            // Ignored
        }

        throw SqlException.$(position + len, "invalid interval qualifier ").put(tok);
    }

    /**
     * Fetches next non-whitespace token that's not part of single or multiline comment.
     *
     * @param lexer input lexer
     * @return with next valid token or null if end of input is reached .
     */
    public static CharSequence fetchNext(GenericLexer lexer) throws SqlException {
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
                // unclosed quote check
                if (cs.length() == 1 && cs.charAt(0) == '"') {
                    throw SqlException.$(lexer.lastTokenPosition(), "unclosed quotation mark");
                }
                return cs;
            }
        }
        return null;
    }

    public static byte implicitCastAsByte(long value, int fromType) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.BYTE);
    }

    public static char implicitCastAsChar(long value, int fromType) {
        if (value >= 0 && value <= 9) {
            return (char) (value + '0');
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.CHAR);
    }

    public static float implicitCastAsFloat(double value, int fromType) {
        if ((value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) || Double.isNaN(value)) {
            return (float) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.FLOAT);
    }

    public static int implicitCastAsInt(long value, int fromType) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }

        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.INT);
    }

    public static short implicitCastAsShort(long value, int fromType) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return (short) value;
        }
        throw ImplicitCastException.inconvertibleValue(value, fromType, ColumnType.SHORT);
    }

    // used by bytecode assembler
    @SuppressWarnings("unused")
    public static byte implicitCastCharAsByte(char value, int toType) {
        return implicitCastCharAsType(value, toType);
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
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.CHAR, toType);
    }

    public static byte implicitCastCharAsType(char value, int toType) {
        byte v = (byte) (value - '0');
        if (v > -1 && v < 10) {
            return v;
        }
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.CHAR, toType);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastDoubleAsByte(double value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }

        if (Double.isNaN(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.BYTE);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static float implicitCastDoubleAsFloat(double value) {
        final double d = Math.abs(value);
        if ((d >= Float.MIN_VALUE && d <= Float.MAX_VALUE) || (Double.isNaN(value) || Double.isInfinite(value) || d == 0.0)) {
            return (float) value;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.FLOAT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastDoubleAsInt(double value) {
        if (Double.isNaN(value)) {
            return Numbers.INT_NaN;
        }
        return implicitCastAsInt((long) value, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastDoubleAsLong(double value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Double.isNaN(value)) {
            return Numbers.LONG_NaN;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastDoubleAsShort(double value) {
        if (Double.isNaN(value)) {
            return 0;
        }
        return implicitCastAsShort((long) value, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastFloatAsByte(float value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return (byte) value;
        }

        if (Float.isNaN(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.BYTE);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastFloatAsInt(float value) {
        if (value > Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return (int) value;
        }

        if (Float.isNaN(value)) {
            return Numbers.INT_NaN;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.INT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastFloatAsLong(float value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Float.isNaN(value)) {
            return Numbers.LONG_NaN;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastFloatAsShort(float value) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return (short) value;
        }

        if (Float.isNaN(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.SHORT);
    }

    public static long implicitCastGeoHashAsGeoHash(long value, int fromType, int toType) {
        final int fromBits = ColumnType.getGeoHashBits(fromType);
        final int toBits = ColumnType.getGeoHashBits(toType);
        assert fromBits >= toBits;
        return GeoHashes.widen(value, fromBits, toBits);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastIntAsByte(int value) {
        if (value != Numbers.INT_NaN) {
            return implicitCastAsByte(value, ColumnType.INT);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastIntAsShort(int value) {
        if (value != Numbers.INT_NaN) {
            return implicitCastAsShort(value, ColumnType.INT);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastLongAsByte(long value) {
        if (value != Numbers.LONG_NaN) {
            return implicitCastAsByte(value, ColumnType.LONG);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastLongAsInt(long value) {
        if (value != Numbers.LONG_NaN) {
            return implicitCastAsInt(value, ColumnType.LONG);
        }
        return Numbers.INT_NaN;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastLongAsShort(long value) {
        if (value != Numbers.LONG_NaN) {
            return implicitCastAsShort(value, ColumnType.LONG);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastShortAsByte(short value) {
        return implicitCastAsByte(value, ColumnType.SHORT);
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
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.BYTE);
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

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.CHAR);
    }

    public static long implicitCastStrAsDate(CharSequence value) {
        try {
            return DateFormatUtils.parseDate(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.DATE);
        }
    }

    public static double implicitCastStrAsDouble(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseDouble(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.DOUBLE);
            }
        }
        return Double.NaN;
    }

    public static float implicitCastStrAsFloat(CharSequence value) {
        if (value != null) {
            try {
                return FastFloatParser.parseFloat(value, 0, value.length(), true);
            } catch (NumericException ignored) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.FLOAT);
            }
        }
        return Float.NaN;
    }

    public static int implicitCastStrAsIPv4(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseIPv4(value);
            } catch (NumericException exception) {
                throw ImplicitCastException.instance().put("invalid ipv4 format: ").put(value);
            }
        }
        return Numbers.IPv4_NULL;
    }

    public static int implicitCastStrAsInt(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.INT);
            }
        }
        return Numbers.INT_NaN;
    }

    public static long implicitCastStrAsLong(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NaN;
    }

    public static Long256Constant implicitCastStrAsLong256(CharSequence value) {
        if (value == null || value.length() == 0) {
            return Long256NullConstant.INSTANCE;
        }
        int start = 0;
        int end = value.length();
        if (end > 2 && value.charAt(start) == '0' && (value.charAt(start + 1) | 32) == 'x') {
            start += 2;
        }
        Long256ConstantFactory factory = LONG256_FACTORY.get();
        Long256FromCharSequenceDecoder.decode(value, start, end, factory);
        return factory.pop();
    }

    public static void implicitCastStrAsLong256(CharSequence value, Long256Acceptor long256Acceptor) {
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
    }

    public static short implicitCastStrAsShort(@Nullable CharSequence value) {
        try {
            return value != null ? Numbers.parseShort(value) : 0;
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.SHORT);
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
                    return DATE_FORMATS_FOR_TIMESTAMP[i].parse(value, 0, hi, EN_LOCALE) * 1000L;
                } catch (NumericException ignore) {
                }
            }

            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.TIMESTAMP);
        }
        return Numbers.LONG_NaN;
    }

    public static void implicitCastStrAsUuid(CharSequence str, Uuid uuid) {
        if (str == null || str.length() == 0) {
            uuid.ofNull();
            return;
        }
        try {
            uuid.of(str);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(str, ColumnType.STRING, ColumnType.UUID);
        }
    }

    public static void implicitCastStrAsUuid(DirectUtf8Sequence str, Uuid uuid) {
        if (str == null || str.size() == 0) {
            uuid.ofNull();
            return;
        }
        try {
            uuid.of(str);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(str, ColumnType.STRING, ColumnType.UUID);
        }
    }

    public static boolean implicitCastUuidAsStr(long lo, long hi, CharSink sink) {
        if (Uuid.isNull(lo, hi)) {
            return false;
        }
        Numbers.appendUuid(lo, hi, sink);
        return true;
    }

    /**
     * Returns true if the model stands for a SELECT ... FROM tab; or a SELECT ... FROM tab WHERE ...; query.
     * We're aiming for potential page frame support with this check.
     */
    public static boolean isPlainSelect(QueryModel model) {
        while (model != null) {
            if (model.getSelectModelType() != QueryModel.SELECT_MODEL_NONE
                    || model.getGroupBy().size() > 0
                    || model.getJoinModels().size() > 1
                    || model.getLatestByType() != QueryModel.LATEST_BY_NONE
                    || model.getUnionModel() != null) {
                return false;
            }
            model = model.getNestedModel();
        }
        return true;
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
            boolean nonLiteral
    ) {
        final boolean disallowed = nonLiteral && disallowedAliases.contains(base);

        // short and sweet version
        if (indexOfDot == -1 && !disallowed && aliasToColumnMap.excludes(base)) {
            return base;
        }

        final CharacterStoreEntry characterStoreEntry = store.newEntry();

        if (indexOfDot == -1) {
            if (disallowed || Numbers.parseIntQuiet(base) != Numbers.INT_NaN) {
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

    static ExpressionNode nextLiteral(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return pool.next().of(ExpressionNode.LITERAL, token, 0, position);
    }

    private static class Long256ConstantFactory implements Long256Acceptor {
        private Long256Constant long256;

        @Override
        public void setAll(long l0, long l1, long l2, long l3) {
            assert long256 == null;
            long256 = new Long256Constant(l0, l1, l2, l3);
        }

        Long256Constant pop() {
            Long256Constant v = long256;
            long256 = null;
            return v;
        }
    }

    static {
        for (int i = 0, n = OperatorExpression.operators.size(); i < n; i++) {
            SqlUtil.disallowedAliases.add(OperatorExpression.operators.getQuick(i).token);
        }
        SqlUtil.disallowedAliases.add("");

        final DateFormatCompiler milliCompiler = new DateFormatCompiler();
        final DateFormat pgDateTimeFormat = milliCompiler.compile("y-MM-dd HH:mm:ssz");

        // we are using "millis" compiler deliberately because clients encode millis into strings
        DATE_FORMATS_FOR_TIMESTAMP = new DateFormat[]{
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                pgDateTimeFormat
        };

        DATE_FORMATS_FOR_TIMESTAMP_SIZE = DATE_FORMATS_FOR_TIMESTAMP.length;
    }
}
