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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.Long256NullConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.Long256Impl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.fastdouble.FastFloatParser;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_FORMAT;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_Z_FORMAT;

public class SqlUtil {

    static final LowerCaseCharSequenceHashSet disallowedAliases = new LowerCaseCharSequenceHashSet();
    private static final DateFormat[] IMPLICIT_CAST_FORMATS;
    private static final int IMPLICIT_CAST_FORMATS_SIZE;
    private static final ThreadLocal<Long256ConstantFactory> LONG256_FACTORY = new ThreadLocal<>(Long256ConstantFactory::new);

    public static void addSelectStar(
            QueryModel model,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        model.addBottomUpColumn(nextColumn(queryColumnPool, expressionNodePool, "*", "*", 0));
        model.setArtificialStar(true);
    }

    public static long castPGDates(CharSequence value, int fromColumnType, TimestampDriver driver) {
        final int hi = value.length();
        for (int i = 0; i < IMPLICIT_CAST_FORMATS_SIZE; i++) {
            try {
                return driver.fromDate(IMPLICIT_CAST_FORMATS[i].parse(value, 0, hi, EN_LOCALE));
            } catch (NumericException ignore) {
            }
        }
        throw ImplicitCastException.inconvertibleValue(value, fromColumnType, driver.getTimestampType());
    }

    public static CharSequence createExprColumnAlias(
            CharacterStore store,
            CharSequence base,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap,
            int maxLength
    ) {
        return createExprColumnAlias(store, base, aliasToColumnMap, maxLength, false);
    }

    public static CharSequence createExprColumnAlias(
            CharacterStore store,
            CharSequence base,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap,
            int maxLength,
            boolean nonLiteral
    ) {
        // We need to wrap disallowed aliases with double quotes to avoid later conflicts.
        final int baseLen = base.length();
        final int indexOfDot = Chars.indexOfLastUnquoted(base, '.');
        final boolean prefixedLiteral = !nonLiteral && indexOfDot > -1 && indexOfDot < baseLen - 1;
        boolean quote = nonLiteral
                ? !Chars.isDoubleQuoted(base) && (indexOfDot > -1 || disallowedAliases.contains(base))
                : indexOfDot > -1 && disallowedAliases.contains(base, indexOfDot + 1, base.length());

        // early exit for simple cases
        if (!prefixedLiteral && !quote && aliasToColumnMap.excludes(base)
                && baseLen > 0 && baseLen <= maxLength && base.charAt(baseLen - 1) != ' ') {
            return base;
        }

        final int start = prefixedLiteral ? indexOfDot + 1 : 0;
        int len = baseLen - start;
        final CharacterStoreEntry entry = store.newEntry();
        final int entryLen = entry.length();
        if (quote) {
            entry.put('"');
            len += 2;
        }
        entry.put(base, start, baseLen);

        int sequence = 1;
        int seqSize = 0;
        while (true) {
            if (sequence > 1) {
                seqSize = (int) Math.log10(sequence) + 2; // Remember the _
            }
            len = Math.min(len, maxLength - seqSize - (quote ? 1 : 0));

            // We don't want the alias to finish with a space.
            if (!quote && len > 0 && base.charAt(start + len - 1) == ' ') {
                final int lastSpace = Chars.lastIndexOfDifferent(base, start, start + len, ' ') - start;
                if (lastSpace > 0) {
                    len = lastSpace + 1;
                }
            }

            entry.trimTo(entryLen + len - (quote ? 1 : 0));
            if (sequence > 1) {
                entry.put('_');
                entry.put(sequence);
            }
            if (quote) {
                entry.put('"');
            }
            CharSequence alias = entry.toImmutable();
            if (len > 0 && aliasToColumnMap.excludes(alias)) {
                return alias;
            }
            sequence++;
        }
    }

    public static long expectMicros(CharSequence tok, int position) throws SqlException {
        final int len = tok.length();
        final int k = findEndOfDigitsPos(tok, len, position);

        try {
            long interval = Numbers.parseLong(tok, 0, k);
            int nChars = len - k;
            if (nChars > 2) {
                throw SqlException.$(position + k, "expected 1/2 letter interval qualifier in ").put(tok);
            }
            TimestampDriver driver = MicrosTimestampDriver.INSTANCE;

            switch (tok.charAt(k)) {
                case 's':
                    if (nChars == 1) {
                        // seconds
                        return driver.fromSeconds(interval);
                    }
                    break;
                case 'm':
                    if (nChars == 1) {
                        // minutes
                        return driver.fromMinutes((int) interval);
                    } else {
                        if (tok.charAt(k + 1) == 's') {
                            // millis
                            return driver.fromMillis((int) interval);
                        }
                    }
                    break;
                case 'h':
                    if (nChars == 1) {
                        // hours
                        return driver.fromHours((int) interval);
                    }
                    break;
                case 'd':
                    if (nChars == 1) {
                        // days
                        return driver.fromDays((int) interval);
                    }
                    break;
                case 'u':
                    if (nChars == 2 && tok.charAt(k + 1) == 's') {
                        return driver.fromMicros(interval);
                    }
                    break;
                case 'n':
                    if (nChars == 2 && tok.charAt(k + 1) == 's') {
                        return driver.fromNanos(interval);
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
        final int len = tok.length();
        final int k = findEndOfDigitsPos(tok, len, position);

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
                    return interval * Micros.MINUTE_SECONDS;
                case 'h': // hours
                    return interval * Micros.HOUR_SECONDS;
                case 'd': // days
                    return interval * Micros.DAY_SECONDS;
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
        return fetchNext(lexer, false);
    }

    /**
     * Fetches next non-whitespace token that's not part of single or multiline comment.
     *
     * @param lexer        The input lexer containing the token stream to process
     * @param includeHints If true, hint block markers (/*+) are treated as valid tokens and returned;
     *                     if false, hint blocks are treated as comments and skipped
     * @return The next meaningful token as a CharSequence, or null if the end of input is reached
     * @throws SqlException If a parsing error occurs while processing the token stream
     * @see #fetchNextHintToken(GenericLexer) For handling tokens within hint blocks
     */
    public static CharSequence fetchNext(GenericLexer lexer, boolean includeHints) throws SqlException {
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

            if (Chars.equals("/*+", cs) && (!includeHints || blockCount > 0)) {
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

    /**
     * Fetches the next non-whitespace, non-comment hint token from the lexer.
     * <p>
     * This method should only be called after entering a hint block. Specifically,
     * a previous call to {@link #fetchNext(GenericLexer, boolean)} must have returned
     * a hint start token (<code>/*+</code>) before this method can be used.
     * <p>
     * The method processes the input stream, skipping over any nested comments and whitespace,
     * and returns the next meaningful hint token. This allows for clean parsing of hint
     * content without manual handling of comments and formatting characters.
     * <p>
     * When the end of the hint block is reached, the method returns null, indicating
     * no more hint tokens are available for processing.
     * <p>
     * If a hint contains unbalanced quotes, the method will NOT throw an exception, instead
     * it will consume all tokens until the end of the hint block is reached and then return null indicating
     * the end of the hint block.
     *
     * @param lexer The input lexer containing the token stream to process
     * @return The next meaningful hint token, or null if the end of the hint block is reached
     * @see #fetchNext(GenericLexer, boolean) For entering the hint block initially
     */
    public static CharSequence fetchNextHintToken(GenericLexer lexer) {
        int blockCount = 0;
        boolean lineComment = false;
        boolean inError = false;
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

            if (Chars.equals("/*+", cs)) {
                // nested hints are treated as regular comments
                blockCount++;
                continue;
            }

            // end of hints or a nested comment
            if (Chars.equals("*/", cs)) {
                if (blockCount > 0) {
                    blockCount--;
                    continue;
                }
                return null;
            }

            if (!inError && blockCount == 0 && GenericLexer.WHITESPACE.excludes(cs)) {
                // unclosed quote check
                if (cs.length() == 1 && cs.charAt(0) == '"') {
                    inError = true;
                } else {
                    return cs;
                }
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
        if ((value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) || Numbers.isNull(value)) {
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

        if (Numbers.isNull(value)) {
            return 0;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.BYTE);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static float implicitCastDoubleAsFloat(double value) {
        final double d = Math.abs(value);
        if ((d >= Float.MIN_VALUE && d <= Float.MAX_VALUE) || (Numbers.isNull(value) || d == 0.0)) {
            return (float) value;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.FLOAT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastDoubleAsInt(double value) {
        if (Numbers.isNull(value)) {
            return Numbers.INT_NULL;
        }
        return implicitCastAsInt((long) value, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastDoubleAsLong(double value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Numbers.isNull(value)) {
            return Numbers.LONG_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.DOUBLE, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastDoubleAsShort(double value) {
        if (Numbers.isNull(value)) {
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

        if (Numbers.isNull(value)) {
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

        if (Numbers.isNull(value)) {
            return Numbers.INT_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.INT);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static long implicitCastFloatAsLong(float value) {
        if (value > Long.MIN_VALUE && value <= Long.MAX_VALUE) {
            return (long) value;
        }

        if (Numbers.isNull(value)) {
            return Numbers.LONG_NULL;
        }

        throw ImplicitCastException.inconvertibleValue(value, ColumnType.FLOAT, ColumnType.LONG);
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastFloatAsShort(float value) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return (short) value;
        }

        if (Numbers.isNull(value)) {
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
        if (value != Numbers.INT_NULL) {
            return implicitCastAsByte(value, ColumnType.INT);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastIntAsShort(int value) {
        if (value != Numbers.INT_NULL) {
            return implicitCastAsShort(value, ColumnType.INT);
        }
        return 0;
    }

    public static boolean implicitCastLong256AsStr(Long256 long256, CharSink<?> sink) {
        if (Long256Impl.isNull(long256)) {
            return false;
        }
        Numbers.appendLong256(long256, sink);
        return true;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static byte implicitCastLongAsByte(long value) {
        if (value != Numbers.LONG_NULL) {
            return implicitCastAsByte(value, ColumnType.LONG);
        }
        return 0;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static int implicitCastLongAsInt(long value) {
        if (value != Numbers.LONG_NULL) {
            return implicitCastAsInt(value, ColumnType.LONG);
        }
        return Numbers.INT_NULL;
    }

    @SuppressWarnings("unused")
    // used by the row copier
    public static short implicitCastLongAsShort(long value) {
        if (value != Numbers.LONG_NULL) {
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
        return implicitCastStrVarcharAsDate0(value, ColumnType.STRING);
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
                throw ImplicitCastException.instance().put("invalid IPv4 format: ").put(value);
            }
        }
        return Numbers.IPv4_NULL;
    }

    public static int implicitCastStrAsIPv4(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseIPv4(value);
            } catch (NumericException exception) {
                throw ImplicitCastException.instance().put("invalid IPv4 format: ").put(value);
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
        return Numbers.INT_NULL;
    }

    public static long implicitCastStrAsLong(CharSequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NULL;
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

    public static void implicitCastStrAsUuid(Utf8Sequence str, Uuid uuid) {
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

    public static ArrayView implicitCastStringAsDoubleArray(CharSequence value, DoubleArrayParser parser, int expectedType) {
        try {
            parser.of(value, ColumnType.decodeArrayDimensionality(expectedType));
        } catch (IllegalArgumentException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.STRING, expectedType);
        }
        return parser;
    }

    public static boolean implicitCastUuidAsStr(long lo, long hi, CharSink<?> sink) {
        if (Uuid.isNull(lo, hi)) {
            return false;
        }
        Numbers.appendUuid(lo, hi, sink);
        return true;
    }

    public static byte implicitCastVarcharAsByte(Utf8Sequence value) {
        if (value != null) {
            try {
                int res = Numbers.parseInt(value);
                if (res >= Byte.MIN_VALUE && res <= Byte.MAX_VALUE) {
                    return (byte) res;
                }
            } catch (NumericException ignore) {
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.BYTE);
        }
        return 0;
    }

    public static char implicitCastVarcharAsChar(Utf8Sequence value) {
        if (value == null || value.size() == 0) {
            return 0;
        }

        int encodedResult = Utf8s.utf8CharDecode(value);
        short consumedBytes = Numbers.decodeLowShort(encodedResult);
        if (consumedBytes == value.size()) {
            return (char) Numbers.decodeHighShort(encodedResult);
        }
        throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.CHAR);
    }

    public static long implicitCastVarcharAsDate(CharSequence value) {
        return implicitCastStrVarcharAsDate0(value, ColumnType.VARCHAR);
    }

    public static double implicitCastVarcharAsDouble(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseDouble(value.asAsciiCharSequence());
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.DOUBLE);
            }
        }
        return Double.NaN;
    }

    public static ArrayView implicitCastVarcharAsDoubleArray(Utf8Sequence value, DoubleArrayParser parser, int expectedType) {
        try {
            parser.of(value.asAsciiCharSequence(), ColumnType.decodeArrayDimensionality(expectedType));
        } catch (IllegalArgumentException e) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, expectedType);
        }
        return parser;
    }

    public static float implicitCastVarcharAsFloat(Utf8Sequence value) {
        if (value != null) {
            try {
                CharSequence ascii = value.asAsciiCharSequence();
                return FastFloatParser.parseFloat(ascii, 0, ascii.length(), true);
            } catch (NumericException ignored) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.FLOAT);
            }
        }
        return Float.NaN;
    }

    public static int implicitCastVarcharAsInt(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.INT);
            }
        }
        return Numbers.INT_NULL;
    }

    public static long implicitCastVarcharAsLong(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.LONG);
            }
        }
        return Numbers.LONG_NULL;
    }

    public static short implicitCastVarcharAsShort(@Nullable Utf8Sequence value) {
        try {
            return value != null ? Numbers.parseShort(value) : 0;
        } catch (NumericException ignore) {
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.SHORT);
        }
    }

    public static boolean isNotPlainSelectModel(QueryModel model) {
        return model.getTableName() != null
                || model.getGroupBy().size() > 0
                || model.getJoinModels().size() > 1
                || model.getLatestByType() != QueryModel.LATEST_BY_NONE
                || model.getUnionModel() != null;
    }

    public static boolean isParallelismSupported(ObjList<Function> functions) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).supportsParallelism()) {
                return false;
            }
        }
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

    public static ExpressionNode nextExpr(ObjectPool<ExpressionNode> pool, int exprNodeType, CharSequence token, int position) {
        return pool.next().of(exprNodeType, token, 0, position);
    }

    public static int parseArrayDimensionality(GenericLexer lexer, int columnType, int typeTagPosition) throws SqlException {
        if (ColumnType.tagOf(columnType) == ColumnType.ARRAY) {
            throw SqlException.position(typeTagPosition).put("the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
        }
        boolean hasNumericDimensionality = false;
        int dimensionalityFirstPos = -1;
        int dim = 0;
        do {
            CharSequence tok = fetchNext(lexer);
            if (Chars.equalsNc(tok, '[')) {
                // Check for whitespace before '[' in array type declaration
                int openBracketPosition = lexer.lastTokenPosition();
                if (openBracketPosition > 0 && Character.isWhitespace(lexer.getContent().charAt(openBracketPosition - 1))) {
                    throw SqlException.position(openBracketPosition)
                            .put("array type requires no whitespace between type and brackets");
                }

                // could be a start of array type
                tok = fetchNext(lexer);

                if (Chars.equalsNc(tok, ']')) {
                    dim++;
                } else {
                    // check if someone is trying to specify numeric dimensionality, e.g. double[1]
                    try {
                        Numbers.parseInt(tok);
                        hasNumericDimensionality = true;
                        if (dimensionalityFirstPos == -1) {
                            dimensionalityFirstPos = lexer.lastTokenPosition();
                        }
                        continue;
                    } catch (NumericException ignore) {
                        // never mind
                    }

                    // we are looking at something like `type[something` right now, lets consume the rest of the
                    // lexer until we hit one of the following: `]`, `,` or `)` to get the complete picture of
                    // what the user provide. We will show what we see and offer what we expect to see.

                    // we will fail here regardless, so we do not care about the state of the parser
                    int stopPos;
                    do {
                        int p = lexer.lastTokenPosition();
                        tok = fetchNext(lexer);
                        if (tok == null || Chars.equals(tok, ']') || Chars.equals(tok, ',') || Chars.equals(tok, ')')) {
                            if (!Chars.equalsNc(tok, ']')) {
                                stopPos = p;
                            } else {
                                stopPos = lexer.lastTokenPosition();
                            }
                            break;
                        }
                    } while (true);

                    SqlException e = SqlException.position(openBracketPosition)
                            .put("syntax error at column type definition, expected array type: '")
                            .put(ColumnType.nameOf(columnType));

                    // add dimensionality we found so far
                    for (int i = 0, n = dim + 1; i < n; i++) {
                        e.put("[]");
                    }
                    e.put("...', but found: '")
                            .put(lexer.getContent(), typeTagPosition, stopPos)
                            .put('\'');

                    throw e;
                }
            } else {
                lexer.unparseLast();
                break;
            }
        } while (true);

        if (hasNumericDimensionality) {
            throw SqlException.$(dimensionalityFirstPos, "arrays do not have a fixed size, remove the number");
        }
        return dim;
    }

    public static int toPersistedType(@NotNull CharSequence tok, int tokPosition) throws SqlException {
        final int columnType = ColumnType.typeOf(tok);
        if (columnType == -1) {
            throw SqlException.$(tokPosition, "unsupported column type: ").put(tok);
        }
        if (ColumnType.isPersisted(ColumnType.tagOf(columnType))) {
            return columnType;
        }
        throw SqlException.$(tokPosition, "non-persisted type: ").put(tok);
    }

    private static int findEndOfDigitsPos(CharSequence tok, int tokLen, int tokPosition) throws SqlException {
        int k = -1;
        // look for end of digits
        for (int i = 0; i < tokLen; i++) {
            char c = tok.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            throw SqlException.$(tokPosition + tokLen, "expected interval qualifier in ").put(tok);
        }
        return k;
    }

    private static long implicitCastStrVarcharAsDate0(CharSequence value, int columnType) {
        assert columnType == ColumnType.VARCHAR || columnType == ColumnType.STRING;
        try {
            return DateFormatUtils.parseDate(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(value, columnType, ColumnType.DATE);
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
            if (disallowed || Numbers.parseIntQuiet(base) != Numbers.INT_NULL) {
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
            CharSequence column,
            int position
    ) {
        return queryColumnPool.next().of(alias, nextLiteral(sqlNodePool, column, position));
    }

    static ExpressionNode nextConstant(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return nextExpr(pool, ExpressionNode.CONSTANT, token, position);
    }

    static ExpressionNode nextLiteral(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return nextExpr(pool, ExpressionNode.LITERAL, token, position);
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
        // note: it's safe to take any registry (new or old) because we don't use precedence here
        OperatorRegistry registry = OperatorExpression.getRegistry();
        for (int i = 0, n = registry.operators.size(); i < n; i++) {
            SqlUtil.disallowedAliases.add(registry.operators.getQuick(i).operator.token);
        }
        SqlUtil.disallowedAliases.add("");

        final DateFormatCompiler milliCompiler = new DateFormatCompiler();
        final DateFormat pgDateTimeFormat = milliCompiler.compile("y-MM-dd HH:mm:ssz");

        // we are using "millis" compiler deliberately because clients encode millis into strings
        IMPLICIT_CAST_FORMATS = new DateFormat[]{
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                pgDateTimeFormat
        };

        IMPLICIT_CAST_FORMATS_SIZE = IMPLICIT_CAST_FORMATS.length;
    }
}
