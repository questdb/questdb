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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.model.IntervalSearch;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

/**
 * Immutable published value; full timestamp types retain their precision.
 */
public final class ConstantExpression extends BoundExpression {
    public static final ObjectFactory<ConstantExpression> FACTORY = ConstantExpression::new;
    private final StringSink decodedText = new StringSink();
    private final Long256Impl long256 = new Long256Impl();
    private RequirementFact administrativeFact;
    private long hh;
    private long hl;
    private IntervalSearch intervalSearch;
    private boolean isLiteral;
    private long lh;
    private long longValue;
    private FunctionExpression source;
    private Object value;

    /**
     * Puts the SQL string literal that spells {@code text}: the text in single quotes, each single quote doubled.
     */
    public static void putSqlLiteral(Utf16Sink sink, CharSequence text) {
        sink.put('\'');
        for (int i = 0, n = text.length(); i < n; i++) {
            final char c = text.charAt(i);
            if (c == '\'') {
                sink.put('\'');
            }
            sink.put(c);
        }
        sink.put('\'');
    }

    @Override
    public void clear() {
        super.clear();
        administrativeFact = null;
        hh = 0;
        hl = 0;
        lh = 0;
        intervalSearch = null;
        isLiteral = false;
        source = null;
        longValue = 0;
        value = null;
    }

    /**
     * The administrative fact of the first call the fold that made the constant evaluated, in the order code
     * generation builds calls, see {@link RequirementFact#firstAdministrative}, or null. Master's code generation
     * builds that call before it folds it, so the PIVOT IN check of a view rejects it as it rejects the calls a
     * description still shows. The fact has no part in the value or the spelling of the constant: unlike
     * {@link #getSource()}, which interval extraction, filter pushdown and the JIT read, a fold that keeps no source
     * keeps the fact all the same.
     */
    public RequirementFact getAdministrativeFact() {
        return administrativeFact;
    }

    /**
     * The characters of a STRING, SYMBOL, VARCHAR or CHAR constant: the text {@link #getText()} returns, or the one
     * character of a CHAR constant, which the constant puts into a sink it owns; null for a NULL text value or any
     * other type. A CHAR constant holds one character even when that is the NULL character, so a caller that reads a
     * CHAR NULL as NULL checks the value first.
     */
    public CharSequence getChars() {
        if (ColumnType.tagOf(getDataType()) == ColumnType.CHAR) {
            decodedText.clear();
            decodedText.put((char) longValue);
            return decodedText;
        }
        return getText();
    }

    public long getDecimalHh() {
        return hh;
    }

    public long getDecimalHl() {
        return hl;
    }

    public long getDecimalLh() {
        return lh;
    }

    public double getDoubleValue() {
        return Double.longBitsToDouble(longValue);
    }

    public float getFloatValue() {
        return Float.intBitsToFloat((int) longValue);
    }

    public long getIntervalHi() {
        return lh;
    }

    /**
     * The intervals the binder parsed from the text of the constant, the right operand of an interval IN, see
     * {@link #withIntervalSearch}; null for any other constant.
     */
    public IntervalSearch getIntervalSearch() {
        return intervalSearch;
    }

    /**
     * The SQL spelling of a floating-point literal, or null when the value is not spelled as one.
     */
    public CharSequence getLiteralText() {
        final int tag = ColumnType.tagOf(getDataType());
        return tag == ColumnType.DOUBLE || tag == ColumnType.FLOAT ? (CharSequence) value : null;
    }

    public long getLong128Hi() {
        return lh;
    }

    public long getLong128Lo() {
        return longValue;
    }

    public Long256 getLong256Value() {
        return (Long256) value;
    }

    public long getLongValue() {
        return longValue;
    }

    /**
     * The folded operator expression, when the value is folded from an operator call.
     */
    public FunctionExpression getSource() {
        return source;
    }

    public String getStrValue() {
        return (String) value;
    }

    /**
     * The text {@code =} compares with a SYMBOL by: the text {@link #getText()} returns for a STRING, SYMBOL or
     * VARCHAR constant, or the decimal text of a BYTE, SHORT, INT or LONG constant as {@code =} formats it, which the
     * constant puts into a sink it owns; null for a NULL value or any other type, CHAR included.
     */
    public CharSequence getSymbolKeyText() {
        final int tag = ColumnType.tagOf(getDataType());
        switch (tag) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                final long key = tag == ColumnType.INT ? Numbers.intToLong((int) longValue) : longValue;
                if (key == Numbers.LONG_NULL) {
                    return null;
                }
                decodedText.clear();
                decodedText.put(key);
                return decodedText;
            }
            default -> {
                return getText();
            }
        }
    }

    /**
     * The UTF-16 text of a STRING, SYMBOL or VARCHAR constant; null for a NULL value or any other type. A non-ASCII
     * VARCHAR decodes into a sink the constant owns.
     */
    public CharSequence getText() {
        return switch (ColumnType.tagOf(getDataType())) {
            case ColumnType.STRING, ColumnType.SYMBOL -> (String) value;
            case ColumnType.VARCHAR -> {
                final Utf8Sequence text = (Utf8Sequence) value;
                if (text == null || text.isAscii()) {
                    yield text == null ? null : text.asAsciiCharSequence();
                }
                decodedText.clear();
                yield Utf8s.utf8ToUtf16(text, decodedText) ? decodedText : text.toString();
            }
            default -> null;
        };
    }

    public CharSequence getTimestampText() {
        assert ColumnType.isTimestamp(getDataType());
        return (CharSequence) value;
    }

    public Utf8Sequence getVarcharValue() {
        return (Utf8Sequence) value;
    }

    /**
     * The value is spelled in SQL as a literal, optionally negated, rather than folded from an expression.
     */
    public boolean isLiteral() {
        return isLiteral;
    }

    /**
     * Whether the constant holds the NULL of its type, as {@link io.questdb.cairo.sql.Function#isNullConstant()} of the
     * constant function the generator builds for it answers: BOOLEAN, BYTE, SHORT and CHAR have no NULL, and DOUBLE and
     * FLOAT read NaN and the infinities as NULL.
     */
    public boolean isNull() {
        final int type = getDataType();
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.NULL, ColumnType.BINARY -> true;
            case ColumnType.INT -> (int) longValue == Numbers.INT_NULL;
            case ColumnType.IPv4 -> (int) longValue == Numbers.IPv4_NULL;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP -> longValue == Numbers.LONG_NULL;
            case ColumnType.DOUBLE -> Numbers.isNull(getDoubleValue());
            case ColumnType.FLOAT -> Numbers.isNull(getFloatValue());
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR -> value == null;
            case ColumnType.UUID, ColumnType.LONG128 -> longValue == Numbers.LONG_NULL && lh == Numbers.LONG_NULL;
            case ColumnType.LONG256 -> Long256Impl.NULL_LONG256.equals(long256);
            case ColumnType.GEOBYTE -> (byte) longValue == GeoHashes.BYTE_NULL;
            case ColumnType.GEOSHORT -> (short) longValue == GeoHashes.SHORT_NULL;
            case ColumnType.GEOINT -> (int) longValue == GeoHashes.INT_NULL;
            case ColumnType.GEOLONG -> longValue == GeoHashes.NULL;
            case ColumnType.DECIMAL8 -> (byte) longValue == Decimals.DECIMAL8_NULL;
            case ColumnType.DECIMAL16 -> (short) longValue == Decimals.DECIMAL16_NULL;
            case ColumnType.DECIMAL32 -> (int) longValue == Decimals.DECIMAL32_NULL;
            case ColumnType.DECIMAL64 -> longValue == Decimals.DECIMAL64_NULL;
            case ColumnType.DECIMAL128 -> Decimal128.isNull(lh, longValue);
            case ColumnType.DECIMAL256 -> Decimal256.isNull(hh, hl, lh, longValue);
            case ColumnType.INTERVAL -> longValue == Numbers.LONG_NULL && lh == Numbers.LONG_NULL;
            default -> false;
        };
    }

    /**
     * Both constants hold the same value of the same type.
     */
    public boolean isSameValue(ConstantExpression that) {
        return getDataType() == that.getDataType() && longValue == that.longValue && hh == that.hh && hl == that.hl && lh == that.lh
                && (value == that.value
                || value instanceof CharSequence text && that.value instanceof CharSequence other && Chars.equals(text, other)
                || value instanceof Utf8Sequence text && that.value instanceof Utf8Sequence other && Utf8s.equals(text, other));
    }

    /**
     * A hash of the value that agrees with {@link #isSameValue}: constants of the same value hash equally.
     */
    public int valueHash() {
        int hash = 31 * getDataType() + Long.hashCode(longValue);
        hash = 31 * hash + Long.hashCode(hh);
        hash = 31 * hash + Long.hashCode(hl);
        hash = 31 * hash + Long.hashCode(lh);
        if (value instanceof CharSequence text) {
            return 31 * hash + Chars.hashCode(text);
        }
        if (value instanceof Utf8Sequence text) {
            return 31 * hash + Utf8s.hashCode(text);
        }
        return hash;
    }

    public ConstantExpression markLiteral() {
        isLiteral = true;
        return this;
    }

    public ConstantExpression markLiteral(FunctionExpression source) {
        isLiteral = true;
        this.source = source;
        return this;
    }

    /**
     * Copies the value of the constant, its interval search and its administrative fact; the copy reads the given
     * folded expression, which is the caller's copy of the original's source.
     */
    public ConstantExpression of(ConstantExpression that, FunctionExpression source) {
        configure(that.getDataType(), that.getPosition(), that.getFunctionFlags());
        hh = that.hh;
        hl = that.hl;
        lh = that.lh;
        longValue = that.longValue;
        isLiteral = that.isLiteral;
        intervalSearch = that.intervalSearch;
        administrativeFact = that.administrativeFact;
        if (that.value == that.long256) {
            long256.copyFrom(that.long256);
            value = long256;
        } else {
            value = that.value;
        }
        this.source = source;
        return this;
    }

    public ConstantExpression ofBinaryNull(int position) {
        configure(ColumnType.BINARY, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        return this;
    }

    public ConstantExpression ofBoolean(boolean value, int position) {
        configure(ColumnType.BOOLEAN, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value ? 1 : 0;
        return this;
    }

    public ConstantExpression ofByte(byte value, int position) {
        configure(ColumnType.BYTE, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofChar(char value, int position) {
        configure(ColumnType.CHAR, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofDate(long value, int position) {
        configure(ColumnType.DATE, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofDecimal(int type, long hh, long hl, long lh, long ll, int position) {
        assert ColumnType.isDecimal(type);
        configure(type, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        this.hh = hh;
        this.hl = hl;
        this.lh = lh;
        longValue = ll;
        return this;
    }

    public ConstantExpression ofDouble(double value, int position) {
        configure(ColumnType.DOUBLE, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = Double.doubleToRawLongBits(value);
        return this;
    }

    public ConstantExpression ofFloat(float value, int position) {
        configure(ColumnType.FLOAT, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = Float.floatToRawIntBits(value);
        return this;
    }

    public ConstantExpression ofGeoHash(long value, int type, int position) {
        assert ColumnType.isGeoHash(type);
        configure(type, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofIPv4(int value, int position) {
        configure(ColumnType.IPv4, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofInt(int value, int position) {
        configure(ColumnType.INT, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofInterval(long lo, long hi, int type, int position) {
        configure(type, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = lo;
        lh = hi;
        return this;
    }

    public ConstantExpression ofLong(long value, int position) {
        configure(ColumnType.LONG, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofLong128(long lo, long hi, int position) {
        configure(ColumnType.LONG128, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = lo;
        lh = hi;
        return this;
    }

    public ConstantExpression ofLong256(Long256 value, int position) {
        configure(ColumnType.LONG256, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        long256.copyFrom(value);
        this.value = long256;
        return this;
    }

    public ConstantExpression ofNull(int position) {
        configure(ColumnType.NULL, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = 0;
        return this;
    }

    public ConstantExpression ofShort(short value, int position) {
        configure(ColumnType.SHORT, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        return this;
    }

    public ConstantExpression ofString(String value, int position) {
        configure(ColumnType.STRING, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        this.value = value;
        return this;
    }

    public ConstantExpression ofSymbol(String value, int position) {
        configure(ColumnType.SYMBOL, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        this.value = value;
        return this;
    }

    public ConstantExpression ofTimestamp(long value, int type, int position) {
        assert ColumnType.isTimestamp(type);
        configure(type, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = value;
        this.value = null;
        return this;
    }

    public ConstantExpression ofUuid(long lo, long hi, int position) {
        configure(ColumnType.UUID, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        longValue = lo;
        lh = hi;
        return this;
    }

    public ConstantExpression ofVarchar(Utf8Sequence value, int position) {
        configure(ColumnType.VARCHAR, position, CONSTANT | STABLE_WITHIN_EXECUTION);
        this.value = value != null ? Utf8String.newInstance(value) : null;
        return this;
    }

    /**
     * Records the administrative fact of the first call the fold that made the constant evaluated, see
     * {@link #getAdministrativeFact()}; null records that it evaluated none.
     */
    public ConstantExpression withAdministrativeFact(RequirementFact fact) {
        administrativeFact = fact;
        return this;
    }

    /**
     * Records the intervals the text of the constant spells at the precision of the key of the interval IN the
     * constant is the right operand of, which the Java filter and the JIT read instead of the text.
     */
    public ConstantExpression withIntervalSearch(IntervalSearch intervalSearch) {
        this.intervalSearch = intervalSearch;
        return this;
    }

    public ConstantExpression withLiteralText(CharSequence text) {
        assert ColumnType.tagOf(getDataType()) == ColumnType.DOUBLE || ColumnType.tagOf(getDataType()) == ColumnType.FLOAT;
        value = text;
        return this;
    }

    public ConstantExpression withSource(FunctionExpression source) {
        this.source = source;
        return this;
    }

    public void withTimestampText(CharSequence text) {
        assert ColumnType.isTimestamp(getDataType());
        value = text;
    }
}
