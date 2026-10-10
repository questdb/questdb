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
import io.questdb.std.Chars;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

/**
 * Immutable published value; full timestamp types retain their precision.
 */
public final class ConstantExpression extends BoundExpression {
    public static final ObjectFactory<ConstantExpression> FACTORY = ConstantExpression::new;
    private final Long256Impl long256 = new Long256Impl();
    private long hh;
    private long hl;
    private boolean isLiteral;
    private long lh;
    private long longValue;
    private FunctionExpression source;
    private Object value;

    @Override
    public void clear() {
        super.clear();
        hh = 0;
        hl = 0;
        lh = 0;
        isLiteral = false;
        source = null;
        longValue = 0;
        value = null;
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
     * Both constants hold the same value of the same type.
     */
    public boolean isSameValue(ConstantExpression that) {
        return getDataType() == that.getDataType() && longValue == that.longValue && hh == that.hh && hl == that.hl && lh == that.lh
                && (value == that.value
                || value instanceof CharSequence text && that.value instanceof CharSequence other && Chars.equals(text, other)
                || value instanceof Utf8Sequence text && that.value instanceof Utf8Sequence other && Utf8s.equals(text, other));
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
     * Copies the value of the constant; the copy reads the given folded expression, which is the caller's copy
     * of the original's source.
     */
    public ConstantExpression of(ConstantExpression that, FunctionExpression source) {
        configure(that.getDataType(), that.getPosition(), that.getFunctionFlags());
        hh = that.hh;
        hl = that.hl;
        lh = that.lh;
        longValue = that.longValue;
        isLiteral = that.isLiteral;
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
