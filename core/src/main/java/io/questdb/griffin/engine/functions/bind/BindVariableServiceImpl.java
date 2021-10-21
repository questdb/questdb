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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.millitime.DateFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.millitime.DateFormatUtils.*;

public class BindVariableServiceImpl implements BindVariableService {
    private static final DateFormat[] DATE_FORMATS;
    private static final int DATE_FORMATS_SIZE;
    private static final DateFormat[] TIMESTAMP_FORMATS;
    private static final int TIMESTAMP_FORMATS_SIZE;
    private final CharSequenceObjHashMap<Function> namedVariables = new CharSequenceObjHashMap<>();
    private final ObjList<Function> indexedVariables = new ObjList<>();
    private final ObjectPool<DoubleBindVariable> doubleVarPool;
    private final ObjectPool<FloatBindVariable> floatVarPool;
    private final ObjectPool<IntBindVariable> intVarPool;
    private final ObjectPool<LongBindVariable> longVarPool;
    private final ObjectPool<TimestampBindVariable> timestampVarPool;
    private final ObjectPool<DateBindVariable> dateVarPool;
    private final ObjectPool<ShortBindVariable> shortVarPool;
    private final ObjectPool<ByteBindVariable> byteVarPool;
    private final ObjectPool<GeoHashBindVariable> geoHashVarPool;
    private final ObjectPool<BooleanBindVariable> booleanVarPool;
    private final ObjectPool<StrBindVariable> strVarPool;
    private final ObjectPool<CharBindVariable> charVarPool;
    private final ObjectPool<Long256BindVariable> long256VarPool;

    public BindVariableServiceImpl(CairoConfiguration configuration) {
        final int poolSize = configuration.getBindVariablePoolSize();
        this.doubleVarPool = new ObjectPool<>(DoubleBindVariable::new, poolSize);
        this.floatVarPool = new ObjectPool<>(FloatBindVariable::new, poolSize);
        this.intVarPool = new ObjectPool<>(IntBindVariable::new, poolSize);
        this.longVarPool = new ObjectPool<>(LongBindVariable::new, poolSize);
        this.timestampVarPool = new ObjectPool<>(TimestampBindVariable::new, poolSize);
        this.dateVarPool = new ObjectPool<>(DateBindVariable::new, poolSize);
        this.shortVarPool = new ObjectPool<>(ShortBindVariable::new, poolSize);
        this.byteVarPool = new ObjectPool<>(ByteBindVariable::new, poolSize);
        this.geoHashVarPool = new ObjectPool<>(GeoHashBindVariable::new, poolSize);
        this.booleanVarPool = new ObjectPool<>(BooleanBindVariable::new, poolSize);
        this.strVarPool = new ObjectPool<>(() -> new StrBindVariable(configuration.getFloatToStrCastScale()), poolSize);
        this.charVarPool = new ObjectPool<>(CharBindVariable::new, 8);
        this.long256VarPool = new ObjectPool<>(Long256BindVariable::new, 8);
    }

    public static long parseDate(CharSequence value) throws NumericException {
        final int hi = value.length();
        for (int i = 0; i < DATE_FORMATS_SIZE; i++) {
            try {
                return DATE_FORMATS[i].parse(value, 0, hi, DateFormatUtils.enLocale);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.parseLong(value, 0, hi);
    }

    @Override
    public void clear() {
        namedVariables.clear();
        indexedVariables.clear();
        doubleVarPool.clear();
        floatVarPool.clear();
        intVarPool.clear();
        longVarPool.clear();
        timestampVarPool.clear();
        dateVarPool.clear();
        shortVarPool.clear();
        byteVarPool.clear();
        booleanVarPool.clear();
        strVarPool.clear();
        charVarPool.clear();
        long256VarPool.clear();
        geoHashVarPool.clear();
    }

    @Override
    public int define(int index, int type, int position) throws SqlException {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN:
                setBoolean(index);
                return type;
            case ColumnType.BYTE:
                setByte(index);
                return type;
            case ColumnType.SHORT:
                setShort(index);
                return type;
            case ColumnType.CHAR:
                setChar(index);
                return type;
            case ColumnType.INT:
                setInt(index);
                return type;
            case ColumnType.LONG:
                setLong(index);
                return type;
            case ColumnType.DATE:
                setDate(index);
                return type;
            case ColumnType.TIMESTAMP:
                setTimestamp(index);
                return type;
            case ColumnType.FLOAT:
                setFloat(index);
                return type;
            case ColumnType.DOUBLE:
                setDouble(index);
                return type;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VAR_ARG:
                setStr(index);
                return ColumnType.STRING;
            case ColumnType.LONG256:
                setLong256(index);
                return type;
            case ColumnType.BINARY:
                setBin(index);
                return type;
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                setGeoHash(index, type);
                return type;
            default:
                throw SqlException.$(position, "bind variable cannot be used [contextType=").put(ColumnType.nameOf(type)).put(", index=").put(index).put(']');
        }
    }

    @Override
    public Function getFunction(CharSequence name) {
        assert name != null;
        assert Chars.startsWith(name, ':');
        return namedVariables.valueAt(namedVariables.keyIndex(name, 1, name.length()));
    }

    @Override
    public Function getFunction(int index) {
        final int n = indexedVariables.size();
        if (index < n) {
            return indexedVariables.getQuick(index);
        }
        return null;
    }

    @Override
    public int getIndexedVariableCount() {
        return indexedVariables.size();
    }

    @Override
    public void setBin(CharSequence name, BinarySequence value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new BinBindVariable(value));
        } else {
            Function function = namedVariables.valueAtQuick(index);
            if (function instanceof BinBindVariable) {
                ((BinBindVariable) function).value = value;
            } else {
                throw SqlException.$(0, "bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    @Override
    public void setBin(int index) throws SqlException {
        setBin(index, null);
    }

    @Override
    public void setBin(int index, BinarySequence value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        Function function = indexedVariables.getQuick(index);
        if (function == null) {
            indexedVariables.setQuick(index, new BinBindVariable(value));
        } else if (function instanceof BinBindVariable) {
            ((BinBindVariable) function).value = value;
        } else {
            throw SqlException.$(0, "bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
        }
    }

    @Override
    public void setBoolean(CharSequence name, boolean value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final BooleanBindVariable function;
            namedVariables.putAt(index, name, function = booleanVarPool.next());
            function.value = value;
        } else {
            setBoolean0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setBoolean(int index) throws SqlException {
        setBoolean(index, false);
    }

    @Override
    public void setBoolean(int index, boolean value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setBoolean0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = booleanVarPool.next());
            ((BooleanBindVariable) function).value = value;
        }
    }

    @Override
    public void setByte(CharSequence name, byte value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final ByteBindVariable function;
            namedVariables.putAt(index, name, function = byteVarPool.next());
            function.value = value;
        } else {
            setByte0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setByte(int index, byte value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setByte0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = byteVarPool.next());
            ((ByteBindVariable) function).value = value;
        }
    }

    @Override
    public void setGeoHash(int index, long value, int type) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setGeoByte0(function, value, type, index, null);
        } else {
            indexedVariables.setQuick(index, function = geoHashVarPool.next());
            ((GeoHashBindVariable) function).value = value;
            ((GeoHashBindVariable) function).setType(type);
        }
    }

    @Override
    public void setByte(int index) throws SqlException {
        setByte(index, (byte) 0);
    }

    @Override
    public void setGeoHash(int index, int type) throws SqlException {
        setGeoHash(index, GeoHashes.NULL, type);
    }

    @Override
    public void setChar(CharSequence name, char value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final CharBindVariable function;
            namedVariables.putAt(index, name, function = charVarPool.next());
            function.value = value;
        } else {
            setChar0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setChar(int index) throws SqlException {
        setChar(index, (char) 0);
    }

    @Override
    public void setChar(int index, char value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setChar0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = charVarPool.next());
            ((CharBindVariable) function).value = value;
        }
    }

    @Override
    public void setDate(CharSequence name, long value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final DateBindVariable function;
            namedVariables.putAt(index, name, function = dateVarPool.next());
            function.value = value;
        } else {
            setLong0(namedVariables.valueAtQuick(index), value, -1, name, ColumnType.DATE);
        }
    }

    @Override
    public void setDate(int index) throws SqlException {
        setDate(index, Numbers.LONG_NaN);
    }

    @Override
    public void setDate(int index, long value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setLong0(function, value, index, null, ColumnType.DATE);
        } else {
            indexedVariables.setQuick(index, function = dateVarPool.next());
            ((DateBindVariable) function).value = value;
        }
    }

    @Override
    public void setDouble(CharSequence name, double value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final DoubleBindVariable function;
            namedVariables.putAt(index, name, function = doubleVarPool.next());
            function.value = value;
        } else {
            setDouble0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setDouble(int index) throws SqlException {
        setDouble(index, Double.NaN);
    }

    @Override
    public void setDouble(int index, double value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setDouble0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = doubleVarPool.next());
            ((DoubleBindVariable) function).value = value;
        }
    }

    @Override
    public void setFloat(CharSequence name, float value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final FloatBindVariable function;
            namedVariables.putAt(index, name, function = floatVarPool.next());
            function.value = value;
        } else {
            setFloat0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setFloat(int index) throws SqlException {
        setFloat(index, Float.NaN);
    }

    @Override
    public void setFloat(int index, float value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setFloat0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = floatVarPool.next());
            ((FloatBindVariable) function).value = value;
        }
    }

    @Override
    public void setInt(CharSequence name, int value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final IntBindVariable function;
            namedVariables.putAt(index, name, function = intVarPool.next());
            function.value = value;
        } else {
            setInt0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setInt(int index) throws SqlException {
        setInt(index, Numbers.INT_NaN);
    }

    @Override
    public void setInt(int index, int value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setInt0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = intVarPool.next());
            ((IntBindVariable) function).value = value;
        }
    }

    @Override
    public void setLong(CharSequence name, long value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final LongBindVariable function;
            namedVariables.putAt(index, name, function = longVarPool.next());
            function.value = value;
        } else {
            setLong0(namedVariables.valueAtQuick(index), value, -1, name, ColumnType.LONG);
        }
    }

    @Override
    public void setLong(int index) throws SqlException {
        setLong(index, Numbers.LONG_NaN);
    }

    @Override
    public void setLong(int index, long value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setLong0(function, value, index, null, ColumnType.LONG);
        } else {
            indexedVariables.setQuick(index, function = longVarPool.next());
            ((LongBindVariable) function).value = value;
        }
    }

    @Override
    public void setLong256(CharSequence name, long l0, long l1, long l2, long l3) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final Long256BindVariable function;
            namedVariables.putAt(index, name, function = long256VarPool.next());
            function.value.setAll(l0, l1, l2, l3);
        } else {
            setLong2560(namedVariables.valueAtQuick(index), l0, l1, l2, l3, -1, name);
        }
    }

    @Override
    public void setLong256(CharSequence name, Long256 value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final Long256BindVariable function;
            namedVariables.putAt(index, name, function = long256VarPool.next());
            function.value.copyFrom(value);
        } else {
            setLong2560(
                    namedVariables.valueAtQuick(index),
                    value.getLong0(),
                    value.getLong1(),
                    value.getLong2(),
                    value.getLong3(),
                    -1,
                    name
            );
        }
    }

    @Override
    public void setLong256(int index) throws SqlException {
        setLong256(index, Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN, Numbers.LONG_NaN);
    }

    @Override
    public void setLong256(int index, long l0, long l1, long l2, long l3) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setLong2560(
                    function,
                    l0,
                    l1,
                    l2,
                    l3,
                    index,
                    null
            );
        } else {
            indexedVariables.setQuick(index, function = long256VarPool.next());
            ((Long256BindVariable) function).setValue(l0, l1, l2, l3);
        }
    }

    @Override
    public void setLong256(CharSequence name) throws SqlException {
        setLong256(name, Long256Impl.NULL_LONG256);
    }

    @Override
    public void setShort(int index) throws SqlException {
        setShort(index, (short) 0);
    }

    @Override
    public void setShort(int index, short value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setShort0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = shortVarPool.next());
            ((ShortBindVariable) function).value = value;
        }
    }

    @Override
    public void setShort(CharSequence name, short value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final ShortBindVariable function;
            namedVariables.putAt(index, name, function = shortVarPool.next());
            function.value = value;
        } else {
            setShort0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setStr(int index) throws SqlException {
        setStr(index, null);
    }

    @Override
    public void setStr(int index, CharSequence value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setStr0(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = strVarPool.next());
            ((StrBindVariable) function).setValue(value);
        }
    }

    @Override
    public void setStr(CharSequence name, CharSequence value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final StrBindVariable function;
            namedVariables.putAt(index, name, function = strVarPool.next());
            function.setValue(value);
        } else {
            setStr0(namedVariables.valueAtQuick(index), value, -1, name);
        }
    }

    @Override
    public void setTimestamp(int index) throws SqlException {
        setTimestamp(index, Numbers.LONG_NaN);
    }

    @Override
    public void setTimestamp(int index, long value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setTimestamp0(function, value, index);
        } else {
            indexedVariables.setQuick(index, function = timestampVarPool.next());
            ((TimestampBindVariable) function).value = value;
        }
    }

    @Override
    public void setTimestamp(CharSequence name, long value) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final TimestampBindVariable function;
            namedVariables.putAt(index, name, function = timestampVarPool.next());
            function.value = value;
        } else {
            setLong0(namedVariables.valueAtQuick(index), value, -1, name, ColumnType.TIMESTAMP);
        }
    }

    private static long parseTimestamp(CharSequence value) throws NumericException {
        final int hi = value.length();
        for (int i = 0; i < TIMESTAMP_FORMATS_SIZE; i++) {
            try {
                return TIMESTAMP_FORMATS[i].parse(value, 0, hi, enLocale);
            } catch (NumericException ignore) {
            }
        }
        // Parse as ISO with variable length.
        return IntervalUtils.parseFloorPartialDate(value);
    }

    private static void reportError(Function function, int srcType, int index, @Nullable CharSequence name) throws SqlException {
        if (name == null) {
            throw SqlException.$(0, "bind variable at ").put(index).put(" is defined as ").put(ColumnType.nameOf(function.getType())).put(" and cannot accept ").put(ColumnType.nameOf(srcType));
        }
        throw SqlException.$(0, "bind variable '").put(name).put("' is defined as ").put(ColumnType.nameOf(function.getType())).put(" and cannot accept ").put(ColumnType.nameOf(srcType));
    }

    private static void setDouble0(Function function, double value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                if (value == value) {
                    ((StrBindVariable) function).setValue(value);
                } else {
                    ((StrBindVariable) function).setValue(null);
                }
                break;
            default:
                reportError(function, ColumnType.DOUBLE, index, name);
                break;
        }
    }

    private static void setFloat0(Function function, float value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                if (value == value) {
                    ((StrBindVariable) function).setValue(value);
                } else {
                    ((StrBindVariable) function).setValue(null);
                }
                break;
            default:
                reportError(function, ColumnType.FLOAT, index, name);
                break;
        }
    }

    private static void setInt0(Function function, int value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.INT:
                ((IntBindVariable) function).value = value;
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = value != Numbers.INT_NaN ? value : Numbers.LONG_NaN;
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = value != Numbers.INT_NaN ? value : Numbers.LONG_NaN;
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = value != Numbers.INT_NaN ? value : Numbers.LONG_NaN;
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value != Numbers.INT_NaN ? value : Float.NaN;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value != Numbers.INT_NaN ? value : Double.NaN;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            default:
                reportError(function, ColumnType.INT, index, name);
                break;
        }
    }

    private static void setLong0(Function function, long value, int index, @Nullable CharSequence name, int srcType) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.INT:
                ((IntBindVariable) function).value = (int) value;
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = value;
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = value;
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = value;
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value != Numbers.LONG_NaN ? value : Float.NaN;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value != Numbers.LONG_NaN ? value : Double.NaN;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            default:
                reportError(function, srcType, index, name);
                break;
        }
    }

    private static void setTimestamp0(Function function, long value, int index) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.INT:
                ((IntBindVariable) function).value = (int) value;
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = value;
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = value;
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = value != Numbers.LONG_NaN ? value / 1000 : value;
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value != Numbers.LONG_NaN ? value : Float.NaN;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value != Numbers.LONG_NaN ? value : Double.NaN;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            default:
                reportError(function, ColumnType.TIMESTAMP, index, null);
                break;
        }
    }

    private static void setBoolean0(Function function, boolean value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BOOLEAN:
                ((BooleanBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(Boolean.toString(value));
                break;
            default:
                reportError(function, ColumnType.BOOLEAN, index, name);
                break;
        }
    }

    private static void setChar0(Function function, char value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            default:
                reportError(function, ColumnType.CHAR, index, name);
                break;
        }
    }

    private static void setLong2560(
            Function function,
            long l0,
            long l1,
            long l2,
            long l3,
            int index,
            @Nullable CharSequence name
    ) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.LONG256:
                ((Long256BindVariable) function).setValue(l0, l1, l2, l3);
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(l0, l1, l2, l3);
                break;
            default:
                reportError(function, ColumnType.LONG256, index, name);
                break;
        }
    }

    private static void setShort0(Function function, short value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = value;
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = value;
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = value;
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = value;
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = value;
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = (byte) value;
                break;
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = (char) value;
                break;
            default:
                reportError(function, ColumnType.SHORT, index, name);
                break;
        }
    }

    private static void setByte0(Function function, byte value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = value;
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = value;
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = value;
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = value;
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = value;
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = value;
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = value;
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            default:
                reportError(function, ColumnType.BYTE, index, name);
                break;
        }
    }

    private static void setGeoByte0(Function function, long value, int type, int index, @Nullable CharSequence name) throws SqlException {
        final int varType = function.getType();
        switch (ColumnType.tagOf(varType)) {
            case ColumnType.GEOBYTE:
            case ColumnType.GEOINT:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOLONG:
                int fromBits = ColumnType.getGeoHashBits(type);
                int toBits = ColumnType.getGeoHashBits(varType);
                if (fromBits == toBits) {
                    ((GeoHashBindVariable) function).value = value;
                } else if (fromBits > toBits) {
                    ((GeoHashBindVariable) function).value = ColumnType.truncateGeoHashBits(value, fromBits, toBits);
                } else if (name != null) {
                    throw SqlException.$(0, "inconvertible types: ")
                            .put(ColumnType.nameOf(type))
                            .put(" -> ")
                            .put(ColumnType.nameOf(varType))
                            .put(" [varName=").put(name).put(']');
                } else {
                    throw SqlException.$(0, "inconvertible types: ")
                            .put(ColumnType.nameOf(type))
                            .put(" -> ")
                            .put(ColumnType.nameOf(varType))
                            .put(" [varIndex=").put(index).put(']');
                }
                break;
            default:
                reportError(function, type, index, name);
        }
    }

    private static void setStr0(Function function, CharSequence value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        try {
            switch (functionType) {
                case ColumnType.BOOLEAN:
                    ((BooleanBindVariable) function).value = SqlKeywords.isTrueKeyword(value);
                    break;
                case ColumnType.BYTE:
                    ((ByteBindVariable) function).value = (byte) Numbers.parseShort(value);
                    break;
                case ColumnType.SHORT:
                    ((ShortBindVariable) function).value = Numbers.parseShort(value);
                    break;
                case ColumnType.CHAR:
                    ((CharBindVariable) function).value = (char) (Chars.nonEmpty(value) ? Numbers.parseShort(value) : 0);
                    break;
                case ColumnType.INT:
                    ((IntBindVariable) function).value = value != null ? Numbers.parseInt(value) : Numbers.INT_NaN;
                    break;
                case ColumnType.LONG:
                    ((LongBindVariable) function).value = value != null ? Numbers.parseLong(value) : Numbers.LONG_NaN;
                    break;
                case ColumnType.TIMESTAMP:
                    ((TimestampBindVariable) function).value = value != null ? parseTimestamp(value) : Numbers.LONG_NaN;
                    break;
                case ColumnType.DATE:
                    ((DateBindVariable) function).value = value != null ? parseDate(value) : Numbers.LONG_NaN;
                    break;
                case ColumnType.FLOAT:
                    ((FloatBindVariable) function).value = value != null ? Numbers.parseFloat(value) : Float.NaN;
                    break;
                case ColumnType.DOUBLE:
                    ((DoubleBindVariable) function).value = value != null ? Numbers.parseDouble(value) : Double.NaN;
                    break;
                case ColumnType.STRING:
                    ((StrBindVariable) function).setValue(value);
                    break;
                case ColumnType.LONG256:
                    if (value != null) {
                        Long256FromCharSequenceDecoder.decode(value, 0, value.length(), ((Long256BindVariable) function).value);
                    } else {
                        ((Long256BindVariable) function).value.copyFrom(Long256Impl.NULL_LONG256);
                    }
                    break;
                default:
                    reportError(function, ColumnType.STRING, index, name);
                    break;
            }
        } catch (NumericException e) {
            throw SqlException.$(0, "could not parse [value='").put(value).put("', as=").put(ColumnType.nameOf(functionType)).put(", index=").put(index).put(']');
        }
    }

    static {
        final DateFormatCompiler milliCompiler = new DateFormatCompiler();
        final DateFormat pgDateTimeFormat = milliCompiler.compile("yyyy-MM-dd HH:mm:ssz");

        DATE_FORMATS = new DateFormat[]{
                pgDateTimeFormat,
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                UTC_FORMAT
        };

        DATE_FORMATS_SIZE = DATE_FORMATS.length;

        // we are using "millis" compiler deliberately because clients encode millis into strings
        TIMESTAMP_FORMATS = new DateFormat[]{
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                pgDateTimeFormat
        };

        TIMESTAMP_FORMATS_SIZE = TIMESTAMP_FORMATS.length;
    }
}
