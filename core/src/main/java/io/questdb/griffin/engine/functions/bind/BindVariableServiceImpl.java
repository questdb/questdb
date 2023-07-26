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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.*;
import org.jetbrains.annotations.Nullable;

public class BindVariableServiceImpl implements BindVariableService {
    private final ObjectPool<IPv4BindVariable> IPv4VarPool;
    private final ObjectPool<BooleanBindVariable> booleanVarPool;
    private final ObjectPool<ByteBindVariable> byteVarPool;
    private final ObjectPool<CharBindVariable> charVarPool;
    private final ObjectPool<DateBindVariable> dateVarPool;
    private final ObjectPool<DoubleBindVariable> doubleVarPool;
    private final ObjectPool<FloatBindVariable> floatVarPool;
    private final ObjectPool<GeoHashBindVariable> geoHashVarPool;
    private final ObjList<Function> indexedVariables = new ObjList<>();
    private final ObjectPool<IntBindVariable> intVarPool;
    private final ObjectPool<Long256BindVariable> long256VarPool;
    private final ObjectPool<LongBindVariable> longVarPool;
    private final CharSequenceObjHashMap<Function> namedVariables = new CharSequenceObjHashMap<>();
    private final ObjectPool<ShortBindVariable> shortVarPool;
    private final ObjectPool<StrBindVariable> strVarPool;
    private final ObjectPool<TimestampBindVariable> timestampVarPool;
    private final ObjectPool<UuidBindVariable> uuidVarPool;

    public BindVariableServiceImpl(CairoConfiguration configuration) {
        final int poolSize = configuration.getBindVariablePoolSize();
        this.doubleVarPool = new ObjectPool<>(DoubleBindVariable::new, poolSize);
        this.floatVarPool = new ObjectPool<>(FloatBindVariable::new, poolSize);
        this.intVarPool = new ObjectPool<>(IntBindVariable::new, poolSize);
        this.IPv4VarPool = new ObjectPool<>(IPv4BindVariable::new, poolSize);
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
        this.uuidVarPool = new ObjectPool<>(UuidBindVariable::new, 8);
    }

    @Override
    public void clear() {
        namedVariables.clear();
        indexedVariables.clear();
        doubleVarPool.clear();
        floatVarPool.clear();
        intVarPool.clear();
        IPv4VarPool.clear();
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
            case ColumnType.IPv4:
                setIPv4(index);
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
            case ColumnType.UUID:
                setUuid(index);
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
    public ObjList<CharSequence> getNamedVariables() {
        return namedVariables.keys();
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
    public void setByte(int index) throws SqlException {
        setByte(index, (byte) 0);
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
    public void setGeoHash(CharSequence name, long value, int type) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final GeoHashBindVariable function;
            namedVariables.putAt(index, name, function = geoHashVarPool.next());
            function.value = value;
            function.setType(type);
        } else {
            setGeoHash0(namedVariables.valueAtQuick(index), value, type, -1, name);
        }
    }

    @Override
    public void setGeoHash(int index, long value, int type) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setGeoHash0(function, value, type, index, null);
        } else {
            indexedVariables.setQuick(index, function = geoHashVarPool.next());
            ((GeoHashBindVariable) function).value = value;
            ((GeoHashBindVariable) function).setType(type);
        }
    }

    @Override
    public void setGeoHash(int index, int type) throws SqlException {
        setGeoHash(index, GeoHashes.NULL, type);
    }

    @Override
    public void setIPv4(int index) throws SqlException {
        setIPv4(index, Numbers.IPv4_NULL);
    }

    @Override
    public void setIPv4(int index, int value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setIPv40(function, value, index, null);
        } else {
            indexedVariables.setQuick(index, function = IPv4VarPool.next());
            ((IPv4BindVariable) function).value = value;
        }
    }

    @Override
    public void setIPv4(int index, CharSequence value) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setIPv40(function, Numbers.parseIPv4Quiet(value), index, null);
        } else {
            indexedVariables.setQuick(index, function = IPv4VarPool.next());
            ((IPv4BindVariable) function).value = Numbers.parseIPv4Quiet(value);
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

    @Override
    public void setUuid(int index, long lo, long hi) throws SqlException {
        indexedVariables.extendPos(index + 1);
        // variable exists
        Function function = indexedVariables.getQuick(index);
        if (function != null) {
            setUuid(function, lo, hi, index, null);
        } else {
            indexedVariables.setQuick(index, function = uuidVarPool.next());
            ((UuidBindVariable) function).set(lo, hi);
        }
    }

    @Override
    public void setUuid(CharSequence name, long lo, long hi) throws SqlException {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            final UuidBindVariable function;
            namedVariables.putAt(index, name, function = uuidVarPool.next());
            function.set(lo, hi);
        } else {
            setUuid(namedVariables.valueAtQuick(index), lo, hi, -1, name);
        }
    }

    public void setUuid(int index) throws SqlException {
        setUuid(index, Numbers.LONG_NaN, Numbers.LONG_NaN);
    }

    private static void reportError(Function function, int srcType, int index, @Nullable CharSequence name) throws SqlException {
        if (name == null) {
            throw SqlException.$(0, "bind variable at ").put(index).put(" is defined as ").put(ColumnType.nameOf(function.getType())).put(" and cannot accept ").put(ColumnType.nameOf(srcType));
        }
        throw SqlException.$(0, "bind variable '").put(name).put("' is defined as ").put(ColumnType.nameOf(function.getType())).put(" and cannot accept ").put(ColumnType.nameOf(srcType));
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
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = SqlUtil.implicitCastAsChar(value, ColumnType.BYTE);
                break;
            default:
                reportError(function, ColumnType.BYTE, index, name);
                break;
        }
    }

    private static void setChar0(Function function, char value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        // treat char as string representation of numeric type
        switch (functionType) {
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.BYTE);
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.SHORT);
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.INT);
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.LONG);
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.DATE);
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.TIMESTAMP);
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.FLOAT);
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = SqlUtil.implicitCastCharAsType(value, ColumnType.DOUBLE);
                break;
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = value;
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            case ColumnType.GEOBYTE:
                ((GeoHashBindVariable) function).value = SqlUtil.implicitCastCharAsGeoHash(value, function.getType());
                break;
            default:
                reportError(function, ColumnType.CHAR, index, name);
                break;
        }
    }

    private static void setDouble0(Function function, double value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = SqlUtil.implicitCastAsFloat(value, ColumnType.DOUBLE);
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

    private static void setGeoHash0(Function function, long value, int type, int index, @Nullable CharSequence name) throws SqlException {
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
                    ((GeoHashBindVariable) function).value = GeoHashes.widen(value, fromBits, toBits);
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

    private static void setIPv40(Function function, int value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        ((IPv4BindVariable) function).value = value;
    }

    private static void setInt0(Function function, int value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = value != Numbers.INT_NaN ? SqlUtil.implicitCastAsByte(value, ColumnType.INT) : 0;
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = value != Numbers.INT_NaN ? SqlUtil.implicitCastAsShort(value, ColumnType.INT) : 0;
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = value;
                break;
            case ColumnType.IPv4:
                ((IPv4BindVariable) function).value = value != Numbers.INT_NaN ? value : Numbers.IPv4_NULL;
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
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = value != Numbers.INT_NaN ? SqlUtil.implicitCastAsChar(value, ColumnType.INT) : 0;
                break;
            default:
                reportError(function, ColumnType.INT, index, name);
                break;
        }
    }

    private static void setLong0(Function function, long value, int index, @Nullable CharSequence name, int srcType) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsByte(value, ColumnType.LONG) : 0;
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsShort(value, ColumnType.LONG) : 0;
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsInt(value, ColumnType.LONG) : Numbers.INT_NaN;
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
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsChar(value, ColumnType.LONG) : 0;
                break;
            default:
                reportError(function, srcType, index, name);
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
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = SqlUtil.implicitCastAsByte(value, ColumnType.SHORT);
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
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = SqlUtil.implicitCastAsChar(value, ColumnType.SHORT);
                break;
            default:
                reportError(function, ColumnType.SHORT, index, name);
                break;
        }
    }

    private static void setStr0(Function function, CharSequence value, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BOOLEAN:
                ((BooleanBindVariable) function).value = SqlKeywords.isTrueKeyword(value);
                break;
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = SqlUtil.implicitCastStrAsByte(value);
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = SqlUtil.implicitCastStrAsShort(value);
                break;
            case ColumnType.CHAR:
                ((CharBindVariable) function).value = SqlUtil.implicitCastStrAsChar(value);
                break;
            case ColumnType.IPv4:
                ((IPv4BindVariable) function).value = SqlUtil.implicitCastStrAsIPv4(value);
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = SqlUtil.implicitCastStrAsInt(value);
                break;
            case ColumnType.LONG:
                ((LongBindVariable) function).value = SqlUtil.implicitCastStrAsLong(value);
                break;
            case ColumnType.TIMESTAMP:
                ((TimestampBindVariable) function).value = SqlUtil.implicitCastStrAsTimestamp(value);
                break;
            case ColumnType.DATE:
                ((DateBindVariable) function).value = SqlUtil.implicitCastStrAsDate(value);
                break;
            case ColumnType.FLOAT:
                ((FloatBindVariable) function).value = SqlUtil.implicitCastStrAsFloat(value);
                break;
            case ColumnType.DOUBLE:
                ((DoubleBindVariable) function).value = SqlUtil.implicitCastStrAsDouble(value);
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setValue(value);
                break;
            case ColumnType.LONG256:
                SqlUtil.implicitCastStrAsLong256(value, ((Long256BindVariable) function).value);
                break;
            case ColumnType.UUID:
                SqlUtil.implicitCastStrAsUuid(value, ((UuidBindVariable) function).value);
                break;
            default:
                reportError(function, ColumnType.STRING, index, name);
                break;
        }
    }

    private static void setTimestamp0(Function function, long value, int index) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.BYTE:
                ((ByteBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsByte(value, ColumnType.TIMESTAMP) : 0;
                break;
            case ColumnType.SHORT:
                ((ShortBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsShort(value, ColumnType.TIMESTAMP) : 0;
                break;
            case ColumnType.INT:
                ((IntBindVariable) function).value = value != Numbers.LONG_NaN ? SqlUtil.implicitCastAsInt(value, ColumnType.TIMESTAMP) : Numbers.INT_NaN;
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
                ((StrBindVariable) function).setTimestamp(value);
                break;
            default:
                reportError(function, ColumnType.TIMESTAMP, index, null);
                break;
        }
    }

    private static void setUuid(Function function, long lo, long hi, int index, @Nullable CharSequence name) throws SqlException {
        final int functionType = ColumnType.tagOf(function.getType());
        switch (functionType) {
            case ColumnType.UUID:
                ((UuidBindVariable) function).set(lo, hi);
                break;
            case ColumnType.STRING:
                ((StrBindVariable) function).setUuidValue(lo, hi);
                break;
            default:
                reportError(function, ColumnType.UUID, index, name);
                break;
        }
    }
}
