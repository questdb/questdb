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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.cairo.arr.VarcharArrayParser;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;

public final class ArrayBindVariable extends ArrayFunction implements Mutable {
    private final DoubleArrayParser doubleArrayParser = new DoubleArrayParser();
    private final VarcharArrayParser varcharArrayParser = new VarcharArrayParser();
    private ArrayView view;

    public void assignType(int type) throws SqlException {
        assert ColumnType.isArray(type);
        if (view == null) {
            this.type = type;
            return;
        }
        int viewType = view.getType();
        if (ColumnType.isUndefined(viewType)) {
            this.type = type;
        }

        if (view.getType() != type) {
            throw SqlException.$(0, "type mismatch");
        }
    }

    @Override
    public void clear() {
        view = null;
        type = ColumnType.UNDEFINED;
    }

    @Override
    public ArrayView getArray(Record rec) {
        if (view == null) {
            return ArrayConstant.NULL;
        }
        return view;
    }

    public void parseArray(CharSequence value) {
        short elementType = ColumnType.decodeArrayElementType(type);
        if (elementType == ColumnType.VARCHAR) {
            view = SqlUtil.implicitCastStringAsVarcharArray(value, varcharArrayParser, type);
        } else {
            view = SqlUtil.implicitCastStringAsDoubleArray(value, doubleArrayParser, type);
        }
        if (ColumnType.isUndefined(type)) {
            type = view.getType();
        } else {
            if (type != view.getType()) {
                throw CairoException.nonCritical().put("array type mismatch [expected=").put(ColumnType.nameOf(type))
                        .put(", actual=").put(ColumnType.nameOf(view.getType())).put(']');
            }
        }
    }

    public void parseVarcharArrayFromUtf8(long start, long end) {
        assert ColumnType.decodeArrayElementType(type) == ColumnType.VARCHAR;
        varcharArrayParser.of(start, end, ColumnType.decodeWeakArrayDimensionality(type));
        view = varcharArrayParser;
    }

    public void release() {
        Misc.free(varcharArrayParser);
    }

    public void setView(ArrayView view) {
        if (view == null) {
            clear();
            return;
        }

        int elementType = ColumnType.decodeArrayElementType(view.getType());
        if (elementType != ColumnType.LONG && elementType != ColumnType.DOUBLE && elementType != ColumnType.VARCHAR) {
            throw CairoException.nonCritical().put("unsupported array type, only DOUBLE is currently supported [type=")
                    .put(ColumnType.nameOf(elementType)).put(']');
        }
        if (ColumnType.isUndefined(type)) {
            type = view.getType();
        } else {
            if (type != view.getType()) {
                throw CairoException.nonCritical().put("array type mismatch [expected=").put(ColumnType.nameOf(type))
                        .put(", actual=").put(ColumnType.nameOf(view.getType())).put(']');
            }
        }
        this.view = view;
    }
}
