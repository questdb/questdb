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

package io.questdb.griffin.engine.functions.json;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.json.SimdJsonParser;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public interface JsonPathFunction extends Function {
    String DEFAULT_FUNCTION_NAME = "json_path";
    String STRICT_FUNCTION_NAME = "json_path_s";

    static @NotNull DirectUtf8Sink varcharConstantToJsonPointer(Function fn) {
        assert fn.isConstant();
        final Utf8Sequence seq = fn.getVarcharA(null);
        assert seq != null;
        try (DirectUtf8Sink path = new DirectUtf8Sink(seq.size())) {
            path.put(seq);
            final DirectUtf8Sink pointer = new DirectUtf8Sink(seq.size());
            SimdJsonParser.convertJsonPathToPointer(path, pointer);
            return pointer;
        }
    }

    default void applyDefault(int position, int targetType, @NotNull Function defaultValueFn) throws SqlException {
        switch (targetType) {
            case ColumnType.LONG:
                setDefaultLong(defaultValueFn.getLong(null));
                break;
            case ColumnType.DOUBLE:
                setDefaultDouble(defaultValueFn.getDouble(null));
                break;
            case ColumnType.BOOLEAN:
                setDefaultBool(defaultValueFn.getBool(null));
                break;
            case ColumnType.SYMBOL:
                setDefaultSymbol(defaultValueFn.getSymbol(null));
                break;
            case ColumnType.SHORT:
                setDefaultShort(defaultValueFn.getShort(null));
                break;
            case ColumnType.INT:
                setDefaultInt(defaultValueFn.getInt(null));
                break;
            case ColumnType.FLOAT:
                setDefaultFloat(defaultValueFn.getFloat(null));
                break;
            case ColumnType.VARCHAR:
                setDefaultVarchar(defaultValueFn.getVarcharA(null));
                break;
            default:
                // TODO: Better error messaging to use the name of the type, e.g. "DATE" instead of "7".
                throw SqlException.position(position).put("unsupported target type: ").put(targetType);
        }
    }

    void setDefaultBool(boolean bool);

    void setDefaultDouble(double aDouble);

    void setDefaultFloat(float aFloat);

    void setDefaultInt(int anInt);

    void setDefaultLong(long aLong);

    void setDefaultShort(short aShort);

    void setDefaultSymbol(CharSequence symbol);

    void setDefaultVarchar(Utf8Sequence varcharA);
}
