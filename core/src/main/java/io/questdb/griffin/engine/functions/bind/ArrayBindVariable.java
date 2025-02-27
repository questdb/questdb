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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.pgwire.modern.DoubleArrayParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.Mutable;

public final class ArrayBindVariable extends ArrayFunction implements Mutable {

    private ArrayView view;
    private final DoubleArrayParser doubleArrayParser = new DoubleArrayParser();

    public void assignType(int type) throws SqlException {
        if (view == null || view.getType() == ColumnType.UNDEFINED) {
            super.type = type;
            return;
        }
        if (view.getType() != type) {
            throw SqlException.$(0, "type mismatch");
        }
    }

    @Override
    public void clear() {
        view = null;
    }

    @Override
    public ArrayView getArray(Record rec) {
        return view;
    }

    public void parseArray(CharSequence value) {
        view = SqlUtil.implicitCastStringAsDoubleArray(value, doubleArrayParser, super.type);
    }

    public void setType(int colType) {
        assert view == null || view.getType() == colType;
        super.type = colType;
    }

    public void setView(ArrayView view) {
        if (view == null) {
            clear();
            return;
        }
        if (ColumnType.decodeArrayElementType(view.getType()) != ColumnType.LONG && ColumnType.decodeArrayElementType(view.getType()) != ColumnType.DOUBLE) {
            throw new UnsupportedOperationException("not imlemented yet");
        }
        this.view = view;
    }
}
