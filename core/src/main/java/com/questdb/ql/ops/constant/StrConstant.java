/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.questdb.ql.ops.constant;

import com.questdb.io.sink.CharSink;
import com.questdb.misc.Chars;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.store.ColumnType;
import com.questdb.store.VariableColumn;

public class StrConstant extends AbstractVirtualColumn {
    private final String value;

    public StrConstant(CharSequence value) {
        super(ColumnType.STRING);
        this.value = Chars.toString(value);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return value;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return value;
    }

    @Override
    public CharSequence getStr(Record rec) {
        return value;
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(value);
    }

    @Override
    public int getStrLen(Record rec) {
        return value == null ? VariableColumn.NULL_LEN : value.length();
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }
}
