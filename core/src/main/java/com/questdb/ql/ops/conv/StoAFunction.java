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

package com.questdb.ql.ops.conv;

import com.questdb.io.sink.CharSink;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.VariableColumn;

public class StoAFunction extends AbstractUnaryOperator {

    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new StoAFunction();
        }
    };

    private StoAFunction() {
        super(ColumnType.STRING);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return value.getSym(rec);
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr(rec);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return value.getSym(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(value.getSym(rec));
    }

    @Override
    public int getStrLen(Record rec) {
        CharSequence cs = value.getSym(rec);
        return cs == null ? VariableColumn.NULL_LEN : cs.length();
    }
}
