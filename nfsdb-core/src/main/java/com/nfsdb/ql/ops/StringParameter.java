/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.ql.ops;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.storage.ColumnType;

public class StringParameter extends AbstractVirtualColumn {
    private String value;

    public StringParameter() {
        super(ColumnType.STRING);
    }

    @Override
    public CharSequence getFlyweightStr() {
        return value;
    }

    @Override
    public CharSequence getStr() {
        return value;
    }

    @Override
    public void getStr(CharSink sink) {
        sink.put(value);
    }

    public void setValue(String value) {
        this.value = value;
    }
}
