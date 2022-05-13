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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DelegatingRecordImpl;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.str.CharSink;

public class UnionDelegatingRecordImpl extends DelegatingRecordImpl {

    private RecordMetadata underlyingRecordMetadata;

    @Override
    public void of(Record base) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(int col) {
        if (isSymbol(col)) {
            return base.getSym(col);
        }
        return base.getStr(col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (isSymbol(col)) {
            sink.put(base.getSym(col));
            return;
        }
        base.getStr(col, sink);
    }

    @Override
    public CharSequence getStrB(int col) {
        if (isSymbol(col)) {
            return base.getSymB(col);
        }
        return base.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        if (isSymbol(col)) {
            CharSequence val = base.getSym(col);
            if (val != null) {
                return val.length();
            } else {
                return TableUtils.NULL_LEN;
            }
        }
        return base.getStrLen(col);
    }

    public void of(Record base, RecordMetadata underlyingRecordMetadata) {
        this.underlyingRecordMetadata = underlyingRecordMetadata;
        super.of(base);
        this.base = base;
    }

    private boolean isSymbol(int col) {
        return ColumnType.isSymbol(underlyingRecordMetadata.getColumnType(col));
    }
}
