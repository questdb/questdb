/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.ops.constant;

import com.questdb.misc.Chars;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.std.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.VariableColumn;

public class StrConstant extends AbstractVirtualColumn {
    private final String value;

    public StrConstant(CharSequence value, int position) {
        super(ColumnType.STRING, position);
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
