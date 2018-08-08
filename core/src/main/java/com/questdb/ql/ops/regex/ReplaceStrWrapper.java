/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops.regex;

import com.questdb.ex.ParserException;
import com.questdb.ql.ops.AbstractVirtualColumn;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;

public class ReplaceStrWrapper extends AbstractVirtualColumn implements Function {
    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new ReplaceStrWrapper(position);
    private final ReplaceStrFunction funcA;
    private final ReplaceStrFunction funcB;

    public ReplaceStrWrapper(int position) {
        super(ColumnType.STRING, position);
        this.funcA = new ReplaceStrFunction(position);
        this.funcB = new ReplaceStrFunction(position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return funcA.getFlyweightStr(rec);
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return funcB.getFlyweightStrB(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        funcA.getStr(rec, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        return funcA.getStrLen(rec);
    }

    @Override
    public boolean isConstant() {
        return funcA.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        this.funcA.setArg(pos, arg);
        this.funcB.setArg(pos, arg);
    }
}
