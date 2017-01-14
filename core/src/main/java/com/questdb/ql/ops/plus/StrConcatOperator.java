/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.ql.ops.plus;

import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.SplitCharSequence;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.VariableColumn;

public class StrConcatOperator extends AbstractBinaryOperator {
    public final static VirtualColumnFactory<Function> FACTORY = new VirtualColumnFactory<Function>() {
        @Override
        public Function newInstance(int position, ServerConfiguration configuration) {
            return new StrConcatOperator(position);
        }
    };

    private final SplitCharSequence csA = new SplitCharSequence();
    private final SplitCharSequence csB = new SplitCharSequence();

    private StrConcatOperator(int position) {
        super(ColumnType.STRING, position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        csA.init(lhs.getFlyweightStr(rec), rhs.getFlyweightStr(rec));
        return csA;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        csB.init(lhs.getFlyweightStrB(rec), rhs.getFlyweightStrB(rec));
        return csB;
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        lhs.getStr(rec, sink);
        rhs.getStr(rec, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        int ll = lhs.getStrLen(rec);
        int rl = rhs.getStrLen(rec);

        if (ll == VariableColumn.NULL_LEN) {
            return rl;
        }

        if (rl == VariableColumn.NULL_LEN) {
            return ll;
        }
        return ll + rl;
    }
}
