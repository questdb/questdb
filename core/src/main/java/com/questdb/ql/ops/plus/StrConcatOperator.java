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

package com.questdb.ql.ops.plus;

import com.questdb.common.ColumnType;
import com.questdb.common.Record;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.str.CharSink;
import com.questdb.store.VariableColumn;

public class StrConcatOperator extends AbstractBinaryOperator {
    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new StrConcatOperator(position);

    private final SplitCharSequence csA = new SplitCharSequence();
    private final SplitCharSequence csB = new SplitCharSequence();

    private StrConcatOperator(int position) {
        super(ColumnType.STRING, position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return csA.of(lhs.getFlyweightStr(rec), rhs.getFlyweightStr(rec));
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return csB.of(lhs.getFlyweightStrB(rec), rhs.getFlyweightStrB(rec));
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
