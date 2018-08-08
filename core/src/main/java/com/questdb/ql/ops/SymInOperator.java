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

package com.questdb.ql.ops;

import com.questdb.ex.ParserException;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;
import com.questdb.store.SymbolTable;

public class SymInOperator extends AbstractVirtualColumn implements Function {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new SymInOperator(position);

    private final IntHashSet set = new IntHashSet(10, .5f, -10);
    private final CharSequenceHashSet values = new CharSequenceHashSet();
    private VirtualColumn lhs;

    private SymInOperator(int position) {
        super(ColumnType.BOOLEAN, position);
    }

    @Override
    public boolean getBool(Record rec) {
        return set.contains(lhs.getInt(rec));
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        lhs.prepare(facade);
        SymbolTable tab = lhs.getSymbolTable();
        for (int i = 0, n = values.size(); i < n; i++) {
            set.add(tab.getQuick(values.get(i)));
        }
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        if (pos == 0) {
            lhs = arg;
        } else {
            assertConstant(arg);

            switch (arg.getType()) {
                case ColumnType.STRING:
                    CharSequence cs = arg.getFlyweightStr(null);
                    values.add(cs == null ? null : cs.toString());
                    break;
                default:
                    typeError(arg.getPosition(), ColumnType.STRING);
                    break;
            }
        }
    }
}
