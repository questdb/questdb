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

package com.questdb.ql.ops;

import com.questdb.ex.ParserException;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.MMappedSymbolTable;

public class SymInOperator extends AbstractVirtualColumn implements Function {

    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new SymInOperator();
        }
    };

    private final IntHashSet set = new IntHashSet();
    private final CharSequenceHashSet values = new CharSequenceHashSet();
    private VirtualColumn lhs;

    private SymInOperator() {
        super(ColumnType.BOOLEAN);
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
        MMappedSymbolTable tab = lhs.getSymbolTable();
        for (int i = 0, n = values.size(); i < n; i++) {
            int k = tab.getQuick(values.get(i));
            if (k > -1) {
                set.add(k);
            }
        }
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        if (pos == 0) {
            lhs = arg;
        } else {
            values.add(arg.getStr(null).toString());
        }
    }
}
