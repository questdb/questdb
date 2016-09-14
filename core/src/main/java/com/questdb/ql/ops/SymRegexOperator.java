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

import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.regex.Matcher;
import com.questdb.regex.Pattern;
import com.questdb.std.IntHashSet;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

public class SymRegexOperator extends AbstractBinaryOperator {

    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new SymRegexOperator();
        }
    };

    private final IntHashSet set = new IntHashSet();

    private SymRegexOperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public boolean getBool(Record rec) {
        return set.contains(lhs.getInt(rec));
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        final Matcher matcher = Pattern.compile(rhs.getStr(null).toString()).matcher("");
        set.clear();
        SymbolTable tab = lhs.getSymbolTable();
        for (int i = 0, n = tab.size(); i < n; i++) {
            if (matcher.reset(tab.value(i)).find()) {
                set.add(i);
            }
        }
    }
}
