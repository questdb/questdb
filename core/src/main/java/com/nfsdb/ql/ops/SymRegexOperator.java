/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.std.IntHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import com.nfsdb.store.SymbolTable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SymRegexOperator extends AbstractBinaryOperator {

    public final static SymRegexOperator FACTORY = new SymRegexOperator();

    private final IntHashSet set = new IntHashSet();
    private Matcher matcher;

    private SymRegexOperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public boolean getBool(Record rec) {
        return set.contains(lhs.getInt(rec));
    }

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return new SymRegexOperator();
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        set.clear();
        SymbolTable tab = lhs.getSymbolTable();
        for (SymbolTable.Entry e : tab.values()) {
            if (matcher.reset(e.value).find()) {
                set.add(e.key);
            }
        }
    }

    @Override
    public void setRhs(VirtualColumn rhs) {
        super.setRhs(rhs);
        matcher = Pattern.compile(rhs.getStr(null).toString()).matcher("");
    }
}
