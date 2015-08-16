/*
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
 */

package com.nfsdb.ql.ops;

import com.nfsdb.collections.ObjList;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.ColumnType;

public class SymEqualsROperator extends AbstractBinaryOperator {

    public final static SymEqualsROperator FACTORY = new SymEqualsROperator();
    private int key;

    private SymEqualsROperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public boolean getBool(Record rec) {
        return key > -1 && rhs.getInt(rec) == key;
    }

    @Override
    public Function newInstance(ObjList<VirtualColumn> args) {
        return new SymEqualsROperator();
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        this.key = rhs.getSymbolTable().getQuick(lhs.getFlyweightStr(null));
    }
}
