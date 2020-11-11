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

package io.questdb.griffin.engine.analytic;

import com.questdb.ql.StorageFacade;
import com.questdb.std.ObjList;
import com.questdb.store.SymbolTable;

public class AnalyticRecordStorageFacade implements StorageFacade {
    private final int split;
    private final ObjList<AnalyticFunction> functions;
    private StorageFacade a;

    public AnalyticRecordStorageFacade(int split, ObjList<AnalyticFunction> functions) {
        this.split = split;
        this.functions = functions;
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        if (index < split) {
            return a.getSymbolTable(index);
        }
        return functions.get(index - split).getSymbolTable();
    }

    public void prepare(StorageFacade a) {
        this.a = a;
    }
}
