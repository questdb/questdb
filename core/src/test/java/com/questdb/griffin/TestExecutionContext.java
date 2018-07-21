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

package com.questdb.griffin;

import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;

import java.io.Closeable;

public class TestExecutionContext implements SqlExecutionContext, Mutable {
    private final BindVariableService bindVariableService = new BindVariableService();
    private final ObjList<Closeable> closeables = new ObjList<>();
    private final SqlCodeGenerator sqlCodeGenerator;

    public TestExecutionContext(SqlCodeGenerator sqlCodeGenerator) {
        this.sqlCodeGenerator = sqlCodeGenerator;
    }

    @Override
    public void clear() {
        closeables.clear();
        bindVariableService.clear();
    }

    @Override
    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    @Override
    public ObjList<Closeable> getCloseables() {
        return closeables;
    }

    @Override
    public SqlCodeGenerator getCodeGenerator() {
        return sqlCodeGenerator;
    }
}
