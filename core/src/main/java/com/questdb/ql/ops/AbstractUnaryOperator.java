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
import com.questdb.parser.sql.QueryError;
import com.questdb.store.StorageFacade;

public abstract class AbstractUnaryOperator extends AbstractVirtualColumn implements Function {
    protected VirtualColumn value;

    protected AbstractUnaryOperator(int columnType, int position) {
        super(columnType, position);
    }

    @Override
    public boolean isConstant() {
        return value.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        value.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                this.value = arg;
                break;
            default:
                throw QueryError.$(0, "Too many arguments");
        }
    }
}
