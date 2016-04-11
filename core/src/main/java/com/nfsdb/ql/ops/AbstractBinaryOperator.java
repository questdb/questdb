/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.ops;

import com.nfsdb.ex.ParserException;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.parser.QueryError;
import com.nfsdb.store.ColumnType;

public abstract class AbstractBinaryOperator extends AbstractVirtualColumn implements Function {
    protected VirtualColumn lhs;
    protected VirtualColumn rhs;

    protected AbstractBinaryOperator(ColumnType type) {
        super(type);
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant() && rhs.isConstant();
    }

    @Override
    public void prepare(StorageFacade facade) {
        lhs.prepare(facade);
        rhs.prepare(facade);
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                setLhs(arg);
                break;
            case 1:
                setRhs(arg);
                break;
            default:
                throw QueryError.position(0).$("Too many arguments").$();
        }
    }

    public void setLhs(VirtualColumn lhs) {
        this.lhs = lhs;
    }

    public void setRhs(VirtualColumn rhs) {
        this.rhs = rhs;
    }
}
