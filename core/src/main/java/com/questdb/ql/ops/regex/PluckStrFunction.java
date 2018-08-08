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

package com.questdb.ql.ops.regex;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryError;
import com.questdb.ql.ops.AbstractBinaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.std.str.FlyweightCharSequence;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.StorageFacade;


public class PluckStrFunction extends AbstractBinaryOperator {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new PluckStrFunction(position);
    private final FlyweightCharSequence csA = new FlyweightCharSequence();
    private final FlyweightCharSequence csB = new FlyweightCharSequence();
    private Matcher matcher;

    private PluckStrFunction(int position) {
        super(ColumnType.STRING, position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return getFlyweightStr0(rhs.getFlyweightStr(rec), csA);
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr0(rhs.getFlyweightStrB(rec), csB);
    }

    public CharSequence getFlyweightStr0(CharSequence base, FlyweightCharSequence to) {
        if (base != null && matcher.reset(base).find() && matcher.groupCount() > 0) {
            int lo = matcher.firstStartQuick();
            int hi = matcher.firstEndQuick();
            return to.of(base, lo, hi - lo);
        }
        return null;
    }

    @Override
    public void prepare(StorageFacade facade) {
        super.prepare(facade);
        matcher = Pattern.compile(lhs.getFlyweightStr(null).toString()).matcher("");
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        switch (pos) {
            case 0:
                assertConstant(arg);
                if (arg.getFlyweightStr(null) == null) {
                    throw QueryError.$(arg.getPosition(), "null pattern?");
                }
                break;
            default:
                break;
        }
        super.setArg(pos, arg);
    }
}
