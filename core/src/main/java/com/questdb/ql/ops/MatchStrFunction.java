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
import com.questdb.std.CharSink;
import com.questdb.std.FlyweightCharSequence;
import com.questdb.std.ObjectFactory;
import com.questdb.store.ColumnType;


public class MatchStrFunction extends AbstractBinaryOperator {

    public final static ObjectFactory<Function> FACTORY = new ObjectFactory<Function>() {
        @Override
        public Function newInstance() {
            return new MatchStrFunction();
        }
    };
    private final FlyweightCharSequence csA = new FlyweightCharSequence();
    private final FlyweightCharSequence csB = new FlyweightCharSequence();
    private Matcher matcher;

    private MatchStrFunction() {
        super(ColumnType.STRING);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return getFlyweightStr0(rhs.getFlyweightStr(rec), csA);
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr0(rhs.getFlyweightStrB(rec), csB);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return getFlyweightStr(rec).toString();
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        sink.put(getFlyweightStr(rec));
    }

    @Override
    public int getStrLen(Record rec) {
        return getFlyweightStr(rec).length();
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
        matcher = Pattern.compile(lhs.getStr(null).toString()).matcher("");
    }
}
