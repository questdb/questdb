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

package com.questdb.ql.analytic.denserank;

import com.questdb.ql.analytic.AnalyticFunction;
import com.questdb.ql.map.DirectMap;
import com.questdb.ql.map.LongResolver;
import com.questdb.std.Misc;
import com.questdb.store.Record;

import java.io.Closeable;
import java.io.IOException;

public abstract class AbstractRankOrderedAnalyticFunction extends AbstractRankAnalyticFunction implements Closeable {

    protected final DirectMap map;
    protected boolean closed = false;

    public AbstractRankOrderedAnalyticFunction(int pageSize, String name) {
        super(name);
        this.map = new DirectMap(pageSize, LongResolver.INSTANCE, LongResolver.INSTANCE);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        Misc.free(map);
        closed = true;
    }

    @Override
    public int getType() {
        return AnalyticFunction.TWO_PASS;
    }

    @Override
    public void prepareFor(Record record) {
        map.locate(record.getRowId());
        rank = map.getValues().getLong(0);
    }

    @Override
    public void reset() {
        map.clear();
        rank = -1;
    }

    @Override
    public void toTop() {
        rank = -1;
    }
}
