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

package io.questdb.griffin.engine.analytic.next;

import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.ops.VirtualColumn;

public class NextAnalyticFunction extends AbstractNextAnalyticFunction {

    private long prevAddress = -1;

    public NextAnalyticFunction(int pageSize, VirtualColumn valueColumn) {
        super(pageSize, valueColumn);
    }

    @Override
    public void add(Record record) {
        if (prevAddress != -1) {
            Unsafe.getUnsafe().putLong(prevAddress, record.getRowId());
        }
        prevAddress = pages.allocate(8);
        Unsafe.getUnsafe().putLong(prevAddress, -1);
    }

    @Override
    public void reset() {
        super.reset();
        prevAddress = -1;
    }

    @Override
    public void toTop() {
        super.toTop();
        prevAddress = -1;
    }
}
