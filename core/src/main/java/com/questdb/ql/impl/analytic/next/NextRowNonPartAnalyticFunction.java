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

package com.questdb.ql.impl.analytic.next;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;

public class NextRowNonPartAnalyticFunction extends AbstractNextRowAnalyticFunction {

    private long prevAddress = -1;

    public NextRowNonPartAnalyticFunction(int pageSize, RecordMetadata parentMetadata, String columnName) {
        super(pageSize, parentMetadata, columnName);
    }

    @Override
    public void addRecord(Record record, long rowid) {
        if (prevAddress != -1) {
            Unsafe.getUnsafe().putLong(prevAddress, rowid);
        }
        prevAddress = pages.allocate(8);
        Unsafe.getUnsafe().putLong(prevAddress, -1);
    }

    @Override
    public void reset() {
        super.reset();
        prevAddress = -1;
    }
}
