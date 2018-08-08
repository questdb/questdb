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

package com.questdb.store;

import java.io.File;

public class TempPartition<T> extends Partition<T> {

    private final Lock lock;

    public TempPartition(Journal<T> journal, Interval interval, int partitionIndex, String name) {
        super(journal, interval, partitionIndex, Journal.TX_LIMIT_EVAL, null, false);
        setPartitionDir(new File(journal.getLocation(), name), null);
        this.lock = LockManager.lockShared(getPartitionDir().getAbsolutePath());
    }

    @Override
    public void close() {
        super.close();
        LockManager.release(lock);
    }
}
