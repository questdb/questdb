/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.misc.Interval;
import com.nfsdb.storage.Lock;
import com.nfsdb.storage.LockManager;

import java.io.File;

public class TempPartition<T> extends Partition<T> {

    private final Lock lock;

    public TempPartition(Journal<T> journal, Interval interval, int partitionIndex, String name) throws JournalException {
        super(journal, interval, partitionIndex, Journal.TX_LIMIT_EVAL, null);
        setPartitionDir(new File(journal.getLocation(), name), null);
        this.lock = LockManager.lockShared(getPartitionDir());
    }

    @Override
    public void close() {
        super.close();
        LockManager.release(lock);
    }
}
