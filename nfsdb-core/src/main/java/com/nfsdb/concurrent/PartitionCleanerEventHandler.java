/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.concurrent;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.nfsdb.JournalMode;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.tx.TxLog;

class PartitionCleanerEventHandler implements EventHandler<PartitionCleanerEvent>, LifecycleAware {
    private final JournalWriter writer;
    volatile TxLog txLog;

    public PartitionCleanerEventHandler(JournalWriter writer) {
        this.writer = writer;
    }

    @Override
    public void onEvent(PartitionCleanerEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (endOfBatch) {
            writer.purgeUnusedTempPartitions(txLog);
        }
    }

    @Override
    public void onStart() {
        try {
            this.txLog = new TxLog(writer.getLocation(), JournalMode.READ);
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void onShutdown() {
        if (this.txLog != null) {
            this.txLog.close();
            this.txLog = null;
        }
    }
}
