/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.concurrent.*;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.storage.TxLog;
import com.nfsdb.utils.NamedDaemonThreadFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
class PartitionCleaner {
    private final ExecutorService executor;
    private final Sequence pubSeq;
    private final Sequence subSeq;
    private final JournalWriter writer;
    private final TxLog txLog;

    public PartitionCleaner(final JournalWriter writer, String name) throws JournalException {
        this.executor = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("nfsdb-journal-cleaner-" + name, true));
        this.writer = writer;
        this.pubSeq = new SPSequence(32);
        this.subSeq = new SCSequence(new BlockingWaitStrategy());
        this.pubSeq.followedBy(subSeq);
        this.subSeq.followedBy(pubSeq);
        this.txLog = new TxLog(writer.getLocation(), JournalMode.READ, writer.getMetadata().getTxCountHint());
    }

    public void halt() {
        executor.shutdown();
        subSeq.alert();
    }

    public void purge() {
        pubSeq.done(pubSeq.nextBully());
    }

    public void start() {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        subSeq.done(subSeq.waitForNext());
                        try {
                            writer.purgeUnusedTempPartitions(txLog);
                        } catch (JournalException e) {
                            throw new JournalRuntimeException(e);
                        }
                    } catch (AlertedException ignore) {
                        break;
                    }
                }
            }
        });
    }
}
