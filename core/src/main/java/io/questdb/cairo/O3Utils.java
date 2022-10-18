/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.Nullable;

public class O3Utils {

    private static final Log LOG = LogFactory.getLog(O3Utils.class);

    public static void setupWorkerPool(
            WorkerPool workerPool,
            CairoEngine cairoEngine,
            @Nullable SqlExecutionCircuitBreakerConfiguration sqlExecutionCircuitBreakerConfiguration,
            @Nullable FunctionFactoryCache functionFactoryCache
    ) throws SqlException {
        final MessageBus messageBus = cairoEngine.getMessageBus();
        final int workerCount = workerPool.getWorkerCount();
        final O3PartitionPurgeJob purgeDiscoveryJob = new O3PartitionPurgeJob(messageBus, workerPool.getWorkerCount());
        final ColumnPurgeJob columnPurgeJob = new ColumnPurgeJob(cairoEngine, functionFactoryCache);

        workerPool.assign(purgeDiscoveryJob);
        workerPool.assign(columnPurgeJob);
        workerPool.assign(new O3PartitionJob(messageBus));
        workerPool.assign(new O3OpenColumnJob(messageBus));
        workerPool.assign(new O3CopyJob(messageBus));
        workerPool.assign(new O3CallbackJob(messageBus));
        workerPool.freeOnExit(purgeDiscoveryJob);
        workerPool.freeOnExit(columnPurgeJob);

        final MicrosecondClock microsecondClock = messageBus.getConfiguration().getMicrosecondClock();
        final NanosecondClock nanosecondClock = messageBus.getConfiguration().getNanosecondClock();

        for (int i = 0; i < workerCount; i++) {
            // create job per worker to allow each worker to have
            // own shard walk sequence
            final PageFrameReduceJob pageFrameReduceJob = new PageFrameReduceJob(
                    messageBus,
                    new Rnd(microsecondClock.getTicks(), nanosecondClock.getTicks()),
                    sqlExecutionCircuitBreakerConfiguration
            );
            workerPool.assign(i, pageFrameReduceJob);
            workerPool.freeOnExit(pageFrameReduceJob);
        }
    }

    static long getVarColumnLength(long srcLo, long srcHi, long srcFixAddr) {
        return findVarOffset(srcFixAddr, srcHi + 1) - findVarOffset(srcFixAddr, srcLo);
    }

    static long findVarOffset(long srcFixAddr, long srcLo) {
        return Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
    }

    static void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        Vect.shiftCopyFixedSizeColumnData(shift, src, srcLo, srcHi, dstAddr);
    }

    static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        Vect.copyFromTimestampIndex(src, srcLo, srcHi, dstAddr);
    }

    static void unmapAndClose(FilesFacade ff, long dstFixFd, long dstFixAddr, long dstFixSize) {
        unmap(ff, dstFixAddr, dstFixSize);
        close(ff, dstFixFd);
    }

    static void unmap(FilesFacade ff, long addr, long size) {
        if (addr != 0 && size > 0) {
            ff.munmap(addr, size, MemoryTag.MMAP_O3);
        }
    }

    static void close(FilesFacade ff, long fd) {
        if (fd > 0) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            ff.close(fd);
        }
    }
}
