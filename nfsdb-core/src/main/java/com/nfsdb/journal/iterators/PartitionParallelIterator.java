/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.iterators;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;

public class PartitionParallelIterator<T> extends AbstractParallelIterator<T> {
    private final Partition<T> partition;
    private final long lo;
    private final long hi;

    public PartitionParallelIterator(Partition<T> partition, long lo, long hi, int bufferSize) {
        super(bufferSize);
        this.partition = partition;
        this.lo = lo;
        this.hi = hi;
    }

    @Override
    public Journal<T> getJournal() {
        return partition.getJournal();
    }

    @Override
    protected Runnable getRunnable() {
        return new Runnable() {

            @Override
            public void run() {

                for (long i = lo; i <= hi; i++) {
                    try {
                        partition.open();
                        if (barrier.isAlerted()) {
                            break;
                        }

                        long seq = buffer.next();
                        Holder<T> holder = buffer.get(seq);
                        partition.getJournal().clearObject(holder.object);
                        partition.read(i, holder.object);
                        buffer.publish(seq);
                    } catch (JournalException e) {
                        throw new JournalRuntimeException("Cannot read partition " + partition + " at " + (i - 1), e);
                    }
                }
                long seq = buffer.next();
                buffer.get(seq).hasNext = false;
                buffer.publish(seq);
            }
        };
    }
}
