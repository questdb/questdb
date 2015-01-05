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

package com.nfsdb.iterators;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Rows;

import java.util.List;

public class JournalConcurrentIterator<T> extends AbstractConcurrentIterator<T> {
    private final Journal<T> journal;
    private final List<JournalIteratorRange> ranges;

    public JournalConcurrentIterator(Journal<T> journal, List<JournalIteratorRange> ranges, int bufferSize) {
        super(bufferSize);
        this.journal = journal;
        this.ranges = ranges;
    }

    @Override
    public Journal<T> getJournal() {
        return journal;
    }

    @Override
    protected Runnable getRunnable() {
        return new Runnable() {

            boolean hasNext = true;
            private int currentIndex = 0;
            private long currentRowID;
            private long currentUpperBound;
            private int currentPartitionID;

            @Override
            public void run() {
                updateVariables();
                while (!barrier.isAlerted()) {
                    try {
                        long outSeq = buffer.next();
                        Holder<T> holder = buffer.get(outSeq);
                        boolean hadNext = hasNext;
                        if (hadNext) {
                            journal.read(Rows.toRowID(currentPartitionID, currentRowID), holder.object);
                            if (currentRowID < currentUpperBound) {
                                currentRowID++;
                            } else {
                                currentIndex++;
                                updateVariables();
                            }
                        }
                        holder.hasNext = hadNext;
                        buffer.publish(outSeq);

                        if (!hadNext) {
                            break;
                        }
                    } catch (JournalException e) {
                        throw new JournalRuntimeException("Error in iterator [" + this + "]", e);
                    }
                }
            }

            private void updateVariables() {
                if (currentIndex < ranges.size()) {
                    JournalIteratorRange w = ranges.get(currentIndex);
                    currentRowID = w.lo;
                    currentUpperBound = w.hi;
                    currentPartitionID = w.partitionID;
                } else {
                    hasNext = false;
                }
            }

        };
    }
}
