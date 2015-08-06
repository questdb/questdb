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

package com.nfsdb.query.iterator;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.query.ResultSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
public class ResultSetConcurrentIterator<T> extends AbstractConcurrentIterator<T> {
    private final ResultSet<T> rs;

    public ResultSetConcurrentIterator(ResultSet<T> rs, int bufferSize) {
        super(bufferSize);
        this.rs = rs;
    }

    @Override
    public Journal<T> getJournal() {
        return rs.getJournal();
    }

    @Override
    protected Runnable getRunnable() {
        return new Runnable() {

            @Override
            public void run() {
                for (int i = 0, sz = rs.size(); i < sz; i++) {
                    try {
                        if (barrier.isAlerted()) {
                            break;
                        }

                        long seq = buffer.next();
                        Holder<T> holder = buffer.get(seq);
                        rs.read(i, holder.object);
                        buffer.publish(seq);
                    } catch (JournalException e) {
                        throw new JournalRuntimeException("Cannot read ResultSet %s at %d", e, rs, (i - 1));
                    }
                }
                long seq = buffer.next();
                buffer.get(seq).hasNext = false;
                buffer.publish(seq);
            }
        };
    }
}
