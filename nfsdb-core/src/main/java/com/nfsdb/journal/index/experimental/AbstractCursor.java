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

package com.nfsdb.journal.index.experimental;

import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.index.Cursor;

public abstract class AbstractCursor implements Cursor {

    private long next = -1;

    @Override
    public boolean hasNext() {
        if (next == -1) {
            next = getNext();
        }
        return next > -1;
    }

    @Override
    public long next() {
        if (next == -1) {
            return getNext();
        } else {
            long n = next;
            next = -1;
            return n;
        }
    }

    @Override
    public void configure(Partition partition) throws JournalException {
        this.next = -1;
    }

    abstract protected long getNext();
}
