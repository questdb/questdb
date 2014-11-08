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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.JournalEntry;
import com.nfsdb.journal.lang.cst.JournalSource;

public class SlaveResetOuterJoin extends AbstractImmutableIterator<JournalEntry> implements JoinedSource {
    private final JournalSource masterSource;
    private final JournalSource slaveSource;
    private JournalEntry joinedData;
    private boolean nextSlave = false;

    public SlaveResetOuterJoin(JournalSource masterSource, JournalSource slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
    }

    @Override
    public void reset() {
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterSource.hasNext();
    }

    @Override
    public JournalEntry next() {
        if (!nextSlave) {
            joinedData = masterSource.next();
            slaveSource.reset();
        }

        if (nextSlave || slaveSource.hasNext()) {
            joinedData.slave = slaveSource.next();
            nextSlave = slaveSource.hasNext();
        } else {
            joinedData.slave = null;
            nextSlave = false;
        }
        return joinedData;
    }
}
