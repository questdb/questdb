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

package com.nfsdb.net.producer;

import com.nfsdb.Partition;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.net.AbstractObjectProducer;
import com.nfsdb.net.model.IndexedJournal;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Rows;

import java.nio.ByteBuffer;

public class JournalClientStateProducer extends AbstractObjectProducer<IndexedJournal> {

    @Override
    protected int getBufferSize(IndexedJournal value) {
        // journal index + journal max rowid + journal lag rowid
        // + lag partition name length + lag partition name
        // + symbol table count + symbol tables x ( symbol table index + size ).
        return 4 + 8 + 8
                + 2 + 2 * (value.getJournal().getIrregularPartition() == null ? 0 : value.getJournal().getIrregularPartition().getName().length())
                + 2 + value.getJournal().getSymbolTableCount() * (2 + 4);
    }

    @Override
    protected void write(IndexedJournal value, ByteBuffer buffer) {
        try {
            // journal index
            buffer.putInt(value.getIndex());
            // max rowid for non-lag partitions
            Partition p = value.getJournal().lastNonEmptyNonLag();
            if (p == null) {
                buffer.putLong(-1);
            } else {
                buffer.putLong(Rows.toRowID(p.getPartitionIndex(), p.size() - 1));
            }
            // size and name of lag partition
            Partition lag = value.getJournal().getIrregularPartition();
            if (lag != null) {
                buffer.putLong(lag.size());
                ByteBuffers.putStringW(buffer, lag.getName());
            } else {
                buffer.putLong(-1L);
                ByteBuffers.putStringW(buffer, null);
            }
            // symbol table count and their indexes and sizes
            int c;
            buffer.putChar((char) (c = value.getJournal().getSymbolTableCount()));
            for (int i = 0; i < c; i++) {
                SymbolTable tab = value.getJournal().getSymbolTable(i);
                buffer.putChar((char) i);
                buffer.putInt(tab.size());
            }
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }
}
