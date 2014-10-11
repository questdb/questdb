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

package com.nfsdb.journal.net.producer;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.net.ChannelProducer;
import com.nfsdb.journal.net.model.JournalClientState;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Lists;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

public class JournalSymbolTableProducer implements ChannelProducer {

    private final ArrayList<VariableColumnDeltaProducer> symbolTableProducers = new ArrayList<>();
    private final ArrayList<SymbolTable> symbolTables = new ArrayList<>();
    private final ByteBuffer buffer;
    private boolean hasContent = false;

    public JournalSymbolTableProducer(Journal journal) {
        Lists.advance(symbolTableProducers, journal.getSymbolTableCount() - 1);
        Lists.advance(symbolTables, journal.getSymbolTableCount() - 1);

        for (int i = 0; i < journal.getSymbolTableCount(); i++) {
            SymbolTable tab = journal.getSymbolTable(i);
            symbolTables.set(i, tab);
            symbolTableProducers.set(i, new VariableColumnDeltaProducer(tab.getDataColumn()));
        }
        buffer = ByteBuffer.allocateDirect(journal.getMetadata().getColumnCount()).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void configure(JournalClientState status) {
        hasContent = false;
        buffer.rewind();
        for (int i = 0; i < symbolTables.size(); i++) {
            SymbolTable tab = symbolTables.get(i);
            if (tab != null) {
                VariableColumnDeltaProducer p = symbolTableProducers.get(i);
                p.configure(status.getSymbolTabKeys().get(i), tab.size());
                if (p.hasContent()) {
                    buffer.put((byte) 1);
                    hasContent = true;
                } else {
                    buffer.put((byte) 0);
                }
            } else {
                buffer.put((byte) 0);
            }
        }
    }

    @Override
    public boolean hasContent() {
        return hasContent;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        buffer.flip();
        ByteBuffers.copy(buffer, channel);
        for (int i = 0; i < symbolTableProducers.size(); i++) {
            VariableColumnDeltaProducer p = symbolTableProducers.get(i);
            if (p != null && p.hasContent()) {
                p.write(channel);
            }
        }
        hasContent = false;
    }
}
