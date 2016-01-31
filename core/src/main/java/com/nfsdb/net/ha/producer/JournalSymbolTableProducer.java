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

package com.nfsdb.net.ha.producer;

import com.nfsdb.Journal;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.ha.ChannelProducer;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.SymbolTable;
import com.nfsdb.store.Tx;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class JournalSymbolTableProducer implements ChannelProducer {

    private final ObjList<VariableColumnDeltaProducer> symbolTableProducers = new ObjList<>();
    private final ObjList<SymbolTable> symbolTables = new ObjList<>();
    private final ByteBuffer buffer;
    private boolean hasContent = false;

    public JournalSymbolTableProducer(Journal journal) {
        int tabCount = journal.getSymbolTableCount();
        for (int i = 0; i < tabCount; i++) {
            SymbolTable tab = journal.getSymbolTable(i);
            symbolTables.add(tab);
            symbolTableProducers.add(new VariableColumnDeltaProducer(tab.getDataColumn()));
        }
        buffer = ByteBuffer.allocateDirect(journal.getMetadata().getColumnCount()).order(ByteOrder.LITTLE_ENDIAN);
    }

    public void configure(Tx tx) {
        hasContent = false;
        buffer.rewind();
        for (int i = 0, k = symbolTables.size(); i < k; i++) {
            SymbolTable tab = symbolTables.getQuick(i);
            if (tab != null) {
                VariableColumnDeltaProducer p = symbolTableProducers.getQuick(i);
                p.configure(i < tx.symbolTableSizes.length ? tx.symbolTableSizes[i] : 0, tab.size());
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
    public void free() {
        for (int i = 0, k = symbolTableProducers.size(); i < k; i++) {
            symbolTableProducers.getQuick(i).free();
        }
        ByteBuffers.release(buffer);
    }

    @Override
    public boolean hasContent() {
        return hasContent;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        buffer.flip();
        ByteBuffers.copy(buffer, channel);
        for (int i = 0, k = symbolTableProducers.size(); i < k; i++) {
            VariableColumnDeltaProducer p = symbolTableProducers.getQuick(i);
            if (p != null && p.hasContent()) {
                p.write(channel);
            }
        }
        hasContent = false;
    }
}
