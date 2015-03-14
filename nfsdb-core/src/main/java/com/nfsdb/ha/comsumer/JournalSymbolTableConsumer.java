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

package com.nfsdb.ha.comsumer;

import com.nfsdb.Journal;
import com.nfsdb.collections.DirectIntList;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.AbstractChannelConsumer;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Lists;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;

public class JournalSymbolTableConsumer extends AbstractChannelConsumer {

    private final ByteBuffer buffer;
    private final long address;
    private final ArrayList<VariableColumnDeltaConsumer> symbolTableConsumers;
    private final ArrayList<SymbolTable> symbolTables;
    private final DirectIntList symbolTableSizes;
    private final int tabCount;

    public JournalSymbolTableConsumer(Journal journal) {
        this.tabCount = journal.getSymbolTableCount();
        this.buffer = ByteBuffer.allocateDirect(tabCount).order(ByteOrder.LITTLE_ENDIAN);
        this.address = ((DirectBuffer) buffer).address();
        this.symbolTableConsumers = new ArrayList<>(tabCount);
        this.symbolTables = new ArrayList<>(tabCount);
        this.symbolTableSizes = new DirectIntList(tabCount);

        Lists.advance(this.symbolTableConsumers, tabCount - 1);
        Lists.advance(this.symbolTables, tabCount - 1);


        while (symbolTableSizes.size() < tabCount) {
            symbolTableSizes.add(-1);
        }

        for (int i = 0; i < tabCount; i++) {
            SymbolTable tab = journal.getSymbolTable(i);
            symbolTableConsumers.set(i, new VariableColumnDeltaConsumer(tab.getDataColumn()));
            symbolTables.set(i, tab);
            symbolTableSizes.set(i, tab.size());
        }
    }

    @Override
    public void free() {
        super.free();
        ByteBuffers.release(buffer);
        for (int i = 0; i < tabCount; i++) {
            symbolTableConsumers.get(i).free();
        }
        symbolTableSizes.free();
    }

    @Override
    protected void commit() {
        for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
            SymbolTable tab = symbolTables.get(i);
            int oldSize = symbolTableSizes.get(i);
            tab.getDataColumn().commit();
            tab.alignSize();
            tab.updateIndex(oldSize, tab.size());
            tab.commit();
        }
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        buffer.position(0);
        ByteBuffers.copy(channel, buffer);
        for (int i = 0; i < tabCount; i++) {
            symbolTableSizes.set(i, symbolTables.get(i).size());
            if (Unsafe.getUnsafe().getByte(address + i) == 0) {
                continue;
            }
            symbolTableConsumers.get(i).read(channel);
        }
    }
}
