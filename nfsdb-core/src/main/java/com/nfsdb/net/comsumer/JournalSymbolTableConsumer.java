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

package com.nfsdb.net.comsumer;

import com.nfsdb.Journal;
import com.nfsdb.collections.DirectIntList;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.net.AbstractChannelConsumer;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Lists;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;

public class JournalSymbolTableConsumer extends AbstractChannelConsumer {

    private final ByteBuffer buffer;
    private final ArrayList<VariableColumnDeltaConsumer> symbolTableConsumers;
    private final ArrayList<SymbolTable> symbolTables;
    private final DirectIntList symbolTableSizes;
    private final DirectIntList symbolTabDataIndicators = new DirectIntList();
    private boolean complete;
    private int symbolTableIndex = 0;

    public JournalSymbolTableConsumer(Journal journal) {
        this.buffer = ByteBuffer.allocateDirect(journal.getSymbolTableCount()).order(ByteOrder.LITTLE_ENDIAN);
        this.symbolTableConsumers = new ArrayList<>(journal.getSymbolTableCount());
        this.symbolTables = new ArrayList<>(journal.getSymbolTableCount());
        this.symbolTableSizes = new DirectIntList(journal.getSymbolTableCount());

        Lists.advance(this.symbolTableConsumers, journal.getSymbolTableCount() - 1);
        Lists.advance(this.symbolTables, journal.getSymbolTableCount() - 1);

        while (symbolTabDataIndicators.size() < journal.getSymbolTableCount()) {
            symbolTabDataIndicators.add(-1);
        }

        while (symbolTableSizes.size() < journal.getSymbolTableCount()) {
            symbolTableSizes.add(-1);
        }

        for (int i = 0, sz = journal.getSymbolTableCount(); i < sz; i++) {
            SymbolTable tab = journal.getSymbolTable(i);
            symbolTableConsumers.set(i, new VariableColumnDeltaConsumer(tab.getDataColumn()));
            symbolTables.set(i, tab);
            symbolTableSizes.set(i, tab.size());
        }
    }

    @Override
    public boolean isComplete() {
        return complete && symbolTableIndex >= symbolTabDataIndicators.size();
    }

    @Override
    public void reset() {
        super.reset();
        symbolTableIndex = 0;
        complete = false;
        buffer.rewind();
        for (int i = 0, sz = symbolTableConsumers.size(); i < sz; i++) {
            VariableColumnDeltaConsumer c = symbolTableConsumers.get(i);
            if (c != null) {
                c.reset();
                symbolTableSizes.set(i, symbolTables.get(i).size());
            }
        }
    }

    @Override
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(channel, buffer);
        if (!complete && !buffer.hasRemaining()) {
            buffer.flip();
            for (int i = 0, sz = symbolTabDataIndicators.size(); i < sz; i++) {
                symbolTabDataIndicators.set(i, buffer.get());
            }
            symbolTableIndex = 0;
            complete = true;
        }

        while (symbolTableIndex < symbolTabDataIndicators.size()) {

            if (symbolTabDataIndicators.get(symbolTableIndex) == 0) {
                symbolTableIndex++;
                continue;
            }

            VariableColumnDeltaConsumer c = symbolTableConsumers.get(symbolTableIndex);
            c.read(channel);
            if (c.isComplete()) {
                symbolTableIndex++;
            } else {
                break;
            }
        }
    }

    @Override
    protected void commit() {
        for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
            SymbolTable tab = symbolTables.get(i);
            if (tab != null) {
                int oldSize = symbolTableSizes.get(i);
                tab.getDataColumn().commit();
                tab.alignSize();
                tab.updateIndex(oldSize, tab.size());
                tab.commit();
            }
        }
    }

    @Override
    public void free() {
        super.free();
        ByteBuffers.release(buffer);
        for (int i = 0; i < symbolTableConsumers.size(); i++) {
            symbolTableConsumers.get(i).free();
        }
        symbolTableSizes.free();
        symbolTabDataIndicators.free();
    }
}
