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

package com.nfsdb.net.ha.comsumer;

import com.nfsdb.Journal;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.ha.AbstractChannelConsumer;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.SymbolTable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class JournalSymbolTableConsumer extends AbstractChannelConsumer {

    private final ByteBuffer buffer;
    private final long address;
    private final ObjList<VariableColumnDeltaConsumer> symbolTableConsumers;
    private final ObjList<SymbolTable> symbolTables;
    private final IntList symbolTableSizes;
    private final int tabCount;

    public JournalSymbolTableConsumer(Journal journal) {
        this.tabCount = journal.getSymbolTableCount();
        this.buffer = ByteBuffer.allocateDirect(tabCount).order(ByteOrder.LITTLE_ENDIAN);
        this.address = ByteBuffers.getAddress(buffer);
        this.symbolTableConsumers = new ObjList<>(tabCount);
        this.symbolTables = new ObjList<>(tabCount);
        this.symbolTableSizes = new IntList(tabCount);

        while (symbolTableSizes.size() < tabCount) {
            symbolTableSizes.add(-1);
        }

        for (int i = 0; i < tabCount; i++) {
            SymbolTable tab = journal.getSymbolTable(i);
            symbolTableConsumers.extendAndSet(i, new VariableColumnDeltaConsumer(tab.getDataColumn()));
            symbolTables.extendAndSet(i, tab);
            symbolTableSizes.setQuick(i, tab.size());
        }
    }

    @Override
    public void free() {
        ByteBuffers.release(buffer);
        for (int i = 0; i < tabCount; i++) {
            symbolTableConsumers.getQuick(i).free();
        }
    }

    @Override
    protected void commit() {
        for (int i = 0, sz = symbolTables.size(); i < sz; i++) {
            SymbolTable tab = symbolTables.getQuick(i);
            int oldSize = symbolTableSizes.getQuick(i);
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
        for (int i = 0, k = tabCount; i < k; i++) {
            symbolTableSizes.setQuick(i, symbolTables.getQuick(i).size());
            if (Unsafe.getUnsafe().getByte(address + i) == 0) {
                continue;
            }
            symbolTableConsumers.getQuick(i).read(channel);
        }
    }
}
