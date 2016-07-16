/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.comsumer;

import com.questdb.Journal;
import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Unsafe;
import com.questdb.net.ha.AbstractChannelConsumer;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import com.questdb.store.SymbolTable;

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
        for (int i = 0; i < tabCount; i++) {
            symbolTableSizes.setQuick(i, symbolTables.getQuick(i).size());
            if (Unsafe.getUnsafe().getByte(address + i) == 0) {
                continue;
            }
            symbolTableConsumers.getQuick(i).read(channel);
        }
    }
}
