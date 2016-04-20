/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.net.ha.producer;

import com.questdb.Journal;
import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.ByteBuffers;
import com.questdb.net.ha.ChannelProducer;
import com.questdb.std.ObjList;
import com.questdb.store.SymbolTable;
import com.questdb.store.Tx;

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
