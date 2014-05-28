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

package com.nfsdb.journal.tx;

import com.nfsdb.journal.JournalMode;
import com.nfsdb.journal.column.MappedFileImpl;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.utils.ByteBuffers;

import java.io.File;
import java.nio.ByteBuffer;

public class TxLog {

    private long address = 0;
    private MappedFileImpl mf;
    private char[] buf;

    public TxLog(File baseLocation, JournalMode mode) throws JournalException {
        this.mf = new MappedFileImpl(new File(baseLocation, "_tx"), JournalConfiguration.PIPE_BIT_HINT, mode);
    }

    public boolean hasNext() {
        return getTxAddress() > address;
    }

    public boolean isEmpty() {
        return mf.getAppendOffset() <= 9;
    }

    public Tx get() {
        long offset = getTxAddress();
        assert offset > 0 : "zero offset";
        Tx tx = new Tx();
        ByteBuffer buffer = mf.getBuffer(offset, 4);
        int txSize = buffer.getInt();
        buffer = mf.getBuffer(offset + 4, txSize);

        tx.prevTxAddress = buffer.getLong();
        tx.command = buffer.get();
        tx.timestamp = buffer.getLong();
        tx.journalMaxRowID = buffer.getLong();
        tx.lastPartitionTimestamp = buffer.getLong();
        tx.lagSize = buffer.getLong();

        int sz = buffer.get();
        if (sz == 0) {
            tx.lagName = null;
        } else {
            // lagName
            sz = buffer.get();
            if (buf == null || buf.length < sz) {
                buf = new char[sz];
            }
            for (int i = 0; i < sz; i++) {
                buf[i] = buffer.getChar();
            }
            tx.lagName = new String(buf, 0, sz);
        }

        // symbolTableSizes
        sz = buffer.getChar();
        tx.symbolTableSizes = new int[sz];
        for (int i = 0; i < sz; i++) {
            tx.symbolTableSizes[i] = buffer.getInt();
        }

        //symbolTableIndexPointers
        sz = buffer.getChar();
        tx.symbolTableIndexPointers = new long[sz];
        for (int i = 0; i < sz; i++) {
            tx.symbolTableIndexPointers[i] = buffer.getLong();
        }

        //indexPointers
        sz = buffer.getChar();
        tx.indexPointers = new long[sz];
        for (int i = 0; i < sz; i++) {
            tx.indexPointers[i] = buffer.getLong();
        }

        //lagIndexPointers
        sz = buffer.getChar();
        tx.lagIndexPointers = new long[sz];
        for (int i = 0; i < sz; i++) {
            tx.lagIndexPointers[i] = buffer.getLong();
        }

        this.address = offset;
        return tx;
    }

    public void create(Tx tx) {

        if (tx.lagName != null && tx.lagName.length() > 64) {
            throw new JournalRuntimeException("Partition name is too long");
        }

        long offset = Math.max(9, mf.getAppendOffset());
        ByteBuffer buffer = mf.getBuffer(offset, tx.size() + 4);

        // 4
        buffer.putInt(tx.size());
        // 8
        buffer.putLong(tx.prevTxAddress);
        // 1
        buffer.put(tx.command);
        // 8
        buffer.putLong(System.nanoTime());
        // 8
        buffer.putLong(tx.journalMaxRowID);
        // 8
        buffer.putLong(tx.lastPartitionTimestamp);
        // 8
        buffer.putLong(tx.lagSize);
        // 1
        if (tx.lagName == null) {
            buffer.put((byte) 0);
        } else {
            buffer.put((byte) 1);
            // 2
            buffer.put((byte) tx.lagName.length());
            // tx.lagName.len
            for (int i = 0; i < tx.lagName.length(); i++) {
                buffer.putChar(tx.lagName.charAt(i));
            }
        }
        // 2 + 4 * tx.symbolTableSizes.len
        ByteBuffers.putIntW(buffer, tx.symbolTableSizes);
        ByteBuffers.putLongW(buffer, tx.symbolTableIndexPointers);
        ByteBuffers.putLongW(buffer, tx.indexPointers);
        ByteBuffers.putLongW(buffer, tx.lagIndexPointers);

        // write out tx address
        buffer = mf.getBuffer(0, 9);
        buffer.mark();
        buffer.put((byte) 0);
        buffer.putLong(offset);
        buffer.reset();
        buffer.put((byte) 1);
        address = offset + tx.size();
        mf.setAppendOffset(address);
    }

    public void close() {
        mf.close();
    }

    private long getTxAddress() {
        if (isEmpty()) {
            return 0;
        }

        ByteBuffer buffer = mf.getBuffer(0, 9);
        buffer.mark();
        long limit = 100;
        while (limit > 0 && buffer.get() == 0) {
            Thread.yield();
            buffer.reset();
            limit--;
        }
        if (limit == 0) {
            throw new JournalRuntimeException("Could not get spin-lock on txLog");
        }
        return buffer.getLong();
    }
}
