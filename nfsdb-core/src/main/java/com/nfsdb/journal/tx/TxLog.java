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
import com.nfsdb.journal.column.MappedFile;
import com.nfsdb.journal.column.MappedFileImpl;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.Constants;
import com.nfsdb.journal.utils.ByteBuffers;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class TxLog {

    private long address = 0;
    private MappedFile mf;
    private char[] buf;

    public TxLog(File baseLocation, JournalMode mode) throws JournalException {
        // todo: calculate hint
        this.mf = new MappedFileImpl(new File(baseLocation, "_tx"), Constants.PIPE_BIT_HINT, mode);
    }

    public boolean hasNext() {
        return getTxAddress() > address;
    }

    public boolean isEmpty() {
        return mf.getAppendOffset() <= 9 || getTxAddress() <= 0;
    }

    public void head(Tx tx) {
        get(headAddress(), tx);
    }

    public long headAddress() {
        return this.address = getTxAddress();
    }

    public long prevAddress(long address) {
        ByteBuffer buffer = mf.getBuffer(address, 12);
        return buffer.getLong(buffer.position() + 4);
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
        setTxAddress(offset);
        address = offset + tx.size();
        mf.setAppendOffset(address);
    }

    public void close() {
        mf.close();
    }

    public void force() {
        mf.force();
    }

    public long getTxAddress() {
        final ByteBuffer buf = mf.getBuffer(0, 9);
        final int pos = buf.position();

        long address;
        while (true) {
            address = buf.getLong(pos);
            byte checksum = buf.get(pos + 8);
            byte b0 = (byte) address;
            byte b1 = (byte) (address >> 8);
            byte b2 = (byte) (address >> 16);
            byte b3 = (byte) (address >> 24);
            byte b4 = (byte) (address >> 32);
            byte b5 = (byte) (address >> 40);
            byte b6 = (byte) (address >> 48);
            byte b7 = (byte) (address >> 56);

            if ((b0 ^ b1 ^ b2 ^ b3 ^ b4 ^ b5 ^ b6 ^ b7) == checksum) {
                break;
            }
        }
        return address;
    }

    public void setTxAddress(long address) {

        // checksum
        byte b0 = (byte) address;
        byte b1 = (byte) (address >> 8);
        byte b2 = (byte) (address >> 16);
        byte b3 = (byte) (address >> 24);
        byte b4 = (byte) (address >> 32);
        byte b5 = (byte) (address >> 40);
        byte b6 = (byte) (address >> 48);
        byte b7 = (byte) (address >> 56);
        MappedByteBuffer buffer = mf.getBuffer(0, 9);
        int p = buffer.position();
        buffer.putLong(p, address);
        buffer.put(p + 8, (byte) (b0 ^ b1 ^ b2 ^ b3 ^ b4 ^ b5 ^ b6 ^ b7));
    }

    public void get(long address, Tx tx) {
        assert address > 0 : "zero address: " + address;
        tx.address = address;
        ByteBuffer buffer = mf.getBuffer(address, 4);
        int txSize = buffer.getInt(buffer.position());
        buffer = mf.getBuffer(address + 4, txSize);

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
        if (tx.symbolTableSizes == null || tx.symbolTableSizes.length < sz) {
            tx.symbolTableSizes = new int[sz];
        }
        for (int i = 0; i < sz; i++) {
            tx.symbolTableSizes[i] = buffer.getInt();
        }

        //symbolTableIndexPointers
        sz = buffer.getChar();
        if (tx.symbolTableIndexPointers == null || tx.symbolTableIndexPointers.length < sz) {
            tx.symbolTableIndexPointers = new long[sz];
        }
        for (int i = 0; i < sz; i++) {
            tx.symbolTableIndexPointers[i] = buffer.getLong();
        }

        //indexPointers
        sz = buffer.getChar();
        if (tx.indexPointers == null || tx.indexPointers.length < sz) {
            tx.indexPointers = new long[sz];
        }
        for (int i = 0; i < sz; i++) {
            tx.indexPointers[i] = buffer.getLong();
        }

        //lagIndexPointers
        sz = buffer.getChar();
        if (tx.lagIndexPointers == null || tx.lagIndexPointers.length < sz) {
            tx.lagIndexPointers = new long[sz];
        }
        for (int i = 0; i < sz; i++) {
            tx.lagIndexPointers[i] = buffer.getLong();
        }
    }
}
