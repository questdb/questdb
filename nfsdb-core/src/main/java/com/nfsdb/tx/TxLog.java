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

package com.nfsdb.tx;

import com.nfsdb.JournalMode;
import com.nfsdb.column.HugeBuffer;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.utils.Unsafe;

import java.io.File;

public class TxLog {

    private long address = 0;
    private HugeBuffer mf;

    public TxLog(File baseLocation, JournalMode mode) throws JournalException {
        // todo: calculate hint
        this.mf = new HugeBuffer(new File(baseLocation, "_tx"), Constants.PIPE_BIT_HINT, mode);
    }

    public boolean hasNext() {
        return getTxAddress() > address;
    }

    public boolean isEmpty() {
        return mf.getAppendOffset() <= 9 || getTxAddress() <= 0;
    }

    public Tx head(Tx tx) {
        get(headAddress(), tx);
        return tx;
    }

    public long headAddress() {
        return this.address = getTxAddress();
    }

    public long prevAddress(long address) {
        return Unsafe.getUnsafe().getLong(mf.getAddress(address, 8));
    }

    public void create(Tx tx) {
        long offset = Math.max(9, mf.getAppendOffset());
        mf.setPos(offset);
        mf.put(tx.prevTxAddress);
        mf.put(tx.command);
        mf.put(System.nanoTime());
        mf.put(tx.journalMaxRowID);
        mf.put(tx.lastPartitionTimestamp);
        mf.put(tx.lagSize);
        mf.put(tx.lagName);
        mf.put(tx.symbolTableSizes);
        mf.put(tx.symbolTableIndexPointers);
        mf.put(tx.indexPointers);
        mf.put(tx.lagIndexPointers);
        // write out tx address
        address = mf.getPos();
        setTxAddress(offset);
        mf.setAppendOffset(address);
    }

    public void close() {
        mf.close();
    }

    public void force() {
        mf.force();
    }

    public long getTxAddress() {
        long a = mf.getAddress(0, 9);

        long address;
        while (true) {
            address = Unsafe.getUnsafe().getLong(a);
            byte checksum = Unsafe.getUnsafe().getByte(a + 8);
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
        mf.setPos(0);
        mf.put(address);
        mf.put((byte) (b0 ^ b1 ^ b2 ^ b3 ^ b4 ^ b5 ^ b6 ^ b7));
    }

    public void get(long address, Tx tx) {
        assert address > 0 : "zero address: " + address;
        tx.address = address;
        mf.setPos(address);
        tx.prevTxAddress = mf.getLong();
        tx.command = mf.get();
        tx.timestamp = mf.getLong();
        tx.journalMaxRowID = mf.getLong();
        tx.lastPartitionTimestamp = mf.getLong();
        tx.lagSize = mf.getLong();
        tx.lagName = mf.getStr();
        tx.symbolTableSizes = mf.get(tx.symbolTableSizes);
        tx.symbolTableIndexPointers = mf.get(tx.symbolTableIndexPointers);
        tx.indexPointers = mf.get(tx.indexPointers);
        tx.lagIndexPointers = mf.get(tx.lagIndexPointers);
    }
}
