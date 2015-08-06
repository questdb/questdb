/*
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
 */

package com.nfsdb.storage;

import com.nfsdb.JournalMode;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.utils.Rnd;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;

public class TxLog {

    public static final String FILE_NAME = "_tx";
    private final HugeBuffer hb;
    private final Rnd rnd;
    private long headAddress = 0;
    private long currentAddress = 0;
    private long txn;

    public TxLog(File baseLocation, JournalMode mode) throws JournalException {
        // todo: calculate hint
        this.hb = new HugeBuffer(new File(baseLocation, FILE_NAME), Constants.HB_HINT, mode);
        this.rnd = new Rnd(System.currentTimeMillis(), System.nanoTime());
        this.txn = getCurrentTxn() + 1;
    }

    public void close() {
        hb.close();
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public long findAddress(long txn, long txPin) {
        long address = getCurrentTxAddress();
        long curr;
        do {
            hb.setPos(address);
            long prev = hb.getLong();
            curr = hb.getLong();
            if (txn == curr) {
                if (txPin == hb.getLong()) {
                    return address;
                } else {
                    return -1;
                }
            }
            address = prev;
            // exploit the fact that txn is decrementing as we walk the list
            // we can stop looking if txn > curr
        } while (txn < curr && address > 0);

        return -1;
    }

    public void force() {
        hb.force();
    }

    public long getCurrentTxAddress() {
        if (currentAddress == 0) {
            currentAddress = readCurrentTxAddress();
        }
        return currentAddress;
    }

    public final long getCurrentTxn() {
        long address = getCurrentTxAddress();
        if (address == 0) {
            return 0L;
        }

        hb.setPos(address + 8);
        return hb.getLong();
    }

    public long getCurrentTxnPin() {
        long address = getCurrentTxAddress();
        if (address == 0) {
            return 0L;
        }
        hb.setPos(address + 16);
        return hb.getLong();
    }

    public boolean hasNext() {
        return readCurrentTxAddress() > headAddress;
    }

    public boolean head(Tx tx) {
        long address = readCurrentTxAddress();
        boolean result = address != headAddress;
        read(headAddress = currentAddress = address, tx);
        return result;
    }

    public boolean isEmpty() {
        return hb.getAppendOffset() < 10 || readCurrentTxAddress() < 1;
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public void read(long address, Tx tx) {
        assert address > 0 : "zero headAddress: " + address;
        tx.address = address;
        hb.setPos(address);
        tx.prevTxAddress = hb.getLong();
        tx.txn = hb.getLong();
        tx.txPin = hb.getLong();
        tx.timestamp = hb.getLong();
        tx.command = hb.get();
        tx.journalMaxRowID = hb.getLong();
        tx.lastPartitionTimestamp = hb.getLong();
        tx.lagSize = hb.getLong();
        tx.lagName = hb.getStr();
        tx.symbolTableSizes = hb.get(tx.symbolTableSizes);
        tx.symbolTableIndexPointers = hb.get(tx.symbolTableIndexPointers);
        tx.indexPointers = hb.get(tx.indexPointers);
        tx.lagIndexPointers = hb.get(tx.lagIndexPointers);
    }

    public long readCurrentTxAddress() {
        long a = hb.getAddress(0, 9);

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

    public void write(Tx tx, boolean manualTxn) {
        currentAddress = Math.max(9, hb.getAppendOffset());
        hb.setPos(currentAddress);
        hb.put(tx.prevTxAddress);
        hb.put(manualTxn ? txn = tx.txn : txn);
        txn++;
        hb.put(manualTxn ? tx.txPin : rnd.nextPositiveLong());
        hb.put(System.currentTimeMillis());
        hb.put(tx.command);
        hb.put(tx.journalMaxRowID);
        hb.put(tx.lastPartitionTimestamp);
        hb.put(tx.lagSize);
        hb.put(tx.lagName);
        hb.put(tx.symbolTableSizes);
        hb.put(tx.symbolTableIndexPointers);
        hb.put(tx.indexPointers);
        hb.put(tx.lagIndexPointers);
        // write out tx address
        headAddress = hb.getPos();
        writeTxAddress(currentAddress);
        hb.setAppendOffset(headAddress);
    }

    public void writeTxAddress(long address) {

        // checksum
        byte b0 = (byte) address;
        byte b1 = (byte) (address >> 8);
        byte b2 = (byte) (address >> 16);
        byte b3 = (byte) (address >> 24);
        byte b4 = (byte) (address >> 32);
        byte b5 = (byte) (address >> 40);
        byte b6 = (byte) (address >> 48);
        byte b7 = (byte) (address >> 56);
        hb.setPos(0);
        hb.put(address);
        hb.put((byte) (b0 ^ b1 ^ b2 ^ b3 ^ b4 ^ b5 ^ b6 ^ b7));

        currentAddress = address;
    }
}
