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

package com.questdb.store;

import com.questdb.JournalMode;
import com.questdb.ex.JournalException;
import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.File;

public class TxLog implements Closeable {

    public static final String FILE_NAME = "_tx";
    private final UnstructuredFile hb;
    private final Rnd rnd;
    private long headAddress = 0;
    private long currentAddress = 0;
    private long txn;

    public TxLog(File baseLocation, JournalMode mode, int txCount) throws JournalException {
        this.hb = new UnstructuredFile(new File(baseLocation, FILE_NAME), ByteBuffers.getBitHint(512, txCount), mode);
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
        long a = hb.addressOf(0, 9);

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
