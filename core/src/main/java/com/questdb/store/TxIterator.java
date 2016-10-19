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

package com.questdb.store;

import com.questdb.std.AbstractImmutableIterator;
import com.questdb.txt.sink.DelimitedCharSink;
import com.questdb.txt.sink.FlexBufferSink;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class TxIterator extends AbstractImmutableIterator<Tx> {
    private final TxLog txLog;
    private final Tx tx = new Tx();
    private long txAddress = -1;

    public TxIterator(TxLog txLog) {
        this.txLog = txLog;
        reset();
    }

    @Override
    public boolean hasNext() {
        if (txAddress == 0) {
            return false;
        }

        txLog.read(txAddress, tx);

        txAddress = tx.prevTxAddress;
        return true;
    }

    @Override
    public Tx next() {
        return tx;
    }

    public void print() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(FileDescriptor.out)) {
            print(fos.getChannel());
        }
    }

    public void print(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            print(fos.getChannel());
        }
    }

    public final void reset() {
        txAddress = txLog.readCurrentTxAddress();
    }

    private void print(WritableByteChannel channel) throws IOException {
        try (DelimitedCharSink sink = new DelimitedCharSink(new FlexBufferSink(channel, 1024), '\t', "\n")) {
            print(sink);
        }
    }

    private void print(DelimitedCharSink sink) throws IOException {
        reset();

        sink.put("addr").put("prev").put("txn").put("txPin").put("timestamp").put("rowid").put("part timestamp").put("lag size").put("lag name");
        sink.eol();

        for (Tx tx : this) {
            sink
                    .put(tx.address)
                    .put(tx.prevTxAddress)
                    .put(tx.txn)
                    .put(tx.txPin)
                    .putISODate(tx.timestamp)
                    .put(tx.journalMaxRowID)
                    .putISODate(tx.lastPartitionTimestamp)
                    .put(tx.lagSize)
                    .put(tx.lagName);
            sink.eol();

        }

        sink.flush();
    }
}
