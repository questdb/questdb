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

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.io.sink.DelimitedCharSink;
import com.nfsdb.io.sink.FlexBufferSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public Tx next() {
        return tx;
    }

    public void print() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(FileDescriptor.out)) {
            print(fos.getChannel());
        }
    }

    public void print(WritableByteChannel channel) throws IOException {
        try (DelimitedCharSink sink = new DelimitedCharSink(new FlexBufferSink(channel, 1024), '\t', "\n")) {
            print(sink);
        }
    }

    public void print(DelimitedCharSink sink) {
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

    public void print(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            print(fos.getChannel());
        }
    }

    public final void reset() {
        txAddress = txLog.readCurrentTxAddress();
    }
}
