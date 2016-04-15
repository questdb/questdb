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
 *
 ******************************************************************************/

package com.nfsdb.store;

import com.nfsdb.io.sink.DelimitedCharSink;
import com.nfsdb.io.sink.FlexBufferSink;
import com.nfsdb.std.AbstractImmutableIterator;
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
