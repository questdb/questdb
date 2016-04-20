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

package com.questdb;

import com.questdb.ex.JournalException;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Misc;
import com.questdb.store.KVIndex;

import java.io.Closeable;
import java.io.File;

class SymbolIndexProxy<T> implements Closeable {

    private final Partition<T> partition;
    private final int columnIndex;
    private KVIndex index;
    private long txAddress;

    SymbolIndexProxy(Partition<T> partition, int columnIndex, long txAddress) {
        this.partition = partition;
        this.columnIndex = columnIndex;
        this.txAddress = txAddress;
    }

    public void close() {
        index = Misc.free(index);
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setTxAddress(long txAddress) {
        this.txAddress = txAddress;
        if (index != null) {
            index.setTxAddress(txAddress);
        }
    }

    @Override
    public String toString() {
        return "SymbolIndexProxy{" +
                "index=" + index +
                '}';
    }

    KVIndex getIndex() throws JournalException {
        if (index == null) {
            openIndex();
        }
        return index;
    }

    private void openIndex() throws JournalException {
        JournalMetadata<T> meta = partition.getJournal().getMetadata();
        ColumnMetadata columnMetadata = meta.getColumnQuick(columnIndex);

        if (!columnMetadata.indexed) {
            throw new JournalException("There is no index for column: %s", columnMetadata.name);
        }

        index = new KVIndex(
                new File(partition.getPartitionDir(), columnMetadata.name),
                columnMetadata.distinctCountHint,
                meta.getRecordHint(),
                meta.getTxCountHint(),
                partition.getJournal().getMode(),
                txAddress
        );
    }
}
