/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo;

import com.questdb.std.Rows;

public class TableReaderTailRecordCursor extends TableReaderRecordCursor {

    private long txn = TableUtils.INITIAL_TXN;
    private long lastRowId = -1;
    private long dataVersion = -1;

    public void bookmark() {
        lastRowId = record.getRowId();
        this.txn = reader.getTxn();
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext()) {
            return true;
        }
        bookmark();
        return false;
    }

    public boolean reload() {
        long txn;
        if (reader.reload()) {
            if (reader.getDataVersion() != this.dataVersion) {
                lastRowId = -1;
                dataVersion = reader.getDataVersion();
                toTop();
            } else {
                seekToLastSeenRow();
            }
            this.txn = reader.getTxn();
            return true;
        }

        // when reader is created against table that already has data
        // TableReader.reload() would return 'false'. This method
        // must return 'true' in those conditions

        txn = reader.getTxn();

        if (txn > this.txn) {
            this.txn = txn;
            seekToLastSeenRow();
            return true;
        }
        return false;
    }

    public void toBottom() {
        lastRowId = Rows.toRowID(reader.getPartitionCount() - 1, reader.getTransientRowCount() - 1);
        startFrom(lastRowId);
        this.txn = reader.getTxn();
        this.dataVersion = reader.getDataVersion();
    }

    private void seekToLastSeenRow() {
        if (lastRowId > -1) {
            startFrom(lastRowId);
        } else {
            // this is first time this cursor opens
            toTop();
            this.dataVersion = reader.getDataVersion();
        }
    }
}
