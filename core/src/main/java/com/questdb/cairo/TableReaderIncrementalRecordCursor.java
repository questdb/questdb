/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

public class TableReaderIncrementalRecordCursor extends TableReaderRecordCursor {

    private long txn = TableUtils.INITIAL_TXN;
    private long lastRowId = -1;

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
            seekToLast();
            this.txn = reader.getTxn();
            return true;
        }

        // when reader is created against table that already has data
        // TableReader.reload() would return 'false'. This method
        // must return 'true' in those conditions

        txn = reader.getTxn();
        if (txn > this.txn) {
            this.txn = txn;
            seekToLast();
            return true;
        }
        return false;
    }

    private void seekToLast() {
        if (lastRowId > -1) {
            startFrom(lastRowId);
        } else {
            toTop();
        }
    }
}
