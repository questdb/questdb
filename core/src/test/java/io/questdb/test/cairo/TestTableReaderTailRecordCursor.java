/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Rows;
import org.jetbrains.annotations.TestOnly;

public class TestTableReaderTailRecordCursor extends TestTableReaderRecordCursor {
    private long dataVersion = -1;
    private long lastRowId = -1;
    private long txn = TableUtils.INITIAL_TXN;

    public void bookmark() {
        lastRowId = recordA.getRowId();
        txn = reader.getTxn();
    }

    @Override
    public boolean hasNext() {
        if (super.hasNext()) {
            return true;
        }
        bookmark();
        return false;
    }

    @Override
    public TestTableReaderTailRecordCursor of(TableReader reader) {
        super.of(reader);
        return this;
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

    @TestOnly
    public void toBottom() {
        lastRowId = Rows.toRowID(reader.getPartitionCount() - 1, reader.getTransientRowCount() - 1);
        startFrom(lastRowId);
        txn = reader.getTxn();
        dataVersion = reader.getDataVersion();
    }

    private void seekToLastSeenRow() {
        if (lastRowId > -1) {
            startFrom(lastRowId);
        } else {
            // this is first time this cursor opens
            toTop();
            dataVersion = reader.getDataVersion();
        }
    }
}
