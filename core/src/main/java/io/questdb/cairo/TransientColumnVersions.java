/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.std.Vect;

/**
 * A {@code ColumnVersionReader} that can also be upserted into, entirely in memory - no file, no
 * {@code commit()}. Seed it with {@link #readFrom(ColumnVersionReader)}, then upsert into it instead of
 * the table's live {@code ColumnVersionWriter}, which worker threads must not touch directly (see
 * {@link io.questdb.cairo.frm.ColumnTopSink}) and which a caller that reads its own earlier writes back
 * within one call - unlike a plain frozen snapshot - needs a private, mutable copy for anyway.
 */
public class TransientColumnVersions extends ColumnVersionReader {

    /**
     * Same logic as {@link ColumnVersionWriter#mergeColumnTop}.
     */
    public void mergeColumnTop(long partitionTimestamp, int columnIndex, long colTop) {
        if (colTop != 0 || getColumnTop(partitionTimestamp, columnIndex) != 0) {
            upsertColumnTop(partitionTimestamp, columnIndex, colTop);
        }
    }

    /**
     * Same logic as {@link ColumnVersionWriter#upsertColumnTop}, minus its disk-commit bookkeeping.
     */
    public void upsertColumnTop(long partitionTimestamp, int columnIndex, long colTop) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex > -1L) {
            cachedColumnVersionList.setQuick(recordIndex + COLUMN_TOP_OFFSET, colTop);
        } else {
            int defaultRecordIndex = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
            if (defaultRecordIndex >= 0) {
                long columnNameTxn = cachedColumnVersionList.getQuick(defaultRecordIndex + COLUMN_NAME_TXN_OFFSET);
                long defaultPartitionTimestamp = cachedColumnVersionList.getQuick(defaultRecordIndex + COLUMN_TOP_OFFSET);
                if (defaultPartitionTimestamp > partitionTimestamp || colTop > 0) {
                    upsert(partitionTimestamp, columnIndex, columnNameTxn, colTop);
                }
            } else if (colTop > 0) {
                upsert(partitionTimestamp, columnIndex, -1L, colTop);
            }
        }
    }

    private void upsert(long timestamp, int columnIndex, long txn, long columnTop) {
        final int sz = cachedColumnVersionList.size();
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            while (index < sz && cachedColumnVersionList.getQuick(index) == timestamp) {
                final long thisIndex = cachedColumnVersionList.getQuick(index + COLUMN_INDEX_OFFSET);

                if (thisIndex == columnIndex) {
                    if (txn > -1) {
                        cachedColumnVersionList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
                    }
                    cachedColumnVersionList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
                    insert = false;
                    break;
                }

                if (thisIndex > columnIndex) {
                    break;
                }

                index += BLOCK_SIZE;
            }
        } else {
            index = -index - 1;
        }

        if (insert) {
            if (index < sz) {
                cachedColumnVersionList.insert(index, BLOCK_SIZE);
            } else {
                cachedColumnVersionList.setPos(Math.max(index + BLOCK_SIZE, sz + BLOCK_SIZE));
            }
            cachedColumnVersionList.setQuick(index, timestamp);
            cachedColumnVersionList.setQuick(index + COLUMN_INDEX_OFFSET, columnIndex);
            cachedColumnVersionList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
            cachedColumnVersionList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
        }
    }
}
