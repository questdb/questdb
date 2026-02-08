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

package io.questdb.recovery;

import io.questdb.cairo.ColumnVersionReader;
import io.questdb.std.ObjList;

public final class ColumnVersionState {
    private final ObjList<ReadIssue> issues = new ObjList<>();
    private String cvPath;
    private int recordCount;
    private long[] records = new long[0];

    public void addIssue(RecoveryIssueSeverity severity, RecoveryIssueCode code, String message) {
        issues.add(new ReadIssue(severity, code, message));
    }

    public long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        // explicit record for (partitionTimestamp, columnIndex)
        for (int i = 0; i < recordCount; i++) {
            int base = i * ColumnVersionReader.BLOCK_SIZE;
            if (records[base] == partitionTimestamp && records[base + 1] == columnIndex) {
                return records[base + 2];
            }
        }
        // check default partition record
        for (int i = 0; i < recordCount; i++) {
            int base = i * ColumnVersionReader.BLOCK_SIZE;
            if (records[base] == ColumnVersionReader.COL_TOP_DEFAULT_PARTITION && records[base + 1] == columnIndex) {
                return records[base + 2];
            }
        }
        return -1;
    }

    public long getColumnTop(long partitionTimestamp, int columnIndex) {
        // explicit record for (partitionTimestamp, columnIndex)
        for (int i = 0; i < recordCount; i++) {
            int base = i * ColumnVersionReader.BLOCK_SIZE;
            if (records[base] == partitionTimestamp && records[base + 1] == columnIndex) {
                return records[base + 3];
            }
        }
        // check default partition record for when the column was added
        for (int i = 0; i < recordCount; i++) {
            int base = i * ColumnVersionReader.BLOCK_SIZE;
            if (records[base] == ColumnVersionReader.COL_TOP_DEFAULT_PARTITION && records[base + 1] == columnIndex) {
                long addedPartitionTs = records[base + 3];
                if (addedPartitionTs <= partitionTimestamp) {
                    return 0;
                }
                return -1;
            }
        }
        // no record at all means column was present from the start
        return 0;
    }

    public String getCvPath() {
        return cvPath;
    }

    public ObjList<ReadIssue> getIssues() {
        return issues;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public long[] getRecords() {
        return records;
    }

    void setCvPath(String cvPath) {
        this.cvPath = cvPath;
    }

    void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    void setRecords(long[] records) {
        this.records = records;
    }
}
