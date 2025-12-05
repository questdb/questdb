/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

/**
 * Record cursor that iterates through multiple parquet files, reading from each one sequentially.
 * Used to handle glob patterns that match multiple parquet files.
 */
public class HivePartitionedReadParquetRecordCursor implements NoRandomAccessRecordCursor {
    private final FilesFacade ff;
    private final RecordCursor globCursor;
    private final CharSequence nonGlobbedRoot;
    private final ReadParquetRecordCursor parquetCursor;
    private boolean isOpen = true;

    public HivePartitionedReadParquetRecordCursor(
            RecordCursor globCursor,
            ReadParquetRecordCursor parquetCursor,
            FilesFacade ff,
            CharSequence nonGlobbedRoot
    ) {
        this.globCursor = globCursor;
        this.parquetCursor = parquetCursor;
        this.ff = ff;
        this.nonGlobbedRoot = nonGlobbedRoot;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        globCursor.toTop();
        while (!circuitBreaker.checkIfTripped()) {
            if (!globCursor.hasNext()) {
                break;
            }
            switchToNextParquetFileMetadata();

            parquetCursor.calculateSize(circuitBreaker, counter);
        }
    }


    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(parquetCursor);
            Misc.free(globCursor);
        }
    }

    @Override
    public Record getRecord() {
        return parquetCursor.getRecord();
    }

    @Override
    public boolean hasNext() {
        while (true) {
            // Try to get next row from current file
            if (parquetCursor.hasNext()) {
                return true;
            }

            // Current file exhausted, try next file
            if (!globCursor.hasNext()) {
                // No more files
                return false;
            }

            switchToNextParquetFile();
        }
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void skipRows(Counter counter) {
        globCursor.toTop();
        while (counter.get() > 0) {
            if (!globCursor.hasNext()) {
                break;
            }
            switchToNextParquetFileMetadata();

            parquetCursor.skipRows(counter);
        }
    }

    public void switchToNextParquetFile() {
        try {
            // Get path to next file from glob cursor
            Utf8Sequence relativePath = globCursor.getRecord().getVarcharA(0);

            // Construct full path: nonGlobbedRoot + relativePath
            Path path = Path.getThreadLocal("");
            path.of(nonGlobbedRoot).concat(relativePath);

            // Initialize cursor with new file path and read from the beginning
            parquetCursor.of(path.$());
        } catch (Throwable e) {
            // Mark cursor as closed if file switching fails to ensure clean state
            isOpen = false;
            throw e;
        }
    }

    public void switchToNextParquetFileMetadata() {
        // Get path to next file from glob cursor
        Utf8Sequence relativePath = globCursor.getRecord().getVarcharA(0);

        // Construct full path: nonGlobbedRoot + relativePath
        Path path = Path.getThreadLocal("");
        path.of(nonGlobbedRoot).concat(relativePath);

        // Initialize cursor with new file path and read from the beginning
        parquetCursor.ofMetadata(path.$());
    }

    @Override
    public void toTop() {
        parquetCursor.toTop();
        globCursor.toTop();
    }
}
