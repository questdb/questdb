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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class BoundedColumnVersionReader {
    public static final int DEFAULT_MAX_RECORDS = 100_000;
    private final FilesFacade ff;
    private final int maxRecords;

    public BoundedColumnVersionReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_RECORDS);
    }

    public BoundedColumnVersionReader(FilesFacade ff, int maxRecords) {
        this.ff = ff;
        this.maxRecords = Math.max(1, maxRecords);
    }

    public ColumnVersionState read(LPSZ cvPath) {
        final ColumnVersionState state = new ColumnVersionState();
        final String cvPathStr = cvPath.asAsciiCharSequence().toString();
        state.setCvPath(cvPathStr);

        final long fd = ff.openRO(cvPath);
        if (fd < 0) {
            addFileOpenFailure(cvPath, cvPathStr, state);
            return state;
        }

        try {
            final long fileSize = ff.length(fd);

            if (fileSize < ColumnVersionReader.HEADER_SIZE) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.SHORT_FILE,
                        "_cv file is shorter than header [path=" + cvPathStr + ", size=" + fileSize + ']'
                );
                return state;
            }

            long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                readColumnVersion(fd, fileSize, scratch, state);
            } finally {
                Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }

        return state;
    }

    private void addFileOpenFailure(LPSZ cvPath, String cvPathStr, ColumnVersionState state) {
        if (ff.exists(cvPath)) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.IO_ERROR,
                    "cannot open _cv file [path=" + cvPathStr + ", errno=" + ff.errno() + ']'
            );
        } else {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.MISSING_FILE,
                    "_cv file does not exist [path=" + cvPathStr + ']'
            );
        }
    }

    private long readLongValue(long fd, long fileSize, long scratch, ColumnVersionState state, long offset, String fieldName) {
        if (offset < 0 || offset > fileSize - Long.BYTES) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.OUT_OF_RANGE,
                    "field is outside file: " + fieldName + " [offset=" + offset + ", width=" + Long.BYTES + ", fileSize=" + fileSize + ']'
            );
            return 0;
        }

        final long bytesRead = ff.read(fd, scratch, Long.BYTES, offset);
        if (bytesRead != Long.BYTES) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                    "cannot read long field: " + fieldName + " [offset=" + offset + ", width=" + Long.BYTES + ", fileSize=" + fileSize + ']'
            );
            return 0;
        }

        return Unsafe.getUnsafe().getLong(scratch);
    }

    private void readColumnVersion(long fd, long fileSize, long scratch, ColumnVersionState state) {
        int issuesBefore = state.getIssues().size();

        final long version = readLongValue(fd, fileSize, scratch, state, ColumnVersionReader.OFFSET_VERSION_64, "cv.version");
        if (state.getIssues().size() > issuesBefore) {
            return;
        }

        final boolean isA = (version & 1L) == 0L;
        final long areaOffset = readLongValue(
                fd, fileSize, scratch, state,
                isA ? ColumnVersionReader.OFFSET_OFFSET_A_64 : ColumnVersionReader.OFFSET_OFFSET_B_64,
                "cv.areaOffset"
        );
        if (state.getIssues().size() > issuesBefore) {
            return;
        }

        final long areaSize = readLongValue(
                fd, fileSize, scratch, state,
                isA ? ColumnVersionReader.OFFSET_SIZE_A_64 : ColumnVersionReader.OFFSET_SIZE_B_64,
                "cv.areaSize"
        );
        if (state.getIssues().size() > issuesBefore) {
            return;
        }

        if (areaSize == 0) {
            state.setRecordCount(0);
            return;
        }

        if (areaSize < 0) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "cv area size is negative [value=" + areaSize + ']'
            );
            return;
        }

        if (areaOffset < ColumnVersionReader.HEADER_SIZE) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_OFFSET,
                    "cv area offset points into header [value=" + areaOffset + ']'
            );
            return;
        }

        if (areaOffset + areaSize > fileSize) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.PARTIAL_READ,
                    "cv area extends beyond file [areaOffset=" + areaOffset + ", areaSize=" + areaSize + ", fileSize=" + fileSize + ']'
            );
            return;
        }

        if (areaSize % ColumnVersionReader.BLOCK_SIZE_BYTES != 0) {
            state.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.INVALID_COUNT,
                    "cv area size is not block-aligned [areaSize=" + areaSize + ", blockSize=" + ColumnVersionReader.BLOCK_SIZE_BYTES + ']'
            );
        }

        int totalRecords = (int) (areaSize / ColumnVersionReader.BLOCK_SIZE_BYTES);
        if (totalRecords > maxRecords) {
            state.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "cv record count is capped [requested=" + totalRecords + ", cap=" + maxRecords + ']'
            );
            totalRecords = maxRecords;
        }

        long[] records = new long[totalRecords * ColumnVersionReader.BLOCK_SIZE];
        for (int i = 0; i < totalRecords; i++) {
            long entryOffset = areaOffset + (long) i * ColumnVersionReader.BLOCK_SIZE_BYTES;
            for (int j = 0; j < ColumnVersionReader.BLOCK_SIZE; j++) {
                issuesBefore = state.getIssues().size();
                long val = readLongValue(fd, fileSize, scratch, state, entryOffset + (long) j * Long.BYTES, "cv.record[" + i + "][" + j + "]");
                if (state.getIssues().size() > issuesBefore) {
                    state.setRecords(records);
                    state.setRecordCount(i);
                    return;
                }
                records[i * ColumnVersionReader.BLOCK_SIZE + j] = val;
            }
        }

        state.setRecords(records);
        state.setRecordCount(totalRecords);
    }
}
