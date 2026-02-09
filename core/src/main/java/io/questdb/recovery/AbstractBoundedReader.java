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

import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

/**
 * Base class for the four bounded file readers ({@code _txn}, {@code _meta},
 * {@code _cv}, {@code tables.d}). Provides safe read primitives that validate
 * offset and size against the actual file length before issuing a
 * {@link io.questdb.std.FilesFacade#read} call.
 *
 * <p>All methods accumulate {@link ReadIssue} objects instead of throwing,
 * so callers can collect multiple problems from a single file.
 *
 * <p>"Bounded" means each reader caps the number of records it will parse
 * (e.g. max partitions, max columns) to avoid runaway memory allocation
 * on corrupt files.
 */
abstract class AbstractBoundedReader {
    protected final FilesFacade ff;

    protected AbstractBoundedReader(FilesFacade ff) {
        this.ff = ff;
    }

    protected static void addFileOpenFailure(
            FilesFacade ff,
            LPSZ path,
            String pathStr,
            String fileLabel,
            ObjList<ReadIssue> issues
    ) {
        if (ff.exists(path)) {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.IO_ERROR,
                    "cannot open " + fileLabel + " file [path=" + pathStr + ", errno=" + ff.errno() + ']'
            ));
        } else {
            issues.add(new ReadIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.MISSING_FILE,
                    fileLabel + " file does not exist [path=" + pathStr + ']'
            ));
        }
    }

    protected static void addShortReadIssue(
            ObjList<ReadIssue> issues,
            RecoveryIssueCode code,
            String detail,
            long offset,
            long width,
            long fileSize
    ) {
        issues.add(new ReadIssue(
                RecoveryIssueSeverity.ERROR,
                code,
                detail + " [offset=" + offset + ", width=" + width + ", fileSize=" + fileSize + ']'
        ));
    }

    protected static boolean isRangeReadable(long offset, long width, long fileSize) {
        return offset >= 0 && width >= 0 && fileSize >= 0 && offset <= fileSize - width;
    }

    protected int readIntValue(long fd, long fileSize, long scratch, ObjList<ReadIssue> issues, long offset, String fieldName) {
        if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
            addShortReadIssue(
                    issues,
                    RecoveryIssueCode.OUT_OF_RANGE,
                    "field is outside file: " + fieldName,
                    offset,
                    Integer.BYTES,
                    fileSize
            );
            return TxnState.UNSET_INT;
        }

        final long bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
        if (bytesRead != Integer.BYTES) {
            addShortReadIssue(
                    issues,
                    bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                    "cannot read int field: " + fieldName,
                    offset,
                    Integer.BYTES,
                    fileSize
            );
            return TxnState.UNSET_INT;
        }

        return Unsafe.getUnsafe().getInt(scratch);
    }

    protected long readLongValue(long fd, long fileSize, long scratch, ObjList<ReadIssue> issues, long offset, String fieldName) {
        if (!isRangeReadable(offset, Long.BYTES, fileSize)) {
            addShortReadIssue(
                    issues,
                    RecoveryIssueCode.OUT_OF_RANGE,
                    "field is outside file: " + fieldName,
                    offset,
                    Long.BYTES,
                    fileSize
            );
            return TxnState.UNSET_LONG;
        }

        final long bytesRead = ff.read(fd, scratch, Long.BYTES, offset);
        if (bytesRead != Long.BYTES) {
            addShortReadIssue(
                    issues,
                    bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                    "cannot read long field: " + fieldName,
                    offset,
                    Long.BYTES,
                    fileSize
            );
            return TxnState.UNSET_LONG;
        }

        return Unsafe.getUnsafe().getLong(scratch);
    }
}
