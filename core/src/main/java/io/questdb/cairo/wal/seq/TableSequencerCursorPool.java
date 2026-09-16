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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoException;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;
import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2;

public final class TableSequencerCursorPool implements Closeable {
    private final Path path = new Path();
    private TableMetadataChangeLog metadataChangeLog;
    private TransactionLogCursor transactionLogCursorV1;
    private TransactionLogCursor transactionLogCursorV2;

    @Override
    public void close() {
        Throwable failure = null;
        failure = Misc.freeBestEffort(failure, metadataChangeLog);
        failure = Misc.freeBestEffort(failure, path);
        failure = Misc.freeBestEffort(failure, transactionLogCursorV1);
        failure = Misc.freeBestEffort(failure, transactionLogCursorV2);
        CairoException.rethrowCleanupFailure(failure);
    }

    TableMetadataChangeLog getMetadataChangeLog() {
        return metadataChangeLog;
    }

    Path getPath(Utf8Sequence root) {
        return path.of(root);
    }

    TransactionLogCursor getTransactionLogCursor(int formatVersion) {
        return switch (formatVersion) {
            case WAL_SEQUENCER_FORMAT_VERSION_V1 -> transactionLogCursorV1;
            case WAL_SEQUENCER_FORMAT_VERSION_V2 -> transactionLogCursorV2;
            default -> throw new IllegalArgumentException(
                    "unsupported WAL sequencer format version [version=" + formatVersion + ']'
            );
        };
    }

    void registerTransactionLogCursor(int formatVersion, TransactionLogCursor cursor) {
        try {
            setTransactionLogCursor(formatVersion, cursor);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    void setMetadataChangeLog(TableMetadataChangeLog metadataChangeLog) {
        if (this.metadataChangeLog != null && this.metadataChangeLog != metadataChangeLog) {
            throw new IllegalStateException("table metadata change cursor is already configured");
        }
        this.metadataChangeLog = metadataChangeLog;
    }

    void setTransactionLogCursor(int formatVersion, TransactionLogCursor cursor) {
        switch (formatVersion) {
            case WAL_SEQUENCER_FORMAT_VERSION_V1 -> {
                if (transactionLogCursorV1 != null && transactionLogCursorV1 != cursor) {
                    throw new IllegalStateException("WAL sequencer V1 cursor is already configured");
                }
                transactionLogCursorV1 = cursor;
            }
            case WAL_SEQUENCER_FORMAT_VERSION_V2 -> {
                if (transactionLogCursorV2 != null && transactionLogCursorV2 != cursor) {
                    throw new IllegalStateException("WAL sequencer V2 cursor is already configured");
                }
                transactionLogCursorV2 = cursor;
            }
            default -> throw new IllegalArgumentException(
                    "unsupported WAL sequencer format version [version=" + formatVersion + ']'
            );
        }
    }
}
