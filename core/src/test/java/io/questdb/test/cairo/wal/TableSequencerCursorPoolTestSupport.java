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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerCursorPool;
import io.questdb.cairo.wal.seq.TransactionLogCursor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

final class TableSequencerCursorPoolTestSupport {
    private static final MethodHandle REGISTER_TRANSACTION_LOG_CURSOR = findVirtual(
            "registerTransactionLogCursor",
            MethodType.methodType(void.class, int.class, TransactionLogCursor.class)
    );
    private static final MethodHandle SET_METADATA_CHANGE_LOG = findVirtual(
            "setMetadataChangeLog",
            MethodType.methodType(void.class, TableMetadataChangeLog.class)
    );
    private static final MethodHandle SET_TRANSACTION_LOG_CURSOR = findVirtual(
            "setTransactionLogCursor",
            MethodType.methodType(void.class, int.class, TransactionLogCursor.class)
    );

    private TableSequencerCursorPoolTestSupport() {
    }

    static void registerTransactionLogCursor(
            TableSequencerCursorPool pool,
            int formatVersion,
            TransactionLogCursor cursor
    ) {
        try {
            REGISTER_TRANSACTION_LOG_CURSOR.invokeExact(pool, formatVersion, cursor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable th) {
            throw new AssertionError(th);
        }
    }

    static void setMetadataChangeLog(TableSequencerCursorPool pool, TableMetadataChangeLog cursor) {
        try {
            SET_METADATA_CHANGE_LOG.invokeExact(pool, cursor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable th) {
            throw new AssertionError(th);
        }
    }

    static void setTransactionLogCursor(
            TableSequencerCursorPool pool,
            int formatVersion,
            TransactionLogCursor cursor
    ) {
        try {
            SET_TRANSACTION_LOG_CURSOR.invokeExact(pool, formatVersion, cursor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable th) {
            throw new AssertionError(th);
        }
    }

    private static MethodHandle findVirtual(String name, MethodType type) {
        try {
            return MethodHandles.privateLookupIn(TableSequencerCursorPool.class, MethodHandles.lookup())
                    .findVirtual(TableSequencerCursorPool.class, name, type);
        } catch (IllegalAccessException | NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
