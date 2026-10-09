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

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import java.io.Closeable;

/**
 * The tables a statement's plan reads, held for the whole compilation: one reader per bound table and metadata
 * version, acquired when the binder binds the scan and released when the compiler releases the statement's
 * preparations, so the binder, the optimiser and the code generator see the same snapshot of each table.
 */
public final class PlanTables implements Closeable, Mutable {
    private final LongList metadataVersions = new LongList();
    private final ObjList<TableReader> readers = new ObjList<>();
    private final ObjList<TableToken> tokens = new ObjList<>();

    /**
     * The reader of the table at its current metadata version, acquired once per compilation; the scan records the
     * version of the metadata it returns. A table the engine cannot open is a statement error at {@code position}.
     */
    public TableReader acquire(TableToken token, int position, SqlExecutionContext executionContext) throws SqlException {
        return acquire(token, TableUtils.ANY_TABLE_VERSION, position, executionContext);
    }

    /**
     * The reader of the table at {@code metadataVersion}, acquired once per compilation; the UPDATE target binds
     * against the write metadata of that version and reads its rows through this reader.
     */
    public TableReader acquire(TableToken token, long metadataVersion, int position, SqlExecutionContext executionContext) throws SqlException {
        final int index = indexOf(token, metadataVersion);
        if (index > -1) {
            return readers.getQuick(index);
        }
        final TableReader reader;
        try {
            reader = metadataVersion == TableUtils.ANY_TABLE_VERSION
                    ? executionContext.getReader(token) : executionContext.getReader(token, metadataVersion);
        } catch (EntryLockedException e) {
            throw SqlException.position(position).put("table is locked: ").put(token.getTableName());
        } catch (CairoException e) {
            if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                throw e;
            }
            throw SqlException.position(position).put(e).setTableBusy(e instanceof EntryUnavailableException);
        }
        try {
            readers.add(reader);
        } catch (Throwable th) {
            Misc.free(reader, th);
            throw th;
        }
        tokens.add(token);
        metadataVersions.add(metadataVersion == TableUtils.ANY_TABLE_VERSION ? reader.getMetadata().getMetadataVersion() : metadataVersion);
        return reader;
    }

    @Override
    public void clear() {
        CairoException.rethrowCleanupFailure(release(null));
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * The reader the scan was bound from, found by identity rather than by name: a rename after binding leaves the
     * bound plan valid, and the factories keep the bound token, so cursor open still rejects a name that has moved
     * to another table.
     */
    public TableReader of(ScanPlan scan) {
        final int index = indexOf(scan.getTableToken(), scan.getMetadataVersion());
        if (index < 0) {
            throw new IllegalStateException("scan reads a table the binder did not acquire");
        }
        return readers.getQuick(index);
    }

    /**
     * Releases every reader and chains release failures onto {@code primary}.
     */
    public Throwable release(Throwable primary) {
        for (int i = readers.size() - 1; i > -1; i--) {
            primary = Misc.freeBestEffort(primary, readers.getQuick(i));
        }
        readers.clear();
        tokens.clear();
        metadataVersions.clear();
        return primary;
    }

    /**
     * Moves every reader to its table's latest transaction before the plan is optimised and generated, and rejects
     * the plan when a table's metadata changed since binding read it, so the compiler binds it again.
     */
    public void reload() {
        for (int i = 0, n = readers.size(); i < n; i++) {
            final TableReader reader = readers.getQuick(i);
            reader.reload();
            final long metadataVersion = reader.getMetadata().getMetadataVersion();
            if (metadataVersion != metadataVersions.getQuick(i)) {
                final TableToken token = tokens.getQuick(i);
                throw TableReferenceOutOfDateException.of(token, token.getTableId(), reader.getMetadata().getTableId(),
                        metadataVersions.getQuick(i), metadataVersion);
            }
        }
    }

    private int indexOf(TableToken token, long metadataVersion) {
        for (int i = 0, n = tokens.size(); i < n; i++) {
            if ((metadataVersion == TableUtils.ANY_TABLE_VERSION || metadataVersions.getQuick(i) == metadataVersion) && tokens.getQuick(i).equals(token)) {
                return i;
            }
        }
        return -1;
    }
}
