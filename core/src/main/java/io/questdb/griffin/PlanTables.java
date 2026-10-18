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
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The tables a statement's plan reads: one {@link PlanTable} per bound table and metadata version, copied when the
 * binder binds the scan, so the binder, the optimiser and the code generator read the same metadata version of each
 * table. A reader serves a table only while this container copies its facts, so it holds no reader between copies.
 */
public final class PlanTables implements Mutable {
    private final ObjList<PlanTable> tables = new ObjList<>();
    private int tableCount;

    /**
     * The table at its current metadata version, copied once per compilation; the scan records the version of the
     * metadata it returns. A table the engine cannot open is a statement error at {@code position}.
     */
    public PlanTable acquire(TableToken token, int position, SqlExecutionContext executionContext) throws SqlException {
        return acquire(token, TableUtils.ANY_TABLE_VERSION, position, executionContext);
    }

    /**
     * The table at {@code metadataVersion}, copied once per compilation; the UPDATE target binds against the write
     * metadata of that version and generates the scan of its rows from this copy.
     */
    public PlanTable acquire(TableToken token, long metadataVersion, int position, SqlExecutionContext executionContext) throws SqlException {
        final int index = indexOf(token, metadataVersion);
        if (index > -1) {
            return tables.getQuick(index);
        }
        if (tableCount == tables.size()) {
            tables.add(new PlanTable());
        }
        final PlanTable table = tables.getQuick(tableCount);
        try (TableReader reader = getReader(token, metadataVersion, position, executionContext)) {
            table.of(token, position, reader);
        }
        tableCount++;
        return table;
    }

    @Override
    public void clear() {
        Misc.clearObjList(tables);
        tableCount = 0;
    }

    /**
     * The table the scan was bound from, found by its bound token and metadata version rather than by name; the
     * factories keep the bound token, so cursor open rejects a name that has moved to another table.
     */
    public PlanTable of(ScanPlan scan) {
        final int index = indexOf(scan.getTableToken(), scan.getMetadataVersion());
        if (index < 0) {
            throw new IllegalStateException("scan reads a table the binder did not acquire");
        }
        return tables.getQuick(index);
    }

    /**
     * Re-reads the access facts of every table at its latest transaction, or at the one the execution context pins,
     * before the plan is optimised and generated, and rejects the plan when a table's metadata changed since binding
     * read it, so the compiler binds it again.
     */
    public void refresh(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0; i < tableCount; i++) {
            final PlanTable table = tables.getQuick(i);
            try (TableReader reader = getReader(table.getTableToken(), table.getMetadataVersion(), table.getPosition(), executionContext)) {
                table.readFacts(reader);
            }
        }
    }

    private static TableReader getReader(TableToken token, long metadataVersion, int position, SqlExecutionContext executionContext) throws SqlException {
        try {
            return metadataVersion == TableUtils.ANY_TABLE_VERSION
                    ? executionContext.getReader(token) : executionContext.getReader(token, metadataVersion);
        } catch (EntryLockedException e) {
            throw SqlException.position(position).put("table is locked: ").put(token.getTableName());
        } catch (CairoException e) {
            if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                throw e;
            }
            throw SqlException.position(position).put(e).setTableBusy(e instanceof EntryUnavailableException);
        }
    }

    private int indexOf(TableToken token, long metadataVersion) {
        for (int i = 0; i < tableCount; i++) {
            final PlanTable table = tables.getQuick(i);
            if ((metadataVersion == TableUtils.ANY_TABLE_VERSION || table.getMetadataVersion() == metadataVersion) && table.getTableToken().equals(token)) {
                return i;
            }
        }
        return -1;
    }
}
