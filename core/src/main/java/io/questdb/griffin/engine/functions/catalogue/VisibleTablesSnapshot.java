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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.DefaultLocalCacheSnapshotFactory;
import io.questdb.cairo.SecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceObjMap;
import io.questdb.std.ObjList;

/**
 * The tables of the metadata cache that the current principal may see, in snapshot order.
 * Catalogue cursors built on the metadata cache iterate this instead of the raw snapshot.
 * <p>
 * {@link #refresh(SqlExecutionContext)} re-applies the visibility filter on every call, even
 * when the snapshot itself is unchanged: the snapshot version tracks metadata changes, not ACL
 * changes, and the select caches share a compiled factory across principals, so the principal
 * of the previous refresh says nothing about the current one.
 */
public class VisibleTablesSnapshot {
    private final CharSequenceObjMap<CairoTable> snapshot;
    private final ObjList<CairoTable> visibleTables = new ObjList<>();
    private long snapshotVersion = -1;

    public VisibleTablesSnapshot(CairoConfiguration configuration) {
        snapshot = DefaultLocalCacheSnapshotFactory.INSTANCE.newInstance(configuration);
    }

    public CairoTable getQuick(int index) {
        return visibleTables.getQuick(index);
    }

    public void refresh(SqlExecutionContext executionContext) {
        // Reconciles against the table registry before snapshotting, so the
        // catalogue is complete even mid startup hydration.
        snapshotVersion = executionContext.getCairoEngine().getMetadataCache().snapshot(snapshot, snapshotVersion);
        final SecurityContext securityContext = executionContext.getSecurityContext();
        visibleTables.clear();
        for (int i = 0, n = snapshot.size(); i < n; i++) {
            final CairoTable table = snapshot.getAt(i);
            if (securityContext.isTableVisible(table.getTableToken())) {
                visibleTables.add(table);
            }
        }
    }

    public int size() {
        return visibleTables.size();
    }
}
