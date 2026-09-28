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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

// Tables (with the metadata version the optimiser read) and views (with the definition
// txn the parser expanded) that a compiled SELECT plan depends on.
public class PlanDependencies implements Mutable {
    private final LongList tableMetadataVersions = new LongList();
    private final ObjList<TableToken> tableTokens = new ObjList<>();
    private final LongList viewSeqTxns = new LongList();
    private final ObjList<TableToken> viewTokens = new ObjList<>();

    public void addTable(TableToken tableToken, long metadataVersion) {
        tableTokens.add(tableToken);
        tableMetadataVersions.add(metadataVersion);
    }

    public void addViews(ObjList<ViewDefinition> views) {
        for (int i = 0, n = views.size(); i < n; i++) {
            final ViewDefinition view = views.getQuick(i);
            viewTokens.add(view.getViewToken());
            viewSeqTxns.add(view.getSeqTxn());
        }
    }

    @Override
    public void clear() {
        tableTokens.clear();
        tableMetadataVersions.clear();
        viewTokens.clear();
        viewSeqTxns.clear();
    }

    public void copyFrom(PlanDependencies other) {
        clear();
        tableTokens.addAll(other.tableTokens);
        tableMetadataVersions.add(other.tableMetadataVersions);
        viewTokens.addAll(other.viewTokens);
        viewSeqTxns.add(other.viewSeqTxns);
    }

    public boolean isCurrent(CairoEngine engine) {
        for (int i = 0, n = viewTokens.size(); i < n; i++) {
            final TableToken viewToken = viewTokens.getQuick(i);
            if (!viewToken.equals(engine.getTableTokenIfExists(viewToken.getTableName()))) {
                return false;
            }
            final ViewDefinition view = engine.getViewGraph().getViewDefinition(viewToken);
            if (view == null || view.getSeqTxn() != viewSeqTxns.getQuick(i)) {
                return false;
            }
        }
        final int tableCount = tableTokens.size();
        if (tableCount == 0) {
            return true;
        }
        final MetadataCache metadataCache = engine.getMetadataCache();
        for (int i = 0; i < tableCount; i++) {
            final TableToken tableToken = tableTokens.getQuick(i);
            if (!tableToken.equals(engine.getTableTokenIfExists(tableToken.getTableName()))) {
                return false;
            }
            metadataCache.hydrateTableOnDemand(tableToken);
        }
        try (MetadataCacheReader metadataRO = metadataCache.readLock()) {
            for (int i = 0; i < tableCount; i++) {
                final TableToken tableToken = tableTokens.getQuick(i);
                final CairoTable table = metadataRO.getTable(tableToken);
                if (table == null
                        || !tableToken.equals(table.getTableToken())
                        || table.getMetadataVersion() != tableMetadataVersions.getQuick(i)) {
                    return false;
                }
            }
        }
        return true;
    }
}
