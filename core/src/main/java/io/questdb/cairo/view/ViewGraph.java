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

package io.questdb.cairo.view;

import io.questdb.cairo.TableToken;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Function;

/**
 * Holds view definitions.
 * This object is always in-use, even when views are disabled or the node is a read-only replica.
 */
public class ViewGraph implements Mutable {
    private final Function<CharSequence, ViewDependencyList> createDependencyList;
    private final ConcurrentHashMap<ViewDefinition> definitionsByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ViewDependencyList> dependentViewsByTableName = new ConcurrentHashMap<>(false);

    public ViewGraph() {
        createDependencyList = name -> new ViewDependencyList();
    }

    public boolean addView(ViewDefinition viewDefinition) {
        final TableToken viewToken = viewDefinition.getViewToken();
        final ViewDefinition prevDefinition = definitionsByTableDirName.putIfAbsent(viewToken.getDirName(), viewDefinition);

        final ObjList<CharSequence> dependencies = viewDefinition.getDependencies();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final ViewDependencyList list = getOrCreateDependentViews(dependencies.getQuick(i));
            final ObjList<TableToken> dependentViews = list.lockForWrite();
            try {
                dependentViews.add(viewToken);
            } finally {
                list.unlockAfterWrite();
            }
        }

        // WAL table directories are unique, so we don't expect previous value
        return prevDefinition == null;
    }

    @TestOnly
    @Override
    public void clear() {
        definitionsByTableDirName.clear();
        dependentViewsByTableName.clear();
    }

    public void getDependentViews(TableToken tableToken, ObjList<TableToken> sink) {
        final ViewDependencyList list = getOrCreateDependentViews(tableToken.getTableName());
        final ReadOnlyObjList<TableToken> dependentViews = list.lockForRead();
        try {
            sink.addAll(dependentViews);
        } finally {
            list.unlockAfterRead();
        }
    }

    public ViewDefinition getViewDefinition(TableToken viewToken) {
        return definitionsByTableDirName.get(viewToken.getDirName());
    }

    public void getViews(ObjList<TableToken> sink) {
        for (ViewDefinition viewDefinition : definitionsByTableDirName.values()) {
            sink.add(viewDefinition.getViewToken());
        }
    }

    public void removeView(TableToken viewToken) {
        final ViewDefinition viewDefinition = definitionsByTableDirName.remove(viewToken.getDirName());
        if (viewDefinition == null) {
            return;
        }

        final ObjList<CharSequence> dependencies = viewDefinition.getDependencies();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            final ViewDependencyList list = getOrCreateDependentViews(dependencies.getQuick(i));
            final ObjList<TableToken> dependentViews = list.lockForWrite();
            try {
                dependentViews.remove(viewToken);
            } finally {
                list.unlockAfterWrite();
            }
        }
    }

    @NotNull
    private ViewDependencyList getOrCreateDependentViews(CharSequence tableName) {
        return dependentViewsByTableName.computeIfAbsent(tableName, createDependencyList);
    }
}
