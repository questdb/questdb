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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Holds view definitions.
 * This object is always in-use, even when the node is a read-only replica.
 */
public class ViewGraph implements Mutable {
    private static final BiConsumer<ObjList<TableToken>, TableToken> ADD = ObjList::add;
    private static final Function<CharSequence, ViewDependencyList> CREATE_DEPENDENCY_LIST = name -> new ViewDependencyList();
    private static final BiConsumer<ObjList<TableToken>, TableToken> REMOVE = ObjList::remove;
    private final ConcurrentHashMap<ViewDefinition> definitionsByTableDirName = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ViewDependencyList> dependentViewsByTableName = new ConcurrentHashMap<>(false);

    public synchronized boolean addView(ViewDefinition viewDefinition) {
        final TableToken viewToken = viewDefinition.getViewToken();
        final ViewDefinition prevDefinition = definitionsByTableDirName.putIfAbsent(viewToken.getDirName(), viewDefinition);
        if (prevDefinition != null) {
            // WAL table directories are unique, so we don't expect previous value ever,
            // but if it happens somehow, we should ot update dependencies again, just return
            return false;
        }
        updateDependencies(viewToken, viewDefinition, ADD);
        return true;
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

    public synchronized void removeView(TableToken viewToken) {
        final ViewDefinition viewDefinition = definitionsByTableDirName.remove(viewToken.getDirName());
        if (viewDefinition == null) {
            // view has been dropped concurrently
            return;
        }

        updateDependencies(viewToken, viewDefinition, REMOVE);
    }

    public synchronized boolean updateView(
            TableToken viewToken,
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            long seqTxn,
            BlockFileWriter blockFileWriter,
            Path path
    ) {
        final ViewDefinition currentDefinition = getViewDefinition(viewToken);
        if (currentDefinition == null) {
            // view has been dropped concurrently
            return false;
        }
        if (seqTxn <= currentDefinition.getSeqTxn()) {
            // view has been updated to a higher txn, or called from primary wal apply job
            return false;
        }

        final ViewDefinition newDefinition = new ViewDefinition();
        newDefinition.init(viewToken, viewSql, dependencies, seqTxn);

        try (BlockFileWriter definitionWriter = blockFileWriter) {
            path.concat(viewToken).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME);
            definitionWriter.of(path.$());
            ViewDefinition.append(newDefinition, definitionWriter);
        }

        definitionsByTableDirName.put(viewToken.getDirName(), newDefinition);

        updateDependencies(viewToken, currentDefinition, REMOVE);
        updateDependencies(viewToken, newDefinition, ADD);
        return true;
    }

    /**
     * Validates that updating a view with the given query model would not create a cycle.
     * Since referencedViews contains all transitively expanded views from parsing,
     * a cycle exists if and only if viewToken appears in that list.
     *
     * @param viewToken the view being altered
     * @param model     the query model containing referenced views
     * @throws CairoException if a circular dependency would be created
     */
    public void validateNoCycle(TableToken viewToken, QueryModel model) {
        final ObjList<ViewDefinition> referencedViews = model.getReferencedViews();

        for (int i = 0, n = referencedViews.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(referencedViews.get(i).getViewToken().getTableName(), viewToken.getTableName())) {
                if (i == 0) {
                    throw CairoException.critical(0)
                            .put("circular dependency detected: view '").put(viewToken.getTableName())
                            .put("' cannot reference itself");
                } else {
                    final CharSequence directDep = referencedViews.get(0).getViewToken().getTableName();
                    throw CairoException.critical(0)
                            .put("circular dependency detected: '").put(viewToken.getTableName())
                            .put("' cannot depend on '").put(directDep)
                            .put("' because '").put(directDep)
                            .put("' already depends on '").put(viewToken.getTableName()).put('\'');
                }
            }
        }
    }

    @NotNull
    private ViewDependencyList getOrCreateDependentViews(CharSequence tableName) {
        return dependentViewsByTableName.computeIfAbsent(tableName, CREATE_DEPENDENCY_LIST);
    }

    private void updateDependencies(TableToken viewToken, ViewDefinition viewDefinition, BiConsumer<ObjList<TableToken>, TableToken> operation) {
        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = viewDefinition.getDependencies();
        final ObjList<CharSequence> tableNames = dependencies.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            final ViewDependencyList list = getOrCreateDependentViews(tableNames.getQuick(i));
            final ObjList<TableToken> dependentViews = list.lockForWrite();
            try {
                operation.accept(dependentViews, viewToken);
            } finally {
                list.unlockAfterWrite();
            }
        }
    }
}
