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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

public class NoOpMatViewGraph implements MatViewGraph {
    public static final NoOpMatViewGraph INSTANCE = new NoOpMatViewGraph();

    @Override
    public MatViewRefreshState addView(MatViewDefinition viewDefinition) {
        return null;
    }

    @TestOnly
    @Override
    public void clear() {
    }

    @Override
    public void close() {
    }

    @Override
    public void createView(MatViewDefinition viewDefinition) {
    }

    @Override
    public void dropViewIfExists(TableToken matViewToken) {
    }

    @Override
    public void enqueueFullRefresh(TableToken matViewToken) {
    }

    @Override
    public void enqueueIncrementalRefresh(TableToken matViewToken) {
    }

    @Override
    public void enqueueInvalidate(TableToken matViewToken, String invalidationReason) {
    }

    @Override
    public void getDependentMatViews(TableToken baseTableToken, ObjList<TableToken> sink) {
    }

    @Override
    public MatViewDefinition getViewDefinition(TableToken matViewToken) {
        return null;
    }

    @Override
    public MatViewRefreshState getViewRefreshState(TableToken matViewToken) {
        return null;
    }

    @Override
    public void getViews(ObjList<TableToken> bucket) {
    }

    @Override
    public void notifyBaseInvalidated(TableToken baseTableToken) {
    }

    @Override
    public void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn) {
    }

    @Override
    public void notifyTxnApplied(MatViewRefreshTask task, long seqTxn) {
    }

    @Override
    public void orderByDependentViews(ObjHashSet<TableToken> tables, ObjList<TableToken> orderedSink) {
        orderedSink.clear();
        for (int i = 0, n = tables.size(); i < n; i++) {
            orderedSink.add(tables.get(i));
        }
    }

    @Override
    public boolean tryDequeueRefreshTask(MatViewRefreshTask task) {
        return false;
    }
}
