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
import io.questdb.std.Mutable;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public interface MatViewGraph extends QuietCloseable, Mutable {

    // only adds the view, no refresh initiated, no telemetry event logged
    MatViewRefreshState addView(MatViewDefinition viewDefinition);

    @TestOnly
    @Override
    void clear();

    // adds the view, initiates refresh, logs telemetry event
    void createView(MatViewDefinition viewDefinition);

    void dropViewIfExists(TableToken matViewToken);

    void enqueueFullRefresh(TableToken matViewToken);

    void enqueueIncrementalRefresh(TableToken matViewToken);

    void enqueueInvalidate(TableToken matViewToken, String invalidationReason);

    void getDependentMatViews(TableToken baseTableToken, ObjList<TableToken> sink);

    @Nullable
    MatViewDefinition getViewDefinition(TableToken matViewToken);

    @Nullable
    MatViewRefreshState getViewRefreshState(TableToken matViewToken);

    void getViews(ObjList<TableToken> bucket);

    void notifyBaseInvalidated(TableToken baseTableToken);

    void notifyBaseRefreshed(MatViewRefreshTask task, long seqTxn);

    void notifyTxnApplied(MatViewRefreshTask task, long seqTxn);

    /**
     * Writes all table tokens to the destination list in order, so that dependent materialized views
     * go first followed by their base tables (or materialized views).
     * <p>
     * This is used for checkpoints: we want to first take a snapshot of a mat view and only then
     * take a snapshot its base table. That's to prevent situation when a checkpoint contains
     * mat view refreshed with "ghost" base table data that is newer than what's in the checkpoint.
     *
     * @param tables      source set of all table tokens
     * @param orderedSink destination list
     */
    void orderByDependentViews(ObjHashSet<TableToken> tables, ObjList<TableToken> orderedSink);

    boolean tryDequeueRefreshTask(MatViewRefreshTask task);
}
