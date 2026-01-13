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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class ViewStateStoreImpl implements ViewStateStore {
    private final CairoEngine engine;
    private final MicrosecondClock microsecondClock;
    private final ConcurrentHashMap<ViewState> stateByTableDirName = new ConcurrentHashMap<>();
    private final ThreadLocal<ViewCompilerTask> taskHolder = new ThreadLocal<>(ViewCompilerTask::new);
    // using unlimited concurrent queue to avoid dropping notifications when the queue is full,
    // generally not expecting too many view compile notifications, but if table schema changes
    // are very frequent, then the number of notifications can be high too
    private final Queue<ViewCompilerTask> taskQueue = ConcurrentQueue.createConcurrentQueue(ViewCompilerTask::new);

    public ViewStateStoreImpl(CairoEngine engine) {
        this.engine = engine;
        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    @TestOnly
    @Override
    public void clear() {
        stateByTableDirName.clear();
        taskQueue.clear();
    }

    @Override
    public void createViewState(ViewDefinition viewDefinition, @Nullable RecordMetadata metadata) {
        final TableToken viewToken = viewDefinition.getViewToken();
        final ViewMetadata viewMetadata = metadata != null
                ? ViewMetadata.newInstance(viewToken, metadata)
                : ViewMetadata.newInstance(viewToken);
        final ViewState state = new ViewState(viewDefinition, viewMetadata, microsecondClock.getTicks());

        final ViewState prevState = stateByTableDirName.putIfAbsent(viewToken.getDirName(), state);
        if (prevState != null) {
            throw CairoException.critical(0).put("view state already exists [dir=").put(viewToken.getDirName());
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(state.getViewMetadata());
        }
    }

    @Override
    public void enqueueCompile(@NotNull TableToken tableToken) {
        final ViewCompilerTask task = taskHolder.get();
        task.tableToken = tableToken;
        task.updateTimestamp = microsecondClock.getTicks();
        taskQueue.enqueue(task);
    }

    @Override
    public ViewState getViewState(TableToken viewToken) {
        return stateByTableDirName.get(viewToken.getDirName());
    }

    @Override
    public void removeViewState(TableToken viewToken) {
        stateByTableDirName.remove(viewToken.getDirName());
    }

    @Override
    public boolean tryDequeueCompilerTask(ViewCompilerTask task) {
        return taskQueue.tryDequeue(task);
    }
}
