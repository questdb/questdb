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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.Queue;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

public class ViewStateStoreImpl implements ViewStateStore {
    private final MicrosecondClock microsecondClock;
    private final ConcurrentHashMap<ViewState> stateByTableDirName = new ConcurrentHashMap<>();
    private final ThreadLocal<ViewCompilerTask> taskHolder = new ThreadLocal<>(ViewCompilerTask::new);
    private final Queue<ViewCompilerTask> taskQueue = ConcurrentQueue.createConcurrentQueue(ViewCompilerTask::new);

    // todo: add telemetry
    //private final Telemetry<TelemetryViewTask> telemetry;
    //private final ViewTelemetryFacade telemetryFacade;

    public ViewStateStoreImpl(CairoEngine engine) {
//        this.telemetry = engine.getTelemetryMatView();
//        this.telemetryFacade = telemetry.isEnabled()
//                ? this::storeViewTelemetry
//                : (event, tableToken, errorMessage, latencyUs) -> { /* no-op */ };

        this.microsecondClock = engine.getConfiguration().getMicrosecondClock();
    }

    @Override
    public ViewState addViewState(ViewDefinition viewDefinition) {
        final TableToken viewToken = viewDefinition.getViewToken();
        final ViewState state = new ViewState(viewDefinition, microsecondClock.getTicks());

        final ViewState prevState = stateByTableDirName.putIfAbsent(viewToken.getDirName(), state);
        if (prevState != null) {
            throw CairoException.critical(0).put("view state already exists [dir=").put(viewToken.getDirName());
        }
        return state;
    }

    @TestOnly
    @Override
    public void clear() {
        stateByTableDirName.clear();
    }

    @Override
    public void createViewState(ViewDefinition viewDefinition) {
        addViewState(viewDefinition).init();
    }

    @Override
    public void enqueueCompile(@NotNull TableToken tableToken) {
        enqueueViewTask(tableToken);
    }

    @Override
    public ViewState getViewState(TableToken viewToken) {
        final ViewState state = stateByTableDirName.get(viewToken.getDirName());
        if (state != null) {
            if (state.isDropped()) {
                // Housekeeping
                stateByTableDirName.remove(viewToken.getDirName(), state);
            }
            return state;
        }
        return null;
    }

    @Override
    public void removeViewState(TableToken viewToken) {
        final ViewState state = stateByTableDirName.remove(viewToken.getDirName());
        if (state != null) {
            state.markAsDropped();
        }
    }

    @Override
    public boolean tryDequeueCompilerTask(ViewCompilerTask task) {
        return taskQueue.tryDequeue(task);
    }

    private void enqueueViewTask(@NotNull TableToken tableToken) {
        final ViewCompilerTask task = taskHolder.get();
        task.tableToken = tableToken;
        task.updateTimestamp = microsecondClock.getTicks();
        taskQueue.enqueue(task);
    }

//    private void storeViewTelemetry(short event, TableToken tableToken, CharSequence errorMessage, long latencyUs) {
//        TelemetryViewTask.store(telemetry, event, tableToken.getTableId(), errorMessage, latencyUs);
//    }
}
