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
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

/**
 * It holds per-view states.
 * <p>
 * The actual implementation may be no-op in case of disabled views or read-only replica.
 */
public interface ViewStateStore extends Mutable {

    // Only creates the view state, no telemetry event logged.
    ViewState addViewState(ViewDefinition viewDefinition);

    // Creates the view state, logs telemetry event.
    void createViewState(ViewDefinition viewDefinition);

    void enqueueCompile(TableToken viewToken);

    @Nullable
    ViewState getViewState(TableToken viewToken);

    void removeViewState(TableToken viewToken);

    boolean tryDequeueCompilerTask(ViewCompilerTask task);
}
