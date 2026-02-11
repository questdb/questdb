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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class NoOpViewStateStore implements ViewStateStore {
    public static final NoOpViewStateStore INSTANCE = new NoOpViewStateStore();

    @TestOnly
    @Override
    public void clear() {
    }

    @Override
    public void createViewState(ViewDefinition viewDefinition, @Nullable RecordMetadata metadata) {
    }

    @Override
    public void enqueueCompile(@NotNull TableToken viewToken) {
    }

    @Override
    public ViewState getViewState(TableToken viewToken) {
        return null;
    }

    @Override
    public void removeViewState(TableToken viewToken) {
    }

    @Override
    public boolean tryDequeueCompilerTask(ViewCompilerTask task) {
        return false;
    }
}
