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

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * View state serves the purpose of keeping track of invalidated views.
 */
public class ViewState {
    private final StringSink invalidationReason = new StringSink();
    private final ViewDefinition viewDefinition;
    private volatile boolean dropped;
    private volatile boolean invalid;
    private volatile long updateTimestamp;

    public ViewState(@NotNull ViewDefinition viewDefinition, long updateTimestamp) {
        this.viewDefinition = viewDefinition;
        this.updateTimestamp = updateTimestamp;
    }

    public CharSequence getInvalidationReason() {
        return invalidationReason.length() > 0 ? invalidationReason : null;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public @NotNull ViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public void init() {
    }

    public boolean isDropped() {
        return dropped;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public void markAsDropped() {
        dropped = true;
    }

    public synchronized void updateState(boolean invalid, @Nullable CharSequence invalidationReason, long updateTimestamp) {
        this.invalid = invalid;

        this.invalidationReason.clear();
        if (invalidationReason != null) {
            this.invalidationReason.put(invalidationReason);
        }

        this.updateTimestamp = updateTimestamp;
    }
}
