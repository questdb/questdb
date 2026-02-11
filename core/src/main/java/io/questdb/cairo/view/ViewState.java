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

import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * View state serves the purpose of keeping track of view metadata and invalidated flag.
 */
public class ViewState {
    private final StringSink invalidationReason = new StringSink();
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    private final ViewDefinition viewDefinition;
    private boolean invalid;
    private long updateTimestamp;
    private ViewMetadata viewMetadata;

    public ViewState(@NotNull ViewDefinition viewDefinition, long updateTimestamp) {
        this(viewDefinition, ViewMetadata.newInstance(viewDefinition.getViewToken()), updateTimestamp);
    }

    public ViewState(@NotNull ViewDefinition viewDefinition, @NotNull ViewMetadata viewMetadata, long updateTimestamp) {
        this.viewDefinition = viewDefinition;
        this.viewMetadata = viewMetadata;
        this.updateTimestamp = updateTimestamp;
    }

    public Utf16Sink getInvalidationReason(MutableUtf16Sink sink) {
        sink.clear();
        return sink.put(invalidationReason);
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public @NotNull ViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public @NotNull ViewMetadata getViewMetadata() {
        return viewMetadata;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public void lockForRead() {
        lock.readLock().lock();
    }

    public void lockForWrite() {
        lock.writeLock().lock();
    }

    public void unlockAfterRead() {
        lock.readLock().unlock();
    }

    public void unlockAfterWrite() {
        lock.writeLock().unlock();
    }

    public void updateState(boolean invalid, @Nullable CharSequence invalidationReason, @Nullable ViewMetadata viewMetadata, long updateTimestamp) {
        this.invalid = invalid;

        this.invalidationReason.clear();
        if (invalidationReason != null) {
            this.invalidationReason.put(invalidationReason);
        }

        if (viewMetadata != null) {
            this.viewMetadata = viewMetadata;
        }
        this.updateTimestamp = updateTimestamp;
    }
}
