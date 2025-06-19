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

import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * View state serves the purpose of keeping track of invalidated views.
 */
public class ViewState {
    public static final String VIEW_STATE_FILE_NAME = "_view.s";
    public static final int VIEW_STATE_FORMAT_MSG_TYPE = 0;

    private final ViewDefinition viewDefinition;
    private volatile boolean dropped;
    private volatile boolean invalid;

    public ViewState(@NotNull ViewDefinition viewDefinition) {
        this.viewDefinition = viewDefinition;
    }

    public static void append(
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            @NotNull BlockFileWriter writer
    ) {
        final AppendableBlock block = writer.append();
        appendState(invalid, invalidationReason, block);
        block.commit(VIEW_STATE_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public @NotNull ViewDefinition getViewDefinition() {
        return viewDefinition;
    }

    public void init() {
    }

    public void initFromReader(ViewStateReader reader) {
        setInvalidFlag(reader.isInvalid());
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

    public void setInvalidFlag(boolean invalid) {
        this.invalid = invalid;
    }

    private static void appendState(
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            @NotNull AppendableBlock block
    ) {
        block.putBool(invalid);
        block.putStr(invalidationReason);
    }
}
