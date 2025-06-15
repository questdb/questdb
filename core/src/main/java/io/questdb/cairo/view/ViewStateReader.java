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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Contains all view state fields, including invalidation reason string.
 */
public class ViewStateReader implements Mutable {
    private final StringSink invalidationReason = new StringSink();
    private boolean invalid;

    @Override
    public void clear() {
        invalid = false;
        invalidationReason.clear();
    }

    @Nullable
    public CharSequence getInvalidationReason() {
        return invalidationReason.length() > 0 ? invalidationReason : null;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public ViewStateReader of(@NotNull WalEventCursor.ViewInvalidationInfo info) {
        invalid = info.isInvalid();
        invalidationReason.clear();
        invalidationReason.put(info.getInvalidationReason());
        return this;
    }

    public ViewStateReader of(@NotNull BlockFileReader reader, @NotNull TableToken viewToken) {
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == ViewState.VIEW_STATE_FORMAT_MSG_TYPE) {
                invalid = block.getBool(0);
                invalidationReason.clear();
                invalidationReason.put(block.getStr(Byte.BYTES));
                return this;
            }
        }
        throw CairoException.critical(0).put("cannot read view state, block not found [view=")
                .put(viewToken.getTableName())
                .put(']');
    }
}
