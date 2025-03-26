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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MatViewRefreshStateReader {
    private boolean invalid;
    private String invalidationReason;
    private long lastRefreshBaseTxn = -1;
    private long lastRefreshTimestamp = Numbers.LONG_NULL;

    @Nullable
    public String getInvalidationReason() {
        return invalidationReason;
    }

    public long getLastRefreshBaseTxn() {
        return lastRefreshBaseTxn;
    }

    public long getLastRefreshTimestamp() {
        return lastRefreshTimestamp;
    }

    public boolean isInvalid() {
        return invalid;
    }

    public MatViewRefreshStateReader of(
            @NotNull BlockFileReader reader,
            @NotNull TableToken matViewToken
    ) {
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        // Iterate through the blocks until we find the one we recognize.
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() != MatViewRefreshState.MAT_VIEW_STATE_FORMAT_MSG_TYPE) {
                // Unknown block, skip.
                continue;
            }
            invalid = block.getBool(0);
            lastRefreshBaseTxn = block.getLong(Byte.BYTES);
            // todo: add lastRefreshTimestamp to the state file
            lastRefreshTimestamp = Numbers.LONG_NULL;
            invalidationReason = Chars.toString(block.getStr(Long.BYTES + Byte.BYTES));
            return this;
        }
        throw CairoException.critical(0).put("cannot read materialized view state, block not found [view=")
                .put(matViewToken.getTableName())
                .put(']');
    }
}
