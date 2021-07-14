/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.std.LongList;

public class TextLoaderCompletedState {
    private long writtenLineCount;
    private RecordMetadata metadata;
    private TextLoader textLoader;

    public void copyState(TextLoader textLoader) {
        // Some values are come from TableWriter and has to be copied
        // in order to release TableWriter back to the Engine
        this.writtenLineCount = textLoader.getWrittenLineCount();
        this.metadata = textLoader.getMetadata() != null
                ? GenericRecordMetadata.copyOf(textLoader.getMetadata())
                : null;
        // Some values are safe to get from TextLoader
        this.textLoader = textLoader;
    }

    public LongList getColumnErrorCounts() {
        return textLoader.getColumnErrorCounts();
    }

    public long getErrorLineCount() {
        return textLoader.getErrorLineCount();
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getParsedLineCount() {
        return textLoader.getParsedLineCount();
    }

    public int getPartitionBy() {
        return textLoader.getPartitionBy();
    }

    public int getMaxUncommittedRows() {
        return textLoader.getMaxUncommittedRows();
    }

    public CharSequence getTableName() {
        return textLoader.getTableName();
    }

    public CharSequence getTimestampCol() {
        return textLoader.getTimestampCol();
    }

    public long getWrittenLineCount() {
        return writtenLineCount;
    }

    public boolean isForceHeaders() {
        return textLoader.isForceHeaders();
    }

    public int getWarnings() {
        return textLoader.getWarnings();
    }
}
