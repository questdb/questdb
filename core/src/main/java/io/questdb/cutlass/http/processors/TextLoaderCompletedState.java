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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.std.LongList;

public class TextLoaderCompletedState {
    private static final LongList emptyErrors = new LongList();
    private long writtenLineCount;
    private long parsedLineCount;
    private RecordMetadata metadata;
    private CharSequence tableName;
    private long errorLineCount;
    private boolean isForcedHeader;
    private LongList colCounts;
    private String partitionBy;

    public void copyState(TextLoader textLoader) {
        this.writtenLineCount = textLoader.getWrittenLineCount();
        this.parsedLineCount = textLoader.getParsedLineCount();
        this.errorLineCount = textLoader.getErrorLineCount();
        this.metadata = GenericRecordMetadata.copyOf(textLoader.getMetadata());
        this.tableName = textLoader.getTableName();
        this.isForcedHeader = textLoader.isForceHeaders();

        // Lazy copy errors.
        LongList colCounts = textLoader.getColumnErrorCounts();
        if (colCounts != null) {
            if (this.colCounts == null) {
                this.colCounts = new LongList(colCounts);
            } else {
                this.colCounts.clear();
                this.colCounts.add(colCounts);
            }
        } else {
            if (this.colCounts != null) {
                this.colCounts.clear();
            }
        }
        this.partitionBy = PartitionBy.toString(textLoader.getPartitionBy());
    }

    public LongList getColumnErrorCounts() {
        return colCounts != null ? colCounts : emptyErrors;
    }

    public long getErrorLineCount() {
        return errorLineCount;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getParsedLineCount() {
        return parsedLineCount;
    }

    public String getPartitionBy() {
        return partitionBy;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public long getWrittenLineCount() {
        return writtenLineCount;
    }

    public boolean isForceHeaders() {
        return isForcedHeader;
    }
}
