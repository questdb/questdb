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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractPartitionFrameCursorFactory implements PartitionFrameCursorFactory {
    private final RecordMetadata metadata;
    private final long metadataVersion;
    private final TableToken tableToken;

    public AbstractPartitionFrameCursorFactory(TableToken tableToken, long metadataVersion, RecordMetadata metadata) {
        this.tableToken = tableToken;
        this.metadataVersion = metadataVersion;
        this.metadata = metadata;
    }

    @Override
    public void close() {
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean supportsTableRowId(TableToken tableToken) {
        return this.tableToken.equals(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.meta("on").val(tableToken.getTableName());
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("{\"name\":\"")
                .put(this.getClass().getSimpleName())
                .putAscii("\", \"table\":\"")
                .put(tableToken)
                .putAscii("\"}");
    }

    protected TableReader getReader(SqlExecutionContext executionContext) {
        return executionContext.getReader(
                tableToken,
                metadataVersion
        );
    }
}
