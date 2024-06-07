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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

public class ParquetFileRecordCursor implements NoRandomAccessRecordCursor {
    private final RecordMetadata metadata;
    private final Path path;

    public ParquetFileRecordCursor(FilesFacade ff, Path path, RecordMetadata metadata) {
        this.path = path;
        this.metadata = metadata;
    }

    @Override
    public void close() {
    }

    @Override
    public Record getRecord() {
        return null;
    }

    @Override
    public boolean hasNext() throws DataUnavailableException {
        return false;
    }

    public void of(SqlExecutionContext executionContext) {
        try {
            // TODO:
        } catch (DataUnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() throws DataUnavailableException {
        return 0;
    }

    @Override
    public void toTop() {
    }
}
