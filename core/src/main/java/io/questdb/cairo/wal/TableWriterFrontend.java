/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.BaseRecordMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;

import java.io.Closeable;

public interface TableWriterFrontend extends Closeable {
    long applyAlter(AlterOperation operation, boolean contextAllowsAnyStructureChanges)  throws SqlException, AlterTableContextException;

    long applyUpdate(UpdateOperation operations)  throws SqlException;

    @Override
    void close();

    long commit();

    long commitWithLag(long commitLag);

    BaseRecordMetadata getMetadata();

    long getStructureVersion();

    CharSequence getTableName();

    TableWriter.Row newRow();

    TableWriter.Row newRow(long timestamp);

    void rollback();
}
