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

package io.questdb.cairo.sql;

import io.questdb.cairo.*;
import io.questdb.cairo.wal.TableWriterBackend;
import io.questdb.griffin.SqlException;
import io.questdb.tasks.TableWriterTask;

import java.io.Closeable;

public interface AsyncWriterCommand extends Closeable {
    long apply(TableWriterBackend tableWriter, boolean contextAllowsAnyStructureChanges) throws SqlException, AlterTableContextException;

    AsyncWriterCommand deserialize(TableWriterTask task);

    int getCommandType();

    String getCommandName();

    int getTableId();

    String getTableName();

    int getTableNamePosition();

    long getTableVersion();

    long getCorrelationId();

    boolean isStructureChange();

    void startAsync();

    void serialize(TableWriterTask task);

    void setCommandCorrelationId(long correlationId);

    interface Error {
        int OK = 0;
        int READER_OUT_OF_DATE = -1;
        int STRUCTURE_CHANGE_NOT_ALLOWED = -2;
        int SQL_OR_CAIRO_ERROR = -3;
        int UNEXPECTED_ERROR = -4;
    }
}
