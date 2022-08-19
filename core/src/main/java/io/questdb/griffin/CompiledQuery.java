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

package io.questdb.griffin;

import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;

public interface CompiledQuery {
    //these values should be covered in both JsonQueryProcessor and PGConnectionContext
    short SELECT = 1;
    short INSERT = 2;
    short TRUNCATE = 3;
    short ALTER = 4;
    short REPAIR = 5;
    short SET = 6;
    short DROP = 7;
    short COPY_LOCAL = 8;
    short CREATE_TABLE = 9;
    short INSERT_AS_SELECT = 10;
    short COPY_REMOTE = 11;
    short RENAME_TABLE = 12;
    short BACKUP_TABLE = 13;
    short UPDATE = 14;
    short LOCK = 15;
    short UNLOCK = 16;
    short VACUUM = 17;
    short BEGIN = 18;
    short COMMIT = 19;
    short ROLLBACK = 20;
    short CREATE_TABLE_AS_SELECT = 21;
    short SNAPSHOT_DB_PREPARE = 22;
    short SNAPSHOT_DB_COMPLETE = 23;
    short TYPES_COUNT = SNAPSHOT_DB_COMPLETE;

    RecordCursorFactory getRecordCursorFactory();

    TextLoader getTextLoader();

    InsertOperation getInsertOperation();

    UpdateOperation getUpdateOperation();

    AlterOperation getAlterOperation();

    short getType();

    /***
     * Executes the query.
     * If execution is done in sync returns an instance of OperationFuture where isDone() is true.
     * If execution happened async, OperationFuture.await() method should be called for blocking wait
     * or OperationFuture.await(long) for wait with specified timeout.
     * @param eventSubSeq - temporary SCSequence instance to track completion of async ALTER command
     * @return query future that can be used to monitor query progress
     * @throws SqlException - throws exception if command execution fails
     */
    OperationFuture execute(SCSequence eventSubSeq) throws SqlException;

    <T extends AbstractOperation> OperationDispatcher<T> getDispatcher();

    <T extends AbstractOperation> T getOperation();

    /**
     * Returns number of rows changed by this command. Used e.g. in pg wire protocol.
     */
    long getAffectedRowsCount();

    CompiledQuery withContext(SqlExecutionContext sqlExecutionContext);
}
