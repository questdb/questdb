/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;

public interface CompiledQuery {

    // these values should be covered in both JsonQueryProcessor and PGConnectionContext
    short ALTER = 4;
    short BACKUP_TABLE = 13;
    short BEGIN = 18;
    short COMMIT = 19;
    short COPY_LOCAL = 8;
    short COPY_REMOTE = 11;
    short CREATE_TABLE = 9;
    short CREATE_TABLE_AS_SELECT = 21;
    short DEALLOCATE = 24;
    short DROP = 7;
    short EXPLAIN = 25;
    short INSERT = 2;
    short INSERT_AS_SELECT = 10;
    short RENAME_TABLE = 12;
    short REPAIR = 5;
    short ROLLBACK = 20;
    short SELECT = 1;
    short SET = 6;
    short SNAPSHOT_DB_COMPLETE = 23;
    short SNAPSHOT_DB_PREPARE = 22;
    short TABLE_RESUME = 26;
    short TABLE_SET_TYPE = 27;
    short TRUNCATE = 3;
    short TYPES_COUNT = TABLE_SET_TYPE;
    short UPDATE = 14;
    short VACUUM = 17;

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

    OperationFuture execute(SqlExecutionContext context, SCSequence eventSubSeq, boolean closeOnDone) throws SqlException;

    /**
     * Returns number of rows changed by this command. Used e.g. in pg wire protocol.
     *
     * @return number of rows changed by this command
     */
    long getAffectedRowsCount();

    AlterOperation getAlterOperation();

    InsertOperation getInsertOperation();

    RecordCursorFactory getRecordCursorFactory();

    String getSqlStatement();

    /**
     * Returns statement name for DEALLOCATE statement. Used e.g. in pg wire protocol.
     *
     * @return statement name
     */
    CharSequence getStatementName();

    TextLoader getTextLoader();

    short getType();

    UpdateOperation getUpdateOperation();

    CompiledQuery withContext(SqlExecutionContext sqlExecutionContext);

    void withSqlStatement(String sqlStatement);
}
