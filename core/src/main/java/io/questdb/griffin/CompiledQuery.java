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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;

public interface CompiledQuery {

    // these values should be covered in both JsonQueryProcessor and PGConnectionContext

    short SELECT = 1;
    short INSERT = SELECT + 1;//2
    short TRUNCATE = INSERT + 1;//3
    short ALTER = TRUNCATE + 1;//4
    short REPAIR = ALTER + 1;//5
    short SET = REPAIR + 1;//6
    short DROP = SET + 1;//7
    short PSEUDO_SELECT = DROP + 1; //8 used for pseudo-SELECT statements such as COPY
    short CREATE_TABLE = PSEUDO_SELECT + 1;//9
    short INSERT_AS_SELECT = CREATE_TABLE + 1;//10
    short COPY_REMOTE = INSERT_AS_SELECT + 1;//11
    short RENAME_TABLE = COPY_REMOTE + 1;//12
    short BACKUP_TABLE = RENAME_TABLE + 1;// 13
    short UPDATE = BACKUP_TABLE + 1;// 14
    short VACUUM = UPDATE + 3;//17 , gap
    short BEGIN = VACUUM + 1;//18
    short COMMIT = BEGIN + 1;//19
    short ROLLBACK = COMMIT + 1;// 20
    short CREATE_TABLE_AS_SELECT = ROLLBACK + 1;// 21
    short SNAPSHOT_DB_PREPARE = CREATE_TABLE_AS_SELECT + 1;//22
    short SNAPSHOT_DB_COMPLETE = SNAPSHOT_DB_PREPARE + 1;//23
    short DEALLOCATE = SNAPSHOT_DB_COMPLETE + 1;//24
    short EXPLAIN = DEALLOCATE + 1;//25
    short TABLE_RESUME = EXPLAIN + 1;//26
    short TABLE_SET_TYPE = TABLE_RESUME + 1;//27
    short CREATE_USER = TABLE_SET_TYPE + 1;//28
    short ALTER_USER = CREATE_USER + 1;//29
    short TYPES_COUNT = ALTER_USER;

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

    TableToken getTableToken();

    TextLoader getTextLoader();

    short getType();

    UpdateOperation getUpdateOperation();

    CompiledQuery withContext(SqlExecutionContext sqlExecutionContext);

    void withSqlStatement(String sqlStatement);
}
