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

import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.mp.SCSequence;

public interface CompiledQuery {
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
    short LOCK = 14;
    short UNLOCK = 14;
    short VACUUM = 15;

    RecordCursorFactory getRecordCursorFactory();

    InsertStatement getInsertStatement();

    TextLoader getTextLoader();

    AlterStatement getAlterStatement();

    short getType();

    /***
     * Executes the query.
     * If execution is done in sync returns an instance of QueryFuture where isDone() is true.
     * If execution happened async, QueryFuture.await() method should be called for blocking wait
     * or QueryFuture.await(long) for wait with specified timeout.
     * @param eventSubSeq - temporary SCSequence instance to track completion of async ALTER command
     * @return query future that can be used to monitor query progress
     * @throws SqlException - throws exception if command execution fails
     */
    QueryFuture execute(SCSequence eventSubSeq) throws SqlException;
}




