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

package io.questdb.griffin;

import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;

public interface CompiledQuery {
    int SELECT = 0;
    int INSERT = 1;
    int TRUNCATE = 2;
    int ALTER = 3;
    int REPAIR = 4;
    int SET = 5;
    int DROP = 6;
    int COPY_LOCAL = 7;
    int CREATE_TABLE = 8;
    int INSERT_AS_SELECT = 9;
    int COPY_REMOTE = 10;
    int RENAME_TABLE = 11;
    int BACKUP_TABLE = 12;
    int SHOW = 13;

    RecordCursorFactory getRecordCursorFactory();

    InsertStatement getInsertStatement();

    TextLoader getTextLoader();

    int getType();
}
