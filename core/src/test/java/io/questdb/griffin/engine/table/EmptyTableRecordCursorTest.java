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

package io.questdb.griffin.engine.table;

import io.questdb.griffin.engine.EmptyTableRecordCursor;
import org.junit.Test;

public class EmptyTableRecordCursorTest {

    private static final EmptyTableRecordCursor CURSOR = new EmptyTableRecordCursor();

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordAt() {
        // This cursor does not return row ids. Looking up records from cursor by rowid is most
        // likely a bug. Make sure we report it.
        CURSOR.recordAt(123);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSymbolTable() {
        CURSOR.getSymbolTable(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSymbolTable2() {
        CURSOR.getSymbolTable(2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordAt2() {
        // This cursor does not return row ids. Looking up records from cursor by rowid is most
        // likely a bug. Make sure we report it.
        CURSOR.recordAt(CURSOR.getRecord(), 123);
    }
}