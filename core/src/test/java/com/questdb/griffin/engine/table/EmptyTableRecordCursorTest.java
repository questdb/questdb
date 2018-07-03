/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import org.junit.Test;

public class EmptyTableRecordCursorTest {

    private static final EmptyTableRecordCursor CURSOR = new EmptyTableRecordCursor();

    @Test(expected = UnsupportedOperationException.class)
    public void testNext() {
        // next must always fail
        CURSOR.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordAt() {
        // This cursor does not return row ids. Looking up records from cursor by rowid is most
        // likely a bug. Make sure we report it.
        CURSOR.recordAt(123);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordAt2() {
        // This cursor does not return row ids. Looking up records from cursor by rowid is most
        // likely a bug. Make sure we report it.
        CURSOR.recordAt(CURSOR.getRecord(), 123);
    }
}