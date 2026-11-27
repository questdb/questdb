/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;

/**
 * One file of one table, for a test facade that fails calls on that file: the descriptor that the writer opened it
 * under last, with or without the column name txn that a column added later has, as in y.d.1. The facade passes
 * each file that openRW() opens to {@link #onOpen(LPSZ, long)}.
 */
public class TableFile {
    private final String fileSuffix;
    private final String tableDirPrefix;
    private volatile long fd = -1;

    public TableFile(String tableName, String fileName) {
        this.tableDirPrefix = Files.SEPARATOR + tableName + '~';
        this.fileSuffix = Files.SEPARATOR + fileName;
    }

    public boolean is(long fd) {
        return fd == this.fd;
    }

    public void onOpen(LPSZ name, long fd) {
        if (Utf8s.containsAscii(name, tableDirPrefix)
                && (Utf8s.endsWithAscii(name, fileSuffix) || Utf8s.containsAscii(name, fileSuffix + '.'))) {
            this.fd = fd;
        }
    }
}
