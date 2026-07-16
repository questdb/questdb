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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;

abstract class AbstractDeleteApplyTest extends AbstractCairoTest {

    protected long count(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql, sqlExecutionContext);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    protected int countDuplicatePartitionVersionDirs(TableToken tableToken) {
        final FilesFacade ff = configuration.getFilesFacade();
        final java.util.HashSet<String> seenDays = new java.util.HashSet<>();
        final int[] duplicates = {0};
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken);
            final int plen = path.size();
            ff.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (ff.isDirOrSoftLinkDirNoDots(path, plen, pUtf8NameZ, type)) {
                    final byte first = Unsafe.getByte(pUtf8NameZ);
                    if (first >= '0' && first <= '9') {
                        final String name = path.toString().substring(plen + 1);
                        final int dot = name.indexOf('.');
                        final String day = dot < 0 ? name : name.substring(0, dot);
                        if (!seenDays.add(day)) {
                            duplicates[0]++;
                        }
                    }
                    path.trimTo(plen);
                }
            });
        }
        return duplicates[0];
    }
}
