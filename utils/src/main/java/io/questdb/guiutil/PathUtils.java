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

package io.questdb.guiutil;

import io.questdb.std.str.Path;

import java.io.File;

final class PathUtils {
    static class ColumnNameTxn {
        final String columnName;
        final long columnNameTxn;

        private ColumnNameTxn(String columnName, long columnNameTxn) {
            this.columnName = columnName;
            this.columnNameTxn = columnNameTxn;
        }
    }

    static ColumnNameTxn columnNameTxnOf(Path p) {
        // columnName and columnNameTxn (selected path may contain suffix columnName.c.txn)
        int len = p.length();
        int nameStart = findSeparatorIdx(p, 1) + 1;
        int dotIdx = findNextDotIdx(p, nameStart);
        int dotIdx2 = findNextDotIdx(p, dotIdx + 1);
        String tmp = p.toString();
        String columnName = tmp.substring(nameStart, dotIdx);
        long columnNameTxn = dotIdx2 == -1 ? -1L : Long.parseLong(tmp.substring(dotIdx2 + 1, len));
        return new ColumnNameTxn(columnName, columnNameTxn);
    }

    static void selectFileInFolder(Path p, int levelUpCount, String fileName) {
        p.trimTo(findSeparatorIdx(p, levelUpCount));
        if (fileName != null) {
            p.concat(fileName);
        }
    }

    static int findNextDotIdx(CharSequence p, int start) {
        int len = p.length();
        int idx = start;
        while (idx < len && p.charAt(idx) != '.') {
            idx++;
        }
        return idx < len ? idx : -1;
    }

    private static int findSeparatorIdx(CharSequence p, int levelUpCount) {
        int idx = p.length() - 2; // may end in separatorChar
        int found = 0;
        while (idx > 0) {
            char c = p.charAt(idx);
            if (c == File.separatorChar) {
                found++;
                if (found == levelUpCount) {
                    break;
                }
            }
            idx--;
        }
        return idx > 0 ? idx : -1;
    }
}
