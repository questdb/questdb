/*******************************************************************************
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

package io.questdb.cairo;

public interface ColumnTypes {

    /**
     * Returns total size in bytes in case of all fixed-size columns
     * or -1 if there is a var-size column in the given list.
     */
    static int sizeInBytes(ColumnTypes types) {
        if (types == null) {
            return 0;
        }
        int totalSize = 0;
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnType = types.getColumnType(i);
            final int size = ColumnType.sizeOf(columnType);
            if (size > 0) {
                totalSize += size;
            } else {
                return -1;
            }
        }
        return totalSize;
    }

    int getColumnCount();

    int getColumnType(int columnIndex);
}
