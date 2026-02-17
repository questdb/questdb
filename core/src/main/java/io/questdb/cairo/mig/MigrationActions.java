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

package io.questdb.cairo.mig;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;

class MigrationActions {
    public static final Log LOG = LogFactory.getLog(MigrationActions.class);
    public static final long META_COLUMN_DATA_SIZE_606 = 16;
    public static final long META_OFFSET_COLUMN_TYPES_606 = 128;
    public static final long TX_OFFSET_MAP_WRITER_COUNT_505 = 72;

    public static long prefixedBlockOffset(long prefix, long index, long blockSize) {
        return prefix + index * blockSize;
    }
}
