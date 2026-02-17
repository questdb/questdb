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

package io.questdb.griffin;

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.ColumnType;

public final class AnyRecordMetadata extends AbstractRecordMetadata {
    public static final AnyRecordMetadata INSTANCE = new AnyRecordMetadata();

    private AnyRecordMetadata() {
        columnCount = 0;
    }

    // this is a hack metadata, which will be able to validate any column name as index 0 and type LONG
    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        return 0;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return ColumnType.LONG;
    }
}
