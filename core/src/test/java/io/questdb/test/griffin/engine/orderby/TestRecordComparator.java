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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.RecordComparator;

/**
 * Orders {@link SingleLongRecord}s by their single long column. Uses Long.compare rather than a
 * subtraction, which would overflow for values far apart.
 */
class TestRecordComparator implements RecordComparator {
    Record left;

    @Override
    public int compare(Record record) {
        return Long.compare(left.getLong(0), record.getLong(0));
    }

    @Override
    public void setLeft(Record record) {
        left = record;
    }
}
