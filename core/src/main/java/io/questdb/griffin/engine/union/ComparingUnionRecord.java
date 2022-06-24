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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.RecordComparator;

public final class ComparingUnionRecord extends UnionRecord {
    private RecordComparator comparator;

    public void of(Record recordA, Record recordB, RecordComparator comparator) {
        super.of(recordA, recordB);
        this.comparator = comparator;
        this.comparator.setLeft(recordA);
    }

    @Override
    public void of(Record recordA, Record recordB) {
        throw new UnsupportedOperationException("use overloaded of() method with a comparator");
    }

    /**
     * Select the next record by using a comparator
     * <br>
     * Note: The name of the method starts with set to keep it symmetric
     * with {@link #setAb(boolean)}. Otherwise <code>selectByComparing()</code>
     * would be more suitable.
     *
     * @return true when selecting a record from cursorA, otherwise false
     */
    public boolean setByComparing() {
        return useA = (comparator.compare(recordB) <= 0);
    }
}
