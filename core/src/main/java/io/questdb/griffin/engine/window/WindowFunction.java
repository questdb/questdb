/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.std.IntList;

public interface WindowFunction extends Function {
    int ONE_PASS = 1;
    int TWO_PASS = 2;
    int ZERO_PASS = 0;

    default void computeNext(Record record) {
    }

    /**
     * @return number of additional passes over base data set required to calculate this function.
     * {@link  #ZERO_PASS} means window function can be calculated on the fly and doesn't require additional passes .
     */
    default int getPassCount() {
        return ONE_PASS;
    }

    void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order);

    void pass1(Record record, long recordOffset, WindowSPI spi);

    default void pass2(Record record, long recordOffset, WindowSPI spi) {
    }

    default void preparePass2() {
    }

    /**
     * Releases native memory and resets internal state to default/initial. Called on [Cached]Window cursor close.
     **/
    void reset();

    /*
          Set index of record chain column used to store window function result.
         */
    void setColumnIndex(int columnIndex);
}
