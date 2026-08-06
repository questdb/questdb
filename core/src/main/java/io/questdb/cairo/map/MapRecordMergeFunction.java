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

package io.questdb.cairo.map;

@FunctionalInterface
public interface MapRecordMergeFunction {

    /**
     * Called after a source record's key was absent from the merge destination and the
     * complete key-value entry was copied into it.
     */
    void mergeNew(MapRecord srcRecord);

    /**
     * Reports a small batch of newly admitted source rows. Row IDs remain valid until the
     * enclosing map merge returns.
     */
    default void mergeNewBatch(MapRecord srcRecord, long[] srcRowIds, int size) {
        for (int i = 0; i < size; i++) {
            srcRecord.of(srcRowIds[i]);
            mergeNew(srcRecord);
        }
    }
}
