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

package io.questdb.cairo.sql;

/**
 * Implements extended operations, related to non-persisted composite types
 * like record and string array.
 */
public interface FunctionExtension {
    /**
     * Returns the array length.
     *
     * @return the array length
     */
    int getArrayLength();

    /**
     * Returns a record from the given record. Implemented in functions that return a record of values.
     *
     * @param rec the input record
     * @return the output record
     */
    Record getRecord(Record rec);

    /**
     * Returns the string value at the given array index (buffer A).
     *
     * @param rec        the record to read from
     * @param arrayIndex the array index
     * @return the string value
     */
    CharSequence getStrA(Record rec, int arrayIndex);

    /**
     * Returns the string value at the given array index (buffer B).
     *
     * @param rec        the record to read from
     * @param arrayIndex the array index
     * @return the string value
     */
    CharSequence getStrB(Record rec, int arrayIndex);

    /**
     * Returns the string length at the given array index.
     *
     * @param rec        the record to read from
     * @param arrayIndex the array index
     * @return the string length
     */
    int getStrLen(Record rec, int arrayIndex);
}
