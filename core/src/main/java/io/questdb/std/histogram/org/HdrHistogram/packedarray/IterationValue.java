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

package io.questdb.std.histogram.org.HdrHistogram.packedarray;

/**
 * An iteration value representing the index iterated to, and the value found at that index
 */
public class IterationValue {
    private int index;
    private long value;

    IterationValue() {
    }

    /**
     * The index iterated to
     *
     * @return the index iterated to
     */
    public int getIndex() {
        return index;
    }

    /**
     * The value at the index iterated to
     *
     * @return the value at the index iterated to
     */
    public long getValue() {
        return value;
    }

    void set(final int index, final long value) {
        this.index = index;
        this.value = value;
    }
}
