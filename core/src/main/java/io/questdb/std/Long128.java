/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std;

public class Long128 {
    public static final int BYTES = 16;

    // this method is used by byte-code generator
    // Note that the arguments are of weird pattern: aLo, aHi, bHi, bLo
    // this is because of alternation of the order when using getLong128Hi, getLong128Lo
    // instead as A, B records.
    // See special cases for Long128 in RecordComparatorCompiler
    public static int compare(long aHi, long aLo, long bHi, long bLo) {

        if (aHi < bHi) {
            return -1;
        }

        if (aHi > bHi) {
            return 1;
        }

        return Long.compareUnsigned(aLo, bLo);
    }

    public static boolean isNull(long lo, long hi) {
        return hi == Numbers.LONG_NULL && lo == Numbers.LONG_NULL;
    }
}
