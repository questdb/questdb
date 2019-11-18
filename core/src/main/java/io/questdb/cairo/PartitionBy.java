/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.CharSequenceIntHashMap;

/**
 * Setting partition type on JournalKey to override default settings.
 */
public final class PartitionBy {

    public static final int DAY = 0;
    public static final int MONTH = 1;
    public static final int YEAR = 2;
    /**
     * Data is not partitioned at all,
     * all data is stored in a single directory
     */
    public static final int NONE = 3;
    private final static CharSequenceIntHashMap nameToIndexMap = new CharSequenceIntHashMap();

    static {
        nameToIndexMap.put("DAY", DAY);
        nameToIndexMap.put("MONTH", MONTH);
        nameToIndexMap.put("YEAR", YEAR);
        nameToIndexMap.put("NONE", NONE);
    }

    private PartitionBy() {
    }

    public static int fromString(CharSequence name) {
        return nameToIndexMap.get(name);
    }

    public static String toString(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return "DAY";
            case MONTH:
                return "MONTH";
            case YEAR:
                return "YEAR";
            case NONE:
                return "NONE";
            default:
                return "UNKNOWN";
        }
    }
}
