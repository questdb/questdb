/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.CharSequenceIntHashMap;

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
    /**
     * Setting partition type to DEFAULT will use whatever partition type is specified in journal configuration.
     */
    public static final int DEFAULT = 4;
    private final static CharSequenceIntHashMap nameToIndexMap = new CharSequenceIntHashMap();

    private PartitionBy() {
    }

    public static int count() {
        return nameToIndexMap.size();
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
            case DEFAULT:
                return "DEFAULT";
            default:
                return "UNKNOWN";
        }
    }

    static {
        nameToIndexMap.put("DAY", DAY);
        nameToIndexMap.put("MONTH", MONTH);
        nameToIndexMap.put("YEAR", YEAR);
        nameToIndexMap.put("NONE", NONE);
        nameToIndexMap.put("DEFAULT", DEFAULT);
    }
}
