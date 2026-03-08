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

package io.questdb.griffin.model;

public final class IntervalOperation {
    public static final short INTERSECT = 1;
    public static final short INTERSECT_BETWEEN = 3;
    public static final short INTERSECT_INTERVALS = 4;
    public static final short NEGATED_BORDERLINE = 4;
    public static final short NONE = 0;
    public static final short SUBTRACT = 5;
    public static final short SUBTRACT_BETWEEN = 6;
    public static final short SUBTRACT_INTERVALS = 7;
    // UNION is used for bracket expansion in dynamic mode: subsequent intervals
    // are unioned with the first before applying the overall operation
    public static final short UNION = 8;
}
