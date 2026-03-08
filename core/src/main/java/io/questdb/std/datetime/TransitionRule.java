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

package io.questdb.std.datetime;

public class TransitionRule {
    public static final int STANDARD = 1;
    public static final int UTC = 0;
    public static final int WALL = 2;
    public final int dom;
    public final int dow;
    public final int hour;
    public final boolean midnightEOD;
    public final int minute;
    public final int month;
    public final int offsetAfter;
    public final int offsetBefore;
    public final int second;
    public final int standardOffset;
    public final int timeDef;

    public TransitionRule(
            int offsetBefore,
            int offsetAfter,
            int standardOffset,
            int dow,
            int dom,
            int month,
            boolean midnightEOD,
            int hour,
            int minute,
            int second,
            int timeDef
    ) {
        this.offsetBefore = offsetBefore;
        this.offsetAfter = offsetAfter;
        this.standardOffset = standardOffset;
        this.dow = dow;
        this.dom = dom;
        this.month = month;
        this.midnightEOD = midnightEOD;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.timeDef = timeDef;
    }
}
