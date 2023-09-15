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

package io.questdb.cutlass.line;

import io.questdb.cutlass.line.tcp.LineTcpParser;

public class LineTcpTimestampAdapter {
    // Default adapter for line timestamps (nano).
    public static final LineTcpTimestampAdapter DEFAULT_TS_INSTANCE = new LineTcpTimestampAdapter(LineNanoTimestampAdapter.INSTANCE);
    // Adapter for line timestamp columns (micro).
    public static final LineTcpTimestampAdapter TS_COLUMN_INSTANCE = new LineTcpTimestampAdapter(LineMicroTimestampAdapter.INSTANCE);

    private final LineTimestampAdapter defaultAdapter;

    public LineTcpTimestampAdapter(LineTimestampAdapter defaultAdapter) {
        this.defaultAdapter = defaultAdapter;
    }

    public long getMicros(long timestamp, byte unit) {
        LineTimestampAdapter adapter = defaultAdapter;
        switch (unit) {
            case LineTcpParser.ENTITY_UNIT_NANO:
                adapter = LineNanoTimestampAdapter.INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MICRO:
                adapter = LineMicroTimestampAdapter.INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MILLI:
                adapter = LineMilliTimestampAdapter.INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_SECOND:
                adapter = LineSecondTimestampAdapter.INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_MINUTE:
                adapter = LineMinuteTimestampAdapter.INSTANCE;
                break;
            case LineTcpParser.ENTITY_UNIT_HOUR:
                adapter = LineHourTimestampAdapter.INSTANCE;
                break;
        }
        return adapter.getMicros(timestamp);
    }
}
