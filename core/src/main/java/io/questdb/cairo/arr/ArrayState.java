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

package io.questdb.cairo.arr;

import io.questdb.std.str.CharSink;

public interface ArrayState {
    int STATE_CLOSE_BRACKET = 3;
    int STATE_CLOSE_QUOTE = 5;
    int STATE_COMMA_DIMS = 2;
    int STATE_COMMA_VALUES = 1;
    int STATE_MAX = STATE_CLOSE_QUOTE + 1;
    int STATE_OPEN_BRACKET = 0;
    int STATE_OPEN_QUOTE = 4;

    boolean notRecorded(int flatIndex);

    void putAsciiIfNotRecorded(int eventType, CharSink<?> sink, char symbol);

    void record(int flatIndex);
}
