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

package io.questdb.metrics;

import io.questdb.cairo.ColumnType;
import io.questdb.std.str.StringSink;

public interface LongGauge extends Target {

    void add(long value);

    void dec();

    CharSequence getName();

    long getValue();

    void inc();

    void putName(StringSink sink);

    @Override
    default void putType(StringSink sink) {
        sink.put("gauge");
    }

    @Override
    default void putValueAsString(StringSink sink) {
        sink.put(getValue());
    }

    @Override
    default void putValueType(StringSink sink) {
        sink.put(ColumnType.nameOf(ColumnType.LONG));
    }

    void setValue(long value);
}
