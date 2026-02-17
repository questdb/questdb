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

package io.questdb.metrics;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sink;

class PrometheusFormatUtils {
    static final char LF = '\n';
    static final CharSequence METRIC_NAME_PREFIX = "questdb_";
    static final CharSequence TYPE_PREFIX = "# TYPE questdb_";

    static void appendCounterNamePrefix(CharSequence name, CharSink<?> sink) {
        sink.putAscii(METRIC_NAME_PREFIX);
        sink.put(name);
        sink.putAscii("_total");
    }

    static void appendCounterType(CharSequence name, CharSink<?> sink) {
        sink.putAscii(TYPE_PREFIX);
        sink.put(name);
        sink.putAscii("_total counter\n");
    }

    static void appendLabel(Utf8Sink utf8Sink, CharSequence labelName, CharSequence labelValue) {
        utf8Sink.put(labelName);
        utf8Sink.putAscii('=');
        utf8Sink.putQuote().escapeJsonStr(labelValue).putQuote();
    }

    static void appendNewLine(CharSink<?> sink) {
        sink.putAscii(LF);
    }

    static void appendSampleLineSuffix(CharSink<?> sink, long value) {
        sink.putAscii(' ');
        sink.put(value);
        sink.putAscii(LF);
    }

    static void appendSampleLineSuffix(CharSink<?> sink, double value) {
        sink.putAscii(' ');
        sink.put(value);
        sink.putAscii(LF);
    }
}
