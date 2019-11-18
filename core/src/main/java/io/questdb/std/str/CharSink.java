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

package io.questdb.std.str;

import io.questdb.std.Sinkable;

public interface CharSink {

    CharSink encodeUtf8(CharSequence cs);

    CharSink encodeUtf8(CharSequence cs, int from, int len);

    CharSink encodeUtf8AndQuote(CharSequence cs);

    default void flush() {
    }

    default CharSink put(CharSequence cs) {
        throw new UnsupportedOperationException();
    }

    default CharSink put(CharSequence cs, int start, int end) {
        throw new UnsupportedOperationException();
    }

    CharSink put(char c);

    CharSink putUtf8(char c);

    CharSink put(int value);

    CharSink put(long value);

    CharSink put(float value, int scale);

    CharSink put(double value, int scale);

    CharSink put(boolean value);

    CharSink put(Throwable e);

    CharSink put(Sinkable sinkable);

    CharSink putISODate(long value);

    CharSink putISODateMillis(long value);

    CharSink putQuoted(CharSequence cs);
}
