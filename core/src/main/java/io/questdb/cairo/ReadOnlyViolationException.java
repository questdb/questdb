/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Sinkable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class ReadOnlyViolationException extends RuntimeException implements Sinkable, FlyweightMessageContainer {
    private static final ThreadLocal<ReadOnlyViolationException> THREAD_LOCAL = new ThreadLocal<ReadOnlyViolationException>(ReadOnlyViolationException::new) {
        @Override
        public ReadOnlyViolationException get() {
            ReadOnlyViolationException ex = super.get();
            ex.message.clear();
            return ex;
        }
    };

    private final StringSink message = new StringSink();

    public static ReadOnlyViolationException cannotInsert(String tableName, long partitionTimestamp) {
        ReadOnlyViolationException ex = THREAD_LOCAL.get();
        ex.message.put("cannot insert into read-only partition [table=").put(tableName)
                .put(", partitionTimestamp=");
        TimestampFormatUtils.appendDate(ex.message, partitionTimestamp);
        ex.message.put("]");
        return ex;
    }

    public static ReadOnlyViolationException cannotUpdate(String tableName, long partitionTimestamp) {
        ReadOnlyViolationException ex = THREAD_LOCAL.get();
        ex.message.put("cannot update read-only partition [table=").put(tableName)
                .put(", partitionTimestamp=");
        TimestampFormatUtils.appendDate(ex.message, partitionTimestamp);
        ex.message.put(']');
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put(message);
    }
}
