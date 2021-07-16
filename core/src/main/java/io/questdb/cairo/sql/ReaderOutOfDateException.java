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

package io.questdb.cairo.sql;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;

public class ReaderOutOfDateException  extends RuntimeException implements FlyweightMessageContainer {
    private static final ThreadLocal<ReaderOutOfDateException> tlException = new ThreadLocal<>(ReaderOutOfDateException::new);
    private static final String prefix = "cached query plan cannot be used because table schema has changed [table='";
    private final StringSink message = (StringSink)new StringSink().put(prefix);

    public static ReaderOutOfDateException of(CharSequence tableName) {
        ReaderOutOfDateException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new ReaderOutOfDateException()) != null;
        ex.message.clear(prefix.length());
        ex.message.put(tableName).put("']");
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
}
