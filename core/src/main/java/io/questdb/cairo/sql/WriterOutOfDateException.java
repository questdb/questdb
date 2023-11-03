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

package io.questdb.cairo.sql;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;

public class WriterOutOfDateException extends RuntimeException implements FlyweightMessageContainer {

    private static final ThreadLocal<WriterOutOfDateException> tlException = new ThreadLocal<>(WriterOutOfDateException::new);

    private final StringSink message = new StringSink();

    public static WriterOutOfDateException of(CharSequence outdatedTableName) {
        WriterOutOfDateException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new WriterOutOfDateException()) != null;
        ex.message.clear();
        ex.message.put("['").put(outdatedTableName).put("']");
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
