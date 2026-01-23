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

package io.questdb.cairo;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;

public class CommitFailedException extends Exception {
    private static final ThreadLocal<CommitFailedException> tlException = new ThreadLocal<>(CommitFailedException::new);
    protected final StringSink message = new StringSink();
    private Throwable reason;
    private boolean tableDropped;

    public static CommitFailedException instance(Throwable reason, boolean tableDropped) {
        CommitFailedException ex = tlException.get();
        assert (ex = new CommitFailedException()) != null;
        ex.reason = reason;
        ex.message.clear();
        if (!tableDropped) {
            if (reason instanceof FlyweightMessageContainer) {
                ex.message.put(((FlyweightMessageContainer) reason).getFlyweightMessage());
            } else {
                ex.message.put(reason.getMessage());
            }
        } else {
            ex.message.put("table dropped");
        }
        ex.tableDropped = tableDropped;
        return ex;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public Throwable getReason() {
        return reason;
    }

    public boolean isTableDropped() {
        return tableDropped;
    }
}