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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;

public class CopyExportException extends CairoException implements Sinkable, FlyweightMessageContainer {

    private static final io.questdb.std.ThreadLocal<CopyExportException> tlException = new ThreadLocal<>(CopyExportException::new);
    private CopyExportRequestTask.Phase phase;

    public static CopyExportException instance(CopyExportRequestTask.Phase phase, CharSequence message, int errno) {
        CopyExportException te = tlException.get();
        te.phase = phase;
        StringSink sink = te.message;
        sink.clear();
        te.errno = errno;
        sink.put(message);
        return te;
    }

    public static CopyExportException instance(CopyExportRequestTask.Phase phase, int errno) {
        CopyExportException te = tlException.get();
        te.phase = phase;
        te.message.clear();
        te.errno = errno;
        return te;
    }

    public CopyExportRequestTask.Phase getPhase() {
        return phase;
    }
}
