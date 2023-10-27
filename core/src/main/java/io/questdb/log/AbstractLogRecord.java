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

package io.questdb.log;

import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public abstract class AbstractLogRecord implements LogRecord {
    private static final ThreadLocal<ObjHashSet<Throwable>> tlSet = ThreadLocal.withInitial(ObjHashSet::new);

    @Override
    public LogRecord $(@Nullable Throwable e) {
        if (e == null) {
            return this;
        }

        final Utf8Sink sink = sink();
        final ObjHashSet<Throwable> dejaVu = tlSet.get();
        dejaVu.add(e);
        sink.putEOL();
        put0(sink, e);
        sink.putEOL();

        StackTraceElement[] trace = e.getStackTrace();
        for (int i = 0, n = trace.length; i < n; i++) {
            put(sink, trace[i]);
        }

        // Print suppressed exceptions, if any
        Throwable[] suppressed = e.getSuppressed();
        for (int i = 0, n = suppressed.length; i < n; i++) {
            put(sink, suppressed[i], trace, "Suppressed: ", "\t", dejaVu);
        }

        // Print cause, if any
        Throwable ourCause = e.getCause();
        if (ourCause != null) {
            put(sink, ourCause, trace, "Caused by: ", "", dejaVu);
        }

        return this;
    }

    private static void put(Utf8Sink sink, StackTraceElement e) {
        sink.putAscii("\tat ");
        sink.putAscii(e.getClassName());
        sink.putAscii('.');
        sink.putAscii(e.getMethodName());
        if (e.isNativeMethod()) {
            sink.putAscii("(Native Method)");
        } else {
            if (e.getFileName() != null && e.getLineNumber() > -1) {
                sink.putAscii('(').put(e.getFileName()).putAscii(':').put(e.getLineNumber()).putAscii(')');
            } else if (e.getFileName() != null) {
                sink.putAscii('(').put(e.getFileName()).putAscii(')');
            } else {
                sink.putAscii("(Unknown Source)");
            }
        }
        sink.put(Misc.EOL);
    }

    private static void put(
            Utf8Sink sink,
            Throwable throwable,
            StackTraceElement[] enclosingTrace,
            String caption,
            String prefix,
            Set<Throwable> dejaVu
    ) {
        if (dejaVu.contains(throwable)) {
            sink.putAscii("\t[CIRCULAR REFERENCE:");
            put0(sink, throwable);
            sink.putAscii(']');
        } else {
            dejaVu.add(throwable);

            // Compute number of frames in common between this and enclosing trace
            StackTraceElement[] trace = throwable.getStackTrace();
            int m = trace.length - 1;
            int n = enclosingTrace.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(enclosingTrace[n])) {
                m--;
                n--;
            }
            int framesInCommon = trace.length - 1 - m;

            sink.put(prefix).put(caption);
            put0(sink, throwable);
            sink.putEOL();

            for (int i = 0; i <= m; i++) {
                sink.put(prefix);
                put(sink, trace[i]);
            }
            if (framesInCommon != 0) {
                sink.put(prefix).putAscii("\t...").put(framesInCommon).putAscii(" more");
            }

            // Print suppressed exceptions, if any
            Throwable[] suppressed = throwable.getSuppressed();
            for (int i = 0, k = suppressed.length; i < k; i++) {
                put(sink, suppressed[i], trace, "Suppressed: ", prefix + '\t', dejaVu);
            }

            // Print cause, if any
            Throwable cause = throwable.getCause();
            if (cause != null) {
                put(sink, cause, trace, "Caused by: ", prefix, dejaVu);
            }
        }
    }

    private static void put0(Utf8Sink sink, Throwable e) {
        sink.putAscii(e.getClass().getName());
        if (e.getMessage() != null) {
            sink.putAscii(": ").put(e.getMessage());
        }
    }

    protected abstract Utf8Sink sink();
}
