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

package io.questdb.log;

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;
import java.util.Arrays;

import static io.questdb.ParanoiaState.*;

public class LogConsoleWriter extends SynchronizedJob implements Closeable, LogWriter {
    private static final int DEBUG_LOG_HISTORY_LENGTH = 25;
    private final long fd = Files.getStdOutFdInternal();
    private final int level;
    private final LogHistory logHistory;
    private final RingQueue<LogRecordUtf8Sink> ring;
    private final SCSequence subSeq;
    private LogInterceptor interceptor;
    private final QueueConsumer<LogRecordUtf8Sink> myConsumer = this::toStdOut;

    public LogConsoleWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
        if (LOG_PARANOIA_MODE == LOG_PARANOIA_MODE_BASIC) {
            System.out.println("BASIC LOG PARANOIA MODE ACTIVE");
            logHistory = null;
        } else if (LOG_PARANOIA_MODE == LOG_PARANOIA_MODE_AGGRESSIVE) {
            System.out.println("AGGRESSIVE LOG PARANOIA MODE ACTIVE");
            logHistory = new LogHistory();
        } else {
            logHistory = null;
        }
    }

    @Override
    public void bindProperties(LogFactory factory) {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean runSerially() {
        return subSeq.consumeAll(ring, myConsumer);
    }

    public void setInterceptor(LogInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    private void toStdOut(LogRecordUtf8Sink sink) {
        try {
            if ((sink.getLevel() & this.level) != 0) {
                if (interceptor != null) {
                    interceptor.onLog(sink);
                }
                if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
                    toStdOutWithErrorDetection(sink);
                } else {
                    Files.append(fd, sink.ptr(), sink.size());
                }
            }
        } catch (Throwable th) {
            System.out.println("Exception while writing a log line");
            th.printStackTrace(System.out);
        }
    }

    private void toStdOutWithErrorDetection(LogRecordUtf8Sink sink) {
        if (logHistory != null) {
            logHistory.addLine(sink);
        }

        int sinkSize = sink.size();
        long res = Files.append(fd, sink.ptr(), sinkSize);
        if (res == sinkSize) {
            return;
        }

        // The most common reason to reach this point is that the stdout file descriptor is closed.
        // When running through Maven Surefire in a forked process, System.out still works because
        // it uses a different write path, so this will still appear in the output.
        String errMsg = "#$#$ LOGGING ERROR: Files.append() returned " + res + ", expected result was " + sinkSize +
                ". Os.errno() " + Os.errno() + ", fd " + Files.toOsFd(fd) + '.';
        if (logHistory != null) {
            System.out.println(errMsg + " Recent lines logged (up to " + DEBUG_LOG_HISTORY_LENGTH + "):");
            logHistory.print();
        } else {
            System.out.println(errMsg + " To debug this issue, set LogConsoleWriter.DEBUG_CLOSED_STDOUT to true.");
        }
        System.out.println("#$#$ END OF LOGGING ERROR REPORT");
    }

    @FunctionalInterface
    public interface LogInterceptor {
        void onLog(LogRecordUtf8Sink sink);
    }

    private static class LogHistory {
        private final Utf8StringSink[] logLineArray;
        private long arrayPos = 0;

        LogHistory() {
            logLineArray = new Utf8StringSink[DEBUG_LOG_HISTORY_LENGTH];
            Arrays.setAll(logLineArray, i -> new Utf8StringSink(512));
        }

        void addLine(LogRecordUtf8Sink sink) {
            Utf8StringSink debugSink = logLineArray[(int) (arrayPos++ % logLineArray.length)];
            debugSink.clear();
            debugSink.put((DirectUtf8Sequence) sink); // disambiguation cast, guaranteed to succeed
        }

        void print() {
            for (int i = 0; i < logLineArray.length; i++) {
                Utf8StringSink sink = logLineArray[(int) ((arrayPos + i) % logLineArray.length)];
                if (sink.size() == 0) {
                    continue;
                }
                System.out.print(sink);
                sink.clear();
            }
        }
    }
}
