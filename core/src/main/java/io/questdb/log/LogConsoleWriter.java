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

package io.questdb.log;

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.util.Arrays;

public class LogConsoleWriter extends SynchronizedJob implements Closeable, LogWriter {
    private static final boolean DEBUG_CLOSED_STDOUT = false;
    private static final int DEBUG_LOG_HISTORY_LENGTH = 50;
    private final boolean debugBrokenLogging;
    private final Utf8StringSink[] debugSinks;
    private final long fd = Files.getStdOutFdInternal();
    private final int level;
    private final RingQueue<LogRecordUtf8Sink> ring;
    private final SCSequence subSeq;
    private long debugSinkArrayPos = 0;
    private LogInterceptor interceptor;
    private final QueueConsumer<LogRecordUtf8Sink> myConsumer = this::toStdOut;

    public LogConsoleWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
        debugBrokenLogging = LogFactory.isInsideJUnitTest();
        if (debugBrokenLogging) {
            if (DEBUG_CLOSED_STDOUT) {
                System.out.println("#$#$ LOGGING WITH UTF-8 VALIDATION AND CLOSED-STDOUT DIAGNOSTICS");
                debugSinks = new Utf8StringSink[DEBUG_LOG_HISTORY_LENGTH];
                Arrays.setAll(debugSinks, i -> new Utf8StringSink(512));
            } else {
                System.out.println("#$#$ LOGGING WITH UTF-8 VALIDATION");
                debugSinks = null;
            }
        } else {
            debugSinks = null;
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

    private static void terminateJvm() {
        System.out.println("The JVM will now exit with status -1");
        System.exit(-1);
    }

    private void toStdOut(LogRecordUtf8Sink sink) {
        try {
            if ((sink.getLevel() & this.level) != 0) {
                if (interceptor != null) {
                    interceptor.onLog(sink);
                }
                if (!debugBrokenLogging) {
                    Files.append(fd, sink.ptr(), sink.size());
                } else {
                    toStdOutWithErrorDetection(sink);
                }
            }
        } catch (Throwable th) {
            System.out.println("Exception while writing a log line");
            th.printStackTrace(System.out);
        }
    }

    private void toStdOutWithErrorDetection(LogRecordUtf8Sink sink) {
        if (Utf8s.validateUtf8(sink) < 0) {
            System.out.println("#$#$ LOGGING ERROR: " + sink + "#$#$ END OF LOGGING ERROR REPORT");
            terminateJvm();
            return;
        }

        if (DEBUG_CLOSED_STDOUT) {
            Utf8StringSink debugSink = debugSinks[(int) (debugSinkArrayPos++ % debugSinks.length)];
            debugSink.clear();
            debugSink.put((DirectUtf8Sequence) sink); // disambiguation cast, guaranteed to succeed
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
        if (DEBUG_CLOSED_STDOUT) {
            System.out.println(errMsg + " Recent lines logged (up to " + DEBUG_LOG_HISTORY_LENGTH + "):");
            for (int i = 0; i < DEBUG_LOG_HISTORY_LENGTH; i++) {
                Utf8StringSink debugSink = debugSinks[(int) ((debugSinkArrayPos + i) % debugSinks.length)];
                if (debugSink.size() == 0) {
                    continue;
                }
                System.out.print(debugSink);
                debugSink.clear();
            }
        } else {
            System.out.println(errMsg + " To debug this issue, set LogConsoleWriter.DEBUG_CLOSED_STDOUT to true.");
        }
        System.out.println("#$#$ END OF LOGGING ERROR REPORT");
        terminateJvm();
    }

    @FunctionalInterface
    public interface LogInterceptor {
        void onLog(LogRecordUtf8Sink sink);
    }
}
