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

import java.io.Closeable;
import java.util.Arrays;

public class LogConsoleWriter extends SynchronizedJob implements Closeable, LogWriter {
    private final Utf8StringSink[] debugSinks = new Utf8StringSink[100];
    private final long fd = Files.getStdOutFdInternal();
    private final int level;
    private final RingQueue<LogRecordUtf8Sink> ring;
    private final SCSequence subSeq;
    private LogInterceptor interceptor;
    private long sinkArrayPos = 0;
    private final QueueConsumer<LogRecordUtf8Sink> myConsumer = this::toStdOut;

    public LogConsoleWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
        Arrays.setAll(debugSinks, i -> new Utf8StringSink(512));
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

    private void safeDebugPrint() {
        for (long l = sinkArrayPos; l < sinkArrayPos + debugSinks.length; l++) {
            Utf8StringSink sink = debugSinks[(int) (l % debugSinks.length)];
            System.err.println("System.err:");
            String content = sink.toString();
            System.err.println(content);
            System.err.println("System.out:");
            System.out.println(content);
//            for (int n = sink.size(), i = 0; i < n; i++) {
//                byte b = sink.byteAt(i);
//                if (b < 127 && (b > 31 || b == '\r' || b == '\n' || b == '\t')) {
//                    System.err.print((char) b);
//                } else {
//                    System.err.format("\\x%02x", b & 0xFF);
//                }
//            }
        }
    }

    private void toStdOut(LogRecordUtf8Sink sink) {
        try {
            if ((sink.getLevel() & this.level) != 0) {
                if (interceptor != null) {
                    interceptor.onLog(sink);
                }
                Utf8StringSink debugSink = debugSinks[(int) (sinkArrayPos++ % debugSinks.length)];
                debugSink.clear();
                debugSink.put((DirectUtf8Sequence) sink);
                long res = Files.append(fd, sink.ptr(), sink.size());
                if (res != sink.size()) {
                    System.err.println("sink.size() " + sink.size() + ", res " + res + ", errno " +
                            Os.errno() + ", fd " + Files.toOsFd(fd) + ". Text being logged:");
                    safeDebugPrint();
                    Os.sleep(1000);
                    System.exit(-1);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            Os.sleep(1000);
            System.exit(-2);
        }
    }

//    private void toStdOut(LogRecordUtf8Sink sink) {
//        if ((sink.getLevel() & this.level) != 0) {
//            if (interceptor != null) {
//                interceptor.onLog(sink);
//            }
//            Files.append(fd, sink.ptr(), sink.size());
//        }
//    }

    @FunctionalInterface
    public interface LogInterceptor {
        void onLog(LogRecordUtf8Sink sink);
    }
}
