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

package io.questdb.log;

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class LogAlertManagerWriter extends SynchronizedJob implements Closeable, LogWriter {

    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
    private final TimestampFormatCompiler compiler = new TimestampFormatCompiler();
    private final RingQueue<LogRecordSink> ring;
    private final SCSequence subSeq;
    private final int level;
    private final Path path = new Path();
    private final FilesFacade ff;
    private final MicrosecondClock clock;
    private final ObjList<Sinkable> locationComponents = new ObjList<>();
    private long fd = -1;
    private long lim;
    private long buf;
    private int nBufferSize;
    // can be set via reflection
    private String location;
    private long fileTimestamp = 0;
    private String bufferSize;
    private long socketAddress = -1;
    private final QueueConsumer<LogRecordSink> myConsumer = this::copyToBuffer;

    public LogAlertManagerWriter(RingQueue<LogRecordSink> ring, SCSequence subSeq, int level) {
        this(FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, ring, subSeq, level);
    }

    public LogAlertManagerWriter(
            FilesFacade ff,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> ring,
            SCSequence subSeq,
            int level
    ) {
        this.ff = ff;
        this.clock = clock;
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public void bindProperties() {
        parseLocation();
        if (this.bufferSize != null) {
            try {
                nBufferSize = Numbers.parseIntSize(this.bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            nBufferSize = DEFAULT_BUFFER_SIZE;
        }

        this.buf = Unsafe.malloc(nBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.lim = buf + nBufferSize;
        this.fileTimestamp = clock.getTicks();
        connectSocket();
    }

    @Override
    public void close() {
        if (buf != 0) {
            Unsafe.free(buf, nBufferSize, MemoryTag.NATIVE_DEFAULT);
            buf = 0;
        }
        if (this.fd != -1) {
            ff.close(this.fd);
            this.fd = -1;
        }
        Misc.free(path);
    }

    @Override
    public boolean runSerially() {
        return subSeq.consumeAll(ring, myConsumer);
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    private void connectSocket() {

        // todo: get address from config
        socketAddress = Net.sockaddr("127.0.0.1", 9093);
        fd = Net.socketTcp(true);
        if (fd > -1) {
            if (Net.connect(fd, socketAddress) != 0) {

                System.out.println(" E could not connect to");

                Net.close(fd);
                fd = -1;

                freeSocketAddress();
            }
        } else {
            System.out.println(" E could not create TCP socket [errno=" + ff.errno() + "]");
            freeSocketAddress();
        }
    }

    private void copyToBuffer(LogRecordSink sink) {

        final int l = sink.length();
        if ((sink.getLevel() & this.level) != 0 && l > 0 && socketAddress != 0) {

            String req = "POST /api/v1/alerts HTTP/1.1\r\n" +
                    "Host: localhost:9093\r\n" +
                    "User-Agent: QuestDB/7.71.1\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: ";

            long p = buf;
            int len = req.length();
            Chars.asciiStrCpy(
                    req,
                    len,
                    p
            );
            p += len;

            String lenStr = "71";
            len = lenStr.length();

            Chars.asciiStrCpy(
                    lenStr,
                    len,
                    p
            );
            p += len;

            String lnlflnlf = "\r\n\r\n";
            len = lnlflnlf.length();
            Chars.asciiStrCpy(
                    lnlflnlf,
                    len,
                    p
            );
            p += len;

            String msg = "[{\"labels\":{\"alertname\":\"TestAlert1\", \"category\": \"application-logs\"}}]";
            len = msg.length();
            Chars.asciiStrCpy(
                    msg,
                    len,
                    p
            );
            p += len;

            String lnlf = "\r\n";
            len = lnlf.length();
            Chars.asciiStrCpy(
                    lnlf,
                    len,
                    p
            );

            p += len;

            int remaining = (int) (p - buf);
            p = buf;
            while (remaining > 0) {
                int n = Net.send(fd, p, remaining);
                if (n > 0) {
                    remaining -= n;
                    p += n;
                } else {
                    System.out.println("could not send [n="+n+", errno="+ff.errno());
                }
            }
            // now read

            p = buf;
            int n = Net.recv(fd, p, (int) lim);
            System.out.println(n);

            Net.dumpAscii(p, n);
        }
    }

    private void freeSocketAddress() {
        Net.freeSockAddr(socketAddress);
        socketAddress = 0;
    }

    private void parseLocation() {
        locationComponents.clear();
        // parse location into components
        int start = 0;
        boolean dollar = false;
        boolean open = false;
        if (location == null) {
            return;
        }
        for (int i = 0, n = location.length(); i < n; i++) {
            char c = location.charAt(i);
            switch (c) {
                case '$':
                    if (dollar) {
                        locationComponents.add(new SubStrSinkable(i, i + 1));
                        start = i;
                    } else {
                        locationComponents.add(new SubStrSinkable(start, i));
                        start = i;
                        dollar = true;
                    }
                    break;
                case '{':
                    if (dollar) {
                        if (open) {
                            throw new LogError("could not parse location");
                        }
                        open = true;
                        start = i + 1;
                    }
                    break;
                case '}':
                    if (dollar) {
                        if (open) {
                            open = false;
                            dollar = false;
                            if (Chars.startsWith(location, start, i - 1, "date:")) {
                                locationComponents.add(
                                        new DateSinkable(
                                                compiler.compile(location, start + 5, i, false)
                                        )
                                );
                            } else {
                                throw new LogError("unknown variable at " + start);
                            }
                            start = i + 1;
                        } else {
                            throw new LogError("could not parse location");
                        }
                    }
                default:
                    break;
            }
        }

        if (start < location.length()) {
            locationComponents.add(new SubStrSinkable(start, location.length()));
        }
    }

    private class SubStrSinkable implements Sinkable {
        private final int start;
        private final int end;

        public SubStrSinkable(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void toSink(CharSink sink) {
            sink.put(location, start, end);
        }
    }

    private class DateSinkable implements Sinkable {
        private final DateFormat format;

        public DateSinkable(DateFormat format) {
            this.format = format;
        }

        @Override
        public void toSink(CharSink sink) {
            format.format(
                    fileTimestamp,
                    TimestampFormatUtils.enLocale,
                    null,
                    sink
            );
        }
    }
}
