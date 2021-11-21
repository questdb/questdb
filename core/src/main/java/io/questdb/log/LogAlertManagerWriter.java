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
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class LogAlertManagerWriter extends SynchronizedJob implements Closeable, LogWriter {

    static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.json";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9093;
    private static final int IN_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final int OUT_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final String DEFAULT_ENV_VALUE = "GLOBAL";
    private static final String ORG_ID_ENV = "ORGID";
    private static final String NAMESPACE_ENV = "NAMESPACE";
    private static final String CLUSTER_ENV = "CLUSTER_NAME";
    private static final String INSTANCE_ENV = "INSTANCE_NAME";
    private static final String MESSAGE_ENV = "ALERT_MESSAGE";


    private final int level;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSource;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final DollarExpr dollar$ = new DollarExpr();
    private final HttpAlertBuilder alertBuilder = new HttpAlertBuilder();
    private final CharSequenceObjHashMap<CharSequence> alertProps = DollarExpr.adaptMap(System.getenv());
    private String alertFooter;

    {
        if (!alertProps.contains(ORG_ID_ENV)) {
            alertProps.put(ORG_ID_ENV, DEFAULT_ENV_VALUE);
        }
        if (!alertProps.contains(NAMESPACE_ENV)) {
            alertProps.put(NAMESPACE_ENV, DEFAULT_ENV_VALUE);
        }
        if (!alertProps.contains(CLUSTER_ENV)) {
            alertProps.put(CLUSTER_ENV, DEFAULT_ENV_VALUE);
        }
        if (!alertProps.contains(INSTANCE_ENV)) {
            alertProps.put(INSTANCE_ENV, DEFAULT_ENV_VALUE);
        }
        alertProps.put(MESSAGE_ENV, "${" + MESSAGE_ENV + "}");
    }

    // socket
    private String localHostIp;
    private String host;
    private int port;
    private long outBufferPtr;
    private long outBufferLimit;
    private int outBufferSize;
    private long inBufferPtr;
    private long inBufferLimit;
    private final int inBufferSize = IN_BUFFER_SIZE;
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;

    // changed by introspection
    private String location = DEFAULT_ALERT_TPT_FILE;
    private String bufferSize;
    private String socketAddress;


    public LogAlertManagerWriter(RingQueue<LogRecordSink> alertsSrc, SCSequence writeSequence, int level) {
        this(
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                alertsSrc,
                writeSequence,
                level
        );
    }

    public LogAlertManagerWriter(
            FilesFacade ff,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> alertsSrc,
            SCSequence writeSequence,
            int level
    ) {
        this.ff = ff;
        this.clock = clock;
        this.alertsSource = alertsSrc;
        this.writeSequence = writeSequence;
        this.level = level & ~(1 << Numbers.msb(LogLevel.ADVISORY)); // switch off ADVISORY
    }

    @Override
    public void bindProperties() {
        if (bufferSize != null) {
            try {
                outBufferSize = Numbers.parseIntSize(bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            outBufferSize = OUT_BUFFER_SIZE;
        }
        inBufferPtr = Unsafe.malloc(inBufferSize, MemoryTag.NATIVE_DEFAULT);
        inBufferLimit = inBufferPtr + inBufferSize;
        outBufferPtr = Unsafe.malloc(outBufferSize, MemoryTag.NATIVE_DEFAULT);
        outBufferLimit = outBufferPtr + outBufferSize;

        parseSocketAddress();
        loadAlertTemplate();
        connectSocket();
    }

    @Override
    public void close() {
        if (outBufferPtr != 0) {
            Unsafe.free(outBufferPtr, outBufferSize, MemoryTag.NATIVE_DEFAULT);
            outBufferPtr = 0;
            outBufferLimit = 0;
        }
        if (inBufferPtr != 0) {
            Unsafe.free(inBufferPtr, IN_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            inBufferPtr = 0;
            inBufferLimit = 0;
        }
        if (fdSocket != -1) {
            freeSocket();
        }
    }

    @Override
    public boolean runSerially() {
        return writeSequence.consumeAll(alertsSource, alertsProcessor);
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    private void parseSocketAddress() {
        try {
            localHostIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new LogError("Cannot access our ip address info");
        }
        if (socketAddress != null) {
            // expected format: host[:port]
            if (Chars.isQuoted(socketAddress)) {
                socketAddress = socketAddress.subSequence(1, socketAddress.length() - 1).toString();
            }
            final int len = socketAddress.length();
            int portIdx = -1;
            for (int i = 0; i < len; ++i) {
                if (socketAddress.charAt(i) == ':') {
                    host = socketAddress.substring(0, i);
                    portIdx = i + 1;
                }
            }
            if (portIdx != -1) {
                try {
                    port = Numbers.parseInt(socketAddress, portIdx, len);
                } catch (NumericException e) {
                    throw new LogError("Invalid value for socketAddress, should be: host[:port]");
                }
            } else {
                host = socketAddress;
                port = DEFAULT_PORT;
            }
            try {
                host = InetAddress.getByName(host).getHostAddress();
            } catch (UnknownHostException e) {
                throw new LogError("Invalid host value for socketAddress: " + host);
            }
        } else {
            host = DEFAULT_HOST;
            port = DEFAULT_PORT;
            socketAddress = host + ":" + port;
        }
    }

    private void connectSocket() {
        fdSocketAddress = Net.sockaddr(host, port);
        fdSocket = Net.socketTcp(true);
        if (fdSocket > -1) {
            if (Net.connect(fdSocket, fdSocketAddress) != 0) {
                System.out.println(" E could not connect to");
                freeSocket();
            }
        } else {
            System.out.println(" E could not create TCP socket [errno=" + ff.errno() + "]");
            freeSocket();
        }
    }

    private void freeSocket() {
        Net.freeSockAddr(fdSocketAddress);
        fdSocketAddress = -1;
        Net.close(fdSocket);
        fdSocket = -1;
    }

    private void loadAlertTemplate() {
        final long now = clock.getTicks();
        location = dollar$.resolveEnv(location, now).toString();

        // resolve env vars within template, except ALERT_MESSAGE
        long size = -1;
        try (InputStream is = LogAlertManagerWriter.class.getResourceAsStream(location)) {
            if (is != null) {
                dollar$.resolve(new String(is.readAllBytes(), Files.UTF_8), now, alertProps);
            } else {
                size = readTemplateFile(location, inBufferPtr, inBufferLimit, ff);
            }
        } catch (IOException e) {
            size = readTemplateFile(location, inBufferPtr, inBufferLimit, ff);
        }
        if (size != -1) {
            DirectByteCharSequence template = new DirectByteCharSequence();
            template.of(inBufferPtr, inBufferPtr + size);
            dollar$.resolve(template, now, alertProps);
        }

        // consolidate/check/load template onto the outbound socket buffer
        dollar$.resolve(dollar$.toString(), now, alertProps);
        ObjList<Sinkable> components = dollar$.getLocationComponents();
        if (dollar$.getKeyOffset(MESSAGE_ENV) < 0 || components.size() < 3) {
            throw new LogError(String.format("Bad template %s", location));
        }
        alertBuilder
                .using(outBufferPtr, outBufferLimit, localHostIp)
                .put(components.getQuick(0))
                .setMark(); // mark the end of the first static block in buffer
        alertFooter = components.getQuick(2).toString();
    }

    private void onLogRecord(LogRecordSink logRecord) {
        final int logRecordLen = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && logRecordLen > 0 && fdSocket > 0) {
            alertBuilder
                    .rewindToMark()
                    .put(logRecord)
                    .put(alertFooter)
                    .$();

            // send
            int remaining = alertBuilder.length();
            long p = outBufferPtr;
            while (remaining > 0) {
                int n = Net.send(fdSocket, p, remaining);
                if (n > 0) {
                    remaining -= n;
                    p += n;
                } else {
                    System.out.println("could not send [n=" + n + " [errno=" + ff.errno() + "]");
                }
            }

            // receive ack
            p = inBufferPtr;
            int n = Net.recv(fdSocket, p, inBufferSize);
            Net.dumpAscii(p, n);
        }
    }

    private static long readTemplateFile(String location, long address, long limit, FilesFacade ff) {
        try (Path path = new Path()) {
            path.of(location);
            long fdTemplate = ff.openRO(path.$());
            if (fdTemplate == -1) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d]", location, ff.errno()));
            }
            long size = ff.length(fdTemplate);
            if (size > limit - address) {
                throw new LogError("Template file is too big");
            }
            // use the inbound socket buffer temporarily as the socket is not open yet
            if (size < 0 || size != ff.read(fdTemplate, address, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [size=%d, errno=%d]", location, size, ff.errno()));
            }
            return size;
        }
    }
}
