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
import io.questdb.std.str.StringSink;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class LogAlertManagerWriter extends SynchronizedJob implements Closeable, LogWriter {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9093;
    private static final int DEFAULT_SKT_BUFFER_SIZE = 4 * 1024 * 1024;
    static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.json";
    private static final String HEADER_BODY_SEPARATOR = "\r\n\r\n";
    private static final String ORG_ID_PROP = "ORGID";
    private static final String NAMESPACE_PROP = "NAMESPACE";
    private static final String CLUSTER_PROP = "CLUSTER_NAME";
    private static final String INSTANCE_PROP = "INSTANCE_NAME";
    private static final String DEFAULT_PROP_VALUE = "GLOBAL";
    private static final String MESSAGE_PROP = "ALERT_MESSAGE";
    private static final int DOLLAR_MESSAGE_LEN = MESSAGE_PROP.length() + 3; // ${ALERT_MESSAGE}

    private final CharSequenceObjHashMap<CharSequence> alertProps = DollarExprResolver.adaptProperties(System.getProperties());

    {
        if (!alertProps.contains(ORG_ID_PROP)) {
            alertProps.put(ORG_ID_PROP, DEFAULT_PROP_VALUE);
        }
        if (!alertProps.contains(NAMESPACE_PROP)) {
            alertProps.put(NAMESPACE_PROP, DEFAULT_PROP_VALUE);
        }
        if (!alertProps.contains(CLUSTER_PROP)) {
            alertProps.put(CLUSTER_PROP, DEFAULT_PROP_VALUE);
        }
        if (!alertProps.contains(INSTANCE_PROP)) {
            alertProps.put(INSTANCE_PROP, DEFAULT_PROP_VALUE);
        }
        alertProps.put(MESSAGE_PROP, "${" + MESSAGE_PROP + "}");
    }

    private final int level;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSource;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final DollarExprResolver dollar$ = new DollarExprResolver();
    private final StringSink alertBuilder = new StringSink();
    private final StringSink messageSink = new StringSink();
    private CharSequence alertTemplate;
    private int alertTemplateLen;
    private CharSequence httpHeader;
    private int httpHeaderLen;
    private long sktBufferPtr;
    private int sktBufferSize;
    private long sktBufferLimit; // sktBufferPtr + sktBufferSize
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;
    private String host;
    private int port;

    // changed by introspection
    private String location = DEFAULT_ALERT_TPT_FILE;
    private String bufferSize;
    private String socketAddress;


    public LogAlertManagerWriter(
            RingQueue<LogRecordSink> alertsSource,
            SCSequence writeSequence,
            int level
    ) {
        this(
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                alertsSource,
                writeSequence,
                level
        );
    }

    public LogAlertManagerWriter(
            FilesFacade ff,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> alertsSource,
            SCSequence writeSequence,
            int level
    ) {
        this.ff = ff;
        this.clock = clock;
        this.alertsSource = alertsSource;
        this.writeSequence = writeSequence;
        this.level = level & ~(1 << Numbers.msb(LogLevel.ADVISORY)); // switch off ADVISORY
    }

    @Override
    public void bindProperties() {
        parseSocketAddress();
        loadAlertTemplate();
        if (bufferSize != null) {
            try {
                sktBufferSize = Numbers.parseIntSize(bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            sktBufferSize = DEFAULT_SKT_BUFFER_SIZE;
        }
        sktBufferPtr = Unsafe.malloc(sktBufferSize, MemoryTag.NATIVE_DEFAULT);
        sktBufferLimit = sktBufferPtr + sktBufferSize;
        connectSocket();
    }

    @Override
    public void close() {
        if (sktBufferPtr != 0) {
            Unsafe.free(sktBufferPtr, sktBufferSize, MemoryTag.NATIVE_DEFAULT);
            sktBufferPtr = 0;
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
        if (socketAddress != null) {
            // expected format: host[:port]
            if (Chars.isQuoted(socketAddress)) {
                socketAddress = socketAddress.subSequence(1, socketAddress.length() - 1).toString();
            }
            final int len = socketAddress.length();
            int portIdx = -1;
            for (int i = 0, limit = len; i < limit; ++i) {
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
        long ticks = clock.getTicks();
        location = dollar$.resolve(location, ticks).toString();
        alertBuilder.clear();
        alertBuilder.put("POST /api/v1/alerts HTTP/1.1\r\n")
                .put("Host: ").put(socketAddress).put("\r\n")
                .put("User-Agent: QuestDB/7.71.1\r\n")
                .put("Accept: */*\r\n")
                .put("Content-Type: application/json\r\n")
                .put("Content-Length: ");
        httpHeader = alertBuilder.toString();
        httpHeaderLen = alertBuilder.length();
        alertBuilder.clear();
        try (InputStream is = new FileInputStream(location)) {
            alertBuilder.put(new String(is.readAllBytes(), StandardCharsets.UTF_8));
        } catch (NullPointerException | IOException e) {
            throw new LogError("Cannot read " + location, e);
        }
        alertTemplate = dollar$.resolve(alertBuilder, ticks, alertProps).toString();
        alertTemplateLen = alertTemplate.length();
    }

    private void onLogRecord(LogRecordSink logRecord) {
        final int logRecordLen = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && logRecordLen > 0 && fdSocket > 0) {
            messageSink.clear();
            final long address = logRecord.getAddress();
            for (long p = address, limit = address + logRecordLen; p < limit; p++) {
                char c = (char) Unsafe.getUnsafe().getByte(p);
                switch (c) {
                    case '\b':
                    case '\f':
                    case '\t':
                    case '$':
                    case '"':
                    case '\\':
                        messageSink.put('\\');
                        break;
                    case '\r':
                    case '\n':
                        continue;
                }
                messageSink.put(c);
            }

            alertBuilder.clear();
            alertBuilder.put(alertTemplate);
            alertProps.put(MESSAGE_PROP, messageSink);
            dollar$.resolve(alertBuilder, 0, alertProps);

            // move alert message to socket buffer
            long p = sktBufferPtr;
            Chars.asciiStrCpy(httpHeader, httpHeaderLen, p);
            p += httpHeaderLen;
            int contentLen = alertTemplateLen + messageSink.length() - DOLLAR_MESSAGE_LEN;
            String contentLenStr = String.valueOf(contentLen);
            int len = contentLenStr.length();
            Chars.asciiStrCpy(contentLenStr, len, p);
            p += len;
            len = HEADER_BODY_SEPARATOR.length();
            Chars.asciiStrCpy(HEADER_BODY_SEPARATOR, len, p);
            p += len;
            String body = dollar$.toString();
            len = body.length();
            Chars.asciiStrCpy(body, len, p);

            // send
            int remaining = (int) (p + len - sktBufferPtr);
            p = sktBufferPtr;
            while (remaining > 0) {
                int n = Net.send(fdSocket, p, remaining);
                if (n > 0) {
                    remaining -= n;
                    p += n;
                } else {
                    System.out.println("could not send [n=" + n + " [errno=" + ff.errno() + "]");
                }
            }

            // read
            p = sktBufferPtr;
            int n = Net.recv(fdSocket, p, (int) sktBufferLimit);
            Net.dumpAscii(p, n);
        }
    }
}
