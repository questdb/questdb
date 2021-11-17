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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class LogAlertManagerWriter extends SynchronizedJob implements Closeable, LogWriter {

    private final int level;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSource;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final LocationParser locationParser = new LocationParser();
    private final StringSink sink = new StringSink();
    private final StringSink alertBuilder = new StringSink();
    private int alertBuilderStart;
    private int httpBodyMinLen;
    private int contentMarkerStart;
    private long sktBufferPtr;
    private int sktBufferSize;
    private long sktBufferLimit; // sktBufferPtr + sktBufferSize
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;
    private String host;
    private int port;

    // config attrs in log-file.conf (set by reflection):
    //   writers=alert
    //   w.alert.class=io.questdb.log.LogAlertManagerWriter
    //   w.alert.level=ERROR we alert on error, could alert on a new LogLevel.ALERT of value 8
    //   w.alert.location={path to the JSON template file}
    //   w.alert.bufferSize={defaults to 4Mb}
    //   w.alert.socketAddress={defaults to "localhost:9093", comma separated list of alertmanager nodes}
    private String location;
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

    private void onLogRecord(LogRecordSink logRecord) {
        final int l = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && l > 0 && fdSocketAddress > 0) { // advisory is always ignored
            alertBuilder.clear(alertBuilderStart);
            sink.clear();
            logRecord.toSink(sink);
            int messageLen = sink.length();
            alertBuilder.put('"');
            for (int i = 0, limit = sink.length(); i < limit; i++) {
                char c = sink.charAt(i);
                switch (c) {
                    case '\b':
                    case '\f':
                    case '\t':
                    case '"':
                    case '\\':
                        alertBuilder.put('\\');
                        messageLen++;
                        break;
                    case '\r':
                    case '\n':
                        messageLen--;
                        continue;
                }
                alertBuilder.put(c);
            }
            alertBuilder.put('"');
            alertBuilder.put(ALERT_FOOTER);
            alertBuilder.replace(
                    contentMarkerStart,
                    contentMarkerStart + CONTENT_LENGTH_MARKER.length(),
                    String.format(String.format("%%%dd", CONTENT_LENGTH_MARKER.length()), httpBodyMinLen + messageLen)
            );
            System.out.printf("ALERT: %s%n", alertBuilder);
            int len = alertBuilder.length();

            // send
            Chars.asciiStrCpy(alertBuilder, len, sktBufferPtr);
            int remaining = len;
            long p = sktBufferPtr;
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
            System.out.println(n);
            Net.dumpAscii(p, n);
        }
    }

    private void loadAlertTemplate() {
        alertBuilder.clear();
        alertBuilder.put("POST /api/v1/alerts HTTP/1.1\r\n")
                .put("Host: ").put(socketAddress).put("\r\n")
                .put("User-Agent: QuestDB/7.71.1\r\n")
                .put("Accept: */*\r\n")
                .put("Content-Type: application/json\r\n")
                .put("Content-Length: ");
        contentMarkerStart = alertBuilder.length();
        alertBuilder.put(CONTENT_LENGTH_MARKER)
                .put("\r\n\r\n")
                .put("[");
        int httpHeaderLen = alertBuilder.length() - 1;
        extendWithTemplate(loadAlertTemplate0());
        alertBuilder.clear(alertBuilder.length() - 2);
        alertBuilder.put(',').putQuoted("message").put(':');
        alertBuilderStart = alertBuilder.length();
        httpBodyMinLen = alertBuilderStart + ALERT_FOOTER.length() - httpHeaderLen + 2; // quotes around msg
    }

    private void extendWithTemplate(CharSequenceObjHashMap<Object> template) {
        alertBuilder.put('{');
        ObjList<CharSequence> keys = template.keys();
        for (int k = 0, limit = keys.size(); k < limit; k++) {
            CharSequence key = keys.getQuick(k);
            alertBuilder.putQuoted(key).put(':');
            Object value = template.get(key);
            if (value instanceof String) {
                alertBuilder.putQuoted(value.toString());
            } else {
                extendWithTemplate((CharSequenceObjHashMap<Object>) value);
            }
            if (k < limit - 1) {
                alertBuilder.put(',');
            }
        }
        alertBuilder.put('}');
    }

    private CharSequenceObjHashMap<Object> loadAlertTemplate0() {
        final Properties props = new Properties(DEFAULT_ALERT_PROPS);
        try {
            if (location == null || location.isEmpty()) {
                location = DEFAULT_ALERT_TPT_FILE;
                try (InputStream is = LogFactory.class.getResourceAsStream(location)) {
                    props.load(is);
                }
            } else {
                location = locationParser.parse(location, clock.getTicks()).toString();
                try (InputStream is = new FileInputStream(location)) {
                    props.load(is);
                }
            }
        } catch (NullPointerException | IOException e) {
            throw new LogError("Cannot read " + location, e);
        }

        CharSequenceObjHashMap<Object> template = new CharSequenceObjHashMap<>();
        for (Enumeration<String> keys = (Enumeration<String>) props.propertyNames(); keys.hasMoreElements(); ) {
            String key = keys.nextElement();
            if (key.equals(MESSAGE_KEY)) {
                continue;
            }
            String value = locationParser.parse(
                    props.getProperty(key),
                    0,
                    DEFAULT_SYSTEM_PROPS
            ).toString();
            String[] keyParts = key.split("[.]");
            switch (keyParts.length) {
                case 1:
                    template.put(key, value);
                    break;
                case 2:
                    CharSequenceObjHashMap<Object> peers = (CharSequenceObjHashMap<Object>) template.get(keyParts[0]);
                    if (peers == null) {
                        template.put(keyParts[0], peers = new CharSequenceObjHashMap<>());
                    }
                    peers.put(keyParts[1], value);
                    break;
                default:
                    throw new LogError("Bad key " + key + " only up to two depth levels are supported");
            }
        }
        // leave the map keys sorted in reverse alpha order to have Annotations.message
        // as last entry in the Annotations object, which
        template.sortKeys((csa, csb) -> -csa.toString().compareTo(csb.toString()));
        return template;
    }

    private static final String CONTENT_LENGTH_MARKER = "###############";
    private static final String ALERT_FOOTER = "}}]\r\n";
    private static final String MESSAGE_KEY = "Annotations.message";
    static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.conf";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9093;
    private static final int DEFAULT_SKT_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final String DEFAULT_PROP_VALUE = "GLOBAL";
    private static final Properties DEFAULT_ALERT_PROPS = new Properties();
    private static final Properties DEFAULT_SYSTEM_PROPS = new Properties(System.getProperties());

    static {
        // https://prometheus.io/docs/alerting/latest/notifications/#alert
        DEFAULT_ALERT_PROPS.put("Status", "firing");
        DEFAULT_ALERT_PROPS.put("Labels.severity", "critical");
        DEFAULT_ALERT_PROPS.put("Labels.service", "questdb");
        DEFAULT_ALERT_PROPS.put("Labels.orgid", "GLOBAL");
        DEFAULT_ALERT_PROPS.put("Labels.namespace", "GLOBAL");
        DEFAULT_ALERT_PROPS.put("Labels.instance", "GLOBAL");
        DEFAULT_ALERT_PROPS.put("Labels.cluster", "GLOBAL");
        DEFAULT_ALERT_PROPS.put("Labels.category", "application-logs");
        DEFAULT_ALERT_PROPS.put("Labels.alertname", "QuestDbInstanceLogs");
        DEFAULT_ALERT_PROPS.put("Annotations.description", "ERROR");
        DEFAULT_ALERT_PROPS.put(MESSAGE_KEY, ""); // it is in fact ignored

        DEFAULT_SYSTEM_PROPS.put("ORGID", DEFAULT_PROP_VALUE);
        DEFAULT_SYSTEM_PROPS.put("NAMESPACE", DEFAULT_PROP_VALUE);
        DEFAULT_SYSTEM_PROPS.put("CLUSTER_NAME", DEFAULT_PROP_VALUE);
        DEFAULT_SYSTEM_PROPS.put("INSTANCE_NAME", DEFAULT_PROP_VALUE);
    }
}
