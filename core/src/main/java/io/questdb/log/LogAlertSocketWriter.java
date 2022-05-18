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

import io.questdb.BuildInformationHolder;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static io.questdb.log.TemplateParser.TemplateNode;

public class LogAlertSocketWriter extends SynchronizedJob implements Closeable, LogWriter {

    static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.json";
    static final CharSequenceObjHashMap<CharSequence> ALERT_PROPS = TemplateParser.adaptMap(System.getenv());
    private static final String DEFAULT_ENV_VALUE = "GLOBAL";
    private static final String ORG_ID_ENV = "ORGID";
    public static final String QDB_VERSION_ENV = "QDB_VERSION";
    private static final String NAMESPACE_ENV = "NAMESPACE";
    private static final String CLUSTER_ENV = "CLUSTER_NAME";
    private static final String INSTANCE_ENV = "INSTANCE_NAME";
    private static final String MESSAGE_ENV = "ALERT_MESSAGE";
    private static final String MESSAGE_ENV_VALUE = "${" + MESSAGE_ENV + "}";

    private final int level;
    private final MicrosecondClock clock;
    private final StringSink sink = new StringSink();
    private final FilesFacade ff;
    private final NetworkFacade nf;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSourceQueue;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final TemplateParser alertTemplate = new TemplateParser();
    private HttpLogRecordSink alertSink;
    private LogAlertSocket socket;
    private ObjList<TemplateNode> alertTemplateNodes;
    private int alertTemplateNodesLen;
    private Log log;
    // changed by introspection
    private String defaultAlertHost;
    private String defaultAlertPort;
    private String location;
    private String inBufferSize;
    private String outBufferSize;
    private String alertTargets;
    private String reconnectDelay;
    private final CharSequenceObjHashMap<CharSequence> properties;

    public LogAlertSocketWriter(RingQueue<LogRecordSink> alertsSrc, SCSequence writeSequence, int level) {
        this(
                FilesFacadeImpl.INSTANCE,
                NetworkFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                alertsSrc,
                writeSequence,
                level,
                ALERT_PROPS
        );
    }

    public LogAlertSocketWriter(
            FilesFacade ff,
            NetworkFacade nf,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> alertsSrc,
            SCSequence writeSequence,
            int level,
            CharSequenceObjHashMap<CharSequence> properties
    ) {
        this.ff = ff;
        this.nf = nf;
        this.clock = clock;
        this.alertsSourceQueue = alertsSrc;
        this.writeSequence = writeSequence;
        this.level = level & ~(1 << Numbers.msb(LogLevel.ADVISORY)); // switch off ADVISORY
        this.properties = properties;
    }

    @Override
    public void bindProperties(LogFactory factory) {
        int nInBufferSize = LogAlertSocket.IN_BUFFER_SIZE;
        if (inBufferSize != null) {
            try {
                nInBufferSize = Numbers.parseIntSize(inBufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for inBufferSize: " + inBufferSize);
            }
        }
        int nOutBufferSize = LogAlertSocket.OUT_BUFFER_SIZE;
        if (outBufferSize != null) {
            try {
                nOutBufferSize = Numbers.parseIntSize(outBufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for outBufferSize: " + outBufferSize);
            }
        }
        long nReconnectDelay = LogAlertSocket.RECONNECT_DELAY_NANO;
        if (reconnectDelay != null) {
            try {
                nReconnectDelay = Numbers.parseLong(reconnectDelay) * 1000000; // config is in milli
            } catch (NumericException e) {
                throw new LogError("Invalid value for reconnectDelay: " + reconnectDelay);
            }
        }
        if (defaultAlertHost == null) {
            defaultAlertHost = LogAlertSocket.DEFAULT_HOST;
        }
        int nDefaultPort = LogAlertSocket.DEFAULT_PORT;
        if (defaultAlertPort != null) {
            try {
                nDefaultPort = Numbers.parseInt(defaultAlertPort);
            } catch (NumericException e) {
                throw new LogError("Invalid value for defaultAlertPort: " + defaultAlertPort);
            }
        }
        log = factory.create(LogAlertSocketWriter.class.getName());
        socket = new LogAlertSocket(
                nf,
                alertTargets,
                nInBufferSize,
                nOutBufferSize,
                nReconnectDelay,
                defaultAlertHost,
                nDefaultPort,
                log
        );
        alertSink = new HttpLogRecordSink(socket)
                .putHeader(LogAlertSocket.localHostIp)
                .setMark();
        loadLogAlertTemplate();
        socket.connect();
    }

    @Override
    public void close() {
        Misc.free(socket);
    }

    @Override
    public boolean runSerially() {
        return writeSequence.consumeAll(alertsSourceQueue, alertsProcessor);
    }

    @TestOnly
    static void readFile(String location, long address, long addressSize, FilesFacade ff, CharSink sink) {
        long fdTemplate = -1;
        try (Path path = new Path()) {
            path.of(location);
            fdTemplate = ff.openRO(path.$());
            if (fdTemplate == -1) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d]",
                        location,
                        ff.errno()
                ));
            }
            long size = ff.length(fdTemplate);
            if (size > addressSize) {
                throw new LogError("Template file is too big");
            }
            if (size < 0 || size != ff.read(fdTemplate, address, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d, size=%d]",
                        location,
                        ff.errno(),
                        size
                ));
            }
            Chars.utf8Decode(address, address + size, sink);
        } finally {
            if (fdTemplate != -1) {
                ff.close(fdTemplate);
            }
        }
    }

    @TestOnly
    HttpLogRecordSink getAlertSink() {
        return alertSink;
    }

    @TestOnly
    String getAlertTargets() {
        return socket.getAlertTargets();
    }

    @TestOnly
    void setAlertTargets(String alertTargets) {
        this.alertTargets = alertTargets;
    }

    @TestOnly
    String getDefaultAlertHost() {
        return socket.getDefaultAlertHost();
    }

    @TestOnly
    void setDefaultAlertHost(String defaultAlertHost) {
        this.defaultAlertHost = defaultAlertHost;
    }

    @TestOnly
    int getDefaultAlertPort() {
        return socket.getDefaultAlertPort();
    }

    @TestOnly
    void setDefaultAlertPort(String defaultAlertPort) {
        this.defaultAlertPort = defaultAlertPort;
    }

    @TestOnly
    int getInBufferSize() {
        return socket.getInBufferSize();
    }

    @TestOnly
    void setInBufferSize(String inBufferSize) {
        this.inBufferSize = inBufferSize;
    }

    @TestOnly
    String getLocation() {
        return location;
    }

    @TestOnly
    void setLocation(String location) {
        this.location = location;
    }

    @TestOnly
    int getOutBufferSize() {
        return socket.getOutBufferSize();
    }

    @TestOnly
    void setOutBufferSize(String outBufferSize) {
        this.outBufferSize = outBufferSize;
    }

    @TestOnly
    long getReconnectDelay() {
        return socket.getReconnectDelay();
    }

    @TestOnly
    void setReconnectDelay(String reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    private void loadLogAlertTemplate() {
        final long now = clock.getTicks();
        if (location == null || location.isEmpty()) {
            location = DEFAULT_ALERT_TPT_FILE;
        }
        location = alertTemplate.parseEnv(location, now).toString(); // location may contain dollar expressions

        // read template, resolve env vars within (except $ALERT_MESSAGE)
        boolean needsReading = true;
        try (InputStream is = LogAlertSocketWriter.class.getResourceAsStream(location)) {
            if (is != null) {
                byte[] buff = new byte[LogAlertSocket.IN_BUFFER_SIZE];
                int len = is.read(buff, 0, buff.length);
                String template = new String(buff, 0, len, Files.UTF_8);
                alertTemplate.parse(template, now, properties);
                needsReading = false;
            }
        } catch (IOException e) {
            // it was not a resource ("/resource_name")
        }
        if (needsReading) {
            sink.clear();
            readFile(
                    location,
                    socket.getInBufferPtr(),
                    socket.getInBufferSize(),
                    ff,
                    sink
            );
            alertTemplate.parse(sink, now, properties);
        }
        if (alertTemplate.getKeyOffset(MESSAGE_ENV) < 0) {
            throw new LogError(String.format(
                    "Bad template, no %s declaration found %s",
                    MESSAGE_ENV_VALUE,
                    location));
        }
        alertTemplateNodes = alertTemplate.getTemplateNodes();
        alertTemplateNodesLen = alertTemplateNodes.size();
    }

    @TestOnly
    void onLogRecord(LogRecordSink logRecord) {
        final int len = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && len > 0) {
            alertTemplate.setDateValue(clock.getTicks());
            alertSink.rewindToMark();
            for (int i = 0; i < alertTemplateNodesLen; i++) {
                TemplateNode comp = alertTemplateNodes.getQuick(i);
                if (comp.isEnv(MESSAGE_ENV)) {
                    alertSink.put(logRecord);
                } else {
                    alertSink.put(comp);
                }
            }
            sink.clear();
            sink.put(logRecord);
            sink.clear(sink.length() - Misc.EOL.length());
            log.info().$("Sending: ").$(sink).$();
            socket.send(alertSink.$());
        }
    }

    static {
        if (!ALERT_PROPS.contains(ORG_ID_ENV)) {
            ALERT_PROPS.put(ORG_ID_ENV, DEFAULT_ENV_VALUE);
        }
        if (!ALERT_PROPS.contains(NAMESPACE_ENV)) {
            ALERT_PROPS.put(NAMESPACE_ENV, DEFAULT_ENV_VALUE);
        }
        if (!ALERT_PROPS.contains(CLUSTER_ENV)) {
            ALERT_PROPS.put(CLUSTER_ENV, DEFAULT_ENV_VALUE);
        }
        if (!ALERT_PROPS.contains(INSTANCE_ENV)) {
            ALERT_PROPS.put(INSTANCE_ENV, DEFAULT_ENV_VALUE);
        }
        if (!ALERT_PROPS.contains(QDB_VERSION_ENV)) {
            ALERT_PROPS.put(QDB_VERSION_ENV, BuildInformationHolder.INSTANCE.toString());
        }
        ALERT_PROPS.put(MESSAGE_ENV, MESSAGE_ENV_VALUE);
    }
}
