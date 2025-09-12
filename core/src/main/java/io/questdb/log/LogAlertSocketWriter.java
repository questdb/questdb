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
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.*;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.InputStream;

import static io.questdb.log.TemplateParser.TemplateNode;

public class LogAlertSocketWriter extends SynchronizedJob implements Closeable, LogWriter {

    public static final CharSequenceObjHashMap<CharSequence> ALERT_PROPS = TemplateParser.adaptMap(System.getenv());
    public static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.json";
    private static final String CLUSTER_ENV = "CLUSTER_NAME";
    private static final String DEFAULT_ENV_VALUE = "GLOBAL";
    private static final String INSTANCE_ENV = "INSTANCE_NAME";
    private static final String MESSAGE_ENV = "ALERT_MESSAGE";
    private static final String MESSAGE_ENV_VALUE = "${" + MESSAGE_ENV + "}";
    private static final String NAMESPACE_ENV = "NAMESPACE";
    private static final String ORG_ID_ENV = "ORGID";
    private final TemplateParser alertTemplate = new TemplateParser();
    private final RingQueue<LogRecordUtf8Sink> alertsSourceQueue;
    private final Clock clock;
    private final FilesFacade ff;
    private final int level;
    private final NetworkFacade nf;
    private final CharSequenceObjHashMap<CharSequence> properties;
    private final Utf8StringSink sink = new Utf8StringSink();
    private final SCSequence writeSequence;
    private HttpLogRecordUtf8Sink alertSink;
    private String alertTargets;
    private ObjList<TemplateNode> alertTemplateNodes;
    private int alertTemplateNodesLen;
    // changed by introspection
    private String defaultAlertHost;
    private String defaultAlertPort;
    private String inBufferSize;
    private String location;
    private Log log;
    private String outBufferSize;
    private String reconnectDelay;
    private LogAlertSocket socket;
    private final QueueConsumer<LogRecordUtf8Sink> alertsProcessor = this::onLogRecord;

    public LogAlertSocketWriter(RingQueue<LogRecordUtf8Sink> alertsSrc, SCSequence writeSequence, int level) {
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
            Clock clock,
            RingQueue<LogRecordUtf8Sink> alertsSrc,
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

    @TestOnly
    public static void readFile(String location, long address, long addressSize, FilesFacade ff, Utf8Sink sink) {
        long templateFd = -1;
        try (Path path = new Path()) {
            // Paths for logger are typically derived from resources.
            // They may start with `/C:` on Windows OS, which is Java way of emphasising absolute path.
            // We have to remove `/` in that path before calling native methods.
            if (Os.isWindows() && location.charAt(0) == '/') {
                path.of(location, 1, location.length());
            } else {
                path.of(location);
            }
            templateFd = ff.openRO(path.$());
            if (templateFd == -1) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d]",
                        location,
                        ff.errno()
                ));
            }
            long size = ff.length(templateFd);
            if (size > addressSize) {
                throw new LogError("Template file is too big");
            }
            if (size < 0 || size != ff.read(templateFd, address, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [errno=%d, size=%d]",
                        location,
                        ff.errno(),
                        size
                ));
            }
            Utf8s.strCpy(address, address + size, sink);
        } finally {
            ff.close(templateFd);
        }
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
        alertSink = new HttpLogRecordUtf8Sink(socket)
                .putHeader(LogAlertSocket.localHostIp)
                .setMark();
        loadLogAlertTemplate();
        socket.connect();
    }

    @Override
    public void close() {
        Misc.free(socket);
    }

    @TestOnly
    public HttpLogRecordUtf8Sink getAlertSink() {
        return alertSink;
    }

    @TestOnly
    public String getAlertTargets() {
        return socket.getAlertTargets();
    }

    @TestOnly
    public String getDefaultAlertHost() {
        return socket.getDefaultAlertHost();
    }

    @TestOnly
    public int getDefaultAlertPort() {
        return socket.getDefaultAlertPort();
    }

    @TestOnly
    public int getInBufferSize() {
        return socket.getInBufferSize();
    }

    @TestOnly
    public String getLocation() {
        return location;
    }

    @TestOnly
    public int getOutBufferSize() {
        return socket.getOutBufferSize();
    }

    @TestOnly
    public long getReconnectDelay() {
        return socket.getReconnectDelay();
    }

    @TestOnly
    public void onLogRecord(LogRecordUtf8Sink logRecord) {
        final int len = logRecord.size();
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
            sink.put((Utf8Sequence) logRecord);
            sink.clear(sink.size() - Misc.EOL.length());
            log.info().$("Sending: ").$(sink).$();
            socket.send(alertSink.$());
        }
    }

    @Override
    public boolean runSerially() {
        return writeSequence.consumeAll(alertsSourceQueue, alertsProcessor);
    }

    @TestOnly
    public void setAlertTargets(String alertTargets) {
        this.alertTargets = alertTargets;
    }

    @TestOnly
    public void setDefaultAlertHost(String defaultAlertHost) {
        this.defaultAlertHost = defaultAlertHost;
    }

    @TestOnly
    public void setDefaultAlertPort(String defaultAlertPort) {
        this.defaultAlertPort = defaultAlertPort;
    }

    @TestOnly
    public void setInBufferSize(String inBufferSize) {
        this.inBufferSize = inBufferSize;
    }

    @TestOnly
    public void setLocation(String location) {
        this.location = location;
    }

    @TestOnly
    public void setOutBufferSize(String outBufferSize) {
        this.outBufferSize = outBufferSize;
    }

    @TestOnly
    public void setReconnectDelay(String reconnectDelay) {
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
        } catch (LogError e) {
            throw e;
        } catch (Throwable e) {
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
            // originalTxt needs to be a static text and not a mutable sink because it's referred to in template nodes 
            alertTemplate.parse(Utf8s.toString(sink), now, properties);
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
        ALERT_PROPS.put(MESSAGE_ENV, MESSAGE_ENV_VALUE);
    }
}
