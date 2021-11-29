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

import io.questdb.VisibleForTesting;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.*;

public class LogAlertSocketWriter extends SynchronizedJob implements Closeable, LogWriter {

    static final String DEFAULT_ALERT_TPT_FILE = "/alert-manager-tpt.json";

    private static final String DEFAULT_ENV_VALUE = "GLOBAL";
    private static final String ORG_ID_ENV = "ORGID";
    private static final String NAMESPACE_ENV = "NAMESPACE";
    private static final String CLUSTER_ENV = "CLUSTER_NAME";
    private static final String INSTANCE_ENV = "INSTANCE_NAME";
    private static final String MESSAGE_ENV = "ALERT_MESSAGE";
    private static final String MESSAGE_ENV_VALUE = "${" + MESSAGE_ENV + "}";
    private static final CharSequenceObjHashMap<CharSequence> ALERT_PROPS = TemplateParser.adaptMap(System.getenv());

    {
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

    private final int level;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSourceQueue;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final TemplateParser alertTemplate = new TemplateParser();

    private HttpLogRecordSink alertBuilder;
    private LogAlertSocket socket;
    // changed by introspection
    private String location;
    private String bufferSize;
    private String alertTargets;
    private String reconnectDelay;


    public LogAlertSocketWriter(RingQueue<LogRecordSink> alertsSrc, SCSequence writeSequence, int level) {
        this(
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                alertsSrc,
                writeSequence,
                level
        );
    }

    public LogAlertSocketWriter(
            FilesFacade ff,
            MicrosecondClock clock,
            RingQueue<LogRecordSink> alertsSrc,
            SCSequence writeSequence,
            int level
    ) {
        this.ff = ff;
        this.clock = clock;
        this.alertsSourceQueue = alertsSrc;
        this.writeSequence = writeSequence;
        this.level = level & ~(1 << Numbers.msb(LogLevel.ADVISORY)); // switch off ADVISORY
    }

    @Override
    public void bindProperties() {
        int nBufferSize = LogAlertSocket.OUT_BUFFER_SIZE;
        if (bufferSize != null) {
            try {
                nBufferSize = Numbers.parseIntSize(bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize: " + bufferSize);
            }
        }
        long nReconnectDelay = LogAlertSocket.RECONNECT_DELAY_NANO;
        if (reconnectDelay != null) {
            try {
                nReconnectDelay = Numbers.parseLong(reconnectDelay);
            } catch (NumericException e) {
                throw new LogError("Invalid value for reconnectDelay: " + reconnectDelay);
            }
        }
        socket = new LogAlertSocket(alertTargets, nBufferSize, nReconnectDelay);
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

    @VisibleForTesting
    void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    @VisibleForTesting
    int getBufferSize() {
        return socket.getOutBufferSize();
    }

    @VisibleForTesting
    void setLocation(String location) {
        this.location = location;
    }

    @VisibleForTesting
    String getLocation() {
        return location;
    }

    @VisibleForTesting
    void setAlertTargets(String alertTargets) {
        this.alertTargets = alertTargets;
    }

    @VisibleForTesting
    String getAlertTargets() {
        return socket.getAlertTargets();
    }

    @VisibleForTesting
    HttpLogRecordSink getAlertBuilder() {
        return alertBuilder;
    }

    @VisibleForTesting
    long getReconnectDelay() {
        return socket.getReconnectDelay();
    }

    @VisibleForTesting
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
        boolean wasRead = false;
        try (InputStream is = LogAlertSocketWriter.class.getResourceAsStream(location)) {
            if (is != null) {
                alertTemplate.parse(CharSequenceView.of(is), now, ALERT_PROPS);
                wasRead = true;
            }
        } catch (IOException e) {
            // it was not a resource ("/resource_name")
        }
        if (!wasRead) {
            alertTemplate.parse(
                    readFile(
                            location,
                            socket.getInBufferPtr(),
                            socket.getInBufferSize(),
                            ff
                    ),
                    now,
                    ALERT_PROPS
            );
        }
        // consolidate/check/load template to the outbound socket buffer
        alertTemplate.parse(alertTemplate.toString(), now, ALERT_PROPS);
        ObjList<Sinkable> components = alertTemplate.getLocationComponents();
        if (alertTemplate.getKeyOffset(MESSAGE_ENV) < 0 || components.size() < 3) {
            throw new LogError(String.format(
                    "Bad template, no %s declaration found %s",
                    MESSAGE_ENV_VALUE,
                    location));
        }
        alertBuilder = new HttpLogRecordSink(socket)
                .putHeader(LogAlertSocket.localHostIp)
                .put(components.getQuick(0)) // // this is the first static block
                .setMark() // mark in buffer, message is appended here onLogRecord
                .setFooter(components.getQuick(2)); // this is the final static block
    }

    @VisibleForTesting
    void onLogRecord(LogRecordSink logRecord) {
        final int len = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && len > 0) {
            socket.send(alertBuilder.rewindToMark().put(logRecord).$());
        }
    }

    @VisibleForTesting
    static DirectByteCharSequence readFile(String location, long address, long addressSize, FilesFacade ff) {
        try (Path path = new Path()) {
            path.of(location);
            long fdTemplate = ff.openRO(path.$());
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
            DirectByteCharSequence template = new DirectByteCharSequence();
            template.of(address, address + size);
            return template;
        }
    }

    private static class CharSequenceView implements CharSequence {
        private static final byte[] BUFF = new byte[LogAlertSocket.IN_BUFFER_SIZE];

        static CharSequenceView of(InputStream is) throws IOException {
            return new CharSequenceView(is);
        }

        private final int lo;
        private final int len;

        private CharSequenceView(InputStream is) throws IOException {
            this(0, is.read(BUFF, 0, BUFF.length));
        }

        private CharSequenceView(int lo, int len) {
            this.lo = lo;
            this.len = len;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            if (index > -1 && index < len) {
                return (char) BUFF[lo + index];
            }
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }

        @NotNull
        @Override
        public CharSequenceView subSequence(int start, int end) {
            return new CharSequenceView(lo + start, end - start);
        }
    }
}
