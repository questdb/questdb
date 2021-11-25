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


    private final int level;
    private final MicrosecondClock clock;
    private final FilesFacade ff;
    private final SCSequence writeSequence;
    private final RingQueue<LogRecordSink> alertsSource;
    private final QueueConsumer<LogRecordSink> alertsProcessor = this::onLogRecord;
    private final DollarExpr dollar$ = new DollarExpr();
    private final CharSequenceObjHashMap<CharSequence> alertProps = DollarExpr.adaptMap(System.getenv());
    private HttpLogAlertBuilder alertBuilder;
    private LogAlertSocket socket;
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
        alertProps.put(MESSAGE_ENV, MESSAGE_ENV_VALUE);
    }

    // changed by introspection
    private String location = DEFAULT_ALERT_TPT_FILE;
    private String bufferSize;
    private String socketAddress;


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
        this.alertsSource = alertsSrc;
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
                throw new LogError("Invalid value for bufferSize");
            }
        }
        socket = new LogAlertSocket(ff, socketAddress, nBufferSize);
        loadLogAlertTemplate();
        socket.connect();
    }

    @Override
    public void close() {
        if (socket != null) {
            socket.close();
        }
    }

    @Override
    public boolean runSerially() {
        return writeSequence.consumeAll(alertsSource, alertsProcessor);
    }

    @VisibleForTesting
    void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
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
    void setSocketAddress(String socketAddress) {
        this.socketAddress = socketAddress;
    }

    @VisibleForTesting
    String getSocketAddress() {
        return socket.getSocketAddress();
    }

    @VisibleForTesting
    HttpLogAlertBuilder getAlertBuilder() {
        return alertBuilder;
    }

    @VisibleForTesting
    int getBufferSize() {
        return socket.getOutBufferSize();
    }

    private void loadLogAlertTemplate() {
        final long now = clock.getTicks();
        if (location.isEmpty()) {
            location = DEFAULT_ALERT_TPT_FILE;
        }
        location = dollar$.resolveEnv(location, now).toString();

        // read template, resolve env vars within (except $ALERT_MESSAGE)
        boolean wasRead = false;
        try (InputStream is = LogAlertSocketWriter.class.getResourceAsStream(location)) {
            if (is != null) {
                byte[] buff = new byte[LogAlertSocket.IN_BUFFER_SIZE];
                int n = is.read(buff, 0, buff.length);
                if (n > 0) {
                    dollar$.resolve(new String(buff, 0, n, Files.UTF_8), now, alertProps);
                    wasRead = true;
                }
            }
        } catch (IOException e) {
            // it was not a resource ("/resource_name")
        }
        if (!wasRead) {
            dollar$.resolve(
                    readFile(
                            location,
                            socket.getInBufferPtr(),
                            socket.getInBufferLimit(),
                            ff
                    ),
                    now,
                    alertProps
            );
        }

        // consolidate/check/load template to the outbound socket buffer
        dollar$.resolve(dollar$.toString(), now, alertProps);
        ObjList<Sinkable> components = dollar$.getLocationComponents();
        if (dollar$.getKeyOffset(MESSAGE_ENV) < 0 || components.size() < 3) {
            throw new LogError(String.format(
                    "Bad template, no ${%s} declaration found %s",
                    MESSAGE_ENV,
                    location));
        }
        alertBuilder = new HttpLogAlertBuilder(socket.getOutBufferPtr(), socket.getOutBufferSize())
                .putHeader(LogAlertSocket.localHostIp)
                .put(components.getQuick(0))
                .setMark(); // mark the end of the first static block in buffer
        alertFooter = components.getQuick(2).toString();
    }

    @VisibleForTesting
    void onLogRecord(LogRecordSink logRecord) {
        final int len = logRecord.length();
        if ((logRecord.getLevel() & level) != 0 && len > 0) {
            alertBuilder
                    .rewindToMark()
                    .put(logRecord)
                    .put(alertFooter)
                    .$();
            socket.send(alertBuilder.length());
        }
    }

    @VisibleForTesting
    static DirectByteCharSequence readFile(String location, long address, long limit, FilesFacade ff) {
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
            if (size < 0 || size != ff.read(fdTemplate, address, size, 0)) {
                throw new LogError(String.format(
                        "Cannot read %s [size=%d, errno=%d]", location, size, ff.errno()));
            }
            DirectByteCharSequence template = new DirectByteCharSequence();
            template.of(address, address + size);
            return template;
        }
    }
}
