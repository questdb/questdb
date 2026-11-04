/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


package io.questdb.metrics;

import io.questdb.network.ConnectionSteering;
import io.questdb.std.ObjList;
import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

/**
 * The connection steering of each port whose server splits its listener by worker group: one series
 * per state with 1 for the current state, the program attaches, and each group's open connections,
 * all and those the runtime guard counts.
 */
public final class ConnectionSteeringMetrics implements Target {
    private static final String ATTACHES_NAME = "connection_steering_attaches";
    private static final String GUARD_CONNECTIONS_NAME = "connection_steering_guard_connections";
    private static final String LABEL_GROUP = "group";
    private static final String LABEL_SERVER = "server";
    private static final String LABEL_STATE = "state";
    private static final String OPEN_CONNECTIONS_NAME = "connection_steering_open_connections";
    private static final String STATE_NAME = "connection_steering_state";
    private final ObjList<ConnectionSteering> ports = new ObjList<>();

    public synchronized void register(ConnectionSteering steering) {
        if (ports.indexOf(steering) < 0) {
            ports.add(steering);
        }
    }

    @Override
    public synchronized void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        if (ports.size() == 0) {
            return;
        }
        appendType(sink, STATE_NAME, "gauge");
        for (int i = 0, n = ports.size(); i < n; i++) {
            final ConnectionSteering steering = ports.getQuick(i);
            final int reason = steering.getReason();
            for (int r = 0; r < ConnectionSteering.REASON_COUNT; r++) {
                appendNamePrefix(sink, STATE_NAME);
                sink.putAscii('{');
                PrometheusFormatUtils.appendLabel(sink, LABEL_SERVER, steering.getName());
                sink.putAscii(',');
                PrometheusFormatUtils.appendLabel(sink, LABEL_STATE, ConnectionSteering.reasonName(r));
                sink.putAscii('}');
                PrometheusFormatUtils.appendSampleLineSuffix(sink, r == reason ? 1 : 0);
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);

        PrometheusFormatUtils.appendCounterType(ATTACHES_NAME, sink);
        for (int i = 0, n = ports.size(); i < n; i++) {
            final ConnectionSteering steering = ports.getQuick(i);
            PrometheusFormatUtils.appendCounterNamePrefix(ATTACHES_NAME, sink);
            sink.putAscii('{');
            PrometheusFormatUtils.appendLabel(sink, LABEL_SERVER, steering.getName());
            sink.putAscii('}');
            PrometheusFormatUtils.appendSampleLineSuffix(sink, steering.getAttachCount());
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendType(sink, OPEN_CONNECTIONS_NAME, "gauge");
        for (int i = 0, n = ports.size(); i < n; i++) {
            final ConnectionSteering steering = ports.getQuick(i);
            for (int g = 0, m = steering.getGroupCount(); g < m; g++) {
                appendGroupSample(sink, OPEN_CONNECTIONS_NAME, steering, g, steering.getOpenCount(g));
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);

        appendType(sink, GUARD_CONNECTIONS_NAME, "gauge");
        for (int i = 0, n = ports.size(); i < n; i++) {
            final ConnectionSteering steering = ports.getQuick(i);
            for (int g = 0, m = steering.getGroupCount(); g < m; g++) {
                appendGroupSample(sink, GUARD_CONNECTIONS_NAME, steering, g, steering.getGuardCount(g));
            }
        }
        PrometheusFormatUtils.appendNewLine(sink);
    }

    public synchronized void unregister(ConnectionSteering steering) {
        final int index = ports.indexOf(steering);
        if (index >= 0) {
            ports.remove(index);
        }
    }

    private static void appendGroupSample(BorrowableUtf8Sink sink, String name, ConnectionSteering steering, int group, long value) {
        appendNamePrefix(sink, name);
        sink.putAscii('{');
        PrometheusFormatUtils.appendLabel(sink, LABEL_SERVER, steering.getName());
        sink.putAscii(',');
        PrometheusFormatUtils.appendLabel(sink, LABEL_GROUP, Integer.toString(group));
        sink.putAscii('}');
        PrometheusFormatUtils.appendSampleLineSuffix(sink, value);
    }

    private static void appendNamePrefix(BorrowableUtf8Sink sink, String name) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.putAscii(name);
    }

    private static void appendType(BorrowableUtf8Sink sink, String name, String type) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.putAscii(name);
        sink.putAscii(' ');
        sink.putAscii(type);
        sink.putAscii('\n');
    }
}
