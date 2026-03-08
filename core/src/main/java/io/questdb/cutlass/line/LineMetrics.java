/*******************************************************************************
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

package io.questdb.cutlass.line;

import io.questdb.metrics.AtomicLongGauge;
import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;

public class LineMetrics implements Mutable {

    private final Counter aboveMaxConnectionCountCounter;
    private final Counter belowMaxConnectionCountCounter;
    private final AtomicLongGauge httpConnectionCountGauge;
    private final LongGauge tcpConnectionCountGauge;
    private final LongGauge totalIlpHttpBytesGauge;
    private final LongGauge totalIlpTcpBytesGauge;

    public LineMetrics(MetricsRegistry metricsRegistry) {
        this.httpConnectionCountGauge = metricsRegistry.newAtomicLongGauge("line_http_connections");
        this.tcpConnectionCountGauge = metricsRegistry.newLongGauge("line_tcp_connections");
        this.totalIlpTcpBytesGauge = metricsRegistry.newLongGauge("line_tcp_recv_bytes");
        this.totalIlpHttpBytesGauge = metricsRegistry.newLongGauge("line_http_recv_bytes");
        this.aboveMaxConnectionCountCounter = metricsRegistry.newCounter("line_tcp_above_max_connection_count");
        this.belowMaxConnectionCountCounter = metricsRegistry.newCounter("line_tcp_below_max_connection_count");
    }

    public Counter aboveMaxConnectionCountCounter() {
        return aboveMaxConnectionCountCounter;
    }

    public Counter belowMaxConnectionCountCounter() {
        return belowMaxConnectionCountCounter;
    }

    @Override
    public void clear() {
        httpConnectionCountGauge.setValue(0);
        tcpConnectionCountGauge.setValue(0);
        totalIlpTcpBytesGauge.setValue(0);
        totalIlpHttpBytesGauge.setValue(0);
        aboveMaxConnectionCountCounter.reset();
        belowMaxConnectionCountCounter.reset();
    }

    public AtomicLongGauge httpConnectionCountGauge() {
        return httpConnectionCountGauge;
    }

    public LongGauge tcpConnectionCountGauge() {
        return tcpConnectionCountGauge;
    }

    public LongGauge totalIlpHttpBytesGauge() {
        return totalIlpHttpBytesGauge;
    }

    public LongGauge totalIlpTcpBytesGauge() {
        return totalIlpTcpBytesGauge;
    }
}
