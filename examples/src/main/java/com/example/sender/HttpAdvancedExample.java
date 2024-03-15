/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package com.example.sender;

import io.questdb.client.Sender;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class HttpAdvancedExample {
    public static void main(String[] args) {
        // https::addr=localhost:9000 - Sends data over HTTP transport to the server at localhost:9000 with TLS enabled.
        // tls_verify=unsafe_off - Disables SSL verification. Do not use this in production!
        // retry_timeout=20000 - Sets timeout for retries to 20 seconds. This means recoverable errors will be retried for 20 seconds.
        // auto_flush_rows=100000 - Sets maximum number of rows to be sent in a single request to 100,000.
        // auto_flush_interval=5000 - Sets maximum interval between requests to 5 seconds - to avoid long lags in low-throughput scenarios.
        try (Sender sender = Sender.fromConfig("https::addr=localhost:9000;tls_verify=unsafe_off;retry_timeout=20000;auto_flush_rows=100000;auto_flush_interval=5000;")) {
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .timestampColumn("birthday", Instant.parse("1856-07-10T00:00:00.00Z"))
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .at(System.currentTimeMillis(), ChronoUnit.MILLIS);
            sender.table("inventors")
                    .symbol("born", "USA")
                    .timestampColumn("birthday", Instant.parse("1847-02-11T00:00:00.00Z"))
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .at(System.currentTimeMillis(), ChronoUnit.MILLIS);
        }
    }
}
