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

public class HttpExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;")) {
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
