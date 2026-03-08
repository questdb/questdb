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

package io.questdb.test.client.example;

import io.questdb.client.Sender;

/**
 * Example showing how to use Sender API to connect to a remote QuestDB server and ingest data.
 */
public class SenderExamples {

    public static void main(String[] args) {

        // how to use Sender API with no authentication and no encryption
        try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost:9009").build()) {
            sender.table("mytable")
                    .longColumn("id", 0)
                    .stringColumn("name", "Joe Adams")
                    .atNow();
        }

        // how to use Sender API with QuestDB Cloud
        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address("clever-black-363-c1213c97.ilp.b04c.questdb.net:32074")
                .enableTls()
                .enableAuth("admin").authToken("GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0")
                .build()) {
            sender.table("mytable")
                    .longColumn("id", 0)
                    .stringColumn("name", "Joe Adams")
                    .atNow();
        }
    }
}
