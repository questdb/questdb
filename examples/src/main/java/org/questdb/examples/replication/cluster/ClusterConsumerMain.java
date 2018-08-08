/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2018 Appsicle
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

package org.questdb.examples.replication.cluster;

import com.questdb.net.ha.JournalClient;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.store.*;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import org.questdb.examples.support.Price;

public class ClusterConsumerMain {
    public static void main(String[] args) throws Exception {

        final JournalConfiguration configuration = new JournalConfigurationBuilder() {{
            $(Price.class).$ts();
        }}.build(args[0]);

        final Factory factory = new Factory(configuration, 1000, 1, 0);

        final JournalClient client = new JournalClient(new ClientConfig("127.0.0.1:7080,127.0.0.1:7090") {{
            getReconnectPolicy().setRetryCount(6);
            getReconnectPolicy().setSleepBetweenRetriesMillis(1);
            getReconnectPolicy().setLoginRetryCount(2);
        }}, factory);

        final Journal<Price> reader = factory.reader(new JournalKey<>(Price.class, "price-copy", PartitionBy.NONE, 1000000000));
        reader.setSequentialAccess(true);

        client.subscribe(Price.class, null, "price-copy", 1000000000, new JournalListener() {
            @Override
            public void onCommit() {
                int count = 0;
                long t = 0;
                for (Price p : JournalIterators.incrementBufferedIterator(reader)) {
                    if (count == 0) {
                        t = p.getNanos();
                    }
                    count++;
                }
                if (t == 0) {
                    System.out.println("no data received");
                } else {
                    System.out.println("took: " + (System.currentTimeMillis() - t) + ", count=" + count);
                }
            }

            @Override
            public void onEvent(int event) {
                System.out.println("there was an error");
            }
        });
        client.start();

        System.out.println("Client started");
    }
}
