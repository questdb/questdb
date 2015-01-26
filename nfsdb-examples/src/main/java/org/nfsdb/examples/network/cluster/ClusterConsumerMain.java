/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nfsdb.examples.network.cluster;

import com.nfsdb.Journal;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.net.JournalClient;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.tx.TxListener;
import org.nfsdb.examples.model.Price;

public class ClusterConsumerMain {
    public static void main(String[] args) throws Exception {
        JournalFactory factory = new JournalFactory(args[0]);
        final JournalClient client = new JournalClient(new ClientConfig("localhost:7080,localhost:7090") {{
            getReconnectPolicy().setRetryCount(6);
            getReconnectPolicy().setSleepBetweenRetriesMillis(1);
            getReconnectPolicy().setLoginRetryCount(2);
        }}, factory);

        final Journal<Price> reader = factory.bulkReader(Price.class, "price-copy");

        client.subscribe(Price.class, null, "price-copy", new TxListener() {
            @Override
            public void onCommit() {
                int count = 0;
                long t = 0;
                for (Price p : reader.incrementBuffered()) {
                    if (count == 0) {
                        t = p.getNanos();
                    }
                    count++;
                }
                System.out.println("took: " + (System.nanoTime() - t) + ", count=" + count);
            }
        });
        client.start();

        System.out.println("Client started");
    }
}
