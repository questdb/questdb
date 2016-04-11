/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016 Appsicle
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
 ******************************************************************************/

package org.nfsdb.examples.ha.plain;

import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.net.ha.JournalServer;
import org.nfsdb.examples.model.Price;

import java.util.concurrent.TimeUnit;

public class SimpleReplicationServerMain {

    private final String location;

    public SimpleReplicationServerMain(String location) {
        this.location = location;
    }

    public static void main(String[] args) throws Exception {
        new SimpleReplicationServerMain(args[0]).start();
    }

    public void start() throws Exception {
        JournalFactory factory = new JournalFactory(location);
        JournalServer server = new JournalServer(factory);

        JournalWriter<Price> writer = factory.writer(Price.class);
        server.publish(writer);

        server.start();

        System.out.print("Publishing: ");
        for (int i = 0; i < 10; i++) {
            publishPrice(writer, i < 3 ? 1000000 : 100);
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
            System.out.print('.');
        }
        System.out.println(" [Done]");
    }

    private void publishPrice(JournalWriter<Price> writer, int count)
            throws JournalException {
        long tZero = System.currentTimeMillis();
        System.out.println("sending: " + tZero);
        Price p = new Price();
        for (int i = 0; i < count; i++) {
            p.setTimestamp(tZero + i);
            p.setNanos(System.nanoTime());
            p.setSym(String.valueOf(i % 20));
            p.setPrice(i * 1.04598 + i);
            writer.append(p);
        }
        // commit triggers network publishing
        writer.commit();
    }
}
