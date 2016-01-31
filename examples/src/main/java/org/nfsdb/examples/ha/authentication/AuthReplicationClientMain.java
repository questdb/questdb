/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package org.nfsdb.examples.ha.authentication;

import com.nfsdb.Journal;
import com.nfsdb.JournalIterators;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.net.ha.JournalClient;
import com.nfsdb.net.ha.auth.CredentialProvider;
import com.nfsdb.store.TxListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.nfsdb.examples.model.Price;

import java.io.UnsupportedEncodingException;

/**
 * Single journal replication client example.
 *
 * @since 2.0.1
 */
public class AuthReplicationClientMain {
    public static void main(String[] args) throws Exception {
        JournalFactory factory = new JournalFactory(args[0]);
        final JournalClient client = new JournalClient(factory, new CredentialProvider() {
            @SuppressFBWarnings({"BED_BOGUS_EXCEPTION_DECLARATION"})
            @Override
            public byte[] createToken() throws UnsupportedEncodingException {
                return "MY SECRET".getBytes("UTF8");
            }
        });

        final Journal<Price> reader = factory.bulkReader(Price.class, "price-copy");

        client.subscribe(Price.class, null, "price-copy", new TxListener() {
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
                System.out.println("took: " + (System.nanoTime() - t) + ", count=" + count);
            }

            @Override
            public void onError() {
                System.out.println("There was an error");
            }
        });
        client.start();

        System.out.println("Client started");
    }
}
