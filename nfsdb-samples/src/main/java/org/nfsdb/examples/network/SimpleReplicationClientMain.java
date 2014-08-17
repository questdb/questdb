/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package org.nfsdb.examples.network;

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.net.JournalClient;
import com.nfsdb.journal.tx.TxListener;
import org.nfsdb.examples.model.Price;

import java.net.UnknownHostException;

public class SimpleReplicationClientMain {
    public static void main(String[] args) throws JournalException, JournalNetworkException, UnknownHostException {
        JournalFactory factory = new JournalFactory(args[0]);
        JournalClient client = new JournalClient(factory);
        client.subscribe(Price.class, null, "price-copy", new TxListener() {
            @Override
            public void onCommit() {
                System.out.println("commit received");
            }
        });
        client.start();
        System.out.println("Client started");
    }
}
