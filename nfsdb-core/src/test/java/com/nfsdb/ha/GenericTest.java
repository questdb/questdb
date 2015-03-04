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

package com.nfsdb.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.ha.config.ServerNode;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Ignore;
import org.junit.Test;

public class GenericTest extends AbstractTest {
    @Test
    @Ignore
    public void testGenericPublish() throws Exception {

        JournalWriter w = factory.writer(new JournalStructure("xyz") {{
            $sym("x").index();
            $int("y");
            $double("z");
            $ts();
            partitionBy(PartitionType.DAY);
        }});


        JournalServer server = new JournalServer(new ServerConfig() {{
            addNode(new ServerNode(1, "localhost"));
        }}, factory);
        server.publish(w);
        server.start();


        //JournalClient client = new JournalClient(new ClientConfig("localhost"), factory);


        Thread.sleep(10000);

    }
}
