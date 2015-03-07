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
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadataImpl;
import com.nfsdb.ha.comsumer.HugeBufferConsumer;
import com.nfsdb.ha.producer.HugeBufferProducer;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Test;

import java.io.File;

public class MetadataReplicationTest extends AbstractTest {
    @Test
    public void testName() throws Exception {

        JournalWriter w = factory.writer(Quote.class);
        System.out.println(w.getMetadata().getLocation());

        MockByteChannel channel = new MockByteChannel();
        HugeBufferProducer p = new HugeBufferProducer(new File(w.getMetadata().getLocation(), JournalConfiguration.FILE_NAME));
        HugeBufferConsumer c = new HugeBufferConsumer(new File(w.getMetadata().getLocation(), "_remote"));
        p.write(channel);
        c.read(channel);

        JournalMetadataImpl m = new JournalMetadataImpl(c.getHb());
        System.out.println(m);
    }
}
