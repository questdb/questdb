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

package com.nfsdb.journal.net;

import com.nfsdb.journal.net.protocol.commands.IntResponseConsumer;
import com.nfsdb.journal.net.protocol.commands.IntResponseProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntTest {
    private MockByteChannel channel;

    @Before
    public void setUp() throws Exception {
        channel = new MockByteChannel();
    }

    @Test
    public void testInt() throws Exception {
        IntResponseProducer producer = new IntResponseProducer();
        IntResponseConsumer consumer = new IntResponseConsumer();

        producer.write(channel, 155);
        consumer.reset();
        consumer.read(channel);
        Assert.assertTrue(consumer.isComplete());
        Assert.assertEquals(155, consumer.getValue());
    }
}
