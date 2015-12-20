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

package com.nfsdb;

import com.nfsdb.model.Quote;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class JournalKeyTest {
    @Test
    public void testKeyEquals() throws Exception {
        JournalKey<Quote> key1 = new JournalKey<>(Quote.class);
        JournalKey<Quote> key2 = new JournalKey<>(Quote.class, "Quote22");
        Assert.assertNotEquals(key1, key2);
    }

    @Test
    public void testKeySerialization() throws Exception {
        JournalKey<Quote> key = new JournalKey<>(Quote.class, "Quote22");
        ByteBuffer buffer = ByteBuffer.allocate(key.getBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        key.write(buffer);
        Assert.assertEquals(key.getBufferSize(), buffer.position());

        buffer.flip();
        JournalKey<Quote> key2 = JournalKey.fromBuffer(buffer);
        Assert.assertEquals(key, key2);
    }

    @Test
    public void testKeySerializationNullLocation() throws Exception {
        JournalKey<Quote> key = new JournalKey<>(Quote.class);
        ByteBuffer buffer = ByteBuffer.allocate(key.getBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        key.write(buffer);
        Assert.assertEquals(key.getBufferSize(), buffer.position());

        buffer.flip();
        JournalKey<Quote> key2 = JournalKey.fromBuffer(buffer);
        Assert.assertEquals(key, key2);
    }
}
