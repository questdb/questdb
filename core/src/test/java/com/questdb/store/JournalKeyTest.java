/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store;

import com.questdb.model.Quote;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class JournalKeyTest {
    @Test
    public void testKeyEquals() {
        JournalKey<Quote> key1 = new JournalKey<>(Quote.class);
        JournalKey<Quote> key2 = new JournalKey<>(Quote.class, "Quote22");
        Assert.assertNotEquals(key1, key2);
    }

    @Test
    public void testKeySerialization() {
        JournalKey<Quote> key = new JournalKey<>(Quote.class, "Quote22");
        ByteBuffer buffer = ByteBuffer.allocate(key.getBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        key.write(buffer);
        Assert.assertEquals(key.getBufferSize(), buffer.position());

        buffer.flip();
        JournalKey<Quote> key2 = JournalKey.fromBuffer(buffer);
        Assert.assertEquals(key, key2);
    }

    @Test
    public void testKeySerializationNullLocation() {
        JournalKey<Quote> key = new JournalKey<>(Quote.class);
        ByteBuffer buffer = ByteBuffer.allocate(key.getBufferSize()).order(ByteOrder.LITTLE_ENDIAN);
        key.write(buffer);
        Assert.assertEquals(key.getBufferSize(), buffer.position());

        buffer.flip();
        JournalKey<Quote> key2 = JournalKey.fromBuffer(buffer);
        Assert.assertEquals(key, key2);
    }
}
