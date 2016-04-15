/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

package org.nfsdb.examples.messaging;

import com.lmax.disruptor.RingBuffer;
import com.nfsdb.misc.Rnd;

public class TickMarket implements Runnable {
    private final RingBuffer<Tick> buffer;
    private final int messageCount;
    private final Rnd random;
    private final int instrumentCount;

    public TickMarket(RingBuffer<Tick> buffer, int instrumentCount, int messageCount) {
        this.buffer = buffer;
        this.instrumentCount = instrumentCount;
        this.messageCount = messageCount;
        this.random = new Rnd(System.nanoTime(), System.currentTimeMillis());
    }

    @Override
    public void run() {
        int cursor = 0;
        while (cursor < messageCount) {
            long sequence = buffer.next();
            Tick tick = buffer.get(sequence);
            tick.bidAsk = (byte) (random.nextBoolean() ? 'b' : 'a');
            tick.instrument = Math.abs(random.nextInt() % instrumentCount);
            tick.price = Math.abs((random.nextLong() % 10000));
            tick.volume = Math.abs(random.nextInt() % 1000);
            buffer.publish(sequence);
            cursor++;
        }

        // market closed message
        long sequence = buffer.next();
        Tick tick = buffer.get(sequence);
        tick.instrument = -1;
        buffer.publish(sequence);
        System.out.println("Market closed");
    }
}
