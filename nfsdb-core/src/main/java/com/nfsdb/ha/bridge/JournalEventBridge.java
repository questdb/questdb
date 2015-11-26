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

package com.nfsdb.ha.bridge;

import com.nfsdb.concurrent.*;

import java.util.concurrent.TimeUnit;

public class JournalEventBridge {

    private static final int BUFFER_SIZE = 1024;
    private final RingQueue<JournalEvent> queue;
    private final Sequence publisher;
    private final FanOut fanOut;
    private final long time;
    private final TimeUnit unit;

    public JournalEventBridge(long time, TimeUnit unit) {
        this.queue = new RingQueue<>(JournalEvent.EVENT_FACTORY, BUFFER_SIZE);
        this.publisher = new MPSequence(BUFFER_SIZE, null);
        this.fanOut = new FanOut();
        this.publisher.followedBy(fanOut);
        this.time = time;
        this.unit = unit;
    }

    public Sequence createAgentSequence() {
        Sequence sequence = new SCSequence(publisher.current(), new TimeoutBlockingWaitStrategy(time, unit));
        sequence.followedBy(publisher);
        fanOut.add(sequence);
        return sequence;
    }

    public RingQueue<JournalEvent> getQueue() {
        return queue;
    }

    public void publish(final int journalIndex, final long timestamp) {
        long cursor = publisher.nextBully();
        JournalEvent event = queue.get(cursor);
        event.setIndex(journalIndex);
        event.setTimestamp(timestamp);
        publisher.done(cursor);
        System.out.println("PUBLISHED: " + cursor);
    }

    public void removeAgentSequence(Sequence sequence) {
        fanOut.remove(sequence);
    }
}
