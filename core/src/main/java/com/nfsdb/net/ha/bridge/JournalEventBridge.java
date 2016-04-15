/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.ha.bridge;

import com.nfsdb.mp.*;

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
        this.publisher = new MPSequence(BUFFER_SIZE);
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
    }

    public void removeAgentSequence(Sequence sequence) {
        fanOut.remove(sequence);
    }
}
