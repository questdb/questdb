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

package com.questdb.net.ha.bridge;

import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.mp.TimeoutException;

public class JournalEventProcessor {
    private final RingQueue<JournalEvent> queue;
    private final Sequence sequence;

    public JournalEventProcessor(JournalEventBridge bridge) {
        this.queue = bridge.getQueue();
        this.sequence = bridge.createAgentSequence();
    }

    public Sequence getSequence() {
        return sequence;
    }

    public boolean process(JournalEventHandler handler, boolean blocking) {
        try {
            long cursor = blocking ? sequence.waitForNext() : sequence.next();
            if (cursor >= 0) {
                int index = queue.get(cursor).getIndex();
                sequence.done(cursor);
                handler.handle(index);
            }
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }
}
