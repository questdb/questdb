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

package com.nfsdb.net.ha.bridge;

import com.nfsdb.ex.TimeoutException;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
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

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_RETURN_FALSE")
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
