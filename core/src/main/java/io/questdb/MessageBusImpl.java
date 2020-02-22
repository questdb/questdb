/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.sql.scopes.ColumnIndexerScope;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;

public class MessageBusImpl implements MessageBus {
    private final RingQueue<ColumnIndexerScope> queue = new RingQueue<>(ColumnIndexerScope::new, 1024);
    private final MPSequence pubSeq = new MPSequence(queue.getCapacity());
    private final MCSequence subSeq = new MCSequence(queue.getCapacity());

    public MessageBusImpl() {
        this.pubSeq.then(this.subSeq).then(this.pubSeq);
    }

    @Override
    public Sequence getIndexerPubSequence() {
        return pubSeq;
    }

    @Override
    public RingQueue<ColumnIndexerScope> getIndexerQueue() {
        return queue;
    }

    @Override
    public Sequence getIndexerSubSequence() {
        return subSeq;
    }
}
