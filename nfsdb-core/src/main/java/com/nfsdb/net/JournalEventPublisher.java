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

package com.nfsdb.net;

import com.nfsdb.net.bridge.JournalEventBridge;
import com.nfsdb.tx.TxListener;

public class JournalEventPublisher implements TxListener {
    private final int journalIndex;
    private final JournalEventBridge bridge;

    public JournalEventPublisher(int journalIndex, JournalEventBridge bridge) {
        this.journalIndex = journalIndex;
        this.bridge = bridge;
    }

    @Override
    public void onCommit() {
        bridge.publish(journalIndex, System.currentTimeMillis());
    }
}
