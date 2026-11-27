/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts the sequencer metadata of one table that the engine's pool has handed out and not had back, through the
 * engine's pool listener, which reports each get and each return. A statement that compiles a schema change of a WAL
 * table holds one while it compiles, so a count of zero after the statement ended says that it returned it, on
 * whatever thread it ended. Closing the counter puts the listener it replaced back.
 */
final class SequencerMetadataCounter implements PoolListener, Closeable {
    private final AtomicInteger count = new AtomicInteger();
    private final CairoEngine engine;
    private final PoolListener previousListener;
    private final String tableDirName;

    SequencerMetadataCounter(CairoEngine engine, TableToken tableToken) {
        this.engine = engine;
        this.tableDirName = tableToken.getDirName();
        this.previousListener = engine.getPoolListener();
        engine.setPoolListener(this);
    }

    @Override
    public void close() {
        engine.setPoolListener(previousListener);
    }

    @Override
    public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
        if (previousListener != null) {
            previousListener.onEvent(factoryType, thread, tableToken, event, segment, position);
        }
        if (factoryType != PoolListener.SRC_SEQUENCER_METADATA || tableToken == null || !tableDirName.equals(tableToken.getDirName())) {
            return;
        }
        switch (event) {
            case PoolListener.EV_CREATE, PoolListener.EV_GET -> count.incrementAndGet();
            case PoolListener.EV_RETURN, PoolListener.EV_OUT_OF_POOL_CLOSE -> count.decrementAndGet();
            default -> {
                // the other events neither hand a metadata out nor take one back
            }
        }
    }

    int get() {
        return count.get();
    }
}
