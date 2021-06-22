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

package io.questdb.tasks;

import io.questdb.cairo.CairoEngine;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.datetime.microtime.MicrosecondClock;

public final class TelemetryTask {
    public static void doStoreTelemetry(CairoEngine engine, short event, short origin) {
        Sequence telemetryPubSeq = engine.getTelemetryPubSequence();
        if (null != telemetryPubSeq) {
            MicrosecondClock clock = engine.getConfiguration().getMicrosecondClock();
            RingQueue<TelemetryTask> telemetryQueue = engine.getTelemetryQueue();
            long cursor = telemetryPubSeq.next();
            while (cursor == -2) {
                cursor = telemetryPubSeq.next();
            }

            if (cursor > -1) {
                TelemetryTask row = telemetryQueue.get(cursor);

                row.created = clock.getTicks();
                row.event = event;
                row.origin = origin;
                telemetryPubSeq.done(cursor);
            }
        }
    }

    public long created;
    public CharSequence id;
    public short event;
    public short origin;
}
