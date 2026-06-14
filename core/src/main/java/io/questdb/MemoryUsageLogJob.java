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

package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public class MemoryUsageLogJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(MemoryUsageLogJob.class);

    private final MicrosecondClock clock;
    private final BooleanSupplier enabledSupplier;
    private final MemoryUsageFormatter formatter = new MemoryUsageFormatter();
    private final LongSupplier intervalMillisSupplier;
    private long lastRunMicros = Long.MIN_VALUE;

    public MemoryUsageLogJob(
            @NotNull MicrosecondClock clock,
            @NotNull BooleanSupplier enabledSupplier,
            @NotNull LongSupplier intervalMillisSupplier
    ) {
        this.clock = clock;
        this.enabledSupplier = enabledSupplier;
        this.intervalMillisSupplier = intervalMillisSupplier;
    }

    @Override
    protected boolean runSerially() {
        if (!enabledSupplier.getAsBoolean()) {
            // Reset so a subsequent enable fires immediately rather than after a stale wait.
            lastRunMicros = Long.MIN_VALUE;
            return false;
        }
        final long now = clock.getTicks();
        if (lastRunMicros != Long.MIN_VALUE) {
            final long intervalMicros = intervalMillisSupplier.getAsLong() * 1000;
            if (now - lastRunMicros < intervalMicros) {
                return false;
            }
        }
        lastRunMicros = now;
        final LogRecord record = LOG.info();
        if (record.isEnabled()) {
            record.$("memory usage [");
            formatter.format(record);
            record.I$();
        }
        return true;
    }
}
