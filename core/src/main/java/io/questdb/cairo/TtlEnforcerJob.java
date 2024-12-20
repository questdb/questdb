/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;

public class TtlEnforcerJob extends SynchronizedJob {
    private static final long CLEANUP_INTERVAL_MICROS = Timestamps.SECOND_MICROS;
    private static final Log LOG = LogFactory.getLog(TtlEnforcerJob.class.getName());
    private final MicrosecondClock clock;
    private final CairoEngine engine;
    private long lastCleanupTs;

    public TtlEnforcerJob(CairoEngine engine) {
        this.engine = engine;
        this.clock = engine.getConfiguration().getMicrosecondClock();
    }

    public static void assignToPool(WorkerPool pool, CairoEngine engine) {
        TtlEnforcerJob job = new TtlEnforcerJob(engine);
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            pool.assign(i, job);
        }
    }

    @Override
    protected boolean runSerially() {
        final long now = clock.getTicks();
        if (now - lastCleanupTs <= CLEANUP_INTERVAL_MICROS) {
            return false;
        }
        lastCleanupTs = now;
        LOG.info().$("Run TTL enforcer").$();
        final CharSequenceObjHashMap<CairoTable> tableCache = new CharSequenceObjHashMap<>();
        try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
            metadataRO.snapshot(tableCache, -1);
        }
        for (int n = tableCache.size(), i = 0; i < n; i++) {
            CairoTable table = tableCache.getAt(i);
            try (TableWriter w = engine.getWriter(table.getTableToken(), "ttl-enforcer")) {
//                w.enforceTtl();
            }
        }
        return false;
    }
}
