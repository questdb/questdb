/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import org.jetbrains.annotations.Nullable;

public class FlushQueryCacheJob extends AbstractQueueConsumerJob<Object> {
    private static final Log LOG = LogFactory.getLog(FlushQueryCacheJob.class);
    private final HttpServer httpServer;
    private final PGServer pgServer;

    public FlushQueryCacheJob(MessageBus messageBus, @Nullable HttpServer httpServer, PGServer pgServer) {
        super(null, messageBus.getQueryCacheEventSubSeq());
        this.httpServer = httpServer;
        this.pgServer = pgServer;
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        try {
            if (httpServer != null) {
                LOG.info().$("flushing HTTP server select cache").$();
                httpServer.clearSelectCache();
            }
            if (pgServer != null) {
                LOG.info().$("flushing PGWire server select cache").$();
                pgServer.clearSelectCache();
            }
        } finally {
            subSeq.done(cursor);
        }
        return false;
    }
}
