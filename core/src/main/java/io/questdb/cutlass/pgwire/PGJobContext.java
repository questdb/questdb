/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.Misc;
import io.questdb.std.WeakSelfReturningObjectPool;

import java.io.Closeable;

public class PGJobContext implements Closeable {

    private final SqlCompiler compiler;
    private final AssociativeCache<TypesAndSelect> typesAndSelectCache;
    private final WeakSelfReturningObjectPool<TypesAndSelect> typesAndSelectPool;
    private final AssociativeCache<TypesAndUpdate> typesAndUpdateCache;
    private final WeakSelfReturningObjectPool<TypesAndUpdate> typesAndUpdatePool;

    public PGJobContext(
            PGWireConfiguration configuration,
            CairoEngine engine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent
    ) {
        this.compiler = new SqlCompiler(engine, functionFactoryCache, snapshotAgent, configuration.getSqlParserFactory());

        final Metrics metrics = engine.getMetrics();

        final boolean enableSelectCache = configuration.isSelectCacheEnabled();
        final int blockCount = enableSelectCache ? configuration.getSelectCacheBlockCount() : 1;
        final int rowCount = enableSelectCache ? configuration.getSelectCacheRowCount() : 1;
        typesAndSelectCache = new AssociativeCache<>(blockCount, rowCount, metrics.pgWire().cachedSelectsGauge());
        typesAndSelectPool = new WeakSelfReturningObjectPool<>(TypesAndSelect::new, blockCount * rowCount);

        final boolean enabledUpdateCache = configuration.isUpdateCacheEnabled();
        final int updateBlockCount = enabledUpdateCache ? configuration.getUpdateCacheBlockCount() : 1; // 8
        final int updateRowCount = enabledUpdateCache ? configuration.getUpdateCacheRowCount() : 1; // 8
        typesAndUpdateCache = new AssociativeCache<>(updateBlockCount, updateRowCount, metrics.pgWire().cachedUpdatesGauge());
        typesAndUpdatePool = new WeakSelfReturningObjectPool<>(parent -> new TypesAndUpdate(parent, engine), updateBlockCount * updateRowCount);
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(typesAndSelectCache);
        Misc.free(typesAndUpdateCache);
    }

    public void flushQueryCache() {
        typesAndSelectCache.clear();
        typesAndUpdateCache.clear();
    }

    public void handleClientOperation(
            PGConnectionContext context,
            int operation
    ) throws PeerIsSlowToWriteException, PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, BadProtocolException {
        context.handleClientOperation(
                compiler,
                typesAndSelectCache,
                typesAndSelectPool,
                typesAndUpdateCache,
                typesAndUpdatePool,
                operation
        );
    }
}
