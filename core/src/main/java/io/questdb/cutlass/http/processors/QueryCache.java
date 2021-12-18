/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.AssociativeCache;
import io.questdb.std.ThreadLocal;

import java.io.Closeable;

public final class QueryCache implements Closeable {

    private static final Log LOG = LogFactory.getLog(QueryCache.class);
    private static ThreadLocal<QueryCache> TL_QUERY_CACHE;
    private final AssociativeCache<RecordCursorFactory> cache;

    public QueryCache(int blocks, int rows) {
        this.cache = new AssociativeCache<>(blocks, rows);
    }

    public static void configure(HttpServerConfiguration configuration) {
        TL_QUERY_CACHE = new ThreadLocal<>(() -> new QueryCache(configuration.getQueryCacheBlocks(), configuration.getQueryCacheRows()));
    }

    public static QueryCache getInstance() {
        return TL_QUERY_CACHE.get();
    }

    @Override
    public void close() {
        cache.close();
        LOG.info().$("closed").$();
    }

    public RecordCursorFactory poll(CharSequence sql) {
        final RecordCursorFactory factory = cache.poll(sql);
        log(factory == null ? "miss" : "hit", sql);
        return factory;
    }

    public void push(CharSequence sql, RecordCursorFactory factory) {
        if (factory != null) {
            cache.put(sql, factory);
            log("push", sql);
        }
    }

    public void remove(CharSequence sql) {
        cache.put(sql, null);
        log("remove", sql);
    }

    private void log(CharSequence action, CharSequence sql) {
        LOG.info().$(action).$(" [thread=").$(Thread.currentThread().getName()).$(", sql=").utf8(sql).$(']').$();
    }
}
