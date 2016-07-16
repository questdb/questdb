/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.ex.*;
import com.questdb.factory.JournalFactoryPool;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.Request;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.ChannelCheckCancellationHandler;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.AssociativeCache;
import com.questdb.std.Mutable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


public abstract class AbstractQueryContext implements Mutable, Closeable {
    static final ThreadLocal<QueryCompiler> COMPILER = new ThreadLocal<>();
    static final ThreadLocal<AssociativeCache<RecordSource>> CACHE = new ThreadLocal<>();
    static final Log LOG = LogFactory.getLog(AbstractQueryContext.class);
    final ChannelCheckCancellationHandler cancellationHandler;
    final long fd;
    RecordSource recordSource;
    CharSequence query;
    RecordMetadata metadata;
    RecordCursor cursor;
    long count;
    long skip;
    long stop;
    Record record;
    JournalReaderFactory factory;
    QueryState state = QueryState.PREFIX;
    int columnIndex;

    public AbstractQueryContext(long fd, int cyclesBeforeCancel) {
        this.cancellationHandler = new ChannelCheckCancellationHandler(fd, cyclesBeforeCancel);
        this.fd = fd;
    }

    @Override
    public void clear() {
        debug().$("Cleaning context").$();
        metadata = null;
        cursor = null;
        record = null;
        if (factory != null) {
            debug().$("Closing journal factory ").$();
        }
        factory = Misc.free(factory);
        if (recordSource != null) {
            recordSource.reset();
            CACHE.get().put(query.toString(), recordSource);
            recordSource = null;
        }
        query = null;
        state = QueryState.PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() throws IOException {
        debug().$("Closing context").$();
        clear();
    }

    public void compileQuery(ChunkedResponse r, JournalFactoryPool pool, AtomicLong misses, AtomicLong hits) throws IOException {
        try {
            // Prepare Context.
            this.factory = pool.get();
            recordSource = CACHE.get().poll(query);
            if (recordSource == null) {
                recordSource = COMPILER.get().compileSource(factory, query);
                misses.incrementAndGet();
            } else {
                hits.incrementAndGet();
            }
            cursor = recordSource.prepareCursor(factory, cancellationHandler);
            metadata = recordSource.getMetadata();
            header(r, 200);
        } catch (ParserException e) {
            info().$("Parser error executing query ").$(query).$(": at (").$(QueryError.getPosition()).$(") ").$(QueryError.getMessage()).$();
            sendException(r, QueryError.getPosition(), QueryError.getMessage(), 400);
        } catch (JournalRuntimeException e) {
            error().$("Server error executing query ").$(query).$(e).$();
            sendException(r, 0, e.getMessage(), 500);
        } catch (InterruptedException e) {
            error().$("Error executing query. Server is shutting down. Query: ").$(query).$(e).$();
            sendException(r, 0, "Server is shutting down.", 500);
        }
    }

    public boolean parseUrl(ChunkedResponse r, Request request) throws DisconnectedChannelException, SlowWritableChannelException {
        // Query text.
        CharSequence query = request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            info().$("Empty query request received. Sending empty reply.").$();
            sendException(r, 0, "", 200);
            return false;
        }

        // Url Params.
        long skip = 0;
        long stop = Long.MAX_VALUE;

        CharSequence limit = request.getUrlParam("limit");
        if (limit != null) {
            int sepPos = Chars.indexOf(limit, ',');
            try {
                if (sepPos > 0) {
                    skip = Numbers.parseLong(limit, 0, sepPos);
                    if (sepPos + 1 < limit.length()) {
                        stop = Numbers.parseLong(limit, sepPos + 1, limit.length());
                    }
                } else {
                    stop = Numbers.parseLong(limit);
                }
            } catch (NumericException ex) {
                // Skip or stop will have default value.
            }
        }
        if (stop < 0) {
            stop = 0;
        }

        if (skip < 0) {
            skip = 0;
        }

        this.query = query;
        this.skip = skip;
        this.count = 0L;
        this.stop = stop;

        info().$("Query: ").$(query).
                $(", skip: ").$(skip).
                $(", stop: ").$(stop).$();

        return true;
    }

    static void setupThread(ServerConfiguration configuration) {
        if (COMPILER.get() == null) {
            COMPILER.set(new QueryCompiler(configuration));
        }
        if (CACHE.get() == null) {
            CACHE.set(new AssociativeCache<RecordSource>(8, 128));
        }
    }

    LogRecord debug() {
        return LOG.debug().$('[').$(fd).$("] ");
    }

    LogRecord error() {
        return LOG.error().$('[').$(fd).$("] ");
    }

    protected abstract void header(ChunkedResponse r, int code) throws DisconnectedChannelException, SlowWritableChannelException;

    LogRecord info() {
        return LOG.info().$('[').$(fd).$("] ");
    }

    protected abstract void sendException(ChunkedResponse r, int position, CharSequence message, int code) throws DisconnectedChannelException, SlowWritableChannelException;

    enum QueryState {
        PREFIX, METADATA, META_SUFFIX, RECORD_START, RECORD_COLUMNS, RECORD_SUFFIX, DATA_SUFFIX
    }
}
