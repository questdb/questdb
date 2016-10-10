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
import com.questdb.factory.JournalCachingFactory;
import com.questdb.factory.JournalFactory;
import com.questdb.factory.JournalFactoryPool;
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
import com.questdb.ql.model.ParsedModel;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.AssociativeCache;
import com.questdb.std.Mutable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


public abstract class AbstractQueryContext implements Mutable, Closeable {
    public static final int QUERY_PREFIX = 1;
    public static final int QUERY_METADATA = 2;
    public static final int QUERY_META_SUFFIX = 3;
    public static final int QUERY_RECORD_START = 4;
    public static final int QUERY_RECORD_COLUMNS = 5;
    public static final int QUERY_RECORD_SUFFIX = 6;
    public static final int QUERY_DATA_SUFFIX = 7;
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
    JournalCachingFactory factory;
    int queryState = QUERY_PREFIX;
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
            CACHE.get().put(query.toString(), recordSource);
            recordSource = null;
        }
        query = null;
        queryState = QUERY_PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() throws IOException {
        debug().$("Closing context").$();
        clear();
    }

    public void compileQuery(
            ChunkedResponse r,
            JournalFactoryPool pool,
            JournalFactory writerFactory,
            AtomicLong misses,
            AtomicLong hits) throws IOException {
        try {
            this.factory = pool.get();
            recordSource = CACHE.get().poll(query);
            if (recordSource == null) {
                recordSource = executeQuery(r, writerFactory, pool);
                misses.incrementAndGet();
            } else {
                hits.incrementAndGet();
            }

            header(r, 200);
            if (recordSource != null) {
                cursor = recordSource.prepareCursor(factory, cancellationHandler);
                metadata = recordSource.getMetadata();
            } else {
                sendConfirmation(r);
            }
        } catch (ParserException e) {
            syntaxError(r);
        } catch (JournalRuntimeException e) {
            internalError(r, e);
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

    private RecordSource executeQuery(ChunkedResponse r, JournalFactory writerFactory, JournalFactoryPool pool) throws ParserException, DisconnectedChannelException, SlowWritableChannelException {
        QueryCompiler compiler = COMPILER.get();
        ParsedModel model = compiler.parse(query);
        switch (model.getModelType()) {
            case ParsedModel.QUERY:
                return compiler.compile(factory, model);
            default:
                if (writerFactory != null) {
                    try {
                        compiler.execute(writerFactory, factory, pool, model);
                    } catch (JournalException e) {
                        error().$("Server error executing statement ").$(query).$(e).$();
                        sendException(r, 0, e.getMessage(), 500);
                    }
                } else {
                    error().$("Statement execution is not supported: ").$(query).$();
                    sendException(r, 0, "Statement execution is not supported", 400);
                }
                return null;
        }
    }

    protected abstract void header(ChunkedResponse r, int code) throws DisconnectedChannelException, SlowWritableChannelException;

    LogRecord info() {
        return LOG.info().$('[').$(fd).$("] ");
    }

    private void internalError(ChunkedResponse r, Throwable e) throws DisconnectedChannelException, SlowWritableChannelException {
        error().$("Server error executing query ").$(query).$(e).$();
        sendException(r, 0, e.getMessage(), 500);
    }

    private void sendConfirmation(ChunkedResponse r) throws DisconnectedChannelException, SlowWritableChannelException {
        r.put('{').putQuoted("ddl").put(':').putQuoted("OK").put('}');
        r.sendChunk();
        r.done();
    }

    protected abstract void sendException(ChunkedResponse r, int position, CharSequence message, int code) throws DisconnectedChannelException, SlowWritableChannelException;

    private void syntaxError(ChunkedResponse r) throws DisconnectedChannelException, SlowWritableChannelException {
        info().$("Parser error executing query ").$(query).$(": at (").$(QueryError.getPosition()).$(") ").$(QueryError.getMessage()).$();
        sendException(r, QueryError.getPosition(), QueryError.getMessage(), 400);
    }
}
