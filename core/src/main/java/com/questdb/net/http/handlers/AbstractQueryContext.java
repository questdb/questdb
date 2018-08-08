/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.BootstrapEnv;
import com.questdb.ex.ParserException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.Request;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.parser.sql.QueryError;
import com.questdb.parser.sql.model.ParsedModel;
import com.questdb.ql.ChannelCheckCancellationHandler;
import com.questdb.ql.RecordSource;
import com.questdb.std.*;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.Factory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ThreadLocal;
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
        if (cursor != null) {
            cursor.releaseCursor();
            cursor = null;
        }
        record = null;
        if (recordSource != null) {
            CACHE.get().put(query.toString(), recordSource);
            recordSource = null;
        }
        query = null;
        queryState = QUERY_PREFIX;
        columnIndex = 0;
    }

    @Override
    public void close() {
        debug().$("Closing context").$();
        clear();
    }

    public void compileQuery(
            ChunkedResponse r,
            Factory factory,
            AtomicLong misses,
            AtomicLong hits) throws IOException {
        try {
            recordSource = CACHE.get().poll(query);
            int retryCount = 0;
            do {
                if (recordSource == null) {
                    recordSource = executeQuery(r, factory);
                    misses.incrementAndGet();
                } else {
                    hits.incrementAndGet();
                }

                if (recordSource != null) {
                    try {
                        cursor = recordSource.prepareCursor(factory, cancellationHandler);
                        metadata = recordSource.getMetadata();
                        header(r, 200);
                        break;
                    } catch (JournalRuntimeException e) {
                        if (retryCount == 0) {
                            CACHE.get().put(query.toString(), null);
                            recordSource = null;
                            LOG.error().$("RecordSource execution failed. ").$(e.getMessage()).$(". Retrying ...").$();
                            retryCount++;
                        } else {
                            internalError(r, e);
                            break;
                        }
                    }
                } else {
                    header(r, 200);
                    sendConfirmation(r);
                    break;
                }
            } while (true);
        } catch (ParserException e) {
            syntaxError(r);
        } catch (JournalRuntimeException e) {
            internalError(r, e);
        }
    }

    public boolean parseUrl(ChunkedResponse r, Request request) throws DisconnectedChannelException, SlowWritableChannelException {
        // Query text.
        CharSequence query = request.getUrlParam("query");
        if (query == null || query.length() == 0) {
            info().$("Empty query request received. Sending empty reply.").$();
            sendException(r, 0, "No query text", 400);
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

    static void setupThread(BootstrapEnv env) {
        if (COMPILER.get() == null) {
            COMPILER.set(new QueryCompiler(env));
        }
        if (CACHE.get() == null) {
            CACHE.set(new AssociativeCache<>(8, 128));
        }
    }

    LogRecord debug() {
        return LOG.debug().$('[').$(fd).$("] ");
    }

    LogRecord error() {
        return LOG.error().$('[').$(fd).$("] ");
    }

    private RecordSource executeQuery(ChunkedResponse r, Factory factory) throws ParserException, DisconnectedChannelException, SlowWritableChannelException {
        QueryCompiler compiler = COMPILER.get();
        ParsedModel model = compiler.parse(query);
        switch (model.getModelType()) {
            case ParsedModel.QUERY:
                return compiler.compile(factory, model);
            default:
                try {
                    compiler.execute(factory, model);
                } catch (JournalException e) {
                    error().$("Server error executing statement ").$(query).$(e).$();
                    sendException(r, 0, e.getMessage(), 500);
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
