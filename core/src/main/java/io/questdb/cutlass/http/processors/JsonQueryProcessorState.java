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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class JsonQueryProcessorState implements Mutable, Closeable {
    static final int QUERY_RECORD_PREFIX = 9;
    static final int QUERY_SETUP_FIRST_RECORD = 8;
    static final int QUERY_SUFFIX = 7;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_RECORD = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_METADATA_SUFFIX = 3;
    static final int QUERY_METADATA = 2;
    static final int QUERY_PREFIX = 1;
    // Factory cache is thread local due to possibility of factory being
    // closed by another thread. Peer disconnect is a typical example of this.
    // Being asynchronous we may need to be able to return factory to the cache
    // by the same thread that executes the dispatcher.
    static final ThreadLocal<AssociativeCache<RecordCursorFactory>> FACTORY_CACHE = ThreadLocal.withInitial(() -> new AssociativeCache<>(8, 8));
    final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    final long fd;
    final StringSink query = new StringSink();
    final ObjList<JsonQueryProcessor.ValueWriter> valueWriters = new ObjList<>();

    boolean countRows = false;
    boolean noMeta = false;
    RecordCursorFactory recordCursorFactory;
    RecordMetadata metadata;
    RecordCursor cursor;
    long count;
    long skip;
    long stop;
    Record record;
    int queryState = QUERY_PREFIX;
    int columnIndex;


    public JsonQueryProcessorState(long fd, int connectionCheckFrequency) {
        this.fd = fd;
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_PREFIX, this::onQueryPrefix);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_METADATA, this::onQueryMetadata);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_METADATA_SUFFIX, this::onQueryMetadataSuffix);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_SETUP_FIRST_RECORD, this::doFirstRecordLoop);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_RECORD_PREFIX, this::onQueryRecordPrefix);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_RECORD, this::onQueryRecord);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_RECORD_SUFFIX, this::onQueryRecordSuffix);
        resumeActions.extendAndSet(JsonQueryProcessorState.QUERY_SUFFIX, this::doQuerySuffix);
    }

    void resume(
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters,
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        resumeActions.getQuick(queryState).onResume(socket, columnCount, valueWriters);
    }

    private void onQueryRecordSuffix(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordSuffix(socket);
        doNextRecordLoop(socket, columnCount, valueWriters);
    }

    private void onQueryRecord(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecord(socket, columnCount);
        onQueryRecordSuffix(socket, columnCount, valueWriters);
    }

    private void onQueryRecordPrefix(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordPrefix(socket);
        onQueryRecord(socket, columnCount, valueWriters);
    }

    private void doQuerySuffix(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        queryState = QUERY_SUFFIX;
        if (count > -1) {
            socket.bookmark();
            socket.put(']');
            socket.put(',').putQuoted("count").put(':').put(count);
            socket.put('}');
            count = -1;
            socket.sendChunk();
        }
        socket.done();
    }

    private void doRecordFetchLoop(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        do {
            doQueryRecordPrefix(socket);
            doQueryRecord(socket, columnCount);
            doQueryRecordSuffix(socket);
        } while (doQueryNextRecord());
        doQuerySuffix(socket, columnCount, valueWriters);
    }

    private void onQueryMetadataSuffix(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadataSuffix(socket);
        doFirstRecordLoop(socket, columnCount, valueWriters);
    }

    private void onQueryMetadata(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadata(socket, columnCount);
        onQueryMetadataSuffix(socket, columnCount, valueWriters);
    }

    private void doFirstRecordLoop(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (onQuerySetupFirstRecord(valueWriters)) {
            doRecordFetchLoop(socket, columnCount, valueWriters);
        } else {
            doQuerySuffix(socket, columnCount, valueWriters);
        }
    }

    private void onQueryPrefix(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryPrefix(socket)) {
            doQueryMetadata(socket, columnCount);
            doQueryMetadataSuffix(socket);
        }
        doFirstRecordLoop(socket, columnCount, valueWriters);
    }

    private void doNextRecordLoop(
            HttpChunkedResponseSocket socket,
            int columnCount,
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryNextRecord()) {
            doRecordFetchLoop(socket, columnCount, valueWriters);
        } else {
            doQuerySuffix(socket, columnCount, valueWriters);
        }
    }

    private boolean doQueryPrefix(HttpChunkedResponseSocket socket) {
        if (noMeta) {
            socket.bookmark();
            socket.put('{').putQuoted("dataset").put(":[");
            return false;
        }
        socket.bookmark();
        socket.put('{').putQuoted("query").put(':').encodeUtf8AndQuote(query);
        socket.put(',').putQuoted("columns").put(':').put('[');
        columnIndex = 0;
        return true;
    }

    private void doQueryMetadataSuffix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_METADATA_SUFFIX;
        socket.bookmark();
        socket.put("],\"dataset\":[");
    }

    private boolean onQuerySetupFirstRecord(
            ObjList<JsonQueryProcessor.ValueWriter> valueWriters
    ) {
        if (skip > 0) {
            final RecordCursor cursor = this.cursor;
            long target = skip + 1;
            while (target > 0 && cursor.hasNext()) {
                target--;
            }
            if (target > 0) {
                return false;
            }
            count = skip;
        } else {
            if (!cursor.hasNext()) {
                return false;
            }
        }

        columnIndex = 0;
        record = cursor.getRecord();
        this.valueWriters.clear();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            this.valueWriters.add(valueWriters.getQuick(metadata.getColumnType(i)));
        }
        return true;
    }

    private void doQueryMetadata(HttpChunkedResponseSocket socket, int columnCount) {
        queryState = QUERY_METADATA;
        for (; columnIndex < columnCount; columnIndex++) {
            socket.bookmark();
            if (columnIndex > 0) {
                socket.put(',');
            }
            socket.put('{').
                    putQuoted("name").put(':').putQuoted(metadata.getColumnName(columnIndex)).
                    put(',').
                    putQuoted("type").put(':').putQuoted(ColumnType.nameOf(metadata.getColumnType(columnIndex)));
            socket.put('}');
        }
    }

    private boolean doQueryNextRecord() {
        if (cursor.hasNext()) {
            if (count < stop) {
                return true;
            } else {
                onNoMoreData();
            }
        }
        return false;
    }

    private void onNoMoreData() {
        if (countRows) {
            // this is the tail end of the cursor
            // we don't need to read records, just round up record count
            final RecordCursor cursor = this.cursor;
            final long size = cursor.size();
            if (size < 0) {
                long count = 1;
                while (cursor.hasNext()) {
                    count++;
                }
                this.count += count;
            } else {
                this.count = size;
            }
        }
    }

    private void doQueryRecord(HttpChunkedResponseSocket socket, int columnCount) {
        queryState = QUERY_RECORD;
        for (; columnIndex < columnCount; columnIndex++) {
            socket.bookmark();
            if (columnIndex > 0) {
                socket.put(',');
            }
            final JsonQueryProcessor.ValueWriter vw = valueWriters.getQuick(columnIndex);
            if (vw != null) {
                vw.write(socket, record, columnIndex);
            }
        }
    }

    private void doQueryRecordSuffix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_RECORD_SUFFIX;
        count++;
        socket.bookmark();
        socket.put(']');
    }

    private void doQueryRecordPrefix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_RECORD_PREFIX;
        socket.bookmark();
        if (count > skip) {
            socket.put(',');
        }
        socket.put('[');
        columnIndex = 0;
    }

    @Override
    public void clear() {
        metadata = null;
        cursor = Misc.free(cursor);
        record = null;
        if (recordCursorFactory != null) {
            // todo: avoid toString()
            FACTORY_CACHE.get().put(query.toString(), recordCursorFactory);
            recordCursorFactory = null;
        }
        query.clear();
        queryState = QUERY_PREFIX;
        columnIndex = 0;
        countRows = false;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    @FunctionalInterface
    interface StateResumeAction {
        void onResume(
                HttpChunkedResponseSocket socket,
                int columnCount,
                ObjList<JsonQueryProcessor.ValueWriter> valueWriters
        ) throws PeerDisconnectedException, PeerIsSlowToReadException;
    }
}
