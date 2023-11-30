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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.Nullable;

public class LineHttpProcessorState implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LineHttpProcessorState.class);
    private final IlpWalAppender appender;
    private final StringSink error = new StringSink();
    private final IlpTudCache ilpTudCache;
    private final LineTcpParser parser;
    private final int recvBufSize;
    private final WeakClosableObjectPool<SymbolCache> symbolCachePool;
    int errorLine = -1;
    private long buffer;
    private Status currentStatus = Status.OK;
    private int fd = -1;
    private int line = 0;
    private long recvBufEnd;
    private long recvBufPos;
    private long recvBufStartOfMeasurement;
    private DirectUtf8Sequence requestId;

    public LineHttpProcessorState(int recvBufSize, CairoEngine engine, LineHttpProcessorConfiguration configuration) {
        assert recvBufSize > 0;
        this.recvBufSize = recvBufSize;
        this.recvBufPos = this.buffer = Unsafe.malloc(recvBufSize, MemoryTag.NATIVE_HTTP_CONN);
        this.recvBufEnd = this.recvBufPos + recvBufSize;
        this.parser = new LineTcpParser(configuration.isStringAsTagSupported(), configuration.isSymbolAsFieldSupported());
        this.parser.of(buffer);
        this.appender = new IlpWalAppender(configuration.autoCreateNewColumns(),
                configuration.isStringToCharCastAllowed(),
                configuration.getTimestampAdapter(),
                engine.getConfiguration().getMaxFileNameLength(),
                configuration.getMicrosecondClock()
        );
        var defaultColumnTypes = new DefaultColumnTypes(configuration.getDefaultColumnTypeForFloat(), configuration.getDefaultColumnTypeForInteger());
        this.ilpTudCache = new IlpTudCache(
                engine,
                configuration.autoCreateNewColumns(),
                configuration.autoCreateNewTables(),
                defaultColumnTypes,
                configuration.getDefaultPartitionBy()
        );
        symbolCachePool = new WeakClosableObjectPool<>(
                () -> new SymbolCache(configuration.getMicrosecondClock(), configuration.getSymbolCacheWaitUsBeforeReload()), 5);
    }

    public void clear() {
        ilpTudCache.clear();
        Vect.memset(buffer, recvBufSize, 0);
        parser.of(buffer);
        recvBufPos = buffer;
        error.clear();
        currentStatus = Status.OK;
        errorLine = 0;
        line = 0;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, recvBufSize, MemoryTag.NATIVE_HTTP_CONN);
        recvBufEnd = recvBufPos = buffer = 0;
        Misc.free(ilpTudCache);
        Misc.free(symbolCachePool);
    }

    public void commit() {
        ilpTudCache.commitAll();
    }

    public void formatError(Utf8Sink sink) {
        sink.putAscii("{\"code\":\"").putAscii(currentStatus.codeStr);
        sink.putAscii("\",\"message\":\"").putAscii("failed to parse line protocol: errors encountered on line(s):");
        sink.put(error); // TODO: escape to be valid json string
        sink.putAscii("\",\"line\":").put(errorLine).putAscii('}');
    }

    public void formatError(Utf8Sink sink, CharSequence message, @Nullable Throwable exception) {
        sink.put("{\"code\":\"internal error\",\"err\":\"");
        sink.put(message).put("\"");
        if (exception != null) {
            sink.put(",\"message\":\"");
            sink.put(exception.getMessage()); // TODO: escape to be valid json string
        }
        sink.put("\"}");
    }

    public CharSequence getError() {
        return error;
    }

    public int getHttpResponseCode() {
        return currentStatus.responseCode;
    }

    public Status getStatus() {
        return currentStatus;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public void of(int fd, DirectUtf8Sequence requestId) {
        this.fd = fd;
        this.requestId = requestId;
    }

    public void parse(long lo, long hi) {
        if (stopParse()) {
            return;
        }

        try {
            long pos = lo;
            while (pos < hi) {
                pos = copyToLocalBuffer(pos, hi);
                currentStatus = processLocalBuffer();
                if (stopParse()) {
                    return;
                }
            }
        } catch (Throwable th) {
            LOG.error().$("could not parse HTTP request [fd=").$(fd).$(", ex=").$(th).$(']').$();
            error.put(th.getMessage());
            currentStatus = Status.INTERNAL_ERROR;
        }
    }

    public void reset() {
        error.clear();
        currentStatus = Status.OK;
        recvBufPos = buffer;
    }

    private void appendMeasurement() {
        WalTableUpdateDetails tud = this.ilpTudCache.getTableUpdateDetails(AllowAllSecurityContext.INSTANCE, parser, symbolCachePool);

        try {
            appender.appendToWal(AllowAllSecurityContext.INSTANCE, parser, tud);
        } catch (Throwable e) {
            LOG.info().$("problem appending to WAL [table=").$(parser.getMeasurementName()).$(", ex=").$(e).I$();
        }
    }

    private boolean compactBuffer(long recvBufStartOfMeasurement) {
        if (recvBufStartOfMeasurement > buffer) {
            long shl = recvBufStartOfMeasurement - buffer;
            Vect.memmove(buffer, buffer + shl, recvBufPos - recvBufStartOfMeasurement);
            parser.shl(shl);
            recvBufPos -= shl;
            this.recvBufStartOfMeasurement -= shl;
            return true;
        }
        return recvBufPos < recvBufEnd;
    }

    private long copyToLocalBuffer(long lo, long hi) {
        long copyLen = Math.min(hi - lo, recvBufEnd - recvBufPos);
        assert copyLen > 0;
        Vect.memcpy(recvBufPos, lo, copyLen);
        recvBufPos = recvBufPos + copyLen;
        return lo + copyLen;
    }

    private Status processLocalBuffer() {
        Status status = Status.OK;
        while (recvBufPos > buffer) {
            try {
                LineTcpParser.ParseResult rc = parser.parseMeasurement(recvBufPos);
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        appendMeasurement();
                        line++;
                        startNewMeasurement();
                        status = Status.OK;
                        break;
                    }

                    case ERROR: {
                        return saveParseError(parser);
                    }

                    case BUFFER_UNDERFLOW: {
                        if (!compactBuffer(recvBufStartOfMeasurement)) {
                            error.put("buffer underflow [table=").put(parser.getMeasurementName()).put(", line=").put(line + 1).put("]");
                            return Status.MESSAGE_TOO_LARGE;
                        }
                        return Status.NEEDS_REED;
                    }
                }
            } catch (Throwable ex) {
                LOG.error()
                        .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                        .$(", ex=").$(ex)
                        .I$();
                return saveParseError(ex);
            }
        }
        return status;
    }

    private Status saveParseError(LineTcpParser parser) {
        errorLine = line + 1;
        int errorPos = error.length();
        error.put("\nerror parsing line ").put(errorLine);
        switch (parser.getErrorCode()) {
            case NO_FIELDS:
                error.put(": No fields were provided");
                break;
            case MISSING_FIELD_VALUE:
                error.put(": Could not parse entire line. Field value is missing: ").put(parser.getLastEntityName());
                break;
            case MISSING_TAG_VALUE:
                error.put(": Could not parse entire line. Tag value is missing: ").put(parser.getLastEntityName());
                break;
            default:
                error.put(": ").put(String.valueOf(parser.getErrorCode()));
                break;
        }

        LOG.info().$("parse error [table=").$(parser.getMeasurementName())
                .$(", line=").$(line + 1)
                .$(error.subSequence(errorPos, error.length()))
                .$(", request=").$(requestId)
                .$(", fd=").$(fd)
                .$();
        line++;
        return Status.PARSE_ERROR;
    }

    private Status saveParseError(Throwable ex) {
        error.put("parse error [table=").put(parser.getMeasurementName()).put(", line=").put(line + 1).put(", error=").put(ex.getMessage()).put("]");
        errorLine = line + 1;
        return Status.INTERNAL_ERROR;
    }

    private void startNewMeasurement() {
        parser.startNextMeasurement();
        recvBufStartOfMeasurement = parser.getBufferAddress();
        // we ran out of buffer, move to start and start parsing new data from socket
        if (recvBufStartOfMeasurement == recvBufPos) {
            recvBufPos = buffer;
            recvBufStartOfMeasurement = buffer;
            parser.of(buffer);
        }
    }

    private boolean stopParse() {
        return currentStatus != Status.OK && currentStatus != Status.NEEDS_REED;
    }

    public enum Status {
        OK(null, 204),
        NEEDS_REED("invalid", 400),
        PARSE_ERROR("invalid", 400),
        INTERNAL_ERROR("internal error", 500),
        MESSAGE_TOO_LARGE("request too large", 413);

        private final String codeStr;
        private final int responseCode;

        Status(String codeStr, int responseCode) {
            this.codeStr = codeStr;
            this.responseCode = responseCode;
        }
    }
}
