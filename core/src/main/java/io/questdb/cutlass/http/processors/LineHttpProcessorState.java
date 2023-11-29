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
import io.questdb.std.str.StringSink;

public class LineHttpProcessorState implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(LineHttpProcessorState.class);
    private final IlpWalAppender appender;
    private final StringSink error = new StringSink();
    private final IlpTudCache ilpTudCache;
    private final LineTcpParser parser;
    private final int recvBufSize;
    private final WeakClosableObjectPool<SymbolCache> symbolCachePool;
    private long buffer;
    private Status currentStatus = Status.OK;
    private int fd = -1;
    private int line = 0;
    private long recvBufEnd;
    private long recvBufPos;
    private long recvBufStartOfMeasurement;

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
        Vect.memset(buffer, recvBufSize, 0);
        recvBufPos = buffer;
        error.clear();
        currentStatus = Status.OK;
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

    public CharSequence getError() {
        return error;
    }

    public Status getStatus() {
        return currentStatus;
    }

    public void of(int fd) {
        this.fd = fd;
    }

    public Status parse(long lo, long hi) {
        if (currentStatus == Status.ERROR) {
            return currentStatus;
        }

        try {
            long pos = lo;
            while (pos < hi) {
                pos = copyToLocalBuffer(pos, hi);
                currentStatus = processLocalBuffer();
                if (currentStatus == Status.ERROR) {
                    return currentStatus;
                }
            }
            return currentStatus;
        } catch (Throwable th) {
            LOG.error().$("could not parse HTTP request [fd=").$(fd).$(", ex=").$(th).$(']').$();
            error.put(th.getMessage());
            currentStatus = Status.ERROR;
            return currentStatus;
        }
    }

    public void reset() {
        error.clear();
        currentStatus = Status.OK;
        recvBufPos = buffer;
    }

    private void appendMeasurement() {
//        LOG.info().$("parsed measurement [table=").$(parser.getMeasurementName()).$(", fd=").$(fd).I$();
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

//            LOG.info().$("compacting buffer [fd=").$(fd)
//                    .$(", shl=").$(shl)
//                    .$(", recvBufPos=").$(recvBufPos)
//                    .$(", buffer=").$(buffer)
//                    .$(", fd=").$(fd).$(']')
//                    .I$();

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
                        saveParseError();
                        return Status.ERROR;
                    }

                    case BUFFER_UNDERFLOW: {
                        if (!compactBuffer(recvBufStartOfMeasurement)) {
                            error.put("buffer underflow [table=").put(parser.getMeasurementName()).put(", line=").put(line + 1).put("]");
                            return Status.ERROR;
                        }
                        return Status.NEEDS_REED;
                    }
                }
            } catch (Throwable ex) {
                LOG.error()
                        .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                        .$(", ex=").$(ex)
                        .I$();
                saveParseError(ex);
                return Status.ERROR;
            }
        }
        return status;
    }

    private void saveParseError() {
        error.put("parse error [table=").put(parser.getMeasurementName()).put(", line=").put(line + 1).put("]");
        LOG.info().$("parse error [table=").$(parser.getMeasurementName()).$(", line=").$(line + 1).$(']').$();
    }

    private void saveParseError(Throwable ex) {
        error.put("parse error [table=").put(parser.getMeasurementName()).put(", line=").put(line + 1).put(", error=").put(ex.getMessage()).put("]");
    }

    private void startNewMeasurement() {
        parser.startNextMeasurement();
        recvBufStartOfMeasurement = parser.getBufferAddress();
        // we ran out of buffer, move to start and start parsing new data from socket
        if (recvBufStartOfMeasurement == recvBufPos) {
            recvBufPos = buffer;
            recvBufStartOfMeasurement = buffer;
            parser.of(buffer);
//            LOG.info().$("start of line at the end of the buffer, resetting the buffer [fd=").$(fd).I$();
        }
    }

    public enum Status {
        OK,
        ERROR,
        NEEDS_REED
    }
}
