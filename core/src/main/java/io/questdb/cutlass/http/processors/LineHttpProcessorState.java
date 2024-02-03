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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class LineHttpProcessorState implements QuietCloseable, ConnectionAware {
    private static final AtomicLong ERROR_COUNT = new AtomicLong();
    private static final String ERROR_ID = generateErrorId();
    private static final Log LOG = LogFactory.getLog(LineHttpProcessorState.class);
    private final LineWalAppender appender;
    private final StringSink error = new StringSink();
    private final LineHttpTudCache ilpTudCache;
    private final int maxResponseErrorMessageLength;
    private final LineTcpParser parser;
    private final int recvBufSize;
    private final WeakClosableObjectPool<SymbolCache> symbolCachePool;
    int errorLine = -1;
    private long buffer;
    private Status currentStatus = Status.OK;
    private long errorId;
    private int fd = -1;
    private int line = 0;
    private long recvBufEnd;
    private long recvBufPos;
    private long recvBufStartOfMeasurement;
    private SecurityContext securityContext;
    private SendStatus sendStatus = SendStatus.NONE;

    public LineHttpProcessorState(int recvBufSize, int maxResponseContentLength, CairoEngine engine, LineHttpProcessorConfiguration configuration) {
        assert recvBufSize > 0;
        this.recvBufSize = recvBufSize;

        // Response is measured in bytes some error messages can have non-ascii characters
        // approximate 1.5 bytes per character
        this.maxResponseErrorMessageLength = (int) ((maxResponseContentLength - 100) / 1.5);
        this.recvBufPos = this.buffer = Unsafe.malloc(recvBufSize, MemoryTag.NATIVE_HTTP_CONN);
        this.recvBufEnd = this.recvBufPos + recvBufSize;
        this.parser = new LineTcpParser(configuration.isStringAsTagSupported(), configuration.isSymbolAsFieldSupported());
        this.parser.of(buffer);
        this.appender = new LineWalAppender(
                configuration.autoCreateNewColumns(),
                configuration.isStringToCharCastAllowed(),
                configuration.getTimestampAdapter(),
                engine.getConfiguration().getMaxFileNameLength(),
                configuration.getMicrosecondClock()
        );
        DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(configuration.getDefaultColumnTypeForFloat(), configuration.getDefaultColumnTypeForInteger());
        this.ilpTudCache = new LineHttpTudCache(
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
        recvBufStartOfMeasurement = 0;
        sendStatus = SendStatus.NONE;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, recvBufSize, MemoryTag.NATIVE_HTTP_CONN);
        recvBufStartOfMeasurement = recvBufEnd = recvBufPos = buffer = 0;
        Misc.free(ilpTudCache);
        Misc.free(symbolCachePool);
    }

    public void commit() {
        try {
            ilpTudCache.commitAll();
        } catch (Throwable th) {
            ilpTudCache.setDistressed();
            currentStatus = handleCommitError(th);
        }
    }

    public void formatError(Utf8Sink sink) {
        sink.putAscii("{\"code\":\"").putAscii(currentStatus.codeStr);
        sink.putAscii("\",\"message\":\"");
        if (errorLine > -1) {
            sink.putAscii("failed to parse line protocol:");
            sink.putAscii("errors encountered on line(s):");
        }
        sink.put(error, 0, Math.min(error.length(), maxResponseErrorMessageLength));
        if (errorLine > -1) {
            sink.putAscii("\",\"line\":").put(errorLine);
        } else {
            sink.putAscii('\"');
        }
        sink.putAscii(",\"errorId\":\"").putAscii(ERROR_ID).put('-').put(errorId).putAscii("\"").putAscii('}');
    }

    public int getHttpResponseCode() {
        return currentStatus.responseCode;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public void of(int fd, byte timestampPrecision, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
        this.appender.setTimestampAdapter(timestampPrecision);
    }

    @Override
    public void onDisconnected() {
        clear();
        ilpTudCache.reset();
    }

    public void onMessageComplete() {
        if (currentStatus == Status.NEEDS_READ) {
            // Last line did not have \n as a last character
            // this is allowed by the protocol, no error in Influx
            // NEEDS_REED status means that there is still a buffer space to read to.
            assert recvBufPos < recvBufEnd;
            Unsafe.getUnsafe().putByte(recvBufPos++, (byte) '\n');
            currentStatus = processLocalBuffer();
        }
    }

    public void parse(long lo, long hi) {
        if (stopParse()) {
            return;
        }

        long pos = lo;
        while (pos < hi) {
            pos = copyToLocalBuffer(pos, hi);
            currentStatus = processLocalBuffer();
            if (stopParse()) {
                return;
            }
        }
    }

    public void reject(Status status, String errorText, int fd) {
        currentStatus = status;
        error.put(errorText);
        this.fd = fd;
        logError();
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    private static String generateErrorId() {
        return UUID.randomUUID().toString().substring(24, 36);
    }

    private Status appendMeasurement() throws LineHttpTudCache.TableCreateException {
        WalTableUpdateDetails tud = this.ilpTudCache.getTableUpdateDetails(securityContext, parser, symbolCachePool);
        try {
            appender.appendToWal(securityContext, parser, tud);
            return Status.OK;
        } catch (LineProtocolException e) {
            errorLine = ++line;
            int errorStartPos = error.length();
            error.put("\nerror in line ").put(errorLine).put(": ");
            error.put(e.getFlyweightMessage());
            logError(parser, errorStartPos);
            return Status.APPEND_ERROR;
        } catch (CommitFailedException ex) {
            if (ex.isTableDropped()) {
                tud.setIsDropped();
                return Status.OK;
            } else {
                ilpTudCache.setDistressed();
                return handleCommitError(ex.getReason());
            }
        } catch (CairoException e) {
            if (e.isTableDropped()) {
                tud.setIsDropped();
                return Status.OK;
            }
            ilpTudCache.setDistressed();
            throw e;
        } catch (Throwable th) {
            ilpTudCache.setDistressed();
            throw th;
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

    private long getErrorLogLineHi(LineTcpParser parser) {
        return Math.min(parser.getBufferAddress() + 1, recvBufPos);
    }

    private Status handleCommitError(Throwable ex) {
        errorId = ERROR_COUNT.incrementAndGet();
        errorLine = -1;
        LOG.critical()
                .$('[').$(fd).$("] could not commit [table=").$(parser.getMeasurementName())
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", ex=").$(ex.getMessage())
                .I$();

        error.put("commit error for table: ").put(parser.getMeasurementName());
        if (ex instanceof CairoException) {
            CairoException exception = (CairoException) ex;
            error.put(", errno: ").put(exception.getErrno()).put(", error: ").put(exception.getFlyweightMessage());
            return exception.isAuthorizationError() ? Status.SECURITY_ERROR : Status.INTERNAL_ERROR;
        } else {
            error.put(", error: ").put(ex.getClass().getCanonicalName());
            return Status.INTERNAL_ERROR;
        }
    }

    private Status handleLineError(LineTcpParser parser) {
        errorLine = ++line;
        int errorPos = error.length();
        error.put("\nerror in line ").put(errorLine);
        switch (parser.getErrorCode()) {
            case NO_FIELDS:
                error.put(": No fields were provided");
                break;
            case MISSING_FIELD_VALUE:
                error.put(": Could not parse entire line. Field value is missing: ").put(parser.getLastEntityName());
                break;
            case MISSING_TAG_VALUE:
                error.put(": Could not parse entire line. Symbol value is missing: ").put(parser.getLastEntityName());
                break;
            case INVALID_TIMESTAMP:
                error.put(": Could not parse timestamp: ").put(parser.getErrorTimestampValue());
                break;
            case INVALID_FIELD_VALUE:
                error.put(": Could not parse entire line, field value is invalid. Field: ")
                        .put(parser.getLastEntityName()).put("; value: ").put(parser.getErrorFieldValue());
                break;
            case INVALID_TAG_VALUE:
                error.put(": Could not parse entire line, tag value is invalid. Tag: ")
                        .put(parser.getLastEntityName()).put("; value: ").put(parser.getErrorFieldValue());
                break;
            case INVALID_COLUMN_NAME:
                error.put(": table: ").put(parser.getMeasurementName()).put("; invalid column name: ")
                        .put(parser.getLastEntityName());
                break;
            default:
                error.put(": ").put(String.valueOf(parser.getErrorCode()));
                break;
        }
        logError(parser, errorPos);
        return Status.PARSE_ERROR;
    }

    private Status handleLineError(LineTcpParser parser, LineHttpTudCache.TableCreateException ex) {
        errorLine = ++line;
        int errorPos = error.length();
        error.put("\nerror in line ").put(errorLine);
        error.put(": table: ").put(parser.getMeasurementName());
        if (ex.getMsg() != null) {
            error.put("; ").put(ex.getMsg());
        }
        if (ex.getToken() != null) {
            error.put(": ").put(ex.getToken());
        }
        logError(parser, errorPos);
        return Status.PARSE_ERROR;
    }

    private Status handleLineError(LineTcpParser parser, CairoException ex) {
        errorId = ERROR_COUNT.incrementAndGet();
        LogRecord error = ex.isCritical() ? LOG.critical() : LOG.error();
        error
                .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", errno=").$(ex.getErrno())
                .$(", mangledLine=`").$utf8(recvBufStartOfMeasurement == 0 ? buffer : recvBufStartOfMeasurement, getErrorLogLineHi(parser)).$('`')
                .$(", ex=").$(ex.getFlyweightMessage())
                .I$();

        this.error.put("write error: ").put(parser.getMeasurementName())
                .put(", errno: ").put(ex.getErrno())
                .put(", error: ").put(ex.getFlyweightMessage());
        errorLine = line + 1;
        return ex.isAuthorizationError() ? Status.SECURITY_ERROR : Status.INTERNAL_ERROR;
    }

    private Status handleUnknownParseError(Throwable ex) {
        errorId = ERROR_COUNT.incrementAndGet();
        LOG.critical()
                .$('[').$(fd).$("] could not process line data [table=").$(parser.getMeasurementName())
                .$(", mangledLine=`").$utf8(recvBufStartOfMeasurement == 0 ? buffer : recvBufStartOfMeasurement, getErrorLogLineHi(parser)).$('`')
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", ex=").$(ex.getMessage())
                .I$();

        this.error.put("write error: ").put(parser.getMeasurementName())
                .put(", error: ").put(ex.getClass().getCanonicalName());
        errorLine = line + 1;
        return Status.INTERNAL_ERROR;
    }

    private void logError(LineTcpParser parser, int errorPos) {
        errorId = ERROR_COUNT.incrementAndGet();
        LOG.info().$("parse error [errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", table=").$(parser.getMeasurementName())
                .$(", line=").$(errorLine)
                .$(", error=").$(error.subSequence(errorPos, error.length()))
                .$(", fd=").$(fd)
                .$(", mangledLine=`").$utf8(recvBufStartOfMeasurement == 0 ? buffer : recvBufStartOfMeasurement, parser.getBufferAddress()).$('`')
                .I$();
    }

    private void logError() {
        errorId = ERROR_COUNT.incrementAndGet();
        LOG.info().$("parse error [errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", error=").$(error)
                .$(", fd=").$(fd)
                .I$();
    }

    private Status processLocalBuffer() {
        Status status = Status.OK;
        while (recvBufPos > buffer) {
            try {
                LineTcpParser.ParseResult rc = parser.parseMeasurement(recvBufPos);
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        if ((status = appendMeasurement()) != Status.OK) {
                            return status;
                        }
                        line++;
                        startNewMeasurement();
                        break;
                    }

                    case ERROR: {
                        return handleLineError(parser);
                    }

                    case BUFFER_UNDERFLOW: {
                        if (!compactBuffer(recvBufStartOfMeasurement)) {
                            errorLine = ++line;
                            error.put("unable to read data: ILP line does not fit QuestDB ILP buffer size");
                            return Status.MESSAGE_TOO_LARGE;
                        }
                        return Status.NEEDS_READ;
                    }
                }
            } catch (LineHttpTudCache.TableCreateException parseException) {
                return handleLineError(parser, parseException);
            } catch (CairoException parseException) {
                return handleLineError(parser, parseException);
            } catch (Throwable ex) {
                return handleUnknownParseError(ex);
            }
        }
        return status;
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
        return currentStatus != Status.OK && currentStatus != Status.NEEDS_READ;
    }

    public enum Status {
        OK(null, 204),
        ENCODING_NOT_SUPPORTED("not supported", 415),
        PRECISION_NOT_SUPPORTED("not supported", 400),
        NEEDS_READ("invalid", 400),
        PARSE_ERROR("invalid", 400),
        APPEND_ERROR("invalid", 400),
        METHOD_NOT_SUPPORTED("invalid", 404),
        SECURITY_ERROR("unauthorised", 403),
        INTERNAL_ERROR("internal error", 500),
        MESSAGE_TOO_LARGE("request too large", 413),
        COLUMN_ADD_ERROR("invalid", 400),
        COMMITTED(null, 204);

        private final String codeStr;
        private final int responseCode;

        Status(String codeStr, int responseCode) {
            this.codeStr = codeStr;
            this.responseCode = responseCode;
        }
    }
}
