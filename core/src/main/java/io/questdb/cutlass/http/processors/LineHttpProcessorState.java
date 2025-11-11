/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cutlass.line.tcp.AdaptiveRecvBuffer;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.LineProtocolException;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.LineWalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.Zip;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cutlass.http.processors.LineHttpProcessorState.Status.ENCODING_NOT_SUPPORTED;
import static io.questdb.cutlass.http.processors.LineHttpProcessorState.Status.MESSAGE_TOO_LARGE;

public class LineHttpProcessorState implements QuietCloseable, ConnectionAware {
    private static final AtomicLong ERROR_COUNT = new AtomicLong();
    private static final String ERROR_ID = generateErrorId();
    // this field is modified via reflection from tests, via LogFactory.enableGuaranteedLogging
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(LineHttpProcessorState.class);
    private final LineWalAppender appender;
    private final StringSink error = new StringSink();
    private final LineHttpTudCache ilpTudCache;
    private final boolean logMessageOnError;
    private final int maxResponseErrorMessageLength;
    private final LineTcpParser parser;
    private final AdaptiveRecvBuffer recvBuffer;
    private final DirectUtf8Sink utf8Sink = new DirectUtf8Sink(16);
    private final WeakClosableObjectPool<SymbolCache> symbolCachePool;
    int errorLine = -1;
    private Status currentStatus = Status.OK;
    private long errorId;
    private long fd = -1;
    private long inflateStream;
    private boolean isGzipEncoded;
    private int line = 0;
    private SecurityContext securityContext;
    private SendStatus sendStatus = SendStatus.NONE;

    public LineHttpProcessorState(
            int initRecvBufSize,
            int maxResponseContentLength,
            CairoEngine engine,
            LineHttpProcessorConfiguration configuration
    ) {
        assert initRecvBufSize > 0;
        // Response is measured in bytes some error messages can have non-ascii characters
        // approximate 1.5 bytes per character
        this.maxResponseErrorMessageLength = (int) ((maxResponseContentLength - 100) / 1.5);
        this.parser = new LineTcpParser();
        recvBuffer = new AdaptiveRecvBuffer(parser, MemoryTag.NATIVE_HTTP_CONN)
                .of(initRecvBufSize, configuration.getMaxRecvBufferSize());
        this.appender = new LineWalAppender(
                configuration.autoCreateNewColumns(),
                configuration.isStringToCharCastAllowed(),
                configuration.getTimestampUnit(),
                utf8Sink,
                engine.getConfiguration().getMaxFileNameLength()
        );
        final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(configuration);
        this.ilpTudCache = new LineHttpTudCache(
                engine,
                configuration.autoCreateNewColumns(),
                configuration.autoCreateNewTables(),
                defaultColumnTypes,
                configuration.getDefaultPartitionBy()
        );
        this.symbolCachePool = new WeakClosableObjectPool<>(
                () -> new SymbolCache(configuration.getMicrosecondClock(), configuration.getSymbolCacheWaitUsBeforeReload()),
                5
        );
        this.logMessageOnError = configuration.logMessageOnError();
    }

    public void cleanupGzip() {
        if (inflateStream != 0) {
            Zip.inflateEnd(inflateStream);
            inflateStream = 0;
        }
    }

    public void clear() {
        ilpTudCache.clear();
        recvBuffer.clear();
        error.clear();
        currentStatus = Status.OK;
        errorLine = 0;
        line = 0;
        sendStatus = SendStatus.NONE;
        cleanupGzip();
    }

    @Override
    public void close() {
        Misc.free(recvBuffer);
        Misc.free(ilpTudCache);
        Misc.free(symbolCachePool);
        Misc.free(parser);
        Misc.free(utf8Sink);
        cleanupGzip();
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
        sink.escapeJsonStr(error, 0, Math.min(error.length(), maxResponseErrorMessageLength));
        if (errorLine > -1) {
            sink.putAscii("\",\"line\":").put(errorLine);
        } else {
            sink.putQuote();
        }
        sink.putAscii(",\"errorId\":\"").putAscii(ERROR_ID).put('-').put(errorId).putAscii("\"").putAscii('}');
    }

    public int getHttpResponseCode() {
        return currentStatus.responseCode;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void inflateAndParse(long lo, long hi) {
        if (stopParse()) {
            return;
        }

        Zip.setInput(inflateStream, lo, (int) (hi - lo));

        long pp = recvBuffer.getBufPos();
        while (Zip.availIn(inflateStream) > 0 && !stopParse()) {
            long p = recvBuffer.getBufPos();
            int len = (int) (recvBuffer.getBufEnd() - p);
            int ret = Zip.inflate(inflateStream, p, len, false);
            int newBytes = len - Zip.availOut(inflateStream);
            if (newBytes > 0) {
                recvBuffer.setBufPos(p + newBytes);
            }

            if (ret < 0) {
                if (ret != Zip.Z_BUF_ERROR) {
                    reject(ENCODING_NOT_SUPPORTED, "gzip decompression error", fd);
                    cleanupGzip();
                    return;
                }

                // inflate can return Z_BUF_ERROR after writing bytes once the recv buffer runs out of space
                if (newBytes > 0) {
                    currentStatus = processLocalBuffer();
                    pp = recvBuffer.getBufPos();
                    continue;
                }

                reject(MESSAGE_TOO_LARGE, "server buffer is too small", fd);
                cleanupGzip();
                return;
            }

            if (newBytes > 0) {
                currentStatus = processLocalBuffer();
                pp = recvBuffer.getBufPos();
            }

            if (ret == Zip.Z_STREAM_END) {
                cleanupGzip();
                break;
            }
        }

        if (recvBuffer.getBufPos() > pp) {
            currentStatus = processLocalBuffer();
        }
    }

    public boolean isGzipEncoded() {
        return isGzipEncoded;
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public void of(long fd, byte timestampPrecision, SecurityContext securityContext) {
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
            long recvBufPos = recvBuffer.getBufPos();
            assert recvBufPos < recvBuffer.getBufEnd();
            Unsafe.getUnsafe().putByte(recvBufPos, (byte) '\n');
            recvBuffer.setBufPos(recvBufPos + 1);
            currentStatus = processLocalBuffer();
            if (currentStatus == Status.NEEDS_READ) {
                // added \n and parse result is still NEEDS_READ, means there was nothing in this line, e.g.
                // blank space
                currentStatus = Status.OK;
            }
        }
        recvBuffer.tryToShrinkRecvBuffer(false);
    }

    public void parse(long lo, long hi) {
        if (stopParse()) {
            return;
        }

        long pos = lo;
        while (pos < hi) {
            pos = recvBuffer.copyToLocalBuffer(pos, hi);
            currentStatus = processLocalBuffer();
            if (stopParse()) {
                return;
            }
        }
    }

    public void reject(Status status, String errorText, long fd) {
        currentStatus = status;
        error.put(errorText);
        this.fd = fd;
        logError();
    }

    public void setGzipEncoded(boolean gzipEncoded) {
        isGzipEncoded = gzipEncoded;
    }

    public void setInflateStream(long streamAddr) {
        assert this.inflateStream == 0;
        this.inflateStream = streamAddr;
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

    private long getErrorLogLineHi(LineTcpParser parser) {
        return Math.min(parser.getBufferAddress() + 1, recvBuffer.getBufPos());
    }

    private Status handleCommitError(Throwable ex) {
        errorId = ERROR_COUNT.incrementAndGet();
        errorLine = -1;

        final Status status;
        final LogRecord errorRec;
        error.put("commit error for table: ").put(parser.getMeasurementName());
        if (ex instanceof CairoException exception) {
            error.put(", errno: ").put(exception.getErrno()).put(", error: ").put(exception.getFlyweightMessage());
            if (exception.isAuthorizationError()) {
                errorRec = LOG.error();
                status = Status.SECURITY_ERROR;
            } else {
                errorRec = LOG.critical();
                status = Status.INTERNAL_ERROR;
            }
        } else {
            error.put(", error: ").put(ex.getClass().getCanonicalName());
            errorRec = LOG.critical();
            status = Status.INTERNAL_ERROR;
        }

        errorRec.$('[').$(fd).$("] could not commit [table=").$(parser.getMeasurementName())
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", ex=").$safe(ex.getMessage())
                .I$();
        return status;
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
        final LogRecord errorRec = ex.isCritical() ? LOG.critical() : LOG.error();
        errorRec
                .$('[').$(fd).$("] could not process line data 4 [table=").$(parser.getMeasurementName())
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", errno=").$(ex.getErrno());
        if (logMessageOnError) {
            errorRec.$(", mangledLine=`").$safe(recvBuffer.getBufStartOfMeasurement(), getErrorLogLineHi(parser)).$('`');
        }
        errorRec.$(", ex=").$(ex.getFlyweightMessage()).I$();

        error.put("write error: ").put(parser.getMeasurementName())
                .put(", errno: ").put(ex.getErrno())
                .put(", error: ").put(ex.getFlyweightMessage());
        errorLine = line + 1;
        return ex.isAuthorizationError() ? Status.SECURITY_ERROR : Status.INTERNAL_ERROR;
    }

    private Status handleUnknownParseError(Throwable ex) {
        errorId = ERROR_COUNT.incrementAndGet();
        final LogRecord errorRec = LOG.critical()
                .$('[').$(fd).$("] could not process line data 3 [table=").$(parser.getMeasurementName())
                .$(", errorId=").$(ERROR_ID).$('-').$(errorId);
        if (logMessageOnError) {
            errorRec.$(", mangledLine=`").$safe(recvBuffer.getBufStartOfMeasurement(), getErrorLogLineHi(parser)).$('`');
        }
        errorRec.$(", ex=").$safe(ex.getMessage()).I$();

        error.put("write error: ").put(parser.getMeasurementName())
                .put(", error: ").put(ex.getClass().getCanonicalName());
        errorLine = line + 1;
        return Status.INTERNAL_ERROR;
    }

    private void logError(LineTcpParser parser, int errorPos) {
        logError(parser, errorPos, false);
    }

    private void logError(LineTcpParser parser, int errorPos, boolean isError) {
        errorId = ERROR_COUNT.incrementAndGet();
        final LogRecord errorRec = isError ? LOG.error() : LOG.info();
        errorRec.$("parse error [errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", table=").$safe(parser.getMeasurementName())
                .$(", line=").$(errorLine)
                .$(", error=").$safe(error.subSequence(errorPos, error.length()))
                .$(", fd=").$(fd);
        if (logMessageOnError) {
            errorRec.$(", mangledLine=`").$safe(recvBuffer.getBufStartOfMeasurement(), parser.getBufferAddress()).$('`');
        }
        errorRec.I$();
    }

    private void logError() {
        errorId = ERROR_COUNT.incrementAndGet();
        LOG.error().$("parse error [errorId=").$(ERROR_ID).$('-').$(errorId)
                .$(", error=").$safe(error)
                .$(", fd=").$(fd)
                .I$();
    }

    private Status processLocalBuffer() {
        Status status = Status.OK;
        while (recvBuffer.getBufPos() > recvBuffer.getBufStart()) {
            try {
                LineTcpParser.ParseResult rc = parser.parseMeasurement(recvBuffer.getBufPos());
                switch (rc) {
                    case MEASUREMENT_COMPLETE: {
                        if ((status = appendMeasurement()) != Status.OK) {
                            return status;
                        }
                        line++;
                        recvBuffer.startNewMeasurement();
                        break;
                    }

                    case ERROR: {
                        return handleLineError(parser);
                    }

                    case BUFFER_UNDERFLOW: {
                        if (!recvBuffer.tryCompactOrGrowBuffer()) {
                            errorLine = ++line;
                            int errorPos = error.length();
                            error.putAscii("transaction is too large, either flush more frequently or increase buffer size \"line.http.max.recv.buffer.size\" [maxBufferSize=")
                                    .putSize(recvBuffer.getMaxBufSize())
                                    .putAscii(']');
                            logError(parser, errorPos, true);
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
        COMMITTED(null, 204),
        NOT_ACCEPTING_WRITES("not accepting writes", 421);

        private final String codeStr;
        private final int responseCode;

        Status(String codeStr, int responseCode) {
            this.codeStr = codeStr;
            this.responseCode = responseCode;
        }
    }
}
