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

import io.questdb.cairo.*;
import io.questdb.cutlass.http.ConnectionAware;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.http.ilpv4.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * State management for ILP v4 HTTP processing.
 */
public class IlpV4HttpProcessorState implements QuietCloseable, ConnectionAware {
    private static final AtomicLong ERROR_COUNT = new AtomicLong();
    private static final String ERROR_ID = generateErrorId();
    private static final Log LOG = LogFactory.getLog(IlpV4HttpProcessorState.class);

    private final IlpV4StreamingDecoder streamingDecoder;
    private final IlpV4WalAppender walAppender;
    private final StringSink error = new StringSink();
    private final IlpV4HttpTudCache tudCache;
    private final int maxResponseErrorMessageLength;
    private final IlpV4SchemaCache schemaCache;

    // Buffer to accumulate incoming data
    private long bufferAddress;
    private int bufferSize;
    private int bufferPosition;

    private Status currentStatus = Status.OK;
    private long errorId;
    private long fd = -1;
    private SecurityContext securityContext;
    private SendStatus sendStatus = SendStatus.NONE;

    public IlpV4HttpProcessorState(
            int initBufferSize,
            int maxResponseContentLength,
            CairoEngine engine,
            LineHttpProcessorConfiguration configuration
    ) {
        assert initBufferSize > 0;
        this.maxResponseErrorMessageLength = (int) ((maxResponseContentLength - 100) / 1.5);
        this.schemaCache = new IlpV4SchemaCache();
        this.streamingDecoder = new IlpV4StreamingDecoder(schemaCache);
        this.walAppender = new IlpV4WalAppender(
                configuration.autoCreateNewColumns(),
                engine.getConfiguration().getMaxFileNameLength()
        );

        final DefaultColumnTypes defaultColumnTypes = new DefaultColumnTypes(configuration);
        this.tudCache = new IlpV4HttpTudCache(
                engine,
                configuration.autoCreateNewColumns(),
                configuration.autoCreateNewTables(),
                defaultColumnTypes,
                configuration.getDefaultPartitionBy()
        );

        this.bufferSize = initBufferSize;
        this.bufferAddress = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.bufferPosition = 0;
    }

    public void clear() {
        tudCache.clear();
        error.clear();
        currentStatus = Status.OK;
        bufferPosition = 0;
        sendStatus = SendStatus.NONE;
        streamingDecoder.reset();
    }

    @Override
    public void close() {
        Misc.free(tudCache);
        Misc.free(streamingDecoder);
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferAddress = 0;
        }
    }

    public void of(long fd, SecurityContext securityContext) {
        this.fd = fd;
        this.securityContext = securityContext;
    }

    @Override
    public void onDisconnected() {
        clear();
        tudCache.reset();
    }

    public void addData(long lo, long hi) {
        int len = (int) (hi - lo);
        ensureCapacity(bufferPosition + len);
        Unsafe.getUnsafe().copyMemory(lo, bufferAddress + bufferPosition, len);
        bufferPosition += len;
    }

    public void processMessage() {
        if (bufferPosition == 0) {
            return;
        }

        try {
            // Decode using streaming decoder (zero-allocation after warmup)
            IlpV4MessageCursor messageCursor = streamingDecoder.decode(bufferAddress, bufferPosition);

            // Process each table block using streaming cursors
            while (messageCursor.hasNextTable()) {
                IlpV4TableBlockCursor tableBlock = messageCursor.nextTable();

                WalTableUpdateDetails tud = tudCache.getTableUpdateDetails(securityContext, tableBlock.getTableNameUtf8(), tableBlock.getSchema());
                if (tud == null) {
                    reject(Status.INTERNAL_ERROR, "failed to create table update details for: " + tableBlock.getTableName(), fd);
                    return;
                }

                walAppender.appendToWalStreaming(securityContext, tableBlock, tud);
            }

        } catch (IlpV4ParseException e) {
            LOG.error().$('[').$(fd).$("] ILP v4 parse error: ").$(e.getMessage()).$();
            reject(Status.PARSE_ERROR, e.getMessage(), fd);
        } catch (CommitFailedException e) {
            LOG.error().$('[').$(fd).$("] commit failed: ").$(e.getMessage()).$();
            tudCache.setDistressed();
            reject(Status.INTERNAL_ERROR, "commit failed: " + e.getMessage(), fd);
        } catch (CairoException e) {
            LOG.error().$('[').$(fd).$("] cairo error: ").$(e.getFlyweightMessage()).$();
            if (e.isAuthorizationError()) {
                reject(Status.SECURITY_ERROR, e.getFlyweightMessage().toString(), fd);
            } else {
                tudCache.setDistressed();
                reject(Status.INTERNAL_ERROR, e.getFlyweightMessage().toString(), fd);
            }
        } catch (Throwable e) {
            LOG.critical().$('[').$(fd).$("] unexpected error: ").$(e).$();
            tudCache.setDistressed();
            reject(Status.INTERNAL_ERROR, "unexpected error: " + e.getMessage(), fd);
        }
    }

    public void commit() {
        try {
            tudCache.commitAll();
        } catch (Throwable th) {
            tudCache.setDistressed();
            currentStatus = Status.INTERNAL_ERROR;
            errorId = ERROR_COUNT.incrementAndGet();
            error.put("commit error: ").put(th.getMessage());
            LOG.error().$('[').$(fd).$("] commit error: ").$(th).$();
        }
    }

    public void reject(Status status, String errorText, long fd) {
        currentStatus = status;
        error.put(errorText);
        errorId = ERROR_COUNT.incrementAndGet();
        this.fd = fd;
        LOG.error().$('[').$(fd).$("] rejected [status=").$(status).$(", error=").$(errorText).$(']').$();
    }

    public boolean isOk() {
        return currentStatus == Status.OK;
    }

    public int getHttpResponseCode() {
        return currentStatus.responseCode;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public void formatError(Utf8Sink sink) {
        sink.putAscii("{\"code\":\"").putAscii(currentStatus.codeStr);
        sink.putAscii("\",\"message\":\"");
        sink.escapeJsonStr(error, 0, Math.min(error.length(), maxResponseErrorMessageLength));
        sink.putQuote();
        sink.putAscii(",\"errorId\":\"").putAscii(ERROR_ID).put('-').put(errorId).putAscii("\"").putAscii('}');
    }

    private void ensureCapacity(int required) {
        if (required > bufferSize) {
            int newSize = Math.max(bufferSize * 2, required);
            long newAddress = Unsafe.realloc(bufferAddress, bufferSize, newSize, MemoryTag.NATIVE_HTTP_CONN);
            bufferAddress = newAddress;
            bufferSize = newSize;
        }
    }

    private static String generateErrorId() {
        return UUID.randomUUID().toString().substring(24, 36);
    }

    public enum Status {
        OK(null, 204),
        PARSE_ERROR("invalid", 400),
        SECURITY_ERROR("unauthorised", 403),
        INTERNAL_ERROR("internal error", 500),
        NOT_ACCEPTING_WRITES("not accepting writes", 421);

        private final String codeStr;
        private final int responseCode;

        Status(String codeStr, int responseCode) {
            this.codeStr = codeStr;
            this.responseCode = responseCode;
        }
    }
}
