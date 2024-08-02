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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_PRINT_FORMAT;

/**
 * Useful PostgreSQL documentation links:<br>
 * <a href="https://www.postgresql.org/docs/current/protocol-flow.html">Wire protocol</a><br>
 * <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message formats</a>
 */
public class PGConnectionContext extends IOContext<PGConnectionContext> implements WriterSource, OptionsListener {

    public static final byte STATUS_IDLE = 'I';
    public static final byte STATUS_IN_ERROR = 'E';
    public static final byte STATUS_IN_TRANSACTION = 'T';
    public static final String TAG_ALTER_ROLE = "ALTER ROLE";
    public static final String TAG_BEGIN = "BEGIN";
    public static final String TAG_COMMIT = "COMMIT";
    public static final String TAG_CREATE_ROLE = "CREATE ROLE";
    // create as select tag
    public static final String TAG_CTAS = "CTAS";
    public static final String TAG_DEALLOCATE = "DEALLOCATE";
    public static final String TAG_EXPLAIN = "EXPLAIN";
    public static final String TAG_INSERT = "INSERT";
    public static final String TAG_INSERT_AS_SELECT = "TAG_INSERT_AS_SELECT";
    public static final String TAG_OK = "OK";
    public static final String TAG_PSEUDO_SELECT = "PSEUDO_SELECT";
    public static final String TAG_ROLLBACK = "ROLLBACK";
    public static final String TAG_SELECT = "SELECT";
    public static final String TAG_SET = "SET";
    public static final String TAG_UPDATE = "UPDATE";
    static final int ERROR_TRANSACTION = 3;
    static final int INT_BYTES_X = Numbers.bswap(Integer.BYTES);
    static final int INT_NULL_X = Numbers.bswap(-1);
    static final int IN_TRANSACTION = 1;
    static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';
    static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    static final byte MESSAGE_TYPE_EMPTY_QUERY = 'I';
    static final byte MESSAGE_TYPE_NO_DATA = 'n';
    static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';
    static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';
    static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    static final int NO_TRANSACTION = 0;
    private static final int COMMIT_TRANSACTION = 2;
    private static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private static final byte MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE = 'S';
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final int PROTOCOL_TAIL_COMMAND_LENGTH = 64;
    private static final int ROLLING_BACK_TRANSACTION = 4;
    private static final int SSL_REQUEST = 80877103;
    private static final int SYNC_BIND = 3;
    private static final int SYNC_DESCRIBE = 2;
    private static final int SYNC_DESCRIBE_PORTAL = 4;
    private static final int SYNC_PARSE = 1;
    @SuppressWarnings("FieldMayBeFinal")
    private static Log LOG = LogFactory.getLog(PGConnectionContext.class);
    private final BatchCallback batchCallback;
    private final ObjectPool<DirectBinarySequence> binarySequenceParamsPool;
    private final IntList bindVariableTypes = new IntList();
    private final CharacterStore characterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final boolean dumpNetworkTraffic;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
    private final int maxBlobSizeOnQuery;
    private final Metrics metrics;
    private final CharSequenceObjHashMap<PGPipelineEntry> namedPortals;
    private final CharSequenceObjHashMap<PGPipelineEntry> namedStatements;
    private final ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters;
    private final ArrayDeque<PGPipelineEntry> pipeline = new ArrayDeque<>();
    private final int recvBufferSize;
    private final ResponseUtf8Sink responseUtf8Sink = new ResponseUtf8Sink();
    private final SecurityContextFactory securityContextFactory;
    private final int sendBufferSize;
    private final WeakSelfReturningObjectPool<TypesAndInsert> taiPool;
    private final WeakSelfReturningObjectPool<TypesAndUpdate> tauPool;
    private final DirectUtf8String utf8String = new DirectUtf8String();
    private Authenticator authenticator;
    private final BindVariableService bindVariableService;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private boolean errorSkipToSync;
    private boolean freezeRecvBuffer;
    private boolean isPausedQuery = false;
    private byte lastMsgType;
    private Path path;
    private PGPipelineEntry pipelineLastEntry;
    private long recvBuffer;
    private long recvBufferReadOffset = 0;
    private long recvBufferWriteOffset = 0;
    private boolean replyAndContinue;
    private PGResumeCallback resumeCallback;
    private final Rnd rnd;
    private long sendBuffer;
    private long sendBufferLimit;
    private long sendBufferPtr;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private SuspendEvent suspendEvent;
    // insert 'statements' are cached only for the duration of user session
    private SimpleAssociativeCache<TypesAndInsert> taiCache;
    private AssociativeCache<TypesAndSelect> tasCache;
    private SimpleAssociativeCache<TypesAndUpdate> tauCache;
    private boolean tlsSessionStarting = false;
    private long totalReceived = 0;
    private int transactionState = NO_TRANSACTION;

    public PGConnectionContext(
            CairoEngine engine,
            PGWireConfiguration configuration,
            SqlExecutionContextImpl sqlExecutionContext,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            AssociativeCache<TypesAndSelect> tasCache
    ) {
        super(
                configuration.getFactoryProvider().getPGWireSocketFactory(),
                configuration.getNetworkFacade(),
                LOG,
                engine.getMetrics().pgWire().connectionCountGauge()
        );

        try {
            this.path = new Path();
            this.engine = engine;
            this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
            this.recvBufferSize = Numbers.ceilPow2(configuration.getRecvBufferSize());
            this.sendBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
            this.forceSendFragmentationChunkSize = configuration.getForceSendFragmentationChunkSize();
            this.forceRecvFragmentationChunkSize = configuration.getForceRecvFragmentationChunkSize();
            this.characterStore = new CharacterStore(configuration.getCharacterStoreCapacity(), configuration.getCharacterStorePoolCapacity());
            this.maxBlobSizeOnQuery = configuration.getMaxBlobSizeOnQuery();
            this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
            this.circuitBreaker = circuitBreaker;
            this.sqlExecutionContext = sqlExecutionContext;
            this.sqlExecutionContext.with(DenyAllSecurityContext.INSTANCE, bindVariableService, this.rnd = configuration.getRandom());
            this.namedStatements = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
            this.pendingWriters = new ObjObjHashMap<>(configuration.getPendingWritersCacheSize());
            this.namedPortals = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
            this.binarySequenceParamsPool = new ObjectPool<>(DirectBinarySequence::new, configuration.getBinParamCountCapacity());

            this.metrics = engine.getMetrics();

            this.tasCache = tasCache;

            final boolean enabledUpdateCache = configuration.isUpdateCacheEnabled();
            final int updateBlockCount = enabledUpdateCache ? configuration.getUpdateCacheBlockCount() : 1;
            final int updateRowCount = enabledUpdateCache ? configuration.getUpdateCacheRowCount() : 1;
            this.tauCache = new SimpleAssociativeCache<>(updateBlockCount, updateRowCount, metrics.pgWire().cachedUpdatesGauge());
            this.tauPool = new WeakSelfReturningObjectPool<>(parent -> new TypesAndUpdate(parent, engine), updateBlockCount * updateRowCount);

            final boolean enableInsertCache = configuration.isInsertCacheEnabled();
            final int insertBlockCount = enableInsertCache ? configuration.getInsertCacheBlockCount() : 1;
            final int insertRowCount = enableInsertCache ? configuration.getInsertCacheRowCount() : 1;
            this.taiCache = new SimpleAssociativeCache<>(insertBlockCount, insertRowCount);
            this.taiPool = new WeakSelfReturningObjectPool<>(TypesAndInsert::new, insertBlockCount * insertRowCount);

            this.batchCallback = new PGConnectionBatchCallback();
            FactoryProvider factoryProvider = configuration.getFactoryProvider();
            this.securityContextFactory = factoryProvider.getSecurityContextFactory();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static int getInt(long address, long msgLimit, CharSequence errorMessage) throws BadProtocolException {
        if (address + Integer.BYTES <= msgLimit) {
            return getIntUnsafe(address);
        }
        LOG.error().$(errorMessage).$();
        throw BadProtocolException.INSTANCE;
    }

    public static long getLongUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getLong(address));
    }

    public static short getShort(long address, long msgLimit, CharSequence errorMessage) throws BadProtocolException {
        if (address + Short.BYTES <= msgLimit) {
            return getShortUnsafe(address);
        }
        LOG.error().$(errorMessage).$();
        throw BadProtocolException.INSTANCE;
    }

    public static long getStringLengthTedious(long x, long limit) {
        // calculate length
        for (long i = x; i < limit; i++) {
            if (Unsafe.getUnsafe().getByte(i) == 0) {
                return i;
            }
        }
        return -1;
    }

    public static long getUtf8StrSize(long x, long limit, CharSequence errorMessage) throws BadProtocolException {
        long len = Unsafe.getUnsafe().getByte(x) == 0 ? x : getStringLengthTedious(x, limit);
        if (len > -1) {
            return len;
        }
        // we did not find 0 within message limit
        LOG.error().$(errorMessage).$();
        throw BadProtocolException.INSTANCE;
    }

    public static void putInt(long address, int value) {
        Unsafe.getUnsafe().putInt(address, Numbers.bswap(value));
    }

    public static void putLong(long address, long value) {
        Unsafe.getUnsafe().putLong(address, Numbers.bswap(value));
    }

    public static void putShort(long address, short value) {
        Unsafe.getUnsafe().putShort(address, Numbers.bswap(value));
    }

    @Override
    public void clear() {
        super.clear();
        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_PGW_CONN);
        this.sendBuffer = this.sendBufferPtr = this.sendBufferLimit = Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
        pipelineLastEntry = null;
        pipeline.clear();
        prepareForNewQuery();
        clearRecvBuffer();
        clearWriters();
        clearNamedStatements();
        // Clear every field, even if already cleaned to be on the safe side.
        Misc.clear(bindVariableTypes);
        Misc.clear(characterStore);
        Misc.clear(circuitBreaker);
        Misc.clear(responseUtf8Sink);
        Misc.clear(pendingWriters);
        Misc.clear(authenticator);
        Misc.clear(bindVariableService);
        bufferRemainingOffset = 0;
        bufferRemainingSize = 0;
        errorSkipToSync = false;
        freezeRecvBuffer = false;
        lastMsgType = 0;
        replyAndContinue = false;
        resumeCallback = null;
        suspendEvent = null;
        tlsSessionStarting = false;
        totalReceived = 0;
        transactionState = NO_TRANSACTION;
    }

    @Override
    public void clearSuspendEvent() {
        suspendEvent = Misc.free(suspendEvent);
    }

    public void clearWriters() {
        if (pendingWriters != null) {
            closePendingWriters(false);
            pendingWriters.clear();
        }
    }

    @Override
    public void close() {
        // We're about to close the context, so no need to return pending factory to cache.
        clear();
        if (sqlExecutionContext != null) {
            sqlExecutionContext.with(DenyAllSecurityContext.INSTANCE, null, null, -1, null);
        }
        path = Misc.free(path);
        authenticator = Misc.free(authenticator);
        tasCache = Misc.free(tasCache);
        tauCache = Misc.free(tauCache);
        taiCache = Misc.free(taiCache);
    }

    @Override
    public SuspendEvent getSuspendEvent() {
        return suspendEvent;
    }

    @Override
    public TableWriterAPI getTableWriterAPI(TableToken tableToken, @NotNull String lockReason) {
        final int index = pendingWriters.keyIndex(tableToken);
        if (index < 0) {
            return pendingWriters.valueAt(index);
        }
        return engine.getTableWriterAPI(tableToken, lockReason);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(CharSequence tableName, @NotNull String lockReason) {
        return getTableWriterAPI(engine.verifyTableName(tableName), lockReason);
    }

    public void handleClientOperation(int operation) throws Exception {
        assert authenticator != null;

        try {
            handleTlsRequest();
            if (tlsSessionStarting) {
                if (bufferRemainingSize > 0) {
                    sendBuffer(bufferRemainingOffset, bufferRemainingSize);
                }
                tlsSessionStarting = false;
                if (socket.startTlsSession(null) != 0) {
                    LOG.error().$("failed to create new TLS session").$();
                    throw BadProtocolException.INSTANCE;
                }
                // Start listening for read.
                throw PeerIsSlowToWriteException.INSTANCE;
            }
            handleAuthentication();
        } catch (PeerDisconnectedException | PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            // BAU, not error metric
            throw e;
        } catch (Throwable th) {
            metrics.pgWire().getErrorCounter().inc();
            throw th;
        }

        try {
            if (isPausedQuery) {
                isPausedQuery = false;
                if (resumeCallback != null) {
                    resumeCallback.resume(sqlExecutionContext, responseUtf8Sink);
                }
            } else if (bufferRemainingSize > 0) {
                sendBuffer(bufferRemainingOffset, bufferRemainingSize);
                if (resumeCallback != null) {
                    resumeCallback.resume(false);
                }
                if (replyAndContinue) {
                    replyAndContinue();
                }
            }

            long readOffsetBeforeParse = -1;
            // exit from this loop is via exception when either need wait to read / write from socket
            // or disconnection is detected / requested
            //noinspection InfiniteLoopStatement
            while (true) {

                // Read more from socket or throw when
                if (
                    // - parsing stalls, e.g. readOffsetBeforeParse == recvBufferReadOffset
                        readOffsetBeforeParse == recvBufferReadOffset
                                // - recv buffer is empty
                                || recvBufferReadOffset == recvBufferWriteOffset
                                // - socket is signalled ready to read at the first iteration of this loop
                                || (operation == IOOperation.READ && readOffsetBeforeParse == -1)) {
                    // free up recv buffer
                    if (!freezeRecvBuffer) {
                        if (recvBufferReadOffset == recvBufferWriteOffset) {
                            clearRecvBuffer();
                        } else if (recvBufferReadOffset > 0) {
                            // nothing changed?
                            // shift to start
                            shiftReceiveBuffer(recvBufferReadOffset);
                        }
                    }

                    recv();
                }

                // Parse will update the value of recvBufferOffset upon completion of
                // logical block. We cannot count on return value because 'parse' may try to
                // respond to client and fail with exception. When it does fail we would have
                // to retry 'send' but not parse the same input again
                readOffsetBeforeParse = recvBufferReadOffset;
                totalReceived += (recvBufferWriteOffset - recvBufferReadOffset);
                parseMessage(recvBuffer + recvBufferReadOffset, (int) (recvBufferWriteOffset - recvBufferReadOffset));
            }
        } catch (SqlException e) {
            handleException(e.getPosition(), e.getFlyweightMessage(), false, -1, true);
        } catch (ImplicitCastException e) {
            handleException(-1, e.getFlyweightMessage(), false, -1, true);
        } catch (CairoException e) {
            handleException(e.getPosition(), e.getFlyweightMessage(), e.isCritical(), e.getErrno(), e.isInterruption());
        } catch (PeerDisconnectedException | PeerIsSlowToReadException | PeerIsSlowToWriteException |
                 QueryPausedException | BadProtocolException e) {
            throw e;
        } catch (Throwable th) {
            handleException(-1, th.getMessage(), true, -1, true);
        }
    }

    @Override
    public PGConnectionContext of(int fd, @NotNull IODispatcher<PGConnectionContext> dispatcher) {
        super.of(fd, dispatcher);
        sqlExecutionContext.with(fd);
        if (recvBuffer == 0) {
            this.recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_PGW_CONN);
        }
        if (sendBuffer == 0) {
            this.sendBuffer = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
            this.sendBufferPtr = sendBuffer;
            this.sendBufferLimit = sendBuffer + sendBufferSize;
        }
        authenticator.init(socket, recvBuffer, recvBuffer + recvBufferSize, sendBuffer, sendBufferLimit);
        return this;
    }

    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setBinBindVariable(int index, long address, int valueLen) throws SqlException {
        bindVariableService.setBin(index, this.binarySequenceParamsPool.next().of(address, valueLen));
        freezeRecvBuffer = true;
    }

    @Override
    public void setStatementTimeout(long statementTimeout) {
        this.statementTimeout = statementTimeout;
        if (statementTimeout > 0) {
            circuitBreaker.setTimeout(statementTimeout);
        }
    }

    public void setStrBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        Function fn = bindVariableService.getFunction(index);
        // If the function type is VARCHAR, there's no need to convert to UTF-16
        if (fn != null && fn.getType() == ColumnType.VARCHAR) {
            final int sequenceType = Utf8s.getUtf8SequenceType(address, address + valueLen);
            boolean ascii;
            switch (sequenceType) {
                case 0:
                    // ascii sequence
                    ascii = true;
                    break;
                case 1:
                    // non-ASCII sequence
                    ascii = false;
                    break;
                default:
                    LOG.error().$("invalid varchar bind variable type [index=").$(index).I$();
                    throw BadProtocolException.INSTANCE;
            }
            bindVariableService.setVarchar(index, utf8String.of(address, address + valueLen, ascii));

        } else {
            if (Utf8s.utf8ToUtf16(address, address + valueLen, e)) {
                bindVariableService.setStr(index, characterStore.toImmutable());
            } else {
                LOG.error().$("invalid str bind variable type [index=").$(index).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    public void setSuspendEvent(SuspendEvent suspendEvent) {
        this.suspendEvent = suspendEvent;
    }

    public void setTimestampBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long.BYTES, valueLen);
        bindVariableService.setTimestamp(index, getLongUnsafe(address) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private static void ensureValueLength(int index, int required, int actual) throws BadProtocolException {
        if (required == actual) {
            return;
        }
        LOG.error().$("bad parameter value length [required=").$(required).$(", actual=").$(actual).$(", index=").$(index).I$();
        throw BadProtocolException.INSTANCE;
    }

    private void appendBinColumn(Record record, int i) throws SqlException {
        BinarySequence sequence = record.getBin(i);
        if (sequence == null) {
            responseUtf8Sink.setNullValue();
        } else {
            // if length is above max we will error out the result set
            long blobSize = sequence.length();
            if (blobSize < maxBlobSizeOnQuery) {
                responseUtf8Sink.put(sequence);
            } else {
                throw SqlException.position(0).put("blob is too large [blobSize=").put(blobSize).put(", max=").put(maxBlobSizeOnQuery).put(", columnIndex=").put(i).put(']');
            }
        }
    }

    private void appendBooleanColumn(Record record, int columnIndex) {
        responseUtf8Sink.putNetworkInt(Byte.BYTES);
        responseUtf8Sink.put(record.getBool(columnIndex) ? 't' : 'f');
    }

    private void appendBooleanColumnBin(Record record, int columnIndex) {
        responseUtf8Sink.putNetworkInt(Byte.BYTES);
        responseUtf8Sink.put(record.getBool(columnIndex) ? (byte) 1 : (byte) 0);
    }

    private void appendByteColumn(Record record, int columnIndex) {
        long a = responseUtf8Sink.skipInt();
        responseUtf8Sink.put((int) record.getByte(columnIndex));
        responseUtf8Sink.putLenEx(a);
    }

    private void appendByteColumnBin(Record record, int columnIndex) {
        final byte value = record.getByte(columnIndex);
        responseUtf8Sink.putNetworkInt(Short.BYTES);
        responseUtf8Sink.putNetworkShort(value);
    }

    private void appendCharColumn(Record record, int columnIndex) {
        final char charValue = record.getChar(columnIndex);
        if (charValue == 0) {
            responseUtf8Sink.setNullValue();
        } else {
            long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(charValue);
            responseUtf8Sink.putLenEx(a);
        }
    }

    private void appendDateColumn(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            final long a = responseUtf8Sink.skipInt();
            PG_DATE_MILLI_TIME_Z_PRINT_FORMAT.format(longValue, DateFormatUtils.EN_LOCALE, null, responseUtf8Sink);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendDateColumnBin(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            responseUtf8Sink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            responseUtf8Sink.putNetworkLong(longValue * 1000 - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendDoubleColumn(Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (doubleValue == doubleValue) {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(doubleValue);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendDoubleColumnBin(Record record, int columnIndex) {
        final double value = record.getDouble(columnIndex);
        if (value == value) {
            responseUtf8Sink.putNetworkInt(Double.BYTES);
            responseUtf8Sink.putNetworkDouble(value);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendFloatColumn(Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (floatValue == floatValue) {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(floatValue, 3);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendFloatColumnBin(Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (value == value) {
            responseUtf8Sink.putNetworkInt(Float.BYTES);
            responseUtf8Sink.putNetworkFloat(value);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendIPv4Col(Record record, int columnIndex) {
        int value = record.getIPv4(columnIndex);
        if (value == Numbers.IPv4_NULL) {
            responseUtf8Sink.setNullValue();
        } else {
            final long a = responseUtf8Sink.skipInt();
            Numbers.intToIPv4Sink(responseUtf8Sink, value);
            responseUtf8Sink.putLenEx(a);
        }
    }

    private void appendIntCol(Record record, int i) {
        final int intValue = record.getInt(i);
        if (intValue != Numbers.INT_NULL) {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(intValue);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendIntColumnBin(Record record, int columnIndex) {
        final int value = record.getInt(columnIndex);
        if (value != Numbers.INT_NULL) {
            responseUtf8Sink.checkCapacity(8);
            responseUtf8Sink.putIntUnsafe(0, INT_BYTES_X);
            responseUtf8Sink.putIntUnsafe(4, Numbers.bswap(value));
            responseUtf8Sink.bump(8);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void appendLong256Column(Record record, int columnIndex) {
        final Long256 long256Value = record.getLong256A(columnIndex);
        if (long256Value.getLong0() == Numbers.LONG_NULL && long256Value.getLong1() == Numbers.LONG_NULL && long256Value.getLong2() == Numbers.LONG_NULL && long256Value.getLong3() == Numbers.LONG_NULL) {
            responseUtf8Sink.setNullValue();
        } else {
            final long a = responseUtf8Sink.skipInt();
            Numbers.appendLong256(long256Value.getLong0(), long256Value.getLong1(), long256Value.getLong2(), long256Value.getLong3(), responseUtf8Sink);
            responseUtf8Sink.putLenEx(a);
        }
    }

    private void appendLongColumn(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NULL) {
            final long a = responseUtf8Sink.skipInt();
            responseUtf8Sink.put(longValue);
            responseUtf8Sink.putLenEx(a);
        } else {
            responseUtf8Sink.setNullValue();
        }
    }

    private void assertBufferSize(boolean check) throws BadProtocolException {
        if (check) {
            return;
        }
        // we did not find 0 within message limit
        LOG.error().$("undersized receive buffer or someone is abusing protocol").$();
        throw BadProtocolException.INSTANCE;
    }

    private void clearNamedStatements() {
        if (namedStatements != null && namedStatements.size() > 0) {
            ObjList<CharSequence> names = namedStatements.keys();
            for (int i = 0, n = names.size(); i < n; i++) {
                CharSequence name = names.getQuick(i);
                namedStatementWrapperPool.push(namedStatements.get(name));
            }
            namedStatements.clear();
        }
    }

    private <T extends Mutable> void clearPool(
            @Nullable CharSequenceObjHashMap<T> map, @Nullable WeakMutableObjectPool<T> pool, String poolName
    ) {
        if (map == null || pool == null) {
            return;
        }
        for (int i = 0, n = map.keys().size(); i < n; i++) {
            CharSequence key = map.keys().get(i);
            pool.push(map.get(key));
        }
        map.clear();
        int l = pool.resetLeased();
        if (l != 0) {
            LOG.critical().$(poolName).$(" pool is not empty at context clear [fd=").$(socket.getFd()).$(" leased=").$(l).I$();
        }
    }

    private void clearRecvBuffer() {
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
    }

    private void closePendingWriters(boolean commit) {
        for (ObjObjHashMap.Entry<TableToken, TableWriterAPI> pendingWriter : pendingWriters) {
            final TableWriterAPI m = pendingWriter.value;
            if (commit) {
                m.commit();
            } else {
                m.rollback();
            }
            Misc.free(m);
        }
    }

    private void cmdClose(long lo, long msgLimit) throws BadProtocolException {
        final byte type = Unsafe.getUnsafe().getByte(lo);
        switch (type) {
            case 'S':
                lo = lo + 1;
                final long hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length");
                removeNamedStatement(getUtf16Str(lo, hi));
                break;
            case 'P':
                lo = lo + 1;
                final long high = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length");
                final CharSequence portalName = getPortalName(lo, high);
                if (portalName != null) {
                    final int index = namedPortals.keyIndex(portalName);
                    if (index < 0) {
                        namedPortalPool.push(namedPortals.valueAt(index));
                        namedPortals.removeAt(index);
                    } else {
                        LOG.error().$("invalid portal name [value=").$(portalName).I$();
                        throw BadProtocolException.INSTANCE;
                    }
                }
                break;
            default:
                LOG.error().$("invalid type for close message [type=").$(type).I$();
                throw BadProtocolException.INSTANCE;
        }
        prepareCloseComplete();
    }

    private void cmdHlush() throws PeerDisconnectedException, PeerIsSlowToReadException {
        // "The Flush message does not cause any specific output to be generated, but forces the backend to deliver any data pending in its output buffers.
        //  A Flush must be sent after any extended-query command except Sync, if the frontend wishes to examine the results of that command before issuing more commands.
        //  Without Flush, messages returned by the backend will be combined into the minimum possible number of packets to minimize network overhead."
        // some clients (asyncpg) chose not to send 'S' (sync) message
        // but instead fire 'H'. Can't wrap my head around as to why
        // query execution is so ambiguous
        if (activeDueResponses.size() > 0) {
            prepareDueResponses();
        }
        sendBufferAndReset();
    }

    // processes one or more queries (batch/script). "Simple Query" in PostgreSQL docs.
    private void cmdQuery(long lo, long limit) throws Exception {
        sendRNQ = true;
        prepareForNewQuery();
        isEmptyQuery = true; // assume SQL text contains no query until we find out otherwise
        CharacterStoreEntry e = characterStore.newEntry();

        if (Utf8s.utf8ToUtf16(lo, limit - 1, e)) {
            activeSqlText = characterStore.toImmutable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compileBatch(activeSqlText, sqlExecutionContext, batchCallback);
                clearCursorAndFactory();
                if (isEmptyQuery) {
                    prepareEmptyQueryResponse();
                }
                // we need to continue parsing receive buffer even if we errored out
                // this is because PG client might expect separate responses to everything it sent
            } catch (SqlException ex) {
                prepareNonCriticalError(ex.getPosition(), ex.getFlyweightMessage());
            } catch (CairoException ex) {
                if (ex.isInterruption()) {
                    prepareErrorResponse(-1, ex.getFlyweightMessage());
                } else {
                    prepareError(ex.getPosition(), ex.getFlyweightMessage(), ex.isCritical(), ex.getErrno());
                }
            }
        } else {
            LOG.error().$("invalid UTF8 bytes in parse query").$();
            throw BadProtocolException.INSTANCE;
        }
        sendReadyForNewQuery();
    }

    private void doSendWithRetries(int bufferOffset, int bufferSize) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int offset = bufferOffset;
        int remaining = bufferSize;

        while (remaining > 0) {
            int m = socket.send(sendBuffer + offset, Math.min(remaining, forceSendFragmentationChunkSize));
            if (m < 0) {
                LOG.info().$("disconnected on write [code=").$(m).I$();
                throw PeerDisconnectedException.INSTANCE;
            }

            dumpBuffer('<', sendBuffer + offset, m, dumpNetworkTraffic);

            remaining -= m;
            offset += m;

            if (m == 0 || forceSendFragmentationChunkSize < sendBufferSize) {
                // The socket is not ready for write, or we're simulating network fragmentation.
                break;
            }
        }

        if (remaining > 0) {
            bufferRemainingOffset = offset;
            bufferRemainingSize = remaining;
            throw PeerIsSlowToReadException.INSTANCE;
        }
    }

    @Nullable
    private CharSequence getPortalName(long lo, long hi) throws BadProtocolException {
        if (hi - lo > 0) {
            return getString(lo, hi, "invalid UTF8 bytes in portal name");
        }
        return null;
    }

    private CharSequence getString(long lo, long hi, CharSequence errorMessage) throws BadProtocolException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(lo, hi, e)) {
            return characterStore.toImmutable();
        } else {
            LOG.error().$(errorMessage).$();
            throw BadProtocolException.INSTANCE;
        }
    }

    @Nullable
    private CharSequence getUtf16Str(long lo, long hi, String utf8ErrorStr) throws BadProtocolException {
        // todo: use utf8 maps
        if (hi - lo > 0) {
            return getString(lo, hi, utf8ErrorStr);
        }
        return null;
    }

    private void handleAuthentication() throws PeerIsSlowToWriteException, PeerIsSlowToReadException, BadProtocolException, PeerDisconnectedException {
        if (authenticator.isAuthenticated()) {
            return;
        }
        int r;
        try {
            r = authenticator.handleIO();
            if (r == Authenticator.OK) {
                try {
                    final SecurityContext securityContext = securityContextFactory.getInstance(authenticator.getPrincipal(), authenticator.getAuthType(), SecurityContextFactory.PGWIRE);
                    sqlExecutionContext.with(securityContext, bindVariableService, rnd, getFd(), circuitBreaker);
                    securityContext.checkEntityEnabled();
                    r = authenticator.loginOK();
                } catch (CairoException e) {
                    LOG.error().$("failed to authenticate [error=").$(e.getFlyweightMessage()).I$();
                    r = authenticator.denyAccess(e.getFlyweightMessage());
                }
            }
        } catch (AuthenticatorException e) {
            throw PeerDisconnectedException.INSTANCE;
        }
        switch (r) {
            case Authenticator.OK:
                assert authenticator.isAuthenticated();
                break;
            case Authenticator.NEEDS_READ:
                throw PeerIsSlowToWriteException.INSTANCE;
            case Authenticator.NEEDS_WRITE:
                throw PeerIsSlowToReadException.INSTANCE;
            case Authenticator.NEEDS_DISCONNECT:
                throw PeerDisconnectedException.INSTANCE;
            default:
                throw BadProtocolException.INSTANCE;
        }


        // authenticator may have some non-auth data left in the buffer - make sure we don't overwrite it
        recvBufferWriteOffset = authenticator.getRecvBufPos() - recvBuffer;
        recvBufferReadOffset = authenticator.getRecvBufPseudoStart() - recvBuffer;
    }

    private void handleException(int position, CharSequence message, boolean critical, int errno, boolean interruption) throws PeerDisconnectedException, PeerIsSlowToReadException {
        metrics.pgWire().getErrorCounter().inc();
        clearCursorAndFactory();
        if (interruption) {
            prepareErrorResponse(position, message);
        } else {
            prepareError(position, message, critical, errno);
        }
        resumeCallback = null;
        errorSkipToSync = lastMsgType != 'S' && lastMsgType != 'X' && lastMsgType != 'H' && lastMsgType != 'Q';
        if (errorSkipToSync) {
            throw PeerIsSlowToReadException.INSTANCE;
        } else {
            replyAndContinue();
        }
    }

    private void handleTlsRequest() throws PeerIsSlowToWriteException, PeerIsSlowToReadException, BadProtocolException, PeerDisconnectedException {
        if (!socket.supportsTls() || tlsSessionStarting || socket.isTlsSessionStarted()) {
            return;
        }
        recv();
        int expectedLen = 2 * Integer.BYTES;
        int len = (int) (recvBufferWriteOffset - recvBufferReadOffset);
        if (len < expectedLen) {
            throw PeerIsSlowToWriteException.INSTANCE;
        }
        if (len != expectedLen) {
            LOG.error().$("request SSL message expected [actualLen=").$(len).I$();
            throw BadProtocolException.INSTANCE;
        }
        long address = recvBuffer + recvBufferReadOffset;
        int msgLen = getIntUnsafe(address);
        recvBufferReadOffset += Integer.BYTES;
        address += Integer.BYTES;
        if (msgLen != expectedLen) {
            LOG.error().$("unexpected request SSL message [msgLen=").$(msgLen).I$();
            throw BadProtocolException.INSTANCE;
        }
        int request = getIntUnsafe(address);
        recvBufferReadOffset += Integer.BYTES;
        if (request != SSL_REQUEST) {
            LOG.error().$("unexpected request SSL message [request=").$(msgLen).I$();
            throw BadProtocolException.INSTANCE;
        }
        // tell the client that SSL is supported
        responseUtf8Sink.put(MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE);
        tlsSessionStarting = true;
        responseUtf8Sink.sendBufferAndReset();
    }

    private void lookupPipelineEntryForPortalName(@Nullable CharSequence portalName) throws BadProtocolException {
        if (portalName != null) {
            // Alright, the client wants to use the named statement. What if they just
            // send "parse" message and want to abandon it?
            pipelineLastEntry = freeIfAbandoned(pipelineLastEntry);

            // it is safe to overwrite the pipeline entry,
            // named entries will be held in the hash map
            pipelineLastEntry = namedPortals.get(portalName);

            // however, we cannot continue if the portal name is invalid
            if (pipelineLastEntry == null) {
                LOG.error().$(" portal does not exist [name=").$(portalName).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    private void lookupPipelineEntryForStatementName(@Nullable CharSequence statementName) throws BadProtocolException {
        if (statementName != null) {
            // Alright, the client wants to use the named statement. What if they just
            // send "parse" message and want to abandon it?
            pipelineLastEntry = freeIfAbandoned(pipelineLastEntry);

            // it is safe to overwrite the pipeline entry,
            // named entries will be held in the "namedEntries" cache
            pipelineLastEntry = namedStatements.get(statementName);

            // however, we cannot continue if the prepared statement name is invalid
            if (pipelineLastEntry == null) {
                LOG.error().$("statement or portal does not exist [name=").$(statementName).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    private void msgBind(long lo, long msgLimit) throws BadProtocolException, SqlException {
        short parameterValueCount;

        LOG.debug().$("bind").$();

        // portal name
        long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (bind)");
        CharSequence portalName = getUtf16Str(lo, hi, "invalid UTF8 bytes in portal name (bind)");
        // named statement
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length [msgType='B']");

        lookupPipelineEntryForStatementName(getUtf16Str(lo, hi, "invalid UTF8 bytes in statement name (bind)"));

        // Past this point the pipeline entry must not be null.
        // If it is - this means back-to-back "bind" messages were received with no prepared statement name.
        if (pipelineLastEntry == null) {
            LOG.error().$("spurious bind message").I$();
            throw BadProtocolException.INSTANCE;
        }

        pipelineLastEntry.setStateBind(true);

        // "bind" is asking us to create portal.
        // Portal is a reusable cursor, which means after "execute" message we must not
        // close cursor and expect multiple "execute" messages from the client against the same portal.
        msgBindCreateTargetPortal(portalName);

        // parameter format count
        lo = hi + 1;
        final short parameterFormatCodeCount = getShort(lo, msgLimit, "could not read parameter format code count");
        pipelineLastEntry.msgBindCopyParameterFormatCodes(
                lo,
                msgLimit,
                parameterFormatCodeCount
        );
        lo += Short.BYTES;

        // parameter value count
        lo += parameterFormatCodeCount * Short.BYTES;
        parameterValueCount = getShort(lo, msgLimit, "could not read parameter value count");

        LOG.debug().$("binding [parameterValueCount=").$(parameterValueCount).$(", thread=").$(Thread.currentThread().getId()).I$();

        // we now have all parameter counts, validate them
        pipelineLastEntry.msgBindSetParameterValueCount(parameterValueCount);

        lo += Short.BYTES;

        lo = pipelineLastEntry.msgBindDefineBindVariableTypes(
                lo,
                msgLimit,
                bindVariableService,
                // below are some reusable, transient pools
                characterStore,
                utf8String,
                binarySequenceParamsPool
        );

        short columnFormatCodeCount = getShort(lo, msgLimit, "could not read result set column format codes");
        lo += Short.BYTES;
        pipelineLastEntry.msgBindCopySelectFormatCodes(lo, msgLimit, columnFormatCodeCount);
    }

    private void msgBindCreateTargetPortal(CharSequence targetPortalName) throws BadProtocolException {
        if (targetPortalName != null) {
            LOG.info().$("create portal [name=").$(targetPortalName).I$();
            int index = namedPortals.keyIndex(targetPortalName);
            if (index > -1) {
                namedPortals.putAt(index, Chars.toString(targetPortalName), pipelineLastEntry);
                pipelineLastEntry.setPortal(true);
            } else {
                pipelineLastEntry = freeIfAbandoned(pipelineLastEntry);
                LOG.error().$("duplicate portal [name=").$(targetPortalName).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    private void msgDescribe(long lo, long msgLimit) throws BadProtocolException {

        // 'S' = statement name
        // 'P' = portal name
        // followed by the name, which can be NULL, typically with 'P'
        boolean isPortal = Unsafe.getUnsafe().getByte(lo) == 'P';
        long hi = getUtf8StrSize(lo + 1, msgLimit, "bad prepared statement name length (describe)");

        // this could be either:
        // statement name if "isPortal" is false
        // otherwise portal name
        final CharSequence target = getUtf16Str(lo + 1, hi, "invalid UTF8 bytes in portal or statement name (describe)");
        if (isPortal) {
            lookupPipelineEntryForPortalName(target);
        } else {
            lookupPipelineEntryForStatementName(target);
        }

        // some defensive code to have predictable behaviour
        // when dealing with spurious "describe" messages, for which we do not have
        // a pipeline entry

        if (pipelineLastEntry == null) {
            LOG.error().$("spurious describe message received").I$();
            throw BadProtocolException.INSTANCE;
        }

        pipelineLastEntry.setStateDesc(target == null ? 1 : isPortal ? 2 : 3);
    }

    private void msgExecute(long lo, long msgLimit) throws Exception {
        final long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (execute)");
        lookupPipelineEntryForPortalName(getUtf16Str(lo, hi, "invalid UTF8 bytes in portal name (execute)"));

        if (pipelineLastEntry == null) {
            LOG.error().$("spurious execute message").I$();
            throw BadProtocolException.INSTANCE;
        }

        lo = hi + 1;
        pipelineLastEntry.setReturnRowCountLimit(getInt(lo, msgLimit, "could not read max rows value"));
        pipelineLastEntry.setStateExec(true);
        pipelineLastEntry.execute(sqlExecutionContext, transactionState, taiCache, pendingWriters, this, namedStatements);
    }

    private void msgParse(long address, long lo, long msgLimit) throws BadProtocolException, SqlException {

        // 'Parse'
        //message length
        long hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length (parse)");

        // when statement name is present in "parse" message
        // it should be interpreted as "store" command, e.g. we store the
        // parsed SQL as short and sweet statement name.
        final CharSequence targetStatementName = getUtf16Str(lo, hi, "invalid UTF8 bytes in statement name (parse)");

        //query text
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad query text length");

        // it is possible that the client sending "parse" messages to name
        // several prepared statements, we will close it in case of back to back parse messages
        pipelineLastEntry = freeIfAbandoned(pipelineLastEntry);

        // it is safe to overwrite the entry without having memory leak
        pipelineLastEntry = parseSql(lo, hi);
        pipelineLastEntry.setStateParse(true);

        lo = hi + 1;

        msgParseCreateTargetStatement(targetStatementName);

        // parameter type count
        short parameterTypeCount = getShort(lo, msgLimit, "could not read parameter type count");

        // process parameter types
        if (parameterTypeCount > 0) {
            if (lo + Short.BYTES + parameterTypeCount * 4L > msgLimit) {
                LOG.error()
                        .$("could not read parameters [parameterCount=").$(parameterTypeCount)
                        .$(", offset=").$(lo - address)
                        .$(", remaining=")
                        .$(msgLimit - lo)
                        .I$();
                throw BadProtocolException.INSTANCE;
            }

            LOG.debug().$("params [count=").$(parameterTypeCount).I$();
            // copy argument types into the last pipeline entry
            // the entry will also maintain count of these argument types to aid
            // validation of the "bind" message.
            pipelineLastEntry.msgParseCopyParameterTypesFromMsg(lo + Short.BYTES, parameterTypeCount);
        } else if (parameterTypeCount < 0) {
            LOG.error().$("invalid parameter count [parameterCount=").$(parameterTypeCount).$(", offset=").$(lo - address).I$();
            throw BadProtocolException.INSTANCE;
        } else {
            // parameter types were not provided by the client
            // copy parameter types from our SQL type resolution engine
            pipelineLastEntry.msgParseCopyParameterTypesFromService(bindVariableService);
        }
    }

    private void msgParseCreateTargetStatement(CharSequence targetStatementName) throws BadProtocolException {
        if (targetStatementName != null) {
            LOG.info().$("create prepared statement [name=").$(targetStatementName).I$();
            int index = namedStatements.keyIndex(targetStatementName);
            if (index > -1) {
                namedStatements.putAt(index, Chars.toString(targetStatementName), pipelineLastEntry);
                pipelineLastEntry.setPreparedStatement(true);
            } else {
                pipelineLastEntry = freeIfAbandoned(pipelineLastEntry);
                LOG.error().$("duplicate statement [name=").$(targetStatementName).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    private void msgSync() throws Exception {
        // the client wants us to start responding here, not anywhere before
        if (pipelineLastEntry != null) {
            pipeline.add(pipelineLastEntry);
            pipelineLastEntry = null;
        }
        PGPipelineEntry pe;
        while ((pe = pipeline.poll()) != null) {
            // with the sync call the existing pipeline entry will assign its own completion hooks (resume callbacks)
            transactionState = pe.sync(sqlExecutionContext, transactionState, responseUtf8Sink);
            pe.close();
        }
        prepareReadyForQuery();
        prepareForNewQuery();
    }

    /**
     * Returns address of where parsing stopped. If there are remaining bytes left
     * in the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parseMessage(long address, int len) throws Exception {
        // we will wait until we receive the entire header
        if (len < PREFIXED_MESSAGE_HEADER_LEN) {
            // we need to be able to read header and length
            return;
        }

        final byte type = Unsafe.getUnsafe().getByte(address);
        final int msgLen = getIntUnsafe(address + 1);
        LOG.debug().$("received msg [type=").$((char) type).$(", len=").$(msgLen).I$();
        if (msgLen < 1) {
            LOG.error().$("invalid message length [type=").$(type).$(", msgLen=").$(msgLen).$(", recvBufferReadOffset=").$(recvBufferReadOffset).$(", recvBufferWriteOffset=").$(recvBufferWriteOffset).$(", totalReceived=").$(totalReceived).I$();
            throw BadProtocolException.INSTANCE;
        }

        // this check is exactly the same as the one run inside security context on every permission checks.
        // however, this will run even if the command to be executed does not require permission checks.
        // this is useful in case a disabled user intends to hammer the database with queries which do not require authorization.
        sqlExecutionContext.getSecurityContext().checkEntityEnabled();

        // msgLen does not take into account type byte
        if (msgLen > len - 1) {
            // When this happens we need to shift our receive buffer left
            // to fit this message. Outer function will do that if we
            // just exit.
            LOG.debug().$("not enough data in buffer [expected=").$(msgLen).$(", have=").$(len).$(", recvBufferWriteOffset=").$(recvBufferWriteOffset).$(", recvBufferReadOffset=").$(recvBufferReadOffset).I$();
            return;
        }
        // we have enough to read entire message
        recvBufferReadOffset += msgLen + 1;
        final long msgLimit = address + msgLen + 1;
        final long msgLo = address + PREFIXED_MESSAGE_HEADER_LEN; // 8 is offset where name value pairs begin
        lastMsgType = type;
        if (errorSkipToSync) {
            if (lastMsgType == 'S' || lastMsgType == 'H') {
                errorSkipToSync = false;
                replyAndContinue();
            }
            // Skip all input until Sync or Flush messages received.
            return;
        }

        // Command types in the order they usually come over the wire.
        // All "cmd" methods are called only from here and
        // are responsible for handling individual commands. Please do
        // not create other methods that start with "cmd".
        switch (type) {
            case 'P': // parse
                msgParse(address, msgLo, msgLimit);
                break;
            case 'B': // bind
                msgBind(msgLo, msgLimit);
                break;
            case 'D': // describe
                msgDescribe(msgLo, msgLimit);
                break;
            case 'E': // execute
                msgExecute(msgLo, msgLimit);
                break;
            case 'Q': // simple query
                cmdQuery(msgLo, msgLimit);
                break;
            case 'S': // sync
                msgSync();
                // fall thru
            case 'H': // flush - Hlush
                cmdHlush();
                break;
            case 'X': // 'Terminate'
                throw PeerDisconnectedException.INSTANCE;
            case 'C':
                // close
                cmdClose(msgLo, msgLimit);
                break;
            default:
                LOG.error().$("unknown message [type=").$(type).I$();
                throw BadProtocolException.INSTANCE;
        }
    }

    private PGPipelineEntry parseSql(long lo, long hi) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(lo, hi, e)) {
            // todo: these should be pooled in a simple way (stack)
            PGPipelineEntry pe = new PGPipelineEntry(engine);
            pe.of(
                    characterStore.toImmutable(),
                    engine,
                    sqlExecutionContext,
                    taiCache,
                    tauCache,
                    tasCache,
                    taiPool,
                    tauPool
            );
            return pe;
        }
        LOG.error().$("invalid UTF8 bytes in parse query").$();
        throw BadProtocolException.INSTANCE;
    }

    private void prepareBindCompleteResponse() {
        responseUtf8Sink.put(MESSAGE_TYPE_BIND_COMPLETE);
        responseUtf8Sink.putIntDirect(INT_BYTES_X);
    }

    private void prepareCloseComplete() {
        responseUtf8Sink.put(MESSAGE_TYPE_CLOSE_COMPLETE);
        responseUtf8Sink.putIntDirect(INT_BYTES_X);
    }

    private void prepareEmptyQueryResponse() {
        LOG.debug().$("empty").$();
        responseUtf8Sink.put(MESSAGE_TYPE_EMPTY_QUERY);
        responseUtf8Sink.putIntDirect(INT_BYTES_X);
    }

    private void prepareError(int position, CharSequence message, boolean critical, int errno) {
        prepareErrorResponse(position, message);
        if (critical) {
            LOG.critical().$("error [msg=`").utf8(message).$("`, errno=").$(errno).I$();
        } else {
            LOG.error().$("error [msg=`").utf8(message).$("`, errno=").$(errno).I$();
        }
    }

    private void prepareErrorResponse(int position, CharSequence message) {
        responseUtf8Sink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = responseUtf8Sink.skipInt();
        responseUtf8Sink.putAscii('C');
        responseUtf8Sink.putZ("00000");
        responseUtf8Sink.putAscii('M');
        responseUtf8Sink.putZ(message);
        responseUtf8Sink.putAscii('S');
        responseUtf8Sink.putZ("ERROR");
        if (position > -1) {
            responseUtf8Sink.putAscii('P').put(position + 1).put((byte) 0);
        }
        responseUtf8Sink.put((byte) 0);
        responseUtf8Sink.putLen(addr);
    }

    // clears whole state except for characterStore because top-level batch text is using it
    private void prepareForNewBatchQuery() {
        if (completed) {
            LOG.debug().$("prepare for new query").$();
            Misc.clear(bindVariableService);
            freezeRecvBuffer = false;
            sqlExecutionContext.setCacheHit(false);
            sqlExecutionContext.containsSecret(false);
        }
    }

    private void prepareForNewQuery() {
        prepareForNewBatchQuery();
        Misc.clear(characterStore);
    }

    private void prepareNonCriticalError(int position, CharSequence message) {
        prepareErrorResponse(position, message);
        LOG.error().$("error [pos=").$(position).$(", msg=`").utf8(message).$('`').I$();
    }

    private void putGeoHashStringValue(long value, int bitFlags) {
        if (value == GeoHashes.NULL) {
            responseUtf8Sink.setNullValue();
        } else {
            final long a = responseUtf8Sink.skipInt();
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, responseUtf8Sink);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, responseUtf8Sink);
            }
            responseUtf8Sink.putLenEx(a);
        }
    }

    private void removeNamedStatement(CharSequence statementName) {
        if (statementName != null) {
            final int index = namedStatements.keyIndex(statementName);
            // do not freak out if client is closing statement we don't have
            // we could have reported error to client before statement was created
            if (index < 0) {
                namedStatementWrapperPool.push(namedStatements.valueAt(index));
                namedStatements.removeAt(index);
            }
        }
    }

    private void replyAndContinue() throws PeerDisconnectedException, PeerIsSlowToReadException {
        replyAndContinue = true;
        sendReadyForNewQuery();
        freezeRecvBuffer = false;
        clearRecvBuffer();
    }

    private void sendBufferAndResetBlocking() throws PeerDisconnectedException {
        if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
            sendBufferAndResetBlocking0();
        }
    }

    private void sendBufferAndResetBlocking0() throws PeerDisconnectedException {
        // This is simplified waited send for very limited use cases where introducing another state is an overkill.
        // This method busy waits to send buffer.
        while (true) {
            try {
                sendBuffer(bufferRemainingOffset, (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset));
                break;
            } catch (PeerIsSlowToReadException e) {
                Os.sleep(1);
                circuitBreaker.statefulThrowExceptionIfTimeout();
            }
        }
        responseUtf8Sink.reset();
        replyAndContinue = false;
    }

    private void sendReadyForNewQuery() throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareReadyForQuery();
        sendBufferAndReset();
    }

    private void setUuidBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long128.BYTES, valueLen);
        long hi = getLongUnsafe(address);
        long lo = getLongUnsafe(address + Long.BYTES);
        bindVariableService.setUuid(index, lo, hi);
    }

    private void shiftReceiveBuffer(long readOffsetBeforeParse) {
        final long len = recvBufferWriteOffset - readOffsetBeforeParse;
        LOG.debug().$("shift [offset=").$(readOffsetBeforeParse).$(", len=").$(len).I$();

        Vect.memmove(recvBuffer, recvBuffer + readOffsetBeforeParse, len);
        recvBufferWriteOffset = len;
        recvBufferReadOffset = 0;
    }

    static void dumpBuffer(char direction, long buffer, int len, boolean dumpNetworkTraffic) {
        if (dumpNetworkTraffic && len > 0) {
            StdoutSink.INSTANCE.put(direction);
            Net.dump(buffer, len);
        }
    }

    static PGPipelineEntry freeIfAbandoned(PGPipelineEntry pe) {
        if (pe != null && !pe.isPreparedStatement() && !pe.isPortal()) {
            return Misc.free(pe);
        }
        return pe;
    }

    static int getIntUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getInt(address));
    }

    static short getShortUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getShort(address));
    }

    int doReceive(int remaining) {
        final long data = recvBuffer + recvBufferWriteOffset;
        final int n = socket.recv(data, remaining);
        dumpBuffer('>', data, n, dumpNetworkTraffic);
        return n;
    }

    void prepareReadyForQuery() throws PeerDisconnectedException {
        LOG.debug().$("RNQ sent").$();
        sendBufferAndResetBlocking();

        responseUtf8Sink.put(MESSAGE_TYPE_READY_FOR_QUERY);
        responseUtf8Sink.putNetworkInt(Integer.BYTES + Byte.BYTES);
        switch (transactionState) {
            case IN_TRANSACTION:
                responseUtf8Sink.put(STATUS_IN_TRANSACTION);
                break;
            case ERROR_TRANSACTION:
                responseUtf8Sink.put(STATUS_IN_ERROR);
                break;
            default:
                responseUtf8Sink.put(STATUS_IDLE);
                break;
        }
    }

    void recv() throws PeerDisconnectedException, PeerIsSlowToWriteException, BadProtocolException {
        final int remaining = (int) (recvBufferSize - recvBufferWriteOffset);

        assertBufferSize(remaining > 0);

        int n = doReceive(Math.min(forceRecvFragmentationChunkSize, remaining));
        LOG.debug().$("recv [n=").$(n).I$();
        if (n < 0) {
            LOG.info().$("disconnected on read [code=").$(n).I$();
            throw PeerDisconnectedException.INSTANCE;
        }
        if (n == 0) {
            // The socket is not ready for read.
            throw PeerIsSlowToWriteException.INSTANCE;
        }

        recvBufferWriteOffset += n;
    }

    void sendBuffer(int offset, int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int n = socket.send(sendBuffer + offset, Math.min(size, forceSendFragmentationChunkSize));
        dumpBuffer('<', sendBuffer + offset, n, dumpNetworkTraffic);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n < size) {
            doSendWithRetries(offset + n, size - n);
        }
        sendBufferPtr = sendBuffer;
        bufferRemainingSize = 0;
        bufferRemainingOffset = 0;
    }

    public static class Portal implements Mutable {

        public CharSequence statementName = null;

        @Override
        public void clear() {
            statementName = null;
        }
    }

    private class PGConnectionBatchCallback implements BatchCallback {

        @Override
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence text) throws Exception {
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
            prepareForNewBatchQuery();
            circuitBreaker.resetTimer();
        }
    }

    private class ResponseUtf8Sink implements PGResponseSink, Mutable {

        private long bookmarkPtr = -1;

        @Override
        public void assignCallback(PGResumeCallback callback) {
            resumeCallback = callback;
        }

        @Override
        public void bookmark() {
            this.bookmarkPtr = sendBufferPtr;
        }

        @Override
        public void bump(int size) {
            sendBufferPtr += size;
        }

        @Override
        public void checkCapacity(long size) {
            if (sendBufferPtr + size < sendBufferLimit) {
                return;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public void clear() {
            reset();
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                final int size = us.size();
                if (size > 0) {
                    checkCapacity(size);
                    Utf8s.strCpy(us, size, sendBufferPtr);
                    sendBufferPtr += size;
                }
            }
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            checkCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, b);
            return this;
        }

        @Override
        public void put(BinarySequence sequence) {
            final long len = sequence.length();
            if (len > maxBlobSizeOnQuery) {
                setNullValue();
            } else {
                checkCapacity((int) (len + Integer.BYTES));
                // when we reach here the "long" length would have to fit in response buffer
                // if it was larger than integers it would never fit into integer-bound response buffer
                putInt(sendBufferPtr, (int) len);
                sendBufferPtr += Integer.BYTES;
                for (long x = 0; x < len; x++) {
                    Unsafe.getUnsafe().putByte(sendBufferPtr + x, sequence.byteAt(x));
                }
                sendBufferPtr += len;
            }
        }

        @Override
        public void putIntDirect(int value) {
            checkCapacity(Integer.BYTES);
            putIntUnsafe(0, value);
            sendBufferPtr += Integer.BYTES;
        }

        @Override
        public void putIntUnsafe(long offset, int value) {
            Unsafe.getUnsafe().putInt(sendBufferPtr + offset, value);
        }

        @Override
        public void putLen(long start) {
            putInt(start, (int) (sendBufferPtr - start));
        }

        @Override
        public void putLenEx(long start) {
            putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        @Override
        public void putNetworkDouble(double value) {
            checkCapacity(Double.BYTES);
            Unsafe.getUnsafe().putDouble(sendBufferPtr, Double.longBitsToDouble(Numbers.bswap(Double.doubleToLongBits(value))));
            sendBufferPtr += Double.BYTES;
        }

        @Override
        public void putNetworkFloat(float value) {
            checkCapacity(Float.BYTES);
            Unsafe.getUnsafe().putFloat(sendBufferPtr, Float.intBitsToFloat(Numbers.bswap(Float.floatToIntBits(value))));
            sendBufferPtr += Float.BYTES;
        }

        @Override
        public void putNetworkInt(int value) {
            checkCapacity(Integer.BYTES);
            putInt(sendBufferPtr, value);
            sendBufferPtr += Integer.BYTES;
        }

        @Override
        public void putNetworkLong(long value) {
            checkCapacity(Long.BYTES);
            putLong(sendBufferPtr, value);
            sendBufferPtr += Long.BYTES;
        }

        @Override
        public void putNetworkShort(short value) {
            checkCapacity(Short.BYTES);
            putShort(sendBufferPtr, value);
            sendBufferPtr += Short.BYTES;
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            // Once this is actually needed, the impl would look something like:
            // final long size = hi - lo;
            // ensureCapacity(size);
            // Vect.memcpy(sendBufferPtr, lo, size);
            // sendBufferPtr += size;
            // return this;
            throw new UnsupportedOperationException();
        }

        @Override
        public void putZ(CharSequence value) {
            put(value);
            checkCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
        }

        @Override
        public void reset() {
            sendBufferPtr = sendBuffer;
        }

        @Override
        public void resetToBookmark() {
            assert bookmarkPtr != -1;
            sendBufferPtr = bookmarkPtr;
            bookmarkPtr = -1;
        }

        @Override
        public void sendBufferAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendBuffer(bufferRemainingOffset, (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset));
            responseUtf8Sink.reset();
            replyAndContinue = false;
        }

        @Override
        public void setNullValue() {
            putIntDirect(INT_NULL_X);
        }

        @Override
        public long skipInt() {
            checkCapacity(Integer.BYTES);
            long checkpoint = sendBufferPtr;
            sendBufferPtr += Integer.BYTES;
            return checkpoint;
        }
    }
}
