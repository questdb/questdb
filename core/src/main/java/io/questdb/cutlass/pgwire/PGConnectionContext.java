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

package io.questdb.cutlass.pgwire;

import io.questdb.FactoryProvider;
import io.questdb.Metrics;
import io.questdb.QueryLogger;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.OperationFuture.QUERY_COMPLETE;
import static io.questdb.cutlass.pgwire.PGOids.*;
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
    public static final String TAG_OK = "OK";
    public static final String TAG_PSEUDO_SELECT = "PSEUDO_SELECT";
    public static final String TAG_ROLLBACK = "ROLLBACK";
    public static final String TAG_SELECT = "SELECT";
    public static final String TAG_SET = "SET";
    public static final String TAG_UPDATE = "UPDATE";
    private static final int COMMIT_TRANSACTION = 2;
    private static final int ERROR_TRANSACTION = 3;
    private static final int INT_BYTES_X = Numbers.bswap(Integer.BYTES);
    private static final int INT_NULL_X = Numbers.bswap(-1);
    private static final int IN_TRANSACTION = 1;
    private final static Log LOG = LogFactory.getLog(PGConnectionContext.class);
    private static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';
    private static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';
    private static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    private static final byte MESSAGE_TYPE_COPY_IN_RESPONSE = 'G';
    private static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    private static final byte MESSAGE_TYPE_EMPTY_QUERY = 'I';
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final byte MESSAGE_TYPE_NO_DATA = 'n';
    private static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';
    private static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    private static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    private static final byte MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE = 'S';
    private static final int NO_TRANSACTION = 0;
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final int PROTOCOL_TAIL_COMMAND_LENGTH = 64;
    private static final int ROLLING_BACK_TRANSACTION = 4;
    private static final int SSL_REQUEST = 80877103;
    private static final int SYNC_BIND = 3;
    private static final int SYNC_DESCRIBE = 2;
    private static final int SYNC_DESCRIBE_PORTAL = 4;
    private static final int SYNC_PARSE = 1;
    private static final String WRITER_LOCK_REASON = "pgConnection";
    private final BatchCallback batchCallback;
    private final ObjectPool<DirectBinarySequence> binarySequenceParamsPool;
    //stores result format codes (0=Text,1=Binary) from the latest bind message
    //we need it in case cursor gets invalidated and bind used non-default binary format for some column(s)
    //pg clients (like asyncpg) fail when format sent by server is not the same as requested in bind message
    private final IntList bindSelectColumnFormats = new IntList();
    private final IntList bindVariableTypes = new IntList();
    private final CharacterStore characterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final boolean dumpNetworkTraffic;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
    private final int maxBlobSizeOnQuery;
    private final Metrics metrics;
    private final CharSequenceObjHashMap<Portal> namedPortalMap;
    private final WeakMutableObjectPool<Portal> namedPortalPool;
    private final CharSequenceObjHashMap<NamedStatementWrapper> namedStatementMap;
    private final WeakMutableObjectPool<NamedStatementWrapper> namedStatementWrapperPool;
    private final Path path = new Path();
    private final ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters;
    private final QueryLogger queryLogger;
    private final int recvBufferSize;
    private final ResponseCharSinkBase responseCharSinkBase = new ResponseCharSinkBase();
    private final SecurityContextFactory securityContextFactory;
    private final IntList selectColumnTypes = new IntList();
    private final int sendBufferSize;
    private final IntList syncActions = new IntList(4);
    private final SCSequence tempSequence = new SCSequence();
    // insert 'statements' are cached only for the duration of user session
    private final AssociativeCache<TypesAndInsert> typesAndInsertCache;
    private final WeakSelfReturningObjectPool<TypesAndInsert> typesAndInsertPool;
    private final AssociativeCache<TypesAndSelect> typesAndSelectCache;
    private final WeakSelfReturningObjectPool<TypesAndSelect> typesAndSelectPool;
    private final AssociativeCache<TypesAndUpdate> typesAndUpdateCache;
    private final WeakSelfReturningObjectPool<TypesAndUpdate> typesAndUpdatePool;
    private final DirectCharSink utf8Sink;
    // this is a reference to types either from the context or named statement, where it is provided
    private IntList activeBindVariableTypes;
    //list of pair: column types (with format flag stored in first bit) AND additional type flag
    private IntList activeSelectColumnTypes;
    private Authenticator authenticator;
    private BindVariableService bindVariableService;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private boolean completed = true;
    private RecordCursor currentCursor = null;
    private RecordCursorFactory currentFactory = null;
    private boolean errorSkipToSync;
    private boolean freezeRecvBuffer;
    private boolean isEmptyQuery = false;
    private boolean isPausedQuery = false;
    private byte lastMsgType;
    private long maxReceiveRows;
    private long maxSendRows;
    private int parsePhaseBindVariableCount;
    private boolean queryContainsSecret;
    //command tag used when returning row count to client,
    //see CommandComplete (B) at https://www.postgresql.org/docs/current/protocol-message-formats.html
    private CharSequence queryTag;
    private CharSequence queryText;
    private long recvBuffer;
    private long recvBufferReadOffset = 0;
    private long recvBufferWriteOffset = 0;
    private boolean replyAndContinue;
    private PGResumeProcessor resumeProcessor;
    private Rnd rnd;
    private long rowCount;
    private long sendBuffer;
    private long sendBufferLimit;
    private long sendBufferPtr;
    private final PGResumeProcessor resumeExecuteCompleteRef = this::resumeCommandComplete;
    private boolean sendParameterDescription;
    private boolean sendRNQ = true; /* send ReadyForQuery message */
    private SqlExecutionContextImpl sqlExecutionContext;
    private long statementTimeout = -1L;
    private SuspendEvent suspendEvent;
    private boolean tlsSessionStarting = false;
    private long totalReceived = 0;
    private int transactionState = NO_TRANSACTION;
    private final PGResumeProcessor resumeQueryCompleteRef = this::resumeQueryComplete;
    private TypesAndInsert typesAndInsert = null;
    // these references are held by context only for a period of processing single request
    // in PF world this request can span multiple messages, but still, only for one request
    // the rationale is to be able to return "selectAndTypes" instance to thread-local
    // cache, which is "typesAndSelectCache". We typically do this after query results are
    // served to client or query errored out due to network issues
    private TypesAndSelect typesAndSelect = null;
    private boolean typesAndSelectIsCached = true;
    private TypesAndUpdate typesAndUpdate = null;
    private boolean typesAndUpdateIsCached = false;
    private final PGResumeProcessor resumeCursorQueryRef = this::resumeCursorQuery;
    private final PGResumeProcessor resumeComputeCursorSizeQueryRef = this::resumeComputeCursorSizeQuery;
    private final PGResumeProcessor resumeCursorExecuteRef = this::resumeCursorExecute;
    private final PGResumeProcessor setResumeComputeCursorSizeExecuteRef = this::setResumeComputeCursorSizeExecute;
    private NamedStatementWrapper wrapper;

    public PGConnectionContext(
            CairoEngine engine,
            PGWireConfiguration configuration,
            SqlExecutionContextImpl sqlExecutionContext,
            NetworkSqlExecutionCircuitBreaker circuitBreaker
    ) {
        super(
                configuration.getFactoryProvider().getPGWireSocketFactory(),
                configuration.getNetworkFacade(),
                LOG,
                engine.getMetrics().pgWire().connectionCountGauge()
        );
        this.engine = engine;
        queryLogger = engine.getConfiguration().getQueryLogger();
        this.utf8Sink = new DirectCharSink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize());
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        this.recvBufferSize = Numbers.ceilPow2(configuration.getRecvBufferSize());
        this.sendBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.forceSendFragmentationChunkSize = configuration.getForceSendFragmentationChunkSize();
        this.forceRecvFragmentationChunkSize = configuration.getForceRecvFragmentationChunkSize();
        this.characterStore = new CharacterStore(
                configuration.getCharacterStoreCapacity(),
                configuration.getCharacterStorePoolCapacity()
        );
        this.maxBlobSizeOnQuery = configuration.getMaxBlobSizeOnQuery();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.circuitBreaker = circuitBreaker;
        this.sqlExecutionContext = sqlExecutionContext;
        this.sqlExecutionContext.with(DenyAllSecurityContext.INSTANCE, bindVariableService, this.rnd = configuration.getRandom());
        this.namedStatementWrapperPool = new WeakMutableObjectPool<>(NamedStatementWrapper::new, configuration.getNamesStatementPoolCapacity()); // 32
        this.namedPortalPool = new WeakMutableObjectPool<>(Portal::new, configuration.getNamesStatementPoolCapacity()); // 32
        this.namedStatementMap = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
        this.pendingWriters = new ObjObjHashMap<>(configuration.getPendingWritersCacheSize());
        this.namedPortalMap = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
        this.binarySequenceParamsPool = new ObjectPool<>(DirectBinarySequence::new, configuration.getBinParamCountCapacity());

        this.metrics = engine.getMetrics();
        final boolean enableSelectCache = configuration.isSelectCacheEnabled();
        final int selectBlockCount = enableSelectCache ? configuration.getSelectCacheBlockCount() : 1;
        final int selectRowCount = enableSelectCache ? configuration.getSelectCacheRowCount() : 1;
        this.typesAndSelectCache = new AssociativeCache<>(
                selectBlockCount,
                selectRowCount,
                metrics.pgWire().cachedSelectsGauge(),
                metrics.pgWire().selectCacheHitCounter(),
                metrics.pgWire().selectCacheMissCounter()
        );
        this.typesAndSelectPool = new WeakSelfReturningObjectPool<>(TypesAndSelect::new, selectBlockCount * selectRowCount);

        final boolean enabledUpdateCache = configuration.isUpdateCacheEnabled();
        final int updateBlockCount = enabledUpdateCache ? configuration.getUpdateCacheBlockCount() : 1;
        final int updateRowCount = enabledUpdateCache ? configuration.getUpdateCacheRowCount() : 1;
        this.typesAndUpdateCache = new AssociativeCache<>(
                updateBlockCount,
                updateRowCount,
                metrics.pgWire().cachedUpdatesGauge()
        );
        this.typesAndUpdatePool = new WeakSelfReturningObjectPool<>(parent -> new TypesAndUpdate(parent, engine), updateBlockCount * updateRowCount);

        final boolean enableInsertCache = configuration.isInsertCacheEnabled();
        final int insertBlockCount = enableInsertCache ? configuration.getInsertCacheBlockCount() : 1;
        final int insertRowCount = enableInsertCache ? configuration.getInsertCacheRowCount() : 1;
        this.typesAndInsertCache = new AssociativeCache<>(insertBlockCount, insertRowCount);
        this.typesAndInsertPool = new WeakSelfReturningObjectPool<>(TypesAndInsert::new, insertBlockCount * insertRowCount);

        this.batchCallback = new PGConnectionBatchCallback();
        this.queryTag = TAG_OK;
        this.queryContainsSecret = false;
        FactoryProvider factoryProvider = configuration.getFactoryProvider();
        this.securityContextFactory = factoryProvider.getSecurityContextFactory();
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

    public static long getStringLength(
            long x,
            long limit,
            CharSequence errorMessage
    ) throws BadProtocolException {
        long len = Unsafe.getUnsafe().getByte(x) == 0 ? x : getStringLengthTedious(x, limit);
        if (len > -1) {
            return len;
        }
        // we did not find 0 within message limit
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

        freeBuffers();
        completed = true;
        prepareForNewQuery();
        clearWriters();
        evictNamedStatementWrappersAndClear();
        clearCursorAndFactory();

        // Clear every field, even if already cleaned to be on the safe side.
        bindSelectColumnFormats.clear();
        bindVariableTypes.clear();
        characterStore.clear();
        circuitBreaker.clear();

        clearPool(namedPortalMap, namedPortalPool, "named portal");
        clearPool(namedStatementMap, namedStatementWrapperPool, "named statement");

        pendingWriters.clear();
        responseCharSinkBase.reset();
        selectColumnTypes.clear();
        syncActions.clear();
        utf8Sink.clear();
        if (activeBindVariableTypes != null) {
            activeBindVariableTypes.clear();
        }
        if (activeSelectColumnTypes != null) {
            activeSelectColumnTypes.clear();
        }
        authenticator.clear();
        bindVariableService.clear();
        bufferRemainingOffset = 0;
        bufferRemainingSize = 0;
        completed = true;
        assert currentCursor == null;
        assert currentFactory == null;
        errorSkipToSync = false;
        freezeRecvBuffer = false;
        isEmptyQuery = false;
        isPausedQuery = false;
        lastMsgType = 0;
        maxReceiveRows = 0;
        maxSendRows = 0;
        parsePhaseBindVariableCount = 0;
        queryContainsSecret = false;
        queryTag = null;
        queryText = null;
        replyAndContinue = false;
        resumeProcessor = null;
        rowCount = 0;
        sendParameterDescription = false;
        sendRNQ = false;
        statementTimeout = -1L;
        suspendEvent = null;
        tlsSessionStarting = false;
        totalReceived = 0;
        transactionState = NO_TRANSACTION;
        assert typesAndInsert == null;
        assert typesAndSelect == null;
        typesAndSelectIsCached = true;
        assert typesAndUpdate == null;
        typesAndUpdateIsCached = false;
        wrapper = null;
    }

    @Override
    public void clearSuspendEvent() {
        suspendEvent = Misc.free(suspendEvent);
    }

    public void clearWriters() {
        closePendingWriters(false);
        pendingWriters.clear();
    }

    @Override
    public void close() {
        // We're about to close the context, so no need to return pending factory to cache.
        typesAndSelectIsCached = false;
        typesAndUpdateIsCached = false;
        clear();
        sqlExecutionContext.with(DenyAllSecurityContext.INSTANCE, null, null, -1, null);
        Misc.free(path);
        Misc.free(utf8Sink);
        Misc.free(authenticator);
        Misc.free(typesAndSelectCache);
        Misc.free(typesAndUpdateCache);
        Misc.free(typesAndInsertCache);
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
                    doSend(bufferRemainingOffset, bufferRemainingSize);
                }
                tlsSessionStarting = false;
                if (socket.startTlsSession() != 0) {
                    LOG.error().$("failed to create new TLS session").$();
                    throw BadProtocolException.INSTANCE;
                }
                // Start listening for read.
                throw PeerIsSlowToWriteException.INSTANCE;
            }
            handleAuthentication();

            try {
                if (isPausedQuery) {
                    isPausedQuery = false;
                    if (resumeProcessor != null) {
                        resumeProcessor.resume(true);
                    }
                } else if (bufferRemainingSize > 0) {
                    doSend(bufferRemainingOffset, bufferRemainingSize);
                    if (resumeProcessor != null) {
                        resumeProcessor.resume(false);
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
                                    || (operation == IOOperation.READ && readOffsetBeforeParse == -1)
                    ) {
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
                    parse(
                            recvBuffer + recvBufferReadOffset,
                            (int) (recvBufferWriteOffset - recvBufferReadOffset)
                    );
                }
            } catch (SqlException e) {
                handleException(e.getPosition(), e.getFlyweightMessage(), false, -1, true);
            } catch (ImplicitCastException e) {
                handleException(-1, e.getFlyweightMessage(), false, -1, true);
            } catch (CairoException e) {
                handleException(e.getPosition(), e.getFlyweightMessage(), e.isCritical(), e.getErrno(), e.isInterruption());
                if (e.isEntityDisabled()) {
                    throw PeerDisconnectedException.INSTANCE;
                }
            }
        } catch (PeerDisconnectedException | PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            // BAU, not error metric
            throw e;
        } catch (Throwable th) {
            metrics.pgWire().getErrorCounter().inc();
            throw th;
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

    public void setBooleanBindVariable(int index, int valueLen) throws SqlException {
        if (valueLen != 4 && valueLen != 5) {
            throw SqlException.$(0, "bad value for BOOLEAN parameter [index=").put(index).put(", valueLen=").put(valueLen).put(']');
        }
        bindVariableService.setBoolean(index, valueLen == 4);
    }

    public void setCharBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(address, address + valueLen, e)) {
            bindVariableService.setChar(index, characterStore.toImmutable().charAt(0));
        } else {
            LOG.error().$("invalid char UTF8 bytes [index=").$(index).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setDateBindVariable(int index, long address, int valueLen) throws SqlException, BadProtocolException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(address, address + valueLen, e)) {
            bindVariableService.define(index, ColumnType.DATE, 0);
            bindVariableService.setStr(index, characterStore.toImmutable());
        } else {
            LOG.error().$("invalid str UTF8 bytes [index=").$(index).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setDoubleBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Double.BYTES, valueLen);
        bindVariableService.setDouble(index, Double.longBitsToDouble(getLongUnsafe(address)));
    }

    public void setFloatBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Float.BYTES, valueLen);
        bindVariableService.setFloat(index, Float.intBitsToFloat(getIntUnsafe(address)));
    }

    public void setIntBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Integer.BYTES, valueLen);
        bindVariableService.setInt(index, getIntUnsafe(address));
    }

    public void setLongBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long.BYTES, valueLen);
        bindVariableService.setLong(index, getLongUnsafe(address));
    }

    public void setShortBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Short.BYTES, valueLen);
        bindVariableService.setShort(index, getShortUnsafe(address));
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
        if (Utf8s.utf8ToUtf16(address, address + valueLen, e)) {
            bindVariableService.setStr(index, characterStore.toImmutable());
        } else {
            LOG.error().$("invalid str UTF8 bytes [index=").$(index).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setSuspendEvent(SuspendEvent suspendEvent) {
        this.suspendEvent = suspendEvent;
    }

    public void setTimestampBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long.BYTES, valueLen);
        bindVariableService.setTimestamp(index, getLongUnsafe(address) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private static void bindParameterFormats(
            long lo,
            long msgLimit,
            short parameterFormatCount,
            IntList bindVariableTypes
    ) throws BadProtocolException {
        if (lo + Short.BYTES * parameterFormatCount <= msgLimit) {
            LOG.debug().$("processing bind formats [count=").$(parameterFormatCount).I$();
            for (int i = 0; i < parameterFormatCount; i++) {
                final short code = getShortUnsafe(lo + i * Short.BYTES);
                bindVariableTypes.setQuick(i, toParamBinaryType(code, bindVariableTypes.getQuick(i)));
            }
        } else {
            LOG.error().$("invalid format code count [value=").$(parameterFormatCount).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void bindSingleFormatForAll(long lo, long msgLimit, IntList activeBindVariableTypes) throws BadProtocolException {
        short code = getShort(lo, msgLimit, "could not read parameter formats");
        for (int i = 0, n = activeBindVariableTypes.size(); i < n; i++) {
            activeBindVariableTypes.setQuick(i, toParamBinaryType(code, activeBindVariableTypes.getQuick(i)));
        }
    }

    private static void ensureValueLength(int index, int required, int actual) throws BadProtocolException {
        if (required == actual) {
            return;
        }
        LOG.error()
                .$("bad parameter value length [required=").$(required)
                .$(", actual=").$(actual)
                .$(", index=").$(index)
                .I$();
        throw BadProtocolException.INSTANCE;
    }

    private static int getIntUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getInt(address));
    }

    private static short getShortUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getShort(address));
    }

    private static void setupBindVariables(long lo, IntList bindVariableTypes, int count) {
        bindVariableTypes.setPos(count);
        for (int i = 0; i < count; i++) {
            bindVariableTypes.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
    }

    private void appendBinColumn(Record record, int i) throws SqlException {
        BinarySequence sequence = record.getBin(i);
        if (sequence == null) {
            responseCharSinkBase.setNullValue();
        } else {
            // if length is above max we will error out the result set
            long blobSize = sequence.length();
            if (blobSize < maxBlobSizeOnQuery) {
                responseCharSinkBase.put(sequence);
            } else {
                throw SqlException.position(0)
                        .put("blob is too large [blobSize=").put(blobSize)
                        .put(", max=").put(maxBlobSizeOnQuery)
                        .put(", columnIndex=").put(i)
                        .put(']');
            }
        }
    }

    private void appendBooleanColumn(Record record, int columnIndex) {
        responseCharSinkBase.putNetworkInt(Byte.BYTES);
        responseCharSinkBase.put(record.getBool(columnIndex) ? 't' : 'f');
    }

    private void appendBooleanColumnBin(Record record, int columnIndex) {
        responseCharSinkBase.putNetworkInt(Byte.BYTES);
        responseCharSinkBase.put(record.getBool(columnIndex) ? (byte) 1 : (byte) 0);
    }

    private void appendByteColumn(Record record, int columnIndex) {
        long a = responseCharSinkBase.skip();
        responseCharSinkBase.put((int) record.getByte(columnIndex));
        responseCharSinkBase.putLenEx(a);
    }

    private void appendByteColumnBin(Record record, int columnIndex) {
        final byte value = record.getByte(columnIndex);
        responseCharSinkBase.putNetworkInt(Short.BYTES);
        responseCharSinkBase.putNetworkShort(value);
    }

    private void appendCharColumn(Record record, int columnIndex) {
        final char charValue = record.getChar(columnIndex);
        if (charValue == 0) {
            responseCharSinkBase.setNullValue();
        } else {
            long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(charValue);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendDateColumn(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            final long a = responseCharSinkBase.skip();
            PG_DATE_MILLI_TIME_Z_PRINT_FORMAT.format(longValue, DateFormatUtils.EN_LOCALE, null, responseCharSinkBase);
            responseCharSinkBase.putLenEx(a);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendDateColumnBin(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            responseCharSinkBase.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            responseCharSinkBase.putNetworkLong(longValue * 1000 - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendDoubleColumn(Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (doubleValue == doubleValue) {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(doubleValue);
            responseCharSinkBase.putLenEx(a);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendDoubleColumnBin(Record record, int columnIndex) {
        final double value = record.getDouble(columnIndex);
        if (value == value) {
            responseCharSinkBase.putNetworkInt(Double.BYTES);
            responseCharSinkBase.putNetworkDouble(value);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendFloatColumn(Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (floatValue == floatValue) {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(floatValue, 3);
            responseCharSinkBase.putLenEx(a);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendFloatColumnBin(Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (value == value) {
            responseCharSinkBase.putNetworkInt(Float.BYTES);
            responseCharSinkBase.putNetworkFloat(value);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendIPv4Col(Record record, int columnIndex) {
        int value = record.getIPv4(columnIndex);
        if (value == Numbers.IPv4_NULL) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            Numbers.intToIPv4Sink(responseCharSinkBase, value);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendIntCol(Record record, int i) {
        final int intValue = record.getInt(i);
        if (intValue != Numbers.INT_NaN) {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(intValue);
            responseCharSinkBase.putLenEx(a);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendIntColumnBin(Record record, int columnIndex) {
        final int value = record.getInt(columnIndex);
        if (value != Numbers.INT_NaN) {
            responseCharSinkBase.ensureCapacity(8);
            responseCharSinkBase.putIntUnsafe(0, INT_BYTES_X);
            responseCharSinkBase.putIntUnsafe(4, Numbers.bswap(value));
            responseCharSinkBase.bump(8);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendLong256Column(Record record, int columnIndex) {
        final Long256 long256Value = record.getLong256A(columnIndex);
        if (long256Value.getLong0() == Numbers.LONG_NaN &&
                long256Value.getLong1() == Numbers.LONG_NaN &&
                long256Value.getLong2() == Numbers.LONG_NaN &&
                long256Value.getLong3() == Numbers.LONG_NaN) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            Numbers.appendLong256(
                    long256Value.getLong0(),
                    long256Value.getLong1(),
                    long256Value.getLong2(),
                    long256Value.getLong3(),
                    responseCharSinkBase
            );
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendLongColumn(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(longValue);
            responseCharSinkBase.putLenEx(a);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendLongColumnBin(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            responseCharSinkBase.putNetworkInt(Long.BYTES);
            responseCharSinkBase.putNetworkLong(longValue);
        } else {
            responseCharSinkBase.setNullValue();
        }
    }

    private void appendRecord(Record record, int columnCount) throws SqlException {
        responseCharSinkBase.put(MESSAGE_TYPE_DATA_ROW); // data
        final long offset = responseCharSinkBase.skip();
        responseCharSinkBase.putNetworkShort((short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            final int type = activeSelectColumnTypes.getQuick(2 * i);
            final short columnBinaryFlag = getColumnBinaryFlag(type);
            final int typeTag = ColumnType.tagOf(type);

            final int tagWithFlag = toColumnBinaryType(columnBinaryFlag, typeTag);
            switch (tagWithFlag) {
                case BINARY_TYPE_INT:
                    appendIntColumnBin(record, i);
                    break;
                case ColumnType.INT:
                    appendIntCol(record, i);
                    break;
                case ColumnType.IPv4:
                    appendIPv4Col(record, i);
                    break;
                case ColumnType.STRING:
                case BINARY_TYPE_STRING:
                    appendStrColumn(record, i);
                    break;
                case ColumnType.SYMBOL:
                case BINARY_TYPE_SYMBOL:
                    appendSymbolColumn(record, i);
                    break;
                case BINARY_TYPE_LONG:
                    appendLongColumnBin(record, i);
                    break;
                case ColumnType.LONG:
                    appendLongColumn(record, i);
                    break;
                case ColumnType.SHORT:
                    appendShortColumn(record, i);
                    break;
                case BINARY_TYPE_DOUBLE:
                    appendDoubleColumnBin(record, i);
                    break;
                case ColumnType.DOUBLE:
                    appendDoubleColumn(record, i);
                    break;
                case BINARY_TYPE_FLOAT:
                    appendFloatColumnBin(record, i);
                    break;
                case BINARY_TYPE_SHORT:
                    appendShortColumnBin(record, i);
                    break;
                case BINARY_TYPE_DATE:
                    appendDateColumnBin(record, i);
                    break;
                case BINARY_TYPE_TIMESTAMP:
                    appendTimestampColumnBin(record, i);
                    break;
                case BINARY_TYPE_BYTE:
                    appendByteColumnBin(record, i);
                    break;
                case BINARY_TYPE_UUID:
                    appendUuidColumnBin(record, i);
                    break;
                case ColumnType.FLOAT:
                    appendFloatColumn(record, i);
                    break;
                case ColumnType.TIMESTAMP:
                    appendTimestampColumn(record, i);
                    break;
                case ColumnType.DATE:
                    appendDateColumn(record, i);
                    break;
                case ColumnType.BOOLEAN:
                    appendBooleanColumn(record, i);
                    break;
                case BINARY_TYPE_BOOLEAN:
                    appendBooleanColumnBin(record, i);
                    break;
                case ColumnType.BYTE:
                    appendByteColumn(record, i);
                    break;
                case ColumnType.BINARY:
                case BINARY_TYPE_BINARY:
                    appendBinColumn(record, i);
                    break;
                case ColumnType.CHAR:
                case BINARY_TYPE_CHAR:
                    appendCharColumn(record, i);
                    break;
                case ColumnType.LONG256:
                case BINARY_TYPE_LONG256:
                    appendLong256Column(record, i);
                    break;
                case ColumnType.GEOBYTE:
                    putGeoHashStringByteValue(record, i, activeSelectColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOSHORT:
                    putGeoHashStringShortValue(record, i, activeSelectColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOINT:
                    putGeoHashStringIntValue(record, i, activeSelectColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.GEOLONG:
                    putGeoHashStringLongValue(record, i, activeSelectColumnTypes.getQuick(2 * i + 1));
                    break;
                case ColumnType.NULL:
                    responseCharSinkBase.setNullValue();
                    break;
                case ColumnType.UUID:
                    appendUuidColumn(record, i);
                    break;
                default:
                    assert false;
            }
        }
        responseCharSinkBase.putLen(offset);
        rowCount++;
    }

    private void appendShortColumn(Record record, int columnIndex) {
        final long a = responseCharSinkBase.skip();
        responseCharSinkBase.put(record.getShort(columnIndex));
        responseCharSinkBase.putLenEx(a);
    }

    private void appendShortColumnBin(Record record, int columnIndex) {
        final short value = record.getShort(columnIndex);
        responseCharSinkBase.putNetworkInt(Short.BYTES);
        responseCharSinkBase.putNetworkShort(value);
    }

    private void appendSingleRecord(Record record, int columnCount) throws SqlException {
        try {
            appendRecord(record, columnCount);
        } catch (NoSpaceLeftInResponseBufferException e1) {
            // oopsie, buffer is too small for single record
            LOG.error().$("not enough space in buffer for row data [buffer=").$(sendBufferSize).I$();
            responseCharSinkBase.reset();
            freeFactory();
            throw CairoException.critical(0).put("server configuration error: not enough space in send buffer for row data");
        }
    }

    private void appendStrColumn(Record record, int columnIndex) {
        final CharSequence strValue = record.getStr(columnIndex);
        if (strValue == null) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(strValue);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendSymbolColumn(Record record, int columnIndex) {
        final CharSequence strValue = record.getSym(columnIndex);
        if (strValue == null) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            responseCharSinkBase.put(strValue);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendTimestampColumn(Record record, int i) {
        long a;
        long longValue = record.getTimestamp(i);
        if (longValue == Numbers.LONG_NaN) {
            responseCharSinkBase.setNullValue();
        } else {
            a = responseCharSinkBase.skip();
            TimestampFormatUtils.PG_TIMESTAMP_FORMAT.format(longValue, DateFormatUtils.EN_LOCALE, null, responseCharSinkBase);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendTimestampColumnBin(Record record, int columnIndex) {
        final long longValue = record.getTimestamp(columnIndex);
        if (longValue == Numbers.LONG_NaN) {
            responseCharSinkBase.setNullValue();
        } else {
            responseCharSinkBase.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            responseCharSinkBase.putNetworkLong(longValue - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        }
    }

    private void appendUuidColumn(Record record, int columnIndex) {
        final long lo = record.getLong128Lo(columnIndex);
        final long hi = record.getLong128Hi(columnIndex);
        if (Uuid.isNull(lo, hi)) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            Numbers.appendUuid(lo, hi, responseCharSinkBase);
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void appendUuidColumnBin(Record record, int columnIndex) {
        final long lo = record.getLong128Lo(columnIndex);
        final long hi = record.getLong128Hi(columnIndex);
        if (Uuid.isNull(lo, hi)) {
            responseCharSinkBase.setNullValue();
        } else {
            responseCharSinkBase.putNetworkInt(Long.BYTES * 2);
            responseCharSinkBase.putNetworkLong(hi);
            responseCharSinkBase.putNetworkLong(lo);
        }
    }

    //replace column formats in activeSelectColumnTypes with those from latest bind call
    private void applyLatestBindColumnFormats() {
        for (int i = 0; i < bindSelectColumnFormats.size(); i++) {
            int newValue = toColumnBinaryType(
                    (short) bindSelectColumnFormats.get(i),
                    toColumnType(activeSelectColumnTypes.getQuick(2 * i))
            );
            activeSelectColumnTypes.setQuick(2 * i, newValue);
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

    private long bindValuesAsStrings(long lo, long msgLimit, short parameterValueCount) throws BadProtocolException, SqlException {
        for (int j = 0; j < parameterValueCount; j++) {
            final int valueLen = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;

            if (valueLen != -1 && lo + valueLen <= msgLimit) {
                setStrBindVariable(j, lo, valueLen);
                lo += valueLen;
            } else if (valueLen != -1) {
                LOG.error()
                        .$("value length is outside of buffer [parameterIndex=").$(j)
                        .$(", valueLen=").$(valueLen)
                        .$(", messageRemaining=").$(msgLimit - lo)
                        .I$();
                throw BadProtocolException.INSTANCE;
            }
        }
        return lo;
    }

    private long bindValuesUsingSetters(
            long lo,
            long msgLimit,
            short parameterValueCount
    ) throws BadProtocolException, SqlException {
        for (int j = 0; j < parameterValueCount; j++) {
            final int valueLen = getInt(lo, msgLimit, "malformed bind variable");
            lo += Integer.BYTES;
            if (valueLen == -1) {
                // this is null we have already defaulted parameters to
                continue;
            }

            if (lo + valueLen <= msgLimit) {
                switch (activeBindVariableTypes.getQuick(j)) {
                    case X_B_PG_INT4:
                        setIntBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_INT8:
                        setLongBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_TIMESTAMP:
                        setTimestampBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_INT2:
                        setShortBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_FLOAT8:
                        setDoubleBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_FLOAT4:
                        setFloatBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_CHAR:
                        setCharBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_DATE:
                        setDateBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_BOOL:
                        setBooleanBindVariable(j, valueLen);
                        break;
                    case X_B_PG_BYTEA:
                        setBinBindVariable(j, lo, valueLen);
                        break;
                    case X_B_PG_UUID:
                        setUuidBindVariable(j, lo, valueLen);
                        break;
                    default:
                        setStrBindVariable(j, lo, valueLen);
                        break;
                }
                typesAndUpdateIsCached = true;
                typesAndSelectIsCached = true;
                lo += valueLen;
            } else {
                LOG.error()
                        .$("value length is outside of buffer [parameterIndex=").$(j)
                        .$(", valueLen=").$(valueLen)
                        .$(", messageRemaining=").$(msgLimit - lo)
                        .I$();
                throw BadProtocolException.INSTANCE;
            }
        }
        return lo;
    }

    private void buildSelectColumnTypes() {
        final RecordMetadata m = typesAndSelect.getFactory().getMetadata();
        final int columnCount = m.getColumnCount();
        activeSelectColumnTypes.setPos(2 * columnCount);

        for (int i = 0; i < columnCount; i++) {
            int columnType = m.getColumnType(i);
            int flags = GeoHashes.getBitFlags(columnType);
            activeSelectColumnTypes.setQuick(2 * i, columnType);
            activeSelectColumnTypes.setQuick(2 * i + 1, flags);
        }
    }

    private void checkSendBufferFitsProtocolCommand() throws PeerDisconnectedException {
        if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
            sendAndResetWait();
        }
    }

    private void clearCursorAndFactory() {
        resumeProcessor = null;
        currentCursor = Misc.free(currentCursor);
        // do not free factory, we may cache it
        currentFactory = null;
        // we resumed the cursor send the typeAndSelect will be null
        // we do not want to overwrite cache entries and potentially
        // leak memory
        if (typesAndSelect != null) {
            if (typesAndSelectIsCached) {
                typesAndSelectCache.put(queryText, typesAndSelect);
                // clear selectAndTypes so that context doesn't accidentally
                // free the factory when context finishes abnormally
                this.typesAndSelect = null;
            } else {
                this.typesAndSelect = Misc.free(this.typesAndSelect);
            }
        }

        if (typesAndUpdate != null) {
            if (typesAndUpdateIsCached) {
                assert queryText != null;
                typesAndUpdateCache.put(queryText, typesAndUpdate);
                this.typesAndUpdate = null;
            } else {
                typesAndUpdate = Misc.free(typesAndUpdate);
            }
        }
    }

    private <T extends Mutable> void clearPool(CharSequenceObjHashMap<T> map, WeakMutableObjectPool<T> pool, String poolName) {
        for (int i = 0, n = map.keys().size(); i < n; i++) {
            CharSequence key = map.keys().get(i);
            pool.push(map.get(key));
        }
        map.clear();
        int l = pool.resetLeased();
        if (l != 0) {
            LOG.critical().$(poolName).$(" pool is not empty at context clear [fd=").$(socket.getFd())
                    .$(" leased=").$(l).I$();
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

    private boolean compileQuery(boolean doLog) throws SqlException {
        if (queryText != null && queryText.length() > 0) {
            // try insert, peek because this is our private cache,
            // and we do not want to remove statement from it
            typesAndInsert = typesAndInsertCache.peek(queryText);

            // not found or not insert, try select
            // poll this cache because it is shared, and we do not want
            // select factory to be used by another thread concurrently
            if (typesAndInsert != null) {
                queryLogger.logExecQuery(LOG, doLog, getFd(), queryText, sqlExecutionContext.getSecurityContext());
                typesAndInsert.defineBindVariables(bindVariableService);
                queryTag = TAG_INSERT;
                return false;
            }

            typesAndUpdate = typesAndUpdateCache.poll(queryText);

            if (typesAndUpdate != null) {
                queryLogger.logExecQuery(LOG, doLog, getFd(), queryText, sqlExecutionContext.getSecurityContext());
                typesAndUpdate.defineBindVariables(bindVariableService);
                queryTag = TAG_UPDATE;
                typesAndUpdateIsCached = true;
                return false;
            }

            typesAndSelect = typesAndSelectCache.poll(queryText);

            if (typesAndSelect != null) {
                queryLogger.logExecQuery(LOG, doLog, getFd(), queryText, sqlExecutionContext.getSecurityContext());
                LOG.info().$("query cache used [fd=").$(getFd()).I$();
                // cache hit, define bind variables
                bindVariableService.clear();
                typesAndSelect.defineBindVariables(bindVariableService);
                queryTag = TAG_SELECT;
                return false;
            }

            // not cached - compile to see what it is
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);
                processCompiledQuery(cc);
            }
        } else {
            queryLogger.logEmptyQuery(LOG, doLog, getFd(), queryText, sqlExecutionContext.getSecurityContext());
            isEmptyQuery = true;
        }

        return true;
    }

    private void computeCursorSize() throws QueryPausedException {
        try {
            final long cursorRowCount = currentCursor.size();
            if (maxReceiveRows > 0) {
                this.maxSendRows = cursorRowCount > 0 ? Long.min(maxReceiveRows, cursorRowCount) : maxReceiveRows;
            } else {
                this.maxSendRows = Long.MAX_VALUE;
            }
        } catch (DataUnavailableException e) {
            isPausedQuery = true;
            throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
        }
    }

    private void configureContextFromNamedStatement(CharSequence statementName)
            throws BadProtocolException, SqlException {
        this.sendParameterDescription = statementName != null;

        if (wrapper != null) {
            LOG.debug().$("reusing existing wrapper").$();
            return;
        }

        // make sure there is no current wrapper is set, so that we don't assign values
        // from the wrapper back to context on the first pass where named statement is set up
        if (statementName != null) {
            LOG.debug().$("named statement [name=").$(statementName).I$();
            wrapper = namedStatementMap.get(statementName);
            if (wrapper != null) {
                setupVariableSettersFromWrapper(wrapper);
            } else {
                // todo: when we have nothing for prepared statement name we need to produce an error
                LOG.error().$("statement does not exist [name=").$(statementName).I$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    private void configurePortal(CharSequence portalName, CharSequence statementName) throws BadProtocolException {
        int index = namedPortalMap.keyIndex(portalName);
        if (index > -1) {
            Portal portal = namedPortalPool.pop();
            portal.statementName = statementName;
            namedPortalMap.putAt(index, Chars.toString(portalName), portal);
        } else {
            LOG.error().$("duplicate portal [name=").$(portalName).I$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private void configurePreparedStatement(CharSequence statementName) throws BadProtocolException {
        // this is a PARSE message asking us to setup named SQL
        // we need to keep SQL text in case our SQL cache expires
        // as well as PG types of the bind variables, which we will need to configure setters

        int index = namedStatementMap.keyIndex(statementName);
        if (index > -1) {
            wrapper = namedStatementWrapperPool.pop();
            wrapper.queryText = Chars.toString(queryText);
            // it's fine to compile pseudo-SELECT queries multiple times since they must be executed lazily
            wrapper.alreadyExecuted = (queryTag == TAG_OK || queryTag == TAG_CTAS ||
                    (queryTag == TAG_PSEUDO_SELECT && typesAndSelect == null) ||
                    queryTag == TAG_ALTER_ROLE || queryTag == TAG_CREATE_ROLE);
            wrapper.queryContainsSecret = queryContainsSecret;
            namedStatementMap.putAt(index, Chars.toString(statementName), wrapper);
            this.activeBindVariableTypes = wrapper.bindVariableTypes;
            this.activeSelectColumnTypes = wrapper.selectColumnTypes;
        } else {
            LOG.error().$("duplicate statement [name=").$(statementName).I$();
            throw BadProtocolException.INSTANCE;
        }
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

            dumpBuffer('<', sendBuffer + offset, m);

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

    private void dumpBuffer(char direction, long buffer, int len) {
        if (dumpNetworkTraffic && len > 0) {
            StdoutSink.INSTANCE.put(direction);
            Net.dump(buffer, len);
        }
    }

    private void evictNamedStatementWrappersAndClear() {
        if (namedStatementMap.size() > 0) {
            ObjList<CharSequence> names = namedStatementMap.keys();
            for (int i = 0, n = names.size(); i < n; i++) {
                CharSequence name = names.getQuick(i);
                namedStatementWrapperPool.push(namedStatementMap.get(name));
            }
            namedStatementMap.clear();
        }
    }

    private void executeInsert() throws SqlException, PeerDisconnectedException {
        TableWriterAPI writer;
        boolean recompileStale = true;
        for (int retries = 0; true; retries++) {
            try {
                switch (transactionState) {
                    case IN_TRANSACTION:
                        final InsertMethod m = typesAndInsert.getInsert().createMethod(sqlExecutionContext, this);
                        recompileStale = false;
                        try {
                            rowCount = m.execute();
                            writer = m.popWriter();
                            pendingWriters.put(writer.getTableToken(), writer);
                        } catch (Throwable e) {
                            Misc.free(m);
                            throw e;
                        }
                        break;
                    case ERROR_TRANSACTION:
                        // when transaction is in error state, skip execution
                        break;
                    default:
                        // in any other case we will commit in place
                        try (final InsertMethod m2 = typesAndInsert.getInsert().createMethod(sqlExecutionContext, this)) {
                            recompileStale = false;
                            rowCount = m2.execute();
                            m2.commit();
                        }
                        break;
                }
                prepareCommandComplete(true);
                return;
            } catch (TableReferenceOutOfDateException | WriterOutOfDateException ex) {
                if (!recompileStale || retries == TableReferenceOutOfDateException.MAX_RETRY_ATTEMPS) {
                    if (transactionState == IN_TRANSACTION) {
                        transactionState = ERROR_TRANSACTION;
                    }
                    throw ex;
                }
                LOG.info().$(ex.getFlyweightMessage()).$();
                Misc.free(typesAndInsert);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);
                    processCompiledQuery(cc);
                }
            } catch (Throwable e) {
                if (transactionState == IN_TRANSACTION) {
                    transactionState = ERROR_TRANSACTION;
                }
                throw e;
            }
        }
    }

    private void executeTag() {
        LOG.debug().$("executing [tag=").$(queryTag).I$();
        if (queryTag != null && TAG_OK != queryTag) {  //do not run this for OK tag (i.e.: create table)
            executeTag0();
        }
    }

    private void executeTag0() {
        switch (transactionState) {
            case COMMIT_TRANSACTION:
                try {
                    closePendingWriters(true);
                } finally {
                    pendingWriters.clear();
                    transactionState = NO_TRANSACTION;
                }
                break;
            case ROLLING_BACK_TRANSACTION:
                try {
                    closePendingWriters(false);
                } finally {
                    pendingWriters.clear();
                    transactionState = NO_TRANSACTION;
                }
                break;
            default:
                break;
        }
    }

    private void executeUpdate() throws SqlException, PeerDisconnectedException {
        boolean recompileStale = true;
        for (int retries = 0; recompileStale; retries++) {
            try {
                if (transactionState != ERROR_TRANSACTION) {
                    // when transaction is in error state, skip execution
                    executeUpdate0();
                    recompileStale = false;
                }
                prepareCommandComplete(true);
            } catch (TableReferenceOutOfDateException e) {
                if (retries == TableReferenceOutOfDateException.MAX_RETRY_ATTEMPS) {
                    if (transactionState == IN_TRANSACTION) {
                        transactionState = ERROR_TRANSACTION;
                    }
                    throw e;
                }
                LOG.info().$(e.getFlyweightMessage()).$();
                typesAndUpdate = Misc.free(typesAndUpdate);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);
                    processCompiledQuery(cc);
                }
            } catch (CairoException e) {
                if (!e.isAuthorizationError()) {
                    typesAndUpdate = Misc.free(typesAndUpdate);
                }
                if (transactionState == IN_TRANSACTION) {
                    transactionState = ERROR_TRANSACTION;
                }
                throw e;
            } catch (Throwable e) {
                typesAndUpdate = Misc.free(typesAndUpdate);
                if (transactionState == IN_TRANSACTION) {
                    transactionState = ERROR_TRANSACTION;
                }
                throw e;
            }
        }
    }

    private void executeUpdate0() throws SqlException {
        final CompiledQuery cq = typesAndUpdate.getCompiledQuery();
        final UpdateOperation op = cq.getUpdateOperation();
        op.start();

        // check if there is pending writer, which would be pending if there is active transaction
        // when we have writer, execution is synchronous
        TableToken tableToken = op.getTableToken();
        if (tableToken == null) {
            throw CairoException.critical(0).put("invalid update operation plan cached, table token is null");
        }
        final int index = pendingWriters.keyIndex(tableToken);
        if (index < 0) {
            op.withContext(sqlExecutionContext);
            TableWriterAPI tableWriterAPI = pendingWriters.valueAt(index);
            // Update implicitly commits. WAL table cannot do 2 commits in 1 call and require commits to be made upfront.
            tableWriterAPI.commit();
            tableWriterAPI.apply(op);
        } else {
            // execute against writer from the engine, or async
            try (OperationFuture fut = cq.execute(sqlExecutionContext, tempSequence, false)) {
                if (statementTimeout > 0) {
                    // Timeout is explicitly enforced here as during async execution we cannot rely on CircuitBreaker
                    // to enforce it. Why? When a TableWriter in unavailable then an async task will be put into
                    // TableWriter's task queue and will be executed when TableWriter becomes available. However, during this
                    // queueing there is nothing enforcing timeout. So we have to do it here.
                    // Alternatively, we could introduce timeout enforcement in the tasks queue. But it's not clear if it's worth the effort.
                    if (fut.await(statementTimeout) != QUERY_COMPLETE) {
                        if (op.isWriterClosePending()) {
                            // Writer has not tried to execute the command
                            freeUpdateCommand(op);
                        }
                        throw SqlException.$(0, "UPDATE query timeout ").put(statementTimeout).put(" ms");
                    }
                } else {
                    // Default timeouts, can be different for select and update part
                    fut.await();
                }
                rowCount = fut.getAffectedRowsCount();
            } catch (SqlTimeoutException ex) {
                // After timeout, TableWriter can still use the UpdateCommand and Execution Context
                if (op.isWriterClosePending()) {
                    freeUpdateCommand(op);
                }
                throw ex;
            } catch (SqlException | CairoException ex) {
                // These exceptions mean the UpdateOperation cannot be used by writer anymore, and it's safe to re-use it.
                throw ex;
            } catch (Throwable ex) {
                // Unknown exception, assume TableWriter can still use the UpdateCommand and Execution Context
                if (op.isWriterClosePending()) {
                    freeUpdateCommand(op);
                }
                throw ex;
            }
        }
    }

    private void freeBuffers() {
        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_PGW_CONN);
        this.sendBuffer = this.sendBufferPtr = this.sendBufferLimit = Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
    }

    private void freeFactory() {
        currentFactory = null;
        typesAndSelect = Misc.free(typesAndSelect);
    }

    private void freeUpdateCommand(UpdateOperation op) {
        // Create a copy of sqlExecutionContext here
        bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        SqlExecutionContextImpl newSqlExecutionContext = new SqlExecutionContextImpl(
                engine,
                sqlExecutionContext.getWorkerCount(),
                sqlExecutionContext.getSharedWorkerCount()
        );
        newSqlExecutionContext.with(
                sqlExecutionContext.getSecurityContext(),
                bindVariableService,
                sqlExecutionContext.getRandom(),
                sqlExecutionContext.getRequestFd(),
                circuitBreaker
        );
        sqlExecutionContext = newSqlExecutionContext;

        // Do not cache, let last closing party free the resources
        op.close();
        typesAndUpdate = null;
    }

    @Nullable
    private CharSequence getPortalName(long lo, long hi) throws BadProtocolException {
        if (hi - lo > 0) {
            return getString(lo, hi, "invalid UTF8 bytes in portal name");
        }
        return null;
    }

    @Nullable
    private CharSequence getStatementName(long lo, long hi) throws BadProtocolException {
        if (hi - lo > 0) {
            return getString(lo, hi, "invalid UTF8 bytes in statement name");
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

    private void handleAuthentication() throws PeerIsSlowToWriteException, PeerIsSlowToReadException, BadProtocolException, PeerDisconnectedException {
        if (authenticator.isAuthenticated()) {
            return;
        }
        int r;
        try {
            r = authenticator.handleIO();
            if (r == Authenticator.OK) {
                CharSequence principal = authenticator.getPrincipal();
                SecurityContext securityContext = securityContextFactory.getInstance(principal, SecurityContextFactory.PGWIRE);
                try {
                    securityContext.authorizePGWIRE();
                    sqlExecutionContext.with(securityContext, bindVariableService, rnd, getFd(), circuitBreaker);
                    r = authenticator.loginOK();
                } catch (CairoException e) {
                    r = authenticator.denyAccess();
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

        sendRNQ = true;

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
        resumeProcessor = null;
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
        responseCharSinkBase.put(MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE);
        tlsSessionStarting = true;
        sendAndReset();
    }

    /**
     * Returns address of where parsing stopped. If there are remaining bytes left
     * in the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parse(
            long address,
            int len
    ) throws Exception {
        // we will wait until we receive the entire header
        if (len < PREFIXED_MESSAGE_HEADER_LEN) {
            // we need to be able to read header and length
            return;
        }

        final byte type = Unsafe.getUnsafe().getByte(address);
        final int msgLen = getIntUnsafe(address + 1);
        LOG.debug()
                .$("received msg [type=").$((char) type)
                .$(", len=").$(msgLen)
                .I$();
        if (msgLen < 1) {
            LOG.error()
                    .$("invalid message length [type=").$(type)
                    .$(", msgLen=").$(msgLen)
                    .$(", recvBufferReadOffset=").$(recvBufferReadOffset)
                    .$(", recvBufferWriteOffset=").$(recvBufferWriteOffset)
                    .$(", totalReceived=").$(totalReceived)
                    .I$();
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
            LOG.debug()
                    .$("not enough data in buffer [expected=").$(msgLen)
                    .$(", have=").$(len)
                    .$(", recvBufferWriteOffset=").$(recvBufferWriteOffset)
                    .$(", recvBufferReadOffset=").$(recvBufferReadOffset)
                    .I$();
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

        switch (type) {
            case 'P': // parse
                sendRNQ = true;
                processParse(
                        address,
                        msgLo,
                        msgLimit
                );
                break;
            case 'X': // 'Terminate'
                throw PeerDisconnectedException.INSTANCE;
            case 'C':
                // close
                processClose(msgLo, msgLimit);
                sendRNQ = true;
                break;
            case 'B': // bind
                sendRNQ = true;
                processBind(msgLo, msgLimit);
                break;
            case 'E': // execute
                sendRNQ = true;
                processExec(msgLo, msgLimit);
                break;
            case 'S': // sync
                // At completion of each series of extended-query messages, the frontend should issue a Sync message.
                // This parameterless message causes the backend to close the current transaction if it's not inside a BEGIN/COMMIT transaction block
                // (“close” meaning to commit if no error, or roll back if error). Then a ReadyForQuery response is issued.
                // The purpose of Sync is to provide a resynchronization point for error recovery. When an error is detected while processing any extended-query message,
                // the backend issues ErrorResponse, then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal message processing.
                // (But note that no skipping occurs if an error is detected while processing Sync — this ensures that there is one and only one ReadyForQuery sent for each Sync.)
                processSyncActions();
                prepareReadyForQuery();
                prepareForNewQuery();
                sendRNQ = true;
                // fall thru
            case 'H': // flush
                // "The Flush message does not cause any specific output to be generated, but forces the backend to deliver any data pending in its output buffers.
                //  A Flush must be sent after any extended-query command except Sync, if the frontend wishes to examine the results of that command before issuing more commands.
                //  Without Flush, messages returned by the backend will be combined into the minimum possible number of packets to minimize network overhead."
                // some clients (asyncpg) chose not to send 'S' (sync) message
                // but instead fire 'H'. Can't wrap my head around as to why
                // query execution is so ambiguous
                if (syncActions.size() > 0) {
                    processSyncActions();
                }
                sendAndReset();
                break;
            case 'D': // describe
                sendRNQ = true;
                processDescribe(msgLo, msgLimit);
                break;
            case 'Q': // simple query
                sendRNQ = true;
                processQuery(msgLo, msgLimit);
                break;
            case 'd': // COPY data
                break;
            default:
                LOG.error().$("unknown message [type=").$(type).I$();
                throw BadProtocolException.INSTANCE;
        }
    }

    private void parseQueryText(long lo, long hi) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(lo, hi, e)) {
            queryText = characterStore.toImmutable();
            compileQuery(true);
            return;
        }
        LOG.error().$("invalid UTF8 bytes in parse query").$();
        throw BadProtocolException.INSTANCE;
    }

    private void prepareBindComplete() {
        responseCharSinkBase.put(MESSAGE_TYPE_BIND_COMPLETE);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
    }

    private void prepareCloseComplete() {
        responseCharSinkBase.put(MESSAGE_TYPE_CLOSE_COMPLETE);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
    }

    private void prepareDescribePortalResponse() {
        if (typesAndSelect != null) {
            try {
                prepareRowDescription();
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                LOG.error().$("not enough space in buffer for row description [buffer=").$(sendBufferSize).I$();
                responseCharSinkBase.reset();
                freeFactory();
                throw CairoException.critical(0).put("server configuration error: not enough space in send buffer for row description");
            }
        } else {
            prepareNoDataMessage();
        }
    }

    private void prepareDescribeResponse() {
        // only send parameter description when we have named statement
        if (sendParameterDescription) {
            prepareParameterDescription();
        }
        prepareDescribePortalResponse();
    }

    private void prepareEmptyQueryResponse() {
        LOG.debug().$("empty").$();
        responseCharSinkBase.put(MESSAGE_TYPE_EMPTY_QUERY);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
    }

    private void prepareError(int position, CharSequence message, boolean critical, int errno) {
        prepareErrorResponse(position, message);
        if (critical) {
            LOG.critical()
                    .$("error [msg=`").utf8(message)
                    .$("`, errno=").$(errno)
                    .I$();
        } else {
            LOG.error()
                    .$("error [msg=`").utf8(message)
                    .$("`, errno=").$(errno)
                    .I$();
        }
    }

    private void prepareErrorResponse(int position, CharSequence message) {
        responseCharSinkBase.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = responseCharSinkBase.skip();
        responseCharSinkBase.putAscii('C');
        responseCharSinkBase.putZ("00000");
        responseCharSinkBase.putAscii('M');
        responseCharSinkBase.putZ(message);
        responseCharSinkBase.putAscii('S');
        responseCharSinkBase.putZ("ERROR");
        if (position > -1) {
            responseCharSinkBase.putAscii('P').put(position + 1).put((byte) 0);
        }
        responseCharSinkBase.put((byte) 0);
        responseCharSinkBase.putLen(addr);
    }

    //clears whole state except for characterStore because top-level batch text is using it
    private void prepareForNewBatchQuery() {
        if (completed) {
            LOG.debug().$("prepare for new query").$();
            isEmptyQuery = false;
            bindVariableService.clear();
            currentCursor = Misc.free(currentCursor);
            typesAndInsert = null;
            clearCursorAndFactory();
            rowCount = 0;
            queryTag = TAG_OK;
            queryText = null;
            wrapper = null;
            syncActions.clear();
            freezeRecvBuffer = false;
            sendParameterDescription = false;
        }
    }

    private void prepareForNewQuery() {
        prepareForNewBatchQuery();
        characterStore.clear();
    }

    private void prepareNoDataMessage() {
        responseCharSinkBase.put(MESSAGE_TYPE_NO_DATA);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
    }

    private void prepareNonCriticalError(int position, CharSequence message) {
        prepareErrorResponse(position, message);
        LOG.error()
                .$("error [pos=").$(position)
                .$(", msg=`").utf8(message).$('`')
                .I$();
    }

    private void prepareParameterDescription() {
        responseCharSinkBase.put(MESSAGE_TYPE_PARAMETER_DESCRIPTION);
        final long l = responseCharSinkBase.skip();
        final int n = bindVariableService.getIndexedVariableCount();
        responseCharSinkBase.putNetworkShort((short) n);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                responseCharSinkBase.putIntDirect(toParamType(activeBindVariableTypes.getQuick(i)));
            }
        }
        responseCharSinkBase.putLen(l);
    }

    private void prepareParseComplete() {
        responseCharSinkBase.put(MESSAGE_TYPE_PARSE_COMPLETE);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
    }

    private void prepareRowDescription() {
        final RecordMetadata metadata = typesAndSelect.getFactory().getMetadata();
        ResponseCharSinkBase sink = responseCharSinkBase;
        sink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
        final long addr = sink.skip();
        final int n = activeSelectColumnTypes.size() / 2;
        sink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            final int typeFlag = activeSelectColumnTypes.getQuick(2 * i);
            final int columnType = toColumnType(ColumnType.isNull(typeFlag) ? ColumnType.STRING : typeFlag);
            sink.putZ(metadata.getColumnName(i));
            sink.putIntDirect(0); //tableOid ?
            sink.putNetworkShort((short) (i + 1)); //column number, starting from 1
            sink.putNetworkInt(PGOids.getTypeOid(columnType)); // type
            if (ColumnType.tagOf(columnType) < ColumnType.STRING) {
                // type size
                // todo: cache small endian type sizes and do not check if type is valid - its coming from metadata, must be always valid
                sink.putNetworkShort((short) ColumnType.sizeOf(columnType));
            } else {
                // type size
                sink.putNetworkShort((short) -1);
            }

            //type modifier
            sink.putIntDirect(INT_NULL_X);
            // this is special behaviour for binary fields to prevent binary data being hex encoded on the wire
            // format code
            sink.putNetworkShort(ColumnType.isBinary(columnType) ? 1 : getColumnBinaryFlag(typeFlag)); // format code
        }
        sink.putLen(addr);
    }

    private void processBind(long lo, long msgLimit) throws BadProtocolException, SqlException {
        sqlExecutionContext.getCircuitBreaker().resetTimer();

        short parameterFormatCount;
        short parameterValueCount;

        LOG.debug().$("bind").$();
        // portal name
        long hi = getStringLength(lo, msgLimit, "bad portal name length [msgType='B']");
        CharSequence portalName = getPortalName(lo, hi);
        // named statement
        lo = hi + 1;
        hi = getStringLength(lo, msgLimit, "bad prepared statement name length [msgType='B']");
        final CharSequence statementName = getStatementName(lo, hi);

        // clear currentCursor if it wasn't cleared by previous execute with maxRows or parse call
        if (currentCursor != null) {
            clearCursorAndFactory();
        }

        configureContextFromNamedStatement(statementName);
        if (portalName != null) {
            configurePortal(portalName, statementName);
        }

        //parameter format count
        lo = hi + 1;
        parameterFormatCount = getShort(lo, msgLimit, "could not read parameter format code count");
        lo += Short.BYTES;
        if (parameterFormatCount > 0) {
            if (parameterFormatCount == 1) {
                // same format applies to all parameters
                bindSingleFormatForAll(lo, msgLimit, activeBindVariableTypes);
            } else if (activeBindVariableTypes.size() > 0) {//client doesn't need to specify types in Parse message and can use those returned in ParameterDescription
                bindParameterFormats(lo, msgLimit, parameterFormatCount, activeBindVariableTypes);
            }
        }

        // parameter value count
        lo += parameterFormatCount * Short.BYTES;
        parameterValueCount = getShort(lo, msgLimit, "could not read parameter value count");

        LOG.debug().$("binding [parameterValueCount=").$(parameterValueCount).$(", thread=").$(Thread.currentThread().getId()).I$();

        //we now have all parameter counts, validate them
        validateParameterCounts(parameterFormatCount, parameterValueCount, parsePhaseBindVariableCount);

        lo += Short.BYTES;

        try {
            if (parameterValueCount > 0) {
                //client doesn't need to specify any type in Parse message and can just use types returned in ParameterDescription message
                if (this.parsePhaseBindVariableCount == parameterValueCount || activeBindVariableTypes.size() > 0) {
                    lo = bindValuesUsingSetters(lo, msgLimit, parameterValueCount);
                } else {
                    lo = bindValuesAsStrings(lo, msgLimit, parameterValueCount);
                }
            }
        } catch (SqlException | ImplicitCastException e) {
            freeFactory();
            typesAndUpdate = Misc.free(typesAndUpdate);
            throw e;
        }

        if (typesAndSelect != null) {
            bindSelectColumnFormats.clear();

            short columnFormatCodeCount = getShort(lo, msgLimit, "could not read result set column format codes");
            if (columnFormatCodeCount > 0) {

                final RecordMetadata m = typesAndSelect.getFactory().getMetadata();
                final int columnCount = m.getColumnCount();
                // apply format codes to the cursor column types
                // but check if there is message is consistent

                final long spaceNeeded = lo + (columnFormatCodeCount + 1) * Short.BYTES;
                if (spaceNeeded <= msgLimit) {
                    bindSelectColumnFormats.setPos(columnCount);

                    if (columnFormatCodeCount == columnCount) {
                        // good to go
                        for (int i = 0; i < columnCount; i++) {
                            lo += Short.BYTES;
                            final short code = getShortUnsafe(lo);
                            activeSelectColumnTypes.setQuick(2 * i, toColumnBinaryType(code, m.getColumnType(i)));
                            bindSelectColumnFormats.setQuick(i, code);
                            activeSelectColumnTypes.setQuick(2 * i + 1, 0);
                        }
                    } else if (columnFormatCodeCount == 1) {
                        lo += Short.BYTES;
                        final short code = getShortUnsafe(lo);
                        for (int i = 0; i < columnCount; i++) {
                            activeSelectColumnTypes.setQuick(2 * i, toColumnBinaryType(code, m.getColumnType(i)));
                            bindSelectColumnFormats.setQuick(i, code);
                            activeSelectColumnTypes.setQuick(2 * i + 1, 0);
                        }
                    } else {
                        LOG.error()
                                .$("could not process column format codes [fmtCount=").$(columnFormatCodeCount)
                                .$(", columnCount=").$(columnCount)
                                .I$();
                        throw BadProtocolException.INSTANCE;
                    }
                } else {
                    LOG.error()
                            .$("could not process column format codes [bufSpaceNeeded=").$(spaceNeeded)
                            .$(", bufSpaceAvail=").$(msgLimit)
                            .I$();
                    throw BadProtocolException.INSTANCE;
                }
            } else if (columnFormatCodeCount == 0) {
                //if count == 0 then we've to use default and clear binary flags that might come from cached statements
                final RecordMetadata m = typesAndSelect.getFactory().getMetadata();
                final int columnCount = m.getColumnCount();
                bindSelectColumnFormats.setPos(columnCount);

                for (int i = 0; i < columnCount; i++) {
                    activeSelectColumnTypes.setQuick(2 * i, toColumnBinaryType((short) 0, m.getColumnType(i)));
                    bindSelectColumnFormats.setQuick(i, 0);
                }
            }

        }

        syncActions.add(SYNC_BIND);
    }

    private void processClose(long lo, long msgLimit) throws BadProtocolException {
        final byte type = Unsafe.getUnsafe().getByte(lo);
        switch (type) {
            case 'S':
                lo = lo + 1;
                final long hi = getStringLength(lo, msgLimit, "bad prepared statement name length");
                removeNamedStatement(getStatementName(lo, hi));
                break;
            case 'P':
                lo = lo + 1;
                final long high = getStringLength(lo, msgLimit, "bad prepared statement name length");
                final CharSequence portalName = getPortalName(lo, high);
                if (portalName != null) {
                    final int index = namedPortalMap.keyIndex(portalName);
                    if (index < 0) {
                        namedPortalPool.push(namedPortalMap.valueAt(index));
                        namedPortalMap.removeAt(index);
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

    private void processCompiledQuery(CompiledQuery cq) throws SqlException {
        sqlExecutionContext.storeTelemetry(cq.getType(), TelemetryOrigin.POSTGRES);
        queryContainsSecret = false;

        switch (cq.getType()) {
            case CompiledQuery.CREATE_TABLE_AS_SELECT:
                queryTag = TAG_CTAS;
                rowCount = cq.getAffectedRowsCount();
                break;
            case CompiledQuery.EXPLAIN:
                // explain results should not be cached
                typesAndSelectIsCached = false;
                typesAndSelect = typesAndSelectPool.pop();
                typesAndSelect.of(cq.getRecordCursorFactory(), bindVariableService);
                queryTag = TAG_EXPLAIN;
            case CompiledQuery.SELECT:
                typesAndSelect = typesAndSelectPool.pop();
                typesAndSelect.of(cq.getRecordCursorFactory(), bindVariableService);
                queryTag = TAG_SELECT;
                LOG.debug().$("cache select [sql=").$(queryText).$(", thread=").$(Thread.currentThread().getId()).I$();
                break;
            case CompiledQuery.INSERT:
                queryTag = TAG_INSERT;
                typesAndInsert = typesAndInsertPool.pop();
                typesAndInsert.of(cq.getInsertOperation(), bindVariableService);
                if (bindVariableService.getIndexedVariableCount() > 0) {
                    LOG.debug().$("cache insert [sql=").$(queryText).$(", thread=").$(Thread.currentThread().getId()).I$();
                    // we can add insert to cache right away because it is local to the connection
                    typesAndInsertCache.put(queryText, typesAndInsert);
                }
                break;
            case CompiledQuery.UPDATE:
                queryTag = TAG_UPDATE;
                typesAndUpdate = typesAndUpdatePool.pop();
                typesAndUpdate.of(cq, bindVariableService);
                typesAndUpdateIsCached = bindVariableService.getIndexedVariableCount() > 0;
                break;
            case CompiledQuery.INSERT_AS_SELECT:
                queryTag = TAG_INSERT;
                rowCount = cq.getAffectedRowsCount();
                break;
            case CompiledQuery.PSEUDO_SELECT:
                final RecordCursorFactory factory = cq.getRecordCursorFactory();
                if (factory != null) {
                    // this query is non-cacheable
                    typesAndSelectIsCached = false;
                    typesAndSelect = typesAndSelectPool.pop();
                    typesAndSelect.of(cq.getRecordCursorFactory(), bindVariableService);
                }
                queryTag = TAG_PSEUDO_SELECT;
                break;
            case CompiledQuery.SET:
                queryTag = TAG_SET;
                break;
            case CompiledQuery.DEALLOCATE:
                queryTag = TAG_DEALLOCATE;
                removeNamedStatement(cq.getStatementName());
                break;
            case CompiledQuery.BEGIN:
                queryTag = TAG_BEGIN;
                transactionState = IN_TRANSACTION;
                break;
            case CompiledQuery.COMMIT:
                queryTag = TAG_COMMIT;
                if (transactionState != ERROR_TRANSACTION) {
                    transactionState = COMMIT_TRANSACTION;
                }
                break;
            case CompiledQuery.ROLLBACK:
                queryTag = TAG_ROLLBACK;
                transactionState = ROLLING_BACK_TRANSACTION;
                break;
            case CompiledQuery.ALTER_USER:
                queryTag = TAG_ALTER_ROLE;
                queryContainsSecret = sqlExecutionContext.containsSecret();
                break;
            case CompiledQuery.CREATE_USER:
                queryTag = TAG_CREATE_ROLE;
                queryContainsSecret = sqlExecutionContext.containsSecret();
                break;
            case CompiledQuery.ALTER:
                // future-proofing ALTER execution
                try (OperationFuture fut = cq.execute(sqlExecutionContext, tempSequence, true)) {
                    fut.await();
                }
                // fall thru
            default:
                // DDL
                queryTag = TAG_OK;
                break;
        }
    }

    private void processDescribe(long lo, long msgLimit)
            throws SqlException, BadProtocolException {
        sqlExecutionContext.getCircuitBreaker().resetTimer();

        boolean isPortal = Unsafe.getUnsafe().getByte(lo) == 'P';
        long hi = getStringLength(lo + 1, msgLimit, "bad prepared statement name length");

        CharSequence target = getPortalName(lo + 1, hi);
        LOG.debug().$("describe [name=").$(target).I$();
        if (isPortal && target != null) {
            Portal p = namedPortalMap.get(target);
            if (p != null) {
                target = p.statementName;
            } else {
                LOG.error().$("invalid portal [name=").$(target).I$();
                throw BadProtocolException.INSTANCE;
            }
        }

        configureContextFromNamedStatement(target);

        // initialize activeBindVariableTypes from bind variable service
        final int n = bindVariableService.getIndexedVariableCount();
        if (sendParameterDescription && n > 0 && activeBindVariableTypes.size() == 0) {
            activeBindVariableTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                activeBindVariableTypes.setQuick(i, Numbers.bswap(PGOids.getTypeOid(bindVariableService.getFunction(i).getType())));
            }
        }
        if (isPortal) {
            syncActions.add(SYNC_DESCRIBE_PORTAL);
        } else {
            syncActions.add(SYNC_DESCRIBE);
        }
    }

    private void processExec(long lo, long msgLimit) throws Exception {
        sqlExecutionContext.getCircuitBreaker().resetTimer();

        final long hi = getStringLength(lo, msgLimit, "bad portal name length");
        final CharSequence portalName = getPortalName(lo, hi);
        if (portalName != null) {
            LOG.info().$("execute portal [name=").$(portalName).I$();
        }

        lo = hi + 1;
        maxReceiveRows = getInt(lo, msgLimit, "could not read max rows value");

        processSyncActions();
        processExecute();
        wrapper = null;
    }

    private void processExecute() throws Exception {
        if (typesAndSelect != null) {
            LOG.debug().$("executing query").$();
            setupFactoryAndCursor();
            sendCursor(resumeCursorExecuteRef, resumeExecuteCompleteRef, setResumeComputeCursorSizeExecuteRef);
        } else if (typesAndInsert != null) {
            LOG.debug().$("executing insert").$();
            executeInsert();
        } else if (typesAndUpdate != null) {
            LOG.debug().$("executing update").$();
            executeUpdate();
        } else { // this must be an OK/SET/COMMIT/ROLLBACK or empty query
            executeTag();
            prepareCommandComplete(false);
        }
    }

    private void processParse(long address, long lo, long msgLimit) throws BadProtocolException, SqlException {
        sqlExecutionContext.getCircuitBreaker().resetTimer();

        // make sure there are no left-over sync actions
        // we are starting a new iteration of the parse
        syncActions.clear();

        // 'Parse'
        //message length
        long hi = getStringLength(lo, msgLimit, "bad prepared statement name length");

        // When we encounter statement name in the "parse" message
        // we need to ensure the wrapper is properly setup to deal with
        // "describe", "bind" message sequence that will follow next.
        // In that all parameter types that we need to infer will have to be added to the
        // "bindVariableTypes" list.
        // Perhaps this is a good idea to make named statement writer a part of the context
        final CharSequence statementName = getStatementName(lo, hi);

        //query text
        lo = hi + 1;
        hi = getStringLength(lo, msgLimit, "bad query text length");

        // clear currentCursor and factory if they weren't cleared by previous execute with maxRows
        if (currentCursor != null) {
            clearCursorAndFactory();
        }

        //TODO: parsePhaseBindVariableCount have to be checked before parseQueryText and fed into it to serve as type hints !
        parseQueryText(lo, hi);

        //parameter type count
        lo = hi + 1;
        this.parsePhaseBindVariableCount = getShort(lo, msgLimit, "could not read parameter type count");

        if (statementName != null) {
            LOG.info().$("prepare [name=").$(statementName).I$();
            configurePreparedStatement(statementName);
        } else {
            this.activeBindVariableTypes = bindVariableTypes;
            this.activeSelectColumnTypes = selectColumnTypes;
        }

        //process parameter types
        if (this.parsePhaseBindVariableCount > 0) {
            if (lo + Short.BYTES + this.parsePhaseBindVariableCount * 4L > msgLimit) {
                LOG.error()
                        .$("could not read parameters [parameterCount=").$(this.parsePhaseBindVariableCount)
                        .$(", offset=").$(lo - address)
                        .$(", remaining=").$(msgLimit - lo)
                        .I$();
                throw BadProtocolException.INSTANCE;
            }

            LOG.debug().$("params [count=").$(this.parsePhaseBindVariableCount).I$();
            setupBindVariables(lo + Short.BYTES, activeBindVariableTypes, this.parsePhaseBindVariableCount);
        } else if (this.parsePhaseBindVariableCount < 0) {
            LOG.error()
                    .$("invalid parameter count [parameterCount=").$(this.parsePhaseBindVariableCount)
                    .$(", offset=").$(lo - address)
                    .I$();
            throw BadProtocolException.INSTANCE;
        }

        if (typesAndSelect != null) {
            buildSelectColumnTypes();
        }

        syncActions.add(SYNC_PARSE);
    }

    // processes one or more queries (batch/script). "Simple Query" in PostgreSQL docs.
    private void processQuery(
            long lo,
            long limit
    ) throws Exception {
        prepareForNewQuery();
        isEmptyQuery = true; // assume SQL text contains no query until we find out otherwise
        CharacterStoreEntry e = characterStore.newEntry();

        if (Utf8s.utf8ToUtf16(lo, limit - 1, e)) {
            queryText = characterStore.toImmutable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compileBatch(queryText, sqlExecutionContext, batchCallback);
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

    private void processSyncActions() {
        try {
            for (int i = 0, n = syncActions.size(); i < n; i++) {
                switch (syncActions.getQuick(i)) {
                    case SYNC_PARSE:
                        prepareParseComplete();
                        break;
                    case SYNC_DESCRIBE:
                        prepareDescribeResponse();
                        break;
                    case SYNC_BIND:
                        prepareBindComplete();
                        break;
                    case SYNC_DESCRIBE_PORTAL:
                        prepareDescribePortalResponse();
                        break;
                }
            }
        } finally {
            syncActions.clear();
        }
    }

    private void putGeoHashStringByteValue(Record rec, int col, int bitFlags) {
        byte l = rec.getGeoByte(col);
        putGeoHashStringValue(l, bitFlags);
    }

    private void putGeoHashStringIntValue(Record rec, int col, int bitFlags) {
        int l = rec.getGeoInt(col);
        putGeoHashStringValue(l, bitFlags);
    }

    private void putGeoHashStringLongValue(Record rec, int col, int bitFlags) {
        long l = rec.getGeoLong(col);
        putGeoHashStringValue(l, bitFlags);
    }

    private void putGeoHashStringShortValue(Record rec, int col, int bitFlags) {
        short l = rec.getGeoShort(col);
        putGeoHashStringValue(l, bitFlags);
    }

    private void putGeoHashStringValue(long value, int bitFlags) {
        if (value == GeoHashes.NULL) {
            responseCharSinkBase.setNullValue();
        } else {
            final long a = responseCharSinkBase.skip();
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, responseCharSinkBase);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, responseCharSinkBase);
            }
            responseCharSinkBase.putLenEx(a);
        }
    }

    private void removeNamedStatement(CharSequence statementName) {
        if (statementName != null) {
            final int index = namedStatementMap.keyIndex(statementName);
            // do not freak out if client is closing statement we don't have
            // we could have reported error to client before statement was created
            if (index < 0) {
                namedStatementWrapperPool.push(namedStatementMap.valueAt(index));
                namedStatementMap.removeAt(index);
            }
        }
    }

    private void replyAndContinue() throws PeerDisconnectedException, PeerIsSlowToReadException {
        replyAndContinue = true;
        sendReadyForNewQuery();
        freezeRecvBuffer = false;
        clearRecvBuffer();
    }

    private void resumeCommandComplete(boolean queryWasPaused) throws PeerDisconnectedException {
        prepareCommandComplete(true);
    }

    private void resumeComputeCursorSizeQuery(boolean queryWasPaused) throws Exception {
        computeCursorSize();
        resumeProcessor = resumeCursorQueryRef;
        responseCharSinkBase.bookmark();
        sendCursor0(currentCursor.getRecord(), currentFactory.getMetadata().getColumnCount(), resumeQueryCompleteRef);
    }

    private void resumeCursorExecute(boolean queryWasPaused) throws Exception {
        final Record record = currentCursor.getRecord();
        final int columnCount = currentFactory.getMetadata().getColumnCount();
        if (!queryWasPaused) {
            // We resume after no space left in buffer,
            // so we have to write the last record to the buffer once again.
            appendSingleRecord(record, columnCount);
        }
        responseCharSinkBase.bookmark();
        sendCursor0(record, columnCount, resumeExecuteCompleteRef);
    }

    private void resumeCursorQuery(boolean queryWasPaused) throws Exception {
        final Record record = currentCursor.getRecord();
        final int columnCount = currentFactory.getMetadata().getColumnCount();
        if (!queryWasPaused) {
            // We resume after no space left in buffer,
            // so we have to write the last record to the buffer once again.
            appendSingleRecord(record, columnCount);
        }
        responseCharSinkBase.bookmark();
        sendCursor0(record, columnCount, resumeQueryCompleteRef);
        sendReadyForNewQuery();
    }

    private void resumeQueryComplete(boolean queryWasPaused) throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareCommandComplete(true);
        sendReadyForNewQuery();
    }

    private void sendAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException {
        doSend(bufferRemainingOffset, (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset));
        responseCharSinkBase.reset();
        replyAndContinue = false;
    }

    private void sendAndResetWait() throws PeerDisconnectedException {
        // This is simplified waited send for very limited use cases where introducing another state is an overkill.
        // This method busy waits to send buffer.
        while (true) {
            try {
                doSend(bufferRemainingOffset, (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset));
                break;
            } catch (PeerIsSlowToReadException e) {
                Os.sleep(1);
                circuitBreaker.statefulThrowExceptionIfTimeout();
            }
        }
        responseCharSinkBase.reset();
        replyAndContinue = false;
    }

    // This method is currently unused. it's used for the COPY sub-protocol, which is currently not implemented.
    // It's left here so when we add the sub-protocol later we won't need to reimplemented it.
    // We could keep it just in git history, but chances are nobody would recall to search for it there
    private void sendCopyInResponse(CairoEngine engine, TextLoader textLoader) throws PeerDisconnectedException, PeerIsSlowToReadException {
        TableToken tableToken = engine.getTableTokenIfExists(textLoader.getTableName());
        if (
                TableUtils.TABLE_EXISTS == engine.getTableStatus(
                        path,
                        tableToken
                )
        ) {
            responseCharSinkBase.put(MESSAGE_TYPE_COPY_IN_RESPONSE);
            long addr = responseCharSinkBase.skip();
            responseCharSinkBase.put((byte) 0); // TEXT (1=BINARY, which we do not support yet)

            try (TableWriter writer = engine.getWriter(tableToken, WRITER_LOCK_REASON)) {
                RecordMetadata metadata = writer.getMetadata();
                responseCharSinkBase.putNetworkShort((short) metadata.getColumnCount());
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    responseCharSinkBase.putNetworkShort((short) PGOids.getTypeOid(metadata.getColumnType(i)));
                }
            }
            responseCharSinkBase.putLen(addr);
        } else {
            final SqlException e = SqlException.tableDoesNotExist(0, textLoader.getTableName());
            prepareNonCriticalError(e.getPosition(), e.getFlyweightMessage());
            prepareReadyForQuery();
        }
        sendAndReset();
    }

    private void sendCursor(
            PGResumeProcessor cursorResumeProcessor,
            PGResumeProcessor commandCompleteResumeProcessor,
            PGResumeProcessor computeCursorSizeResumeProcessor
    ) throws Exception {
        // the assumption for now is that any record will fit into response buffer. This of course precludes us from
        // streaming large BLOBs, but, and it's a big one, PostgreSQL protocol for DataRow does not allow for
        // streaming anyway. On top of that Java PostgreSQL driver downloads data row fully. This simplifies our
        // approach for general queries. For streaming protocol we will code something else. PostgreSQL Java driver is
        // slow anyway.

        rowCount = 0;
        // this might fail due to missing data
        resumeProcessor = computeCursorSizeResumeProcessor;
        computeCursorSize();

        resumeProcessor = cursorResumeProcessor;
        responseCharSinkBase.bookmark();
        sendCursor0(currentCursor.getRecord(), currentFactory.getMetadata().getColumnCount(), commandCompleteResumeProcessor);
    }

    private void sendCursor0(Record record, int columnCount, PGResumeProcessor commandCompleteResumeProcessor) throws Exception {
        if (!circuitBreaker.isTimerSet()) {
            circuitBreaker.resetTimer();
        }

        try {
            while (currentCursor.hasNext()) {
                try {
                    try {
                        appendRecord(record, columnCount);
                        responseCharSinkBase.bookmark();
                    } catch (NoSpaceLeftInResponseBufferException e) {
                        responseCharSinkBase.resetToBookmark();
                        sendAndReset();
                        appendSingleRecord(record, columnCount);
                        responseCharSinkBase.bookmark();
                    }
                    if (rowCount >= maxSendRows) {
                        break;
                    }
                } catch (SqlException e) {
                    clearCursorAndFactory();
                    responseCharSinkBase.resetToBookmark();
                    throw e;
                }
            }
        } catch (DataUnavailableException e) {
            isPausedQuery = true;
            responseCharSinkBase.resetToBookmark();
            throw QueryPausedException.instance(e.getEvent(), sqlExecutionContext.getCircuitBreaker());
        }

        completed = maxSendRows <= 0 || rowCount < maxSendRows;
        if (completed) {
            clearCursorAndFactory();
            // at this point buffer can contain unsent data,
            // and it may not have enough space for the command
            if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
                resumeProcessor = commandCompleteResumeProcessor;
                sendAndReset();
            }
            prepareCommandComplete(true);
        } else {
            checkSendBufferFitsProtocolCommand();
            prepareSuspended();
            // Prevents re-sending current record row when buffer is sent fully.
            resumeProcessor = null;
        }
    }

    private void sendReadyForNewQuery() throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareReadyForQuery();
        sendAndReset();
    }

    private void setResumeComputeCursorSizeExecute(boolean queryWasPaused) throws Exception {
        computeCursorSize();
        resumeProcessor = resumeCursorExecuteRef;
        responseCharSinkBase.bookmark();
        sendCursor0(currentCursor.getRecord(), currentFactory.getMetadata().getColumnCount(), resumeExecuteCompleteRef);
    }

    private void setUuidBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long128.BYTES, valueLen);
        long hi = getLongUnsafe(address);
        long lo = getLongUnsafe(address + Long.BYTES);
        bindVariableService.setUuid(index, lo, hi);
    }

    private void setupFactoryAndCursor() throws SqlException {
        if (currentCursor == null) {
            boolean recompileStale = true;
            SqlExecutionCircuitBreaker circuitBreaker = sqlExecutionContext.getCircuitBreaker();

            if (!circuitBreaker.isTimerSet()) {
                circuitBreaker.resetTimer();
            }

            for (int retries = 0; recompileStale; retries++) {
                currentFactory = typesAndSelect.getFactory();
                try {
                    currentCursor = currentFactory.getCursor(sqlExecutionContext);
                    recompileStale = false;
                    // cache random if it was replaced
                    rnd = sqlExecutionContext.getRandom();
                } catch (TableReferenceOutOfDateException e) {
                    if (retries == TableReferenceOutOfDateException.MAX_RETRY_ATTEMPS) {
                        throw e;
                    }
                    LOG.info().$(e.getFlyweightMessage()).$();
                    freeFactory();
                    compileQuery(false);
                    buildSelectColumnTypes();
                    applyLatestBindColumnFormats();
                } catch (Throwable e) {
                    freeFactory();
                    throw e;
                }
            }
        }
    }

    private void setupVariableSettersFromWrapper(
            @Transient NamedStatementWrapper wrapper
    ) throws SqlException {
        queryText = wrapper.queryText;
        if (!wrapper.queryContainsSecret) {
            LOG.debug().$("wrapper query [q=`").$(wrapper.queryText).$("`]").$();
        }
        this.activeBindVariableTypes = wrapper.bindVariableTypes;
        this.parsePhaseBindVariableCount = wrapper.bindVariableTypes.size();
        this.activeSelectColumnTypes = wrapper.selectColumnTypes;
        if (!wrapper.alreadyExecuted && compileQuery(false) && typesAndSelect != null) {
            buildSelectColumnTypes();
        }
        // We'll have to compile/execute the statement next time.
        wrapper.alreadyExecuted = false;
    }

    private void shiftReceiveBuffer(long readOffsetBeforeParse) {
        final long len = recvBufferWriteOffset - readOffsetBeforeParse;
        LOG.debug()
                .$("shift [offset=").$(readOffsetBeforeParse)
                .$(", len=").$(len)
                .I$();

        Vect.memmove(recvBuffer, recvBuffer + readOffsetBeforeParse, len);
        recvBufferWriteOffset = len;
        recvBufferReadOffset = 0;
    }

    private void validateParameterCounts(short parameterFormatCount, short parameterValueCount, int parameterTypeCount) throws BadProtocolException {
        if (parameterValueCount > 0) {
            if (parameterValueCount < parameterTypeCount) {
                LOG.error().$("parameter type count must be less or equals to number of parameters values").$();
                throw BadProtocolException.INSTANCE;
            }
            if (parameterFormatCount > 1 && parameterFormatCount != parameterValueCount) {
                LOG.error().$("parameter format count and parameter value count must match").$();
                throw BadProtocolException.INSTANCE;
            }
        }
    }

    int doReceive(int remaining) {
        final long data = recvBuffer + recvBufferWriteOffset;
        final int n = socket.recv(data, remaining);
        dumpBuffer('>', data, n);
        return n;
    }

    void doSend(int offset, int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int n = socket.send(sendBuffer + offset, Math.min(size, forceSendFragmentationChunkSize));
        dumpBuffer('<', sendBuffer + offset, n);
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

    void prepareCommandComplete(boolean addRowCount) throws PeerDisconnectedException {
        checkSendBufferFitsProtocolCommand();
        if (isEmptyQuery) {
            prepareEmptyQueryResponse();
        } else {
            responseCharSinkBase.put(MESSAGE_TYPE_COMMAND_COMPLETE);
            long addr = responseCharSinkBase.skip();
            if (addRowCount) {
                if (queryTag == TAG_INSERT) {
                    LOG.debug().$("insert [rowCount=").$(rowCount).I$();
                    responseCharSinkBase.put(queryTag).putAscii(" 0 ").put(rowCount).put((byte) 0);
                } else {
                    LOG.debug().$("other [rowCount=").$(rowCount).I$();
                    responseCharSinkBase.put(queryTag).putAscii(' ').put(rowCount).put((byte) 0);
                }
            } else {
                LOG.debug().$("no row count").$();
                responseCharSinkBase.put(queryTag).put((byte) 0);
            }
            responseCharSinkBase.putLen(addr);
        }
    }

    void prepareReadyForQuery() throws PeerDisconnectedException {
        if (sendRNQ) {
            LOG.debug().$("RNQ sent").$();
            checkSendBufferFitsProtocolCommand();

            responseCharSinkBase.put(MESSAGE_TYPE_READY_FOR_QUERY);
            responseCharSinkBase.putNetworkInt(Integer.BYTES + Byte.BYTES);
            switch (transactionState) {
                case IN_TRANSACTION:
                    responseCharSinkBase.put(STATUS_IN_TRANSACTION);
                    break;
                case ERROR_TRANSACTION:
                    responseCharSinkBase.put(STATUS_IN_ERROR);
                    break;
                default:
                    responseCharSinkBase.put(STATUS_IDLE);
                    break;
            }
            sendRNQ = false;
        }
    }

    void prepareSuspended() {
        LOG.debug().$("suspended").$();
        responseCharSinkBase.put(MESSAGE_TYPE_PORTAL_SUSPENDED);
        responseCharSinkBase.putIntDirect(INT_BYTES_X);
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

    @FunctionalInterface
    private interface PGResumeProcessor {
        void resume(boolean queryWasPaused) throws Exception;
    }

    public static class NamedStatementWrapper implements Mutable {

        public final IntList bindVariableTypes = new IntList();
        public final IntList selectColumnTypes = new IntList();
        // Used for statements that are executed as a part of compilation (PREPARE), such as DDLs.
        public boolean alreadyExecuted = false;
        public boolean queryContainsSecret = false;
        public CharSequence queryText = null;

        @Override
        public void clear() {
            queryText = null;
            bindVariableTypes.clear();
            selectColumnTypes.clear();
        }
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
            try {
                PGConnectionContext.this.queryText = text;
                processCompiledQuery(cq);

                if (typesAndSelect != null) {
                    activeSelectColumnTypes = selectColumnTypes;
                    buildSelectColumnTypes();
                    assert queryText != null;
                    queryTag = TAG_SELECT;
                    setupFactoryAndCursor();
                    prepareRowDescription();
                    maxReceiveRows = 0; // unlimited
                    sendCursor(resumeCursorQueryRef, resumeQueryCompleteRef, resumeComputeCursorSizeQueryRef);
                } else if (typesAndInsert != null) {
                    executeInsert();
                } else if (typesAndUpdate != null) {
                    executeUpdate();
                } else if (cq.getType() == CompiledQuery.INSERT_AS_SELECT ||
                        cq.getType() == CompiledQuery.CREATE_TABLE_AS_SELECT) {
                    prepareCommandComplete(true);
                } else {
                    executeTag();
                    prepareCommandComplete(false);
                }

                sqlExecutionContext.getCircuitBreaker().unsetTimer();
            } catch (QueryPausedException e) {
                // keep circuit breaker's timer as is
                throw e;
            } catch (Throwable e) {
                sqlExecutionContext.getCircuitBreaker().unsetTimer();
                throw e;
            }
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
            sendRNQ = true;
            prepareForNewBatchQuery();
            PGConnectionContext.this.typesAndInsert = null;
            PGConnectionContext.this.typesAndUpdate = null;
            PGConnectionContext.this.typesAndSelect = null;
            circuitBreaker.resetTimer();
        }
    }

    private class ResponseCharSinkBase implements Utf8Sink {

        private long bookmarkPtr = -1;

        public void bookmark() {
            this.bookmarkPtr = sendBufferPtr;
        }

        public void bump(int size) {
            sendBufferPtr += size;
        }

        @Override
        public Utf8Sink put(long lo, long hi) {
            // Once this is actually needed, the impl would look something like:
            // final long size = hi - lo;
            // ensureCapacity(size);
            // Vect.memcpy(sendBufferPtr, lo, size);
            // sendBufferPtr += size;
            // return this;
            throw new UnsupportedOperationException();
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            // this method is only called by date format utility to print timezone name
            if (us != null) {
                final int size = us.size();
                if (size > 0) {
                    ensureCapacity(size);
                    Utf8s.strCpy(us, size, sendBufferPtr);
                    sendBufferPtr += size;
                }
            }
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, b);
            return this;
        }

        public void put(BinarySequence sequence) {
            final long len = sequence.length();
            if (len > maxBlobSizeOnQuery) {
                setNullValue();
            } else {
                ensureCapacity((int) (len + Integer.BYTES));
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

        public void putIntDirect(int value) {
            ensureCapacity(Integer.BYTES);
            putIntUnsafe(0, value);
            sendBufferPtr += Integer.BYTES;
        }

        public void putIntUnsafe(long offset, int value) {
            Unsafe.getUnsafe().putInt(sendBufferPtr + offset, value);
        }

        public void putLen(long start) {
            putInt(start, (int) (sendBufferPtr - start));
        }

        public void putLenEx(long start) {
            putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        public void putNetworkDouble(double value) {
            ensureCapacity(Double.BYTES);
            Unsafe.getUnsafe().putDouble(sendBufferPtr, Double.longBitsToDouble(Numbers.bswap(Double.doubleToLongBits(value))));
            sendBufferPtr += Double.BYTES;
        }

        public void putNetworkFloat(float value) {
            ensureCapacity(Float.BYTES);
            Unsafe.getUnsafe().putFloat(sendBufferPtr, Float.intBitsToFloat(Numbers.bswap(Float.floatToIntBits(value))));
            sendBufferPtr += Float.BYTES;
        }

        public void putNetworkInt(int value) {
            ensureCapacity(Integer.BYTES);
            putInt(sendBufferPtr, value);
            sendBufferPtr += Integer.BYTES;
        }

        public void putNetworkLong(long value) {
            ensureCapacity(Long.BYTES);
            putLong(sendBufferPtr, value);
            sendBufferPtr += Long.BYTES;
        }

        public void putNetworkShort(short value) {
            ensureCapacity(Short.BYTES);
            putShort(sendBufferPtr, value);
            sendBufferPtr += Short.BYTES;
        }

        public void resetToBookmark() {
            assert bookmarkPtr != -1;
            sendBufferPtr = bookmarkPtr;
            bookmarkPtr = -1;
        }

        private void ensureCapacity(long size) {
            if (sendBufferPtr + size < sendBufferLimit) {
                return;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        void putZ(CharSequence value) {
            put(value);
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
        }

        void reset() {
            sendBufferPtr = sendBuffer;
        }

        void setNullValue() {
            putIntDirect(INT_NULL_X);
        }

        long skip() {
            ensureCapacity(Integer.BYTES);
            long checkpoint = sendBufferPtr;
            sendBufferPtr += Integer.BYTES;
            return checkpoint;
        }
    }
}
