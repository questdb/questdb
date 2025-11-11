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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.griffin.BatchCallback;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.network.IOContext;
import io.questdb.network.IOOperation;
import io.questdb.network.Net;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.QueryPausedException;
import io.questdb.network.SuspendEvent;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.ObjectPool;
import io.questdb.std.ObjectStackPool;
import io.questdb.std.Rnd;
import io.questdb.std.SimpleAssociativeCache;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.Vect;
import io.questdb.std.WeakSelfReturningObjectPool;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.function.Consumer;

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
    static final int IMPLICIT_TRANSACTION = 0;
    static final int INT_BYTES_X = Numbers.bswap(Integer.BYTES);
    static final int INT_NULL_X = Numbers.bswap(-1);
    static final int IN_TRANSACTION = 1;
    static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';
    static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';
    static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    static final byte MESSAGE_TYPE_EMPTY_QUERY = 'I';
    static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    static final byte MESSAGE_TYPE_NO_DATA = 'n';
    static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';
    static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';
    static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    private static final int CACHE_HIT_INSERT_INVALID = 1;
    private static final int CACHE_HIT_INSERT_VALID = 2;
    private static final int CACHE_HIT_SELECT_INVALID = 3;
    private static final int CACHE_HIT_SELECT_VALID = 4;
    private static final int CACHE_MISS = 0;
    private static final Log LOG = LogFactory.getLog(PGConnectionContext.class);
    // Timeout to prevent getting stuck while draining socket's receive buffer
    // before closing the socket. Ensures exit if malformed client keeps sending data.
    private static final long MALFORMED_CLIENT_READ_TIMEOUT_MILLIS = 5000;
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private static final byte MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE = 'S';
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final int PROTOCOL_TAIL_COMMAND_LENGTH = 64;
    private static final int SSL_REQUEST = 80877103;
    private final BatchCallback batchCallback;
    private final ObjectPool<DirectBinarySequence> binarySequenceParamsPool;
    private final BindVariableService bindVariableService;
    private final IntList bindVariableTypes = new IntList();
    private final CharacterStore bindVariableValuesCharacterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final PGConfiguration configuration;
    private final DirectUtf8String directUtf8NamedPortal = new DirectUtf8String();
    private final DirectUtf8String directUtf8NamedStatement = new DirectUtf8String();
    private final boolean dumpNetworkTraffic;
    private final CairoEngine engine;
    private final ObjectStackPool<PGPipelineEntry> entryPool;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
    private final int maxBlobSize;
    private final Metrics metrics;
    private final Utf8SequenceObjHashMap<PGPipelineEntry> namedPortals;
    private final Utf8SequenceObjHashMap<PGPipelineEntry> namedStatements;
    private final ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters;
    private final ArrayDeque<PGPipelineEntry> pipeline = new ArrayDeque<>();
    private final Consumer<? super Utf8Sequence> preparedStatementDeallocator = this::deallocateNamedStatement;
    private final ResponseUtf8Sink responseUtf8Sink = new ResponseUtf8Sink();
    private final Rnd rnd;
    private final SecurityContextFactory securityContextFactory;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final CharacterStore sqlTextCharacterStore;
    private final WeakSelfReturningObjectPool<TypesAndInsert> taiPool;
    private final AssociativeCache<TypesAndSelect> tasCache;
    private final SCSequence tempSequence = new SCSequence();
    private final DirectUtf8String utf8String = new DirectUtf8String();
    private SocketAuthenticator authenticator;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private boolean freezeRecvBuffer;
    private int namedStatementLimit;
    // PG wire protocol has two phases:
    // phase 1 - fill up the pipeline. In this case the current entry is the entry being populated
    // phase 2 - "sync" the pipeline. This is the execution phase and the current entry is the one being executed.
    private PGPipelineEntry pipelineCurrentEntry;
    private long recvBuffer;
    private long recvBufferReadOffset = 0;
    private int recvBufferSize;
    private long recvBufferWriteOffset = 0;
    private PGResumeCallback resumeCallback;
    private long sendBuffer;
    private long sendBufferLimit;
    private long sendBufferPtr;
    private int sendBufferSize;
    private SuspendEvent suspendEvent;
    // insert 'statements' are cached only for the duration of user session
    private SimpleAssociativeCache<TypesAndInsert> taiCache;
    private final PGResumeCallback msgFlushRef = this::msgFlush0;
    private boolean tlsSessionStarting = false;
    private long totalReceived = 0;
    private int transactionState = IMPLICIT_TRANSACTION;
    private final PGResumeCallback msgSyncRef = this::msgSync0;

    public PGConnectionContext(
            CairoEngine engine,
            PGConfiguration configuration,
            SqlExecutionContextImpl sqlExecutionContext,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            AssociativeCache<TypesAndSelect> tasCache
    ) {
        super(
                configuration.getFactoryProvider().getPGWireSocketFactory(),
                configuration.getNetworkFacade(),
                LOG
        );

        try {
            this.engine = engine;
            this.configuration = configuration;
            this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
            this.recvBufferSize = configuration.getRecvBufferSize();
            this.sendBufferSize = configuration.getSendBufferSize();
            this.forceSendFragmentationChunkSize = configuration.getForceSendFragmentationChunkSize();
            this.forceRecvFragmentationChunkSize = configuration.getForceRecvFragmentationChunkSize();
            this.sqlTextCharacterStore = new CharacterStore(
                    configuration.getCharacterStoreCapacity(),
                    configuration.getCharacterStorePoolCapacity()
            );
            this.bindVariableValuesCharacterStore = new CharacterStore(
                    configuration.getCharacterStoreCapacity(),
                    configuration.getCharacterStorePoolCapacity()
            );
            this.maxBlobSize = configuration.getMaxBlobSizeOnQuery();
            this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
            this.circuitBreaker = circuitBreaker;
            this.sqlExecutionContext = sqlExecutionContext;
            this.sqlExecutionContext.with(DenyAllSecurityContext.INSTANCE, bindVariableService, this.rnd = configuration.getRandom());
            this.namedStatements = new Utf8SequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
            this.pendingWriters = new ObjObjHashMap<>(configuration.getPendingWritersCacheSize());
            this.namedPortals = new Utf8SequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
            this.binarySequenceParamsPool = new ObjectPool<>(DirectBinarySequence::new, configuration.getBinParamCountCapacity());
            this.metrics = engine.getMetrics();
            this.tasCache = tasCache;
            this.entryPool = new ObjectStackPool<>(() -> new PGPipelineEntry(engine), configuration.getPipelineCapacity());
            final boolean enableInsertCache = configuration.isInsertCacheEnabled();
            final int insertBlockCount = enableInsertCache ? configuration.getInsertCacheBlockCount() : 1;
            final int insertRowCount = enableInsertCache ? configuration.getInsertCacheRowCount() : 1;
            this.taiCache = new SimpleAssociativeCache<>(insertBlockCount, insertRowCount);
            this.taiPool = new WeakSelfReturningObjectPool<>(TypesAndInsert::new, insertBlockCount * insertRowCount);
            this.namedStatementLimit = configuration.getNamedStatementLimit();

            this.batchCallback = new PGConnectionBatchCallback();
            FactoryProvider factoryProvider = configuration.getFactoryProvider();
            this.securityContextFactory = factoryProvider.getSecurityContextFactory();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static long getLongUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getLong(address));
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

    public static long getUtf8StrSize(long x, long limit, CharSequence errorMessage, @Nullable PGPipelineEntry pe) throws PGMessageProcessingException {
        long len = Unsafe.getUnsafe().getByte(x) == 0 ? x : getStringLengthTedious(x, limit);
        if (len > -1) {
            return len;
        }
        // we did not find 0 within message limit
        if (pe != null) {
            // report error to the pipeline entry and continue parsing messages
            pe.getErrorMessageSink().put(errorMessage);
        } else {
            LOG.error().$(errorMessage).$();
        }
        throw PGMessageProcessingException.INSTANCE;
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

        do {
            if (pipelineCurrentEntry != null) {
                // do not return named portals and statements, since they are returned later
                if (!pipelineCurrentEntry.isPreparedStatement() && !pipelineCurrentEntry.isPortal()) {
                    releaseToPool(pipelineCurrentEntry);
                }
            }
        } while ((pipelineCurrentEntry = pipeline.poll()) != null);

        // clear named statements and named portals
        freePipelineEntriesFrom(namedStatements, true);
        freePipelineEntriesFrom(namedPortals, false);

        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_PGW_CONN);
        this.sendBuffer = this.sendBufferPtr = this.sendBufferLimit = Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
        responseUtf8Sink.bookmarkPtr = sendBufferPtr;

        prepareForNewQuery();
        clearRecvBuffer();
        clearWriters();
        // Clear every field, even if already cleaned to be on the safe side.
        Misc.clear(bindVariableTypes);
        Misc.clear(sqlTextCharacterStore);
        Misc.clear(bindVariableValuesCharacterStore);
        Misc.clear(circuitBreaker);
        Misc.clear(responseUtf8Sink);
        Misc.clear(pendingWriters);
        Misc.clear(authenticator);
        Misc.clear(bindVariableService);
        bufferRemainingOffset = 0;
        bufferRemainingSize = 0;
        freezeRecvBuffer = false;
        resumeCallback = null;
        suspendEvent = null;
        tlsSessionStarting = false;
        totalReceived = 0;
        transactionState = IMPLICIT_TRANSACTION;
        entryPool.resetCapacity();
    }

    @Override
    public void clearSuspendEvent() {
        suspendEvent = Misc.free(suspendEvent);
    }

    public void clearWriters() {
        if (pendingWriters != null) {
            rollbackAndClosePendingWriters();
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
        authenticator = Misc.free(authenticator);
        taiCache = Misc.free(taiCache);

        // assert is intentionally commented out. uncomment if you suspect a PGPipelineEntry leak and run all tests
        // do not forget to remove entryPool.clear() from clear()
        // assert entryPool.getPos() == 0 : "possible resource leak detected, not all entries were returned to pool [pos=" + entryPool.getPos() + ']';
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
                flushRemainingBuffer();
                tlsSessionStarting = false;
                try {
                    socket.startTlsSession(null);
                } catch (TlsSessionInitFailedException e) {
                    LOG.error().$("failed to create new TLS session").$((Throwable) e).$();
                    throw PGMessageProcessingException.INSTANCE;
                }
                // Start listening for read.
                throw PeerIsSlowToWriteException.INSTANCE;
            }
            handleAuthentication();
        } catch (PeerDisconnectedException | PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            // BAU, not error metric
            throw e;
        } catch (PGMessageProcessingException bpe) {
            shutdownSocketGracefully();
            throw bpe; // request disconnection
        } catch (Throwable th) {
            metrics.pgWireMetrics().getErrorCounter().inc();
            throw th;
        }

        flushRemainingBuffer();
        if (resumeCallback != null) {
            resumeCallback.resume();
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
            try {
                parseMessage(recvBuffer + recvBufferReadOffset, (int) (recvBufferWriteOffset - recvBufferReadOffset));
            } catch (PGMessageProcessingException e) {
                // Special handling for access control errors: to distinguish them from other parsing errors for better diagnostics
                if (!Chars.startsWith(e.getFlyweightMessage(), "Access")) {
                    LOG.error().$safe(e.getFlyweightMessage()).I$();
                } else {
                    LOG.error().$("failed to parse message [err: `").$safe(e.getFlyweightMessage()).$("`]").$();
                }
                // ignore, we are interrupting the current message processing, but have to continue processing other
                // messages
            }
        }
    }

    public void setAuthenticator(SocketAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void setSqlTimeout(long sqlTimeout) {
        if (sqlTimeout > 0) {
            circuitBreaker.setTimeout(sqlTimeout);
        }
    }

    public void setSuspendEvent(SuspendEvent suspendEvent) {
        this.suspendEvent = suspendEvent;
    }

    private static void sendErrorResponseAndReset(PGResponseSink sink, CharSequence message) throws PeerIsSlowToReadException, PeerDisconnectedException {
        sink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = sink.skipInt();
        sink.put('C');
        sink.putZ("08P01"); // protocol violation
        sink.put('M');
        sink.putZ(message);
        sink.put('S');
        sink.putZ("ERROR");
        sink.put((char) 0);
        sink.putLen(addr);
        sink.sendBufferAndReset();
    }

    private void addPipelineEntry() {
        if (pipelineCurrentEntry != null) {
            pipeline.add(pipelineCurrentEntry);
            pipelineCurrentEntry = null;
        }
    }

    private void assertBufferSize(boolean check) throws PGMessageProcessingException {
        if (check) {
            return;
        }
        // we did not find 0 within message limit
        LOG.error().$("undersized receive buffer or someone is abusing protocol [recvBufferSize=").$(recvBufferSize).$(']').$();
        throw PGMessageProcessingException.INSTANCE;
    }

    private void clearRecvBuffer() {
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
    }

    private void deallocateNamedStatement(Utf8Sequence statementName) {
        PGPipelineEntry pe = removeNamedStatementFromCache(statementName);

        // the entry with a named prepared statement must be returned back to the pool
        // otherwise we will leak memory until the connection is closed.
        releaseToPool(pe);
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

    private void flushRemainingBuffer() throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (bufferRemainingSize > 0) {
            sendBuffer(bufferRemainingOffset, bufferRemainingSize);
        }
    }

    private void freePipelineEntriesFrom(Utf8SequenceObjHashMap<PGPipelineEntry> cache, boolean isStatementClose) {
        ObjList<Utf8String> names = cache.keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            PGPipelineEntry pe = cache.get(names.getQuick(i));
            pe.setStateClosed(true, isStatementClose);
            pe.cacheIfPossible(tasCache, taiCache);
            releaseToPool(pe);
        }
        cache.clear();
    }

    @Nullable
    private Utf8Sequence getUtf8NamedPortal(long lo, long hi) {
        if (hi - lo > 0) {
            return directUtf8NamedPortal.of(lo, hi);
        }
        return null;
    }

    @Nullable
    private Utf8Sequence getUtf8NamedStatement(long lo, long hi) {
        if (hi - lo > 0) {
            return directUtf8NamedStatement.of(lo, hi);
        }
        return null;
    }

    private void handleAuthentication()
            throws PeerIsSlowToWriteException, PeerIsSlowToReadException, PGMessageProcessingException, PeerDisconnectedException {
        if (authenticator.isAuthenticated()) {
            return;
        }
        int r;
        try {
            r = authenticator.handleIO();
            if (r == SocketAuthenticator.OK) {
                try {
                    final SecurityContext securityContext = securityContextFactory.getInstance(
                            authenticator, SecurityContextFactory.PGWIRE
                    );
                    sqlExecutionContext.with(securityContext, bindVariableService, rnd, getFd(), circuitBreaker);
                    securityContext.checkEntityEnabled();
                    r = authenticator.loginOK();
                } catch (CairoException e) {
                    LOG.error().$("failed to authenticate [error=").$safe(e.getFlyweightMessage()).I$();
                    r = authenticator.denyAccess(e.getFlyweightMessage());
                }
            }
        } catch (AuthenticatorException e) {
            throw PeerDisconnectedException.INSTANCE;
        }
        switch (r) {
            case SocketAuthenticator.OK:
                assert authenticator.isAuthenticated();
                break;
            case SocketAuthenticator.NEEDS_READ:
                throw PeerIsSlowToWriteException.INSTANCE;
            case SocketAuthenticator.NEEDS_WRITE:
                throw PeerIsSlowToReadException.INSTANCE;
            case SocketAuthenticator.NEEDS_DISCONNECT:
                throw PeerDisconnectedException.INSTANCE;
            default:
                throw PGMessageProcessingException.INSTANCE;
        }


        // authenticator may have some non-auth data left in the buffer - make sure we don't overwrite it
        recvBufferWriteOffset = authenticator.getRecvBufPos() - recvBuffer;
        recvBufferReadOffset = authenticator.getRecvBufPseudoStart() - recvBuffer;
    }

    private void handleTlsRequest() throws PeerIsSlowToWriteException, PeerIsSlowToReadException, PGMessageProcessingException, PeerDisconnectedException {
        if (!socket.supportsTls() || tlsSessionStarting || socket.isTlsSessionStarted()) {
            return;
        }

        // This is the initial stage of communication.
        // The only message we've partially sent so far is the SSL request handling error.
        if (bufferRemainingSize > 0) {
            sendBuffer(bufferRemainingOffset, bufferRemainingSize);
            // The client received the response; shutdown the socket
            throw PGMessageProcessingException.INSTANCE;
        }

        recv();
        int expectedLen = 2 * Integer.BYTES;
        int len = (int) (recvBufferWriteOffset - recvBufferReadOffset);
        if (len < expectedLen) {
            throw PeerIsSlowToWriteException.INSTANCE;
        }
        if (len != expectedLen) {
            LOG.error().$("request SSL message expected [actualLen=").$(len).I$();
            sendErrorResponseAndReset(responseUtf8Sink, "request SSL message expected");
            throw PGMessageProcessingException.INSTANCE;
        }
        long address = recvBuffer + recvBufferReadOffset;
        int msgLen = getIntUnsafe(address);
        recvBufferReadOffset += Integer.BYTES;
        address += Integer.BYTES;
        if (msgLen != expectedLen) {
            LOG.error().$("unexpected request SSL message [msgLen=").$(msgLen).I$();
            sendErrorResponseAndReset(responseUtf8Sink, "unexpected request SSL message");
            throw PGMessageProcessingException.INSTANCE;
        }
        int request = getIntUnsafe(address);
        recvBufferReadOffset += Integer.BYTES;
        if (request != SSL_REQUEST) {
            LOG.error().$("unexpected request SSL message [request=").$(msgLen).I$();
            sendErrorResponseAndReset(responseUtf8Sink, "unexpected request SSL message");
            throw PGMessageProcessingException.INSTANCE;
        }
        // tell the client that SSL is supported
        responseUtf8Sink.put(MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE);
        tlsSessionStarting = true;
        responseUtf8Sink.sendBufferAndReset();
    }

    private void lookupPipelineEntryForNamedPortal(@Nullable Utf8Sequence namedPortal) throws PGMessageProcessingException {
        if (namedPortal != null) {
            PGPipelineEntry pe = namedPortals.get(namedPortal);
            if (pe == null) {
                throw msgKaput()
                        .put(" portal does not exist [name=").put(namedPortal).put(']');
            }

            replaceCurrentPipelineEntry(pe);
        }
    }

    private void lookupPipelineEntryForNamedStatement(long lo, long hi) throws PGMessageProcessingException {
        @Nullable Utf8Sequence namedStatement = getUtf8NamedStatement(lo, hi);
        if (namedStatement != null) {
            PGPipelineEntry pe = namedStatements.get(namedStatement);
            if (pe == null) {
                throw msgKaput()
                        .put("statement or portal does not exist [name=").put(namedStatement).put(']');
            }

            replaceCurrentPipelineEntry(pe);
        }
    }

    private void msgBind(long lo, long msgLimit) throws PGMessageProcessingException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        if (pipelineCurrentEntry != null && (pipelineCurrentEntry.isStateExec() || pipelineCurrentEntry.isStateClosed())) {
            // this is the sequence of B/E/B/E where B starts a new pipeline entry
            pipeline.add(pipelineCurrentEntry);
            pipelineCurrentEntry = null;
        }

        // portal name
        long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (bind)", pipelineCurrentEntry);
        Utf8Sequence namedPortal = getUtf8NamedPortal(lo, hi);
        // named statement
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length [msgType='B']", pipelineCurrentEntry);

        lookupPipelineEntryForNamedStatement(lo, hi);

        // Past this point the pipeline entry must not be null.
        // If it is - this means back-to-back "bind" messages were received with no prepared statement name.
        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("received a Bind message without a matching Parse");
        }

        pipelineCurrentEntry.setStateBind(true);

        // "bind" is asking us to create portal. We take the conservative approach and assume
        // that the prepared statement and the portal can be interleaved in the pipeline. For that
        // not to fail, these have to be separate factories and pipeline entries

        if (namedPortal != null) {
            LOG.info().$("create portal [name=").$(namedPortal).I$();
            int index = namedPortals.keyIndex(namedPortal);
            if (index > -1) {
                // intern the name of the portal, the name will be cached in a list
                Utf8String immutableNamedPortal = Utf8String.newInstance(namedPortal);

                // the current pipeline entry could either be named or unnamed
                // we only have to clone the named entries, in case they are interleaved in the
                // pipelines.
                if (pipelineCurrentEntry.isPreparedStatement()) {
                    // the pipeline is named, and we must not attempt to reuse it
                    // as the portal, so we are making a new entry
                    PGPipelineEntry pe = entryPool.next();
                    // the parameter types have to be copied from the parent
                    pe.msgParseCopyParameterTypesFrom(pipelineCurrentEntry);

                    int cachedStatus = CACHE_MISS;
                    final TypesAndSelect tas = tasCache.poll(pipelineCurrentEntry.getSqlText());
                    if (tas != null) {
                        if (pe.msgParseReconcileParameterTypes(tas)) {
                            pe.ofCachedSelect(pipelineCurrentEntry.getSqlText(), tas);
                            cachedStatus = CACHE_HIT_SELECT_VALID;
                        } else {
                            tas.close();
                            cachedStatus = CACHE_HIT_SELECT_INVALID;
                        }
                    }

                    if (cachedStatus != CACHE_HIT_SELECT_VALID) {
                        // When parameter types are not supplied we will assume that the types are STRING
                        // this is done by default, when CairoEngine compiles the SQL text. Assuming we're
                        // compiling the SQL from scratch.
                        pe.compileNewSQL(
                                pipelineCurrentEntry.getSqlText(),
                                engine,
                                sqlExecutionContext,
                                taiPool,
                                false
                        );
                    }

                    pe.setParentPreparedStatement(pipelineCurrentEntry);
                    pe.copyStateFrom(pipelineCurrentEntry);
                    // Keep the reference to the portal name on the prepared statement before we overwrite the
                    // reference. Keeping list of portal names is required in case the client closes the prepared
                    // statement. We will also be required to close all the portals.
                    pipelineCurrentEntry.bindPortalName(immutableNamedPortal);
                    pipelineCurrentEntry.clearState();
                    pipelineCurrentEntry = pe;
                    pipelineCurrentEntry.setStateBind(true);
                }
                // else:
                // portal is being created from "parse" message (i am not 100% the client will be
                // doing this; they would have to send "parse" message without statement name and then
                // send "bind" message without statement name but with portal). So we can use
                // the current entry as the portal
                pipelineCurrentEntry.setNamedPortal(true, immutableNamedPortal);
                namedPortals.putAt(index, namedPortal, pipelineCurrentEntry);
            } else {
                throw msgKaput().put("portal already exists [namedPortal=").put(namedPortal).put(']');
            }
        }

        // Parameter format count. These formats are BigEndian "short" values of 0 or 1,
        // 0 = text, 1 = binary. Meaning that parameter values in the bind message are
        // provided either as text or binary.
        lo = hi + 1;
        final short parameterFormatCodeCount = pipelineCurrentEntry.getShort(
                lo,
                msgLimit,
                "could not read parameter format code count"
        );
        lo += Short.BYTES;

        final short parameterValueCount = pipelineCurrentEntry.getShort(
                lo + parameterFormatCodeCount * Short.BYTES,
                msgLimit,
                "could not read parameter value count"
        );

        pipelineCurrentEntry.msgBindCopyParameterFormatCodes(
                lo,
                msgLimit,
                parameterFormatCodeCount,
                parameterValueCount
        );

        lo += parameterFormatCodeCount * Short.BYTES;
        lo += Short.BYTES;

        // Copy parameter values to the pipeline's arena. The value area size of the
        // bind message is variable, and is dependent on storage method of parameter values.
        // Before we copy value, we have to compute size of the area.
        lo = pipelineCurrentEntry.msgBindCopyParameterValuesArea(lo, msgLimit);
        short columnFormatCodeCount = pipelineCurrentEntry.getShort(lo, msgLimit, "could not read result set column format codes");
        lo += Short.BYTES;
        pipelineCurrentEntry.msgBindCopySelectFormatCodes(lo, columnFormatCodeCount);
    }

    private void msgClose(long lo, long msgLimit) throws PGMessageProcessingException {
        // 'close' message can either:
        // - close the named entity, portal or statement
        final byte type = Unsafe.getUnsafe().getByte(lo);
        PGPipelineEntry lookedUpPipelineEntry;
        boolean isStatementClose = false;
        switch (type) {
            case 'S':
                // invalid statement names are allowed (as noop)
                // Closing statement also closes all bound portals. Portals will have the same
                // reference of the pipeline entry as the statement, so we only need to remove this
                // reference from maps.
                lo = lo + 1;
                final long hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length", pipelineCurrentEntry);
                lookedUpPipelineEntry = removeNamedStatementFromCache(getUtf8NamedStatement(lo, hi));
                isStatementClose = true;
                break;
            case 'P':
                lo = lo + 1;
                final long high = getUtf8StrSize(lo, msgLimit, "bad prepared portal name length (close)", pipelineCurrentEntry);
                lookedUpPipelineEntry = removeNamedPortalFromCache(getUtf8NamedPortal(lo, high));
                break;
            default:
                throw msgKaput().put("invalid type for close message [type=").put(type).put(']');
        }

        if (lookedUpPipelineEntry == null) {
            if (pipelineCurrentEntry == null) {
                pipelineCurrentEntry = entryPool.next();
            }
            // we are liable to look up the current entry, depending on how protocol is used
            // if this the case, we should not attempt to save the current entry prematurely
        } else if (lookedUpPipelineEntry != pipelineCurrentEntry) {
            if (pipelineCurrentEntry != null) {
                if (pipelineCurrentEntry.isDirty()) {
                    addPipelineEntry();
                } else {
                    releaseToPoolIfAbandoned(pipelineCurrentEntry);
                }
            }
            pipelineCurrentEntry = lookedUpPipelineEntry;
        }

        pipelineCurrentEntry.setStateClosed(true, isStatementClose);
    }

    private void msgDescribe(long lo, long msgLimit) throws PGMessageProcessingException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        // 'S' = statement name
        // 'P' = portal name
        // followed by the name, which can be NULL, typically with 'P'
        boolean isPortal = Unsafe.getUnsafe().getByte(lo) == 'P';
        final long hi = getUtf8StrSize(lo + 1, msgLimit, "bad prepared statement name length (describe)", pipelineCurrentEntry);
        if (isPortal) {
            lookupPipelineEntryForNamedPortal(getUtf8NamedPortal(lo + 1, hi));
        } else {
            lookupPipelineEntryForNamedStatement(lo + 1, hi);
        }

        // some defensive code to have predictable behaviour
        // when dealing with spurious "describe" messages, for which we do not have
        // a pipeline entry

        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("spurious describe message received");
        }

        pipelineCurrentEntry.setStateDesc(isPortal ? PGPipelineEntry.SYNC_DESC_ROW_DESCRIPTION : PGPipelineEntry.SYNC_DESC_PARAMETER_DESCRIPTION);
    }

    private void msgExecute(long lo, long msgLimit) throws PGMessageProcessingException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        final long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (execute)", pipelineCurrentEntry);
        lookupPipelineEntryForNamedPortal(getUtf8NamedPortal(lo, hi));

        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("spurious execute message");
        }

        lo = hi + 1;
        pipelineCurrentEntry.setReturnRowCountLimit(pipelineCurrentEntry.getInt(lo, msgLimit, "could not read max rows value"));
        pipelineCurrentEntry.setStateExec(true);
        sqlExecutionContext.initNow();
        transactionState = pipelineCurrentEntry.msgExecute(
                sqlExecutionContext,
                transactionState,
                taiPool,
                pendingWriters,
                this,
                bindVariableValuesCharacterStore,
                utf8String,
                binarySequenceParamsPool,
                tempSequence,
                preparedStatementDeallocator
        );
    }

    private void msgFlush() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
        addPipelineEntry();
        // "The Flush message does not cause any specific output to be generated, but forces the backend to deliver any data pending in its output buffers.
        //  A Flush must be sent after any extended-query command except Sync, if the frontend wishes to examine the results of that command before issuing more commands.
        //  Without Flush, messages returned by the backend will be combined into the minimum possible number of packets to minimize network overhead."
        // some clients (asyncpg) chose not to send 'S' (sync) message
        // but instead fire 'H'. Can't wrap my head around as to why
        // query execution is so ambiguous

        resumeCallback = msgFlushRef;
        msgFlush0();
    }

    private void msgFlush0() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
        syncPipeline();
        resumeCallback = null;
        responseUtf8Sink.sendBufferAndReset();
    }

    private PGMessageProcessingException msgKaput() {
        // The error message and position is to be reported to the client
        // To do that, we store message on the pipeline entry, if it exists and
        // make sure this entry is added to the pipeline (eventually)
        if (pipelineCurrentEntry == null) {
            pipelineCurrentEntry = entryPool.next();
        }
        return PGMessageProcessingException.instance(pipelineCurrentEntry);
    }

    private void msgParse(long address, long lo, long msgLimit) throws PGMessageProcessingException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        // Parse message typically starts a new pipeline entry. So if there is existing one in flight
        // we have to add it to the pipeline
        addPipelineEntry();

        pipelineCurrentEntry = entryPool.next();

        // when processing the "parse" message we use BindVariableService to exchange bind variable types
        // between SQL compiler and the PG "parse" message processing logic. BindVariableService must not
        // be used to pass state from one message to the next. Assume that thread may be interrupted between
        // messages or processing could switch to another thread entirely.
        bindVariableService.clear();

        // mark the pipeline entry as received "parse" message
        pipelineCurrentEntry.setStateParse(true);

        // 'Parse'
        // "statement name" length
        long hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length (parse)", pipelineCurrentEntry);

        // when statement name is present in "parse" message
        // it should be interpreted as "store" command, e.g. we store the
        // parsed SQL as short and sweet statement name.
        final Utf8Sequence namedStatement = getUtf8NamedStatement(lo, hi);

        // read query text from the message
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad query text length", pipelineCurrentEntry);
        final CharacterStoreEntry e = sqlTextCharacterStore.newEntry();
        if (!Utf8s.utf8ToUtf16(lo, hi, e)) {
            throw msgKaput().put("invalid UTF8 bytes in parse query");
        }

        final CharSequence utf16SqlText = e.toImmutable();
        lo = hi + 1;

        // read parameter types before we are able to compile SQL text
        // parameter values are not provided here, but we do not need them to be able to
        // parse/compile the SQL.
        // It is possible that the number of parameter types provided here is
        // different from the number of bind variables in the SQL. At this point
        // we will copy into the pipeline entry whatever was provided
        short parameterTypeCount = pipelineCurrentEntry.getShort(lo, msgLimit, "could not read parameter type count");

        // process parameter types
        if (parameterTypeCount > 0) {
            if (lo + Short.BYTES + parameterTypeCount * 4L > msgLimit) {
                throw msgKaput()
                        .put("could not read parameters [parameterCount=").put(parameterTypeCount)
                        .put(", offset=").put(lo - address)
                        .put(", remaining=").put(msgLimit - lo);
            }

            LOG.debug().$("params [count=").$(parameterTypeCount).I$();
            // copy argument types into the last pipeline entry
            // the entry will also maintain count of these argument types to aid
            // validation of the "bind" message.
            pipelineCurrentEntry.msgParseCopyParameterTypesFromMsg(lo + Short.BYTES, parameterTypeCount);
        } else if (parameterTypeCount < 0) {
            throw msgKaput()
                    .put("invalid parameter count [parameterCount=").put(parameterTypeCount)
                    .put(", offset=").put(lo - address);
        }

        // At this point parameters may or may not be defined.
        // If they are defined, the pipeline entry
        // will have the supplied parameter types.

        int cachedStatus = CACHE_MISS;
        final TypesAndInsert tai = taiCache.poll(utf16SqlText);
        if (tai != null) {
            if (pipelineCurrentEntry.msgParseReconcileParameterTypes(parameterTypeCount, tai)) {
                pipelineCurrentEntry.ofCachedInsert(utf16SqlText, tai);
                cachedStatus = CACHE_HIT_INSERT_VALID;
            } else {
                // Cache miss, get rid of this entry.
                tai.close();
                cachedStatus = CACHE_HIT_INSERT_INVALID;
            }
        }

        if (cachedStatus == CACHE_MISS) {
            final TypesAndSelect tas = tasCache.poll(utf16SqlText);
            if (tas != null) {
                if (pipelineCurrentEntry.msgParseReconcileParameterTypes(parameterTypeCount, tas)) {
                    pipelineCurrentEntry.ofCachedSelect(utf16SqlText, tas);
                    cachedStatus = CACHE_HIT_SELECT_VALID;
                    sqlExecutionContext.resetFlags();
                } else {
                    tas.close();
                    cachedStatus = CACHE_HIT_SELECT_INVALID;
                }
            }
        }

        if (cachedStatus != CACHE_HIT_INSERT_VALID && cachedStatus != CACHE_HIT_SELECT_VALID) {
            // When parameter types are not supplied we will assume that the types are STRING
            // this is done by default, when CairoEngine compiles the SQL text. Assuming we're
            // compiling the SQL from scratch.
            pipelineCurrentEntry.compileNewSQL(utf16SqlText, engine, sqlExecutionContext, taiPool, false);
        }
        msgParseCreateNamedStatement(namedStatement);
    }

    private void msgParseCreateNamedStatement(Utf8Sequence namedStatement) throws PGMessageProcessingException {
        if (namedStatement != null) {
            LOG.info().$("create prepared statement [name=").$(namedStatement).I$();
            int index = namedStatements.keyIndex(namedStatement);
            if (index > -1) {
                if (namedStatements.size() == namedStatementLimit) {
                    throw msgKaput().put("client created too many named statements without closing them. it looks like a buggy client. [limit=").put(namedStatementLimit).put(']');
                }

                final Utf8String immutableNamedStatement = Utf8String.newInstance(namedStatement);
                pipelineCurrentEntry.setNamedStatement(immutableNamedStatement);
                namedStatements.putAt(index, immutableNamedStatement, pipelineCurrentEntry);
            } else {
                throw msgKaput()
                        .put("duplicate statement [name=").put(namedStatement).put(']');
            }
        }
    }

    // processes one or more queries (batch/script). "Simple Query" in PostgreSQL docs.
    private void msgQuery(long lo, long limit) throws PGMessageProcessingException, PeerIsSlowToReadException, QueryPausedException, PeerDisconnectedException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        CharacterStoreEntry e = sqlTextCharacterStore.newEntry();
        if (!Utf8s.utf8ToUtf16(lo, limit - 1, e)) {
            throw msgKaput().put("invalid UTF8 bytes in parse query");
        }
        sqlExecutionContext.initNow();
        CharSequence activeSqlText = sqlTextCharacterStore.toImmutable();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.compileBatch(activeSqlText, sqlExecutionContext, batchCallback);
            if (pipelineCurrentEntry == null) {
                pipelineCurrentEntry = entryPool.next();
                pipelineCurrentEntry.ofEmpty(activeSqlText);
                pipelineCurrentEntry.setStateExec(true);
            }
        } catch (Throwable ex) {
            if (transactionState == IN_TRANSACTION) {
                transactionState = ERROR_TRANSACTION;
            }
            throw msgKaput().put(ex);
        } finally {
            msgSync();
        }
    }

    private void msgSync() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
        if (transactionState == IMPLICIT_TRANSACTION) {
            // implicit transactions must be committed on SYNC
            try {
                if (pipelineCurrentEntry == null) {
                    pipelineCurrentEntry = entryPool.next();
                }
                pipelineCurrentEntry.commit(pendingWriters);
            } catch (PGMessageProcessingException ignore) {
                // the failed commit will have already labelled the pipeline entry as error
                // the intent of the exception is to abort message processing, but this is sync.
                // Sync cannot be aborted. This is the method that will report an error to the client
            }
        }

        addPipelineEntry();

        // the sync0 is liable to get interrupted due to:
        // 1. network client being slow
        // 2. SQL might get paused due to data not being available yet
        // however, sync0 is reenterable and we have to call it until
        // the resume callback clears
        resumeCallback = msgSyncRef;
        msgSync0();
    }

    private void msgSync0() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
        syncPipeline();

        // flush the buffer in case response message does not fit the buffer
        if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
            responseUtf8Sink.sendBufferAndReset();
        }
        outReadForNewQuery();
        resumeCallback = null;
        responseUtf8Sink.sendBufferAndReset();
        prepareForNewQuery();
    }

    private void outReadForNewQuery() {
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

    /**
     * Returns address of where parsing stopped. If there are remaining bytes left
     * in the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parseMessage(long address, int len)
            throws PGMessageProcessingException, PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
        // we will wait until we receive the entire header
        if (len < PREFIXED_MESSAGE_HEADER_LEN) {
            // we need to be able to read header and length
            return;
        }

        final byte type = Unsafe.getUnsafe().getByte(address);
        final int msgLen = getIntUnsafe(address + 1);
        LOG.debug().$("received msg [type=").$((char) type).$(", len=").$(msgLen).I$();
        if (msgLen < 1) {
            LOG.error().$("invalid message length [type=").$(type)
                    .$(", msgLen=").$(msgLen)
                    .$(", recvBufferReadOffset=").$(recvBufferReadOffset)
                    .$(", recvBufferWriteOffset=").$(recvBufferWriteOffset)
                    .$(", totalReceived=").$(totalReceived)
                    .I$();
            throw PGMessageProcessingException.INSTANCE;
        }

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

        // this check is exactly the same as the one run inside security context on every permission checks.
        // however, this will run even if the command to be executed does not require permission checks.
        // this is useful in case a disabled user intends to hammer the database with queries which do not require authorization.
        if (pipelineCurrentEntry == null || !pipelineCurrentEntry.isError()) {
            try {
                // this check can explode, we need to fold it into a pipeline entry
                // it has to be done after "recvBufferReadOffset" is updated to avoid infinite loop
                sqlExecutionContext.getSecurityContext().checkEntityEnabled();
            } catch (Throwable e) {
                throw msgKaput().put(e);
            }
        }


        // Message types in the order they usually come over the wire. All "msg" methods
        // are called only from here and are responsible for handling individual messages.
        // Please do not create other methods that start with "msg".

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
                msgQuery(msgLo, msgLimit);
                break;
            case 'S': // sync
                msgSync();
                break;
            case 'H': // flush
                msgFlush();
                break;
            case 'X': // 'Terminate'
                throw PeerDisconnectedException.INSTANCE;
            case 'C':
                // close
                msgClose(msgLo, msgLimit);
                break;
            default:
                throw msgKaput().put("unknown message [type=").put(type).put(']');
        }
    }

    private void prepareForNewQuery() {
        LOG.debug().$("prepare for new query").$();
        Misc.clear(bindVariableService);
        freezeRecvBuffer = false;
        sqlExecutionContext.setCacheHit(false);
        sqlExecutionContext.containsSecret(false);
        Misc.clear(sqlTextCharacterStore);
    }

    private void releaseToPool(@NotNull PGPipelineEntry pe) {
        pe.close();
        entryPool.release(pe);
    }

    private PGPipelineEntry removeNamedPortalFromCache(Utf8Sequence portalName) {
        if (portalName != null) {
            final int index = namedPortals.keyIndex(portalName);
            if (index < 0) {
                PGPipelineEntry pe = namedPortals.valueAt(index);
                PGPipelineEntry peParent = pe.getParentPreparedStatementPipelineEntry();
                if (peParent != null) {
                    int parentIndex = peParent.getNamedPortals().indexOf(portalName);
                    if (parentIndex != -1) {
                        peParent.getNamedPortals().remove(parentIndex);
                    }
                }
                namedPortals.removeAt(index);
                return pe;
            }
        }
        return null;
    }

    private PGPipelineEntry removeNamedStatementFromCache(Utf8Sequence namedStatement) {
        if (namedStatement != null) {
            int index = namedStatements.keyIndex(namedStatement);
            if (index < 0) {
                PGPipelineEntry pe = namedStatements.valueAt(index);
                namedStatements.removeAt(index);
                // also remove entries for the matching portal names
                ObjList<Utf8String> portalNames = pe.getNamedPortals();
                for (int i = 0, n = portalNames.size(); i < n; i++) {
                    int portalKeyIndex = namedPortals.keyIndex(portalNames.getQuick(i));
                    if (portalKeyIndex < 0) {
                        // release the entry, it must not be referenced from anywhere other than
                        // this list (we enforce portal name uniqueness)
                        Misc.free(namedPortals.valueAt(portalKeyIndex));
                        namedPortals.removeAt(portalKeyIndex);
                    } else {
                        // else: do not make a fuss if portal name does not exist
                        LOG.debug()
                                .$("ignoring non-existent portal [portalName=").$(portalNames.getQuick(i))
                                .$(", namedStatement=").$(namedStatement)
                                .I$();
                    }
                }
                return pe;
            }
        }
        return null;
    }

    private void replaceCurrentPipelineEntry(PGPipelineEntry nextEntry) {
        if (nextEntry == pipelineCurrentEntry) {
            return;
        }
        // Alright, the client wants to use the named statement. What if they just
        // send "parse" message and want to abandon it?
        releaseToPoolIfAbandoned(pipelineCurrentEntry);
        // it is safe to overwrite the pipeline entry,
        // named entries will be held in the hash map
        pipelineCurrentEntry = nextEntry.copyIfExecuted(entryPool);
    }

    private void rollbackAndClosePendingWriters() {
        for (ObjObjHashMap.Entry<TableToken, TableWriterAPI> pendingWriter : pendingWriters) {
            final TableWriterAPI m = pendingWriter.value;
            m.rollback();
            Misc.free(m);
        }
    }

    private void shiftReceiveBuffer(long readOffsetBeforeParse) {
        final long len = recvBufferWriteOffset - readOffsetBeforeParse;
        LOG.debug().$("shift [offset=").$(readOffsetBeforeParse).$(", len=").$(len).I$();

        Vect.memmove(recvBuffer, recvBuffer + readOffsetBeforeParse, len);
        recvBufferWriteOffset = len;
        recvBufferReadOffset = 0;
    }

    private void shutdownSocketGracefully() {
        // calling close on a socket with a
        // non-empty receive kernel-buffer cause the connection to be RST and
        // the send buffer discarded and not sent

        socket.shutdown(Net.SHUT_WR); // sends a FIN packet and flushes the send buffer
        final MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
        final long startTime = clock.getTicks();
        // drain the kernel receive-buffer until we either receive all data or hit the timeout
        while (true) {
            final int n = socket.recv(recvBuffer, recvBufferSize);
            // receive buffer is empty or connection is closed
            // the timeout ensures that the loop exits if all data isn't drained within the specified time limit.
            if (n <= 0 || clock.getTicks() - startTime > MALFORMED_CLIENT_READ_TIMEOUT_MILLIS) {
                break;
            }
        }
    }

    // Send responses from the pipeline entries we have accumulated so far.
    private void syncPipeline() throws PeerIsSlowToReadException, QueryPausedException, PeerDisconnectedException {
        while (pipelineCurrentEntry != null || (pipelineCurrentEntry = pipeline.poll()) != null) {
            // we need to store stateExec flag now
            // because syncing the entry will clear the flag
            boolean isExec = pipelineCurrentEntry.isStateExec();
            boolean isError = pipelineCurrentEntry.isError();
            boolean isClosed = pipelineCurrentEntry.isStateClosed();
            // with the sync call the existing pipeline entry will assign its own completion hooks (resume callbacks)
            while (true) {
                try {
                    pipelineCurrentEntry.msgSync(
                            sqlExecutionContext,
                            pendingWriters,
                            responseUtf8Sink
                    );
                    break;
                } catch (NoSpaceLeftInResponseBufferException e) {
                    responseUtf8Sink.resetToBookmark();
                    if (responseUtf8Sink.sendBufferAndReset() == 0) {
                        // we did not send anything, the sync is stuck
                        responseUtf8Sink.reset();
                        pipelineCurrentEntry.getErrorMessageSink()
                                .put("not enough space in send buffer [sendBufferSize=").put(responseUtf8Sink.getSendBufferSize())
                                .put(", requiredSize=").put(Math.max(e.getBytesRequired(), 2 * responseUtf8Sink.getSendBufferSize()))
                                .put(']');
                        pipelineCurrentEntry.msgSync(
                                sqlExecutionContext,
                                pendingWriters,
                                responseUtf8Sink
                        );
                        break;
                    }
                }
            }

            // we want the pipelineCurrentEntry to retain the last entry of the pipeline
            // unless this entry was already executed, closed and is an error
            // additionally, we do not want to "cacheIfPossible" the last entry, because
            // "cacheIfPossible" has side effects on the entry.

            PGPipelineEntry nextEntry = pipeline.poll();
            if (nextEntry != null || isExec || isError || isClosed) {
                if (!isError) {
                    pipelineCurrentEntry.cacheIfPossible(tasCache, taiCache);
                }
                releaseToPoolIfAbandoned(pipelineCurrentEntry);
                pipelineCurrentEntry = nextEntry;
            } else {
                LOG.debug().$("pipeline entry not consumed [instance=)").$(pipelineCurrentEntry)
                        .$(", sql=").$safe(pipelineCurrentEntry.getSqlText())
                        .$(", stmt=").$safe(pipelineCurrentEntry.getNamedStatement())
                        .$(", portal=").$safe(pipelineCurrentEntry.getNamedPortal())
                        .I$();
                break;
            }
        }
        sqlTextCharacterStore.clear();
    }

    static void dumpBuffer(char direction, long buffer, int len, boolean dumpNetworkTraffic) {
        if (dumpNetworkTraffic && len > 0) {
            StdoutSink.INSTANCE.put(direction);
            Net.dump(buffer, len);
        }
    }

    static int getIntUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getInt(address));
    }

    static short getShortUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getShort(address));
    }

    @Override
    protected void doInit() {
        sqlExecutionContext.with(getFd());

        // re-read recv buffer size in case the config was reloaded
        assert recvBuffer == 0;
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_PGW_CONN);

        // re-read send buffer size in case the config was reloaded
        assert sendBuffer == 0;
        this.sendBufferSize = configuration.getSendBufferSize();
        this.sendBuffer = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
        this.sendBufferPtr = sendBuffer;
        this.responseUtf8Sink.bookmarkPtr = this.sendBufferPtr;
        this.sendBufferLimit = sendBuffer + sendBufferSize;

        // reinitialize the prepared statement limit - this property can be changed at runtime
        // so new connections need to pick up the new value
        this.namedStatementLimit = configuration.getNamedStatementLimit();

        authenticator.init(socket, recvBuffer, recvBuffer + recvBufferSize, sendBuffer, sendBufferLimit);
    }

    int doReceive(int remaining) {
        final long data = recvBuffer + recvBufferWriteOffset;
        final int n = socket.recv(data, remaining);
        dumpBuffer('>', data, n, dumpNetworkTraffic);
        return n;
    }

    void recv() throws PeerDisconnectedException, PeerIsSlowToWriteException, PGMessageProcessingException {
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

    void releaseToPoolIfAbandoned(PGPipelineEntry pe) {
        if (pe != null) {
            if (pe.isCopy || (!pe.isPreparedStatement() && !pe.isPortal())) {
                releaseToPool(pe);
            } else {
                pe.clearState();
            }
        }
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
        responseUtf8Sink.bookmarkPtr = sendBufferPtr;
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
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence queryText) throws Exception {
            CharacterStoreEntry entry = sqlTextCharacterStore.newEntry();
            entry.put(queryText);
            pipelineCurrentEntry.ofSimpleQuery(
                    entry.toImmutable(),
                    sqlExecutionContext,
                    cq,
                    taiPool
            );
            transactionState = pipelineCurrentEntry.msgExecute(
                    sqlExecutionContext,
                    transactionState,
                    taiPool,
                    pendingWriters,
                    PGConnectionContext.this,
                    bindVariableValuesCharacterStore,
                    utf8String,
                    binarySequenceParamsPool,
                    tempSequence,
                    preparedStatementDeallocator
            );
            pipelineCurrentEntry.setStateExec(true);
        }

        @Override
        public boolean preCompile(SqlCompiler compiler, CharSequence sqlText) {
            addPipelineEntry();
            pipelineCurrentEntry = entryPool.next();

            final TypesAndSelect tas = tasCache.poll(sqlText);
            if (tas == null) {
                // cache miss -> we will compile the query for real
                return true;
            }

            if (!pipelineCurrentEntry.msgParseReconcileParameterTypes((short) 0, tas)) {
                // this should not be possible - SIMPLE query do not have parameters.
                // so if there was a cache hit, the cached plan should not have no parameter either
                // -> msgParseReconcileParameterTypes() should always pass
                tas.close();
                return false;
            }

            CharacterStoreEntry entry = sqlTextCharacterStore.newEntry();
            entry.put(sqlText);
            try {
                pipelineCurrentEntry.ofSimpleCachedSelect(entry.toImmutable(), sqlExecutionContext, tas);
                return false; // we will not compile the query
            } catch (Throwable e) {
                // a bad thing happened while we tried to use cached query
                // let's pretend we never tried and compile the query as if there was no cache
                CharSequence msg;
                if (e instanceof FlyweightMessageContainer) {
                    msg = ((FlyweightMessageContainer) e).getFlyweightMessage();
                } else {
                    msg = e.getMessage();
                }
                LOG.info().$("could not use cached select [error=").$(msg).$(']').$();
                pipelineCurrentEntry.clearState();
                tas.close();
                return true;
            }
        }
    }

    private class ResponseUtf8Sink implements PGResponseSink, Mutable {
        private long bookmarkPtr = -1;

        public ResponseUtf8Sink() {
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
            throw NoSpaceLeftInResponseBufferException.instance(size);
        }

        @Override
        public void clear() {
            reset();
        }

        @Override
        public long getMaxBlobSize() {
            return maxBlobSize;
        }

        @Override
        public long getSendBufferPtr() {
            return sendBufferPtr;
        }

        @Override
        public long getSendBufferSize() {
            return sendBufferSize;
        }

        @Override
        public long getWrittenBytes() {
            return sendBufferPtr - sendBuffer;
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
            if (len > maxBlobSize) {
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
        public void putDirectInt(int xValue) {
            checkCapacity(Integer.BYTES);
            Unsafe.getUnsafe().putInt(sendBufferPtr, xValue);
            sendBufferPtr += Integer.BYTES;
        }

        @Override
        public void putDirectShort(short xValue) {
            checkCapacity(Short.BYTES);
            Unsafe.getUnsafe().putShort(sendBufferPtr, xValue);
            sendBufferPtr += Short.BYTES;
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
        public void putNetworkInt(long address, int value) {
            checkCapacity(address, Integer.BYTES);
            putInt(address, value);
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
        public void putNetworkShort(long address, short value) {
            checkCapacity(address, Short.BYTES);
            putShort(address, value);
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
            if (bookmarkPtr != -1) {
                sendBufferPtr = bookmarkPtr;
                bookmarkPtr = -1;
            }
        }

        @Override
        public void resetToBookmark(long address) {
            sendBufferPtr = address;
            bookmarkPtr = -1;
        }

        @Override
        public int sendBufferAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException {
            int sendSize = (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset);
            sendBuffer(bufferRemainingOffset, sendSize);
            responseUtf8Sink.reset();
            return sendSize;
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

        private void checkCapacity(long address, long size) {
            if (address + size < sendBufferLimit) {
                return;
            }
            throw NoSpaceLeftInResponseBufferException.instance(size);
        }
    }
}
