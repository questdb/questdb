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
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.Authenticator;
import io.questdb.cutlass.auth.AuthenticatorException;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

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
    static final int NO_TRANSACTION = 0;
    private static final Log LOG = LogFactory.getLog(PGConnectionContext.class);
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private static final byte MESSAGE_TYPE_SSL_SUPPORTED_RESPONSE = 'S';
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final int PROTOCOL_TAIL_COMMAND_LENGTH = 64;
    private static final int ROLLING_BACK_TRANSACTION = 4;
    private static final int SSL_REQUEST = 80877103;
    private final BatchCallback batchCallback;
    private final ObjectPool<DirectBinarySequence> binarySequenceParamsPool;
    private final BindVariableService bindVariableService;
    private final IntList bindVariableTypes = new IntList();
    private final CharacterStore characterStore;
    private final NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private final boolean dumpNetworkTraffic;
    private final CairoEngine engine;
    private final int forceRecvFragmentationChunkSize;
    private final int forceSendFragmentationChunkSize;
    private final int maxBlobSize;
    private final Metrics metrics;
    private final CharSequenceObjHashMap<PGPipelineEntry> namedPortals;
    private final CharSequenceObjHashMap<PGPipelineEntry> namedStatements;
    private final ObjObjHashMap<TableToken, TableWriterAPI> pendingWriters;
    private final ArrayDeque<PGPipelineEntry> pipeline = new ArrayDeque<>();
    private final int recvBufferSize;
    private final ResponseUtf8Sink responseUtf8Sink = new ResponseUtf8Sink();
    private final Rnd rnd;
    private final SecurityContextFactory securityContextFactory;
    private final int sendBufferSize;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final WeakSelfReturningObjectPool<TypesAndInsert> taiPool;
    private final DirectUtf8String utf8String = new DirectUtf8String();
    private Authenticator authenticator;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private boolean freezeRecvBuffer;
    private Path path;
    // PG wire protocol has two phases:
    // phase 1 - fill up the pipeline. In this case the current entry is the entry being populated
    // phase 2 - "sync" the pipeline. This is the execution phase and the current entry is the one being executed.
    private PGPipelineEntry pipelineCurrentEntry;
    private long recvBuffer;
    private long recvBufferReadOffset = 0;
    private long recvBufferWriteOffset = 0;
    private PGResumeCallback resumeCallback;
    private long sendBuffer;
    private long sendBufferLimit;
    private long sendBufferPtr;
    private SuspendEvent suspendEvent;
    // insert 'statements' are cached only for the duration of user session
    private SimpleAssociativeCache<TypesAndInsert> taiCache;
    private AssociativeCache<TypesAndSelect> tasCache;
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
            this.maxBlobSize = configuration.getMaxBlobSizeOnQuery();
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

    public static long getUtf8StrSize(long x, long limit, CharSequence errorMessage, @Nullable PGPipelineEntry pe) throws BadProtocolException {
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

        // clear named statements and named portals
        freePipelineEntriesFrom(namedStatements);
        freePipelineEntriesFrom(namedPortals);

        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_PGW_CONN);
        this.sendBuffer = this.sendBufferPtr = this.sendBufferLimit = Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_PGW_CONN);
        responseUtf8Sink.bookmarkPtr = sendBufferPtr;
        pipelineCurrentEntry = null;
        pipeline.clear();
        prepareForNewQuery();
        clearRecvBuffer();
        clearWriters();
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
        freezeRecvBuffer = false;
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
                flushRemainingBuffer();
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
                } catch (BadProtocolException e) {
                    // ignore, we are interrupting the current message processing, but have to continue processing other
                    // messages
                }
            }
        } catch (
                PeerDisconnectedException | PeerIsSlowToReadException | PeerIsSlowToWriteException |
                QueryPausedException e) {
            throw e;
        } catch (Throwable th) {
            th.printStackTrace();
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
            this.responseUtf8Sink.bookmarkPtr = this.sendBufferPtr;
            this.sendBufferLimit = sendBuffer + sendBufferSize;
        }
        authenticator.init(socket, recvBuffer, recvBuffer + recvBufferSize, sendBuffer, sendBufferLimit);
        return this;
    }

    public void setAuthenticator(Authenticator authenticator) {
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

    private void addPipelineEntry() {
        if (pipelineCurrentEntry != null) {
            pipeline.add(pipelineCurrentEntry);
            pipelineCurrentEntry = null;
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

    private void freePipelineEntriesFrom(CharSequenceObjHashMap<PGPipelineEntry> cache) {
        ObjList<CharSequence> names = cache.keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            PGPipelineEntry pe = cache.get(names.getQuick(i));
            pe.setStateClosed(true);
            Misc.free(pe);
        }
        cache.clear();
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

    private void handleAuthentication()
            throws PeerIsSlowToWriteException, PeerIsSlowToReadException, BadProtocolException, PeerDisconnectedException {
        if (authenticator.isAuthenticated()) {
            return;
        }
        int r;
        try {
            r = authenticator.handleIO();
            if (r == Authenticator.OK) {
                try {
                    final SecurityContext securityContext = securityContextFactory.getInstance(
                            authenticator.getPrincipal(),
                            authenticator.getAuthType(),
                            SecurityContextFactory.PGWIRE
                    );
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
/*
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
*/
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

    private boolean lookupPipelineEntryForPortalName(@Nullable CharSequence portalName) throws BadProtocolException {
        if (portalName != null) {
            PGPipelineEntry pe = namedPortals.get(portalName);
            if (pe == null) {
                throw msgKaput()
                        .put(" portal does not exist [name=").put(portalName).put(']');
            }

            replaceCurrentPipelineEntry(pe);
            return false;
        }
        return true;
    }

    private boolean lookupPipelineEntryForStatementName(@Nullable CharSequence statementName) throws BadProtocolException {
        if (statementName != null) {
            PGPipelineEntry pe = namedStatements.get(statementName);
            if (pe == null) {
                throw msgKaput()
                        .put("statement or portal does not exist [name=").put(statementName).put(']');
            }

            replaceCurrentPipelineEntry(pe);
            return false;
        }
        return true;
    }

    private void msgBind(long lo, long msgLimit) throws BadProtocolException {

        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isStateExec()) {
            // this is the sequence of B/E/B/E where B starts a new pipeline entry
            pipeline.add(pipelineCurrentEntry);
            pipelineCurrentEntry = null;
        }

        short parameterValueCount;

        LOG.debug().$("bind").$();

        // portal name
        long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (bind)", pipelineCurrentEntry);
        CharSequence portalName = getUtf16Str(lo, hi, "invalid UTF8 bytes in portal name (bind)");
        // named statement
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length [msgType='B']", pipelineCurrentEntry);

        CharSequence statementName = getUtf16Str(lo, hi, "invalid UTF8 bytes in statement name (bind)");

        lookupPipelineEntryForStatementName(statementName);

        // Past this point the pipeline entry must not be null.
        // If it is - this means back-to-back "bind" messages were received with no prepared statement name.
        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("spurious bind message");
        }

        pipelineCurrentEntry.setStateBind(true);

        // "bind" is asking us to create portal. We take the conservative approach and assume
        // that the prepared statement and the portal can be interleaved in the pipeline. For that
        // not to fail, these have to be separate factories and pipeline entries

        if (portalName != null) {
            // check if we can create portal, it makes sense only for SELECT SQLs, such that contain Factory
            if (pipelineCurrentEntry.isFactory()) {
                LOG.info().$("create portal [name=").$(portalName).I$();
                int index = namedPortals.keyIndex(portalName);
                if (index > -1) {
                    // intern the name of the portal, the name will be cached in a list
                    portalName = Chars.toString(portalName);

                    // the current pipeline entry could either be named or unnamed
                    // we only have to clone the named entries, in case they are interleaved in the
                    // pipelines.
                    if (pipelineCurrentEntry.isPreparedStatement()) {
                        // the pipeline is named, and we must not attempt to reuse it
                        // as the portal, so we are making a new entry
                        PGPipelineEntry pe = new PGPipelineEntry(engine);
                        pe.parseNewSql(
                                pipelineCurrentEntry.getSqlText(),
                                engine,
                                sqlExecutionContext,
                                taiPool
                        );
                        pe.setParentPreparedStatement(pipelineCurrentEntry);
                        pe.copyStateFrom(pipelineCurrentEntry);
                        // Keep the reference to the portal name on the prepared statement before we overwrite the
                        // reference. Keeping list of portal names is required in case the client closes the prepared
                        // statement. We will also be required to close all the portals.
                        pipelineCurrentEntry.bindPortalName(portalName);
                        pipelineCurrentEntry.clearState();
                        pipelineCurrentEntry = pe;
                        pipelineCurrentEntry.setStateBind(true);
                    }
                    // else:
                    // portal is being created from "parse" message (i am not 100% the client will be
                    // doing this; they would have to send "parse" message without statement name and then
                    // send "bind" message without statement name but with portal). So we can use
                    // the current entry as the portal
                    pipelineCurrentEntry.setPortal(true, (String) portalName);
                    namedPortals.putAt(index, portalName, pipelineCurrentEntry);
                } else {
                    throw msgKaput().put("portal already exists [portalName=").put(portalName).put(']');
                }
            } else {
                throw msgKaput().put("cannot create portal for non-SELECT SQL [portalName=").put(portalName).put(']');
            }
        }

        // parameter format count
        lo = hi + 1;
        final short parameterFormatCodeCount = pipelineCurrentEntry.getShort(lo, msgLimit, "could not read parameter format code count");
        lo += Short.BYTES;
        pipelineCurrentEntry.msgBindCopyParameterFormatCodes(
                lo,
                msgLimit,
                parameterFormatCodeCount
        );

        // parameter value count
        lo += parameterFormatCodeCount * Short.BYTES;
        parameterValueCount = pipelineCurrentEntry.getShort(lo, msgLimit, "could not read parameter value count");

        LOG.debug().$("binding [parameterValueCount=").$(parameterValueCount).$(", thread=").$(Thread.currentThread().getId()).I$();

        // we now have all parameter counts, validate them
        pipelineCurrentEntry.msgBindSetParameterValueCount(parameterValueCount);

        lo += Short.BYTES;

        lo = pipelineCurrentEntry.msgBindDefineBindVariableTypes(
                lo,
                msgLimit,
                bindVariableService,
                // below are some reusable, transient pools
                characterStore,
                utf8String,
                binarySequenceParamsPool
        );

        short columnFormatCodeCount = pipelineCurrentEntry.getShort(lo, msgLimit, "could not read result set column format codes");
        lo += Short.BYTES;
        pipelineCurrentEntry.msgBindCopySelectFormatCodes(lo, msgLimit, columnFormatCodeCount);
    }

    private void msgClose(long lo, long msgLimit) throws BadProtocolException {
        // 'close' message can either:
        // - close the named entity, portal or statement
        final byte type = Unsafe.getUnsafe().getByte(lo);
        PGPipelineEntry lookedUpPipelineEntry;
        switch (type) {
            case 'S':
                // invalid statement names are allowed (as noop)
                // Closing statement also closes all bound portals. Portals will have the same
                // reference of the pipeline entry as the statement, so we only need to remove this
                // reference from maps.
                lo = lo + 1;
                final long hi = getUtf8StrSize(lo, msgLimit, "bad prepared statement name length", pipelineCurrentEntry);
                lookedUpPipelineEntry = uncacheNamedStatement(getUtf16Str(lo, hi, "invalid UTF8 bytes in statement name (close)"));
                break;
            case 'P':
                lo = lo + 1;
                final long high = getUtf8StrSize(lo, msgLimit, "bad prepared portal name length (close)", pipelineCurrentEntry);
                lookedUpPipelineEntry = uncacheNamedPortal(getUtf16Str(lo, high, "invalid UTF8 bytes in portal name (close)"));
                break;
            default:
                throw msgKaput().put("invalid type for close message [type=").put(type).put(']');
        }

        // if we already have the current pipeline entry, we will use that to produce the 'close'
        // message to the client. Otherwise, our options are:
        // - we can use the entry we looked up using the prepared statements' name
        // - create a brand-new entry
        if (pipelineCurrentEntry == null) {
            if (lookedUpPipelineEntry != null) {
                pipelineCurrentEntry = lookedUpPipelineEntry;
            } else {
                pipelineCurrentEntry = new PGPipelineEntry(engine);
            }
        } else {
            Misc.free(lookedUpPipelineEntry);
        }

        pipelineCurrentEntry.setStateClosed(true);

        // It is possible that the intent to close current pipeline entry was mis-labelled
        // for example, Rust driver creates named statement and then closes "null" 'portal'
        if (lookedUpPipelineEntry == null) {
            if (pipelineCurrentEntry.isPreparedStatement()) {
                uncacheNamedStatement(pipelineCurrentEntry.getPreparedStatementName());
            } else if (pipelineCurrentEntry.isPortal()) {
                uncacheNamedPortal(pipelineCurrentEntry.getPortalName());
            }
        }
    }

    private void msgDescribe(long lo, long msgLimit) throws BadProtocolException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        // 'S' = statement name
        // 'P' = portal name
        // followed by the name, which can be NULL, typically with 'P'
        boolean isPortal = Unsafe.getUnsafe().getByte(lo) == 'P';
        // todo: we can use utf8 names in maps and also lookup 0 terminator more efficiently
        final long hi = getUtf8StrSize(lo + 1, msgLimit, "bad prepared statement name length (describe)", pipelineCurrentEntry);
        final boolean nullTargetName;
        if (isPortal) {
            nullTargetName = lookupPipelineEntryForPortalName(
                    getUtf16Str(lo + 1, hi, "invalid UTF8 bytes in portal name (describe)")
            );
        } else {
            nullTargetName = lookupPipelineEntryForStatementName(
                    getUtf16Str(lo + 1, hi, "invalid UTF8 bytes in statement name (describe)")
            );
        }

        // some defensive code to have predictable behaviour
        // when dealing with spurious "describe" messages, for which we do not have
        // a pipeline entry

        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("spurious describe message received");
        }

        pipelineCurrentEntry.setStateDesc(nullTargetName ? 1 : isPortal ? 2 : 3);
    }

    private void msgExecute(long lo, long msgLimit) throws BadProtocolException {
        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        final long hi = getUtf8StrSize(lo, msgLimit, "bad portal name length (execute)", pipelineCurrentEntry);
        lookupPipelineEntryForPortalName(getUtf16Str(lo, hi, "invalid UTF8 bytes in portal name (execute)"));

        if (pipelineCurrentEntry == null) {
            throw msgKaput().put("spurious execute message");
        }

        lo = hi + 1;
        pipelineCurrentEntry.setReturnRowCountLimit(pipelineCurrentEntry.getInt(lo, msgLimit, "could not read max rows value"));
        pipelineCurrentEntry.setStateExec(true);
        transactionState = pipelineCurrentEntry.execute(
                sqlExecutionContext,
                transactionState,
                taiCache,
                pendingWriters,
                this,
                namedStatements
        );
    }

    private void msgFlush() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, BadProtocolException {
        addPipelineEntry();
        // "The Flush message does not cause any specific output to be generated, but forces the backend to deliver any data pending in its output buffers.
        //  A Flush must be sent after any extended-query command except Sync, if the frontend wishes to examine the results of that command before issuing more commands.
        //  Without Flush, messages returned by the backend will be combined into the minimum possible number of packets to minimize network overhead."
        // some clients (asyncpg) chose not to send 'S' (sync) message
        // but instead fire 'H'. Can't wrap my head around as to why
        // query execution is so ambiguous

        resumeCallback = this::msgFlush0;
        msgFlush0();
    }

    private void msgFlush0() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, BadProtocolException {
        syncPipeline();
        responseUtf8Sink.sendBufferAndReset();
        resumeCallback = null;
    }

    private BadProtocolException msgKaput() {
        // The error message and position is to be reported to the client
        // To do that, we store message on the pipeline entry, if it exists and
        // make sure this entry is added to the pipeline (eventually)
        if (pipelineCurrentEntry == null) {
            pipelineCurrentEntry = new PGPipelineEntry(engine);
        }
        return BadProtocolException.instance(pipelineCurrentEntry);
    }

    private void msgParse(long address, long lo, long msgLimit) throws BadProtocolException {

        if (pipelineCurrentEntry != null && pipelineCurrentEntry.isError()) {
            return;
        }

        // Parse message typically starts a new pipeline entry. So if there is existing one in flight
        // we have to add it to the pipeline
        addPipelineEntry();

        pipelineCurrentEntry = new PGPipelineEntry(engine);

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
        final CharSequence targetStatementName = getUtf16Str(lo, hi, "invalid UTF8 bytes in statement name (parse)");

        // read query text from the message
        lo = hi + 1;
        hi = getUtf8StrSize(lo, msgLimit, "bad query text length", pipelineCurrentEntry);
        final CharacterStoreEntry e = characterStore.newEntry();
        if (!Utf8s.utf8ToUtf16(lo, hi, e)) {
            throw msgKaput().put("invalid UTF8 bytes in parse query");
        }

        final CharSequence utf16SqlText = e.toImmutable();
        lo = hi + 1;

        // read parameter types before we are able to compile SQL text
        // parameter values are not provided here, but we do not need them to be able to
        // parse/compile the SQL
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

        // Let's try to see if we have this SQL cached
        // possible cache hits or misses:
        // 0 - did not hit any cache
        // 1 - hit "insert" cache but decided not to use it
        // 2 - hit "insert" cache and using it

        int cachedHit = 0;
        final TypesAndInsert tai = taiCache.peek(utf16SqlText);
        if (tai != null) {
            if (pipelineCurrentEntry.msgParseReconcileParameterTypes(parameterTypeCount, tai)) {
                pipelineCurrentEntry.ofInsert(utf16SqlText, tai);
                cachedHit = 2;
            } else {
                //todo: find more efficient way to remove from cache what we have already looked up
                // remove cached item, we will create it again, may be
                TypesAndInsert tai2 = taiCache.poll(utf16SqlText);
                assert tai2 == tai;
                tai.close();
                cachedHit = 1;
            }
        }

        if (cachedHit == 0) {
            final TypesAndSelect tas = tasCache.poll(utf16SqlText);
            if (tas != null) {
                if (pipelineCurrentEntry.msgParseReconcileParameterTypes(parameterTypeCount, tas)) {
                    pipelineCurrentEntry.ofSelect(utf16SqlText, tas);
                    cachedHit = 4;
                } else {
                    tas.close();
                    cachedHit = 3;
                }
            }
        }

        if (cachedHit != 2 && cachedHit != 4) {
            // When parameter types are not supplied we will assume that the types are STRING
            // this is done by default, when CairoEngine compiles the SQL text. Assuming we're
            // compiling the SQL from scratch.
            pipelineCurrentEntry.parseNewSql(utf16SqlText, engine, sqlExecutionContext, taiPool);
        }
        msgParseCreateTargetStatement(targetStatementName);
    }

    private void msgParseCreateTargetStatement(CharSequence targetStatementName) throws BadProtocolException {
        if (targetStatementName != null) {
            LOG.info().$("create prepared statement [name=").$(targetStatementName).I$();
            int index = namedStatements.keyIndex(targetStatementName);
            if (index > -1) {
                final String preparedStatementName = Chars.toString(targetStatementName);
                pipelineCurrentEntry.setPreparedStatement(true, preparedStatementName);
                namedStatements.putAt(index, preparedStatementName, pipelineCurrentEntry);
            } else {
                throw msgKaput()
                        .put("duplicate statement [name=").put(targetStatementName).put(']');
            }
        }
    }

    // processes one or more queries (batch/script). "Simple Query" in PostgreSQL docs.
    private void msgQuery(long lo, long limit) throws BadProtocolException, PeerIsSlowToReadException, QueryPausedException, PeerDisconnectedException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Utf8s.utf8ToUtf16(lo, limit - 1, e)) {
            CharSequence activeSqlText = characterStore.toImmutable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compileBatch(activeSqlText, sqlExecutionContext, batchCallback);
            } catch (Throwable ex) {
                transactionState = ERROR_TRANSACTION;
                throw msgKaput().put(ex);
            } finally {
                msgSync();
            }
        } else {
            throw msgKaput().put("invalid UTF8 bytes in parse query");
        }
    }

    private void msgSync() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, BadProtocolException {
        addPipelineEntry();

        // the sync0 is liable to get interrupted due to:
        // 1. network client being slow
        // 2. SQL might get paused due to data not being available yet
        // however, sync0 is reenterable and we have to call it until
        // the resume callback clears
        resumeCallback = this::msgSync0;
        msgSync0();
    }

    private void msgSync0() throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, BadProtocolException {
        syncPipeline();

        // flush the buffer in case response message does not fit the buffer
        if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
            responseUtf8Sink.sendBufferAndReset();
        }
        outReadForNewQuery();
        resumeCallback = null;
        responseUtf8Sink.sendBufferAndReset();

        // todo: this is a wrap, prepare for new query execution
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
            throws BadProtocolException, PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException {
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
        LOG.debug().$("prepare for new query").$();
        Misc.clear(bindVariableService);
        freezeRecvBuffer = false;
        sqlExecutionContext.setCacheHit(false);
        sqlExecutionContext.containsSecret(false);
    }

    private void prepareForNewQuery() {
        prepareForNewBatchQuery();
        Misc.clear(characterStore);
    }

    private void prepareNonCriticalError(int position, CharSequence message) {
        prepareErrorResponse(position, message);
        LOG.error().$("error [pos=").$(position).$(", msg=`").utf8(message).$('`').I$();
    }

    private void replaceCurrentPipelineEntry(PGPipelineEntry newEntry) {
        if (newEntry != pipelineCurrentEntry) {
            // Alright, the client wants to use the named statement. What if they just
            // send "parse" message and want to abandon it?
            freeIfAbandoned(pipelineCurrentEntry);
            // it is safe to overwrite the pipeline entry,
            // named entries will be held in the hash map
            pipelineCurrentEntry = newEntry;
        }
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
    }

    private void sendReadyForNewQuery() throws PeerDisconnectedException {
        LOG.debug().$("RNQ sent").$();
        sendBufferAndResetBlocking();

        outReadForNewQuery();
    }

    private void shiftReceiveBuffer(long readOffsetBeforeParse) {
        final long len = recvBufferWriteOffset - readOffsetBeforeParse;
        LOG.debug().$("shift [offset=").$(readOffsetBeforeParse).$(", len=").$(len).I$();

        Vect.memmove(recvBuffer, recvBuffer + readOffsetBeforeParse, len);
        recvBufferWriteOffset = len;
        recvBufferReadOffset = 0;
    }

    // Send responses from the pipeline entries we have accumulated so far.
    private void syncPipeline() throws PeerIsSlowToReadException, QueryPausedException, BadProtocolException, PeerDisconnectedException {
        while (pipelineCurrentEntry != null || (pipelineCurrentEntry = pipeline.poll()) != null) {
            // with the sync call the existing pipeline entry will assign its own completion hooks (resume callbacks)
            try {
                transactionState = pipelineCurrentEntry.sync(
                        sqlExecutionContext,
                        transactionState,
                        taiCache,
                        pendingWriters,
                        this,
                        namedStatements,
                        responseUtf8Sink
                );
            } catch (NoSpaceLeftInResponseBufferException e) {
                responseUtf8Sink.resetToBookmark();
                responseUtf8Sink.sendBufferAndReset();
                try {
                    transactionState = pipelineCurrentEntry.sync(
                            sqlExecutionContext,
                            transactionState,
                            taiCache,
                            pendingWriters,
                            this,
                            namedStatements,
                            responseUtf8Sink
                    );
                } catch (NoSpaceLeftInResponseBufferException e1) {
                    // oopsie, buffer is too small for single record
                    responseUtf8Sink.reset();
                    throw msgKaput()
                            .put("not enough space in send buffer [sendBufferSize=").put(responseUtf8Sink.getSendBufferSize()).put(']');
                }
            }
            pipelineCurrentEntry.cacheIfPossible(tasCache, taiCache);
            freeIfAbandoned(pipelineCurrentEntry);
            pipelineCurrentEntry = null;
        }
    }

    private PGPipelineEntry uncacheNamedPortal(CharSequence portalName) {
        if (portalName != null) {
            final int index = namedPortals.keyIndex(portalName);
            if (index < 0) {
                PGPipelineEntry pe = namedPortals.valueAt(index);
                PGPipelineEntry peParent = pe.getParentPreparedStatementPipelineEntry();
                if (peParent != null) {
                    int parentIndex = peParent.getPortalNames().indexOf(portalName);
                    if (parentIndex != -1) {
                        peParent.getPortalNames().remove(parentIndex);
                    }
                }
                namedPortals.removeAt(index);
                return pe;
            }
        }
        return null;
    }

    private PGPipelineEntry uncacheNamedStatement(CharSequence statementName) {
        if (statementName != null) {
            int index = namedStatements.keyIndex(statementName);
            if (index < 0) {
                PGPipelineEntry pe = namedStatements.valueAt(index);
                namedStatements.removeAt(index);
                // also remove entries for the matching portal names
                ObjList<CharSequence> portalNames = pe.getPortalNames();
                for (int i = 0, n = portalNames.size(); i < n; i++) {
                    int portalKeyIndex = this.namedPortals.keyIndex(portalNames.getQuick(i));
                    if (portalKeyIndex < 0) {
                        // release the entry, it must not be referenced from anywhere other than
                        // this list (we enforce portal name uniqueness)
                        Misc.free(this.namedPortals.valueAt(portalKeyIndex));
                        this.namedPortals.removeAt(portalKeyIndex);
                    } else {
                        // else: do not make a fuss if portal name does not exist
                        LOG.debug()
                                .$("ignoring non-existent portal [portalName=").$(portalNames.getQuick(i))
                                .$(", statementName=").$(statementName)
                                .I$();
                    }
                }
                return pe;
            }
        }
        return null;
    }

    static void dumpBuffer(char direction, long buffer, int len, boolean dumpNetworkTraffic) {
        if (dumpNetworkTraffic && len > 0) {
            StdoutSink.INSTANCE.put(direction);
            Net.dump(buffer, len);
        }
    }

    static void freeIfAbandoned(PGPipelineEntry pe) {
        if (pe != null) {
            if (!pe.isPreparedStatement() && !pe.isPortal()) {
                Misc.free(pe);
            } else {
                pe.clearState();
            }
        }
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
        public void postCompile(SqlCompiler compiler, CompiledQuery cq, CharSequence text) throws Exception {
            pipelineCurrentEntry.ofSimpleQuery(
                    Chars.toString(text), // todo: we just need an immutable copy of the text, not a new string
                    sqlExecutionContext,
                    cq,
                    taiPool
            );
            transactionState = pipelineCurrentEntry.execute(
                    sqlExecutionContext,
                    transactionState,
                    taiCache,
                    pendingWriters,
                    PGConnectionContext.this,
                    namedStatements
            );
            pipelineCurrentEntry.setStateExec(true);
        }

        @Override
        public void preCompile(SqlCompiler compiler) {
            addPipelineEntry();
            pipelineCurrentEntry = new PGPipelineEntry(engine);
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
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
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
        public long getSendBufferSize() {
            return sendBufferSize;
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
            if (bookmarkPtr != -1) {
                sendBufferPtr = bookmarkPtr;
                bookmarkPtr = -1;
            }
        }

        @Override
        public void sendBufferAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendBuffer(bufferRemainingOffset, (int) (sendBufferPtr - sendBuffer - bufferRemainingOffset));
            responseUtf8Sink.reset();
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
