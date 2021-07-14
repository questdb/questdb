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

package io.questdb.cutlass.pgwire;

import io.questdb.MessageBus;
import io.questdb.Telemetry;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cutlass.pgwire.PGOids.*;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_MILLI_TIME_Z_FORMAT;
import static io.questdb.std.datetime.millitime.DateFormatUtils.PG_DATE_Z_FORMAT;

public class PGConnectionContext implements IOContext, Mutable, WriterSource {
    public static final String TAG_SET = "SET";
    public static final String TAG_BEGIN = "BEGIN";
    public static final String TAG_COMMIT = "COMMIT";
    public static final String TAG_ROLLBACK = "ROLLBACK";
    public static final String TAG_SELECT = "SELECT";
    public static final String TAG_OK = "OK";
    public static final String TAG_COPY = "COPY";
    public static final String TAG_INSERT = "INSERT";
    public static final char STATUS_IN_TRANSACTION = 'T';
    public static final char STATUS_IN_ERROR = 'E';
    public static final char STATUS_IDLE = 'I';
    private static final int INT_BYTES_X = Numbers.bswap(Integer.BYTES);
    private static final int INT_NULL_X = Numbers.bswap(-1);
    private static final int SYNC_PARSE = 1;
    private static final int SYNC_DESCRIBE = 2;
    private static final int SYNC_BIND = 3;
    private static final int SYNC_DESCRIBE_PORTAL = 4;
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final int INIT_SSL_REQUEST = 80877103;
    private static final int INIT_STARTUP_MESSAGE = 196608;
    private static final int INIT_CANCEL_REQUEST = 80877102;
    private static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    private static final byte MESSAGE_TYPE_EMPTY_QUERY = 'I';
    private static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private final static Log LOG = LogFactory.getLog(PGConnectionContext.class);
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    private static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    private static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';
    private static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    private static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';
    private static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';
    private static final byte MESSAGE_TYPE_NO_DATA = 'n';
    private static final byte MESSAGE_TYPE_COPY_IN_RESPONSE = 'G';
    private static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';
    private static final int NO_TRANSACTION = 0;
    private static final int IN_TRANSACTION = 1;
    private static final int COMMIT_TRANSACTION = 2;
    private static final int ERROR_TRANSACTION = 3;
    private static final int ROLLING_BACK_TRANSACTION = 4;
    private static final String WRITER_LOCK_REASON = "pgConnection";
    private static final int PROTOCOL_TAIL_COMMAND_LENGTH = 64;
    private final long recvBuffer;
    private final long sendBuffer;
    private final int recvBufferSize;
    private final CharacterStore characterStore;
    private final BindVariableService bindVariableService;
    private final long sendBufferLimit;
    private final int sendBufferSize;
    private final ResponseAsciiSink responseAsciiSink = new ResponseAsciiSink();
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final int maxBlobSizeOnQuery;
    private final NetworkFacade nf;
    private final boolean dumpNetworkTraffic;
    private final int idleSendCountBeforeGivingUp;
    private final int idleRecvCountBeforeGivingUp;
    private final String serverVersion;
    private final PGAuthenticator authenticator;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final Path path = new Path();
    private final IntList bindVariableTypes = new IntList();
    private final IntList selectColumnTypes = new IntList();
    private final WeakObjectPool<NamedStatementWrapper> namedStatementWrapperPool;
    private final WeakObjectPool<Portal> namedPortalPool;
    private final WeakAutoClosableObjectPool<TypesAndInsert> typesAndInsertPool;
    private final DateLocale locale;
    private final CharSequenceObjHashMap<TableWriter> pendingWriters;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final AssociativeCache<TypesAndInsert> typesAndInsertCache;
    private final CharSequenceObjHashMap<NamedStatementWrapper> namedStatementMap;
    private final CharSequenceObjHashMap<Portal> namedPortalMap;
    private final IntList syncActions = new IntList(4);
    private final CairoEngine engine;
    private IntList activeSelectColumnTypes;
    private int parsePhaseBindVariableCount;
    private long sendBufferPtr;
    private boolean requireInitialMessage = false;
    private long recvBufferWriteOffset = 0;
    private long recvBufferReadOffset = 0;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private RecordCursor currentCursor = null;
    private RecordCursorFactory currentFactory = null;
    // these references are held by context only for a period of processing single request
    // in PF world this request can span multiple messages, but still, only for one request
    // the rationale is to be able to return "selectAndTypes" instance to thread-local
    // cache, which is "typesAndSelectCache". We typically do this after query results are
    // served to client or query errored out due to network issues
    private TypesAndSelect typesAndSelect = null;
    private TypesAndInsert typesAndInsert = null;
    private long fd;
    private CharSequence queryText;
    private CharSequence queryTag;
    private CharSequence username;
    private boolean authenticationRequired = true;
    private IODispatcher<PGConnectionContext> dispatcher;
    private Rnd rnd;
    private long rowCount;
    private boolean completed = true;
    private boolean isEmptyQuery;
    private final PGResumeProcessor resumeCommandCompleteRef = this::resumeCommandComplete;
    private int transactionState = NO_TRANSACTION;
    private final PGResumeProcessor resumeQueryCompleteRef = this::resumeQueryComplete;
    private NamedStatementWrapper wrapper;
    private AssociativeCache<TypesAndSelect> typesAndSelectCache;
    private WeakAutoClosableObjectPool<TypesAndSelect> typesAndSelectPool;
    // this is a reference to types either from the context or named statement, where it is provided
    private IntList activeBindVariableTypes;
    private boolean sendParameterDescription;
    private PGResumeProcessor resumeProcessor;
    private long maxRows;
    private final PGResumeProcessor resumeCursorExecuteRef = this::resumeCursorExecute;
    private final PGResumeProcessor resumeCursorQueryRef = this::resumeCursorQuery;

    public PGConnectionContext(
            CairoEngine engine,
            PGWireConfiguration configuration,
            @Nullable MessageBus messageBus,
            int workerCount
    ) {
        this.engine = engine;
        this.utf8Sink = new DirectCharSink(engine.getConfiguration().getTextConfiguration().getUtf8SinkSize());
        this.typeManager = new TypeManager(engine.getConfiguration().getTextConfiguration(), utf8Sink);
        this.nf = configuration.getNetworkFacade();
        this.bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        this.recvBufferSize = Numbers.ceilPow2(configuration.getRecvBufferSize());
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
        this.sendBufferPtr = sendBuffer;
        this.sendBufferLimit = sendBuffer + sendBufferSize;
        this.characterStore = new CharacterStore(
                configuration.getCharacterStoreCapacity(),
                configuration.getCharacterStorePoolCapacity()
        );
        this.maxBlobSizeOnQuery = configuration.getMaxBlobSizeOnQuery();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.idleSendCountBeforeGivingUp = configuration.getIdleSendCountBeforeGivingUp();
        this.idleRecvCountBeforeGivingUp = configuration.getIdleRecvCountBeforeGivingUp();
        this.serverVersion = configuration.getServerVersion();
        this.authenticator = new PGBasicAuthenticator(configuration.getDefaultUsername(), configuration.getDefaultPassword());
        this.locale = configuration.getDefaultDateLocale();
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount, messageBus);
        this.sqlExecutionContext.setRandom(this.rnd = configuration.getRandom());
        this.namedStatementWrapperPool = new WeakObjectPool<>(NamedStatementWrapper::new, configuration.getNamesStatementPoolCapacity()); // 16
        this.namedPortalPool = new WeakObjectPool<>(Portal::new, configuration.getNamesStatementPoolCapacity()); // 16
        this.typesAndInsertPool = new WeakAutoClosableObjectPool<>(TypesAndInsert::new, configuration.getInsertPoolCapacity()); // 32
        this.typesAndInsertCache = new AssociativeCache<>(
                configuration.getInsertCacheBlockCount(), // 8
                configuration.getInsertCacheRowCount()
        );
        this.namedStatementMap = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
        this.pendingWriters = new CharSequenceObjHashMap<>(configuration.getPendingWritersCacheSize());
        this.namedPortalMap = new CharSequenceObjHashMap<>(configuration.getNamedStatementCacheCapacity());
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
        sendBufferPtr = sendBuffer;
        requireInitialMessage = true;
        bufferRemainingOffset = 0;
        bufferRemainingSize = 0;
        responseAsciiSink.reset();
        prepareForNewQuery();
        authenticationRequired = true;
        username = null;
        typeManager.clear();
        clearWriters();
        clearRecvBuffer();
        typesAndInsertCache.clear();
        namedStatementMap.clear();
        namedPortalMap.clear();
        bindVariableService.clear();
        bindVariableTypes.clear();
    }

    public void clearWriters() {
        for (int i = 0, n = pendingWriters.size(); i < n; i++) {
            Misc.free(pendingWriters.valueQuick(i));
        }
        pendingWriters.clear();
    }

    @Override
    public void close() {
        clear();
        this.fd = -1;
        sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null, -1, null);
        Unsafe.free(sendBuffer, sendBufferSize);
        Unsafe.free(recvBuffer, recvBufferSize);
        Misc.free(typesAndSelectCache);
        Misc.free(path);
        Misc.free(utf8Sink);
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean invalid() {
        return fd == -1;
    }

    @Override
    public IODispatcher<PGConnectionContext> getDispatcher() {
        return dispatcher;
    }

    @Override
    public TableWriter getWriter(CairoSecurityContext context, CharSequence name, CharSequence lockReason) {
        final int index = pendingWriters.keyIndex(name);
        if (index < 0) {
            return pendingWriters.valueAt(index);
        }
        return engine.getWriter(context, name, lockReason);
    }

    public void handleClientOperation(
            @Transient SqlCompiler compiler,
            @Transient AssociativeCache<TypesAndSelect> selectAndTypesCache,
            @Transient WeakAutoClosableObjectPool<TypesAndSelect> selectAndTypesPool,
            int operation
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, PeerIsSlowToWriteException, BadProtocolException {

        this.typesAndSelectCache = selectAndTypesCache;
        this.typesAndSelectPool = selectAndTypesPool;

        try {
            if (bufferRemainingSize > 0) {
                doSend(bufferRemainingOffset, bufferRemainingSize);
                if (resumeProcessor != null) {
                    resumeProcessor.resume();
                }
            }

            boolean keepReceiving = true;
            OUTER:
            do {
                if (operation == IOOperation.READ) {
                    if (recv() == 0) {
                        keepReceiving = false;
                    }
                }

                // we do not pre-compute length because 'parse' will mutate 'recvBufferReadOffset'
                if (keepReceiving) {
                    do {
                        // Parse will update the value of recvBufferOffset upon completion of
                        // logical block. We cannot count on return value because 'parse' may try to
                        // respond to client and fail with exception. When it does fail we would have
                        // to retry 'send' but not parse the same input again

                        long readOffsetBeforeParse = recvBufferReadOffset;
                        parse(
                                recvBuffer + recvBufferReadOffset,
                                (int) (recvBufferWriteOffset - recvBufferReadOffset),
                                compiler
                        );

                        // nothing changed?
                        if (readOffsetBeforeParse == recvBufferReadOffset) {
                            // shift to start
                            if (readOffsetBeforeParse > 0) {
                                shiftReceiveBuffer(readOffsetBeforeParse);
                            }
                            continue OUTER;
                        }
                    } while (recvBufferReadOffset < recvBufferWriteOffset);
                    clearRecvBuffer();
                }
            } while (keepReceiving && operation == IOOperation.READ);
        } catch (SqlException e) {
            reportError(e.getPosition(), e.getFlyweightMessage());
        } catch (CairoException e) {
            reportError(-1, e.getFlyweightMessage());
        }
    }

    public PGConnectionContext of(long clientFd, IODispatcher<PGConnectionContext> dispatcher) {
        this.fd = clientFd;
        sqlExecutionContext.with(clientFd);
        this.dispatcher = dispatcher;
        clear();
        return this;
    }

    public void setBooleanBindVariable(int index, int valueLen) throws SqlException {
        if (valueLen != 4 && valueLen != 5) {
            throw SqlException.$(0, "bad value for BOOLEAN parameter [index=").put(index).put(", valueLen=").put(valueLen).put(']');
        }
        bindVariableService.setBoolean(index, valueLen == 4);
    }

    public void setCharBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Chars.utf8Decode(address, address + valueLen, e)) {
            bindVariableService.setChar(index, characterStore.toImmutable().charAt(0));
        } else {
            LOG.error().$("invalid char UTF8 bytes [index=").$(index).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setDateBindVariable(int index, long address, int valueLen) throws SqlException {
        dbcs.of(address, address + valueLen);
        try {
            bindVariableService.setDate(index, PG_DATE_Z_FORMAT.parse(dbcs, locale));
        } catch (NumericException ex) {
            throw SqlException.$(0, "bad parameter value [index=").put(index).put(", value=").put(dbcs).put(']');
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

    public void setStrBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Chars.utf8Decode(address, address + valueLen, e)) {
            bindVariableService.setStr(index, characterStore.toImmutable());
        } else {
            LOG.error().$("invalid str UTF8 bytes [index=").$(index).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setTimestampBindVariable(int index, long address, int valueLen) throws BadProtocolException, SqlException {
        ensureValueLength(index, Long.BYTES, valueLen);
        bindVariableService.setTimestamp(index, getLongUnsafe(address) + Numbers.JULIAN_EPOCH_OFFSET_USEC);
    }

    private static int getIntUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getInt(address));
    }

    private static short getShortUnsafe(long address) {
        return Numbers.bswap(Unsafe.getUnsafe().getShort(address));
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

    private static void prepareParams(PGConnectionContext.ResponseAsciiSink sink, String name, String value) {
        sink.put(MESSAGE_TYPE_PARAMETER_STATUS);
        final long addr = sink.skip();
        sink.encodeUtf8Z(name);
        sink.encodeUtf8Z(value);
        sink.putLen(addr);
    }

    private static void bindParameterFormats(
            long lo,
            long msgLimit,
            short parameterFormatCount,
            IntList bindVariableTypes
    ) throws BadProtocolException {
        if (lo + Short.BYTES * parameterFormatCount <= msgLimit) {
            LOG.debug().$("processing bind formats [count=").$(parameterFormatCount).$(']').$();
            for (int i = 0; i < parameterFormatCount; i++) {
                final short code = getShortUnsafe(lo + i * Short.BYTES);
                bindVariableTypes.setQuick(i, toParamBinaryType(code, bindVariableTypes.getQuick(i)));
            }
        } else {
            LOG.error().$("invalid format code count [value=").$(parameterFormatCount).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void setupBindVariables(long lo, IntList bindVariableTypes, int count) {
        bindVariableTypes.setPos(count);
        for (int i = 0; i < count; i++) {
            bindVariableTypes.setQuick(i, Unsafe.getUnsafe().getInt(lo + i * 4L));
        }
    }

    private static void bindSingleFormatForAll(long lo, long msgLimit, IntList activeBindVariableTypes) throws BadProtocolException {
        short code = getShort(lo, msgLimit, "could not read parameter formats");
        for (int i = 0, n = activeBindVariableTypes.size(); i < n; i++) {
            activeBindVariableTypes.setQuick(i, toParamBinaryType(code, activeBindVariableTypes.getQuick(i)));
        }
    }

    private void appendBinColumn(Record record, int i) throws SqlException {
        BinarySequence sequence = record.getBin(i);
        if (sequence == null) {
            responseAsciiSink.setNullValue();
        } else {
            // if length is above max we will error out the result set
            long blobSize = sequence.length();
            if (blobSize < maxBlobSizeOnQuery) {
                responseAsciiSink.put(sequence);
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
        responseAsciiSink.putNetworkInt(Byte.BYTES);
        responseAsciiSink.put(record.getBool(columnIndex) ? 't' : 'f');
    }

    private void appendByteColumn(Record record, int columnIndex) {
        long a = responseAsciiSink.skip();
        responseAsciiSink.put((int) record.getByte(columnIndex));
        responseAsciiSink.putLenEx(a);
    }

    private void appendByteColumnBin(Record record, int columnIndex) {
        final byte value = record.getByte(columnIndex);
        responseAsciiSink.putNetworkInt(Short.BYTES);
        responseAsciiSink.putNetworkShort(value);
    }

    private void appendCharColumn(Record record, int columnIndex) {
        final char charValue = record.getChar(columnIndex);
        if (charValue == 0) {
            responseAsciiSink.setNullValue();
        } else {
            long a = responseAsciiSink.skip();
            responseAsciiSink.putUtf8(charValue);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendDateColumn(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            final long a = responseAsciiSink.skip();
            PG_DATE_MILLI_TIME_Z_FORMAT.format(longValue, null, null, responseAsciiSink);
            responseAsciiSink.putLenEx(a);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendDateColumnBin(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            responseAsciiSink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            responseAsciiSink.putNetworkLong(longValue * 1000 - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendDoubleColumn(Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (doubleValue == doubleValue) {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(doubleValue);
            responseAsciiSink.putLenEx(a);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendDoubleColumnBin(Record record, int columnIndex) {
        final double value = record.getDouble(columnIndex);
        if (value == value) {
            responseAsciiSink.putNetworkInt(Double.BYTES);
            responseAsciiSink.putNetworkDouble(value);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendFloatColumn(Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (floatValue == floatValue) {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(floatValue, 3);
            responseAsciiSink.putLenEx(a);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendFloatColumnBin(Record record, int columnIndex) {
        final float value = record.getFloat(columnIndex);
        if (value == value) {
            responseAsciiSink.putNetworkInt(Float.BYTES);
            responseAsciiSink.putNetworkFloat(value);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendIntCol(Record record, int i) {
        final int intValue = record.getInt(i);
        if (intValue != Numbers.INT_NaN) {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(intValue);
            responseAsciiSink.putLenEx(a);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendIntColumnBin(Record record, int columnIndex) {
        final int value = record.getInt(columnIndex);
        if (value != Numbers.INT_NaN) {
            responseAsciiSink.ensureCapacity(8);
            responseAsciiSink.putIntUnsafe(0, INT_BYTES_X);
            responseAsciiSink.putIntUnsafe(4, Numbers.bswap(value));
            responseAsciiSink.bump(8);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendLong256Column(Record record, int columnIndex) {
        final Long256 long256Value = record.getLong256A(columnIndex);
        if (long256Value.getLong0() == Numbers.LONG_NaN &&
                long256Value.getLong1() == Numbers.LONG_NaN &&
                long256Value.getLong2() == Numbers.LONG_NaN &&
                long256Value.getLong3() == Numbers.LONG_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            Numbers.appendLong256(long256Value.getLong0(), long256Value.getLong1(), long256Value.getLong2(), long256Value.getLong3(), responseAsciiSink);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendLongColumn(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(longValue);
            responseAsciiSink.putLenEx(a);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendLongColumnBin(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue != Numbers.LONG_NaN) {
            responseAsciiSink.putNetworkInt(Long.BYTES);
            responseAsciiSink.putNetworkLong(longValue);
        } else {
            responseAsciiSink.setNullValue();
        }
    }

    private void appendRecord(Record record, int columnCount) throws SqlException {
        responseAsciiSink.put(MESSAGE_TYPE_DATA_ROW); // data
        final long offset = responseAsciiSink.skip();
        responseAsciiSink.putNetworkShort((short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            switch (activeSelectColumnTypes.getQuick(i)) {
                case BINARY_TYPE_INT:
                    appendIntColumnBin(record, i);
                    break;
                case ColumnType.INT:
                    appendIntCol(record, i);
                    break;
                case ColumnType.NULL:
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
                case BINARY_TYPE_BOOLEAN:
                    appendBooleanColumn(record, i);
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
                default:
                    assert false;
            }
        }
        responseAsciiSink.putLen(offset);
        rowCount += 1;
    }

    private void appendShortColumn(Record record, int columnIndex) {
        final long a = responseAsciiSink.skip();
        responseAsciiSink.put(record.getShort(columnIndex));
        responseAsciiSink.putLenEx(a);
    }

    private void appendShortColumnBin(Record record, int columnIndex) {
        final short value = record.getShort(columnIndex);
        responseAsciiSink.putNetworkInt(Short.BYTES);
        responseAsciiSink.putNetworkShort(value);
    }

    private void appendSingleRecord(Record record, int columnCount) throws SqlException {
        try {
            appendRecord(record, columnCount);
        } catch (NoSpaceLeftInResponseBufferException e1) {
            // oopsie, buffer is too small for single record
            LOG.error().$("not enough space in buffer for row data [buffer=").$(sendBufferSize).I$();
            responseAsciiSink.reset();
            throw CairoException.instance(0).put("server configuration error: not enough space in send buffer for row data");
        }
    }

    private void appendStrColumn(Record record, int columnIndex) {
        final CharSequence strValue = record.getStr(columnIndex);
        if (strValue == null) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.encodeUtf8(strValue);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendSymbolColumn(Record record, int columnIndex) {
        final CharSequence strValue = record.getSym(columnIndex);
        if (strValue == null) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.encodeUtf8(strValue);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendTimestampColumn(Record record, int i) {
        long a;
        long longValue = record.getTimestamp(i);
        if (longValue == Numbers.LONG_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            a = responseAsciiSink.skip();
            TimestampFormatUtils.PG_TIMESTAMP_FORMAT.format(longValue, null, null, responseAsciiSink);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendTimestampColumnBin(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue == Numbers.LONG_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            responseAsciiSink.putNetworkInt(Long.BYTES);
            // PG epoch starts at 2000 rather than 1970
            responseAsciiSink.putNetworkLong(longValue - Numbers.JULIAN_EPOCH_OFFSET_USEC);
        }
    }

    private void assertTrue(boolean check, String message) throws BadProtocolException {
        if (check) {
            return;
        }
        // we did not find 0 within message limit
        LOG.error().$(message).$();
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
                        .$(']').$();
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
                    default:
                        setStrBindVariable(j, lo, valueLen);
                        break;
                }
                lo += valueLen;
            } else {
                LOG.error()
                        .$("value length is outside of buffer [parameterIndex=").$(j)
                        .$(", valueLen=").$(valueLen)
                        .$(", messageRemaining=").$(msgLimit - lo)
                        .$(']').$();
                throw BadProtocolException.INSTANCE;
            }
        }
        return lo;
    }

    private void buildSelectColumnTypes() {
        final RecordMetadata m = typesAndSelect.getFactory().getMetadata();
        final int columnCount = m.getColumnCount();
        activeSelectColumnTypes.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            activeSelectColumnTypes.setQuick(i, m.getColumnType(i));
        }
    }

    void clearRecvBuffer() {
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
    }

    private boolean compileQuery(@Transient SqlCompiler compiler)
            throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        if (queryText != null && queryText.length() > 0) {

            // try insert, peek because this is our private cache
            // and we do not want to remove statement from it
            typesAndInsert = typesAndInsertCache.peek(queryText);

            // not found or not insert, try select
            // poll this cache because it is shared and we do not want
            // select factory to be used by another thread concurrently
            if (typesAndInsert != null) {
                typesAndInsert.defineBindVariables(bindVariableService);
                queryTag = TAG_INSERT;
                return false;
            }

            typesAndSelect = typesAndSelectCache.poll(queryText);

            if (typesAndSelect != null) {
                // cache hit, define bind variables
                bindVariableService.clear();
                typesAndSelect.defineBindVariables(bindVariableService);
                queryTag = TAG_SELECT;
                return false;
            }

            // not cached - compile to see what it is
            final CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);
            sqlExecutionContext.storeTelemetry(cc.getType(), Telemetry.ORIGIN_POSTGRES);

            switch (cc.getType()) {
                case CompiledQuery.SELECT:
                    typesAndSelect = typesAndSelectPool.pop();
                    typesAndSelect.of(cc.getRecordCursorFactory(), bindVariableService);
                    queryTag = TAG_SELECT;
                    LOG.debug().$("cache select [sql=").$(queryText).$(", thread=").$(Thread.currentThread().getId()).$(']').$();
                    break;
                case CompiledQuery.INSERT:
                    queryTag = TAG_INSERT;
                    typesAndInsert = typesAndInsertPool.pop();
                    typesAndInsert.of(cc.getInsertStatement(), bindVariableService);
                    if (bindVariableService.getIndexedVariableCount() > 0) {
                        LOG.debug().$("cache insert [sql=").$(queryText).$(", thread=").$(Thread.currentThread().getId()).$(']').$();
                        // we can add insert to cache right away because it is local to the connection
                        typesAndInsertCache.put(queryText, typesAndInsert);
                    }
                    break;
                case CompiledQuery.COPY_LOCAL:
                    // uncached
                    queryTag = TAG_COPY;
                    sendCopyInResponse(compiler.getEngine(), cc.getTextLoader());
                    break;
                case CompiledQuery.SET:
                    configureContextForSet();
                    break;
                default:
                    // DDL SQL
                    queryTag = TAG_OK;
                    break;
            }
        } else {
            isEmptyQuery = true;
        }

        return true;
    }

    private void configureContextForSet() {
        if (SqlKeywords.isBegin(queryText)) {
            queryTag = TAG_BEGIN;
            transactionState = IN_TRANSACTION;
        } else if (SqlKeywords.isCommit(queryText)) {
            queryTag = TAG_COMMIT;
            if (transactionState != ERROR_TRANSACTION) {
                transactionState = COMMIT_TRANSACTION;
            }
        } else if (SqlKeywords.isRollback(queryText)) {
            queryTag = TAG_ROLLBACK;
            transactionState = ROLLING_BACK_TRANSACTION;
        } else {
            queryTag = TAG_SET;
        }
    }

    private void configureContextFromNamedStatement(CharSequence statementName, @Nullable @Transient SqlCompiler compiler)
            throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {

        this.sendParameterDescription = statementName != null;

        if (wrapper != null) {
            LOG.debug().$("reusing existing wrapper").$();
            return;
        }

        // make sure there is no current wrapper is set, so that we don't assign values
        // from the wrapper back to context on the first pass where named statement is setup
        if (statementName != null) {
            LOG.debug().$("named statement [name=").$(statementName).$(']').$();
            wrapper = namedStatementMap.get(statementName);
            if (wrapper != null) {
                setupVariableSettersFromWrapper(wrapper, compiler);
            } else {
                // todo: when we have nothing for prepared statement name we need to produce an error
                LOG.error().$("statement does not exist [name=").$(statementName).$(']').$();
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
            LOG.error().$("duplicate portal [name=").$(portalName).$(']').$();
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
            namedStatementMap.putAt(index, Chars.toString(statementName), wrapper);
            this.activeBindVariableTypes = wrapper.bindVariableTypes;
            this.activeSelectColumnTypes = wrapper.selectColumnTypes;
        } else {
            LOG.error().$("duplicate statement [name=").$(statementName).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private void doAuthentication(long msgLo, long msgLimit) throws BadProtocolException, PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        final CairoSecurityContext cairoSecurityContext = authenticator.authenticate(username, msgLo, msgLimit);
        if (cairoSecurityContext != null) {
            sqlExecutionContext.with(cairoSecurityContext, bindVariableService, rnd, this.fd, null);
            authenticationRequired = false;
            prepareLoginOk();
            sendAndReset();
        }
    }

    int doReceive(int remaining) {
        final long data = recvBuffer + recvBufferWriteOffset;
        final int n = nf.recv(getFd(), data, remaining);
        dumpBuffer('>', data, n);
        return n;
    }

    void doSend(int offset, int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int n = nf.send(getFd(), sendBuffer + offset, size);
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

    private void doSendWithRetries(int bufferOffset, int bufferSize) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int offset = bufferOffset;
        int remaining = bufferSize;
        int idleSendCount = 0;

        while (remaining > 0 && idleSendCount < idleSendCountBeforeGivingUp) {
            int m = nf.send(
                    getFd(),
                    sendBuffer + offset,
                    remaining
            );
            if (m < 0) {
                throw PeerDisconnectedException.INSTANCE;
            }

            dumpBuffer('<', sendBuffer + offset, m);

            if (m > 0) {
                remaining -= m;
                offset += m;
            } else {
                idleSendCount++;
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

    private void executeInsert() {
        final TableWriter w;
        try {
            switch (transactionState) {
                case IN_TRANSACTION:
                    final InsertMethod m = typesAndInsert.getInsert().createMethod(sqlExecutionContext, this);
                    try {
                        rowCount = m.execute();
                        w = m.popWriter();
                        pendingWriters.put(w.getTableName(), w);
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
                        rowCount = m2.execute();
                        m2.commit();
                    }
                    break;
            }
            prepareCommandComplete(true);
        } catch (Throwable e) {
            if (transactionState == IN_TRANSACTION) {
                transactionState = ERROR_TRANSACTION;
            }
            throw e;
        }
    }

    private void executeTag() {
        LOG.debug().$("executing [tag=").$(queryTag).$(']').$();
        if (queryTag != null && TAG_OK != queryTag) {  //do not run this for OK tag (i.e.: create table)
            executeTag0();
        }
    }

    private void executeTag0() {
        switch (transactionState) {
            case COMMIT_TRANSACTION:
                try {
                    for (int i = 0, n = pendingWriters.size(); i < n; i++) {
                        final TableWriter m = pendingWriters.valueQuick(i);
                        m.commit();
                        Misc.free(m);
                    }
                } finally {
                    pendingWriters.clear();
                    transactionState = NO_TRANSACTION;
                }
                break;
            case ROLLING_BACK_TRANSACTION:
                try {
                    for (int i = 0, n = pendingWriters.size(); i < n; i++) {
                        final TableWriter m = pendingWriters.valueQuick(i);
                        m.rollback();
                        Misc.free(m);
                    }
                } finally {
                    pendingWriters.clear();
                    transactionState = NO_TRANSACTION;
                }
                break;
            default:
                break;
        }
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
        if (Chars.utf8Decode(lo, hi, e)) {
            return characterStore.toImmutable();
        } else {
            LOG.error().$(errorMessage).$();
            throw BadProtocolException.INSTANCE;
        }
    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parse(long address, int len, @Transient SqlCompiler compiler)
            throws PeerDisconnectedException, PeerIsSlowToReadException, BadProtocolException, SqlException {

        if (requireInitialMessage) {
            processInitialMessage(address, len);
            return;
        }

        // this is a type-prefixed message
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
                .$(']').$();
        if (msgLen < 1) {
            LOG.error().$("invalid message length [type=").$(type).$(", msgLen=").$(msgLen).$(']').$();
            throw BadProtocolException.INSTANCE;
        }

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
                    .$(']').$();
            return;
        }
        // we have enough to read entire message
        recvBufferReadOffset += msgLen + 1;
        final long msgLimit = address + msgLen + 1;
        final long msgLo = address + PREFIXED_MESSAGE_HEADER_LEN; // 8 is offset where name value pairs begin

        if (authenticationRequired) {
            doAuthentication(msgLo, msgLimit);
            return;
        }
        switch (type) {
            case 'P':
                processParse(
                        address,
                        msgLo,
                        msgLimit,
                        compiler
                );
                break;
            case 'X':
                // 'Terminate'
                throw PeerDisconnectedException.INSTANCE;
            case 'C':
                // close
                processClose(msgLo, msgLimit);
                break;
            case 'B': // bind
                processBind(msgLo, msgLimit, compiler);
                break;
            case 'E': // execute
                processExec(msgLo, msgLimit);
                break;
            case 'S': // sync
                processSyncActions();
                prepareReadyForQuery();
                prepareForNewQuery();
                // fall thru
            case 'H': // flush
                sendAndReset();
                break;
            case 'D': // describe
                processDescribe(msgLo, msgLimit, compiler);
                break;
            case 'Q':
                processQuery(msgLo, msgLimit, compiler);
                break;
            case 'd':
                System.out.println("data " + msgLen);
                // msgLen includes 4 bytes of self
                break;
            default:
                LOG.error().$("unknown message [type=").$(type).$(']').$();
                throw BadProtocolException.INSTANCE;
        }
    }

    private void parseQueryText(long lo, long hi, @Transient SqlCompiler compiler)
            throws BadProtocolException, PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        CharacterStoreEntry e = characterStore.newEntry();
        if (Chars.utf8Decode(lo, hi, e)) {
            queryText = characterStore.toImmutable();

            LOG.info().$("parse [q=").utf8(queryText).$(']').$();
            compileQuery(compiler);
            return;
        }
        LOG.error().$("invalid UTF8 bytes in parse query").$();
        throw BadProtocolException.INSTANCE;
    }

    private void prepareBindComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_BIND_COMPLETE);
        responseAsciiSink.putIntDirect(INT_BYTES_X);
    }

    private void prepareCloseComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_CLOSE_COMPLETE);
        responseAsciiSink.putIntDirect(INT_BYTES_X);
    }

    void prepareCommandComplete(boolean addRowCount) {
        if (isEmptyQuery) {
            LOG.debug().$("empty").$();
            responseAsciiSink.put(MESSAGE_TYPE_EMPTY_QUERY);
            responseAsciiSink.putIntDirect(INT_BYTES_X);
        } else {
            responseAsciiSink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
            long addr = responseAsciiSink.skip();
            if (addRowCount) {
                if (queryTag == TAG_INSERT) {
                    LOG.debug().$("insert [rowCount=").$(rowCount).$(']').$();
                    responseAsciiSink.encodeUtf8(queryTag).put(" 0 ").put(rowCount).put((char) 0);
                } else {
                    LOG.debug().$("other [rowCount=").$(rowCount).$(']').$();
                    responseAsciiSink.encodeUtf8(queryTag).put(' ').put(rowCount).put((char) 0);
                }
            } else {
                LOG.debug().$("now row count").$();
                responseAsciiSink.encodeUtf8(queryTag).put((char) 0);
            }
            responseAsciiSink.putLen(addr);
        }
    }

    private void prepareDescribePortalResponse() {
        if (typesAndSelect != null) {
            try {
                prepareRowDescription();
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                LOG.error().$("not enough space in buffer for row description [buffer=").$(sendBufferSize).I$();
                responseAsciiSink.reset();
                throw CairoException.instance(0).put("server configuration error: not enough space in send buffer for row description");
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

    private void prepareError(int position, CharSequence message) {
        responseAsciiSink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = responseAsciiSink.skip();
        responseAsciiSink.put('C');
        responseAsciiSink.encodeUtf8Z("00000");
        responseAsciiSink.put('M');
        responseAsciiSink.encodeUtf8Z(message);
        responseAsciiSink.put('S');
        responseAsciiSink.encodeUtf8Z("ERROR");
        if (position > -1) {
            responseAsciiSink.put('P').put(position + 1).put((char) 0);
        }
        responseAsciiSink.put((char) 0);
        responseAsciiSink.putLen(addr);
        LOG.error().$("error [pos=").$(position).$(", msg=`").$(message).$("`]").$();
    }

    private void prepareForNewQuery() {
        if (completed) {
            LOG.debug().$("prepare for new query").$();
            isEmptyQuery = false;
            characterStore.clear();
            bindVariableService.clear();
            currentCursor = Misc.free(currentCursor);
            typesAndInsert = null;
            typesAndSelect = null;
            rowCount = 0;
            queryTag = TAG_OK;
            queryText = null;
            wrapper = null;
            syncActions.clear();
            sendParameterDescription = false;
        }
    }

    private void prepareLoginOk() {
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(Integer.BYTES * 2); // length of this message
        responseAsciiSink.putIntDirect(0); // response code
        prepareParams(responseAsciiSink, "TimeZone", "GMT");
        prepareParams(responseAsciiSink, "application_name", "QuestDB");
        prepareParams(responseAsciiSink, "server_version", serverVersion);
        prepareParams(responseAsciiSink, "integer_datetimes", "on");
        prepareParams(responseAsciiSink, "client_encoding", "UTF8");
        prepareReadyForQuery();
    }

    private void prepareLoginResponse() {
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(Integer.BYTES * 2);
        responseAsciiSink.putNetworkInt(3);
    }

    private void prepareNoDataMessage() {
        responseAsciiSink.put(MESSAGE_TYPE_NO_DATA);
        responseAsciiSink.putIntDirect(INT_BYTES_X);
    }

    private void prepareParameterDescription() {
        responseAsciiSink.put(MESSAGE_TYPE_PARAMETER_DESCRIPTION);
        final long l = responseAsciiSink.skip();
        final int n = bindVariableService.getIndexedVariableCount();
        responseAsciiSink.putNetworkShort((short) n);
        if (n > 0) {
            for (int i = 0; i < n; i++) {
                responseAsciiSink.putIntDirect(toParamType(activeBindVariableTypes.getQuick(i)));
            }
        }
        responseAsciiSink.putLen(l);
    }

    private void prepareParseComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_PARSE_COMPLETE);
        responseAsciiSink.putIntDirect(INT_BYTES_X);
    }

    void prepareReadyForQuery() {
        responseAsciiSink.put(MESSAGE_TYPE_READY_FOR_QUERY);
        responseAsciiSink.putNetworkInt(Integer.BYTES + Byte.BYTES);
        switch (transactionState) {
            case IN_TRANSACTION:
                responseAsciiSink.put(STATUS_IN_TRANSACTION);
                break;
            case ERROR_TRANSACTION:
                responseAsciiSink.put(STATUS_IN_ERROR);
                break;
            default:
                responseAsciiSink.put(STATUS_IDLE);
                break;
        }
    }

    private void prepareRowDescription() {
        final RecordMetadata metadata = typesAndSelect.getFactory().getMetadata();
        ResponseAsciiSink sink = responseAsciiSink;
        sink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
        final long addr = sink.skip();
        final int n = activeSelectColumnTypes.size();
        sink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            final int typeFlag = activeSelectColumnTypes.getQuick(i);
            final int columnType = toColumnType(typeFlag == ColumnType.NULL ? ColumnType.STRING : typeFlag);
            sink.encodeUtf8Z(metadata.getColumnName(i));
            sink.putIntDirect(0); //tableOid ?
            sink.putNetworkShort((short) (i + 1)); //column number, starting from 1
            sink.putNetworkInt(TYPE_OIDS.get(columnType)); // type
            if (columnType < ColumnType.STRING) {
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
            sink.putNetworkShort(typeFlag == ColumnType.BINARY ? 1 : getColumnBinaryFlag(typeFlag)); // format code
        }
        sink.putLen(addr);
    }

    private void prepareSslResponse() {
        responseAsciiSink.put('N');
    }

    void prepareSuspended() {
        LOG.debug().$("suspended").$();
        responseAsciiSink.put(MESSAGE_TYPE_PORTAL_SUSPENDED);
        responseAsciiSink.putIntDirect(INT_BYTES_X);
    }

    private void processBind(long lo, long msgLimit, @Transient SqlCompiler compiler)
            throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {

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

        configureContextFromNamedStatement(statementName, compiler);
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
            } else if (parameterFormatCount == parsePhaseBindVariableCount) {
                bindParameterFormats(lo, msgLimit, parameterFormatCount, activeBindVariableTypes);
            }
        }

        // parameter value count
        lo += parameterFormatCount * Short.BYTES;
        parameterValueCount = getShort(lo, msgLimit, "could not read parameter value count");

        LOG.debug().$("binding [parameterValueCount=").$(parameterValueCount).$(", thread=").$(Thread.currentThread().getId()).$(']').$();

        //we now have all parameter counts, validate them
        validateParameterCounts(parameterFormatCount, parameterValueCount, parsePhaseBindVariableCount);

        lo += Short.BYTES;
        if (parameterValueCount > 0) {
            if (this.parsePhaseBindVariableCount == parameterValueCount) {
                lo = bindValuesUsingSetters(lo, msgLimit, parameterValueCount);
            } else {
                lo = bindValuesAsStrings(lo, msgLimit, parameterValueCount);
            }
        }

        if (typesAndSelect != null) {
            short columnFormatCodeCount = getShort(lo, msgLimit, "could not read result set column format codes");
            if (columnFormatCodeCount > 0) {

                final RecordMetadata m = typesAndSelect.getFactory().getMetadata();
                final int columnCount = m.getColumnCount();
                // apply format codes to the cursor column types
                // but check if there is message is consistent

                final long spaceNeeded = lo + (columnFormatCodeCount + 1) * Short.BYTES;
                if (spaceNeeded <= msgLimit) {
                    if (columnFormatCodeCount == columnCount) {
                        // good to go
                        for (int i = 0; i < columnCount; i++) {
                            lo += Short.BYTES;
                            final short code = getShortUnsafe(lo);
                            activeSelectColumnTypes.setQuick(i, toColumnBinaryType(code, m.getColumnType(i)));
                        }
                    } else if (columnFormatCodeCount == 1) {
                        lo += Short.BYTES;
                        final short code = getShortUnsafe(lo);
                        for (int i = 0; i < columnCount; i++) {
                            activeSelectColumnTypes.setQuick(i, toColumnBinaryType(code, m.getColumnType(i)));
                        }
                    } else {
                        LOG.error()
                                .$("could not process column format codes [fmtCount=").$(columnFormatCodeCount)
                                .$(", columnCount=").$(columnCount)
                                .$(']').$();
                        throw BadProtocolException.INSTANCE;
                    }
                } else {
                    LOG.error()
                            .$("could not process column format codes [bufSpaceNeeded=").$(spaceNeeded)
                            .$(", bufSpaceAvail=").$(msgLimit)
                            .$(']').$();
                    throw BadProtocolException.INSTANCE;
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
                final CharSequence statementName = getStatementName(lo, hi);
                if (statementName != null) {
                    final int index = namedStatementMap.keyIndex(statementName);
                    if (index < 0) {
                        namedStatementWrapperPool.push(namedStatementMap.valueAt(index));
                        namedStatementMap.removeAt(index);
                    } else {
                        LOG.error().$("invalid statement name [value=").$(statementName).$(']').$();
                        throw BadProtocolException.INSTANCE;
                    }
                }
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
                        LOG.error().$("invalid portal name [value=").$(portalName).$(']').$();
                        throw BadProtocolException.INSTANCE;
                    }
                }
                break;
            default:
                LOG.error().$("invalid type for close message [type=").$(type).$(']').$();
                throw BadProtocolException.INSTANCE;
        }
        prepareCloseComplete();
    }

    private void processDescribe(long lo, long msgLimit, @Transient SqlCompiler compiler)
            throws SqlException, BadProtocolException, PeerDisconnectedException, PeerIsSlowToReadException {

        boolean isPortal = Unsafe.getUnsafe().getByte(lo) == 'P';
        long hi = getStringLength(lo + 1, msgLimit, "bad prepared statement name length");

        CharSequence target = getPortalName(lo + 1, hi);
        LOG.debug().$("describe [name=").$(target).$(']').$();
        if (isPortal && target != null) {
            Portal p = namedPortalMap.get(target);
            if (p != null) {
                target = p.statementName;
            } else {
                LOG.error().$("invalid portal [name=").$(target).$(']').$();
                throw BadProtocolException.INSTANCE;
            }
        }

        configureContextFromNamedStatement(target, compiler);

        // initialize activeBindVariableTypes from bind variable service
        final int n = bindVariableService.getIndexedVariableCount();
        if (sendParameterDescription && n > 0 && activeBindVariableTypes.size() == 0) {
            activeBindVariableTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                activeBindVariableTypes.setQuick(i, Numbers.bswap(TYPE_OIDS.getQuick(bindVariableService.getFunction(i).getType())));
            }
        }
        if (isPortal) {
            syncActions.add(SYNC_DESCRIBE_PORTAL);
        } else {
            syncActions.add(SYNC_DESCRIBE);
        }
    }

    private void processExec(long lo, long msgLimit)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException, BadProtocolException {
        final long hi = getStringLength(lo, msgLimit, "bad portal name length");
        final CharSequence portalName = getPortalName(lo, hi);
        if (portalName != null) {
            LOG.info().$("execute portal [name=").$(portalName).$(']').$();
        }

        lo = hi + 1;
        final int maxRows = getInt(lo, msgLimit, "could not read max rows value");

        processSyncActions();
        processExecute(maxRows);
        wrapper = null;
    }

    private void processExecute(int maxRows) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        if (typesAndSelect != null) {
            LOG.debug().$("executing query").$();
            setupFactoryAndCursor();
            sendCursor(maxRows, resumeCursorExecuteRef, resumeCommandCompleteRef);
        } else if (typesAndInsert != null) {
            LOG.debug().$("executing insert").$();
            executeInsert();
        } else { //this must be a OK/SET/COMMIT/ROLLBACK or empty query
            executeTag();
            prepareCommandComplete(false);
        }
    }

    private void processInitialMessage(long address, int len) throws PeerDisconnectedException, PeerIsSlowToReadException, BadProtocolException {
        int msgLen;
        long msgLimit;// expect startup request
        if (len < Long.BYTES) {
            return;
        }

        // there is data for length
        // this is quite specific to message type :(
        msgLen = getIntUnsafe(address); // postgresql includes length bytes in length of message

        // do we have the rest of the message?
        if (msgLen > len) {
            // we have length - get the rest when ready
            return;
        }

        // enough to read login request
        recvBufferReadOffset += msgLen;

        // consume message
        // process protocol
        int protocol = getIntUnsafe(address + Integer.BYTES);
        switch (protocol) {
            case INIT_SSL_REQUEST:
                // SSLRequest
                prepareSslResponse();
                sendAndReset();
                return;
            case INIT_STARTUP_MESSAGE:
                // StartupMessage
                // extract properties
                requireInitialMessage = false;
                msgLimit = address + msgLen;
                long lo = address + Long.BYTES;
                // there is an extra byte at the end and it has to be 0
                LOG.info()
                        .$("protocol [major=").$(protocol >> 16)
                        .$(", minor=").$((short) protocol)
                        .$(']').$();

                while (lo < msgLimit - 1) {

                    final long nameLo = lo;
                    final long nameHi = getStringLength(lo, msgLimit, "malformed property name");
                    lo = nameHi + 1;
                    final long valueLo = lo;
                    final long valueHi = getStringLength(valueLo, msgLimit, "malformed property value");

                    // store user
                    dbcs.of(nameLo, nameHi);
                    if (Chars.equals(dbcs, "user")) {
                        CharacterStoreEntry e = characterStore.newEntry();
                        e.put(dbcs.of(valueLo, valueHi));
                        this.username = e.toImmutable();
                    }

                    LOG.info().$("property [name=").$(dbcs.of(nameLo, nameHi)).$(", value=").$(dbcs.of(valueLo, valueHi)).$(']').$();
                }

                characterStore.clear();

                assertTrue(this.username != null, "user is not specified");
                prepareLoginResponse();
                sendAndReset();
                break;
            case INIT_CANCEL_REQUEST:
                //todo - 1. do not disconnect
                //       2. should cancel running query only if PID and secret provided are the same as the ones provided upon logon
                //       3. send back error message (e) for the cancelled running query
                LOG.info().$("cancel request").$();
                throw PeerDisconnectedException.INSTANCE;
            default:
                LOG.error().$("unknown init message [protocol=").$(protocol).$(']').$();
                throw BadProtocolException.INSTANCE;
        }
    }

    private void processParse(long address, long lo, long msgLimit, @Transient SqlCompiler compiler)
            throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
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

        parseQueryText(lo, hi, compiler);

        //parameter type count
        lo = hi + 1;
        this.parsePhaseBindVariableCount = getShort(lo, msgLimit, "could not read parameter type count");

        if (statementName != null) {
            LOG.info().$("prepare [name=").$(statementName).$(']').$();
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
                        .$(']').$();
                throw BadProtocolException.INSTANCE;
            }

            LOG.debug().$("params [count=").$(this.parsePhaseBindVariableCount).$(']').$();
            setupBindVariables(lo + Short.BYTES, activeBindVariableTypes, this.parsePhaseBindVariableCount);
        } else if (this.parsePhaseBindVariableCount < 0) {
            LOG.error()
                    .$("invalid parameter count [parameterCount=").$(this.parsePhaseBindVariableCount)
                    .$(", offset=").$(lo - address)
                    .$(']').$();
            throw BadProtocolException.INSTANCE;
        }

        if (typesAndSelect != null) {
            buildSelectColumnTypes();
        }

        syncActions.add(SYNC_PARSE);
    }

    private void processQuery(long lo, long limit, @Transient SqlCompiler compiler)
            throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        // simple query, typically a script, which we don't yet support
        prepareForNewQuery();
        parseQueryText(lo, limit - 1, compiler);

        if (typesAndSelect != null) {
            activeSelectColumnTypes = selectColumnTypes;
            buildSelectColumnTypes();
            assert queryText != null;
            queryTag = TAG_SELECT;
            setupFactoryAndCursor();
            prepareRowDescription();
            sendCursor(0, resumeCursorQueryRef, resumeQueryCompleteRef);
        } else if (typesAndInsert != null) {
            executeInsert();
        } else {
            executeTag();
            prepareCommandComplete(false);
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

    int recv() throws PeerDisconnectedException, PeerIsSlowToWriteException, BadProtocolException {
        final int remaining = (int) (recvBufferSize - recvBufferWriteOffset);

        assertTrue(remaining > 0, "undersized receive buffer or someone is abusing protocol");

        int n = doReceive(remaining);
        LOG.debug().$("recv [n=").$(n).$(']').$();
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n == 0) {
            int retriesRemaining = idleRecvCountBeforeGivingUp;
            while (retriesRemaining > 0) {
                n = doReceive(remaining);
                if (n == 0) {
                    retriesRemaining--;
                    continue;
                }

                if (n < 0) {
                    LOG.info().$("disconnect [code=").$(n).$(']').$();
                    throw PeerDisconnectedException.INSTANCE;
                }

                break;
            }

            if (retriesRemaining == 0) {
                throw PeerIsSlowToWriteException.INSTANCE;
            }
        }
        recvBufferWriteOffset += n;
        return n;
    }

    private void reportError(int position, CharSequence flyweightMessage) throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareError(position, flyweightMessage);
        sendReadyForNewQuery();
        clearRecvBuffer();
    }

    private void resumeCommandComplete() {
        prepareCommandComplete(true);
    }

    private void resumeCursorExecute() throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        final Record record = currentCursor.getRecord();
        final int columnCount = currentFactory.getMetadata().getColumnCount();
        responseAsciiSink.bookmark();
        appendSingleRecord(record, columnCount);
        sendCursor0(record, columnCount, resumeCommandCompleteRef);
    }

    private void resumeCursorQuery() throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        resumeCursorQuery0();
        sendReadyForNewQuery();
    }

    private void resumeCursorQuery0() throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        final Record record = currentCursor.getRecord();
        final int columnCount = currentFactory.getMetadata().getColumnCount();
        responseAsciiSink.bookmark();
        appendSingleRecord(record, columnCount);
        sendCursor0(record, columnCount, resumeQueryCompleteRef);
    }

    private void resumeQueryComplete() throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareCommandComplete(true);
        sendReadyForNewQuery();
    }

    private void sendAndReset() throws PeerDisconnectedException, PeerIsSlowToReadException {
        doSend(0, (int) (sendBufferPtr - sendBuffer));
        responseAsciiSink.reset();
    }

    private void sendCopyInResponse(CairoEngine engine, TextLoader textLoader) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (
                TableUtils.TABLE_EXISTS == engine.getStatus(
                        sqlExecutionContext.getCairoSecurityContext(),
                        path,
                        textLoader.getTableName()
                )) {
            responseAsciiSink.put(MESSAGE_TYPE_COPY_IN_RESPONSE);
            long addr = responseAsciiSink.skip();
            responseAsciiSink.put((byte) 0); // TEXT (1=BINARY, which we do not support yet)
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), textLoader.getTableName(), WRITER_LOCK_REASON)) {
                RecordMetadata metadata = writer.getMetadata();
                responseAsciiSink.putNetworkShort((short) metadata.getColumnCount());
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    responseAsciiSink.putNetworkShort((short) TYPE_OIDS.get(metadata.getColumnType(i)));
                }
            }
            responseAsciiSink.putLen(addr);
        } else {
            final SqlException e = SqlException.$(0, "table '").put(textLoader.getTableName()).put("' does not exist");
            prepareError(e.getPosition(), e.getFlyweightMessage());
            prepareReadyForQuery();
        }
        sendAndReset();
    }

    private void sendCursor(
            int maxRows,
            PGResumeProcessor cursorResumeProcessor,
            PGResumeProcessor commandCompleteResumeProcessor
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        // the assumption for now is that any record will fit into response buffer. This of course precludes us from
        // streaming large BLOBs, but, and its a big one, PostgreSQL protocol for DataRow does not allow for
        // streaming anyway. On top of that Java PostgreSQL driver downloads data row fully. This simplifies our
        // approach for general queries. For streaming protocol we will code something else. PostgreSQL Java driver is
        // slow anyway.

        rowCount = 0;
        final Record record = currentCursor.getRecord();
        final RecordMetadata metadata = currentFactory.getMetadata();
        final int columnCount = metadata.getColumnCount();
        final long cursorRowCount = currentCursor.size();
        this.maxRows = maxRows > 0 ? Long.min(maxRows, cursorRowCount) : Long.MAX_VALUE;
        this.resumeProcessor = cursorResumeProcessor;
        sendCursor0(record, columnCount, commandCompleteResumeProcessor);
    }

    private void sendCursor0(Record record, int columnCount, PGResumeProcessor commandCompleteResumeProcessor)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        while (currentCursor.hasNext()) {
            // create checkpoint to which we can undo the buffer in case
            // current DataRow will does not fit fully.
            responseAsciiSink.bookmark();
            try {
                try {
                    appendRecord(record, columnCount);
                    if (rowCount >= maxRows) break;
                } catch (NoSpaceLeftInResponseBufferException e) {
                    responseAsciiSink.resetToBookmark();
                    sendAndReset();
                    appendSingleRecord(record, columnCount);
                }
            } catch (SqlException e) {
                responseAsciiSink.resetToBookmark();
                throw e;
            }
        }

        completed = maxRows <= 0 || rowCount < maxRows;
        if (completed) {
            resumeProcessor = null;
            currentCursor = Misc.free(currentCursor);
            // do not free factory, it will be cached
            currentFactory = null;
            // we we resumed the cursor send the typeAndSelect will be null
            // we do not want to overwrite cache entries and potentially
            // leak memory
            if (typesAndSelect != null) {
                typesAndSelectCache.put(queryText, typesAndSelect);
                // clear selectAndTypes so that context doesn't accidentally
                // free the factory when context finishes abnormally
                this.typesAndSelect = null;
            }
            // at this point buffer can contain unsent data
            // and it may not have enough space for the command
            if (sendBufferLimit - sendBufferPtr < PROTOCOL_TAIL_COMMAND_LENGTH) {
                resumeProcessor = commandCompleteResumeProcessor;
                sendAndReset();
            }
            prepareCommandComplete(true);
        } else {
            prepareSuspended();
        }
    }

    private void sendReadyForNewQuery() throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareReadyForQuery();
        sendAndReset();
    }

    private void setupFactoryAndCursor() {
        if (currentCursor == null) {
            currentFactory = typesAndSelect.getFactory();
            try {
                currentCursor = currentFactory.getCursor(sqlExecutionContext);
                // cache random if it was replaced
                this.rnd = sqlExecutionContext.getRandom();
            } catch (Throwable e) {
                currentFactory = Misc.free(currentFactory);
                throw e;
            }
        }
    }

    private void setupVariableSettersFromWrapper(
            @Transient NamedStatementWrapper wrapper,
            @Nullable @Transient SqlCompiler compiler
    ) throws SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        queryText = wrapper.queryText;
        LOG.debug().$("wrapper query [q=`").$(wrapper.queryText).$("`]").$();
        this.activeBindVariableTypes = wrapper.bindVariableTypes;
        this.parsePhaseBindVariableCount = wrapper.bindVariableTypes.size();
        this.activeSelectColumnTypes = wrapper.selectColumnTypes;
        if (compileQuery(compiler) && typesAndSelect != null) {
            buildSelectColumnTypes();
        }
    }

    private void shiftReceiveBuffer(long readOffsetBeforeParse) {
        final long len = recvBufferWriteOffset - readOffsetBeforeParse;
        LOG.debug()
                .$("shift [offset=").$(readOffsetBeforeParse)
                .$(", len=").$(len)
                .$(']').$();

        Vect.memcpy(
                recvBuffer + readOffsetBeforeParse,
                recvBuffer,
                len
        );
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

    @FunctionalInterface
    private interface PGResumeProcessor {
        void resume() throws PeerIsSlowToReadException, SqlException, PeerDisconnectedException;
    }

    public static class Portal implements Mutable {
        public CharSequence statementName = null;

        @Override
        public void clear() {
            statementName = null;
        }
    }

    public static class NamedStatementWrapper implements Mutable {

        public final IntList bindVariableTypes = new IntList();
        public final IntList selectColumnTypes = new IntList();
        public CharSequence queryText = null;

        @Override
        public void clear() {
            queryText = null;
            bindVariableTypes.clear();
            selectColumnTypes.clear();
        }
    }

    class ResponseAsciiSink extends AbstractCharSink {

        private long bookmarkPtr = -1;

        public void bookmark() {
            this.bookmarkPtr = sendBufferPtr;
        }

        public void bump(int size) {
            sendBufferPtr += size;
        }

        @Override
        public CharSink put(CharSequence cs) {
            // this method is only called by date format utility to print timezone name
            final int len;
            if (cs != null && (len = cs.length()) > 0) {
                ensureCapacity(len);
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(sendBufferPtr + i, (byte) cs.charAt(i));
                }
                sendBufferPtr += len;
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) c);
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            ensureCapacity(len);
            Chars.asciiCopyTo(chars, start, len, sendBufferPtr);
            sendBufferPtr += len;
            return this;
        }

        public CharSink put(byte b) {
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

        void encodeUtf8Z(CharSequence value) {
            encodeUtf8(value);
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
        }

        private void ensureCapacity(int size) {
            if (sendBufferPtr + size < sendBufferLimit) {
                return;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
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
