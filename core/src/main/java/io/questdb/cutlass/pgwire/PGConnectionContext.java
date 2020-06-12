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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.microtime.TimestampLocale;
import io.questdb.std.str.*;
import io.questdb.std.time.DateLocale;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cutlass.pgwire.PGJobContext.*;
import static io.questdb.std.time.DateFormatUtils.PG_DATE_TIME_Z_FORMAT;
import static io.questdb.std.time.DateFormatUtils.PG_DATE_Z_FORMAT;

public class PGConnectionContext implements IOContext, Mutable {
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final int INIT_SSL_REQUEST = 80877103;
    private static final int INIT_STARTUP_MESSAGE = 196608;
    private static final int INIT_CANCEL_REQUEST = 80877102;
    private static final int TAIL_NONE = 0;
    private static final int TAIL_SUCCESS = 1;
    private static final int TAIL_ERROR = 2;
    private static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    private static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private final static Log LOG = LogFactory.getLog(PGConnectionContext.class);
    private static final IntList typeOids = new IntList();
    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    private static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    private static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    private static final byte MESSAGE_TYPE_COPY_IN_RESPONSE = 'G';
    private final long recvBuffer;
    private final long sendBuffer;
    private final int recvBufferSize;
    private final CharacterStore connectionCharacterStore;
    private final CharacterStore queryCharacterStore;
    private final BindVariableService bindVariableService = new BindVariableService();
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
    private final BindVariableSetter doubleSetter = this::setDoubleBindVariable;
    private final BindVariableSetter doubleTxtSetter = this::setDoubleTextBindVariable;
    private final BindVariableSetter intSetter = this::setIntBindVariable;
    private final BindVariableSetter intTxtSetter = this::setIntTextBindVariable;
    private final BindVariableSetter longSetter = this::setLongBindVariable;
    private final BindVariableSetter longTxtSetter = this::setLongTextBindVariable;
    private final BindVariableSetter floatSetter = this::setFloatBindVariable;
    private final BindVariableSetter floatTxtSetter = this::setFloatTextBindVariable;
    private final BindVariableSetter byteSetter = this::setByteBindVariable;
    private final BindVariableSetter byteTxtSetter = this::setByteTextBindVariable;
    private final BindVariableSetter booleanSetter = this::setBooleanBindVariable;
    private final BindVariableSetter strSetter = this::setStrBindVariable;
    private final BindVariableSetter noopSetter = this::setNoopBindVariable;
    private final ObjList<ColumnAppender> columnAppenders = new ObjList<>();
    private final DateLocale dateLocale;
    private final BindVariableSetter dateSetter = this::setDateBindVariable;
    private final TimestampLocale timestampLocale;
    private int sendCurrentCursorTail = TAIL_NONE;
    private long sendBufferPtr;
    private boolean requireInitalMessage = false;
    private long recvBufferWriteOffset = 0;
    private long recvBufferReadOffset = 0;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private RecordCursor currentCursor = null;
    private RecordCursorFactory currentFactory = null;
    private InsertStatement currentInsertStatement = null;
    private long fd;
    private CharSequence queryText;
    private CharSequence username;
    private boolean authenticationRequired = true;
    private long transientCopyBuffer = 0;
    private IODispatcher<PGConnectionContext> dispatcher;
    private Rnd rnd;

    public PGConnectionContext(
            CairoEngine engine,
            PGWireConfiguration configuration,
            @Nullable MessageBus messageBus,
            int workerCount
    ) {
        this.nf = configuration.getNetworkFacade();
        this.recvBufferSize = Numbers.ceilPow2(configuration.getRecvBufferSize());
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
        this.sendBufferPtr = sendBuffer;
        this.sendBufferLimit = sendBuffer + sendBufferSize;
        this.queryCharacterStore = new CharacterStore(
                configuration.getCharacterStoreCapacity(),
                configuration.getCharacterStorePoolCapacity()
        );
        this.connectionCharacterStore = new CharacterStore(256, 2);
        this.maxBlobSizeOnQuery = configuration.getMaxBlobSizeOnQuery();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.idleSendCountBeforeGivingUp = configuration.getIdleSendCountBeforeGivingUp();
        this.idleRecvCountBeforeGivingUp = configuration.getIdleRecvCountBeforeGivingUp();
        this.serverVersion = configuration.getServerVersion();
        this.authenticator = new PGBasicAuthenticator(configuration.getDefaultUsername(), configuration.getDefaultPassword());
        this.dateLocale = configuration.getDefaultDateLocale();
        this.timestampLocale = configuration.getDefaultTimestampLocale();
        this.sqlExecutionContext = new SqlExecutionContextImpl(messageBus, workerCount, engine);
        populateAppender();
    }

    public static int getInt(long address) {
        int b = Unsafe.getUnsafe().getByte(address) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 1) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 2) & 0xff;
        return (b << 8) | Unsafe.getUnsafe().getByte(address + 3) & 0xff;
    }

    public static long getLong(long address) {
        long b = Unsafe.getUnsafe().getByte(address) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 1) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 2) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 3) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 4) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 5) & 0xff;
        b = (b << 8) | Unsafe.getUnsafe().getByte(address + 6) & 0xff;
        return (b << 8) | Unsafe.getUnsafe().getByte(address + 7) & 0xff;
    }

    public static short getShort(long address) {
        int b = Unsafe.getUnsafe().getByte(address) & 0xff;
        return (short) ((b << 8) | Unsafe.getUnsafe().getByte(address + 1) & 0xff);
    }

    public static long getStringLength(long x, long limit) {
        return Unsafe.getUnsafe().getByte(x) == 0 ? x : getStringLengthTedious(x, limit);
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
        Unsafe.getUnsafe().putByte(address, (byte) (value >>> 24));
        Unsafe.getUnsafe().putByte(address + 1, (byte) (value >>> 16));
        Unsafe.getUnsafe().putByte(address + 2, (byte) (value >>> 8));
        Unsafe.getUnsafe().putByte(address + 3, (byte) (value));
    }

    public static void putShort(long address, short value) {
        Unsafe.getUnsafe().putByte(address, (byte) (value >>> 8));
        Unsafe.getUnsafe().putByte(address + 1, (byte) (value));
    }

    @Override
    public void clear() {
        sendCurrentCursorTail = TAIL_NONE;
        sendBufferPtr = sendBuffer;
        requireInitalMessage = true;
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
        bufferRemainingOffset = 0;
        bufferRemainingSize = 0;
        responseAsciiSink.reset();
        prepareForNewQuery();
        // todo: test that both of these are cleared (unit test)
        authenticationRequired = true;
        username = null;
    }

    @Override
    public void close() {
        clear();
        this.fd = -1;
        sqlExecutionContext.setRequestFd(-1);
        Unsafe.free(sendBuffer, sendBufferSize);
        Unsafe.free(recvBuffer, recvBufferSize);
        Misc.free(path);
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

    public void handleClientOperation(
            @Transient SqlCompiler compiler,
            @Transient AssociativeCache<Object> factoryCache,
            @Transient ObjList<BindVariableSetter> binsVariableSetters
    ) throws PeerDisconnectedException,
            PeerIsSlowToReadException,
            PeerIsSlowToWriteException,
            BadProtocolException {

        if (bufferRemainingSize > 0) {
            doSend(bufferRemainingOffset, bufferRemainingSize);
        }

        sendExecuteTail();

        // If we have empty buffer we need to try to read something from socket
        // however the opposite  is a little tricky. If buffer is non-empty
        // we still may need to read from socket if contents of this buffer
        // is incomplete and cannot be parsed
        if (recvBufferReadOffset == recvBufferWriteOffset) {
            recv();
        }

        try {
            long readOffsetBeforeParse = recvBufferReadOffset;

            // Parse will update the value of recvBufferOffset upon completion of
            // logical block. We cannot count on return value because 'parse' may try to
            // respond to client and fail with exception. When it does fail we would have
            // to retry 'send' but not parse the same input again
            parse(
                    recvBuffer + recvBufferReadOffset,
                    (int) (recvBufferWriteOffset - recvBufferReadOffset),
                    compiler,
                    factoryCache,
                    binsVariableSetters
            );

            // nothing changed?
            if (readOffsetBeforeParse == recvBufferReadOffset) {
                // how come we have something in buffer and parse didn't do anything?
                if (readOffsetBeforeParse < recvBufferWriteOffset) {
                    // may be content was incomplete?
                    recv();
                    // still nothing? oh well
                    if (readOffsetBeforeParse == recvBufferReadOffset) {
                        return;
                    }
                    // at this point we have some contact and parse did do something
                } else {
                    return;
                }
            }

            // we do not pre-compute length because 'parse' will mutate 'recvBufferReadOffset'
            if (recvBufferWriteOffset - recvBufferReadOffset > 0) {
                // did we not parse input fully?
                do {
                    readOffsetBeforeParse = recvBufferReadOffset;
                    parse(
                            recvBuffer + recvBufferReadOffset,
                            (int) (recvBufferWriteOffset - recvBufferReadOffset),
                            compiler,
                            factoryCache,
                            binsVariableSetters
                    );
                    // nothing changed?
                    if (readOffsetBeforeParse == recvBufferReadOffset) {
                        // shift to start
                        Unsafe.getUnsafe().copyMemory(
                                recvBuffer + readOffsetBeforeParse,
                                recvBuffer,
                                recvBufferWriteOffset - readOffsetBeforeParse);
                        recvBufferWriteOffset = recvBufferWriteOffset - readOffsetBeforeParse;
                        recvBufferReadOffset = 0;
                        // read more
                        return;
                    }
                } while (recvBufferReadOffset < recvBufferWriteOffset);
            }
            clearRecvBuffer();
        } catch (SqlException e) {
            sendExecuteTail(TAIL_ERROR);
            clearRecvBuffer();
        }
    }

    public PGConnectionContext of(long clientFd, IODispatcher<PGConnectionContext> dispatcher) {
        this.fd = clientFd;
        sqlExecutionContext.setRequestFd(clientFd);
        this.dispatcher = dispatcher;
        clear();
        return this;
    }

    @SuppressWarnings("unused")
    public void setBooleanBindVariable(int index, long address, int valueLen) throws SqlException {
        if (valueLen != 4 && valueLen != 5) {
            throw SqlException.$(0, "bad value for BOOLEAN parameter [index=").put(index).put(", valueLen=").put(valueLen).put(']');
        }
        bindVariableService.setBoolean(index, valueLen == 4);
    }

    public void setByteBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        ensureValueLength(Short.BYTES, valueLen);
        bindVariableService.setByte(index, (byte) getShort(address));
    }

    public void setByteTextBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        try {
            bindVariableService.setByte(index, (byte) Numbers.parseInt(dbcs.of(address, address + valueLen)));
        } catch (NumericException e) {
            LOG.error().$("bad byte variable value [index=").$(index).$(", value=`").$(dbcs).$("`").$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setDateBindVariable(int index, long address, int valueLen) throws SqlException {
        dbcs.of(address, address + valueLen);
        try {
            bindVariableService.setDate(index, PG_DATE_Z_FORMAT.parse(dbcs, dateLocale));
        } catch (NumericException ex) {
            try {
                bindVariableService.setDate(index, PG_DATE_TIME_Z_FORMAT.parse(dbcs, dateLocale));
            } catch (NumericException exc) {
                throw SqlException.$(0, "bad parameter value [index=").put(index).put(", value=").put(dbcs).put(']');
            }
        }
    }

    public void setDoubleBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        ensureValueLength(Double.BYTES, valueLen);
        bindVariableService.setDouble(index, Double.longBitsToDouble(getLong(address)));
    }

    public void setDoubleTextBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        try {
            bindVariableService.setDouble(index, Numbers.parseDouble(dbcs.of(address, address + valueLen)));
        } catch (NumericException e) {
            LOG.error().$("bad double variable value [index=").$(index).$(", value=`").$(dbcs).$("`]").$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setFloatBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        ensureValueLength(Float.BYTES, valueLen);
        bindVariableService.setFloat(index, Float.intBitsToFloat(getInt(address)));
    }

    public void setFloatTextBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        try {
            bindVariableService.setFloat(index, Numbers.parseFloat(dbcs.of(address, address + valueLen)));
        } catch (NumericException e) {
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setIntBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        ensureValueLength(Integer.BYTES, valueLen);
        bindVariableService.setInt(index, getInt(address));
    }

    public void setIntTextBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        try {
            bindVariableService.setInt(index, Numbers.parseInt(dbcs.of(address, address + valueLen)));
        } catch (NumericException e) {
            LOG.error().$("bad int variable value [index=").$(index).$(", value=`").$(dbcs).$("`]").$();
            throw BadProtocolException.INSTANCE;
        }
    }

    public void setLongBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        ensureValueLength(Long.BYTES, valueLen);
        bindVariableService.setLong(index, getLong(address));
    }

    public void setLongTextBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        try {
            bindVariableService.setLong(index, Numbers.parseLong(dbcs.of(address, address + valueLen)));
        } catch (NumericException e) {
            LOG.error().$("bad long variable value [index=").$(index).$(", value=`").$(dbcs).$("`]").$();
            throw BadProtocolException.INSTANCE;
        }
    }

    @SuppressWarnings("unused")
    public void setNoopBindVariable(int index, long address, int valueLen) {
    }

    public void setStrBindVariable(int index, long address, int valueLen) throws BadProtocolException {
        CharacterStoreEntry e = queryCharacterStore.newEntry();
        if (Chars.utf8Decode(address, address + valueLen, e)) {
            bindVariableService.setStr(index, queryCharacterStore.toImmutable());
        } else {
            LOG.error().$("invalid UTF8 bytes [index=").$(index).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void ensureValueLength(int required, int valueLen) throws BadProtocolException {
        if (required != valueLen) {
            LOG.error().$("bad parameter value length [required=").$(required).$(", actual=").$(valueLen).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void ensureData(long lo, int required, long msgLimit, int j) throws BadProtocolException {
        if (lo + required > msgLimit) {
            LOG.info().$("not enough bytes for parameter [index=").$(j).$(']').$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private static void prepareParams(PGConnectionContext.ResponseAsciiSink sink, String name, String value) {
        sink.put(MESSAGE_TYPE_PARAMETER_STATUS);
        final long addr = sink.skip();
        sink.encodeUtf8Z(name);
        sink.encodeUtf8Z(value);
        sink.putLen(addr);
    }

    static void prepareReadyForQuery(ResponseAsciiSink responseAsciiSink) {
        responseAsciiSink.put(MESSAGE_TYPE_READY_FOR_QUERY);
        responseAsciiSink.putNetworkInt(Integer.BYTES + Byte.BYTES);
        responseAsciiSink.put('I');
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

    private void appendDateColumn(Record record, int columnIndex) {
        final long longValue = record.getDate(columnIndex);
        if (longValue == Numbers.LONG_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            PG_DATE_TIME_Z_FORMAT.format(longValue, null, null, responseAsciiSink);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendDoubleColumn(Record record, int columnIndex) {
        final double doubleValue = record.getDouble(columnIndex);
        if (Double.isNaN(doubleValue)) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(doubleValue, Numbers.MAX_SCALE);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendFloatColumn(Record record, int columnIndex) {
        final float floatValue = record.getFloat(columnIndex);
        if (Float.isNaN(floatValue)) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(floatValue, 3);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendIntCol(Record record, int i) {
        final int intValue = record.getInt(i);
        if (intValue == Numbers.INT_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(intValue);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendLongColumn(Record record, int columnIndex) {
        final long longValue = record.getLong(columnIndex);
        if (longValue == Numbers.LONG_NaN) {
            responseAsciiSink.setNullValue();
        } else {
            final long a = responseAsciiSink.skip();
            responseAsciiSink.put(longValue);
            responseAsciiSink.putLenEx(a);
        }
    }

    private void appendRecord(
            Record record,
            RecordMetadata metadata,
            int columnCount
    ) throws SqlException {
        responseAsciiSink.put(MESSAGE_TYPE_DATA_ROW); // data
        final long offset = responseAsciiSink.skip();
        responseAsciiSink.putNetworkShort((short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnAppenders.getQuick(metadata.getColumnType(i)).append(record, i);
        }
        responseAsciiSink.putLen(offset);
    }

    private void appendShortColumn(Record record, int columnIndex) {
        final long a = responseAsciiSink.skip();
        responseAsciiSink.put(record.getShort(columnIndex));
        responseAsciiSink.putLenEx(a);
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

    private void bindVariables(
            long lo,
            long msgLimit,
            short parameterCount,
            @Transient ObjList<BindVariableSetter> bindVariableSetters
    ) throws BadProtocolException, SqlException {
        // do we have enough data for all codes?
        if (lo + Short.BYTES * parameterCount > msgLimit) {
            LOG.error().$("invalid format code count [value=").$(parameterCount).$(']').$();
            throw BadProtocolException.INSTANCE;
        }

        for (int j = 0; j < parameterCount; j++) {
            final short code = getShort(lo + j * Short.BYTES);
            if (code == 1) {
                continue;
            }

            if (code == 0) {
                bindVariableSetters.setQuick(j * 2, bindVariableSetters.getQuick(j * 2 + 1));
            } else {
                LOG.error().$("unsupported code [index=").$(j).$(", code=").$(code).$(']').$();
                throw BadProtocolException.INSTANCE;
            }
        }

        lo += parameterCount * Short.BYTES;

        checkNotTrue(lo + Short.BYTES > msgLimit, "could not read parameter value count");
        parameterCount = getShort(lo);

        if (parameterCount != bindVariableService.getIndexedVariableCount()) {
            LOG.error()
                    .$("parameter count from parse message does not match parameter value count [valueCount=").$(parameterCount)
                    .$(", typeCount=").$(bindVariableService.getIndexedVariableCount())
                    .$(']').$();
            throw BadProtocolException.INSTANCE;
        }

        lo += Short.BYTES;

        for (int j = 0; j < parameterCount; j++) {
            if (lo + Integer.BYTES > msgLimit) {
                LOG.error().$("could not read parameter value length [index=").$(j).$(']').$();
                throw BadProtocolException.INSTANCE;
            }

            int valueLen = getInt(lo);
            lo += Integer.BYTES;
            if (valueLen == -1) {
                // this is null we have already defaulted parameters to
                continue;
            }

            if (lo + valueLen > msgLimit) {
                LOG.error()
                        .$("value length is outside of buffer [parameterIndex=").$(j)
                        .$(", valueLen=").$(valueLen)
                        .$(", messageRemaining=").$(msgLimit - lo)
                        .$(']').$();
                throw BadProtocolException.INSTANCE;
            }
            ensureData(lo, valueLen, msgLimit, j);
            bindVariableSetters.getQuick(j * 2).set(j, lo, valueLen);
            lo += valueLen;
        }
    }

    private void checkNotTrue(boolean check, String message) throws BadProtocolException {
        if (check) {
            // we did not find 0 within message limit
            LOG.error().$(message).$();
            throw BadProtocolException.INSTANCE;
        }
    }

    void clearRecvBuffer() {
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
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
            doSendWithRetries(n, size - n);
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

    private void executeInsert() throws PeerDisconnectedException, PeerIsSlowToReadException {
        try (final InsertMethod m = currentInsertStatement.createMethod(sqlExecutionContext)) {
            m.execute();
            m.commit();
            sendExecuteTail(TAIL_SUCCESS);
        } catch (CairoException e) {
            responseAsciiSink.put(MESSAGE_TYPE_ERROR_RESPONSE);
            final long addr = responseAsciiSink.skip();
            responseAsciiSink.put('M');
            responseAsciiSink.encodeUtf8Z((e).getFlyweightMessage());
            responseAsciiSink.put('S');
            responseAsciiSink.encodeUtf8Z("ERROR");
            responseAsciiSink.put((char) 0);
            responseAsciiSink.putLen(addr);
            sendExecuteTail(TAIL_ERROR);
        } finally {
            currentInsertStatement = null;
        }
    }

    private void executeSelect(
            @NotNull RecordCursorFactory factory
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        this.currentFactory = factory;
        currentCursor = factory.getCursor(sqlExecutionContext);
        prepareRowDescription();
        sendCursor();
    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parse(
            long address,
            int len,
            @Transient SqlCompiler compiler,
            @Transient AssociativeCache<Object> factoryCache,
            @Transient ObjList<BindVariableSetter> bindVariableSetters
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, BadProtocolException, SqlException {
        final long limit = address + len;
        final int remaining = (int) (limit - address);

        if (requireInitalMessage) {
            processInitialMessage(address, remaining);
            return;
        }

        // this is a type-prefixed message
        // we will wait until we receive the entire header

        if (remaining < PREFIXED_MESSAGE_HEADER_LEN) {
            // we need to be able to read header and length
            return;
        }

        final byte type = Unsafe.getUnsafe().getByte(address);
        LOG.debug().$("received msg [type=").$((char) type).$(']').$();
        final int msgLen = getInt(address + 1);
        if (msgLen < 1) {
            LOG.error().$("invalid message length [type=").$(type).$(", msgLen=").$(msgLen).$(']').$();
            throw BadProtocolException.INSTANCE;
        }

        // msgLen does not take into account type byte
        if (msgLen > remaining - 1) {
            // When this happens we need to shift our receive buffer left
            // to fit this message. Outer function will do that if we
            // just exit.
            return;
        }
        // we have enough to read entire message
        recvBufferReadOffset += msgLen + 1;
        final long msgLimit = address + msgLen + 1;
        long lo = address + PREFIXED_MESSAGE_HEADER_LEN; // 8 is offset where name value pairs begin

        if (authenticationRequired) {
            CairoSecurityContext cairoSecurityContext;
            try {
                cairoSecurityContext = authenticator.authenticate(username, lo, msgLimit);
            } catch (SqlException e) {
                prepareError(e);
                send();
                return;
            }

            if (cairoSecurityContext != null) {
                sqlExecutionContext.with(cairoSecurityContext, bindVariableService, rnd);
                authenticationRequired = false;
                prepareLoginOk(responseAsciiSink);
                send();
            }
            return;
        }
        switch (type) {
            case 'P':
                processParse(address, lo, msgLimit, compiler, factoryCache, bindVariableSetters);
                break;
            case 'X':
                // 'Terminate'
                throw PeerDisconnectedException.INSTANCE;
            case 'C':
                // close
                processClose();
                break;
            case 'B': // bind
                processBind(bindVariableSetters, msgLimit, lo);
                break;
            case 'E': // execute
                processExecute();
                break;
            case 'S': // sync?
                break;
            case 'D': // describe?
                processDescribe();
                break;
            case 'Q':
                processQuery(lo, limit, compiler, factoryCache);
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

    private void parseQueryText(long lo, long hi) throws BadProtocolException {
        CharacterStoreEntry e = queryCharacterStore.newEntry();
        if (Chars.utf8Decode(lo, hi, e)) {
            queryText = queryCharacterStore.toImmutable();
            LOG.info().$("parse [q=").utf8(queryText).$(']').$();
        } else {
            LOG.error().$("invalid UTF8 bytes in parse query").$();
            throw BadProtocolException.INSTANCE;
        }
    }

    private void populateAppender() {
        columnAppenders.extendAndSet(ColumnType.INT, this::appendIntCol);
        columnAppenders.extendAndSet(ColumnType.STRING, this::appendStrColumn);
        columnAppenders.extendAndSet(ColumnType.SYMBOL, this::appendSymbolColumn);
        columnAppenders.extendAndSet(ColumnType.LONG, this::appendLongColumn);
        columnAppenders.extendAndSet(ColumnType.SHORT, this::appendShortColumn);
        columnAppenders.extendAndSet(ColumnType.DOUBLE, this::appendDoubleColumn);
        columnAppenders.extendAndSet(ColumnType.FLOAT, this::appendFloatColumn);
        columnAppenders.extendAndSet(ColumnType.TIMESTAMP, this::appendTimestampColumn);
        columnAppenders.extendAndSet(ColumnType.DATE, this::appendDateColumn);
        columnAppenders.extendAndSet(ColumnType.BOOLEAN, this::appendBooleanColumn);
        columnAppenders.extendAndSet(ColumnType.BYTE, this::appendByteColumn);
        columnAppenders.extendAndSet(ColumnType.BINARY, this::appendBinColumn);
    }

    void prepareCommandComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
        long addr = responseAsciiSink.skip();
        responseAsciiSink.encodeUtf8(queryText).put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareError(SqlException e) {
        responseAsciiSink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = responseAsciiSink.skip();
        responseAsciiSink.put('M');
        responseAsciiSink.encodeUtf8Z(e.getFlyweightMessage());
        responseAsciiSink.put('S');
        responseAsciiSink.encodeUtf8Z("ERROR");
        if (e.getPosition() > -1) {
            responseAsciiSink.put('P').put(e.getPosition() + 1).put((char) 0);
        }
        responseAsciiSink.put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareForNewQuery() {
        queryCharacterStore.clear();
        bindVariableService.clear();
        currentCursor = Misc.free(currentCursor);
        currentFactory = null;
        currentInsertStatement = null;
    }

    private void prepareLoginOk(ResponseAsciiSink sink) {
        sink.reset();
        sink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        sink.putNetworkInt(Integer.BYTES * 2); // length of this message
        sink.putNetworkInt(0); // response code
        prepareParams(sink, "TimeZone", "GMT");
        prepareParams(sink, "application_name", "QuestDB");
        prepareParams(sink, "server_version", serverVersion);
        prepareParams(sink, "integer_datetimes", "on");
        prepareReadyForQuery(sink);
    }

    private void prepareParseComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_PARSE_COMPLETE);
        responseAsciiSink.putNetworkInt(Integer.BYTES);
    }

    private void prepareRowDescription() {
        final RecordMetadata metadata = currentFactory.getMetadata();
        ResponseAsciiSink sink = responseAsciiSink;
        sink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
        final long addr = sink.skip();
        final int n = metadata.getColumnCount();
        sink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            sink.encodeUtf8Z(metadata.getColumnName(i));
            sink.putNetworkInt(0);
            sink.putNetworkShort((short) 0);
            sink.putNetworkInt(typeOids.get(columnType)); // type
            sink.putNetworkShort((short) 0); // type size?
            sink.putNetworkInt(0); // type mod?
            // this is special behaviour for binary fields to prevent binary data being hex encoded on the wire
            sink.putNetworkShort((short) (columnType == ColumnType.BINARY ? 1 : 0)); // format code
        }
        sink.putLen(addr);
    }

    private void processBind(@Transient ObjList<BindVariableSetter> bindVariableSetters, long msgLimit, long lo) throws BadProtocolException, SqlException {
        long hi;
        short parameterCount;
        hi = getStringLength(lo, msgLimit);
        checkNotTrue(hi == -1, "bad portal name length [msgType='B']");

        lo = hi + 1;
        hi = getStringLength(lo, msgLimit);
        checkNotTrue(hi == -1, "bad prepared statement name length [msgType='B']");

        lo = hi + 1;
        checkNotTrue(lo + Short.BYTES > msgLimit, "could not read parameter format code count");

        parameterCount = getShort(lo);
        if (parameterCount != bindVariableService.getIndexedVariableCount()) {
            LOG.error()
                    .$("parameter count from parse message does not match format code count [fmtCodeCount=").$(parameterCount)
                    .$(", typeCount=").$(bindVariableService.getIndexedVariableCount())
                    .$(']').$();
            throw BadProtocolException.INSTANCE;
        }
        if (parameterCount > 0) {
            lo += Short.BYTES;
            bindVariables(lo, msgLimit, parameterCount, bindVariableSetters);
        }
    }

    private void processClose() throws PeerDisconnectedException, PeerIsSlowToReadException {
        prepareForNewQuery();
        sink().put('3'); // close complete
        sink().putNetworkInt(Integer.BYTES);
        send();
    }

    private void processDescribe() throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (currentFactory != null) {
            prepareRowDescription();
            send();
            LOG.info().$("described").$();
        }
    }

    private void processExecute() throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (currentFactory != null) {
            LOG.info().$("executing query").$();
            currentCursor = currentFactory.getCursor(sqlExecutionContext);
            // cache random if it was replaced
            this.rnd = sqlExecutionContext.getRandom();
            sendCursor();
        } else if (currentInsertStatement != null) {
            executeInsert();
        }
    }

    private void processInitialMessage(long address, int remaining) throws PeerDisconnectedException, PeerIsSlowToReadException, BadProtocolException {
        int msgLen;
        long msgLimit;// expect startup request
        if (remaining < Long.BYTES) {
            return;
        }

        // there is data for length
        // this is quite specific to message type :(
        msgLen = getInt(address); // postgesql includes length bytes in length of message

        // do we have the rest of the message?
        if (msgLen > remaining) {
            // we have length - get the rest when ready
            return;
        }

        // enough to read login request
        recvBufferReadOffset += msgLen;

        // consume message
        // process protocol
        int protocol = getInt(address + Integer.BYTES);
        switch (protocol) {
            case INIT_SSL_REQUEST:
                // SSLRequest
                responseAsciiSink.put('N');
                send();
                return;
            case INIT_STARTUP_MESSAGE:
                // StartupMessage
                // extract properties
                requireInitalMessage = false;
                msgLimit = address + msgLen;
                long lo = address + Long.BYTES;
                // there is an extra byte at the end and it has to be 0
                LOG.info()
                        .$("protocol [major=").$(protocol >> 16)
                        .$(", minor=").$((short) protocol)
                        .$(']').$();

                connectionCharacterStore.clear();

                while (lo < msgLimit - 1) {

                    final LogRecord log = LOG.info();
                    log.$("property [");
                    try {
                        long hi = getStringLength(lo, msgLimit);
                        if (hi == -1) {
                            // we did not find 0 within message limit
                            log.$("malformed property name");
                            throw BadProtocolException.INSTANCE;
                        }

                        log.$("name=").$(dbcs.of(lo, hi));

                        final boolean username = Chars.equals("user", dbcs);

                        // name is ready
                        lo = hi + 1;
                        hi = getStringLength(lo, msgLimit);
                        if (hi == -1) {
                            // we did not find 0 within message limit
                            log.$(", malformed property value");
                            throw BadProtocolException.INSTANCE;
                        }

                        log.$(", value=").$(dbcs.of(lo, hi));
                        lo = hi + 1;
                        if (username) {
                            CharacterStoreEntry e = connectionCharacterStore.newEntry();
                            e.put(dbcs);
                            this.username = e.toImmutable();
                        }
                    } finally {
                        log.$(']').$(); // release under all circumstances
                    }
                }

                checkNotTrue(this.username == null, "user is not specified");
                sendClearTextPasswordChallenge();
                break;
            case INIT_CANCEL_REQUEST:
                LOG.info().$("cancel request").$();
                throw PeerDisconnectedException.INSTANCE;
            default:
                LOG.error().$("unknown init message [protocol=").$(protocol).$(']').$();
                throw BadProtocolException.INSTANCE;
        }
    }

    private void processParse(
            long address,
            long lo,
            long msgLimit,
            @Transient SqlCompiler compiler,
            @Transient AssociativeCache<Object> factoryCache,
            @Transient ObjList<BindVariableSetter> bindVariableSetters
    ) throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        // 'Parse'
        // this appears to be the execution side - we must at least return 'RowDescription'
        // possibly more, check QueryExecutionImpl.processResults() in PG driver for more info

        long hi = getStringLength(lo, msgLimit);
        checkNotTrue(hi == -1, "bad prepared statement name length");

        lo = hi + 1;

        hi = getStringLength(lo, msgLimit);
        checkNotTrue(hi == -1, "bad query text length");

        prepareForNewQuery();
        parseQueryText(lo, hi);

        lo = hi + 1;

        checkNotTrue(lo + Short.BYTES > msgLimit, "could not read parameter count");

        short parameterCount = getShort(lo);

        if (parameterCount > 0) {
            if (lo + Short.BYTES + parameterCount * Integer.BYTES > msgLimit) {
                LOG.error()
                        .$("could not read parameters [parameterCount=").$(parameterCount)
                        .$(", offset=").$(lo - address)
                        .$(", remaining=").$(msgLimit - lo)
                        .$(']').$();
                throw BadProtocolException.INSTANCE;
            }

            LOG.debug().$("params [count=").$(parameterCount).$(']').$();
            lo += Short.BYTES;

            bindVariableService.clear();
            setupBindVariables(lo, parameterCount, bindVariableSetters);
        } else if (parameterCount < 0) {
            LOG.error()
                    .$("invalid parameter count [parameterCount=").$(parameterCount)
                    .$(", offset=").$(lo - address)
                    .$(']').$();
            throw BadProtocolException.INSTANCE;
        }

        // at this point we may have a current query that is not null
        // this is ok to lose reference to this query because we have cache
        // of all of them, which is looked up by query text

        responseAsciiSink.reset();
        final Object statement = factoryCache.peek(queryText);
        if (statement == null) {
            final CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);
            if (cc.getType() == CompiledQuery.SELECT) {
                currentFactory = cc.getRecordCursorFactory();
                factoryCache.put(queryText, currentFactory);
            } else if (cc.getType() == CompiledQuery.INSERT) {
                currentInsertStatement = cc.getInsertStatement();
                factoryCache.put(queryText, currentInsertStatement);
            } else if (cc.getType() == CompiledQuery.COPY_LOCAL) {
                sendCopyInResponse(compiler.getEngine(), cc.getTextLoader());
            } else {
                // DDL SQL
                prepareParseComplete();
                prepareReadyForQuery(responseAsciiSink);
                LOG.info().$("executed DDL").$();
                send();
            }
        } else {
            if (statement instanceof RecordCursorFactory) {
                currentFactory = (RecordCursorFactory) statement;
            } else if (statement instanceof InsertStatement) {
                currentInsertStatement = (InsertStatement) statement;
            } else {
                assert false;
            }
        }
    }

    private void processQuery(
            long lo,
            long limit,
            @Transient SqlCompiler compiler,
            @Transient AssociativeCache<Object> factoryCache
    ) throws BadProtocolException, SqlException, PeerDisconnectedException, PeerIsSlowToReadException {
        // vanilla query
        prepareForNewQuery();
        parseQueryText(lo, limit - 1);

        if (SqlKeywords.isSemicolon(queryText)) {
            sendExecuteTail(TAIL_SUCCESS);
            return;
        }

        final Object statement = factoryCache.peek(queryText);
        if (statement == null) {
            CompiledQuery cc = compiler.compile(queryText, sqlExecutionContext);

            if (cc.getType() == CompiledQuery.SELECT) {
                final RecordCursorFactory factory = cc.getRecordCursorFactory();
                factoryCache.put(queryText, factory);
                executeSelect(factory);
            } else if (cc.getType() == CompiledQuery.COPY_REMOTE) {
                sendCopyInResponse(compiler.getEngine(), cc.getTextLoader());
            } else if (cc.getType() == CompiledQuery.INSERT) {
                // todo: we are throwing away insert model here
                //    we know what this is INSERT without parameters, we should
                //    execute it as we parse without generating models etc.
                currentInsertStatement = cc.getInsertStatement();
                executeInsert();
            } else {
                // DDL SQL
                sendExecuteTail(TAIL_SUCCESS);
            }
        } else {
            if (statement instanceof RecordCursorFactory) {
                executeSelect((RecordCursorFactory) statement);
            }
        }
    }

    void recv() throws PeerDisconnectedException, PeerIsSlowToWriteException, BadProtocolException {
        final int remaining = (int) (recvBufferSize - recvBufferWriteOffset);

        checkNotTrue(remaining < 1, "undersized receive buffer or someone is abusing protocol");

        int n = doReceive(remaining);
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
                    // todo: test that we close resources and rollback transaction when peer disconnects
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
    }

    private void send(int tailType) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendCurrentCursorTail = tailType;
        send();
    }

    private void send() throws PeerDisconnectedException, PeerIsSlowToReadException {
        doSend(
                0,
                (int) (sendBufferPtr - sendBuffer)
        );
    }

    private void sendClearTextPasswordChallenge() throws PeerDisconnectedException, PeerIsSlowToReadException {
        responseAsciiSink.reset();
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(Integer.BYTES * 2);
        responseAsciiSink.putNetworkInt(3);
        send();
    }

    private void sendCopyInResponse(CairoEngine engine, TextLoader textLoader) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (TableUtils.TABLE_EXISTS == engine.getStatus(
                sqlExecutionContext.getCairoSecurityContext(),
                path,
                textLoader.getTableName()
        )) {
            responseAsciiSink.put(MESSAGE_TYPE_COPY_IN_RESPONSE);
            long addr = responseAsciiSink.skip();
            responseAsciiSink.put((byte) 0); // TEXT (1=BINARY, which we do not support yet)
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), textLoader.getTableName())) {
                RecordMetadata metadata = writer.getMetadata();
                responseAsciiSink.putNetworkShort((short) metadata.getColumnCount());
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    responseAsciiSink.putNetworkShort((short) typeOids.get(metadata.getColumnType(i)));
                }
            }
            responseAsciiSink.putLen(addr);
            transientCopyBuffer = Unsafe.malloc(1024 * 1024);
            send();
        } else {
            prepareError(SqlException.$(0, "table '").put(textLoader.getTableName()).put("' does not exist"));
            prepareReadyForQuery(responseAsciiSink);
            send();
        }
    }

    private void sendCursor() throws PeerDisconnectedException, PeerIsSlowToReadException {
        // the assumption for now is that any  will fit into response buffer. This of course precludes us from
        // streaming large BLOBs, but, and its a big one, PostgreSQL protocol for DataRow does not allow for
        // streaming anyway. On top of that Java PostgreSQL driver downloads data row fully. This simplifies our
        // approach for general queries. For streaming protocol we will code something else. PostgeSQL Java driver is
        // slow anyway.

        final Record record = currentCursor.getRecord();
        final RecordMetadata metadata = currentFactory.getMetadata();
        final int columnCount = metadata.getColumnCount();
        while (currentCursor.hasNext()) {
            // create checkpoint to which we can undo the buffer in case
            // current DataRow will does not fit fully.
            responseAsciiSink.bookmark();
            try {
                try {
                    appendRecord(record, metadata, columnCount);
                } catch (NoSpaceLeftInResponseBufferException e) {
                    responseAsciiSink.resetToBookmark();
                    send();
                    // this is now start of send buffer, when this fails we need to log and disconnect
                    appendRecord(record, metadata, columnCount);
                }
            } catch (SqlException e) {
                responseAsciiSink.resetToBookmark();
                LOG.error().$(e.getFlyweightMessage()).$();
                prepareForNewQuery();
                send(TAIL_ERROR);
                return;
            }
        }

        prepareForNewQuery();
        send(TAIL_SUCCESS);
        sendExecuteTail();
    }

    private void sendExecuteTail(int tail) throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendCurrentCursorTail = tail;
        sendExecuteTail();
    }

    private void sendExecuteTail() throws PeerDisconnectedException, PeerIsSlowToReadException {
        switch (sendCurrentCursorTail) {
            case TAIL_SUCCESS:
                prepareCommandComplete();
                prepareReadyForQuery(responseAsciiSink);
                LOG.info().$("executed query").$();
                break;
            case PGConnectionContext.TAIL_ERROR:
                SqlException e = SqlException.last();
                prepareError(e);
                prepareReadyForQuery(responseAsciiSink);
                LOG.info().$("SQL exception [pos=").$(e.getPosition()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                break;
            default:
                break;
        }
        send(PGConnectionContext.TAIL_NONE);
    }

    private void setupBindVariables(
            long lo,
            short pc,
            @Transient ObjList<BindVariableSetter> bindVariableSetters
    ) throws SqlException {
        bindVariableSetters.clear();
        for (int j = 0; j < pc; j++) {
            int pgType = getInt(lo + j * Integer.BYTES);
            switch (pgType) {
                case PG_FLOAT8: // FLOAT8 - double
                    bindVariableService.setDouble(j, Double.NaN);
                    bindVariableSetters.add(doubleSetter);
                    bindVariableSetters.add(doubleTxtSetter);
                    break;
                case PG_INT4: // INT
                    bindVariableService.setInt(j, Numbers.INT_NaN);
                    bindVariableSetters.add(intSetter);
                    bindVariableSetters.add(intTxtSetter);
                    break;
                case PG_INT8:
                    bindVariableService.setLong(j, Numbers.LONG_NaN);
                    bindVariableSetters.add(longSetter);
                    bindVariableSetters.add(longTxtSetter);
                    break;
                case PG_FLOAT4:
                    bindVariableService.setFloat(j, Float.NaN);
                    bindVariableSetters.add(floatSetter);
                    bindVariableSetters.add(floatTxtSetter);
                    break;
                case PG_INT2:
                    bindVariableService.setByte(j, (byte) 0);
                    bindVariableSetters.add(byteSetter);
                    bindVariableSetters.add(byteTxtSetter);
                    break;
                case PG_BOOL:
                    bindVariableService.setBoolean(j, false);
                    bindVariableSetters.add(booleanSetter);
                    bindVariableSetters.add(booleanSetter);
                    break;
                case PG_VARCHAR:
                    bindVariableService.setStr(j, null);
                    bindVariableSetters.add(strSetter);
                    bindVariableSetters.add(strSetter);
                    break;
                case PG_DATE:
                    bindVariableService.setDate(j, Numbers.LONG_NaN);
                    bindVariableSetters.add(noopSetter);
                    bindVariableSetters.add(noopSetter);
                    break;
                case PG_UNSPECIFIED:
                case PG_TIMESTAMP:
                case PG_TIMESTAMPZ:
                    // postgres JDBC driver does not seem to send
                    // microseconds with its text timestamp
                    // on top of this parameters such as setDate, setTimestamp
                    // cause driver to send UNSPECIFIED type
                    // QuestDB has to know types to resolve function linkage
                    // at compile time rather than at runtime.
                    bindVariableService.setDate(j, Numbers.LONG_NaN);
                    bindVariableSetters.add(dateSetter);
                    bindVariableSetters.add(dateSetter);
                    break;
                default:
                    throw SqlException.$(0, "unsupported parameter [type=").put(pgType).put(", index=").put(j).put(']');
            }
        }
    }

    ResponseAsciiSink sink() {
        return responseAsciiSink;
    }

    @FunctionalInterface
    private interface ColumnAppender {
        void append(Record record, int columnIndex) throws SqlException;
    }

    class ResponseAsciiSink extends AbstractCharSink {

        private long bookmarkPtr = -1;

        public void bookmark() {
            this.bookmarkPtr = sendBufferPtr;
        }

        @Override
        public CharSink put(CharSequence cs) {
            // this method is only called by date format utility to print timezone name
            if (cs == null) {
                return this;
            }

            final int len = cs.length();
            if (len == 0) {
                return this;
            }

            ensureCapacity(len);
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(sendBufferPtr + i, (byte) cs.charAt(i));
            }
            sendBufferPtr += len;
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

        public void putLen(long start) {
            putInt(start, (int) (sendBufferPtr - start));
        }

        public void putLenEx(long start) {
            putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        public void putNetworkInt(int len) {
            ensureCapacity(Integer.BYTES);
            putInt(sendBufferPtr, len);
            sendBufferPtr += Integer.BYTES;
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
            putNetworkInt(-1);
        }

        long skip() {
            ensureCapacity(Integer.BYTES);
            long checkpoint = sendBufferPtr;
            sendBufferPtr += Integer.BYTES;
            return checkpoint;
        }
    }

    static {
        typeOids.extendAndSet(ColumnType.STRING, PG_VARCHAR); // VARCHAR
        typeOids.extendAndSet(ColumnType.TIMESTAMP, PG_TIMESTAMP); // TIMESTAMPZ
        typeOids.extendAndSet(ColumnType.DOUBLE, PG_FLOAT8); // FLOAT8
        typeOids.extendAndSet(ColumnType.FLOAT, PG_FLOAT4); // FLOAT4
        typeOids.extendAndSet(ColumnType.INT, PG_INT4); // INT4
        typeOids.extendAndSet(ColumnType.SHORT, PG_INT2); // INT2
        typeOids.extendAndSet(ColumnType.CHAR, PG_CHAR);
        typeOids.extendAndSet(ColumnType.SYMBOL, PG_VARCHAR); // NAME
        typeOids.extendAndSet(ColumnType.LONG, PG_INT8); // INT8
        typeOids.extendAndSet(ColumnType.BYTE, PG_INT2); // INT2
        typeOids.extendAndSet(ColumnType.BOOLEAN, PG_BOOL); // BOOL
        typeOids.extendAndSet(ColumnType.DATE, PG_TIMESTAMP); // DATE
        typeOids.extendAndSet(ColumnType.BINARY, PG_BYTEA); // BYTEA
    }
}
