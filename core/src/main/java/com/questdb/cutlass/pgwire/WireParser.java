/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.pgwire;

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.*;
import com.questdb.std.*;
import com.questdb.std.microtime.DateFormatUtils;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.StdoutSink;

import java.io.Closeable;

import static com.questdb.network.Net.dump;
import static com.questdb.std.time.DateFormatUtils.PG_DATE_FORMAT;
import static com.questdb.std.time.DateFormatUtils.defaultLocale;

public class WireParser implements Closeable {

    private static final int PREFIXED_MESSAGE_HEADER_LEN = 5;
    private static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    private static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    private static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    private static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    private static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    private static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    private static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    private static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    private final static Log LOG = LogFactory.getLog(WireParser.class);
    private static final IntIntHashMap typeOidMap = new IntIntHashMap();
    private static final int TAIL_NONE = 0;
    private static final int TAIL_SUCCESS = 1;
    private static final int TAIL_ERROR = 2;

    static {
        typeOidMap.put(ColumnType.STRING, 1043); // VARCHAR
        typeOidMap.put(ColumnType.TIMESTAMP, 1184); // TIMESTAMPZ
        typeOidMap.put(ColumnType.DOUBLE, 701); // FLOAT8
        typeOidMap.put(ColumnType.FLOAT, 700); // FLOAT4
        typeOidMap.put(ColumnType.INT, 23); // INT4
        typeOidMap.put(ColumnType.SHORT, 21); // INT2
        typeOidMap.put(ColumnType.SYMBOL, 1043); // NAME
        typeOidMap.put(ColumnType.LONG, 20); // INT8
        typeOidMap.put(ColumnType.BYTE, 21); // INT2
        typeOidMap.put(ColumnType.BOOLEAN, 16); // BOOL
        typeOidMap.put(ColumnType.DATE, 1082); // DATE
        typeOidMap.put(ColumnType.BINARY, 17); // BYTEA
    }

    private final NetworkFacade nf;
    private final long recvBuffer;
    private final long sendBuffer;
    private final long sendBufferLimit;
    private final int recvBufferSize;
    private final int sendBufferSize;
    private final SqlCompiler compiler;
    private final ResponseAsciiSink responseAsciiSink = new ResponseAsciiSink();
    private final int idleSendCountBeforeGivingUp;
    private final int idleRecvCountBeforeGivingUp;
    private final AssociativeCache<RecordCursorFactory> factoryCache = new AssociativeCache<>(16, 16);
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final BindVariableService bindVariableService = new BindVariableService();
    private final int maxBlobSizeOnQuery;
    private RecordCursor currentCursor = null;
    private int sendCurrentCursorTail = TAIL_NONE;
    private long sendBufferPtr;
    private boolean loginRequestProcessed = false;
    private long recvBufferWriteOffset = 0;
    private long recvBufferReadOffset = 0;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private RecordCursorFactory currentFactory = null;
    private boolean dumpNetworkTraffic;

    public WireParser(WireParserConfiguration configuration, CairoEngine engine) {
        this.nf = configuration.getNetworkFacade();
        this.compiler = new SqlCompiler(engine);
        this.recvBufferSize = Numbers.ceilPow2(configuration.getRecvBufferSize());
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
        this.sendBufferPtr = sendBuffer;
        this.sendBufferLimit = sendBuffer + sendBufferSize;
        this.idleSendCountBeforeGivingUp = configuration.getIdleSendCountBeforeGivingUp();
        this.idleRecvCountBeforeGivingUp = configuration.getIdleRecvCountBeforeGivingUp();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.maxBlobSizeOnQuery = configuration.getMaxBlobSizeOnQuery();
        LOG.info()
                .$("init [recvBufferSize=").$(recvBufferSize)
                .$(", sendBufferSize=").$(sendBufferSize)
                .$(", maxBlobSizeOnQuery=").$(maxBlobSizeOnQuery)
                .$(']').$();
    }

    @Override
    public void close() {
        Unsafe.free(sendBuffer, sendBufferSize);
        Unsafe.free(recvBuffer, recvBufferSize);
        Misc.free(compiler);
    }

    public void process(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException, PeerIsSlowToWriteException {

        if (bufferRemainingSize > 0) {
            doSend(fd, bufferRemainingOffset, bufferRemainingSize);
        }

        sendExecuteTail(fd);

        // If we have empty buffer we need to try to read something from socket
        // however the opposite  is a little tricky. If buffer is non-empty
        // we still may need to read from socket if contents of this buffer
        // is incomplete and cannot be parsed
        if (recvBufferReadOffset == recvBufferWriteOffset) {
            recv(fd);
        }

        long readOffsetBeforeParse = recvBufferReadOffset;

        // Parse will update the value of recvBufferOffset upon completion of
        // logical block. We cannot count on return value because 'parse' may try to
        // respond to client and fail with exception. When it does fail we would have
        // to retry 'send' but not parse the same input again
        parse(fd, recvBuffer + recvBufferReadOffset, (int) (recvBufferWriteOffset - recvBufferReadOffset));

        // nothing changed?
        if (readOffsetBeforeParse == recvBufferReadOffset) {
            // how come we have something in buffer and parse didn't do anything?
            if (readOffsetBeforeParse < recvBufferWriteOffset) {
                // may be content was incomplete?
                recv(fd);
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
                parse(fd, recvBuffer + recvBufferReadOffset, (int) (recvBufferWriteOffset - recvBufferReadOffset));
                // nothing changed?
                if (readOffsetBeforeParse == recvBufferReadOffset) {
                    // shift to start
                    Unsafe.getUnsafe().copyMemory(recvBuffer + readOffsetBeforeParse, recvBuffer, recvBufferWriteOffset - readOffsetBeforeParse);
                    recvBufferWriteOffset = recvBufferWriteOffset - readOffsetBeforeParse;
                    recvBufferReadOffset = 0;
                    // read more
                    return;
                }
            } while (recvBufferReadOffset < recvBufferWriteOffset);
        }
        recvBufferWriteOffset = 0;
        recvBufferReadOffset = 0;
    }

    private void disconnectClient(long fd) {
        nf.close(fd);
        loginRequestProcessed = false;
    }

    private int doReceive(long fd, int remaining) {
        int n = nf.recv(fd, recvBuffer + recvBufferWriteOffset, remaining);
        dumpBuffer('>', recvBuffer + recvBufferWriteOffset, n);
        return n;
    }

    private void doSend(long fd, int offset, int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int n = nf.send(fd, sendBuffer + offset, size);
        dumpBuffer('<', sendBuffer, n);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n < size) {
            doSendWithRetries(fd, n, size - n);
        }
        sendBufferPtr = sendBuffer;
        bufferRemainingSize = 0;
        bufferRemainingOffset = 0;
    }

    private void doSendWithRetries(long fd, int bufferOffset, int bufferSize) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int offset = bufferOffset;
        int remaining = bufferSize;
        int idleSendCount = 0;

        while (remaining > 0 && idleSendCount < idleSendCountBeforeGivingUp) {
            int m = nf.send(fd, sendBuffer + offset, remaining);
            if (m < 0) {
                throw PeerDisconnectedException.INSTANCE;
            }

            dumpBuffer('<', sendBuffer, m);

            if (m > 0) {
                remaining -= m;
                offset += m;
            } else {
                idleSendCount++;
            }
        }

        if (remaining > 0) {
            this.bufferRemainingOffset = offset;
            this.bufferRemainingSize = remaining;
            throw PeerIsSlowToReadException.INSTANCE;
        }
    }

    private void dumpBuffer(char direction, long buffer, int len) {
        if (dumpNetworkTraffic && len > 0) {
            StdoutSink.INSTANCE.put(direction);
            dump(buffer, len);
        }
    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private void parse(long fd, long address, int len) throws PeerDisconnectedException, PeerIsSlowToReadException {
        long limit = address + len;
        final int remaining = (int) (limit - address);

        if (!loginRequestProcessed) {
            processLoginRequest(fd, address, limit, remaining);
            return;
        }

        // this is a type-prefixed message
        // we will wait until we receive the entire header

        if (remaining < PREFIXED_MESSAGE_HEADER_LEN) {
            // we need to be able to read header and length
            return;
        }


        final byte type = Unsafe.getUnsafe().getByte(address);
        LOG.info().$("got msg '").$((char) type).$('\'').$();
        final int msgLen = NetworkByteOrderUtils.getInt(address + 1);
        assert msgLen > 0;

        // msgLen does not take into account type byte
        if (msgLen > remaining - 1) {
            return;
        }

        // we have enough to read entire message
        recvBufferReadOffset += msgLen + 1;

        long msgLimit = address + msgLen + 1;
        long lo = address + PREFIXED_MESSAGE_HEADER_LEN; // 8 is offset where name value pairs begin

        switch (type) {
            case 'p':
                // +1 is 'type' byte that message length does not account for
                long hi = getStringLength(lo, msgLimit);

                assert hi > -1;

                dbcs.of(lo, hi);

                LOG.info().$("password=").$(dbcs).$();

                // todo: check that this is all client sent
                assert limit == msgLimit;

                // send login ok
                sendLoginOk(fd);
                break;
            case 'P':

                // 'Parse'
                // this appears to be the execution side - we must at least return 'RowDescription'
                // possibly more, check QueryExecutionImpl.processResults() in PG driver for more info

                hi = getStringLength(lo, msgLimit);
                assert hi >= lo;

                LOG.info().$("prepared statement name: ").$(dbcs.of(lo, hi)).$();

                lo = hi + 1;
                hi = getStringLength(lo, msgLimit);
                assert hi >= lo;

                dbcs.of(lo, hi);
                LOG.info().$("parse [q=`").$(dbcs).$("`]").$();

                // todo: read parameter information

                bindVariableService.clear();
                parseQuery(dbcs);
                if (currentFactory == null) {
                    prepareReadyForQuery();
                    LOG.info().$("executed DDL").$();
                    send(fd);
                }
                break;
            case 'X':
                // 'Terminate'
                disconnectClient(fd);
                break;
            case 'C':
                // close
                // todo: read what we are closing
                currentFactory = null;
                responseAsciiSink.put('3'); // close complete
                responseAsciiSink.putNetworkInt(Integer.BYTES);
                send(fd);
                break;
            case 'B': // bind
                // todo: process parameter values when we are _not_ dealing with an DDL
                break;
            case 'E': // execute
                if (currentFactory != null) {
                    LOG.info().$("executing query").$();
                    currentCursor = currentFactory.getCursor();
                    sendCursor(fd, currentCursor, currentFactory.getMetadata());
                    sendExecuteTail(fd);
                }
                break;
            case 'S': // sync?
                break;
            case 'D': // describe?
                if (currentFactory != null) {
                    prepareRowDescription(currentFactory.getMetadata());
                    send(fd);
                    LOG.info().$("described").$();
                }
                break;
            default:
                LOG.error().$("unknown message").$();
                assert false;
        }
    }

    private void processLoginRequest(long fd, long address, long limit, int remaining) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // todo: make it configurable how we authorize and authenticate users
        int msgLen;
        long msgLimit;// expect startup request
        if (remaining < 4) {
            return;
        }

        // there is data for length
        // this is quite specific to message type :(
        msgLen = NetworkByteOrderUtils.getInt(address); // postgesql includes length bytes in length of message

        // do we have the rest of the message?
        if (msgLen > remaining) {
            // we have length - get the rest when ready
            return;
        }

        // enough to read login request
        recvBufferReadOffset += msgLen;
        loginRequestProcessed = true;

        // consume message
        // process protocol
        int protocol = NetworkByteOrderUtils.getInt(address + 4);
        // todo: validate protocol, see 'NegotiateProtocolVersion'

        // extract properties
        msgLimit = address + msgLen;
        long lo = address + 8; // 8 is offset where name value pairs begin
        // there is an extra byte at the end and it has to be 0
        while (lo < msgLimit - 1) {

            long hi = getStringLength(lo, msgLimit);

            // todo: close connection when protocol is broken
            assert hi > -1;
            CharSequence name = new DirectByteCharSequence().of(lo, hi);

            // name is ready

            lo = hi + 1;

            hi = getStringLength(lo, msgLimit);
            assert hi > -1;
            CharSequence value = new DirectByteCharSequence().of(lo, hi);

            lo = hi + 1;

            LOG.info()
                    .$("protocol [major=").$(protocol >> 16)
                    .$(", minor=").$((short) protocol)
                    .$(", name=").$(name)
                    .$(", value=").$(value)
                    .$(']').$();
        }

        // todo: close connection if protocol is violated
        assert Unsafe.getUnsafe().getByte(lo) == 0;

        // todo: check that there is no more data sent
        assert lo + 1 == limit;
        sendClearTextPasswordChallenge(fd);
    }

    private long getStringLength(long x, long limit) {
        // calculate length
        for (long i = x; i < limit; i++) {
            if (Unsafe.getUnsafe().getByte(i) == 0) {
                return i;
            }
        }
        return -1;
    }

    private void parseQuery(CharSequence query) {
        // at this point we may have a current query that is not null
        // this is ok to lose reference to this query because we have cache
        // of all of them, which is looked up by query text

        responseAsciiSink.reset();
        if (!Chars.startsWith(query, "SET")) {
            try {
                currentFactory = factoryCache.peek(query);
                if (currentFactory == null) {
                    currentFactory = compiler.compile(query, bindVariableService);
                    if (currentFactory != null) {
                        factoryCache.put(query, currentFactory);
                    } else {
                        // DDL SQL
                        prepareParseComplete();
                    }
                }
            } catch (SqlException e) {
                prepareError(e);
            }
        } else {
            // same as DDL, but special handling because SQL compiler does not yet understand these SETs
            prepareParseComplete();
        }
    }

    private void prepareCommandComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_COMMAND_COMPLETE);
        long addr = responseAsciiSink.skip();
        responseAsciiSink.encodeUtf8("SELECT ").put(0).put(' ').put(0).put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareError(SqlException e) {
        responseAsciiSink.put(MESSAGE_TYPE_ERROR_RESPONSE);
        long addr = responseAsciiSink.skip();
        responseAsciiSink.put('M');
        responseAsciiSink.encodeUtf8Z(e.getFlyweightMessage());
        responseAsciiSink.put('S');
        responseAsciiSink.encodeUtf8Z("ERROR");
        responseAsciiSink.put('P').put(e.getPosition()).put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareLoginOk() {
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(8); // length of this message
        responseAsciiSink.putNetworkInt(0); // response code
    }

    private void prepareParams(String timeZone, String gmt) {
        responseAsciiSink.put(MESSAGE_TYPE_PARAMETER_STATUS);
        final long addr = responseAsciiSink.skip();
        responseAsciiSink.encodeUtf8Z(timeZone);
        responseAsciiSink.encodeUtf8Z(gmt);
        responseAsciiSink.putLen(addr);
    }

    private void prepareParseComplete() {
        responseAsciiSink.put(MESSAGE_TYPE_PARSE_COMPLETE);
        responseAsciiSink.putNetworkInt(Integer.BYTES);
    }

    private void prepareReadyForQuery() {
        responseAsciiSink.put(MESSAGE_TYPE_READY_FOR_QUERY);
        responseAsciiSink.putNetworkInt(5);
        responseAsciiSink.put('I');
    }

    private void recv(long fd) throws PeerDisconnectedException, PeerIsSlowToWriteException {
        final int remaining = (int) (recvBufferSize - recvBufferWriteOffset);

        if (remaining < 1) {
            throw new RuntimeException("buffer overflow");
        }

        int n = doReceive(fd, remaining);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n == 0) {
            int retriesRemaining = idleRecvCountBeforeGivingUp;
            while (retriesRemaining > 0) {
                n = doReceive(fd, remaining);
                if (n == 0) {
                    retriesRemaining--;
                    continue;
                }

                if (n < 0) {
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

    private void prepareRowDescription(RecordMetadata metadata) {
        responseAsciiSink.put(MESSAGE_TYPE_ROW_DESCRIPTION);
        final long addr = responseAsciiSink.skip();
        final int n = metadata.getColumnCount();
        responseAsciiSink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            responseAsciiSink.encodeUtf8Z(metadata.getColumnName(i));
            responseAsciiSink.putNetworkInt(0);
            responseAsciiSink.putNetworkShort((short) 0);
            responseAsciiSink.putNetworkInt(typeOidMap.get(columnType)); // type
            responseAsciiSink.putNetworkShort((short) 0); // type size?
            responseAsciiSink.putNetworkInt(0); // type mod?
            // this is special behaviour for binary fields to prevent binary data being hex encoded on the wire
            responseAsciiSink.putNetworkShort((short) (columnType == ColumnType.BINARY ? 1 : 0)); // format code
        }

        responseAsciiSink.putLen(addr);
    }

    private void send(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doSend(fd, 0, (int) (sendBufferPtr - sendBuffer));
    }

    private void sendClearTextPasswordChallenge(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        responseAsciiSink.reset();
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(8);
        responseAsciiSink.putNetworkInt(3);
        send(fd);
    }

    private void sendCursor(long fd, RecordCursor cursor, RecordMetadata metadata) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // the assumption for now is that any  will fit into response buffer. This of course precludes us from
        // streaming large BLOBs, but, and its a big one, PostgreSQL protocol for DataRow does not allow for
        // streaming anyway. On top of that Java PostgreSQL driver downloads data row fully. This simplifies our
        // approach for general queries. For streaming protocol we will code something else. PostgeSQL Java driver is
        // slow anyway.

        final Record record = cursor.getRecord();
        final int columnCount = metadata.getColumnCount();
        while (cursor.hasNext()) {
            // create checkpoint to which we can undo the buffer in case
            // current DataRow will does not fit fully.
            responseAsciiSink.bookmark();
            try {
                try {
                    sendRecord(record, metadata, columnCount);
                } catch (NoSpaceLeftInResponseBufferException e) {
                    responseAsciiSink.resetToBookmark();
                    send(fd);
                    // this is now start of send buffer, when this fails we need to log and disconnect
                    sendRecord(record, metadata, columnCount);
                }
            } catch (SqlException e) {
                responseAsciiSink.resetToBookmark();
                LOG.error().$(e.getFlyweightMessage()).$();
                currentCursor = Misc.free(currentCursor);
                sendCurrentCursorTail = TAIL_ERROR;
                send(fd);
                return;
            }
        }

        currentCursor = Misc.free(currentCursor);
        sendCurrentCursorTail = TAIL_SUCCESS;
        send(fd);
    }

    private void sendRecord(Record record, RecordMetadata metadata, int columnCount) throws SqlException {
        responseAsciiSink.put(MESSAGE_TYPE_DATA_ROW); // data
        long b = responseAsciiSink.skip();
        long a;
        responseAsciiSink.putNetworkShort((short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            switch (metadata.getColumnType(i)) {
                case ColumnType.INT:
                    final int intValue = record.getInt(i);
                    if (intValue == Numbers.INT_NaN) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.put(intValue);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.STRING:
                    CharSequence strValue = record.getStr(i);
                    if (strValue == null) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.encodeUtf8(strValue);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.SYMBOL:
                    strValue = record.getSym(i);
                    if (strValue == null) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.encodeUtf8(strValue);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.TIMESTAMP:
                    long longValue = record.getTimestamp(i);
                    if (longValue == Numbers.LONG_NaN) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        DateFormatUtils.PG_TIMESTAMP_FORMAT.format(longValue, DateFormatUtils.defaultLocale, "", responseAsciiSink);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.DATE:
                    longValue = record.getDate(i);
                    if (longValue == Numbers.LONG_NaN) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        PG_DATE_FORMAT.format(longValue, defaultLocale, "", responseAsciiSink);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.DOUBLE:
                    final double doubleValue = record.getDouble(i);
                    if (Double.isNaN(doubleValue)) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.put(doubleValue, 3);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.FLOAT:
                    final float floatValue = record.getFloat(i);
                    if (Float.isNaN(floatValue)) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.put(floatValue, 3);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.SHORT:
                    a = responseAsciiSink.skip();
                    responseAsciiSink.put(record.getShort(i));
                    responseAsciiSink.putLenEx(a);
                    break;
                case ColumnType.LONG:
                    longValue = record.getLong(i);
                    if (longValue == Numbers.LONG_NaN) {
                        responseAsciiSink.setNullValue();
                    } else {
                        a = responseAsciiSink.skip();
                        responseAsciiSink.put(longValue);
                        responseAsciiSink.putLenEx(a);
                    }
                    break;
                case ColumnType.BYTE:
                    a = responseAsciiSink.skip();
                    responseAsciiSink.put((int) record.getByte(i));
                    responseAsciiSink.putLenEx(a);
                    break;
                case ColumnType.BOOLEAN:
                    responseAsciiSink.putNetworkInt(Byte.BYTES);
                    responseAsciiSink.put(record.getBool(i) ? 't' : 'f');
                    break;
                case ColumnType.BINARY:
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
                                    .put(", columnName=").put(metadata.getColumnName(i))
                                    .put(']');
                        }
                    }
                    break;
                default:
                    assert false;
            }
        }
        responseAsciiSink.putLen(b);
    }

    private void sendExecuteTail(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        switch (sendCurrentCursorTail) {
            case TAIL_SUCCESS:
                prepareCommandComplete();
                prepareReadyForQuery();
                LOG.info().$("executed query").$();
                sendCurrentCursorTail = TAIL_NONE;
                send(fd);
                break;
            case TAIL_ERROR:
                prepareError(SqlException.last());
                prepareReadyForQuery();
                LOG.info().$("could not execute query").$();
                sendCurrentCursorTail = TAIL_NONE;
                send(fd);
                break;
            default:
                break;
        }
    }

    private void sendLoginOk(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        responseAsciiSink.reset();
        prepareLoginOk();
        prepareParams("TimeZone", "GMT");
        prepareParams("application_name", "QuestDB");
        prepareParams("server_version_num", "100000");
        prepareParams("integer_datetimes", "on");
        prepareReadyForQuery();
        send(fd);
    }

    private class ResponseAsciiSink extends AbstractCharSink {

        private long bookmarkPtr = -1;

        @Override
        public CharSink put(char c) {
            if (sendBufferPtr < sendBufferLimit) {
                Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) c);
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        public void bookmark() {
            this.bookmarkPtr = sendBufferPtr;
        }

        public void resetToBookmark() {
            assert bookmarkPtr != -1;
            sendBufferPtr = bookmarkPtr;
            bookmarkPtr = -1;
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
                NetworkByteOrderUtils.putInt(sendBufferPtr, (int) len);
                sendBufferPtr += Integer.BYTES;
                for (long x = 0; x < len; x++) {
                    Unsafe.getUnsafe().putByte(sendBufferPtr + x, sequence.byteAt(x));
                }
                sendBufferPtr += len;
            }
        }

        public void putLen(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start));
        }

        public void putLenEx(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        public void putNetworkInt(int len) {
            ensureCapacity(Integer.BYTES);
            NetworkByteOrderUtils.putInt(sendBufferPtr, len);
            sendBufferPtr += Integer.BYTES;
        }

        private void ensureCapacity(int size) {
            if (sendBufferPtr + size < sendBufferLimit) {
                return;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        public void putNetworkShort(short value) {
            ensureCapacity(Short.BYTES);
            NetworkByteOrderUtils.putShort(sendBufferPtr, value);
            sendBufferPtr += Short.BYTES;
        }

        void encodeUtf8Z(CharSequence value) {
            encodeUtf8(value);
            ensureCapacity(Byte.BYTES);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
        }

        void reset() {
            sendBufferPtr = sendBuffer;
        }

        private void setNullValue() {
            putNetworkInt(-1);
        }

        long skip() {
            ensureCapacity(Integer.BYTES);
            long checkpoint = sendBufferPtr;
            sendBufferPtr += Integer.BYTES;
            return checkpoint;
        }
    }
}
