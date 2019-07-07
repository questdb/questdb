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
import com.questdb.cutlass.pgwire.codecs.AbstractTypePrefixedHeader;
import com.questdb.cutlass.pgwire.codecs.NetworkByteOrderUtils;
import com.questdb.cutlass.pgwire.codecs.in.StartupMessage;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.NetworkFacade;
import com.questdb.network.NoSpaceLeftInResponseBufferException;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
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

    public static final byte MESSAGE_TYPE_LOGIN_RESPONSE = 'R';
    public static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';
    public static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';
    public static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';
    public static final byte MESSAGE_TYPE_DATA_ROW = 'D';
    public static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';
    public static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';
    public static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';
    private final static Log LOG = LogFactory.getLog(WireParser.class);
    private static final IntIntHashMap typeOidMap = new IntIntHashMap();
    private final NetworkFacade nf;
    private final long recvBuffer;
    private final long sendBuffer;
    private final long sendBufferLimit;
    private final int recvBufferSize;
    private final int sendBufferSize;
    private final SqlCompiler compiler;
    private final ResponseAsciiSink responseAsciiSink = new ResponseAsciiSink();
    private final int idleSendCountBeforeGivingUp;
    private final AssociativeCache<RecordCursorFactory> factoryCache = new AssociativeCache<>(16, 16);
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private final BindVariableService bindVariableService = new BindVariableService();
    private RecordCursor currentCursor = null;
    private long sendBufferPtr;
    private boolean loggedIn = false;
    private long recvBufferOffset = 0;
    private int bufferRemainingOffset = 0;
    private int bufferRemainingSize = 0;
    private RecordCursorFactory currentFactory = null;
    private boolean dumpNetworkTraffic;

    public WireParser(WireParserConfiguration configuration, CairoEngine engine) {
        this.nf = configuration.getNetworkFacade();
        this.compiler = new SqlCompiler(engine);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = configuration.getSendBufferSize();
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
        this.sendBufferPtr = sendBuffer;
        this.sendBufferLimit = sendBuffer + sendBufferSize;
        this.idleSendCountBeforeGivingUp = configuration.getIdleSendCountBeforeGivingUp();
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
    }

    @Override
    public void close() {
        Unsafe.free(sendBuffer, sendBufferSize);
        Unsafe.free(recvBuffer, recvBufferSize);
        Misc.free(compiler);
    }

    public void recv(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int remaining = (int) (recvBufferSize - recvBufferOffset);

        if (remaining < 1) {
            throw new RuntimeException("buffer overflow");
        }

        final int n = nf.recv(fd, recvBuffer + recvBufferOffset, remaining);
        dumpBuffer('>', recvBuffer + recvBufferOffset, n);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n == 0) {
            // todo: stay in tight loop for a bit before giving up
            // todo: this exception is misplaced - peer is writing here
            throw PeerIsSlowToReadException.INSTANCE;
        }

        int parsed = parse(fd, recvBuffer, n);
        if (parsed == 0) {
            recvBufferOffset += n;
        } else if (parsed < n) {
            int offset = parsed;
            while (true) {
                int len = n - offset;
                parsed = parse(fd, recvBuffer + offset, len);
                if (parsed == 0) {
                    // shift to start
                    Unsafe.getUnsafe().copyMemory(recvBuffer + offset, recvBuffer, len);
                    recvBufferOffset = len;
                    // read more
                    break;
                } else if (parsed < (n - offset)) {
                    offset += parsed;
                } else {
                    recvBufferOffset = 0;
                    break;
                }
            }
        } else {
            recvBufferOffset = 0;
        }
    }

    public void sendRemaininBuffer(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (bufferRemainingSize > 0) {
            doSendWithRetries(fd, bufferRemainingOffset, bufferRemainingSize);
        }
    }

    private void disconnectClient(long fd) {
        nf.close(fd);
        loggedIn = false;
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
        if (dumpNetworkTraffic) {
            StdoutSink.INSTANCE.put(direction);
            dump(buffer, len);
        }
    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private int parse(long fd, long address, int len) throws PeerDisconnectedException, PeerIsSlowToReadException {
        long limit = address + len;
        final int remaining = (int) (limit - address);

        if (!loggedIn) {
            return processLogin(fd, address, limit, remaining);
        }

        // this is a type-prefixed message
        // we will wait until we receive the entire header

        if (remaining < AbstractTypePrefixedHeader.LEN) {
            // we need to be able to read header and length
            return 0;
        }

        final int msgLen = AbstractTypePrefixedHeader.getLen(address);
        assert msgLen > 0;

        // msgLen does not take into account type byte
        if (msgLen > remaining - 1) {
            return 0;
        }

        final byte type = AbstractTypePrefixedHeader.getType(address);

        LOG.info().$("got msg '").$((char) type).$('\'').$();

        long msgLimit = address + msgLen + 1;
        long lo = address + AbstractTypePrefixedHeader.LEN; // 8 is offset where name value pairs begin

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
                parseQuery(fd, dbcs);
                if (currentFactory == null) {
                    prepareReadyForQuery();
                    send(fd);
                    LOG.info().$("executed DDL").$();
                }
                break;
            case 'X':
                // 'Terminate'
                disconnectClient(fd);
                break;
            case 'C':
                // close
                // todo: read what we are closing
                responseAsciiSink.put('3'); // close complete
                responseAsciiSink.putNetworkInt(Integer.BYTES);
                send(fd);
                break;
            case 'B': // bind
                // todo: decide what we do with this
                if (currentFactory != null) {
                    // todo: set parameter values
                }
                break;
            case 'E': // execute
                if (currentFactory != null) {
                    LOG.info().$("executing query").$();
                    currentCursor = currentFactory.getCursor();
                    sendCursor(fd, currentCursor, currentFactory.getMetadata());
                    currentCursor.close();
                    prepareCommandComplete();
                    prepareReadyForQuery();
                    send(fd);
                    LOG.info().$("executed query").$();
                }
                break;
            case 'S': // sync?
                // todo: wtf is this?
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
        return msgLen + 1;
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

    private void parseQuery(long fd, CharSequence query) {
        if (currentFactory != null) {
            // todo: disconnect - protocol violation
        }

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

    private void prepareNoData() {
        responseAsciiSink.put('n');
        responseAsciiSink.putNetworkInt(Integer.BYTES);
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

    private int processLogin(long fd, long address, long limit, int remaining) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int msgLen;
        long msgLimit;// expect startup request
        if (remaining < 4) {
            return 0;
        }

        // there is data for length
        // this is quite specific to message type :(
        msgLen = StartupMessage.getLen(address); // postgesql includes length bytes in length of message

        // do we have the rest of the message?
        if (msgLen > remaining) {
            // we have length - get the rest when ready
            return 0;
        }

        // 'StartupMessage'

        // consume message
        // process protocol
        int protocol = StartupMessage.getProtocol(address);
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
        loggedIn = true;

        // return msgLen as is because on login message length is all inclusive
        return msgLen;
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
        final int len = (int) (sendBufferPtr - sendBuffer);
        dumpBuffer('<', sendBuffer, len);
        final int n = nf.send(fd, sendBuffer, len);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n < len) {
            doSendWithRetries(fd, n, len - n);
        }
        sendBufferPtr = sendBuffer;
    }

    private void sendClearTextPasswordChallenge(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        responseAsciiSink.reset();
        responseAsciiSink.put(MESSAGE_TYPE_LOGIN_RESPONSE);
        responseAsciiSink.putNetworkInt(8);
        responseAsciiSink.putNetworkInt(3);
        send(fd);
    }

    private void sendCursor(long fd, RecordCursor cursor, RecordMetadata metadata) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final Record record = cursor.getRecord();
        final int columnCount = metadata.getColumnCount();
        while (cursor.hasNext()) {
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
                            responseAsciiSink.put(sequence);
                        }
                        break;
                    default:
                        assert false;
                }
            }
            responseAsciiSink.putLen(b);
        }
        send(fd);
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
        public CharSink put(CharSequence cs) {
            if (cs != null) {
                this.put(cs, 0, cs.length());
            }
            return this;
        }

        @Override
        public CharSink put(CharSequence cs, int start, int end) {
            final long len = end - start;
            if (sendBufferPtr + len < sendBufferLimit) {
                for (int i = start; i < end; i++) {
                    Unsafe.getUnsafe().putByte(sendBufferPtr + i, (byte) cs.charAt(i));
                }
                sendBufferPtr += len;
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public CharSink put(char c) {
            if (sendBufferPtr < sendBufferLimit) {
                Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) c);
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        public CharSink put(byte b) {
            if (sendBufferPtr < sendBufferLimit) {
                Unsafe.getUnsafe().putByte(sendBufferPtr++, b);
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        public void put(BinarySequence sequence) {
            final long len = sequence.length();
            if (sendBufferPtr + len + Integer.BYTES < sendBufferLimit) {
                // when we reach here the "long" length would have to fit in response buffer
                // if it was larger than integers it would never fit into integer-bound response buffer
                NetworkByteOrderUtils.putInt(sendBufferPtr, (int) len);
                sendBufferPtr += Integer.BYTES;
                for (long x = 0; x < len; x++) {
                    Unsafe.getUnsafe().putByte(sendBufferPtr + x, sequence.byteAt(x));
                }
                sendBufferPtr += len;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
        }

        public void putLen(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start));
        }

        public void putLenEx(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        public void putNetworkInt(int len) {
            if (sendBufferPtr + Integer.BYTES < sendBufferLimit) {
                NetworkByteOrderUtils.putInt(sendBufferPtr, len);
                sendBufferPtr += Integer.BYTES;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
        }

        public void putNetworkShort(short value) {
            if (sendBufferPtr + Short.BYTES < sendBufferLimit) {
                NetworkByteOrderUtils.putShort(sendBufferPtr, value);
                sendBufferPtr += Short.BYTES;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
        }

        void encodeUtf8Z(CharSequence value) {
            encodeUtf8(value);
            if (sendBufferPtr < sendBufferLimit) {
                Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
        }

        void reset() {
            sendBufferPtr = sendBuffer;
        }

        private void setNullValue() {
            putNetworkInt(-1);
        }

        long skip() {
            if (sendBufferPtr + Integer.BYTES < sendBufferLimit) {
                long checkpoint = sendBufferPtr;
                sendBufferPtr += Integer.BYTES;
                return checkpoint;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }
    }

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
}
