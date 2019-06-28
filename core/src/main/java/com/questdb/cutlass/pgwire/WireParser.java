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
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.Net;
import com.questdb.network.NetworkFacade;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.std.Chars;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class WireParser implements Closeable {

    private final static Log LOG = LogFactory.getLog(WireParser.class);
    private static final IntIntHashMap typeOidMap = new IntIntHashMap();

    static {
        typeOidMap.put(ColumnType.STRING, 1043); // VARCHAR
        typeOidMap.put(ColumnType.TIMESTAMP, 1184); // TIMESTAMPZ
        typeOidMap.put(ColumnType.DOUBLE, 701); // FLOAT8
        typeOidMap.put(ColumnType.FLOAT, 700);
        typeOidMap.put(ColumnType.INT, 23); // INT4
        typeOidMap.put(ColumnType.SHORT, 21); // INT2
    }

    private final NetworkFacade nf;
    private final long recvBuffer;
    private final long sendBuffer;
    private final int recvBufferSize;
    private final int sendBufferSize;
    private final SqlCompiler compiler;
    private final ResponseAsciiSink responseAsciiSink = new ResponseAsciiSink();
    private long sendBufferPtr;
    private int state = 0;
    private long recvBufferOffset = 0;

    public WireParser(WireParserConfiguration configuration, CairoEngine engine) {
        this.nf = configuration.getNetworkFacade();
        this.compiler = new SqlCompiler(engine);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = configuration.getSendBufferSize();
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
        this.sendBufferPtr = sendBuffer;
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

        final int n = Net.recv(fd, recvBuffer + recvBufferOffset, remaining);
        if (n < 0) {
            throw PeerDisconnectedException.INSTANCE;
        }

        if (n == 0) {
            // todo: stay in tight loop for a bit before giving up
            // todo: this exception is misplaced - peer is writing here
            throw PeerIsSlowToReadException.INSTANCE;
        }

        if (parse(fd, recvBuffer, n)) {
            recvBufferOffset = 0;
        } else {
            recvBufferOffset += n;
        }
    }

    private void disconnectClient(long fd) {
        nf.close(fd);
    }

    private void executeParseAndSendResult(long fd, CharSequence query) {
        responseAsciiSink.reset();
        if (Chars.startsWith(query, "SET")) {
            prepareParseComplete();
            prepareReadyForQuery();
            send(fd);
        } else {
            try {
                try (RecordCursorFactory factory = compiler.compile(query)) {
                    if (factory != null) {
                        prepareRowDescription(factory.getMetadata());

                        RecordCursor cursor = factory.getCursor();
                        Record record = cursor.getRecord();
                        RecordMetadata metadata = factory.getMetadata();
                        final int columnCount = metadata.getColumnCount();

                        while (cursor.hasNext()) {
                            responseAsciiSink.put('D'); // data
                            long b = responseAsciiSink.skip();
                            long a;
                            responseAsciiSink.putNetworkShort((short) columnCount);
                            for (int i = 0; i < columnCount; i++) {
                                switch (metadata.getColumnType(i)) {
                                    case ColumnType.INT:
                                        a = responseAsciiSink.skip();
                                        responseAsciiSink.put(record.getInt(i));
                                        responseAsciiSink.putLenEx(a);
                                        break;
                                    case ColumnType.STRING:
                                        responseAsciiSink.putNetworkInt(record.getStrLen(i));
                                        responseAsciiSink.encodeUtf8(record.getStr(i));
                                        break;
                                    case ColumnType.TIMESTAMP:
                                        responseAsciiSink.putNetworkInt(Long.BYTES);
                                        responseAsciiSink.put(record.getTimestamp(i));
                                        break;
                                    case ColumnType.DOUBLE:
                                        responseAsciiSink.putNetworkInt(Double.BYTES);
                                        responseAsciiSink.put(record.getDouble(i), 3);
                                        break;
                                    default:
                                        assert false;
                                }
                            }
                            responseAsciiSink.putLen(b);
                        }

                        prepareCommandComplete();
                        prepareReadyForQuery();
                        send(fd);
                    } else {
                        prepareParseComplete();
                        prepareReadyForQuery();
                        send(fd);
                    }
                }
            } catch (SqlException e) {
                prepareError(e);
                prepareReadyForQuery();
                send(fd);
            }
        }
    }

    private void prepareCommandComplete() {
        responseAsciiSink.put('C');
        long addr = responseAsciiSink.skip();
        responseAsciiSink.encodeUtf8("SELECT ").put(0).put(' ').put(0).put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareRowDescription(RecordMetadata metadata) {
        responseAsciiSink.put('T');
        final long addr = responseAsciiSink.skip();
        final int n = metadata.getColumnCount();
        responseAsciiSink.putNetworkShort((short) n);
        for (int i = 0; i < n; i++) {
            responseAsciiSink.encodeUtf8Z(metadata.getColumnName(i));
            responseAsciiSink.putNetworkInt(0);
            responseAsciiSink.putNetworkShort((short) 0);
            responseAsciiSink.putNetworkInt(typeOidMap.get(metadata.getColumnType(i))); // type
            responseAsciiSink.putNetworkShort((short) 0); // type size?
            responseAsciiSink.putNetworkInt(0); // type mod?
            responseAsciiSink.putNetworkShort((short) 0); // format code
        }

        responseAsciiSink.putLen(addr);
    }

    private void prepareNoData() {
        responseAsciiSink.put('n');
        responseAsciiSink.putNetworkInt(Integer.BYTES);
    }

    private void prepareError(SqlException e) {
        responseAsciiSink.put('E');
        long addr = responseAsciiSink.skip();
        responseAsciiSink.put('M');
        responseAsciiSink.encodeUtf8Z(e.getFlyweightMessage());
        responseAsciiSink.put('S');
        responseAsciiSink.encodeUtf8Z("ERROR");
        responseAsciiSink.put('P').put(e.getPosition()).put((char) 0);
        responseAsciiSink.putLen(addr);
    }

    private void prepareParseComplete() {
        responseAsciiSink.put('1');
        responseAsciiSink.putNetworkInt(Integer.BYTES);
    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     */
    private boolean parse(long fd, long address, int len) {
        long limit = address + len;
        int msgLen;
        long msgLimit;

        final int remaining = (int) (limit - address);

        switch (state) {
            case 0:
                // expect startup request
                if (remaining < 4) {
                    return false;
                }

                // there is data for length
                // this is quite specific to message type :(
                msgLen = StartupMessage.getLen(address); // postgesql includes length bytes in length of message

                // do we have the rest of the message?
                if (msgLen > remaining) {
                    // we have length - get the rest when ready
                    return false;
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
                state = 3;
                return true;
            case 3:

                // this is a type-prefixed message
                // we will wait until we receive the entire header

                if (remaining < AbstractTypePrefixedHeader.LEN) {
                    // we need to be able to read header and length
                    return false;
                }

                msgLen = AbstractTypePrefixedHeader.getLen(address);
                assert msgLen > 0;

                // msgLen does not take into account type byte
                if (msgLen > remaining - 1) {
                    return false;
                }

                final byte type = AbstractTypePrefixedHeader.getType(address);

                LOG.info().$("got msg '").$((char) type).$('\'').$();

                msgLimit = address + msgLen + 1;
                lo = address + AbstractTypePrefixedHeader.LEN; // 8 is offset where name value pairs begin

                switch (type) {
                    case 'p':
                        // +1 is 'type' byte that message length does not account for
                        long hi = getStringLength(lo, msgLimit);

                        CharSequence password = new DirectByteCharSequence().of(lo, hi);

                        LOG.info().$("password=").$(password).$();

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

                        CharSequence preparedStatementName = new DirectByteCharSequence().of(lo, hi);
                        LOG.info().$("prepared statement name: ").$(preparedStatementName).$();

                        lo = hi + 1;
                        hi = getStringLength(lo, msgLimit);
                        CharSequence query = new DirectByteCharSequence().of(lo, hi);
                        LOG.info().$("query: ").$(query).$();

                        // todo: read parameter information

                        executeParseAndSendResult(fd, query);
                        break;

                    case 'X':

                        // 'Terminate'
                        disconnectClient(fd);
                        state = 0;
                        break;

                }
        }
        return true;
    }

    private void sendClearTextPasswordChallenge(long fd) {
        responseAsciiSink.reset();
        responseAsciiSink.put('R');
        responseAsciiSink.putNetworkInt(8);
        responseAsciiSink.putNetworkInt(3);
        send(fd);
    }

    private void sendLoginOk(long fd) {
        responseAsciiSink.reset();
        prepareLoginOk();
        prepareParams("TimeZone", "GMT");
        prepareParams("application_name", "QuestDB");
        prepareParams("server_version_num", "100000");
        prepareParams("integer_datetimes", "on");
        prepareReadyForQuery();
        send(fd);
    }

    private void send(long fd) {
        // todo: deal with incomplete send
        int n = nf.send(fd, sendBuffer, (int) (sendBufferPtr - sendBuffer));
        LOG.info().$("sent [n=").$(n).$(']').$();
    }

    private void prepareLoginOk() {
        responseAsciiSink.put('R');
        responseAsciiSink.putNetworkInt(8); // length of this message
        responseAsciiSink.putNetworkInt(0); // response code
    }

    private void prepareReadyForQuery() {
        responseAsciiSink.put('Z');
        responseAsciiSink.putNetworkInt(5);
        responseAsciiSink.put('I');
    }

    private void prepareParams(String timeZone, String gmt) {
        responseAsciiSink.put('S');
        final long addr = responseAsciiSink.skip();
        responseAsciiSink.encodeUtf8Z(timeZone);
        responseAsciiSink.encodeUtf8Z(gmt);
        responseAsciiSink.putLen(addr);
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

    private class ResponseAsciiSink extends AbstractCharSink {
        @Override
        public CharSink put(CharSequence cs, int start, int end) {
            for (int i = start; i < end; i++) {
                Unsafe.getUnsafe().putByte(sendBufferPtr + i, (byte) cs.charAt(i));
            }
            sendBufferPtr += (end - start);
            return this;
        }

        @Override
        public CharSink put(char c) {
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) c);
            return this;
        }

        public void putNetworkInt(int len) {
            NetworkByteOrderUtils.putInt(sendBufferPtr, len);
            sendBufferPtr += Integer.BYTES;
        }

        public void putNetworkShort(short value) {
            NetworkByteOrderUtils.putShort(sendBufferPtr, value);
            sendBufferPtr += Short.BYTES;
        }

        public void putLen(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start));
        }

        public void putLenEx(long start) {
            NetworkByteOrderUtils.putInt(start, (int) (sendBufferPtr - start - Integer.BYTES));
        }

        long skip() {
            long checkpoint = sendBufferPtr;
            sendBufferPtr += Integer.BYTES;
            return checkpoint;
        }

        void reset() {
            sendBufferPtr = sendBuffer;
        }

        void encodeUtf8Z(CharSequence value) {
            encodeUtf8(value);
            Unsafe.getUnsafe().putByte(sendBufferPtr++, (byte) 0);
        }
    }
}
