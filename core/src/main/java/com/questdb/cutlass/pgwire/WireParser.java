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

import com.questdb.cutlass.pgwire.codecs.AbstractTypePrefixedHeader;
import com.questdb.cutlass.pgwire.codecs.in.StartupMessage;
import com.questdb.cutlass.pgwire.codecs.out.AuthenticationMsg;
import com.questdb.cutlass.pgwire.codecs.out.ParameterStatusMsg;
import com.questdb.cutlass.pgwire.codecs.out.ReadyForQueryMsg;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.Net;
import com.questdb.network.NetworkFacade;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class WireParser implements Closeable {

    private final static Log LOG = LogFactory.getLog(WireParser.class);

    private int state = 0;
    private final NetworkFacade nf;
    private final long recvBuffer;
    private final long sendBuffer;
    private final int recvBufferSize;
    private final int sendBufferSize;
    private long recvBufferOffset = 0;

    public WireParser(WireParserConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(this.recvBufferSize);
        this.sendBufferSize = configuration.getSendBufferSize();
        this.sendBuffer = Unsafe.malloc(this.sendBufferSize);
    }

    @Override
    public void close() {
        Unsafe.free(sendBuffer, sendBufferSize);
        Unsafe.free(recvBuffer, recvBufferSize);
    }

    public void recv(long fd) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final int remaining = (int) (recvBufferSize - recvBufferOffset);

        if (remaining < 1) {
            throw new RuntimeException("buffer overflow");
        }

        final int n = Net.recv(fd, recvBuffer + recvBufferOffset, remaining);
        if (n == -1) {
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
        long offset;
        // send 'ParseComplete'
        AuthenticationMsg.setType(sendBuffer, (byte) '1');
        AuthenticationMsg.setLen(sendBuffer, 4);

        offset = 5;
//                                    Net.send(clientFd, sendBuffer, 5);

        // send 'ReadyForQuery'
        ReadyForQueryMsg.setType(sendBuffer + offset, (byte) 'Z');
        ReadyForQueryMsg.setLen(sendBuffer + offset, 5);
        ReadyForQueryMsg.setStatus(sendBuffer + offset, (byte) 'I');
        Net.send(fd, sendBuffer, (int) (offset + 6));

    }

    /**
     * returns address of where parsing stopped. If there are remaining bytes left
     * int the buffer they need to be passed again in parse function along with
     * any additional bytes received
     *
     * @param address
     * @param len
     * @return
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
        AuthenticationMsg.setType(sendBuffer, (byte) 'R');
        AuthenticationMsg.setLen(sendBuffer, 8);
        AuthenticationMsg.setResponseCode(sendBuffer, 3);
        Net.send(fd, sendBuffer, 9);
        // todo: deal with incomplete send
    }

    private void sendLoginOk(long fd) {
        // send login ok
        // send authentication challenge
        AuthenticationMsg.setType(sendBuffer, (byte) 'R');
        AuthenticationMsg.setLen(sendBuffer, 8);
        AuthenticationMsg.setResponseCode(sendBuffer, 0);
//                                    Net.send(clientFd, sendBuffer, 9);
        // length so far 9

        // send 'ParameterStatus'
        long offset = 9;
        offset += ParameterStatusMsg.setParameterPair(
                sendBuffer + offset,
                "TimeZone", "GMT");

        offset += ParameterStatusMsg.setParameterPair(
                sendBuffer + offset,
                "application_name", "QuestDB");

        offset += ParameterStatusMsg.setParameterPair(
                sendBuffer + offset,
                "server_version_num", "100000");

        offset += ParameterStatusMsg.setParameterPair(
                sendBuffer + offset,
                "integer_datetimes", "on");

        // send 'ReadyForQuery'
        ReadyForQueryMsg.setType(sendBuffer + offset, (byte) 'Z');
        ReadyForQueryMsg.setLen(sendBuffer + offset, 5);
        ReadyForQueryMsg.setStatus(sendBuffer + offset, (byte) 'I');
        nf.send(fd, sendBuffer, (int) (offset + 6));
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

    @FunctionalInterface
    public interface MessageHandler {
        int onMessage(int action);
    }
}
