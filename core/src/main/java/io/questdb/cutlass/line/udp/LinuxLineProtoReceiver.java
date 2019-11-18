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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cutlass.line.CairoLineProtoParser;
import io.questdb.cutlass.line.LineProtoLexer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Misc;

import java.io.Closeable;

public class LinuxLineProtoReceiver implements Closeable, Job {
    private static final Log LOG = LogFactory.getLog(LinuxLineProtoReceiver.class);

    private final int msgCount;
    private final LineProtoLexer lexer;
    private final CairoLineProtoParser parser;
    private final NetworkFacade nf;
    private long fd;
    private long msgVec;
    private int commitRate;
    private long totalCount = 0;

    public LinuxLineProtoReceiver(
            LineUdpReceiverConfiguration receiverCfg,
            CairoEngine engine,
            CairoSecurityContext cairoSecurityContext
    ) {

        nf = receiverCfg.getNetworkFacade();

        fd = nf.socketUdp();
        if (fd < 0) {
            int errno = nf.errno();
            LOG.error().$("cannot open UDP socket [errno=").$(errno).$(']').$();
            throw CairoException.instance(errno).put("Cannot open UDP socket");
        }

        try {
            // when listening for multicast packets bind address must be 0
            if (nf.bindUdp(fd, 0, receiverCfg.getPort())) {
                if (nf.join(fd, receiverCfg.getBindIPv4Address(), receiverCfg.getGroupIPv4Address())) {
                    this.commitRate = receiverCfg.getCommitRate();
                    this.msgCount = receiverCfg.getMsgCount();

                    if (receiverCfg.getReceiveBufferSize() != -1 && nf.setRcvBuf(fd, receiverCfg.getReceiveBufferSize()) != 0) {
                        LOG.error().$("cannot set receive buffer size [fd=").$(fd).$(", size=").$(receiverCfg.getReceiveBufferSize()).$(']').$();
                    }

                    msgVec = nf.msgHeaders(receiverCfg.getMsgBufferSize(), msgCount);
                    lexer = new LineProtoLexer(receiverCfg.getMsgBufferSize());
                    parser = new CairoLineProtoParser(engine, cairoSecurityContext);
                    lexer.withParser(parser);

                    LOG.info().$("started [fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", group=").$(receiverCfg.getGroupIPv4Address()).$(", port=").$(receiverCfg.getPort()).$(", batch=").$(msgCount).$(", commitRate=").$(commitRate).$(']').$();

                    return;
                }
                int errno = nf.errno();
                LOG.error().$("cannot join group [errno=").$(errno).$(", fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", group=").$(receiverCfg.getGroupIPv4Address()).$(']').$();
                throw CairoException.instance(nf.errno()).put("Cannot join group ").put(receiverCfg.getGroupIPv4Address()).put(" [bindTo=").put(receiverCfg.getBindIPv4Address()).put(']');
            }
            int errno = nf.errno();
            LOG.error().$("cannot bind socket [errno=").$(errno).$(", fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", port=").$(receiverCfg.getPort()).$(']').$();
            throw CairoException.instance(nf.errno()).put("Cannot bind to ").put(receiverCfg.getBindIPv4Address()).put(':').put(receiverCfg.getPort());

        } catch (CairoException e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() {
        if (fd > -1) {
            if (nf.close(fd) != 0) {
                LOG.error().$("failed to close [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
            } else {
                LOG.info().$("closed [fd=").$(fd).$(']').$();
            }
            if (msgVec != 0) {
                nf.freeMsgHeaders(msgVec);
            }
            if (parser != null) {
                parser.commitAll();
                parser.close();
            }
            Misc.free(lexer);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
        }
    }

    @Override
    public boolean run() {
        boolean ran = false;
        int count;
        while ((count = nf.recvmmsg(fd, msgVec, msgCount)) > 0) {
            long p = msgVec;
            for (int i = 0; i < count; i++) {
                long buf = nf.getMMsgBuf(p);
                lexer.parse(buf, buf + nf.getMMsgBufLen(p));
                lexer.parseLast();
                p += Net.MMSGHDR_SIZE;
            }

            totalCount += count;

            if (totalCount > commitRate) {
                totalCount = 0;
                parser.commitAll();
            }

            if (ran) {
                continue;
            }

            ran = true;
        }
        parser.commitAll();
        return ran;
    }
}
