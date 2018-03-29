/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cutlass.receiver;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.TableWriter;
import com.questdb.cairo.pool.ResourcePool;
import com.questdb.cutlass.receiver.parser.CairoLineProtoParser;
import com.questdb.cutlass.receiver.parser.LineProtoLexer;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.Job;
import com.questdb.std.Misc;
import com.questdb.std.Net;
import com.questdb.std.NetFacade;
import com.questdb.std.Os;

import java.io.Closeable;

public class LinuxLineProtoReceiver implements Closeable, Job {
    private static final Log LOG = LogFactory.getLog(LinuxLineProtoReceiver.class);

    private final int msgCount;
    private final LineProtoLexer lexer;
    private final CairoLineProtoParser parser;
    private final NetFacade nf;
    private long fd;
    private long msgVec;
    private int commitRate;
    private long totalCount = 0;

    public LinuxLineProtoReceiver(ReceiverConfiguration receiverCfg, CairoConfiguration cairoCfg, ResourcePool<TableWriter> writerPool) {

        nf = receiverCfg.getNetFacade();

        fd = nf.socketUdp();
        if (fd < 0) {
            int errno = Os.errno();
            LOG.error().$("cannot open UDP socket [errno=").$(errno).$(']').$();
            throw CairoException.instance(errno).put("Cannot open UDP socket");
        }

        try {
            if (!nf.bindUdp(fd, receiverCfg.getBindIPv4Address(), receiverCfg.getPort())) {
                int errno = Os.errno();
                LOG.error().$("cannot bind socket [errno=").$(errno).$(", fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", port=").$(receiverCfg.getPort()).$(']').$();
                throw CairoException.instance(Os.errno()).put("Cannot bind to ").put(receiverCfg.getBindIPv4Address()).put(':').put(receiverCfg.getPort());
            }

            if (!nf.join(fd, receiverCfg.getBindIPv4Address(), receiverCfg.getGroupIPv4Address())) {
                int errno = Os.errno();
                LOG.error().$("cannot join group [errno=").$(errno).$(", fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", group=").$(receiverCfg.getGroupIPv4Address()).$(']').$();
                throw CairoException.instance(Os.errno()).put("Cannot join group ").put(receiverCfg.getGroupIPv4Address()).put(" [bindTo=").put(receiverCfg.getBindIPv4Address()).put(']');
            }
        } catch (CairoException e) {
            close();
            throw e;
        }

        this.commitRate = receiverCfg.getCommitRate();
        this.msgCount = receiverCfg.getMsgCount();

        if (receiverCfg.getReceiveBufferSize() != -1 && nf.setRcvBuf(fd, receiverCfg.getReceiveBufferSize()) != 0) {
            LOG.error().$("cannot set receive buffer size [fd=").$(fd).$(", size=").$(receiverCfg.getReceiveBufferSize()).$(']').$();
        }

        msgVec = nf.msgHeaders(receiverCfg.getMsgBufferSize(), msgCount);
        lexer = new LineProtoLexer(receiverCfg.getMsgBufferSize());
        parser = new CairoLineProtoParser(cairoCfg, writerPool);
        lexer.withParser(parser);

        LOG.info().$("started [fd=").$(fd).$(", bind=").$(receiverCfg.getBindIPv4Address()).$(", group=").$(receiverCfg.getGroupIPv4Address()).$(", port=").$(receiverCfg.getPort()).$(", batch=").$(msgCount).$(", commitRate=").$(commitRate).$(']').$();
    }

    @Override
    public void close() {
        if (fd > -1) {
            nf.close(fd);
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
