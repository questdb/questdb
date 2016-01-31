/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.concurrent.*;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.logging.Log;
import com.nfsdb.logging.LogFactory;
import com.nfsdb.net.NonBlockingSecureSocketChannel;
import com.nfsdb.net.PlainSocketChannel;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.Set;

public class IOLoopJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(IOLoopJob.class);
    private final Selector selector;
    private final SelectionKey serverKey;
    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<IOEvent> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();
    private final Clock clock;
    private final HttpServerConfiguration configuration;

    public IOLoopJob(Selector selector,
                     SelectionKey serverKey,
                     RingQueue<IOEvent> ioQueue,
                     Sequence ioSequence,
                     Clock clock,
                     HttpServerConfiguration configuration) {
        this.selector = selector;
        this.serverKey = serverKey;
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.interestQueue = new RingQueue<>(IOEvent.FACTORY, ioQueue.getCapacity());
        this.interestPubSequence = new MPSequence(interestQueue.getCapacity());
        this.interestPubSequence.followedBy(this.interestSubSequence);
        this.interestSubSequence.followedBy(this.interestPubSequence);
        this.clock = clock;
        this.configuration = configuration;
    }

    public void registerChannel(IOContext context, int op) {
        long cursor = interestPubSequence.nextBully();
        IOEvent evt = interestQueue.get(cursor);
        evt.context = context;
        evt.op = op;
        interestPubSequence.done(cursor);
        selector.wakeup();
    }

    @Override
    protected boolean _run() {
        try {
            boolean useful = processRegistrations();
            selector.select(1);

            Set<SelectionKey> keys = selector.selectedKeys();

            if (keys.isEmpty()) {
                return useful;
            }

            for (SelectionKey key : keys) {
                try {
                    if (serverKey.equals(key)) {
                        // connection request
                        configure(((ServerSocketChannel) key.channel()).accept());
                        continue;
                    }

                    int operation = 0;

                    if (key.isReadable()) {
                        operation |= SelectionKey.OP_READ;
                    }

                    if (key.isWritable()) {
                        operation |= SelectionKey.OP_WRITE;
                    }

                    if (operation > 0) {
                        long cursor = ioSequence.nextBully();

                        IOEvent evt = ioQueue.get(cursor);
                        evt.context = (IOContext) key.attachment();
                        evt.op = operation;

                        ioSequence.done(cursor);
                        key.cancel();
                    }
                } catch (CancelledKeyException e) {
                    key.channel().close();
                }
            }

            keys.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    private void configure(SocketChannel channel) throws IOException, JournalNetworkException {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, Request.SO_RVCBUF_DOWNLD);
        channel.register(selector, SelectionKey.OP_READ).attach(
                new IOContext(
                        configuration.getSslConfig().isSecure() ? new NonBlockingSecureSocketChannel<>(channel, configuration.getSslConfig()) : new PlainSocketChannel<>(channel),
                        clock,
                        configuration.getHttpBufReqHeader(),
                        configuration.getHttpBufReqContent(),
                        configuration.getHttpBufReqMultipart(),
                        configuration.getHttpBufRespHeader(),
                        configuration.getHttpBufRespContent()
                )
        );
    }

    @SuppressWarnings("MagicConstant")
    private boolean processRegistrations() {
        long cursor;
        boolean useful = false;
        while ((cursor = interestSubSequence.next()) >= 0) {
            useful = true;
            try {
                IOEvent evt = interestQueue.get(cursor);
                IOContext context = evt.context;
                int op = evt.op;
                interestSubSequence.done(cursor);

                if (context.channel != null) {
                    while (true) {
                        try {
                            context.channel.getChannel().register(selector, op, context);
                            break;
                        } catch (CancelledKeyException e) {
                            selector.selectNow();
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("Failed to process channel registrations").$(e).$();
            }
        }

        return useful;
    }
}
