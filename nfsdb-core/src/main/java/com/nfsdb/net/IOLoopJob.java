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

package com.nfsdb.net;

import com.nfsdb.concurrent.*;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.Set;

public class IOLoopJob extends SynchronizedJob<IOWorkerContext> {
    private final Selector selector;
    private final SelectionKey serverKey;
    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final RingQueue<IOEvent> interestQueue;
    private final MPSequence interestPubSequence;
    private final SCSequence interestSubSequence = new SCSequence();

    public IOLoopJob(Selector selector, SelectionKey serverKey, RingQueue<IOEvent> ioQueue, Sequence ioSequence, int interestQueueLen) {
        this.selector = selector;
        this.serverKey = serverKey;
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.interestQueue = new RingQueue<>(IOEvent.FACTORY, interestQueueLen);
        this.interestPubSequence = new MPSequence(interestQueueLen, null);
        this.interestPubSequence.followedBy(this.interestSubSequence);
        this.interestSubSequence.followedBy(this.interestPubSequence);
    }

    public void registerChannel(SocketChannel channel, int op, IOContext context) {
        long cursor = interestPubSequence.nextBully();
        IOEvent evt = interestQueue.get(cursor);

        evt.channel = channel;
        evt.op = op;
        evt.context = context;

        interestPubSequence.done(cursor);
        selector.wakeup();
    }

    @Override
    protected boolean _run(IOWorkerContext context) {
        try {
            boolean useful = processRegistrations();
            selector.select(1);

            Set<SelectionKey> keys = selector.selectedKeys();

            if (keys.size() == 0) {
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
                        evt.channel = (SocketChannel) key.channel();
                        evt.op = operation;
                        evt.context = (IOContext) key.attachment();

                        ioSequence.done(cursor);
                        key.cancel();
                    }
                } catch (CancelledKeyException e) {
                    key.channel().close();
                }
            }

            keys.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    private void configure(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, IOHttpJob.SO_RVCBUF_DOWNLD);
        channel.register(selector, SelectionKey.OP_READ).attach(new IOContext());
    }

    @SuppressWarnings("MagicConstant")
    private boolean processRegistrations() {
        long cursor;
        boolean useful = false;
        while ((cursor = interestSubSequence.next()) >= 0) {
            useful = true;
            try {
                IOEvent evt = interestQueue.get(cursor);

                SocketChannel channel = evt.channel;
                IOContext context = evt.context;
                int op = evt.op;

                interestSubSequence.done(cursor);

                if (channel != null) {
                    while (true) {
                        try {
                            channel.register(selector, op, context);
                            break;
                        } catch (CancelledKeyException e) {
                            selector.selectNow();
                        }
                    }
                }
            } catch (Throwable e) {
                // todo: do something about this
                e.printStackTrace();
            }
        }

        return useful;
    }
}
