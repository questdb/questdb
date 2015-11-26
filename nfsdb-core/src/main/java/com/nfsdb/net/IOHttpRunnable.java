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

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.exceptions.DisconnectedChannelException;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.exceptions.MalformedHeaderException;
import com.nfsdb.exceptions.SlowChannelException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.net.http.*;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class IOHttpRunnable implements Runnable {
    // todo: extract config
    public static final int SO_READ_RETRY_COUNT = 1000;
    public static final int SO_RCVBUF_UPLOAD = 4 * 1024 * 1024;
    public static final int SO_RVCBUF_DOWNLD = 128 * 1024;

    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final IOLoopRunnable loop;
    private final CharSequenceObjHashMap<ContextHandler> handlers;

    public IOHttpRunnable(RingQueue<IOEvent> ioQueue, Sequence ioSequence, IOLoopRunnable loop, CharSequenceObjHashMap<ContextHandler> handlers) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.loop = loop;
        this.handlers = handlers;
    }

    @Override
    public void run() {
        long cursor = ioSequence.next();
        if (cursor < 0) {
            return;
        }

        IOEvent evt = ioQueue.get(cursor);

        final SocketChannel channel = evt.channel;
        final int op = evt.op;
        final IOContext context = evt.context;

        ioSequence.done(cursor);

        process(channel, context, op);
    }

    private void feedMultipartContent(MultipartListener handler, IOContext context, SocketChannel channel)
            throws HeadersTooLargeException, SlowChannelException, IOException, MalformedHeaderException, DisconnectedChannelException {

        channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF_UPLOAD);
        Request r = context.request;
        MultipartParser parser = r.getMultipartParser().of(r.getBoundary());
        while (true) {
//            MultipartParser.dump(r.in);
            try {
                int sz = r.in.remaining();
                if (sz > 0 && parser.parse(context, ((DirectBuffer) r.in).address() + r.in.position(), sz, handler)) {
                    break;
                }
            } finally {
                r.in.clear();
            }
            ByteBuffers.copyNonBlocking(channel, r.in, SO_READ_RETRY_COUNT);
        }
        channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_RVCBUF_DOWNLD);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void process(SocketChannel channel, IOContext context, int op) {
        Request r = context.request;
        Request.ChannelStatus status = Request.ChannelStatus.READY;

        boolean _new = r.isIncomplete();
        try {
            if (_new) {
                while ((status = r.read(channel)) == Request.ChannelStatus.NEED_REQUEST) {
                    // loop
                }
            }

            final Response response = context.response;
            response.setChannel(channel);

            if (status == Request.ChannelStatus.READY) {
                ContextHandler handler = handlers.get(r.getUrl());
                if (handler != null) {
                    if (r.isMultipart()) {
                        if (handler instanceof MultipartListener) {
                            if (_new) {
                                handler.onHeaders(context);
                            }
                            try {
                                feedMultipartContent((MultipartListener) handler, context, channel);
                                handler.onComplete(context);
                            } catch (SlowChannelException e) {
                                handler.park(context);
                                throw e;
                            }
                        } else {
                            // todo: 400 - bad request
                        }
                    }
                } else {
                    // todo: 404
                }
                r.clear();
            }
        } catch (HeadersTooLargeException | MalformedHeaderException | DisconnectedChannelException e) {
            status = Request.ChannelStatus.DISCONNECTED;
        } catch (SlowChannelException e) {
            System.out.println("slow");
            status = Request.ChannelStatus.READY;
        } catch (IOException e) {
            status = Request.ChannelStatus.DISCONNECTED;
            e.printStackTrace();
        }

        if (status != Request.ChannelStatus.DISCONNECTED) {
            loop.registerChannel(channel, SelectionKey.OP_READ, context);
        } else {
            System.out.println("Disconnected: " + channel);
            try {
                channel.close();
                context.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
