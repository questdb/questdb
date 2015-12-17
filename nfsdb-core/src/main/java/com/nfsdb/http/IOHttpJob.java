/*
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
 */

package com.nfsdb.http;

import com.nfsdb.concurrent.Job;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.exceptions.*;
import com.nfsdb.misc.ByteBuffers;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class IOHttpJob implements Job<IOWorkerContext> {
    // todo: extract config
    public static final int SO_READ_RETRY_COUNT = 1000;
    public static final int SO_WRITE_RETRY_COUNT = 10;
    public static final int SO_RCVBUF_UPLOAD = 4 * 1024 * 1024;
    public static final int SO_RVCBUF_DOWNLD = 128 * 1024;

    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final IOLoopJob loop;
    private final UrlMatcher urlMatcher;

    public IOHttpJob(RingQueue<IOEvent> ioQueue, Sequence ioSequence, IOLoopJob loop, UrlMatcher urlMatcher) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.loop = loop;
        this.urlMatcher = urlMatcher;
    }

    @Override
    public boolean run(IOWorkerContext context) {
        long cursor = ioSequence.next();
        if (cursor < 0) {
            return false;
        }

        IOEvent evt = ioQueue.get(cursor);

        final SocketChannel channel = evt.channel;
        final int op = evt.op;
        final IOContext ioContext = evt.context;

        ioSequence.done(cursor);


        ioContext.threadContext = context;
        process(channel, ioContext, op);

        return true;
    }

    private void feedMultipartContent(MultipartListener handler, IOContext context, SocketChannel channel)
            throws HeadersTooLargeException, IOException, MalformedHeaderException {

        channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF_UPLOAD);
        try {
            Request r = context.request;
            MultipartParser parser = r.getMultipartParser().of(r.getBoundary());
            while (true) {
                try {
                    int sz = r.in.remaining();
                    if (sz > 0 && parser.parse(context, ByteBuffers.getAddress(r.in) + r.in.position(), sz, handler)) {
                        break;
                    }
                } finally {
                    r.in.clear();
                }
                ByteBuffers.copyNonBlocking(channel, r.in, SO_READ_RETRY_COUNT);
            }
        } finally {
            channel.setOption(StandardSocketOptions.SO_RCVBUF, SO_RVCBUF_DOWNLD);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void process(SocketChannel channel, IOContext context, int op) {
        final Request r = context.request;
        final Response response = context.response;

        response.setChannel(channel);

        ChannelStatus status = ChannelStatus.READ;

        boolean _new = r.isIncomplete();
        try {
            if (_new) {
                while ((status = r.read(channel)) == ChannelStatus.NEED_REQUEST) {
                    // loop
                }
            }

            if (status == ChannelStatus.READ) {
                if (r.getUrl() == null) {
                    status = response.simple(400);
                } else {
                    ContextHandler handler = urlMatcher.get(r.getUrl());
                    if (handler != null) {

                        // write what's left to
                        if ((op & SelectionKey.OP_WRITE) != 0) {
                            response._continue();
                            handler._continue(context);
                        }

                        if ((op & SelectionKey.OP_READ) != 0) {
                            if (r.isMultipart()) {
                                if (handler instanceof MultipartListener) {
                                    feedMultipartContent((MultipartListener) handler, context, channel);
                                    handler.handle(context);
                                } else {
                                    status = response.simple(400);
                                }
                            } else {
                                if (handler instanceof MultipartListener) {
                                    status = response.simple(400);
                                } else {
                                    handler.handle(context);
                                }
                            }
                        }
                    } else {
                        status = response.simple(404);
                    }
                }
                context.clear();
            }
        } catch (HeadersTooLargeException ignored) {
            response.simple(431);
            status = ChannelStatus.READ;
        } catch (MalformedHeaderException | DisconnectedChannelException e) {
            status = ChannelStatus.DISCONNECTED;
        } catch (SlowReadableChannelException e) {
            status = ChannelStatus.READ;
        } catch (SlowWritableChannelException e) {
            status = ChannelStatus.WRITE;
        } catch (IOException e) {
            status = ChannelStatus.DISCONNECTED;
            e.printStackTrace();
        } catch (Throwable e) {
            response.simple(500, e.getMessage());
            status = ChannelStatus.DISCONNECTED;
            e.printStackTrace();
        }

        if (status != ChannelStatus.DISCONNECTED) {
            loop.registerChannel(channel, status == ChannelStatus.WRITE ? SelectionKey.OP_WRITE : SelectionKey.OP_READ, context);
        } else {
            try {
                channel.close();
                context.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}