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

import com.nfsdb.concurrent.Job;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.exceptions.*;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class IOHttpJob implements Job<IOWorkerContext> {
    // todo: extract config
    public static final int SO_WRITE_RETRY_COUNT = 10;

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

        final IOContext ioContext = evt.context;
        final int op = evt.op;

        ioSequence.done(cursor);


        ioContext.threadContext = context;
        process(ioContext, op);

        return true;
    }

    private void process(IOContext context, int op) {
        final Request r = context.request;
        final Response response = context.response;

        ChannelStatus status = ChannelStatus.READ;
        try {

            if ((op & SelectionKey.OP_READ) != 0) {
                r.read();
            }

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
                                r.parseMultipart(context, (MultipartListener) handler);
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
            loop.registerChannel(context, status == ChannelStatus.WRITE ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);
        } else {
            System.out.println("DISCO");
            context.close();
        }
    }
}