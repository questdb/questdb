/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.ex.*;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.mp.Job;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;
import com.nfsdb.mp.WorkerContext;

import java.io.IOException;

public class IOHttpJob implements Job {
    // todo: extract config
    public static final int SO_WRITE_RETRY_COUNT = 10;
    private final static Log ACCESS = LogFactory.getLog("access");
    private final static Log LOG = LogFactory.getLog(IOHttpJob.class);

    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final IODispatcher ioDispatcher;
    private final UrlMatcher urlMatcher;

    public IOHttpJob(RingQueue<IOEvent> ioQueue, Sequence ioSequence, IODispatcher ioDispatcher, UrlMatcher urlMatcher) {
        this.ioQueue = ioQueue;
        this.ioSequence = ioSequence;
        this.ioDispatcher = ioDispatcher;
        this.urlMatcher = urlMatcher;
    }

    @Override
    public boolean run(WorkerContext context) {
        long cursor = ioSequence.next();
        if (cursor < 0) {
            return false;
        }

        IOEvent evt = ioQueue.get(cursor);

        final IOContext ioContext = evt.context;
        final ChannelStatus op = evt.status;

        ioSequence.done(cursor);


        ioContext.threadContext = context;
        process(ioContext, op);

        return true;
    }

    private static void silent(SimpleResponse sr, int code, CharSequence msg) {
        try {
            sr.send(code, msg);
        } catch (IOException ignore) {
        }
    }

    private void process(IOContext context, ChannelStatus status) {
        final Request r = context.request;
        final SimpleResponse sr = context.simpleResponse();

        try {

            boolean log = r.isIncomplete();
            if (status == ChannelStatus.READ) {
                r.read();
            }

            if (r.getUrl() == null) {
                sr.send(400);
            } else {

                if (log && !r.isIncomplete()) {
                    ACCESS.xinfo().$(" - ").$(r.getUrl()).$();
                }

                ContextHandler handler = urlMatcher.get(r.getUrl());
                if (handler != null) {
                    switch (status) {
                        case WRITE:
                            context.resume();
                            handler.resume(context);
                            break;
                        case READ:
                            if (r.isMultipart()) {
                                if (handler instanceof MultipartListener) {
                                    r.parseMultipart(context, (MultipartListener) handler);
                                    handler.handle(context);
                                } else {
                                    sr.send(400);
                                }
                            } else {
                                if (handler instanceof MultipartListener) {
                                    sr.send(400);
                                } else {
                                    handler.handle(context);
                                }
                            }
                            break;
                        default:
                            LOG.error().$("Unexpected status: ").$(status).$();
                    }
                } else {
                    sr.send(404);
                }
            }
            context.clear();
            status = ChannelStatus.READ;
        } catch (HeadersTooLargeException ignored) {
            silent(sr, 431, null);
            LOG.info().$("Headers too large").$();
            status = ChannelStatus.READ;
        } catch (MalformedHeaderException | DisconnectedChannelException e) {
            status = ChannelStatus.DISCONNECTED;
        } catch (SlowReadableChannelException e) {
            LOG.info().$("Slow read").$();
            status = ChannelStatus.READ;
        } catch (SlowWritableChannelException e) {
            LOG.info().$("Slow write").$();
            status = ChannelStatus.WRITE;
        } catch (IOException e) {
            status = ChannelStatus.DISCONNECTED;
            LOG.error().$("Unexpected IOException: ").$(e).$();
        } catch (Throwable e) {
            silent(sr, 500, e.getMessage());
            status = ChannelStatus.DISCONNECTED;
            LOG.error().$("Internal error: ").$(e).$();
        }
        ioDispatcher.registerChannel(context, status);
    }
}