/*
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
 */

package com.nfsdb.net.http;

import com.nfsdb.ex.*;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Net;
import com.nfsdb.mp.Job;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;

import java.io.IOException;

public class IOHttpJob implements Job {
    public static final int SO_WRITE_RETRY_COUNT = 1000;
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
    public boolean run() {
        long cursor = ioSequence.next();
        if (cursor < 0) {
            return false;
        }

        IOEvent evt = ioQueue.get(cursor);

        final IOContext ioContext = evt.context;
        final ChannelStatus op = evt.status;

        ioSequence.done(cursor);
        process(ioContext, op);

        return true;
    }

    private static void silent(SimpleResponse sr, int code, CharSequence msg) {
        try {
            sr.send(code, msg);
        } catch (IOException ignore) {
        }
    }

    private void process(IOContext context, final ChannelStatus status) {
        final Request r = context.request;
        final SimpleResponse sr = context.simpleResponse();

        ChannelStatus result;

        try {

            boolean log = r.isIncomplete();
            if (status == ChannelStatus.READ) {
                r.read();
            }

            if (r.getUrl() == null) {
                sr.send(400);
            } else {

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
                            break;
                    }
                } else {
                    sr.send(404);
                }

                if (log && !r.isIncomplete()) {
                    ACCESS.xinfo().
                            $ip(Net.getPeerIP(context.channel.getFd())).
                            $(" -").
                            $(" -").
                            $(" [").
                            $ts(System.currentTimeMillis()).
                            $("] ").
                            $('"').$(r.getMethodLine()).$('"').
                            $(' ').$(context.getResponseCode()).
                            $(' ').$(context.channel.getTotalWrittenAndReset()).
                            $();
                }
            }
            context.clear();
            result = ChannelStatus.READ;
        } catch (HeadersTooLargeException ignored) {
            silent(sr, 431, null);
            LOG.info().$("Headers too large").$();
            result = ChannelStatus.READ;
        } catch (MalformedHeaderException | DisconnectedChannelException e) {
            result = ChannelStatus.DISCONNECTED;
        } catch (EndOfChannelException e) {
            result = ChannelStatus.EOF;
        } catch (SlowReadableChannelException e) {
            LOG.debug().$("Slow read").$();
            result = ChannelStatus.READ;
        } catch (SlowWritableChannelException e) {
            LOG.debug().$("Slow write").$();
            result = ChannelStatus.WRITE;
        } catch (IOException e) {
            result = ChannelStatus.DISCONNECTED;
            LOG.error().$("Unexpected IOException: ").$(e).$();
        } catch (Throwable e) {
            silent(sr, 500, e.getMessage());
            result = ChannelStatus.DISCONNECTED;
            LOG.error().$("Internal error: ").$(e).$();
        }
        ioDispatcher.registerChannel(context, result);
    }
}