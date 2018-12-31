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

package com.questdb.tuck.http;

import com.questdb.ex.DisconnectedChannelRuntimeException;
import com.questdb.ex.HeadersTooLargeException;
import com.questdb.ex.MalformedHeaderException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.Job;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.EndOfChannelException;
import com.questdb.std.ex.SlowReadableChannelException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.tuck.event.ChannelStatus;
import com.questdb.tuck.event.Dispatcher;
import com.questdb.tuck.event.Event;

import java.io.IOException;

public class IOHttpJob implements Job {
    public static final int SO_WRITE_RETRY_COUNT = 1000;
    private final static Log ACCESS = LogFactory.getLog("access");
    private final static Log LOG = LogFactory.getLog(IOHttpJob.class);

    private final RingQueue<Event<IOContext>> ioQueue;
    private final Sequence ioSequence;
    private final Dispatcher<IOContext> ioDispatcher;
    private final UrlMatcher urlMatcher;

    IOHttpJob(RingQueue<Event<IOContext>> ioQueue, Sequence ioSequence, Dispatcher<IOContext> ioDispatcher, UrlMatcher urlMatcher) {
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

        Event<IOContext> evt = ioQueue.get(cursor);
        final IOContext ioContext = evt.context;
        final int status = evt.channelStatus;
        ioSequence.done(cursor);
        process(ioContext, status);

        return true;
    }

    @Override
    public void setupThread() {
        ioDispatcher.setupThread();
        urlMatcher.setupHandlers();
    }

    private static void logAccess(IOContext context) {
        ACCESS.xinfo().
                $ip(context.channel.getIp()).
                $(" -").
                $(" -").
                $(" [").
                $ts(System.currentTimeMillis()).
                $("] ").
                $('"').$(context.request.getMethodLine()).$('"').
                $(' ').$(context.getResponseCode()).
                $(' ').$(context.channel.getTotalWrittenAndReset()).
                $();
    }

    private void process(IOContext context, final int channelStatus) {
        final Request r = context.request;
        final SimpleResponse sr = context.simpleResponse();

        int newChannelStatus;

        try {

            boolean log = r.isIncomplete();
            if (channelStatus == ChannelStatus.READ) {
                r.read();
            }

            if (r.getUrl() == null) {
                sr.send(400);
            } else {

                ContextHandler handler = urlMatcher.get(r.getUrl());
                if (handler != null) {
                    switch (channelStatus) {
                        case ChannelStatus.WRITE:
                            context.resume();
                            handler.resume(context);
                            break;
                        case ChannelStatus.READ:
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
                            LOG.error().$("Unexpected status: ").$(channelStatus).$();
                            break;
                    }
                } else {
                    sr.send(404);
                }

                if (log && !r.isIncomplete()) {
                    logAccess(context);
                }
            }
            context.clear();
            newChannelStatus = ChannelStatus.READ;
        } catch (HeadersTooLargeException ignored) {
            silent(context, 431, null);
            LOG.info().$("Headers too large").$();
            logAccess(context);
            newChannelStatus = ChannelStatus.READ;
        } catch (MalformedHeaderException | DisconnectedChannelException | DisconnectedChannelRuntimeException e) {
            newChannelStatus = ChannelStatus.DISCONNECTED;
        } catch (EndOfChannelException e) {
            newChannelStatus = ChannelStatus.EOF;
        } catch (SlowReadableChannelException e) {
            LOG.debug().$("Slow read").$();
            newChannelStatus = ChannelStatus.READ;
        } catch (SlowWritableChannelException e) {
            LOG.debug().$("Slow write").$();
            newChannelStatus = ChannelStatus.WRITE;
        } catch (Throwable e) {
            context.clear();
            silent(context, 500, e.getMessage());
            newChannelStatus = ChannelStatus.DISCONNECTED;
            LOG.error().$("Internal error: ").$(e).$();
            logAccess(context);
        }
        ioDispatcher.registerChannel(context, newChannelStatus);
    }

    private void silent(IOContext context, int code, CharSequence msg) {
        try {
            context.emergencyResponse().send(code, msg);
        } catch (IOException ignore) {
        }
    }
}