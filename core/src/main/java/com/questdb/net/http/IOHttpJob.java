/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.ex.*;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Net;
import com.questdb.mp.Job;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;

import java.io.IOException;

public class IOHttpJob implements Job {
    public static final int SO_WRITE_RETRY_COUNT = 1000;
    private final static Log ACCESS = LogFactory.getLog("access");
    private final static Log LOG = LogFactory.getLog(IOHttpJob.class);

    private final RingQueue<IOEvent> ioQueue;
    private final Sequence ioSequence;
    private final IODispatcher ioDispatcher;
    private final UrlMatcher urlMatcher;

    IOHttpJob(RingQueue<IOEvent> ioQueue, Sequence ioSequence, IODispatcher ioDispatcher, UrlMatcher urlMatcher) {
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

    private static void logAccess(IOContext context) {
        ACCESS.xinfo().
                $ip(Net.getPeerIP(context.channel.getFd())).
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
                    logAccess(context);
                }
            }
            context.clear();
            result = ChannelStatus.READ;
        } catch (HeadersTooLargeException ignored) {
            silent(context, 431, null);
            LOG.info().$("Headers too large").$();
            logAccess(context);
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
        } catch (Throwable e) {
            silent(context, 500, e.getMessage());
            result = ChannelStatus.DISCONNECTED;
            LOG.error().$("Internal error: ").$(e).$();
            logAccess(context);
        }
        ioDispatcher.registerChannel(context, result);
    }

    private void silent(IOContext context, int code, CharSequence msg) {
        try {
            context.emergencyResponse().send(code, msg);
        } catch (IOException ignore) {
        }
    }
}