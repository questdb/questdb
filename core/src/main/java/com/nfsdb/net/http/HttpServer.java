/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 *
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.ex.FatalError;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.NetworkError;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.iter.clock.MilliClock;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Os;
import com.nfsdb.mp.*;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class HttpServer {
    private final static Log LOG = LogFactory.getLog(HttpServer.class);
    private final static int ioQueueSize = 1024;
    private final InetSocketAddress address;
    private final ObjList<Worker> workers;
    private final CountDownLatch haltLatch;
    private final int workerCount;
    private final CountDownLatch startComplete = new CountDownLatch(1);
    private final UrlMatcher urlMatcher;
    private final HttpServerConfiguration configuration;
    private volatile boolean running = true;
    private Clock clock = MilliClock.INSTANCE;
    private IODispatcher dispatcher;
    private RingQueue<IOEvent> ioQueue;

    public HttpServer(HttpServerConfiguration configuration, UrlMatcher urlMatcher) {
        this.address = new InetSocketAddress(configuration.getHttpIP(), configuration.getHttpPort());
        this.urlMatcher = urlMatcher;
        this.workerCount = configuration.getHttpThreads();
        this.haltLatch = new CountDownLatch(workerCount);
        this.workers = new ObjList<>(workerCount);
        this.configuration = configuration;
    }

    public void halt() {
        if (running) {
            running = false;
            try {
                startComplete.await();
                for (int i = 0; i < workers.size(); i++) {
                    workers.getQuick(i).halt();
                }
                haltLatch.await();
                dispatcher.close();
            } catch (Exception e) {
                throw new JournalRuntimeException(e);
            }

            for (int i = 0; i < ioQueue.getCapacity(); i++) {
                IOEvent ev = ioQueue.get(i);
                if (ev != null && ev.context != null) {
                    ev.context = Misc.free(ev.context);
                }
            }
        }
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public void start(ObjHashSet<? extends Job> extraJobs) {
        this.running = true;
        ioQueue = new RingQueue<>(IOEvent.FACTORY, ioQueueSize);
        SPSequence ioPubSequence = new SPSequence(ioQueueSize);
        MCSequence ioSubSequence = new MCSequence(ioQueueSize, null);
        ioPubSequence.followedBy(ioSubSequence);
        ioSubSequence.followedBy(ioPubSequence);

        try {
            this.dispatcher = createDispatcher("0.0.0.0", address.getPort(), ioQueue, ioPubSequence, clock, configuration);
        } catch (NetworkError e) {
            LOG.error().$("Server failed to start: ").$(e.getMessage()).$();
            running = false;
            return;
        }

        IOHttpJob ioHttp = new IOHttpJob(ioQueue, ioSubSequence, this.dispatcher, urlMatcher);

        ObjHashSet<Job> jobs = new ObjHashSet<>();
        jobs.add(this.dispatcher);
        jobs.add(ioHttp);
        if (extraJobs != null) {
            jobs.addAll(extraJobs);
        }

        for (int i = 0; i < workerCount; i++) {
            Worker w;
            workers.add(w = new Worker(jobs, haltLatch));
            w.start();
        }

        startComplete.countDown();
    }

    public void start() {
        start(null);
    }

    private IODispatcher createDispatcher(
            CharSequence ip,
            int port,
            RingQueue<IOEvent> ioQueue,
            Sequence ioSequence,
            Clock clock,
            HttpServerConfiguration configuration
    ) {

        switch (Os.type) {
            case Os.OSX:
                return new KQueueDispatcher(ip, port, ioQueue, ioSequence, clock, configuration);
            case Os.WINDOWS:
                return new Win32SelectDispatcher(ip, port, ioQueue, ioSequence, clock, configuration);
            case Os.LINUX:
                return new EpollDispatcher(ip, port, ioQueue, ioSequence, clock, configuration);
            default:
                throw new FatalError("Unsupported operating system");
        }
    }

    int getConnectionCount() {
        return this.dispatcher.getConnectionCount();
    }
}
