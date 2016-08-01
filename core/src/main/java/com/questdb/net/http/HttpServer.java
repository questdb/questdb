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
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.ex.FatalError;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.ex.NetworkError;
import com.questdb.iter.clock.Clock;
import com.questdb.iter.clock.MilliClock;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Misc;
import com.questdb.misc.Os;
import com.questdb.mp.*;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class HttpServer {
    private final static Log LOG = LogFactory.getLog(HttpServer.class);
    private final InetSocketAddress address;
    private final ObjList<Worker> workers;
    private final CountDownLatch haltLatch;
    private final int workerCount;
    private final CountDownLatch startComplete = new CountDownLatch(1);
    private final UrlMatcher urlMatcher;
    private final ServerConfiguration configuration;
    private volatile boolean running = true;
    private Clock clock = MilliClock.INSTANCE;
    private IODispatcher dispatcher;
    private RingQueue<IOEvent> ioQueue;

    public HttpServer(ServerConfiguration configuration, UrlMatcher urlMatcher) {
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

    public boolean start(ObjHashSet<? extends Job> extraJobs, int queueDepth) {
        this.running = true;
        ioQueue = new RingQueue<>(IOEvent.FACTORY, queueDepth);
        SPSequence ioPubSequence = new SPSequence(ioQueue.getCapacity());
        MCSequence ioSubSequence = new MCSequence(ioQueue.getCapacity(), null);
        ioPubSequence.setBarrier(ioSubSequence);
        ioSubSequence.setBarrier(ioPubSequence);

        try {
            this.dispatcher = createDispatcher("0.0.0.0", address.getPort(), ioQueue, ioPubSequence, clock, configuration, queueDepth);
        } catch (NetworkError e) {
            LOG.error().$("Server failed to start: ").$(e.getMessage()).$();
            running = false;
            return false;
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
        return true;
    }

    public void start() {
        start(null, 1024);
    }

    private IODispatcher createDispatcher(
            CharSequence ip,
            int port,
            RingQueue<IOEvent> ioQueue,
            Sequence ioSequence,
            Clock clock,
            ServerConfiguration configuration,
            int capacity
    ) {

        switch (Os.type) {
            case Os.OSX:
                return new KQueueDispatcher(ip, port, ioQueue, ioSequence, clock, configuration, capacity);
            case Os.WINDOWS:
                return new Win32SelectDispatcher(ip, port, ioQueue, ioSequence, clock, configuration, capacity);
            case Os.LINUX:
                return new EpollDispatcher(ip, port, ioQueue, ioSequence, clock, configuration, capacity);
            default:
                throw new FatalError("Unsupported operating system");
        }
    }

    int getConnectionCount() {
        return this.dispatcher.getConnectionCount();
    }
}
