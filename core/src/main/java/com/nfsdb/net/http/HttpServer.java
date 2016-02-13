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

import com.nfsdb.iter.clock.Clock;
import com.nfsdb.iter.clock.MilliClock;
import com.nfsdb.misc.Os;
import com.nfsdb.mp.*;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class HttpServer {
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

    public HttpServer(HttpServerConfiguration configuration, UrlMatcher urlMatcher) {
        this.address = new InetSocketAddress(configuration.getHttpPort());
        this.urlMatcher = urlMatcher;
        this.workerCount = configuration.getHttpThreads();
        this.haltLatch = new CountDownLatch(workerCount);
        this.workers = new ObjList<>(workerCount);
        this.configuration = configuration;
    }

    public int getConnectionCount() {
        return this.dispatcher.getConnectionCount();
    }

    public void halt() throws IOException, InterruptedException {
        if (running) {
            running = false;
            startComplete.await();
            for (int i = 0; i < workers.size(); i++) {
                workers.getQuick(i).halt();
            }
            haltLatch.await();
            dispatcher.close();
        }
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public void start(ObjHashSet<? extends Job> extraJobs) {
        this.running = true;
        RingQueue<IOEvent> ioQueue = new RingQueue<>(IOEvent.FACTORY, ioQueueSize);
        SPSequence ioPubSequence = new SPSequence(ioQueueSize);
        MCSequence ioSubSequence = new MCSequence(ioQueueSize, null);
        ioPubSequence.followedBy(ioSubSequence);
        ioSubSequence.followedBy(ioPubSequence);

        this.dispatcher = createDispatcher("0.0.0.0", address.getPort(), ioQueue, ioPubSequence, clock, configuration);
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
                throw new RuntimeException("Unsupported operating system");
        }
    }
}
