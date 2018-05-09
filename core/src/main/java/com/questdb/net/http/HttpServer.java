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

package com.questdb.net.http;

import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.common.JournalRuntimeException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.*;
import com.questdb.net.*;
import com.questdb.std.*;
import com.questdb.std.ex.FatalError;
import com.questdb.std.ex.NetworkError;
import com.questdb.std.time.MillisecondClock;
import com.questdb.std.time.MillisecondClockImpl;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class HttpServer {
    private final static Log LOG = LogFactory.getLog(HttpServer.class);
    private static final ObjectFactory<Event<IOContext>> EVENT_FACTORY = Event::new;
    private final InetSocketAddress address;
    private final ObjList<Worker> workers;
    private final CountDownLatch haltLatch;
    private final int workerCount;
    private final CountDownLatch startComplete = new CountDownLatch(1);
    private final UrlMatcher urlMatcher;
    private final ServerConfiguration configuration;
    private final ContextFactory<IOContext> contextFactory;
    private final ObjHashSet<Job> jobs = new ObjHashSet<>();
    private volatile boolean running = true;
    private MillisecondClock clock = MillisecondClockImpl.INSTANCE;
    private Dispatcher<IOContext> dispatcher;
    private RingQueue<Event<IOContext>> ioQueue;

    public HttpServer(BootstrapEnv env) {
        this.configuration = env.configuration;
        this.address = new InetSocketAddress(configuration.getHttpIP(), configuration.getHttpPort());
        this.urlMatcher = env.matcher;
        this.workerCount = configuration.getHttpThreads();
        this.haltLatch = new CountDownLatch(workerCount);
        this.workers = new ObjList<>(workerCount);
        this.contextFactory = (fd, clock) -> new IOContext(new NetworkChannelImpl(fd), configuration, clock);
    }

    public ObjHashSet<Job> getJobs() {
        return jobs;
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
                Event<IOContext> ev = ioQueue.get(i);
                if (ev != null && ev.context != null) {
                    ev.context = Misc.free(ev.context);
                }
            }
        }
    }

    public void setClock(MillisecondClock clock) {
        this.clock = clock;
    }

    public boolean start() {
        this.running = true;
        ioQueue = new RingQueue<>(EVENT_FACTORY, configuration.getHttpQueueDepth());
        SPSequence ioPubSequence = new SPSequence(ioQueue.getCapacity());
        MCSequence ioSubSequence = new MCSequence(ioQueue.getCapacity());
        ioPubSequence.then(ioSubSequence).then(ioPubSequence);

        try {
            this.dispatcher = createDispatcher(address.getHostName(), address.getPort(), ioQueue, ioPubSequence, clock, configuration);
        } catch (NetworkError e) {
            LOG.error().$("Server failed to start: ").$(e.getMessage()).$();
            running = false;
            return false;
        }

        jobs.add(this.dispatcher);
        jobs.add(new IOHttpJob(ioQueue, ioSubSequence, this.dispatcher, urlMatcher));

        for (int i = 0; i < workerCount; i++) {
            Worker w;
            workers.add(w = new Worker(jobs, haltLatch));
            w.start();
        }

        startComplete.countDown();
        LOG.info().$("Server is running").$();
        return true;
    }

    private Dispatcher<IOContext> createDispatcher(
            CharSequence ip,
            int port,
            RingQueue<Event<IOContext>> ioQueue,
            Sequence ioSequence,
            MillisecondClock clock,
            ServerConfiguration configuration
    ) {

        switch (Os.type) {
            case Os.OSX:
                return new KQueueDispatcher<>(
                        ip,
                        port,
                        configuration.getHttpMaxConnections(),
                        configuration.getHttpTimeout(),
                        ioQueue,
                        ioSequence,
                        clock,
                        configuration.getHttpQueueDepth(),
                        EVENT_FACTORY,
                        contextFactory
                );
            case Os.WINDOWS:
                return new Win32SelectDispatcher<>(
                        ip,
                        port,
                        configuration.getHttpMaxConnections(),
                        configuration.getHttpTimeout(),
                        ioQueue,
                        ioSequence,
                        clock,
                        configuration.getHttpQueueDepth(),
                        EVENT_FACTORY,
                        contextFactory
                );
            case Os.LINUX:
                return new EpollDispatcher<>(
                        ip,
                        port,
                        configuration.getHttpMaxConnections(),
                        configuration.getHttpTimeout(),
                        ioQueue,
                        ioSequence,
                        clock,
                        configuration.getHttpQueueDepth(),
                        EVENT_FACTORY,
                        contextFactory
                );
            default:
                throw new FatalError("Unsupported operating system");
        }
    }

    int getConnectionCount() {
        return this.dispatcher.getConnectionCount();
    }
}
