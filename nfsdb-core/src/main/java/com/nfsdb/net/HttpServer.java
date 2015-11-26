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

package com.nfsdb.net;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.concurrent.MCSequence;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.SPSequence;
import com.nfsdb.concurrent.Worker;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.net.http.ContextHandler;
import com.nfsdb.net.http.handlers.ImportHandler;
import com.nfsdb.net.http.handlers.UploadHandler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;

public class HttpServer {
    private final InetSocketAddress address;
    private final ObjList<Worker> workers;
    private final CountDownLatch haltLatch;
    private final int ioQueueSize;
    private final int workerCount;
    private final CountDownLatch startComplete = new CountDownLatch(1);
    private final CharSequenceObjHashMap<ContextHandler> handlers = new CharSequenceObjHashMap<>();
    private ServerSocketChannel channel;
    private Selector selector;
    private volatile boolean running = true;


    public HttpServer(InetSocketAddress address, int workerCount, int ioQueueSize) {
        this.address = address;
        this.haltLatch = new CountDownLatch(workerCount);
        this.workers = new ObjList<>(workerCount);
        this.ioQueueSize = ioQueueSize;
        this.workerCount = workerCount;
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            return;
        }
        String dir = args[0];
        HttpServer server = new HttpServer(new InetSocketAddress(9000), 2, 1024);
        server.add("/up", new UploadHandler(new File(dir)));
        server.add("/imp", new ImportHandler(new JournalFactory(dir)));
        server.start();
        System.out.println("Server started");
    }

    public void add(String url, ContextHandler handler) {
        if (handlers.get(url) != null) {
            throw new IllegalArgumentException(("Duplicate url: " + url));
        }
        handlers.put(url, handler);
    }

    public void halt() throws IOException, InterruptedException {
        if (running) {
            running = false;
            startComplete.await();
            for (int i = 0; i < workers.size(); i++) {
                workers.getQuick(i).halt();
            }
            haltLatch.await();
            selector.close();
            channel.close();
        }
    }

    public void start() throws IOException {
        this.running = true;
        this.channel = ServerSocketChannel.open();
        this.channel.bind(address);
        this.channel.configureBlocking(false);
        this.selector = Selector.open();

        instrumentSelector();

        RingQueue<IOEvent> ioQueue = new RingQueue<>(IOEvent.FACTORY, ioQueueSize);
        SPSequence ioPubSequence = new SPSequence(ioQueueSize);
        MCSequence ioSubSequence = new MCSequence(ioQueueSize, null);
        ioPubSequence.followedBy(ioSubSequence);
        ioSubSequence.followedBy(ioPubSequence);

        IOLoopRunnable ioLoop = new IOLoopRunnable(selector, channel.register(selector, SelectionKey.OP_ACCEPT), ioQueue, ioPubSequence, ioQueueSize);
        IOHttpRunnable ioHttp = new IOHttpRunnable(ioQueue, ioSubSequence, ioLoop, handlers);

        ObjList<Runnable> jobs = new ObjList<>();
        jobs.add(ioLoop);
        jobs.add(ioHttp);

        for (int i = 0; i < workerCount; i++) {
            Worker w;
            workers.add(w = new Worker(jobs, haltLatch));
            w.start();
        }

        startComplete.countDown();
    }

    private void instrumentSelector() {
        try {
            Class<?> impl = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

            if (!impl.isAssignableFrom(selector.getClass())) {
                return;
            }

            Field selectedKeys = impl.getDeclaredField("selectedKeys");
            Field publicSelectedKeys = impl.getDeclaredField("publicSelectedKeys");

            selectedKeys.setAccessible(true);
            publicSelectedKeys.setAccessible(true);

            ObjHashSet<SelectionKey> set = new ObjHashSet<>();

            selectedKeys.set(this.selector, set);
            publicSelectedKeys.set(this.selector, set);

        } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
