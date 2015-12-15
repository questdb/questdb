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

package com.nfsdb.http;

import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.collections.ObjList;
import com.nfsdb.concurrent.*;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.http.handlers.ImportHandler;
import com.nfsdb.http.handlers.StaticContentHandler;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.iter.clock.MilliClock;
import com.nfsdb.logging.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class HttpServer {
    private final static int ioQueueSize = 1024;
    private final static Logger LOGGER = Logger.getLogger(HttpServer.class);
    private final InetSocketAddress address;
    private final ObjList<Worker<IOWorkerContext>> workers;
    private final CountDownLatch haltLatch;
    private final int workerCount;
    private final CountDownLatch startComplete = new CountDownLatch(1);
    private final UrlMatcher urlMatcher;
    private final HttpServerConfiguration configuration;
    private ServerSocketChannel channel;
    private Selector selector;
    private volatile boolean running = true;
    private Clock clock = MilliClock.INSTANCE;

    public HttpServer(HttpServerConfiguration configuration, UrlMatcher urlMatcher) {
        this.address = new InetSocketAddress(configuration.getHttpPort());
        this.urlMatcher = urlMatcher;
        this.workerCount = configuration.getHttpThreads();
        this.haltLatch = new CountDownLatch(workerCount);
        this.workers = new ObjList<>(workerCount);
        this.configuration = configuration;
    }

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        if (args.length < 1) {
            return;
        }
        String dir = args[0];
        extractSite(dir);
        File conf = new File(dir, "conf/nfsdb.conf");

        if (!conf.exists()) {
            LOGGER.error("Configuration file does not exist: " + conf);
            return;
        }

        final HttpServerConfiguration configuration = new HttpServerConfiguration(conf);
        final FileSender fileSender = new FileSender(new MimeTypes(configuration.getMimeTypes()));
        final SimpleUrlMatcher matcher = new SimpleUrlMatcher();

        matcher.put("/imp", new ImportHandler(new JournalFactory(configuration.getDbPath().getAbsolutePath())));
        matcher.put("/tmp", new StaticContentHandler(fileSender));

        HttpServer server = new HttpServer(configuration, matcher);
        server.start();
        LOGGER.info("Server started");
        LOGGER.info(configuration);
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

    public void setClock(Clock clock) {
        this.clock = clock;
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

        IOLoopJob ioLoop = new IOLoopJob(selector, channel.register(selector, SelectionKey.OP_ACCEPT), ioQueue, ioPubSequence, clock, configuration);
        IOHttpJob ioHttp = new IOHttpJob(ioQueue, ioSubSequence, ioLoop, urlMatcher);

        ObjList<Job<IOWorkerContext>> jobs = new ObjList<>();
        jobs.add(ioLoop);
        jobs.add(ioHttp);

        for (int i = 0; i < workerCount; i++) {
            Worker<IOWorkerContext> w;
            workers.add(w = new Worker<>(jobs, haltLatch, new IOWorkerContext()));
            w.start();
        }

        startComplete.countDown();
    }

    private static void extractSite(String dir) throws URISyntaxException, IOException {
        URL url = HttpServer.class.getResource("/site/");
        final Path source = Paths.get(url.toURI());
        final Path target = Paths.get(dir);
        final EnumSet<FileVisitOption> walkOptions = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final CopyOption[] copyOptions = new CopyOption[]{COPY_ATTRIBUTES, REPLACE_EXISTING};

        Files.walkFileTree(source, walkOptions, Integer.MAX_VALUE, new FileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                Path newDirectory = target.resolve(source.relativize(dir));
                try {
                    Files.copy(dir, newDirectory, copyOptions);
                } catch (FileAlreadyExistsException ignore) {
                } catch (IOException x) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), copyOptions);
                return CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });
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
