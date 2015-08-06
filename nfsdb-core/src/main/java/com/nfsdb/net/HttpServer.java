/*
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
 */

package com.nfsdb.net;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.ObjHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpServer {
    private static final int BUFFER_SIZE = 1024 * 8;
    private final CharSequenceObjHashMap<ContextHandler> contextHandlers = new CharSequenceObjHashMap<>();
    private final InetSocketAddress address;
    private final int threadCount;
    private ExecutorService executor;
    private WorkerPool<NetworkEvent> workerPool;
    private ServerSocketChannel channel;
    private Selector selector;
    private Dispatcher dispatcher;

    public HttpServer(final InetSocketAddress address, int threadCount) {
        this.address = address;
        this.threadCount = threadCount;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        HttpServer server = new HttpServer(new InetSocketAddress(9000), 2);

        server.addContext("/hello", new ContextHandler() {
            @Override
            public void handle(Request request, Session session, SocketChannel channel, ByteBuffer buffer) throws IOException {
                final String cannedResponse = "HTTP/1.1 200 OK\r\n" +
                        "Content-Length:14\r\n" +
                        "Content-Type:text/html\r\n" +
                        "\r\n" +
                        "This is a test";

                buffer.clear();
                for (int i = 0, n = cannedResponse.length(); i < n; i++) {
                    buffer.put((byte) cannedResponse.charAt(i));
                }
                buffer.flip();
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }

            }
        });

        server.start();
    }

    public void addContext(String context, ContextHandler handler) {
        contextHandlers.put(context, handler);
    }

    public void halt() throws InterruptedException, IOException {
        dispatcher.halt();
        workerPool.halt();
        executor.shutdownNow();
        selector.close();
        channel.close();
    }

    public void start() throws IOException {
        this.executor = Executors.newFixedThreadPool(threadCount + 1);
        this.channel = ServerSocketChannel.open();
        this.channel.bind(address);
        this.channel.configureBlocking(false);
        this.selector = openSelector();
        RingBuffer<NetworkEvent> networkEvents = RingBuffer.createSingleProducer(NetworkEvent.EVENT_FACTORY, BUFFER_SIZE, new BlockingWaitStrategy());
        this.dispatcher = new Dispatcher(selector, channel.register(selector, SelectionKey.OP_ACCEPT), networkEvents);
        Worker[] workers = new Worker[threadCount];
        for (int i = 0; i < threadCount; i++) {
            workers[i] = new Worker(dispatcher, contextHandlers);
        }
        this.workerPool = new WorkerPool<>(networkEvents, networkEvents.newBarrier(), new FatalExceptionHandler(), workers);
        networkEvents.addGatingSequences(workerPool.getWorkerSequences());
        executor.submit(dispatcher);
        workerPool.start(executor);
    }

    @SuppressFBWarnings({"REC_CATCH_EXCEPTION"})
    private Selector openSelector() throws IOException {
        Selector selector = Selector.open();

        ObjHashSet<SelectionKey> set = new ObjHashSet<>();

        try {
            Class<?> selectorImplClass = Class.forName("sun.nio.ch.SelectorImpl", false, HttpServer.class.getClassLoader());
            // Ensure the current selector implementation is what we can instrument.
            if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
                return selector;
            }
            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
            selectedKeysField.setAccessible(true);
            publicSelectedKeysField.setAccessible(true);
            selectedKeysField.set(selector, set);
            publicSelectedKeysField.set(selector, set);
            return selector;
        } catch (Exception e) {
            return selector;
        }
    }
}