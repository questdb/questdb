/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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


import com.lmax.disruptor.*;
import com.nfsdb.collections.ObjHashSet;

import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.concurrent.CountDownLatch;

public class Dispatcher implements Runnable {
    private final CountDownLatch haltLatch = new CountDownLatch(1);
    private final Selector selector;
    private final SelectionKey serverKey;
    private final RingBuffer<NetworkEvent> eventBuffer;
    private final SequenceBarrier registrationBarrier;
    private final RingBuffer<NetworkEvent> registrationEventBuffer;
    private final Sequence registrationSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private long nextSequence = 0;
    private boolean running = true;

    public Dispatcher(Selector selector, SelectionKey serverKey, RingBuffer<NetworkEvent> eventBuffer) {
        this.selector = selector;
        this.serverKey = serverKey;
        this.eventBuffer = eventBuffer;
        this.registrationEventBuffer = RingBuffer.createMultiProducer(NetworkEvent.EVENT_FACTORY, eventBuffer.getBufferSize(), new BlockingWaitStrategy());
        this.registrationBarrier = registrationEventBuffer.newBarrier();
        this.registrationEventBuffer.addGatingSequences(registrationSequence);
    }

    public void halt() throws InterruptedException {
        this.running = false;
        haltLatch.await();
    }

    public void registerChannel(NetworkEvent event) {
        long seq = registrationEventBuffer.next();
        NetworkEvent e = registrationEventBuffer.get(seq);
        e.channel = event.channel;
        e.context = event.context;
        registrationEventBuffer.publish(seq);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (running) {
            try {
                processRegister();
                selector.select(100L);

                ObjHashSet<SelectionKey> set = (ObjHashSet<SelectionKey>) selector.selectedKeys();

                for (int i = 0, n = set.size(); i < n; i++) {
                    SelectionKey key = set.get(i);
                    try {
                        if (serverKey.equals(key)) {
                            SocketChannel ch = ((ServerSocketChannel) key.channel()).accept();
                            ch.configureBlocking(false);
                            ch.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);
                            SelectionKey k = ch.register(selector, SelectionKey.OP_READ);
                            k.attach(new Session());
                        } else if (key.isReadable()) {
                            long seq = eventBuffer.next();
                            NetworkEvent e = eventBuffer.get(seq);
                            e.channel = (SocketChannel) key.channel();
                            e.context = (Session) key.attachment();
                            eventBuffer.publish(seq);
                            key.cancel();
                        }
                    } catch (CancelledKeyException e) {
                        key.channel().close();
                    }
                }
                set.clear();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        haltLatch.countDown();
    }

    private void processRegister() {
        registrationBarrier.clearAlert();
        long availableSequence = registrationBarrier.getCursor();
        try {
            while (nextSequence <= availableSequence) {
                NetworkEvent event = registrationEventBuffer.get(nextSequence);
                if (event.channel != null) {
                    while (true) {
                        try {
                            SelectionKey k = event.channel.register(selector, SelectionKey.OP_READ);
                            k.attach(event.context);
                            break;
                        } catch (CancelledKeyException e) {
                            selector.selectNow();
                        }
                    }
                }
                nextSequence++;
            }
            registrationSequence.set(availableSequence);
        } catch (final Throwable e) {
            registrationSequence.set(nextSequence);
            nextSequence++;
            e.printStackTrace();
        }
    }
}