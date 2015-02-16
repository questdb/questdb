/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.nfsdb.exp.StringSink;
import com.nfsdb.logging.Logger;
import com.nfsdb.utils.Dates;

public class JournalServerLogger {
    private static final Logger LOGGER = Logger.getLogger(JournalServerLogger.class);
    private final RingBuffer<ServerLogMsg> ringBuffer;
    private final BatchEventProcessor<ServerLogMsg> eventProcessor;
    private final StringSink sink = new StringSink();

    public JournalServerLogger() {
        this.ringBuffer = RingBuffer.createMultiProducer(ServerLogMsg.EVENT_FACTORY, 1024, new BlockingWaitStrategy());
        this.eventProcessor = new BatchEventProcessor<>(ringBuffer, ringBuffer.newBarrier(), new EventHandler<ServerLogMsg>() {
            @Override
            public void onEvent(ServerLogMsg msg, long sequence, boolean endOfBatch) throws Exception {

                sink.clear();
                Dates.appendDateTime(sink, System.currentTimeMillis());
                sink.put(' ')
                        .put(msg.getSocketAddress().toString())
                        .put(' ')
                        .put(msg.getMessage());


                if (msg.getLevel() == null) {
                    LOGGER.trace(sink);
                } else {
                    switch (msg.getLevel()) {
                        case INFO:
                            LOGGER.info(sink, msg.getThrowable());
                            break;
                        case ERROR:
                            LOGGER.error(sink, msg.getThrowable());
                            break;
                        default:
                            LOGGER.trace(sink);
                            break;
                    }
                }
            }
        });
        ringBuffer.addGatingSequences(eventProcessor.getSequence());
    }

    public void halt() {
        eventProcessor.halt();
    }

    public ServerLogMsg msg() {
        long sequence = ringBuffer.next();
        ServerLogMsg msg = ringBuffer.get(sequence);
        if (msg.ringBuffer == null) {
            msg.ringBuffer = ringBuffer;
        }

        msg.sequence = sequence;
        msg.setException(null);
        return msg;
    }

    public void start() {
        Thread thread = new Thread(eventProcessor);
        thread.setName("nfsdb-server-logger");
        thread.start();
    }
}
