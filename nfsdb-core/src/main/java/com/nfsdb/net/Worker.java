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

import com.lmax.disruptor.WorkHandler;
import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.utils.ByteBuffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Worker implements WorkHandler<NetworkEvent> {
    private static final String NOT_FOUND = "HTTP/1.1 404 NotFound\r\n" +
            "Content-Length:9\r\n" +
            "Content-Type:text/html\r\n" +
            "\r\n" +
            "NOT FOUND";

    private static final String cannedResponse2 = "HTTP/1.1 100 CONTINUE\r\n" +
            "Content-Length:0\r\n" +
            "Content-Type:text/html\r\n" +
            "\r\n";

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(128 * 1024);
    private final Dispatcher dispatcher;
    private final Request request = new Request();
    private final CharSequenceObjHashMap<ContextHandler> contextHandlers;

    public Worker(Dispatcher dispatcher, CharSequenceObjHashMap<ContextHandler> contextHandlers) {
        this.dispatcher = dispatcher;
        this.contextHandlers = contextHandlers;
    }

    public boolean consumeHeaders(SocketChannel channel, long timeout) throws IOException {
        buffer.clear();
        int count = 0;
        long deadline = System.currentTimeMillis() + timeout;
        do {
            if (channel.read(buffer) > 0) {
                count++;
                long lo = ByteBuffers.getAddress(buffer);
                long hi = lo + buffer.position();
                buffer.position((int) (request.parse(lo, hi) - lo));
            }
        } while (count == 0 && System.currentTimeMillis() < deadline);

        buffer.clear();
        return count > 0;
    }

    @Override
    public void onEvent(NetworkEvent event) throws Exception {
        SocketChannel channel = event.channel;
        try {
            request.init();

            if (consumeHeaders(event.channel, 500)) {
                ContextHandler handler = contextHandlers.get(request.getUrl());
                if (handler != null) {
                    handler.handle(request, event.context, event.channel, buffer);
                } else {
                    notFound(event.channel);
                }
                dispatcher.registerChannel(event);
            } else {
                channel.close();
            }
        } catch (IOException e) {
            System.out.println("Cannot read: " + e.getMessage());
            channel.close();
        }
    }

    private void notFound(SocketChannel channel) throws IOException {
        buffer.clear();
        for (int i = 0, n = NOT_FOUND.length(); i < n; i++) {
            buffer.put((byte) NOT_FOUND.charAt(i));
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}