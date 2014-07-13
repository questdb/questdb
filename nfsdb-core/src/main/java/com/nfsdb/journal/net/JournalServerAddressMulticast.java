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

package com.nfsdb.journal.net;

import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.net.config.ServerConfig;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class JournalServerAddressMulticast implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(JournalServerAddressMulticast.class);

    private final ExecutorService executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("jj-server-multicast", true));
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ServerConfig config;
    private DatagramPacket packetRcv;
    private DatagramPacket packetSnd;
    private MulticastSocket socket;
    private Future future;

    public JournalServerAddressMulticast(ServerConfig config) {
        this.config = config;
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void run() {
        while (isRunning()) {
            try {
                socket.receive(packetRcv);
                byte[] data = packetRcv.getData();
                switch (data[0]) {
                    case ServerConfig.ADDRESS_REQUEST_PREFIX:
                        socket.send(packetSnd);
                        LOGGER.info("Replying");
                        break;
                    default:
                        LOGGER.warn("Unlnown command: " + data[0]);
                }
            } catch (IOException e) {
                LOGGER.debug("Server multicast error: %s", e.getMessage());
            }
        }
    }

    public void start() throws JournalNetworkException {
        if (!isRunning()) {
            this.socket = config.openMulticastSocket();
            this.packetRcv = new DatagramPacket(new byte[9], 9);

            InetSocketAddress address = config.getSocketAddress();
            int port = address.getPort();
            byte snd[] = new byte[10];
            snd[0] = ServerConfig.ADDRESS_RESPONSE_PREFIX;
            System.arraycopy(address.getAddress().getAddress(), 0, snd, 1, 4);
            snd[5] = (byte) (config.getSslConfig().isSecure() ? 1 : 0);
            int offset = 6;
            snd[offset] = (byte) (port >> 24);
            snd[offset + 1] = (byte) (port >> 16);
            snd[offset + 2] = (byte) (port >> 8);
            snd[offset + 3] = (byte) port;

            try {
                this.packetSnd = new DatagramPacket(snd, snd.length, config.getMulticastSocketAddress());
            } catch (SocketException e) {
                throw new JournalNetworkException("Cannot create send packet. Should never occur", e);
            }

            running.set(true);
            this.future = executor.submit(this);
            this.executor.shutdown();
        }
    }

    public void halt() {
        if (isRunning()) {
            running.set(false);
            socket.close();
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.debug(e);
            }
        }
    }
}
