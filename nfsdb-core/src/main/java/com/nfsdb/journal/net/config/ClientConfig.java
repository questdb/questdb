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

package com.nfsdb.journal.net.config;

import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.logging.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.*;

public class ClientConfig extends NetworkConfig {

    public static final Logger LOGGER = Logger.getLogger(ClientConfig.class);
    public static final ClientConfig INSTANCE = new ClientConfig() {{
        setHostname("127.0.0.1");
    }};
    private int soSndBuf = 8192;
    private boolean keepAlive = true;
    private boolean tcpNoDelay = true;
    private int linger = -1;

    public int getSoSndBuf() {
        return soSndBuf;
    }

    public void setSoSndBuf(int soSndBuf) {
        this.soSndBuf = soSndBuf;
    }

    public boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getLinger() {
        return linger;
    }

    public void setLinger(int linger) {
        this.linger = linger;
    }

    public SocketChannel openSocketChannel() throws JournalNetworkException {
        String host = getHostname();
        InetSocketAddress address;
        ServerInformation serverInformation = null;

        if (host == null) {
            serverInformation = lookupServerInformation();
            address = serverInformation.address;
        } else {
            address = new InetSocketAddress(host, getPort());
        }
        try {
            SocketChannel channel;
            if (!getSslConfig().isSecure() && (serverInformation == null || !serverInformation.ssl)) {
                channel = SocketChannel.open(address);
            } else {
                channel = getSslConfig().getSslContext().getSocketFactory().createSocket(address.getHostName(), address.getPort()).getChannel();
            }

            channel.socket().setSoTimeout(getSoTimeout());
            channel.socket().setTcpNoDelay(isTcpNoDelay());
            channel.socket().setKeepAlive(getKeepAlive());
            channel.socket().setSendBufferSize(getSoSndBuf());
            if (channel.socket().getSendBufferSize() != getSoSndBuf()) {
                LOGGER.warn("SO_SNDBUF value is ignored");
            }
            channel.socket().setReceiveBufferSize(getSoRcvBuf());
            if (channel.socket().getReceiveBufferSize() != getSoRcvBuf()) {
                LOGGER.warn("SO_RCVBUF value is ignored");
            }
            channel.socket().setReuseAddress(isReuseAddress());
            channel.socket().setSoLinger(getLinger() > -1, getLinger());
            LOGGER.info("Connected to %s", address);
            return channel;
        } catch (UnresolvedAddressException e) {
            throw new JournalNetworkException("DNS lookup error: %s", address);
        } catch (IOException e) {
            throw new JournalNetworkException("%s: %s", address, e.getMessage());
        }
    }

    public ServerInformation lookupServerInformation() throws JournalNetworkException {
        final ExecutorService executor = Executors.newFixedThreadPool(1, new NamedDaemonThreadFactory("jj-client-multicast", true));
        final MulticastSocket socket = openMulticastSocket();
        final DatagramPacket packetSnd;
        try {
            packetSnd = new DatagramPacket(new byte[]{ADDRESS_REQUEST_PREFIX}, 1, getMulticastSocketAddress());
        } catch (SocketException e) {
            throw new JournalNetworkException("Cannot create send packet. Should never occur", e);
        }
        final DatagramPacket packetRcv = new DatagramPacket(new byte[10], 10);

        Future<ServerInformation> future = executor.submit(new Callable<ServerInformation>() {
            @Override
            public ServerInformation call() throws Exception {
                while (true) {
                    socket.receive(packetRcv);
                    byte[] data = packetRcv.getData();

                    switch (data[0]) {
                        case ADDRESS_RESPONSE_PREFIX:
                            ServerInformation info = new ServerInformation();
                            //  server address
                            String host = Integer.toString(data[1] & 0xFF) + "."
                                    + Integer.toString(data[2] & 0xFF) + "."
                                    + Integer.toString(data[3] & 0xFF) + "."
                                    + Integer.toString(data[4] & 0xFF);
                            info.ssl = data[5] == 1;
                            int offset = 6;
                            int port = data[offset] << 24 | (data[offset + 1] & 0xFF) << 16 | (data[offset + 2] & 0xFF) << 8 | (data[offset + 3] & 0xFF);
                            info.address = new InetSocketAddress(host, port);
                            LOGGER.debug("Received: %s:%d", host, port);
                            return info;
                    }
                }
            }
        });


        ServerInformation info = null;
        int tryCount = 6;
        while (tryCount > 0) {
            try {
                LOGGER.debug("Asking");
                socket.send(packetSnd);
                info = future.get(1, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException e) {
                tryCount--;
            } catch (Exception e) {
                throw new JournalNetworkException("Server address lookup failed", e);
            }
        }

        executor.shutdown();
        socket.close();

        if (info == null) {
            throw new JournalNetworkException("No servers found on network");
        }
        return info;
    }

    public static class ServerInformation {
        InetSocketAddress address;
        boolean ssl;
    }
}
