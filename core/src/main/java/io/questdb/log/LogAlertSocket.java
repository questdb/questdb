/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.log;

import io.questdb.VisibleForTesting;
import io.questdb.network.Net;
import io.questdb.std.*;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class LogAlertSocket implements Closeable {

    public static final String localHostIp;

    static {
        try {
            localHostIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new LogError("Cannot access our ip address info");
        }
    }

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9093;
    public static final int IN_BUFFER_SIZE = 2 * 1024 * 1024;
    public static final int OUT_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final int MAX_HOSTS = 12; // will never happen


    private final Rnd rand;
    private final FilesFacade ff;
    private final String[] hosts = new String[MAX_HOSTS];
    private final int[] ports = new int[MAX_HOSTS];
    private int hostPortLimit;
    private int currentHostPortIdx;
    private int outBufferSize;
    private long outBufferPtr;
    private long outBufferLimit;
    private final int inBufferSize = IN_BUFFER_SIZE;
    private long inBufferPtr;
    private long inBufferLimit;
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;
    private String socketAddress; // host[:port](,host[:port])*


    public LogAlertSocket(FilesFacade ff, String socketAddress, int outBufferSize) {
        this.ff = ff;
        this.rand = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());
        this.outBufferSize = outBufferSize;
        this.outBufferPtr = Unsafe.malloc(outBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.outBufferLimit = outBufferPtr + outBufferSize;
        this.inBufferPtr = Unsafe.malloc(inBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.inBufferLimit = inBufferPtr + inBufferSize;
        this.socketAddress = socketAddress;
        parseSocketAddress();
    }

    public String getCurrentHost() {
        return hosts[currentHostPortIdx];
    }

    public int getCurrentPort() {
        return ports[currentHostPortIdx];
    }

    public long getOutBufferPtr() {
        return outBufferPtr;
    }

    public int getOutBufferSize() {
        return outBufferSize;
    }

    public long getInBufferPtr() {
        return inBufferPtr;
    }

    public int getInBufferSize() {
        return inBufferSize;
    }

    public long getInBufferLimit() {
        return inBufferLimit;
    }

    public void connectSocket() {
        fdSocketAddress = Net.sockaddr(hosts[currentHostPortIdx], ports[currentHostPortIdx]);
        fdSocket = Net.socketTcp(true);
        if (fdSocket > -1) {
            if (Net.connect(fdSocket, fdSocketAddress) != 0) {
                System.out.println(" E could not connect to");
                freeSocket();
            }
        } else {
            System.out.println(" E could not create TCP socket [errno=" + ff.errno() + "]");
            freeSocket();
        }
    }

    public void send(int len) {
        if (fdSocket > 0) {
            int remaining = len;
            long p = outBufferPtr;
            while (remaining > 0) {
                int n = Net.send(fdSocket, p, remaining);
                if (n > 0) {
                    remaining -= n;
                    p += n;
                } else {
                    System.out.println("could not send [n=" + n + " [errno=" + ff.errno() + "]");
                }
            }

            // receive ack
            p = inBufferPtr;
            int n = Net.recv(fdSocket, p, inBufferSize);
            Net.dumpAscii(p, n);
        }
    }

    @Override
    public void close() {
        if (outBufferPtr != 0) {
            Unsafe.free(outBufferPtr, outBufferSize, MemoryTag.NATIVE_DEFAULT);
            outBufferPtr = 0;
            outBufferLimit = 0;
        }
        if (inBufferPtr != 0) {
            Unsafe.free(inBufferPtr, IN_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            inBufferPtr = 0;
            inBufferLimit = 0;
        }
        if (fdSocket != -1) {
            freeSocket();
        }
    }

    @VisibleForTesting
    String getSocketAddress() {
        return socketAddress;
    }

    @VisibleForTesting
    String[] getHosts() {
        return hosts;
    }

    @VisibleForTesting
    int[] getPorts() {
        return ports;
    }

    @VisibleForTesting
    int getHostPortLimit() {
        return hostPortLimit;
    }

    @VisibleForTesting
    int getCurrentHostPortIdx() {
        return currentHostPortIdx;
    }

    private void freeSocket() {
        Net.freeSockAddr(fdSocketAddress);
        fdSocketAddress = -1;
        Net.close(fdSocket);
        fdSocket = -1;
    }

    private void parseSocketAddress() {
        if (socketAddress == null) {
            hosts[currentHostPortIdx] = DEFAULT_HOST;
            ports[currentHostPortIdx] = DEFAULT_PORT;
            socketAddress = DEFAULT_HOST + ":" + DEFAULT_PORT;
            currentHostPortIdx = 0;
            hostPortLimit = 1;
            return;
        }
        if (Chars.isQuoted(socketAddress)) {
            socketAddress = socketAddress.subSequence(1, socketAddress.length() - 1).toString();
        }
        socketAddress = socketAddress.trim();
        final int len = socketAddress.length();
        if (len == 0) {
            hosts[currentHostPortIdx] = DEFAULT_HOST;
            ports[currentHostPortIdx] = DEFAULT_PORT;
            socketAddress = DEFAULT_HOST + ":" + DEFAULT_PORT;
            currentHostPortIdx = 0;
            hostPortLimit = 1;
            return;
        }
        // expected format: host[:port](,host[:port])*
        int hostIdx = 0;
        int portIdx = -1;
        for (int i = 0; i < len; ++i) {
            char c = socketAddress.charAt(i);
            switch (c) {
                case ':':
                    portIdx = i;
                    break;

                case ',':
                    setHostPort(hostIdx, portIdx, i);
                    hostIdx = i + 1;
                    portIdx = -1;
                    hostPortLimit++;
                    break;
            }
        }
        setHostPort(hostIdx, portIdx, len);
        hostPortLimit++;
        currentHostPortIdx = rand.nextInt(hostPortLimit);
    }

    private void setHostPort(int hostIdx, int portLimit, int hostLimit) {
        String host;
        if (portLimit == -1) {
            host = socketAddress.substring(hostIdx, hostLimit).trim();
            if (host.isEmpty()) {
                host = DEFAULT_HOST;
            }
            ports[hostPortLimit] = DEFAULT_PORT;
        } else {
            host = socketAddress.substring(hostIdx, portLimit).trim();
            if (host.isEmpty()) {
                host = DEFAULT_HOST;
            }
            String port = socketAddress.substring(portLimit + 1, hostLimit).trim();
            if (port.isEmpty()) {
                ports[hostPortLimit] = DEFAULT_PORT;
            } else {
                try {
                    ports[hostPortLimit] = Numbers.parseInt(port);
                } catch (NumericException e) {
                    throw new LogError(String.format(
                            "Invalid port value [%s] at position %d for socketAddress: %s",
                            port,
                            portLimit + 1,
                            socketAddress
                    ));
                }
            }
        }
        try {
            hosts[hostPortLimit] = InetAddress.getByName(host).getHostAddress();
        } catch (UnknownHostException e) {
            throw new LogError(String.format(
                    "Invalid host value [%s] at position %d for socketAddress: %s",
                    host,
                    hostIdx,
                    socketAddress
            ));
        }
    }
}
