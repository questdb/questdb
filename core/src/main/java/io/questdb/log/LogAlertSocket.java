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
import java.util.function.Consumer;

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
    private static final int HOSTS_LIMIT = 12; // will never happen
    private static final int FAIL_OVER_LIMIT = 100;


    private final Rnd rand;
    private final FilesFacade ff;
    private final String[] hosts = new String[HOSTS_LIMIT];
    private final int[] ports = new int[HOSTS_LIMIT];
    private int hostPortSize;
    private int hostPortIdx;
    private int outBufferSize;
    private long outBufferPtr;
    private int inBufferSize;
    private long inBufferPtr;
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;
    private String socketAddress; // host[:port](,host[:port])*

    public LogAlertSocket(FilesFacade ff, String socketAddress) {
        this(ff, socketAddress, IN_BUFFER_SIZE, OUT_BUFFER_SIZE);
    }

    public LogAlertSocket(FilesFacade ff, String socketAddress, int outBufferSize) {
        this(ff, socketAddress, IN_BUFFER_SIZE, outBufferSize);
    }

    public LogAlertSocket(FilesFacade ff, String socketAddress, int inBufferSize, int outBufferSize) {
        this.ff = ff;
        this.rand = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());
        this.socketAddress = socketAddress;
        parseSocketAddress();
        this.inBufferSize = inBufferSize;
        this.inBufferPtr = Unsafe.malloc(inBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.outBufferSize = outBufferSize;
        this.outBufferPtr = Unsafe.malloc(outBufferSize, MemoryTag.NATIVE_DEFAULT);
    }

    public void connect() {
        fdSocketAddress = Net.sockaddr(hosts[hostPortIdx], ports[hostPortIdx]);
        fdSocket = Net.socketTcp(true);
        System.out.printf("Connecting with: %s:%d%n", hosts[hostPortIdx], ports[hostPortIdx]);
        if (fdSocket > -1) {
            if (Net.connect(fdSocket, fdSocketAddress) != 0) {
                System.out.println(" E could not connect");
                freeSocket();
            }
        } else {
            System.out.println(" E could not create TCP socket [errno=" + ff.errno() + "]");
            freeSocket();
        }
    }

    public void send(int len) {
        send(len, 0, null);
    }

    public void send(int len, Consumer<String> ackReceiver) {
        send(len, 0, ackReceiver);
    }

    @Override
    public void close() {
        if (fdSocket != -1) {
            freeSocket();
        }
        if (outBufferPtr != 0) {
            Unsafe.free(outBufferPtr, outBufferSize, MemoryTag.NATIVE_DEFAULT);
            outBufferPtr = 0;
        }
        if (inBufferPtr != 0) {
            Unsafe.free(inBufferPtr, IN_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            inBufferPtr = 0;
        }
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

    public long getInBufferSize() {
        return inBufferSize;
    }

    private void send(int len, int failOverLevel, Consumer<String> ackReceiver) {
        if (fdSocket > 0) {
            //
            int remaining = len;
            long p = outBufferPtr;
            boolean sendFail = false;
            while (remaining > 0) {
                int n = Net.send(fdSocket, p, remaining);
                if (n > 0) {
                    remaining -= n;
                    p += n;
                } else {
                    System.out.println("could not send [n=" + n + " [errno=" + ff.errno() + "]");
                    sendFail = true;
                }
            }
            if (sendFail) {
                failOver(len, failOverLevel + 1, ackReceiver);
            } else {
                // receive ack
                p = inBufferPtr;
                final int n = Net.recv(fdSocket, p, inBufferSize);
                if (n > 0) {
                    if (ackReceiver != null) {
                        ackReceiver.accept(Chars.stringFromUtf8Bytes(inBufferPtr, inBufferPtr + n));
                    } else {
                        Net.dumpAscii(p, n);
                    }
                } else {
                    failOver(len, failOverLevel + 1, ackReceiver);
                }
            }
        } else {
            failOver(len, failOverLevel + 1, ackReceiver);
        }
    }

    private void failOver(int len, int failOverLevel, Consumer<String> ackReceiver) {
        freeSocket();
        hostPortIdx = (hostPortIdx + 1) % hostPortSize;
        if (failOverLevel < FAIL_OVER_LIMIT) {
            System.out.printf("Failing over to: %d%n", hostPortIdx);
            connect();
            send(len, failOverLevel, ackReceiver);
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
    int getNumberOfHosts() {
        return hostPortSize;
    }

    @VisibleForTesting
    void freeSocket() {
        if (fdSocketAddress != -1) {
            Net.freeSockAddr(fdSocketAddress);
            fdSocketAddress = -1;
        }
        if (fdSocket != -1) {
            Net.close(fdSocket);
            fdSocket = -1;
        }
    }

    private void parseSocketAddress() {
        if (socketAddress == null) {
            setDefaultHostPort();
            return;
        }

        if (Chars.isQuoted(socketAddress)) {
            socketAddress = socketAddress.subSequence(1, socketAddress.length() - 1).toString();
        }
        socketAddress = socketAddress.trim();
        final int len = socketAddress.length();
        if (len == 0) {
            setDefaultHostPort();
            return;
        }

        // expected format: host[:port](,host[:port])*
        int hostIdx = 0;
        int portIdx = -1;
        for (int i = 0; i < len; ++i) {
            char c = socketAddress.charAt(i);
            switch (c) {
                case ':':
                    if (portIdx != -1) {
                        throw new LogError(String.format(
                                "Unexpected ':' found at position %d: %s",
                                i,
                                socketAddress));
                    }
                    portIdx = i;
                    break;

                case ',':
                    setHostPort(hostIdx, portIdx, i);
                    hostIdx = i + 1;
                    portIdx = -1;
                    break;
            }
        }
        setHostPort(hostIdx, portIdx, len);
        hostPortIdx = rand.nextInt(hostPortSize);
    }

    private void setDefaultHostPort() {
        hosts[hostPortIdx] = DEFAULT_HOST;
        ports[hostPortIdx] = DEFAULT_PORT;
        socketAddress = DEFAULT_HOST + ":" + DEFAULT_PORT;
        hostPortIdx = 0;
        hostPortSize = 1;
    }

    private void setHostPort(int hostIdx, int portLimit, int hostLimit) {
        // host0:port0, host1 : port1 , ..., host9:port9
        //              ^     ^       ^
        //              |     |       hostLimit
        //              |     portLimit
        //              hostIdx

        String host;
        boolean resolved = false;
        if (portLimit == -1) { // no ':' was found
            host = socketAddress.substring(hostIdx, hostLimit).trim();
            if (host.isEmpty()) {
                hosts[hostPortSize] = DEFAULT_HOST;
                resolved = true;
            }
            ports[hostPortSize] = DEFAULT_PORT;
        } else {
            host = socketAddress.substring(hostIdx, portLimit).trim();
            if (host.isEmpty()) {
                hosts[hostPortSize] = DEFAULT_HOST;
                resolved = true;
            }
            String port = socketAddress.substring(portLimit + 1, hostLimit).trim();
            if (port.isEmpty()) {
                ports[hostPortSize] = DEFAULT_PORT;
            } else {
                try {
                    ports[hostPortSize] = Numbers.parseInt(port);
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
        if (!resolved) {
            try {
                hosts[hostPortSize] = InetAddress.getByName(host).getHostAddress();
            } catch (UnknownHostException e) {
                throw new LogError(String.format(
                        "Invalid host value [%s] at position %d for socketAddress: %s",
                        host,
                        hostIdx,
                        socketAddress
                ));
            }
        }
        hostPortSize++;
    }
}
