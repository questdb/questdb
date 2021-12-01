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
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class LogAlertSocket implements Closeable {

    public static final String NACK = "NACK";

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
    public static final long RECONNECT_DELAY_NANO = 250_000_000; // 1/4th sec
    private static final int HOSTS_LIMIT = 12;

    private final Rnd rand;
    private final NetworkFacade nf;
    private final String[] alertHosts = new String[HOSTS_LIMIT]; // indexed by alertHostIdx < alertHostsCount
    private final int[] alertPorts = new int[HOSTS_LIMIT]; // indexed by alertHostIdx < alertHostsCount
    private final int outBufferSize;
    private final int inBufferSize;
    private final long reconnectDelay;
    private final String defaultHost;
    private final int defaultPort;
    private long outBufferPtr;
    private long inBufferPtr;
    private int alertHostsCount;
    private int alertHostIdx;
    private long fdSocketAddress = -1; // tcp/ip host:port address
    private long fdSocket = -1;
    private String alertTargets; // host[:port](,host[:port])*

    public LogAlertSocket(String alertTargets) {
        this(
                NetworkFacadeImpl.INSTANCE,
                alertTargets,
                IN_BUFFER_SIZE,
                OUT_BUFFER_SIZE,
                RECONNECT_DELAY_NANO,
                DEFAULT_HOST,
                DEFAULT_PORT
        );
    }

    public LogAlertSocket(
            String alertTargets,
            int inBufferSize,
            int outBufferSize,
            long reconnectDelay,
            String defaultHost,
            int defaultPort
    ) {
        this(
                NetworkFacadeImpl.INSTANCE,
                alertTargets,
                inBufferSize,
                outBufferSize,
                reconnectDelay,
                defaultHost,
                defaultPort
        );
    }

    private LogAlertSocket(
            NetworkFacade nf,
            String alertTargets,
            int inBufferSize,
            int outBufferSize,
            long reconnectDelay,
            String defaultHost,
            int defaultPort
    ) {
        this.nf = nf;
        this.rand = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
        this.alertTargets = alertTargets;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        parseAlertTargets();
        this.inBufferSize = inBufferSize;
        this.inBufferPtr = Unsafe.malloc(inBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.outBufferSize = outBufferSize;
        this.outBufferPtr = Unsafe.malloc(outBufferSize, MemoryTag.NATIVE_DEFAULT);
        this.reconnectDelay = reconnectDelay;
    }

    public void connect() {
        fdSocketAddress = nf.sockaddr(alertHosts[alertHostIdx], alertPorts[alertHostIdx]);
        fdSocket = nf.socketTcp(true);
        System.out.printf(
                "Connecting with: %s:%d%n",
                alertHosts[alertHostIdx],
                alertPorts[alertHostIdx]
        );
        if (fdSocket > -1) {
            if (nf.connect(fdSocket, fdSocketAddress) != 0) {
                System.err.printf(
                        "%sCould not connect [errno=%d]%n",
                        LogLevel.ERROR_HEADER,
                        nf.errno()
                );
                freeSocketAndAddress();
            }
        } else {
            System.out.printf(
                    "%sCould not create TCP socket [errno=%d]",
                    LogLevel.ERROR_HEADER,
                    nf.errno()
            );
            freeSocketAndAddress();
        }
    }

    public boolean send(int len) {
        return send(len, null);
    }

    public boolean send(int len, Consumer<String> ackReceiver) {
        if (len < 1) {
            return false;
        }

        int sendAttempts = 2 * alertHostsCount; // empirical, say twice per host at most
        while (sendAttempts > 0) {
            if (fdSocket > 0) {
                int remaining = len;
                long p = outBufferPtr;
                boolean sendFail = false;
                while (remaining > 0) {
                    int n = nf.send(fdSocket, p, remaining);
                    if (n > 0) {
                        remaining -= n;
                        p += n;
                    } else {
                        System.err.printf(
                                "%sCould not send [errno=%d, n=%d]%n",
                                LogLevel.ERROR_HEADER,
                                nf.errno(),
                                n
                        );
                        sendFail = true;
                        // do fail over, could not send
                        break;
                    }
                }
                if (!sendFail) {
                    // receive ack
                    p = inBufferPtr;
                    final int n = nf.recv(fdSocket, p, inBufferSize);
                    if (n > 0) {
                        if (ackReceiver != null) {
                            ackReceiver.accept(Chars.stringFromUtf8Bytes(inBufferPtr, inBufferPtr + n));
                        } else {
                            System.out.printf("%sReceived:%s%n",
                                    LogLevel.INFO_HEADER,
                                    Chars.stringFromUtf8Bytes(inBufferPtr, inBufferPtr + n)
                            );
                        }
                        break;
                    }
                    // do fail over, ack was not received
                }
            }

            // fail to the next host and attempt to send again
            freeSocketAndAddress();
            int alertHostIdx = this.alertHostIdx;
            this.alertHostIdx = (this.alertHostIdx + 1) % alertHostsCount;
            if (alertHostIdx == this.alertHostIdx) {
                LockSupport.parkNanos(reconnectDelay);
            }
            connect();
            sendAttempts--;
        }

        boolean success = sendAttempts > 0;
        if (!success) {
            System.err.printf(
                    "%sNo alert hosts are available after %d attempts, giving up sending alert.%n",
                    LogLevel.ERROR_HEADER,
                    alertHostsCount
            );
            if (ackReceiver != null) {
                ackReceiver.accept(NACK);
            }
        }
        return success;
    }

    @Override
    public void close() {
        freeSocketAndAddress();
        if (outBufferPtr != 0) {
            Unsafe.free(outBufferPtr, outBufferSize, MemoryTag.NATIVE_DEFAULT);
            outBufferPtr = 0;
        }
        if (inBufferPtr != 0) {
            Unsafe.free(inBufferPtr, inBufferSize, MemoryTag.NATIVE_DEFAULT);
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

    public int getInBufferSize() {
        return inBufferSize;
    }

    @VisibleForTesting
    String getAlertTargets() {
        return alertTargets;
    }

    @VisibleForTesting
    String[] getAlertHosts() {
        return alertHosts;
    }

    @VisibleForTesting
    int[] getAlertPorts() {
        return alertPorts;
    }

    @VisibleForTesting
    int getAlertHostsCount() {
        return alertHostsCount;
    }

    @VisibleForTesting
    long getReconnectDelay() {
        return reconnectDelay;
    }

    @VisibleForTesting
    String getDefaultAlertHost() {
        return defaultHost;
    }

    @VisibleForTesting
    int getDefaultAlertPort() {
        return defaultPort;
    }

    private void freeSocketAndAddress() {
        if (fdSocketAddress != -1) {
            Net.freeSockAddr(fdSocketAddress);
            fdSocketAddress = -1;
        }
        if (fdSocket != -1) {
            Net.close(fdSocket);
            fdSocket = -1;
        }
    }

    private void parseAlertTargets() {
        if (alertTargets == null || alertTargets.isEmpty()) {
            setDefaultHostPort();
            return;
        }
        int startIdx = 0;
        int endIdx = alertTargets.length();
        if (endIdx == 0) {
            setDefaultHostPort();
            return;
        }

        if (Chars.isQuoted(alertTargets)) {
            startIdx++;
            endIdx--;
        }
        while (alertTargets.charAt(startIdx) == ' ' && startIdx < endIdx - 1) {
            startIdx++;
        }
        while (alertTargets.charAt(endIdx - 1) == ' ' && endIdx > startIdx) {
            endIdx--;
        }
        final int len = endIdx - startIdx;
        if (len == 0) {
            setDefaultHostPort();
            return;
        }

        // expected format: host[:port](,host[:port])*
        int hostIdx = startIdx;
        int portIdx = -1;
        for (int i = startIdx; i < endIdx; ++i) {
            char c = alertTargets.charAt(i);
            switch (c) {
                case ':':
                    if (portIdx != -1) {
                        throw new LogError(String.format(
                                "Unexpected ':' found at position %d: %s",
                                i,
                                alertTargets));
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
        alertHostIdx = rand.nextInt(alertHostsCount);
    }

    private void setDefaultHostPort() {
        alertHosts[alertHostIdx] = defaultHost;
        alertPorts[alertHostIdx] = defaultPort;
        alertTargets = defaultHost + ":" + defaultPort;
        alertHostIdx = 0;
        alertHostsCount = 1;
    }

    private void setHostPort(int hostIdx, int portLimit, int hostLimit) {
        // host0:port0, host1 : port1 , ..., host9:port9
        //              ^     ^       ^
        //              |     |       hostLimit
        //              |     portLimit
        //              hostIdx

        boolean hostResolved = false;
        int hostEnd = hostLimit;
        if (portLimit == -1) { // no ':' was found
            if (hostIdx + 1 > hostLimit) {
                alertHosts[alertHostsCount] = defaultHost;
                hostResolved = true;
            }
            alertPorts[alertHostsCount] = defaultPort;
        } else {
            if (hostIdx + 1 > portLimit) {
                alertHosts[alertHostsCount] = defaultHost;
                hostResolved = true;
            } else {
                hostEnd = portLimit;
            }
            if (portLimit + 2 > hostLimit) {
                alertPorts[alertHostsCount] = defaultPort;
            } else {
                int port = 0;
                int scale = 1;
                for (int i = hostLimit - 1; i > portLimit; i--) {
                    int c = alertTargets.charAt(i) - '0';
                    if (c > -1 && c < 10) {
                        port += c * scale;
                        scale *= 10;
                    } else {
                        throw new LogError(String.format(
                                "Invalid port value [%s] at position %d for socketAddress: %s",
                                alertTargets.substring(portLimit + 1, hostLimit),
                                portLimit + 1,
                                alertTargets
                        ));
                    }
                }
                alertPorts[alertHostsCount] = port;
            }
        }
        if (!hostResolved) {
            String host = alertTargets.substring(hostIdx, hostEnd).trim();
            try {
                alertHosts[alertHostsCount] = InetAddress.getByName(host).getHostAddress();
            } catch (UnknownHostException e) {
                throw new LogError(String.format(
                        "Invalid host value [%s] at position %d for socketAddress: %s",
                        host,
                        hostIdx,
                        alertTargets
                ));
            }
        }
        alertHostsCount++;
    }
}
