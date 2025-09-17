/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.network.NetworkFacade;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.LockSupport;

public class LogAlertSocket implements Closeable {

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 9093;
    public static final int IN_BUFFER_SIZE = 2 * 1024 * 1024;
    public static final int OUT_BUFFER_SIZE = 4 * 1024 * 1024;
    public static final long RECONNECT_DELAY_NANO = 250_000_000; // 1/4th sec
    public static final String localHostIp;
    private static final int HOSTS_LIMIT = 12;
    private final String[] alertHosts = new String[HOSTS_LIMIT]; // indexed by alertHostIdx < alertHostsCount
    private final int[] alertPorts = new int[HOSTS_LIMIT]; // indexed by alertHostIdx < alertHostsCount
    private final String defaultHost;
    private final int defaultPort;
    private final int inBufferSize;
    private final Log log;
    private final NetworkFacade nf;
    private final int outBufferSize;
    private final Rnd rand;
    private final long reconnectDelay;
    private final Runnable onReconnectRef = this::onReconnect;
    private final StringSink responseSink = new StringSink();
    private long addressInfoAddr = -1; // tcp/ip host:port address
    private int alertHostIdx;
    private int alertHostsCount;
    private String alertTargets; // host[:port](,host[:port])*
    private long inBufferPtr;
    private long outBufferPtr;
    private long socketFd = -1;

    public LogAlertSocket(NetworkFacade nf, String alertTargets, Log log) {
        this(
                nf,
                alertTargets,
                IN_BUFFER_SIZE,
                OUT_BUFFER_SIZE,
                RECONNECT_DELAY_NANO,
                DEFAULT_HOST,
                DEFAULT_PORT,
                log
        );
    }

    public LogAlertSocket(
            NetworkFacade nf,
            String alertTargets,
            int inBufferSize,
            int outBufferSize,
            long reconnectDelay,
            String defaultHost,
            int defaultPort,
            Log log
    ) {
        this.nf = nf;
        this.log = log;
        this.rand = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
        this.alertTargets = alertTargets;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        parseAlertTargets();
        this.inBufferSize = inBufferSize;
        this.inBufferPtr = Unsafe.malloc(inBufferSize, MemoryTag.NATIVE_LOGGER);
        this.outBufferSize = outBufferSize;
        this.outBufferPtr = Unsafe.malloc(outBufferSize, MemoryTag.NATIVE_LOGGER);
        this.reconnectDelay = reconnectDelay;
    }

    @Override
    public void close() {
        freeSocketAndAddress();
        if (outBufferPtr != 0) {
            Unsafe.free(outBufferPtr, outBufferSize, MemoryTag.NATIVE_LOGGER);
            outBufferPtr = 0;
        }
        if (inBufferPtr != 0) {
            Unsafe.free(inBufferPtr, inBufferSize, MemoryTag.NATIVE_LOGGER);
            inBufferPtr = 0;
        }
    }

    public void connect() {
        addressInfoAddr = nf.getAddrInfo(alertHosts[alertHostIdx], alertPorts[alertHostIdx]);
        if (addressInfoAddr == -1) {
            logNetworkConnectError("Could not create addr info with");
        } else {
            socketFd = nf.socketTcp(true);
            nf.configureKeepAlive(socketFd);
            if (socketFd > -1) {
                if (nf.connectAddrInfo(socketFd, addressInfoAddr) != 0) {
                    logNetworkConnectError("Could not connect with");
                    freeSocketAndAddress();
                }
            } else {
                logNetworkConnectError("Could not create TCP socket with");
                freeSocketAndAddress();
            }
        }
    }

    @TestOnly
    public String[] getAlertHosts() {
        return alertHosts;
    }

    @TestOnly
    public int getAlertHostsCount() {
        return alertHostsCount;
    }

    @TestOnly
    public int[] getAlertPorts() {
        return alertPorts;
    }

    @TestOnly
    public String getAlertTargets() {
        return alertTargets;
    }

    @TestOnly
    public String getDefaultAlertHost() {
        return defaultHost;
    }

    @TestOnly
    public int getDefaultAlertPort() {
        return defaultPort;
    }

    public long getInBufferPtr() {
        return inBufferPtr;
    }

    public int getInBufferSize() {
        return inBufferSize;
    }

    public long getOutBufferPtr() {
        return outBufferPtr;
    }

    public int getOutBufferSize() {
        return outBufferSize;
    }

    @TestOnly
    public void logResponse(int len) {
        responseSink.clear();
        Utf8s.utf8ToUtf16(inBufferPtr, inBufferPtr + len, responseSink);
        final int responseLen = responseSink.length();
        int contentLength = 0;
        int lineStart = 0;
        int colonIdx = -1;
        boolean headerEndFound = false;
        for (int i = 0; i < responseLen; i++) {
            switch (responseSink.charAt(i)) {
                case ':':
                    if (colonIdx == -1) { // values may contain ':', e.g. Date: Thu, 09 Dec 2021 09:37:22 GMT
                        colonIdx = i;
                    }
                    break;

                case '\n':
                    if (colonIdx != -1) {
                        if (isContentLength(responseSink, lineStart, colonIdx)) {
                            int startSize = colonIdx + 1;
                            int limSize = i - 1;
                            while (startSize < responseLen && responseSink.charAt(startSize) == ' ') {
                                startSize++;
                            }
                            while (limSize > startSize) {
                                char c = responseSink.charAt(limSize);
                                if (c == '\r' || c == ' ') {
                                    limSize--;
                                } else {
                                    break;
                                }
                            }
                            try {
                                contentLength = Numbers.parseInt(responseSink, startSize, limSize + 1);
                            } catch (NumericException e) {
                                $currentAlertHost(log.info().$("Received")).$(": ").$(responseSink).$();
                                return;
                            }
                        }
                        colonIdx = -1;
                    } else if (i - lineStart == 1 && responseSink.charAt(i - 1) == '\r') {
                        lineStart = i + 1;
                        headerEndFound = true;
                        break; // for loop
                    }
                    lineStart = i + 1;
                    break;
            }
        }
        int start = headerEndFound && contentLength == responseLen - lineStart ? lineStart : 0;
        $currentAlertHost(log.info().$("Received"))
                .$(": ")
                .$safe(responseSink, start, responseLen)
                .$();
    }

    public boolean send(int len) {
        return send(len, onReconnectRef);
    }

    public boolean send(int len, Runnable onReconnect) {
        if (len < 1) {
            return false;
        }

        final int maxSendAttempts = 2 * alertHostsCount;
        int sendAttempts = maxSendAttempts; // empirical, say twice per host at most
        while (sendAttempts > 0) {
            if (socketFd > 0) {
                int remaining = len;
                long p = outBufferPtr;
                boolean sendFail = false;
                while (remaining > 0) {
                    int n = nf.sendRaw(socketFd, p, remaining);
                    if (n > 0) {
                        remaining -= n;
                        p += n;
                    } else {
                        $currentAlertHost(log.info().$("Could not send"))
                                .$(" [errno=").$(nf.errno())
                                .$(", size=").$(n)
                                .$(", log=").$safe(outBufferPtr, outBufferPtr + len).I$();
                        sendFail = true;
                        // do fail over, could not send
                        break;
                    }
                }
                if (!sendFail) {
                    // receive ack
                    p = inBufferPtr;
                    final int n = nf.recvRaw(socketFd, p, inBufferSize);
                    if (n > 0) {
                        logResponse(n);
                        break;
                    }
                    // do fail over, ack was not received
                }
            }

            // fail to the next host and attempt to send again
            freeSocketAndAddress();
            int alertHostIdx = this.alertHostIdx;
            this.alertHostIdx = (this.alertHostIdx + 1) % alertHostsCount;
            LogRecord logFailOver = $alertHost(
                    this.alertHostIdx,
                    $alertHost(
                            alertHostIdx,
                            log.info().$("Failing over from")
                    ).$(" to")
            );
            if (alertHostIdx == this.alertHostIdx) {
                logFailOver.$(" with a delay of ")
                        .$(reconnectDelay / 1000000)
                        .$(" millis (as it is the same alert manager)")
                        .$();
                onReconnect.run();
            } else {
                logFailOver.$();
            }
            connect();
            sendAttempts--;
        }
        boolean success = sendAttempts > 0;
        if (!success) {
            log.info()
                    .$("None of the configured alert managers are accepting alerts.\n")
                    .$("Giving up sending after ")
                    .$(maxSendAttempts)
                    .$(" attempts: [")
                    .$safe(outBufferPtr, outBufferPtr + len)
                    .I$();
        }
        return success;
    }

    private static boolean isContentLength(CharSequence tok, int lo, int hi) {
        return hi - lo > 13 &&
                (tok.charAt(lo++) | 32) == 'c' &&
                (tok.charAt(lo++) | 32) == 'o' &&
                (tok.charAt(lo++) | 32) == 'n' &&
                (tok.charAt(lo++) | 32) == 't' &&
                (tok.charAt(lo++) | 32) == 'e' &&
                (tok.charAt(lo++) | 32) == 'n' &&
                (tok.charAt(lo++) | 32) == 't' &&
                (tok.charAt(lo++) | 32) == '-' &&
                (tok.charAt(lo++) | 32) == 'l' &&
                (tok.charAt(lo++) | 32) == 'e' &&
                (tok.charAt(lo++) | 32) == 'n' &&
                (tok.charAt(lo++) | 32) == 'g' &&
                (tok.charAt(lo++) | 32) == 't' &&
                (tok.charAt(lo) | 32) == 'h';
    }

    private LogRecord $alertHost(int idx, LogRecord logRecord) {
        return logRecord.$(" [").$(idx).$("] ").$(alertHosts[idx]).$(':').$(alertPorts[idx]);
    }

    private LogRecord $currentAlertHost(LogRecord logRecord) {
        return $alertHost(alertHostIdx, logRecord);
    }

    private void freeSocketAndAddress() {
        if (addressInfoAddr != -1) {
            nf.freeAddrInfo(addressInfoAddr);
            addressInfoAddr = -1;
        }
        if (socketFd != -1) {
            nf.close(socketFd, log);
            socketFd = -1;
        }
    }

    private void logNetworkConnectError(CharSequence message) {
        $currentAlertHost(log.info().$(message)).$(" [errno=").$(nf.errno()).I$();
    }

    private void onReconnect() {
        LockSupport.parkNanos(reconnectDelay);
    }

    private void parseAlertTargets() {
        if (alertTargets == null || alertTargets.isEmpty()) {
            setDefaultHostPort();
            return;
        }
        int startIdx = 0;
        int endIdx = alertTargets.length();

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
                                alertTargets
                        ));
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
        $currentAlertHost(log.info().$("Added default alert manager")).$();
    }

    private void setHostPort(int hostIdx, int portLimit, int hostLimit) {
        // host0:port0, host1 : port1 , ..., host9:port9
        //              ^     ^       ^
        //              |     |       hostEnd
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
                                "Invalid port value [%s] at position %d for alertTargets: %s",
                                alertTargets.substring(portLimit + 1, hostLimit),
                                portLimit + 1,
                                alertTargets
                        ));
                    }
                }
                alertPorts[alertHostsCount] = port;
            }
        }
        LogRecord logRecord = log.info()
                .$("Added alert manager [")
                .$(alertHostsCount)
                .$("]: ");
        try {
            if (!hostResolved) {
                String host = alertTargets.substring(hostIdx, hostEnd).trim();
                try {
                    alertHosts[alertHostsCount] = InetAddress.getByName(host).getHostAddress();
                    logRecord.$(host).$(" (").$(alertHosts[alertHostsCount]).$(')');
                } catch (UnknownHostException e) {
                    throw new LogError(String.format(
                            "Invalid host value [%s] at position %d for alertTargets: %s",
                            host,
                            hostIdx,
                            alertTargets
                    ));
                }
            } else {
                logRecord.$(alertHosts[alertHostsCount]);
            }
            logRecord.$(':').$(alertPorts[alertHostsCount]);
            alertHostsCount++;
        } finally {
            logRecord.$();
        }
    }

    @TestOnly
    long getReconnectDelay() {
        return reconnectDelay;
    }

    static {
        try {
            localHostIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new LogError("Cannot access our ip address info");
        }
    }
}
