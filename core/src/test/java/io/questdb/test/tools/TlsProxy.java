/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.tools;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class TlsProxy {
    private final String dstHost;
    private final int dstPort;
    private final String keystore;
    private final char[] keystorePassword;
    private final Set<Link> links = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Thread acceptorThread;
    private volatile boolean killAfterAccepting;
    private ServerSocket serverSocket;
    private volatile boolean shutdownRequested;

    public TlsProxy(String dstHost, int dstPort, String keystore, char[] keystorePassword) {
        this.dstHost = dstHost;
        this.dstPort = dstPort;
        this.keystore = keystore;
        this.keystorePassword = keystorePassword;
    }

    public synchronized void killAfterAccepting() {
        killAfterAccepting = true;
    }

    public synchronized void killConnections() {
        Iterator<Link> iterator = links.iterator();
        while (iterator.hasNext()) {
            Link link = iterator.next();
            link.kill();
            iterator.remove();
        }
    }

    public int start() {
        return TestUtils.unchecked(() -> {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(KeyStore.getInstance(KeyStore.getDefaultType()));

            KeyStore myKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            myKeyStore.load(TlsProxy.class.getResourceAsStream(keystore), keystorePassword);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(myKeyStore, keystorePassword);
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
            serverSocket = factory.createServerSocket();
            serverSocket.bind(null);

            acceptorThread = new Thread(() -> acceptorLoop(serverSocket));
            acceptorThread.start();
            return serverSocket.getLocalPort();
        });
    }

    public synchronized void stop() {
        shutdownRequested = true;
        TestUtils.unchecked(() -> serverSocket.close());
        acceptorThread.interrupt();
        TestUtils.unchecked(() -> acceptorThread.join());
        for (Link link : links) {
            link.shutDown();
        }
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // whatever
            }
        }
    }

    private void acceptorLoop(ServerSocket socket) {
        while (!shutdownRequested) {
            Socket frontendSocket = null;
            Socket backendSocket;
            try {
                frontendSocket = socket.accept();
                backendSocket = SocketFactory.getDefault().createSocket(dstHost, dstPort);
            } catch (IOException e) {
                if (shutdownRequested) {
                    return;
                }
                closeQuietly(frontendSocket);
                continue;
            }
            synchronized (this) {
                if (shutdownRequested) {
                    closeQuietly(frontendSocket);
                    closeQuietly(backendSocket);
                    return;
                }
                if (killAfterAccepting) {
                    closeQuietly(frontendSocket);
                    closeQuietly(backendSocket);
                    continue;
                }
                Link link = new Link(frontendSocket, backendSocket);
                links.add(link);
                link.start();
            }
        }
    }

    private static class Link {
        private final Socket backend;
        private final Pump backendToFrontend;
        private final Socket frontend;
        private final Pump frontendToBackend;

        private Link(Socket frontend, Socket backend) {
            AtomicInteger race = new AtomicInteger(2);
            this.frontend = frontend;
            this.backend = backend;
            frontendToBackend = TestUtils.unchecked(() -> new Pump(frontend.getInputStream(), backend.getOutputStream(), race, "front->backend"));
            backendToFrontend = TestUtils.unchecked(() -> new Pump(backend.getInputStream(), frontend.getOutputStream(), race, "backend->frontend"));
        }

        private void kill() {
            closeQuietly(frontend);
            closeQuietly(backend);
        }

        private void shutDown() {
            closeQuietly(frontend);
            closeQuietly(backend);
            frontendToBackend.shutdown();
            backendToFrontend.shutdown();
        }

        private void start() {
            Thread frontToBackThread = new Thread(frontendToBackend);
            frontToBackThread.setName("front-to-back");
            frontendToBackend.setOwningThread(frontToBackThread);
            frontToBackThread.start();
            Thread backToFrontThread = new Thread(backendToFrontend);
            backToFrontThread.setName("back-to-front");
            backendToFrontend.setOwningThread(backToFrontThread);
            backToFrontThread.start();
        }
    }

    private static final class Pump implements Runnable {
        private final InputStream from;
        private final String name;
        private final AtomicInteger race;
        private final OutputStream to;
        private volatile Thread owningThread;
        private volatile boolean shutdownRequested;

        private Pump(InputStream from, OutputStream to, AtomicInteger race, String name) {
            this.from = from;
            this.to = to;
            this.race = race;
            this.name = name;
        }

        @Override
        public void run() {
            byte[] buffer = new byte[1024];
            long totalRead = 0;
            long totalWritten = 0;
            while (!shutdownRequested) {
                int i;
                try {
                    i = from.read(buffer);
                    if (i < 0) {
                        break;
                    }
                    totalRead += i;
                } catch (IOException e) {
                    break;
                }
                try {
                    to.write(buffer, 0, i);
                    to.flush();
                    totalWritten += i;
                } catch (IOException e) {
                    break;
                }
            }
            try {
                to.flush();
            } catch (IOException e) {
                // already closed, no problem
            }
            System.out.println(name + "Total read: " + totalRead + ", Total written: " + totalWritten);
            if (race.decrementAndGet() == 0) {
                closeQuietly(from);
                closeQuietly(to);
            }
        }

        public void setOwningThread(Thread owningThread) {
            this.owningThread = owningThread;
        }

        private void shutdown() {
            shutdownRequested = true;
            owningThread.interrupt();
            TestUtils.unchecked(() -> owningThread.join());
        }
    }
}
