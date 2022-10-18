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
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class TlsProxy {
    private final String dstHost;
    private final int dstPort;
    private final String keystore;
    private final char[] keystorePassword;
    private volatile boolean shutdownRequested;
    private final Set<Link> links = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Thread acceptorThread;
    private ServerSocket serverSocket;

    public TlsProxy(String dstHost, int dstPort, String keystore, char[] keystorePassword) {
        this.dstHost = dstHost;
        this.dstPort = dstPort;
        this.keystore = keystore;
        this.keystorePassword = keystorePassword;
    }

    public int start() {
        try {
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
        } catch (NoSuchAlgorithmException | IOException | KeyManagementException | CertificateException |
                 KeyStoreException | UnrecoverableKeyException e) {
            throw new RuntimeException(e);
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
                Link link = new Link(frontendSocket, backendSocket);
                links.add(link);
                link.start();
            }
        }
    }

    public synchronized void stop() {
        shutdownRequested = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        acceptorThread.interrupt();
        try {
            acceptorThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        for (Link link : links) {
            link.shutDown();
        }
    }

    private static class Link {
        private final Pump frontendToBackend;
        private final Pump backendToFrontend;

        private Link(Socket frontend, Socket backend) {
            AtomicInteger race = new AtomicInteger(2);
            try {
                frontendToBackend = new Pump(frontend.getInputStream(), backend.getOutputStream(), race, "front->backend");
                backendToFrontend = new Pump(backend.getInputStream(), frontend.getOutputStream(), race, "backend->frontend");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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

        private void shutDown() {
            frontendToBackend.shutdown();
            backendToFrontend.shutdown();
        }
    }

    private static final class Pump implements Runnable {
        private final InputStream from;
        private final OutputStream to;
        private final String name;
        private volatile boolean shutdownRequested;
        private volatile Thread owningThread;
        private final AtomicInteger race;

        private Pump(InputStream from, OutputStream to, AtomicInteger race, String name) {
            this.from = from;
            this.to = to;
            this.race = race;
            this.name = name;
        }

        public void setOwningThread(Thread owningThread) {
            this.owningThread = owningThread;
        }

        private void shutdown() {
            shutdownRequested = true;
            owningThread.interrupt();
            try {
                owningThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
}
