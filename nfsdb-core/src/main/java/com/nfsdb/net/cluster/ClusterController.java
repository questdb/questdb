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

package com.nfsdb.net.cluster;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.logging.Logger;
import com.nfsdb.net.JournalClient;
import com.nfsdb.net.JournalServer;
import com.nfsdb.net.config.ClientConfig;
import com.nfsdb.net.config.ServerConfig;
import com.nfsdb.net.config.ServerNode;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterController {

    private final Logger LOGGER = Logger.getLogger(ClusterController.class);

    private final Runnable up = new Runnable() {
        @Override
        public void run() {
            try {
                up();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    };
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int instance;
    private final ClusterStatusListener listener;
    private final JournalFactory factory;
    private final List<JournalWriter> writers;
    private final ExecutorService service = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public
        @Nonnull
        Thread newThread(@Nonnull Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("nfsdb-cluster-controller-");
            return thread;
        }
    });
    private final ServerConfig serverConfig;
    private final ClientConfig clientConfig;
    private JournalClient client;
    private JournalServer server;

    public ClusterController(
            ServerConfig serverConfig
            , ClientConfig clientConfig
            , JournalFactory factory
            , int instance
            , List<JournalWriter> writers
            , ClusterStatusListener listener
    ) {
        this.serverConfig = serverConfig;
        this.clientConfig = clientConfig;
        this.factory = factory;
        this.instance = instance;
        this.writers = writers;
        this.listener = listener;
        for (ServerNode node : serverConfig.nodes()) {
            if (node.getId() != instance) {
                clientConfig.addNode(node);
            }
        }
    }

    public void halt() throws JournalNetworkException {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        listener.onShutdown();
        service.shutdown();
        if (client != null) {
            client.halt();
        }

        if (server != null) {
            server.halt();
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            service.submit(up);
        }
    }

    private ServerNode thisNode() {
        return serverConfig.getNode(instance);
    }

    private void up() throws JournalNetworkException {
        ServerNode activeNode = getActiveNode();

        try {
            if (activeNode != null) {
                LOGGER.info(thisNode() + " There is active node already %s. Yielding", activeNode);
                setupClient(activeNode);
                return;
            }
        } catch (JournalNetworkException ignore) {
            LOGGER.info("Exception during initial server acquisition. It is safe to ignore: %s", ignore.getMessage());
        }

        LOGGER.info(thisNode() + " Starting server");
//        serverConfig.setEnableMultiCast(false);
        server = new JournalServer(serverConfig, factory, null, thisNode().getId());

        for (int i = 0, writersSize = writers.size(); i < writersSize; i++) {
            server.publish(writers.get(i));
        }
        server.start();


        if ((activeNode = getActiveNode()) != null && !client.voteInstance(instance)) {
            LOGGER.info(thisNode() + " Lost tie-break vote, becoming a client");
            // don't stop server explicitly, it wil shut down after being voted out
            setupClient(activeNode);
            return;
        }

        // after this point server cannot be voted out
        server.setIgnoreVoting(true);

        if (client != null) {
            LOGGER.info(thisNode() + " Stopping client remnants");
            client.halt();
            client = null;
        }

        if (activeNode != null) {
            LOGGER.info("%s is waiting for %s to shutdown", thisNode(), activeNode);
            waitTillDies(activeNode);
        }

        LOGGER.info(thisNode() + " Activating callback");
        listener.onNodeActive();
    }

    private void waitTillDies(final ServerNode node) {
        try {
//          clientConfig.setEnableMultiCast(false);
            JournalClient client = new JournalClient(clientConfig, factory);

            try {
                while (client.pingServer(node)) {
                    Thread.yield();
                }
            } finally {
                client.halt();
            }
        } catch (JournalNetworkException ignore) {
        }
    }

    private ServerNode getActiveNode() {
        // ping each cluster node except for current one
        try {
//            clientConfig.setEnableMultiCast(false);
            for (ServerNode node : clientConfig.nodes()) {
                if (node.getId() == instance) {
                    continue;
                }

                client = new JournalClient(clientConfig, factory);

                if (client.pingServer(node)) {
                    return node;
                }

                client.halt();
            }
        } catch (JournalNetworkException e) {
            return null;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void setupClient(ServerNode node) throws JournalNetworkException {

        LOGGER.info(thisNode() + " Subscribing journals");
        for (int i = 0, sz = writers.size(); i < sz; i++) {
            JournalWriter w = writers.get(i);
            client.subscribe(w.getKey(), w, null);
        }

        LOGGER.info(thisNode() + " Starting client");
        client.setDisconnectCallback(new JournalClient.DisconnectCallback() {
            @Override
            public void onDisconnect(JournalClient.DisconnectReason reason) {
                if (running.get()) {
                    LOGGER.info("Cluster is re-voting");
                    service.submit(up);
                }
            }
        }).start();

        if (listener != null) {
            LOGGER.info(thisNode() + " Notifying callback of standby state");
            listener.onNodeStandingBy(node);
        }

    }
}
