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
    private final List<ClusterNode> nodes;
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
    private JournalClient client;
    private JournalServer server;

    public ClusterController(List<ClusterNode> nodes, int instance, ClusterStatusListener listener, JournalFactory factory, List<JournalWriter> writers) {
        this.nodes = nodes;
        this.instance = instance;
        this.listener = listener;
        this.factory = factory;
        this.writers = writers;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            service.submit(up);
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

    private ClusterNode thisNode() {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getId() == instance) {
                return nodes.get(i);
            }
        }
        return null;
    }

    private void up() throws JournalNetworkException {
        ClusterNode activeNode = getActiveNode();

        try {
            if (activeNode != null) {
                LOGGER.info(thisNode() + " There is active node already %s. Yielding");
                setupClient(activeNode);
                return;
            }
        } catch (JournalNetworkException ignore) {
            LOGGER.info("Exception during initial server acquisition. It is safe to ignore: %s", ignore.getMessage());
        }

        LOGGER.info(thisNode() + " Starting server");
        server = new JournalServer(new ServerConfig() {{
            setHostname(thisNode().getAddress());
            setEnableMulticast(false);
        }}, factory, null, thisNode().getId());

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

        if (client != null) {
            LOGGER.info(thisNode() + " Stopping client remnants");
            client.halt();
            client = null;
        }
        LOGGER.info(thisNode() + " Activating callback");
        listener.onNodeActive();
    }

    private ClusterNode getActiveNode() {
        // ping each cluster node except for current one
        try {
            for (int i = 0; i < nodes.size(); i++) {
                final ClusterNode node = nodes.get(i);
                if (node.getId() == instance) {
                    continue;
                }

                client = new JournalClient(new ClientConfig() {{
                    setHostname(node.getAddress());
                    setEnableMulticast(false);
                }}, factory);

                if (client.pingServer()) {
                    return node;
                }

                client.halt();
            }
        } catch (JournalNetworkException e) {
            return null;
        }

        return null;
    }

    private void setupClient(ClusterNode node) throws JournalNetworkException {

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
