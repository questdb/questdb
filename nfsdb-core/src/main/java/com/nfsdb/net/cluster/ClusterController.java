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

        private boolean startup = true;
        @Override
        public void run() {
            try {
                vote(startup);
                startup = false;
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
    private final ExecutorService service;
    private final ServerConfig serverConfig;
    private final ClientConfig clientConfig;
    private JournalClient client;
    private JournalServer server;

    public ClusterController(
            ServerConfig serverConfig
            , ClientConfig clientConfig
            , JournalFactory factory
            , final int instance
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
        service = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public
            @Nonnull
            Thread newThread(@Nonnull Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("nfsdb-cluster-controller-" + instance);
                return thread;
            }
        });
    }

    public void halt() throws JournalNetworkException {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        listener.onShutdown();
        service.shutdown();
        haltClient();

        if (server != null) {
            server.halt();
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            service.submit(up);
        }
    }

    private ServerNode getActiveNodeAndSetupClient() throws JournalNetworkException {
        // ping each cluster node except for current one
        try {
            for (ServerNode node : clientConfig.nodes()) {
                if (node.getId() == instance) {
                    continue;
                }


                client = new JournalClient(clientConfig, factory);
                if (client.pingServer(node)) {
                    return node;
                }

                haltClient();
            }
        } catch (JournalNetworkException e) {
            haltClient();
            return null;
        }

        return null;
    }

    private void haltClient() throws JournalNetworkException {
        if (client != null) {
            client.halt();
            client = null;
        }
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

    private ServerNode thisNode() {
        return serverConfig.getNode(instance);
    }

    private void vote(boolean startup) throws JournalNetworkException {

        // this method can be called during both, standalone start and cluster re-vote
        // during re-vote all members scramble to become ALPHA so checking for sever presence
        // is only appropriate during standalone start, where new server node will always assume
        // slave role if there is existing server.
        ServerNode activeNode;
        if (startup) {
            try {
                if ((activeNode = getActiveNodeAndSetupClient()) != null) {
                    LOGGER.info(thisNode() + " There is active node already %s. Yielding", activeNode);
                    setupClient(activeNode);
                    return;
                }
            } catch (JournalNetworkException ignore) {
                LOGGER.info("Exception during initial server acquisition. It is safe to ignore: %s", ignore.getMessage());
            }

            haltClient();
        }

        // scramble server to get noticed by other cluster controllers.
        LOGGER.info(thisNode() + " Starting server");
        server = new JournalServer(serverConfig, factory, null, thisNode().getId());

        for (int i = 0, writersSize = writers.size(); i < writersSize; i++) {
            server.publish(writers.get(i));
        }
        server.start();
        //

        // start voting looking for ALPHA in the process.
        // if this node wins a vote it does not mean it would become ALPHA
        // node has to keep voting until it is last node standing.
        //
        // on other hand, if this node loses, it is enough to become a slave, so give up voting in this case.
        boolean isClient = false;
        while (!isClient && server.isRunning() && (activeNode = getActiveNodeAndSetupClient()) != null && client != null) {
            LOGGER.info("%s thinks that %s is present", thisNode(), activeNode);
            switch (client.voteInstance(instance, activeNode)) {
                case ALPHA:
                    LOGGER.info(thisNode() + " Lost tie-break vote, becoming a client");
                    // don't stop server explicitly, it wil shut down after being voted out
                    setupClient(activeNode);
                    return;
                case THEM:
                    LOGGER.info("%s lost tie-break against %s, wait for ALPHA node", thisNode(), activeNode);
                    isClient = true;
                    break;
                default:
                    LOGGER.info("%s WON tie-break against %s", thisNode(), activeNode);
            }

            // always stop client because we will create new one when looking for ALPHA
            haltClient();
            Thread.yield();
        }

        if (!isClient) {
            // after this point server cannot be voted out and it becomes the ALPHA
            server.setIgnoreVoting(true);
            LOGGER.info(thisNode() + " Activating callback");
            listener.onNodeActive();
            return;
        }

        // look for ALPHA in a loop
        // this loop cannot exit unless it finds ALPHA or runs out of nodes to check and errors out
        while (true) {
            activeNode = getActiveNodeAndSetupClient();
            if (activeNode == null || client == null) {
                throw new JournalNetworkException("Expected ALPHA node but got none");
            }

            if (client.voteInstance(instance, activeNode) == JournalClient.VoteResult.ALPHA) {
                setupClient(activeNode);
                return;
            }
            haltClient();
        }

    }
}
