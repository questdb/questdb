/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.logging.Log;
import com.nfsdb.logging.LogFactory;
import com.nfsdb.net.ha.config.ClientConfig;
import com.nfsdb.net.ha.config.ServerConfig;
import com.nfsdb.net.ha.config.ServerNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterController {

    private final Log LOG = LogFactory.getLog(ClusterController.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ClusterStatusListener listener;
    private final JournalFactory factory;
    private final List<JournalWriter> writers;
    private final ServerConfig serverConfig;
    private final ClientConfig clientConfig;
    private final ServerNode thisNode;
    private final ClusterStatusListener statusListener = new StatusListener();
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
        this.writers = writers;
        this.listener = listener;
        this.thisNode = serverConfig.getNodeByUID(instance);
        if (thisNode == null) {
            throw new JournalRuntimeException("Instance " + instance + " is not found in server config");
        }
    }

    public void halt() {
        if (running.compareAndSet(true, false)) {
            haltClient();
            if (server != null) {
                server.halt();
            }

            if (listener != null) {
                listener.onShutdown();
            }
        }
    }

    public boolean isLeader() {
        return server != null && server.isLeader();
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public void start() throws JournalNetworkException {
        if (running.compareAndSet(false, true)) {
            server = new JournalServer(serverConfig, factory, null, thisNode.getId());
            for (int i = 0, k = writers.size(); i < k; i++) {
                server.publish(writers.get(i));
            }
            server.start();
            server.joinCluster(statusListener);
        }
    }

    private void haltClient() {
        if (client != null) {
            client.halt();
            client = null;
        }
    }

    private class StatusListener implements ClusterStatusListener {

        private int lastActive = -1;

        @Override
        public void goActive() {
            if (listener != null) {
                listener.goActive();
            }
        }

        @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
        @Override
        @SuppressWarnings("unchecked")
        public void goPassive(ServerNode activeNode) {
            if (activeNode.getId() != lastActive) {

                lastActive = activeNode.getId();

                // halt existing client
                haltClient();

                // configure client to connect to new active node
                clientConfig.clearNodes();
                clientConfig.addNode(activeNode);

                client = new JournalClient(clientConfig, factory);
                LOG.info().$(thisNode.toString()).$(" Subscribing journals").$();
                for (int i = 0, sz = writers.size(); i < sz; i++) {
                    JournalWriter w = writers.get(i);
                    client.subscribe(w.getKey(), w, null);
                }

                try {

                    client.start();
                    client.setDisconnectCallback(new JournalClient.DisconnectCallback() {
                        @Override
                        public void onDisconnect(JournalClient.DisconnectReason reason) {
                            switch (reason) {
                                case INCOMPATIBLE_JOURNAL:
                                case CLIENT_HALT:
                                    halt();
                                    break;
                                default:
                                    if (running.get()) {
                                        server.joinCluster(statusListener);
                                    }
                            }
                        }
                    });

                    if (listener != null) {
                        listener.goPassive(activeNode);
                    }

                } catch (JournalNetworkException e) {
                    LOG.error().$("Failed to start client").$(e).$();
                    haltClient();
                    server.joinCluster(statusListener);
                }
            }
        }

        @Override
        public void onShutdown() {

        }

    }
}

