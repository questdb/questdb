/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.config.ServerNode;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.Factory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterController {

    private final Log LOG = LogFactory.getLog(ClusterController.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ClusterStatusListener listener;
    private final Factory factory;
    private final List<JournalWriter> writers;
    private final ServerConfig serverConfig;
    private final ClientConfig clientConfig;
    private final ServerNode thisNode;
    private final ClusterStatusListener statusListener = new StatusListener();
    private final ClientCallback clientCallback = new ClientCallback();
    private JournalClient client;
    private JournalServer server;

    public ClusterController(
            ServerConfig serverConfig
            , ClientConfig clientConfig
            , Factory factory
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

                client = new JournalClient(clientConfig, factory, null, clientCallback);
                LOG.info().$(thisNode.toString()).$(" Subscribing journals").$();
                for (int i = 0, sz = writers.size(); i < sz; i++) {
                    JournalWriter w = writers.get(i);
                    client.subscribe(w.getMetadata().getKey(), w, null);
                }

                client.start();

                if (listener != null) {
                    listener.goPassive(activeNode);
                }
            }
        }

        @Override
        public void onShutdown() {

        }

    }

    private class ClientCallback implements JournalClient.Callback {
        @Override
        public void onEvent(int evt) {
            switch (evt) {
                case JournalClientEvents.EVT_INCOMPATIBLE_JOURNAL:
                case JournalClientEvents.EVT_CLIENT_HALT:
                case JournalClientEvents.EVT_AUTH_CONFIG_ERROR:
                case JournalClientEvents.EVT_CLIENT_EXCEPTION:
                    halt();
                    break;
                case JournalClientEvents.EVT_SERVER_ERROR:
                    if (running.get()) {
                        server.joinCluster(statusListener);
                    }
                    break;
                default:
                    break;
            }
        }
    }
}

