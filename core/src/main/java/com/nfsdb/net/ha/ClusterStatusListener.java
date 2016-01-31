/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.net.ha.config.ServerNode;

public interface ClusterStatusListener {

    /**
     * Activates producer on current node. This can only happen once in a lifespan of cluster node.
     * Node cannot become neither passive nor active again unless it cluster controller is restarted.
     */
    void goActive();

    /**
     * Notifies producer that current node is passive. Current active node is provided as a parameter.
     * From passive state node can become either active or passive again. Implementations should check
     * if active node has changed to avoid repeating work.
     *
     * @param activeNode current active node
     */
    void goPassive(ServerNode activeNode);

    /**
     * Cluster controller shutdown callback.
     */
    void onShutdown();
}
