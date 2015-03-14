/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.ha.config;

import com.nfsdb.collections.IntObjHashMap;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.utils.Numbers;
import org.jetbrains.annotations.NotNull;

public class ServerNode {
    private final int id;
    private final String address;
    private final String hostname;
    private final int port;

    public ServerNode(int id, @NotNull String address) {
        this.id = id;
        this.address = address;

        int ob = address.indexOf('[');
        if (ob > -1) {
            int cb = address.indexOf(']');
            // IPv6
            this.hostname = address.substring(ob + 1, cb);
            if (address.length() > cb + 1) {
                this.port = Numbers.parseInt(address.subSequence(cb + 2, address.length()));
            } else {
                this.port = NetworkConfig.DEFAULT_DATA_PORT;
            }
        } else {
            String parts[] = address.split(":");
            switch (parts.length) {
                case 1:
                    this.hostname = address;
                    this.port = NetworkConfig.DEFAULT_DATA_PORT;
                    break;
                case 2:
                    this.hostname = parts[0];
                    port = Numbers.parseInt(parts[1]);
                    break;
                default:
                    throw new JournalRuntimeException("Unknown host name format: " + address);
            }
        }
    }

    public ServerNode(int id, String hostname, int port) {
        this.id = id;
        this.hostname = hostname;
        this.port = port;
        this.address = hostname + ":" + port;
    }

    public static void parse(String nodes, IntObjHashMap<ServerNode> to) {
        String parts[] = nodes.split(",");
        for (int i = 0; i < parts.length; i++) {
            to.put(i, new ServerNode(i, parts[i]));
        }
    }

    public String getAddress() {
        return address;
    }

    public String getHostname() {
        return hostname;
    }

    public int getId() {
        return id;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "ServerNode{" +
                "id=" + id +
                ", address='" + address + '\'' +
                '}';
    }
}
