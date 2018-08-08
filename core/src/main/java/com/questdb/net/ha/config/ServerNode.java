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

package com.questdb.net.ha.config;

import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.store.JournalRuntimeException;
import org.jetbrains.annotations.NotNull;

public class ServerNode {
    private final int id;
    private final String address;
    private final String hostname;
    private final int port;

    public ServerNode(int id, @NotNull String address) {
        this.id = id;
        this.address = address;

        try {
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
                        throw NumericException.INSTANCE;
                }
            }
        } catch (NumericException e) {
            throw new JournalRuntimeException("Unknown host name format: " + address);
        }
    }

    public ServerNode(int id, String hostname, int port) {
        this.id = id;
        this.hostname = hostname;
        this.port = port;
        this.address = hostname + ':' + port;
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
