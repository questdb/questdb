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

package com.nfsdb.net.ha.config;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

public class ServerNode {
    private final int id;
    private final String address;
    private final String hostname;
    private final int port;

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
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
