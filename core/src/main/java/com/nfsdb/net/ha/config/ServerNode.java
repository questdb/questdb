/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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
