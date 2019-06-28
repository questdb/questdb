/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.pgwire;

import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.network.*;
import com.questdb.std.Os;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class WireParserTest extends AbstractGriffinTest {

    @Test
    public void testSimple() throws SQLException {
        // start simple server

        long fd = Net.socketTcp(true);

        WireParser wireParser = new WireParser(new WireParserConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return NetworkFacadeImpl.INSTANCE;
            }

            @Override
            public int getRecvBufferSize() {
                return 1024 * 1024;
            }

            @Override
            public int getSendBufferSize() {
                return 1024 * 1024;
            }
        }, engine);

        Net.setReusePort(fd);

        if (Net.bindTcp(fd, 0, 9120)) {
            Net.listen(fd, 128);

            new Thread(() -> {
                final long clientFd = Net.accept(fd);
                while (true) {
                    try {
                        wireParser.recv(clientFd);
                    } catch (PeerDisconnectedException e) {
                        break;
                    } catch (PeerIsSlowToReadException ignored) {
                    }
                }
            }).start();

            Properties properties = new Properties();
            properties.setProperty("user", "xyz");
            properties.setProperty("password", "oh");
            properties.setProperty("sslmode", "disable");

            final Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9120/nabu_app", properties);
            Statement statement = connection.createStatement();
//            ResultSet rs = statement.executeQuery("select rnd_str(4,4,0) s, timestamp_sequence(to_timestamp(0),10000) t, rnd_double(0) d, rnd_int() i from long_sequence(4000)2");
            ResultSet rs = statement.executeQuery("select rnd_str(4,4,0) s, rnd_int() i from long_sequence(5)");
            while (rs.next()) {
                System.out.print(rs.getString(1));
                System.out.print(",");
                System.out.print(rs.getInt(2));
                System.out.println();
            }
            connection.close();
        } else {
            throw NetworkError.instance(Os.errno()).couldNotBindSocket();
        }

    }
}