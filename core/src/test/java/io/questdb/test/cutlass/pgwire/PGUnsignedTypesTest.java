/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cutlass.pgwire;

import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGUnsignedTypesTest extends BasePGTest {

    @Test
    public void testUnsignedSelectAsStringAcrossPgModes() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "create table pg_uint(u16 uint16, u32 uint32, u64 uint64)"
            )) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "insert into pg_uint values (-1, -1, -1)"
            )) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "select u16, u32, u64 from pg_uint"
            )) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals("65535", resultSet.getString(1));
                    Assert.assertEquals("4294967295", resultSet.getString(2));
                    Assert.assertEquals("18446744073709551615", resultSet.getString(3));
                    Assert.assertFalse(resultSet.next());
                }
            }
        });
    }

    @Test
    public void testUnsignedNullHandlingAcrossPgModes() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "CREATE TABLE pg_uint_null(u16 UINT16, u32 UINT32, u64 UINT64)"
            )) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO pg_uint_null VALUES (NULL, NULL, NULL)"
            )) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO pg_uint_null VALUES (-1, -1, -1)"
            )) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT u16, u32, u64 FROM pg_uint_null"
            )) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    // NULL row
                    Assert.assertTrue(resultSet.next());
                    Assert.assertNull(resultSet.getString(1));
                    Assert.assertTrue(resultSet.wasNull());
                    Assert.assertNull(resultSet.getString(2));
                    Assert.assertTrue(resultSet.wasNull());
                    Assert.assertNull(resultSet.getString(3));
                    Assert.assertTrue(resultSet.wasNull());

                    // max value row
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals("65535", resultSet.getString(1));
                    Assert.assertEquals("4294967295", resultSet.getString(2));
                    Assert.assertEquals("18446744073709551615", resultSet.getString(3));

                    Assert.assertFalse(resultSet.next());
                }
            }
        });
    }
}
