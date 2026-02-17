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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

abstract class AbstractAlterTableSetTypeRestartTest extends AbstractBootstrapTest {
    static void assertConvertFileContent(Path convertFilePath, byte expected) throws IOException {
        final byte[] fileContent = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(convertFilePath.toString()));
        assertEquals(1, fileContent.length);
        assertEquals(expected, fileContent[0]);
    }

    static void assertConvertFileDoesNotExist(CairoEngine engine, TableToken token) {
        doesConvertFileExist(engine, token, false);
    }

    static Path assertConvertFileExists(CairoEngine engine, TableToken token) {
        return doesConvertFileExist(engine, token, true);
    }

    static void assertNumOfRows(CairoEngine engine, String tableName, int count) throws SqlException {
        try (SqlExecutionContext context = createSqlExecutionContext(engine)) {
            TestUtils.assertSql(
                    engine,
                    context,
                    "SELECT count() FROM " + tableName,
                    Misc.getThreadLocalSink(),
                    "count\n" +
                            count + "\n"
            );
        }
    }

    static void assertSeqTxn(CairoEngine engine, TableToken token, long expectedSeqTxn) {
        try (final TableReader reader = engine.getReader(token)) {
            assertEquals(expectedSeqTxn, reader.getTxFile().getSeqTxn());
        }
    }

    static void checkSuspended(String tableName) throws SQLException {
        try (
                final Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                final PreparedStatement stmt = connection.prepareStatement("select name, suspended from wal_tables()")
        ) {
            final ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                assertEquals(tableName, rs.getString(1));
                assertTrue(rs.getBoolean(2));
            }
            if (rs.next()) {
                fail("Too many records returned");
            }
        }
    }

    static void createNonPartitionedTable(String tableName) throws SQLException {
        runSqlViaPG("create table " + tableName + " (ts timestamp, x long) timestamp(ts)");
        LOG.info().$("created table: ").$safe(tableName).$();
    }

    static SqlExecutionContext createSqlExecutionContext(CairoEngine engine) {
        return new SqlExecutionContextImpl(engine, 1).with(
                AllowAllSecurityContext.INSTANCE,
                null,
                null,
                -1,
                null
        );
    }

    static void createTable(String tableName, String walMode) throws SQLException {
        runSqlViaPG("create table " + tableName + " (ts timestamp, x long) timestamp(ts) PARTITION BY DAY " + walMode);
        LOG.info().$("created table: ").$safe(tableName).$();
    }

    static Path doesConvertFileExist(CairoEngine engine, TableToken token, boolean doesExist) {
        final Path path = Path.PATH.get().of(engine.getConfiguration().getDbRoot()).concat(token).concat(WalUtils.CONVERT_FILE_NAME);
        Assert.assertEquals(Utf8s.toString(path), doesExist, Files.exists(path.$()));
        return doesExist ? path : null;
    }

    static void dropTable(String tableName) throws SQLException {
        runSqlViaPG("drop table " + tableName);
        LOG.info().$("dropped table: ").$safe(tableName).$();
    }

    static void insertInto(String tableName) throws SQLException {
        runSqlViaPG("insert into " + tableName + " values('2016-01-01T00:00:00.000Z', 1234)");
        LOG.info().$("inserted 1 row into table: ").$safe(tableName).$();
    }

    static void runSqlViaPG(String sql) throws SQLException {
        try (
                final Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                final PreparedStatement stmt = connection.prepareStatement(sql)
        ) {
            stmt.execute();
        }
    }

    static void setSeqTxn(CairoEngine engine, TableToken token) {
        try (final TableWriter writer = TestUtils.getWriter(engine, token)) {
            writer.commitSeqTxn(12345);
        }
    }

    static void setType(String tableName, String walMode) throws SQLException {
        runSqlViaPG("alter table " + tableName + " set type " + walMode);
        LOG.info().$("scheduled table type conversion for table ").$safe(tableName).$(" to ").$(walMode).$();
    }

    void validateShutdown(String tableName) throws SQLException {
        try {
            insertInto(tableName);
            fail("Expected exception has not been thrown");
        } catch (PSQLException psqlException) {
            TestUtils.assertContains(psqlException.getMessage(), "Connection to 127.0.0.1:" + PG_PORT + " refused.");
        }
    }
}
