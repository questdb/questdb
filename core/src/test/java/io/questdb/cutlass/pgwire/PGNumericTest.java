/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.mp.WorkerPool;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class PGNumericTest extends BasePGTest {
    @Test
    public void testInsertBigDecimalToLong256Column() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("create table x (l long256)");
                }
                try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                    stmt.setBigDecimal(1, new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935"));
                    stmt.execute();
                }

                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(l) from x");
                     ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal bigDecimal = rs.getBigDecimal(1);
                    assertEquals("57398447894837100344168337480586971291799409762944964421131331554666233064935", bigDecimal.toPlainString());
                }
            }
        });
    }

    @Test
    public void testInsertLong256DecToLong256Column() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("create table x (l long256)");
                }
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("insert into x values (57398447894837100344168337480586971291799409762944964421131331554666233064935)");
                }

                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(l) from x");
                     ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal bigDecimal = rs.getBigDecimal(1);
                    assertEquals("57398447894837100344168337480586971291799409762944964421131331554666233064935", bigDecimal.toPlainString());
                }
            }
        });
    }

    @Test
    public void testNumericType_parsingDecConstant() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                // dec
                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(57398447894837100344168337480586971291799409762944964421131331554666233064935) from long_sequence(1)");
                     ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal fromQuest = rs.getBigDecimal(1);
                    BigDecimal expected = new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935");
                    assertEquals(expected, fromQuest);
                }
            }
        });
    }

    @Test
    public void testNumericType_parsingDecConstantPrepared() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                // dec
                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(?) from long_sequence(1)")) {
                    ps.setBigDecimal(1, new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935"));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        BigDecimal fromQuest = rs.getBigDecimal(1);
                        BigDecimal expected = new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935");
                        assertEquals(expected, fromQuest);
                    }
                }
            }
        });
    }

    @Test
    public void testNumericType_parsingHexConstant() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199bf5c2aa91ba39c022fa261bdede7) from long_sequence(1)");
                     ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal fromQuest = rs.getBigDecimal(1);
                    BigDecimal expected = new BigDecimal(new BigInteger("7ee65ec7b6e3bc3a422a8855e9d7bfd29199bf5c2aa91ba39c022fa261bdede7", 16));
                    assertEquals(expected, fromQuest);
                }

            }
        });
    }

    @Test
    public void testQueryLong256ColumnWithBigDecimal() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary) -> {
            try (final PGWireServer server = createPGServer(1);
                 final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("create table x (l long256)");
                }
                try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                    stmt.setBigDecimal(1, new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935"));
                    stmt.execute();
                    stmt.setBigDecimal(1, new BigDecimal("123434354353423321"));
                    stmt.execute();
                    stmt.setBigDecimal(1, new BigDecimal("42"));
                    stmt.execute();
                }

                try (PreparedStatement ps = connection.prepareStatement("select asNumeric(l) from x where l = ?")) {
                    ps.setBigDecimal(1, new BigDecimal("57398447894837100344168337480586971291799409762944964421131331554666233064935"));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        BigDecimal bigDecimal = rs.getBigDecimal(1);
                        assertEquals("57398447894837100344168337480586971291799409762944964421131331554666233064935", bigDecimal.toPlainString());
                        assertFalse(rs.next());
                    }
                    ps.setBigDecimal(1, new BigDecimal("42"));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        BigDecimal bigDecimal = rs.getBigDecimal(1);
                        assertEquals("42", bigDecimal.toPlainString());
                        assertFalse(rs.next());
                    }
                    ps.setBigDecimal(1, new BigDecimal(Long.MAX_VALUE));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertFalse(rs.next());
                    }
                }
            }
        });
    }
}
