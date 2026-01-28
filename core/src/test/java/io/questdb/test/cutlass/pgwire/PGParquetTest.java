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

import io.questdb.PropertyKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGParquetTest extends BasePGTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 13);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
    }

    @Test
    public void testPageFrameMemoryPoolReuseWithParquetAndQueryCache() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    """
                            CREATE TABLE trades AS (
                              SELECT
                                timestamp_sequence('2026-01-01', 3600000000L) as timestamp,
                                case when (x - 1) / 2 % 4 = 0 then 'BTC'
                                     when (x - 1) / 2 % 4 = 1 then 'ETH'
                                     when (x - 1) / 2 % 4 = 2 then 'SOL'
                                     else 'DOGE' end as symbol,
                                x * 1.5 as price,
                                case when x % 2 = 1 then 'sell' else 'buy' end as side
                              FROM long_sequence(1000)
                            ) TIMESTAMP(timestamp) PARTITION BY DAY"""
            )) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE timestamp >= 0"
            )) {
                Assert.assertFalse(ps.execute());
            }

            final String query = """
                    WITH
                    buy AS (SELECT timestamp, symbol, price FROM trades WHERE side = 'buy'),
                    sell AS (SELECT timestamp, symbol, price FROM trades WHERE side = 'sell')
                    SELECT
                       buy.timestamp as timestamp,
                       buy.symbol as symbol,
                       (buy.price - sell.price) as spread
                    FROM buy ASOF JOIN sell ON (symbol)
                    ORDER BY buy.timestamp
                    LIMIT 5""";

            final String expected = """
                    timestamp[TIMESTAMP],symbol[VARCHAR],spread[DOUBLE]
                    2026-01-01 01:00:00.0,BTC,1.5
                    2026-01-01 03:00:00.0,ETH,1.5
                    2026-01-01 05:00:00.0,SOL,1.5
                    2026-01-01 07:00:00.0,DOGE,1.5
                    2026-01-01 09:00:00.0,BTC,1.5
                    """;

            try (PreparedStatement ps = connection.prepareStatement(query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(expected, sink, rs);
                }
            }

            // hit query cache
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        });
    }

    @Test
    public void testParquetGroupByQueryCacheReuse() throws Exception {
        sharedQueryWorkerCount = 2; // Set to 2 to enable parallel query plans
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    """
                            CREATE TABLE x AS (
                              SELECT
                                x as id,
                                x % 5 as key,
                                x * 1.5 as value,
                                timestamp_sequence('1970-01-01', 86400000000L) as ts
                              FROM long_sequence(1000)
                            ) TIMESTAMP(ts) PARTITION BY MONTH"""
            )) {
                Assert.assertFalse(ps.execute());
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0"
            )) {
                Assert.assertFalse(ps.execute());
            }

            final String query = "SELECT key, sum(value), count() FROM x GROUP BY key ORDER BY key";
            final String expected = """
                    key[BIGINT],sum[DOUBLE],count[BIGINT]
                    0,150750.0,200
                    1,149550.0,200
                    2,149850.0,200
                    3,150150.0,200
                    4,150450.0,200
                    """;

            try (PreparedStatement ps = connection.prepareStatement(query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(expected, sink, rs);
                }
            }

            try (PreparedStatement ps = connection.prepareStatement(query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    sink.clear();
                    assertResultSet(expected, sink, rs);
                }
            }
        });
    }
}
