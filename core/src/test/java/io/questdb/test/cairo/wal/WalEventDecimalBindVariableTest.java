/*+*****************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Round-trips decimal bind variables through a WAL SQL event. Columns and variables suffixed with
 * 'n' take a NULL bind, those suffixed with 'v' take a value bind.
 */
public class WalEventDecimalBindVariableTest extends AbstractCairoTest {

    private static final String[] NAMES = {"d8", "d16", "d32", "d64", "d128", "d256"};
    private static final int[] TYPES = {
            ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0),
            ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 0),
            ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 0),
            ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 0),
            ColumnType.getDecimalType(ColumnType.DECIMAL128, 38, 0),
            ColumnType.getDecimalType(ColumnType.DECIMAL256, 76, 0),
    };
    // negative values, one per width, wide enough to use every word of their storage
    private static final long[][] VALUES = {
            {-1, -1, -1, -7},                   // -7
            {-1, -1, -1, -1234},                // -1234
            {-1, -1, -1, -123456789},           // -123456789
            {-1, -1, -1, -123456789012345678L}, // -123456789012345678
            {-1, -1, -2, -5},                   // -(2^64 + 5)
            {-1, -2, -1, -5},                   // -(2^128 + 5)
    };

    @Test
    public void testIndexedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable("x_indexed");
            final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
            bindVariableService.clear();
            for (int i = 0; i < TYPES.length; i++) {
                setNullVariable(bindVariableService, 2 * i, TYPES[i]);
                bindVariableService.setDecimal(2 * i + 1, VALUES[i][0], VALUES[i][1], VALUES[i][2], VALUES[i][3], TYPES[i]);
            }

            update("UPDATE x_indexed SET d8n = $1, d8v = $2, d16n = $3, d16v = $4, d32n = $5, d32v = $6," +
                    " d64n = $7, d64v = $8, d128n = $9, d128v = $10, d256n = $11, d256v = $12");

            final BindVariableService replayed = new BindVariableServiceImpl(configuration);
            replaySqlEvent(tableToken, replayed);
            for (int i = 0; i < TYPES.length; i++) {
                assertNullVariable(replayed.getFunction(2 * i), TYPES[i]);
                assertValueVariable(replayed.getFunction(2 * i + 1), TYPES[i], VALUES[i]);
            }
            replayed.clear();

            assertUpdatedRow("x_indexed");
        });
    }

    @Test
    public void testNamedVariables() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tableToken = createTable("x_named");
            final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
            bindVariableService.clear();
            for (int i = 0; i < TYPES.length; i++) {
                setNullVariable(bindVariableService, NAMES[i] + "n", TYPES[i]);
                bindVariableService.setDecimal(NAMES[i] + "v", VALUES[i][0], VALUES[i][1], VALUES[i][2], VALUES[i][3], TYPES[i]);
            }

            update("UPDATE x_named SET d8n = :d8n, d8v = :d8v, d16n = :d16n, d16v = :d16v, d32n = :d32n, d32v = :d32v," +
                    " d64n = :d64n, d64v = :d64v, d128n = :d128n, d128v = :d128v, d256n = :d256n, d256v = :d256v");

            final BindVariableService replayed = new BindVariableServiceImpl(configuration);
            replaySqlEvent(tableToken, replayed);
            for (int i = 0; i < TYPES.length; i++) {
                assertNullVariable(replayed.getFunction(':' + NAMES[i] + "n"), TYPES[i]);
                assertValueVariable(replayed.getFunction(':' + NAMES[i] + "v"), TYPES[i], VALUES[i]);
            }
            replayed.clear();

            assertUpdatedRow("x_named");
        });
    }

    private static void assertNullVariable(Function function, int type) {
        assertEquals(type, function.getType());
        final Decimal256 decimal256 = new Decimal256();
        function.getDecimal256(null, decimal256);
        assertTrue("decimal256 must be null, type=" + ColumnType.nameOf(type), decimal256.isNull());
        final Decimal128 decimal128 = new Decimal128();
        function.getDecimal128(null, decimal128);
        assertTrue("decimal128 must be null, type=" + ColumnType.nameOf(type), decimal128.isNull());
        assertEquals(Decimals.DECIMAL8_NULL, function.getDecimal8(null));
        assertEquals(Decimals.DECIMAL16_NULL, function.getDecimal16(null));
        assertEquals(Decimals.DECIMAL32_NULL, function.getDecimal32(null));
        assertEquals(Decimals.DECIMAL64_NULL, function.getDecimal64(null));
    }

    private static void assertValueVariable(Function function, int type, long[] expected) {
        assertEquals(type, function.getType());
        final Decimal256 decimal256 = new Decimal256();
        function.getDecimal256(null, decimal256);
        assertFalse("value must not be null, type=" + ColumnType.nameOf(type), decimal256.isNull());
        assertEquals(expected[0], decimal256.getHh());
        assertEquals(expected[1], decimal256.getHl());
        assertEquals(expected[2], decimal256.getLh());
        assertEquals(expected[3], decimal256.getLl());
    }

    private static void setNullVariable(BindVariableService bindVariableService, int index, int type) throws SqlException {
        bindVariableService.setDecimal(
                index,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                type
        );
    }

    private static void setNullVariable(BindVariableService bindVariableService, CharSequence name, int type) throws SqlException {
        bindVariableService.setDecimal(
                name,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                type
        );
    }

    private void assertUpdatedRow(String tableName) throws Exception {
        drainWalQueue();
        assertQuery("select d8n is null n8, d16n is null n16, d32n is null n32," +
                " d64n is null n64, d128n is null n128, d256n is null n256 from " + tableName)
                .noLeakCheck()
                .expectSize()
                .returns("n8\tn16\tn32\tn64\tn128\tn256\n" +
                        "true\ttrue\ttrue\ttrue\ttrue\ttrue\n");
        assertQuery("select d8v, d16v, d32v, d64v, d128v, d256v from " + tableName)
                .noLeakCheck()
                .expectSize()
                .returns("d8v\td16v\td32v\td64v\td128v\td256v\n" +
                        "-7\t-1234\t-123456789\t-123456789012345678\t-18446744073709551621\t-340282366920938463463374607431768211461\n");
    }

    private TableToken createTable(String tableName) throws SqlException {
        execute("create table " + tableName + " (" +
                "ts timestamp," +
                "d8n decimal(2,0), d8v decimal(2,0)," +
                "d16n decimal(4,0), d16v decimal(4,0)," +
                "d32n decimal(9,0), d32v decimal(9,0)," +
                "d64n decimal(18,0), d64v decimal(18,0)," +
                "d128n decimal(38,0), d128v decimal(38,0)," +
                "d256n decimal(76,0), d256v decimal(76,0)" +
                ") timestamp(ts) partition by day wal");
        execute("insert into " + tableName + "(ts) values ('2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        return engine.verifyTableName(tableName);
    }

    private void replaySqlEvent(TableToken tableToken, BindVariableService bindVariableService) {
        try (Path path = new Path(); WalEventReader reader = new WalEventReader(configuration)) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat("wal1").slash().put(0);
            WalEventCursor cursor = reader.of(path, 0);
            do {
                if (cursor.getType() == WalTxnType.SQL) {
                    cursor.getSqlInfo().populateBindVariableService(bindVariableService);
                    return;
                }
            } while (cursor.hasNext());
            fail("SQL event not found in the WAL segment");
        }
    }
}
