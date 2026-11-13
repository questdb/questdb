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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.griffin.SqlException;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class QwpSchemaControlTest extends AbstractCairoTest {
    private static final int BUFFER_SIZE = 1024;
    private static final Method DESCRIBE;
    private static final Method ENCODE_FEEDBACK;
    private final boolean isWal;

    public QwpSchemaControlTest(boolean isWal) {
        this.isWal = isWal;
    }

    @Parameterized.Parameters(name = "wal={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Test
    public void testDescribeDuringDropRecreate() throws Exception {
        assertDescribeDuringReplacement(false);
    }

    @Test
    public void testDescribeDuringRename() throws Exception {
        assertDescribeDuringReplacement(true);
    }

    @Test
    public void testDescribeMissing() throws Exception {
        assertMemoryLeak(() -> {
            QwpSchemaResponse response = describe(AllowAllSecurityContext.INSTANCE);
            Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING, response.getResult());
            Assert.assertEquals(0, response.getColumnCount());
        });
    }

    @Test
    public void testDescribeReplacementDenied() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            AtomicInteger authorizations = new AtomicInteger();
            SecurityContext replacement = replacingContext(false, authorizations);
            QwpSchemaResponse response = describe(new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    replacement.authorizeInsert(tableToken);
                    if (authorizations.get() > 1) {
                        throw CairoException.authorization().put("replacement denied");
                    }
                }
            });
            Assert.assertEquals(QwpSchemaProtocol.RESULT_DENIED, response.getResult());
            Assert.assertEquals(0, response.getColumnCount());
            Assert.assertEquals(2, authorizations.get());
        });
    }

    @Test
    public void testDescribeRetryLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 2);
        assertMemoryLeak(() -> {
            createTable("schema_race");
            AtomicInteger authorizations = new AtomicInteger();
            QwpSchemaResponse response = describe(new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    Assert.assertEquals(engine.verifyTableName("schema_race"), tableToken);
                    Assert.assertTrue(authorizations.incrementAndGet() <= 3);
                    replaceTable(false);
                }
            });
            Assert.assertEquals(QwpSchemaProtocol.RESULT_UNAVAILABLE, response.getResult());
            Assert.assertEquals(0, response.getColumnCount());
            Assert.assertEquals(3, authorizations.get());
        });
    }

    @Test
    public void testDescribeUnavailableDoesNotRetry() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            AtomicInteger authorizations = new AtomicInteger();
            QwpSchemaResponse response = describe(new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    authorizations.incrementAndGet();
                    throw CairoException.nonCritical().put("unavailable");
                }
            });
            Assert.assertEquals(QwpSchemaProtocol.RESULT_UNAVAILABLE, response.getResult());
            Assert.assertEquals(0, response.getColumnCount());
            Assert.assertEquals(1, authorizations.get());
        });
    }

    @Test
    public void testFeedbackDuringDropRecreate() throws Exception {
        assertFeedbackDuringReplacement(false);
    }

    @Test
    public void testFeedbackDuringRename() throws Exception {
        assertFeedbackDuringReplacement(true);
    }

    @Test
    public void testFeedbackCapacityBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            // Standalone schema payload: 26-byte prefix + n (9) + ts (10).
            // Feedback adds count (2), name length/name (13), payload length (4).
            for (int limit = 0; limit <= 65; limit++) {
                assertFeedback(limit, 45, limit >= 64 ? 64 : limit >= 29 ? 29 : -1,
                        limit >= 64 ? QwpSchemaProtocol.RESULT_KNOWN : QwpSchemaProtocol.RESULT_UNAVAILABLE);
            }
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN,
                    describe(AllowAllSecurityContext.INSTANCE, 57).getResult());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE,
                    describe(AllowAllSecurityContext.INSTANCE, 56).getResult());
            assertFeedback(29, 44, 29, QwpSchemaProtocol.RESULT_TOO_LARGE);
            assertFeedback(64, 44, 29, QwpSchemaProtocol.RESULT_TOO_LARGE);
            assertFeedback(29, 25, 29, QwpSchemaProtocol.RESULT_TOO_LARGE);

            execute("ALTER TABLE schema_race ADD COLUMN gone LONG");
            execute("ALTER TABLE schema_race DROP COLUMN gone");
            assertFeedback(29, 45, 29, QwpSchemaProtocol.RESULT_UNAVAILABLE);
            assertFeedback(64, 45, 64, QwpSchemaProtocol.RESULT_KNOWN);
        });
    }

    @Test
    public void testFeedbackCapacityPreservesColumnCountLimit() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "schema_race", PartitionBy.DAY).timestamp("ts");
            for (int i = 0; i < QwpSchemaProtocol.MAX_COLUMN_COUNT; i++) {
                model.col("c" + i, ColumnType.LONG);
            }
            createTable(isWal ? model.wal() : model.noWal());
            assertFeedback(29, 1_048_564, 29, QwpSchemaProtocol.RESULT_TOO_LARGE);
        });
    }

    @Test
    public void testFeedbackCapacityPreservesLegacyNameLimit() throws Exception {
        assertUndescribableName("user-agent");
    }

    @Test
    public void testFeedbackCapacityPreservesNameLengthLimit() throws Exception {
        assertUndescribableName("x".repeat(128));
    }

    @Test
    public void testFeedbackCapacityPreservesPermissionAndUnavailableFallbacks() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            assertFeedback(new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    throw CairoException.authorization().put("denied");
                }
            }, 29, 45, -1, QwpSchemaProtocol.RESULT_DENIED);
            assertFeedback(new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    throw CairoException.nonCritical().put("unavailable");
                }
            }, 29, 45, -1, QwpSchemaProtocol.RESULT_UNAVAILABLE);
        });
    }

    @Test
    public void testFeedbackReservesEveryMinimalEntry() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_a");
            createTable("schema_b");
            createTable("schema_c");
            assertReservedFeedback("schema_a", "schema_b", "schema_c", 8);
            assertReservedFeedback("schema_c", "schema_b", "schema_a", 8);
        });
    }

    @Test
    public void testFeedbackReservesUtf8MinimalEntries() throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_a");
            createTable("schéma_界");
            createTable("schema_c");
            assertReservedFeedback("schema_a", "schéma_界", "schema_c", 11);
            assertReservedFeedback("schema_c", "schéma_界", "schema_a", 11);
        });
    }

    private void assertDescribeDuringReplacement(boolean isRename) throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            if (isRename) {
                createTable("replacement");
            }
            TableToken before = engine.verifyTableName("schema_race");
            AtomicInteger authorizations = new AtomicInteger();
            QwpSchemaResponse response = describe(replacingContext(isRename, authorizations));
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
            TableToken after = engine.verifyTableName("schema_race");
            Assert.assertNotEquals(before.getTableId(), after.getTableId());
            Assert.assertEquals(2, authorizations.get());
            Assert.assertEquals(after.getTableId(), response.getTableId());
        });
    }

    private void assertFeedbackDuringReplacement(boolean isRename) throws Exception {
        assertMemoryLeak(() -> {
            createTable("schema_race");
            if (isRename) {
                createTable("replacement");
            }
            TableToken before = engine.verifyTableName("schema_race");
            LowerCaseCharSequenceObjHashMap<String> tableNames = new LowerCaseCharSequenceObjHashMap<>();
            tableNames.put("schema_race", "schema_race");
            long address = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                AtomicInteger authorizations = new AtomicInteger();
                // Both ACK and NACK callers turn -1 into nameless invalidation.
                Assert.assertEquals(-1, (int) ENCODE_FEEDBACK.invoke(
                        null, engine, replacingContext(isRename, authorizations), tableNames, address, BUFFER_SIZE, BUFFER_SIZE
                ));
                Assert.assertEquals(1, authorizations.get());
                TableToken after = engine.verifyTableName("schema_race");
                Assert.assertNotEquals(before.getTableId(), after.getTableId());
                Assert.assertTrue((int) ENCODE_FEEDBACK.invoke(
                        null, engine, AllowAllSecurityContext.INSTANCE, tableNames, address, BUFFER_SIZE, BUFFER_SIZE
                ) > 0);
            } finally {
                Unsafe.free(address, BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void assertFeedback(int limit, int schemaLimit, int expectedLength, int expectedResult) throws Exception {
        assertFeedback(AllowAllSecurityContext.INSTANCE, limit, schemaLimit, expectedLength, expectedResult);
    }

    private void assertFeedback(
            SecurityContext securityContext,
            int limit,
            int schemaLimit,
            int expectedLength,
            int expectedResult
    ) throws Exception {
        LowerCaseCharSequenceObjHashMap<String> tableNames = new LowerCaseCharSequenceObjHashMap<>();
        tableNames.put("schema_race", "schema_race");
        long allocation = Unsafe.malloc(BUFFER_SIZE + 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long address = allocation + Long.BYTES;
        try {
            Unsafe.putLong(allocation, 0x123456789abcdef0L);
            Unsafe.putLong(address + limit, 0x123456789abcdef0L);
            int length = (int) ENCODE_FEEDBACK.invoke(
                    null, engine, securityContext, tableNames, address, limit, schemaLimit
            );
            Assert.assertEquals("limit=" + limit + ", schemaLimit=" + schemaLimit, expectedLength, length);
            Assert.assertEquals(0x123456789abcdef0L, Unsafe.getLong(allocation));
            Assert.assertEquals(0x123456789abcdef0L, Unsafe.getLong(address + limit));
            if (length > 0) {
                Assert.assertEquals(1, Unsafe.getShort(address));
                Assert.assertEquals(11, Unsafe.getShort(address + 2));
                int payloadLength = Unsafe.getInt(address + 15);
                Assert.assertEquals(length - 19, payloadLength);
                QwpSchemaResponse response = QwpSchemaProtocol.decodeFeedbackPayload(address + 19, payloadLength);
                Assert.assertEquals(expectedResult, response.getResult());
                Assert.assertEquals(0, response.getRequestId());
                if (expectedResult == QwpSchemaProtocol.RESULT_KNOWN) {
                    Assert.assertEquals(2, response.getColumnCount());
                    Assert.assertEquals(1, response.getDesignatedIndex());
                }
            }
        } finally {
            Unsafe.free(allocation, BUFFER_SIZE + 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertReservedFeedback(String first, String middle, String last, int middleNameBytes) throws Exception {
        LowerCaseCharSequenceObjHashMap<String> tableNames = new LowerCaseCharSequenceObjHashMap<>();
        tableNames.put(first, first);
        tableNames.put(middle, middle);
        tableNames.put(last, last);
        // Each schema is 45 bytes; a result-only payload is 10. All three
        // entries need their six-byte overhead and UTF-8 names, plus count:u16.
        int minimum = 2 + 3 * (6 + 10) + 8 + middleNameBytes + 8;
        int allKnown = minimum + 3 * (45 - 10);
        long allocation = Unsafe.malloc(BUFFER_SIZE + 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long address = allocation + Long.BYTES;
        try {
            for (int limit = 0; limit <= allKnown + 1; limit++) {
                Unsafe.putLong(allocation, 0x123456789abcdef0L);
                Unsafe.putLong(address + limit, 0x123456789abcdef0L);
                int length = (int) ENCODE_FEEDBACK.invoke(
                        null, engine, AllowAllSecurityContext.INSTANCE, tableNames, address, limit, BUFFER_SIZE
                );
                int knownCount = limit >= minimum ? Math.min(3, (limit - minimum) / (45 - 10)) : 0;
                Assert.assertEquals("limit=" + limit + ", minimum=" + minimum,
                        limit >= minimum ? minimum + knownCount * (45 - 10) : -1, length);
                Assert.assertEquals(0x123456789abcdef0L, Unsafe.getLong(allocation));
                Assert.assertEquals(0x123456789abcdef0L, Unsafe.getLong(address + limit));
                if (length < 0) {
                    continue;
                }
                Assert.assertEquals(3, Unsafe.getShort(address));
                long p = address + Short.BYTES;
                for (int i = 0; i < 3; i++) {
                    String tableName = tableNames.keys().getQuick(i).toString();
                    int nameBytes = tableName.equals(middle) ? middleNameBytes : 8;
                    Assert.assertEquals(nameBytes, Unsafe.getShort(p));
                    p += Short.BYTES;
                    StringSink name = new StringSink();
                    Assert.assertTrue(Utf8s.utf8ToUtf16(p, p + nameBytes, name));
                    Assert.assertEquals(tableName, name.toString());
                    p += nameBytes;
                    int payloadLength = Unsafe.getInt(p);
                    p += Integer.BYTES;
                    QwpSchemaResponse response = QwpSchemaProtocol.decodeFeedbackPayload(p, payloadLength);
                    Assert.assertEquals(i < knownCount ? QwpSchemaProtocol.RESULT_KNOWN : QwpSchemaProtocol.RESULT_UNAVAILABLE,
                            response.getResult());
                    Assert.assertEquals(0, response.getRequestId());
                    Assert.assertEquals(i < knownCount ? 45 : 10, payloadLength);
                    if (i < knownCount) {
                        Assert.assertEquals(engine.verifyTableName(tableName).getTableId(), response.getTableId());
                        Assert.assertEquals(2, response.getColumnCount());
                        Assert.assertEquals(1, response.getDesignatedIndex());
                    }
                    p += payloadLength;
                }
                Assert.assertEquals(length, p - address);
            }
        } finally {
            Unsafe.free(allocation, BUFFER_SIZE + 2 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertUndescribableName(String columnName) throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "schema_race", PartitionBy.DAY)
                    .col("n", ColumnType.LONG).col(columnName, ColumnType.VARCHAR).timestamp("ts");
            createTable(isWal ? model.wal() : model.noWal());
            assertFeedback(29, BUFFER_SIZE, 29, QwpSchemaProtocol.RESULT_TOO_LARGE);
            Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, describe(AllowAllSecurityContext.INSTANCE).getResult());
        });
    }

    private void createTable(String tableName) throws SqlException {
        execute("CREATE TABLE " + tableName + " (n LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL"));
    }

    private QwpSchemaResponse describe(SecurityContext securityContext) throws Exception {
        return describe(securityContext, BUFFER_SIZE);
    }

    private QwpSchemaResponse describe(SecurityContext securityContext, int responseLimit) throws Exception {
        byte[] request = QwpSchemaProtocol.encodeDescribe(42, "schema_race");
        long requestAddress = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        long responseAddress = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(requestAddress + i, request[i]);
            }
            int length = (int) DESCRIBE.invoke(
                    null, engine, securityContext, requestAddress, request.length,
                    new StringSink(), responseAddress, responseLimit
            );
            QwpSchemaResponse response = QwpSchemaProtocol.decodeResponse(responseAddress, length);
            Assert.assertEquals(42, response.getRequestId());
            return response;
        } finally {
            Unsafe.free(responseAddress, BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(requestAddress, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void replaceTable(boolean isRename) {
        try {
            if (isRename) {
                execute("RENAME TABLE schema_race TO old_schema_race");
                execute("RENAME TABLE replacement TO schema_race");
            } else {
                execute("DROP TABLE schema_race");
                createTable("schema_race");
            }
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    private SecurityContext replacingContext(boolean isRename, AtomicInteger authorizations) {
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                Assert.assertEquals("schema_race", tableToken.getTableName());
                Assert.assertEquals(engine.verifyTableName("schema_race"), tableToken);
                if (authorizations.incrementAndGet() > 1) {
                    return;
                }
                // Deterministically replace the token between name lookup and metadata acquisition.
                replaceTable(isRename);
            }
        };
    }

    static {
        try {
            Class<?> schemaControl = Class.forName("io.questdb.cutlass.qwp.server.QwpSchemaControl");
            DESCRIBE = schemaControl.getDeclaredMethod(
                    "describe", CairoEngine.class, SecurityContext.class, long.class, int.class,
                    StringSink.class, long.class, int.class
            );
            DESCRIBE.setAccessible(true);
            ENCODE_FEEDBACK = schemaControl.getDeclaredMethod(
                    "encodeFeedback", CairoEngine.class, SecurityContext.class,
                    LowerCaseCharSequenceObjHashMap.class, long.class, int.class, int.class
            );
            ENCODE_FEEDBACK.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
