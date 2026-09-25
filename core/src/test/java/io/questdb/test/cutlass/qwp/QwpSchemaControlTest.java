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
import io.questdb.test.AbstractCairoTest;
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
                        null, engine, replacingContext(isRename, authorizations), tableNames, address, BUFFER_SIZE
                ));
                Assert.assertEquals(1, authorizations.get());
                TableToken after = engine.verifyTableName("schema_race");
                Assert.assertNotEquals(before.getTableId(), after.getTableId());
                Assert.assertTrue((int) ENCODE_FEEDBACK.invoke(
                        null, engine, AllowAllSecurityContext.INSTANCE, tableNames, address, BUFFER_SIZE
                ) > 0);
            } finally {
                Unsafe.free(address, BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void createTable(String tableName) throws SqlException {
        execute("CREATE TABLE " + tableName + " (n LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL"));
    }

    private QwpSchemaResponse describe(SecurityContext securityContext) throws Exception {
        byte[] request = QwpSchemaProtocol.encodeDescribe(42, "schema_race");
        long requestAddress = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        long responseAddress = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(requestAddress + i, request[i]);
            }
            int length = (int) DESCRIBE.invoke(
                    null, engine, securityContext, requestAddress, request.length,
                    new StringSink(), responseAddress, BUFFER_SIZE
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
                    LowerCaseCharSequenceObjHashMap.class, long.class, int.class
            );
            ENCODE_FEEDBACK.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

}
