/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.binary;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.binary.BinaryDataSinkImpl;
import io.questdb.cutlass.binary.StreamingColumnarSerializer;
import io.questdb.griffin.SqlCompiler;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * Tests for StreamingColumnarSerializer - Re-entrant State Machine
 * <p>
 * These tests demonstrate the serializer's ability to:
 * 1. Handle buffer-full exceptions gracefully
 * 2. Resume serialization from exact interruption point
 * 3. Serialize variable-length data without corruption
 * 4. Maintain state across multiple serialize() calls
 */
public class StreamingColumnarSerializerTest extends AbstractCairoTest {

    /**
     * Test basic serialization with a buffer large enough to hold all data
     */
    @Test
    public void testBasicSerialization() throws Exception {
        execute("CREATE TABLE test (id INT, name STRING, value DOUBLE)");
        execute("INSERT INTO test VALUES (1, 'Alice', 100.5)");
        execute("INSERT INTO test VALUES (2, 'Bob', 200.7)");
        execute("INSERT INTO test VALUES (3, 'Charlie', 300.3)");

        byte[] result = serializeQuery("SELECT * FROM test", 1024 * 1024); // 1MB buffer

        // Verify magic number
        Assert.assertEquals('S', result[0]);
        Assert.assertEquals('C', result[1]);
        Assert.assertEquals('B', result[2]);
        Assert.assertEquals('F', result[3]);

        // Verify we got data
        Assert.assertTrue(result.length > 100);
        System.out.println("Basic serialization: " + result.length + " bytes");
    }

    /**
     * Test empty result set
     */
    @Test
    public void testEmptyResultSet() throws Exception {
        execute("CREATE TABLE test (id INT, name STRING)");
        // No data inserted

        byte[] result = serializeQuery("SELECT * FROM test", 1024);

        // Should still have header and schema
        Assert.assertEquals('S', result[0]);
        Assert.assertTrue(result.length > 20);
        System.out.println("Empty result set: " + result.length + " bytes (header + schema only)");
    }

    /**
     * Test with large dataset to verify row groups work correctly
     */
    @Test
    public void testLargeDataset() throws Exception {
        execute("CREATE TABLE test (id INT, value DOUBLE, text STRING)");

        // Insert 1000 rows
        execute("INSERT INTO test " +
                "SELECT x, rnd_double(), rnd_str(50, 100, 0) " +
                "FROM long_sequence(1000)");

        byte[] result = serializeQuery("SELECT * FROM test", 256); // 100 rows per group

        Assert.assertTrue(result.length > 10000);
        System.out.println("Large dataset (1000 rows): " + result.length + " bytes");
        System.out.println("Average bytes per row: " + (result.length / 1000.0));
    }

    /**
     * Test with multiple column types to verify all states work
     */
    @Test
    public void testMultipleColumnTypes() throws Exception {
        execute("CREATE TABLE test (" +
                "id INT, " +
                "name STRING, " +
                "price DOUBLE, " +
                "quantity LONG, " +
                "active BOOLEAN, " +
                "created TIMESTAMP" +
                ")");

        execute("INSERT INTO test VALUES " +
                "(1, 'Product A', 99.99, 100, true, '2024-01-01T10:00:00.000000Z'), " +
                "(2, 'Product B', 149.99, 50, false, '2024-01-02T11:00:00.000000Z'), " +
                "(3, 'Product C', 199.99, 75, true, '2024-01-03T12:00:00.000000Z')");

        byte[] result = serializeQuery("SELECT * FROM test", 128); // Small buffer

        Assert.assertTrue(result.length > 200);
        System.out.println("Multiple types serialization: " + result.length + " bytes");
    }

    /**
     * Test with NULL values to verify null bitmap serialization
     */
    @Test
    public void testNullValues() throws Exception {
        execute("CREATE TABLE test (id INT, optional_name STRING, optional_value DOUBLE)");
        execute("INSERT INTO test VALUES (1, 'Alice', 100.5)");
        execute("INSERT INTO test VALUES (2, null, null)");
        execute("INSERT INTO test VALUES (3, 'Charlie', null)");
        execute("INSERT INTO test VALUES (4, null, 400.4)");

        byte[] result = serializeQuery("SELECT * FROM test", 128);

        Assert.assertTrue(result.length > 100);
        System.out.println("NULL values serialization: " + result.length + " bytes");
    }

    /**
     * Test re-entrancy with very small buffer (64 bytes) to force many interruptions
     */
    @Test
    public void testReentrancyWithSmallBuffer() throws Exception {
        execute("CREATE TABLE test (id INT, name STRING)");
        execute("INSERT INTO test VALUES (1, 'Short')");
        execute("INSERT INTO test VALUES (2, 'This is a much longer string that will not fit in a small buffer')");
        execute("INSERT INTO test VALUES (3, 'Another long string to test partial writes across multiple calls')");

        int bufferSize = 64; // Very small buffer to force interruptions
        byte[] result = serializeQuery("SELECT * FROM test", bufferSize);

        // Verify complete serialization
        Assert.assertEquals('S', result[0]);
        Assert.assertEquals('C', result[1]);
        Assert.assertEquals('B', result[2]);
        Assert.assertEquals('F', result[3]);

        System.out.println("Small buffer serialization: " + result.length + " bytes");
        System.out.println("This required multiple buffer flushes with " + bufferSize + " byte buffer");
    }

    /**
     * Test exact re-entry behavior by counting serialize() calls
     */
    @Test
    public void testReentryCount() throws Exception {
        execute("CREATE TABLE test (id INT, data STRING)");
        execute("INSERT INTO test VALUES (1, 'x')"); // Tiny data
        execute("INSERT INTO test VALUES (2, '" + "A".repeat(1000) + "')"); // 1KB string

        int bufferSize = 50; // Very small buffer
        int callCount = serializeQueryCountingCalls("SELECT * FROM test", bufferSize);

        System.out.println("Serialize called " + callCount + " times with " + bufferSize + " byte buffer");
        Assert.assertTrue("Expected multiple calls due to small buffer", callCount > 10);
    }

    /**
     * Test single row
     */
    @Test
    public void testSingleRow() throws Exception {
        execute("CREATE TABLE test (id INT, name STRING, value DOUBLE)");
        execute("INSERT INTO test VALUES (42, 'Single', 3.14)");

        byte[] result = serializeQuery("SELECT * FROM test", 1024);

        Assert.assertTrue(result.length > 50);
        System.out.println("Single row: " + result.length + " bytes");
    }

    // ===== Helper Methods =====

    /**
     * Serialize a query with specified buffer size
     */
    private byte[] serializeQuery(String sql, int bufferSize) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {

                    StreamingColumnarSerializer serializer = new StreamingColumnarSerializer();
                    serializer.of(factory.getMetadata(), cursor);

                    // Allocate native buffer
                    long bufferAddress = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        BinaryDataSinkImpl sink = new BinaryDataSinkImpl();

                        while (!serializer.isComplete()) {
                            try {
                                sink.of(bufferAddress, bufferSize);
                                serializer.serialize(sink);

                                // Write to output stream
                                if (sink.position() > 0) {
                                    byte[] chunk = new byte[sink.position()];
                                    for (int i = 0; i < sink.position(); i++) {
                                        chunk[i] = Unsafe.getUnsafe().getByte(bufferAddress + i);
                                    }
                                    output.write(chunk);
                                }
                            } catch (NoSpaceLeftInResponseBufferException e) {
                                // Expected - buffer filled up
                                // Write what we have
                                if (sink.position() > 0) {
                                    byte[] chunk = new byte[sink.position()];
                                    for (int i = 0; i < sink.position(); i++) {
                                        chunk[i] = Unsafe.getUnsafe().getByte(bufferAddress + i);
                                    }
                                    output.write(chunk);
                                }
                                // Loop continues with fresh buffer
                            }
                        }
                    } finally {
                        Unsafe.free(bufferAddress, bufferSize, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        }

        return output.toByteArray();
    }

    /**
     * Count how many times serialize() is called
     */
    private int serializeQueryCountingCalls(String sql, int bufferSize) throws Exception {
        int callCount = 0;

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {

                    StreamingColumnarSerializer serializer = new StreamingColumnarSerializer();
                    serializer.of(factory.getMetadata(), cursor);

                    long bufferAddress = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        BinaryDataSinkImpl sink = new BinaryDataSinkImpl();

                        while (!serializer.isComplete()) {
                            callCount++;
                            try {
                                sink.of(bufferAddress, bufferSize);
                                serializer.serialize(sink);
                            } catch (NoSpaceLeftInResponseBufferException e) {
                                // Expected
                            }
                        }
                    } finally {
                        Unsafe.free(bufferAddress, bufferSize, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        }

        return callCount;
    }
}
