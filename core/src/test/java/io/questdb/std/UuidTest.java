/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.StringSink;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class UuidTest {

    @Test
    public void testEdgeCases() {
        assertEqualsString(0, 0);
        assertEqualsString(1, 1);
        assertEqualsString(-1, -1);
        assertEqualsString(Long.MAX_VALUE, Long.MAX_VALUE);
        assertEqualsString(Long.MIN_VALUE, Long.MIN_VALUE);
        assertEqualsString(Long.MIN_VALUE, 1);
    }

    @Test
    public void testEqualsAndHashcode() throws Exception {
        Uuid m1 = new Uuid();
        Uuid m2 = new Uuid();
        for (int i = 0; i < 100; i++) {
            UUID uuid = UUID.randomUUID();
            m1.of(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
            m2.of(uuid.toString());
            assertEquals(m1, m1);
            assertEquals(m1, m2);
            assertEquals(m1.hashCode(), m2.hashCode());

            m2.of(UUID.randomUUID().toString());
            assertNotEquals(m1, m2);
        }
        assertNotEquals(m1, null);
        assertNotEquals(m1, "foo");

        m1.of(0, 1);
        m1.of(0, 2);
        assertNotEquals(m1, m2);

        m1.of(1, 0);
        m1.of(2, 0);
        assertNotEquals(m1, m2);
    }

    @Test
    public void testHappyParsing() throws Exception {
        UUID javaUuid = UUID.randomUUID();

        Uuid uuid = new Uuid();
        uuid.of(javaUuid.toString());

        assertEqualsBitS(javaUuid, uuid);
    }

    @Test
    public void testHappySinking() {
        UUID javaUuid = UUID.randomUUID();
        Uuid uuid = new Uuid(javaUuid.getLeastSignificantBits(), javaUuid.getMostSignificantBits());

        assertEqualsString(javaUuid, uuid);
    }

    @Test
    public void testInvalid() {
        assertExceptionWhileParsing("a0eebc99-9c0b-4ef8-bb6d");
        assertExceptionWhileParsing("a0eebc99-9c0b-4ef8-bb6d-");
        assertExceptionWhileParsing("a0eebc99-9c0b-4ef8-");
        assertExceptionWhileParsing("11111111-1111-1111-коль-111111111111");
    }

    @Test
    public void testInvalidOffsets() {
        String uuids = "'11111111-1111-1111-1111-111111111111'";
        assertExceptionWhileParsing(uuids, 0, 0);
        assertExceptionWhileParsing(uuids, 10, 0);
        assertExceptionWhileParsing(uuids, 3, uuids.length() - 1);
        assertExceptionWhileParsing(uuids, 1, uuids.length() + 1);
    }

    @Test
    public void testRandomized() {
        for (int i = 0; i < 100_000; i++) {
            long lo = ThreadLocalRandom.current().nextLong();
            long hi = ThreadLocalRandom.current().nextLong();
            assertEqualsString(lo, hi);
        }
    }

    @Test
    public void testSmoke() {
        assertEqualsString(-4442449726822927731L, -8889930662239044040L);
    }

    @Test
    public void testUpperCaseUuid() throws Exception {
        // rfc4122 section 3 says:
        // The hexadecimal values "a" through "f" are output as lower case characters and are case insensitive on input.

        Uuid mutableUuid = new Uuid();
        for (int i = 0; i < 100; i++) {
            UUID uuid = UUID.randomUUID();
            mutableUuid.of(uuid.toString().toUpperCase());
            assertEquals(uuid.getMostSignificantBits(), mutableUuid.getHi());
            assertEquals(uuid.getLeastSignificantBits(), mutableUuid.getLo());
        }
    }

    @Test
    public void testWithOffsets() throws NumericException {
        UUID juuid = UUID.randomUUID();
        String prefix = "prefix";
        String suffix = "suffix";
        String withUuid = prefix + juuid + suffix;

        Uuid uuid = new Uuid();
        uuid.of(withUuid, prefix.length(), withUuid.length() - suffix.length());
        assertEqualsBitS(juuid, uuid);

        UUID juuid1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Uuid uuid1 = new Uuid();
        String uuids = "'11111111-1111-1111-1111-111111111111'";
        uuid1.of(uuids, 1, uuids.length() - 1);
        assertEqualsBitS(juuid1, uuid1);
    }

    private static void assertEqualsBitS(UUID expected, Uuid actual) {
        assertEquals("Bad parsing " + expected, expected.getMostSignificantBits(), actual.getHi());
        assertEquals("Bad parsing " + expected, expected.getLeastSignificantBits(), actual.getLo());
    }

    private static void assertEqualsString(long lo, long hi) {
        UUID javaUuid = new UUID(hi, lo);
        Uuid uuid = new Uuid();
        uuid.of(lo, hi);

        assertEqualsString(javaUuid, uuid);
    }

    private static void assertEqualsString(UUID expected, Uuid actual) {
        StringSink sink = new StringSink();
        actual.toSink(sink);

        assertEquals("Bad string representation for UUID '" + expected + "'", expected.toString(), sink.toString());
    }

    private static void assertExceptionWhileParsing(String uuid) {
        assertExceptionWhileParsing(uuid, 0, 0);
    }

    private static void assertExceptionWhileParsing(String uuid, int lo, int hi) {
        Uuid muuid = new Uuid();
        try {
            muuid.of(uuid, lo, hi);
            fail();
        } catch (NumericException expected) {

        }
    }
}
