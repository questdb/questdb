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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.pgwire.PGMessageProcessingException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

public class PGParseMessageFuzzSmokeTest extends AbstractCairoTest {

    @Test
    public void testDirectParseFuzzSmoke() throws Exception {
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine)) {
                PGParseMessageFuzz.setHarness(harness);
                try {
                    PGParseMessageFuzz.fuzzerTestOneInput(newParseFrame("select 1"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newQueryFrame("select 1"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newQueryFrame("select 'm'::byte"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newQueryFrame("select ().file:"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newQueryFrame("ifl\u001fAstrakjaec\u001fepa::select`::selec\u001fepa:81"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newQueryFrame("\u001f2a(vil/ile.select 0et.ece.select &,6.f)5"));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGParseMessageFuzz.fuzzerTestOneInput(newFrame('P', 0));
                    PGParseMessageFuzz.fuzzerTestOneInput(newFrame('P', Integer.MAX_VALUE));
                    PGParseMessageFuzz.fuzzerTestOneInput(newParseWithTrailingBytesFrame());
                    PGParseMessageFuzz.fuzzerTestOneInput(newFrame('X', 4));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());
                } finally {
                    PGParseMessageFuzz.clearHarness(harness);
                }
            }
        });
    }

    @Test
    public void testWrappedUnexpectedRuntimeFailsFuzzing() throws Exception {
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine)) {
                PGMessageProcessingException ex = new PGMessageProcessingException();
                setFlyweightCause(ex, new NullPointerException("boom"));
                try {
                    harness.rethrowUnexpectedProcessingError(ex, "parse", new byte[]{'Q'});
                    Assert.fail();
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), "unexpected throwable hidden inside PGMessageProcessingException");
                    TestUtils.assertContains(e.getMessage(), "java.lang.NullPointerException: boom");
                }
            }
        });
    }

    @Test
    public void testWrappedCriticalCairoExceptionFailsFuzzing() throws Exception {
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine)) {
                PGMessageProcessingException ex = new PGMessageProcessingException();
                setFlyweightCause(ex, CairoException.critical(42).put("boom"));
                try {
                    harness.rethrowUnexpectedProcessingError(ex, "parse", new byte[]{'Q'});
                    Assert.fail();
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), "unexpected throwable hidden inside PGMessageProcessingException");
                    TestUtils.assertContains(e.getMessage(), "io.questdb.cairo.CairoException: [42] boom");
                }
            }
        });
    }

    private static byte[] newFrame(int type, int declaredLength) {
        final byte[] frame = new byte[5];
        frame[0] = (byte) type;
        putInt(frame, 1, declaredLength);
        return frame;
    }

    private static byte[] newParseFrame(String sql) {
        final byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        final byte[] frame = new byte[5 + 1 + sqlBytes.length + 1 + Short.BYTES];
        frame[0] = 'P';
        putInt(frame, 1, frame.length - 1);
        int p = 5;
        frame[p++] = 0;
        System.arraycopy(sqlBytes, 0, frame, p, sqlBytes.length);
        p += sqlBytes.length;
        frame[p++] = 0;
        frame[p++] = 0;
        frame[p] = 0;
        return frame;
    }

    private static byte[] newParseWithTrailingBytesFrame() {
        return new byte[]{
                'P',
                0, 0, 0, 16,
                0x0f, 0x27, 0,
                0,
                0, 0,
                0, '1', 0, 0, 0
        };
    }

    private static byte[] newQueryFrame(String sql) {
        final byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        final byte[] frame = new byte[5 + sqlBytes.length + 1];
        frame[0] = 'Q';
        putInt(frame, 1, frame.length - 1);
        System.arraycopy(sqlBytes, 0, frame, 5, sqlBytes.length);
        frame[frame.length - 1] = 0;
        return frame;
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static void setFlyweightCause(PGMessageProcessingException ex, Throwable cause) throws Exception {
        Field causeField = PGMessageProcessingException.class.getDeclaredField("flyweightCause");
        causeField.setAccessible(true);
        causeField.set(ex, cause);
    }
}
