package io.questdb.test.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpResponseHeader;
import io.questdb.cutlass.http.processors.SwitchProcessor;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.SwitchInFlightException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SwitchProcessorTest {

    @Test
    public void testEmptyBodyReturns400() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        try (DirectUtf8Sink body = new DirectUtf8Sink(64)) {
            SwitchProcessor.handlePost(orch, body, r);
        }
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"error\":\"invalid_request\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("role required"));
        Assert.assertNull("submitSwitch must not be called", orch.lastRequestedRole);
        Assert.assertEquals("timeout setter must not be called", 0, orch.timeoutSetterCalls.get());
    }

    @Test
    public void testInvalidRoleReturns400() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"banana\"}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("role required"));
        Assert.assertNull(orch.lastRequestedRole);
    }

    @Test
    public void testMalformedJsonReturns400() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{not valid", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"error\":\"invalid_request\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("malformed JSON"));
        Assert.assertNull(orch.lastRequestedRole);
    }

    @Test
    public void testNegativeTimeoutReturns400() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"primary\",\"timeout_ms\":-1}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("timeout_ms must be non-negative"));
        Assert.assertNull(orch.lastRequestedRole);
        // Validation runs BEFORE the orchestrator setter is invoked.
        Assert.assertEquals("timeout setter must not be called when timeout is invalid",
                0, orch.timeoutSetterCalls.get());
    }

    @Test
    public void testNonPostReturns405() throws Exception {
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        SwitchProcessor.writeMethodNotAllowed(r);
        Assert.assertEquals(405, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"error\":\"method_not_allowed\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("POST required"));
    }

    @Test
    public void testSwitchInFlightReturns409() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        orch.throwOnSubmit = new SwitchInFlightException(Role.PRIMARY, Role.REPLICA);
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"replica\"}", r);
        Assert.assertEquals(409, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"error\":\"switch_in_flight\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"current_role\":\"PRIMARY\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"target_role\":\"REPLICA\""));
    }

    @Test
    public void testTimeoutMsAbsentDoesNotCallSetter() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"primary\"}", r);
        Assert.assertEquals(202, r.statusCode);
        Assert.assertEquals(Role.PRIMARY, orch.lastRequestedRole);
        Assert.assertEquals("timeout setter must not be called when timeout_ms is absent",
                0, orch.timeoutSetterCalls.get());
    }

    @Test
    public void testTimeoutMsThreadedToOrchestrator() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"primary\",\"timeout_ms\":12345}", r);
        Assert.assertEquals(202, r.statusCode);
        Assert.assertEquals(Role.PRIMARY, orch.lastRequestedRole);
        Assert.assertEquals(1, orch.timeoutSetterCalls.get());
        Assert.assertEquals(12_345L, orch.capturedTimeoutMs.get());
    }

    @Test
    public void testUnknownFieldReturns400() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"primary\",\"expected_cutoff\":\"abc\"}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("unknown field: expected_cutoff"));
        Assert.assertNull(orch.lastRequestedRole);
    }

    @Test
    public void testNestedObjectReturns400() throws Exception {
        // WR-02 regression: {"role":{"x":"y"}} previously parsed as unknownField="x" with a
        // misleading 400; worse, {"role":{"role":"primary"}} accidentally succeeded. With object
        // depth tracking, both bodies now fail with the "malformed JSON" path because the parser
        // raises JsonException at the inner OBJ_START.
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":{\"x\":\"y\"}}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("\"error\":\"invalid_request\""));
        Assert.assertTrue("body: " + r.body(), r.body().contains("malformed JSON"));
        Assert.assertNull("submitSwitch must not be called for nested objects", orch.lastRequestedRole);
    }

    @Test
    public void testNestedObjectWithSameKeyReturns400() throws Exception {
        // WR-02 regression: the pre-fix parser would accidentally parse {"role":{"role":"primary"}}
        // as parsedRole=PRIMARY because the inner NAME overwrote pendingName silently. Now: the
        // inner OBJ_START is rejected before any inner NAME/VALUE is honoured.
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":{\"role\":\"primary\"}}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("malformed JSON"));
        Assert.assertNull(orch.lastRequestedRole);
    }

    @Test
    public void testTopLevelArrayReturns400() throws Exception {
        // WR-02 regression: arrays anywhere in the body are rejected outright.
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":[\"primary\"]}", r);
        Assert.assertEquals(400, r.statusCode);
        Assert.assertTrue("body: " + r.body(), r.body().contains("malformed JSON"));
        Assert.assertNull(orch.lastRequestedRole);
    }

    @Test
    public void testValidPostReturns202() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"primary\"}", r);
        Assert.assertEquals(202, r.statusCode);
        Assert.assertEquals("{\"accepted\":true}", r.body());
        Assert.assertEquals(Role.PRIMARY, orch.lastRequestedRole);
    }

    @Test
    public void testValidReplicaPostReturns202() throws Exception {
        StubOrchestrator orch = new StubOrchestrator();
        FakeHttpChunkedResponse r = new FakeHttpChunkedResponse();
        runHandlePost(orch, "{\"role\":\"replica\"}", r);
        Assert.assertEquals(202, r.statusCode);
        Assert.assertEquals(Role.REPLICA, orch.lastRequestedRole);
    }

    private static void runHandlePost(LifecycleOrchestrator orch, String json, FakeHttpChunkedResponse r) throws Exception {
        try (DirectUtf8Sink body = new DirectUtf8Sink(64)) {
            body.put(json);
            SwitchProcessor.handlePost(orch, body, r);
        }
    }

    static final class FakeHttpChunkedResponse implements HttpChunkedResponse {
        private final StringBuilder buf = new StringBuilder();
        int statusCode = -1;

        String body() {
            return buf.toString();
        }

        @Override
        public void bookmark() {
        }

        @Override
        public void done() {
        }

        @Override
        public HttpResponseHeader headers() {
            return null;
        }

        @Override
        public @NotNull HttpChunkedResponse put(byte b) {
            buf.append((char) (b & 0xFF));
            return this;
        }

        @Override
        public @NotNull HttpChunkedResponse put(@Nullable Utf8Sequence us) {
            if (us != null) {
                buf.append(us);
            }
            return this;
        }

        @Override
        public @NotNull HttpChunkedResponse putNonAscii(long lo, long hi) {
            return this;
        }

        @Override
        public boolean resetToBookmark() {
            return false;
        }

        @Override
        public void sendChunk(boolean done) {
        }

        @Override
        public void sendHeader() {
        }

        @Override
        public void shutdownWrite() {
        }

        @Override
        public void status(int status, CharSequence contentType) {
            this.statusCode = status;
        }

        @Override
        public int writeBytes(long srcAddr, int len) {
            return len;
        }
    }

    /**
     * Hand-rolled stub: overrides only the entry points SwitchProcessor calls.
     * Avoids constructing the real ThreadPoolExecutor by extending and overriding both
     * {@link LifecycleOrchestrator#submitSwitch} and
     * {@link LifecycleOrchestrator#setRoleSwitchTimeoutMs}.
     */
    static final class StubOrchestrator extends LifecycleOrchestrator {
        final AtomicLong capturedTimeoutMs = new AtomicLong(-1L);
        Role lastRequestedRole;
        final AtomicReference<Role> roleRef = new AtomicReference<>(Role.PRIMARY);
        SwitchInFlightException throwOnSubmit;
        final AtomicInteger timeoutSetterCalls = new AtomicInteger();

        StubOrchestrator() {
            super(Role.PRIMARY, null, null, null);
        }

        @Override
        public void setRoleSwitchTimeoutMs(long ms) {
            capturedTimeoutMs.set(ms);
            timeoutSetterCalls.incrementAndGet();
        }

        @Override
        public void submitSwitch(Role newRole) {
            lastRequestedRole = newRole;
            if (throwOnSubmit != null) {
                throw throwOnSubmit;
            }
        }
    }
}
