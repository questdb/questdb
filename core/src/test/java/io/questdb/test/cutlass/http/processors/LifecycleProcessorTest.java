package io.questdb.test.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpResponseHeader;
import io.questdb.cutlass.http.processors.LifecycleProcessor;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.RestoreProgress;
import io.questdb.lifecycle.State;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class LifecycleProcessorTest {

    @Test
    public void testEmptyComponentsArray() throws Exception {
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, new ObjList<>());
        String body = captureResponseBody(snap);
        Assert.assertEquals("{\"capturedAtMicros\":1000,\"components\":[]}", body);
    }

    @Test
    public void testHardDepsArrayShape() throws Exception {
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        ObjList<String> hardDeps = new ObjList<>();
        hardDeps.add("factory-provider");
        hardDeps.add("backup-restore");
        components.add(new LifecycleSnapshot.ComponentSnapshot("engine", State.READY, 100L, null, hardDeps, new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);
        String body = captureResponseBody(snap);
        Assert.assertTrue("body must contain hardRequiredDependencies factory-provider + backup-restore",
                body.contains("\"hardRequiredDependencies\":[\"factory-provider\",\"backup-restore\"]"));
    }

    @Test
    public void testMultipleComponentsHaveCommaSeparator() throws Exception {
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        components.add(new LifecycleSnapshot.ComponentSnapshot("min-http", State.READY, 100L, null, new ObjList<>(), new ObjList<>()));
        components.add(new LifecycleSnapshot.ComponentSnapshot("engine", State.READY, 200L, null, listOf("backup-restore"), new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);
        String body = captureResponseBody(snap);
        int firstCompEnd = body.indexOf("},");
        int secondCompStart = body.indexOf("{\"name\":\"engine\"");
        Assert.assertTrue(firstCompEnd > 0 && secondCompStart > firstCompEnd);
        Assert.assertFalse("no trailing comma before ]}", body.contains(",]"));
    }

    @Test
    public void testSingleComponentWithNullProgress() throws Exception {
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        components.add(new LifecycleSnapshot.ComponentSnapshot("min-http", State.READY, 500L, null, listOf("factory-provider"), new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);
        String body = captureResponseBody(snap);
        Assert.assertTrue("body must contain latestProgress:null literal", body.contains("\"latestProgress\":null"));
        Assert.assertTrue("body must contain min-http name", body.contains("\"name\":\"min-http\""));
        Assert.assertTrue("body must contain state READY (uppercase)", body.contains("\"state\":\"READY\""));
        Assert.assertTrue("body must contain lastTransitionMicros 500", body.contains("\"lastTransitionMicros\":500"));
        Assert.assertTrue("body must contain hardRequiredDependencies factory-provider",
                body.contains("\"hardRequiredDependencies\":[\"factory-provider\"]"));
    }

    @Test
    public void testSingleComponentWithRestoreProgress() throws Exception {
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        components.add(new LifecycleSnapshot.ComponentSnapshot("backup-restore", State.STARTING, 600L,
                new RestoreProgress(3, 7, 0L, 0L), listOf("factory-provider"), new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);
        String body = captureResponseBody(snap);
        Assert.assertTrue("body must contain restore progress payload",
                body.contains("\"latestProgress\":{\"type\":\"restore\",\"tablesDone\":3,\"tablesTotal\":7,\"bytesDone\":0,\"bytesTotal\":0}"));
    }

    @Test
    public void testResumableSerializationAcrossBufferOverflow() throws Exception {
        // Build a snapshot whose full JSON (~700 bytes) far exceeds the 200-byte bounded buffer,
        // so the resumable serializer must overflow and flush several times to emit it all.
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        for (int i = 0; i < 5; i++) {
            components.add(new LifecycleSnapshot.ComponentSnapshot(
                    "comp" + i, State.READY, 100L + i, null, listOf("factory-provider"), new ObjList<>()));
        }
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);

        // Canonical full body via the non-resumable writer.
        StringBuilder full = new StringBuilder();
        LifecycleProcessor.writeSnapshot(new FakeHttpChunkedResponse(full), snap);

        // Resumable writer through a small bounded buffer that forces overflow/flush cycles.
        BoundedFakeHttpChunkedResponse bounded = new BoundedFakeHttpChunkedResponse(200);
        LifecycleProcessor.writeSnapshotResumable(bounded, snap);

        Assert.assertEquals(
                "resumable output across overflow boundaries must equal the full snapshot",
                full.toString(), bounded.flushed.toString());
        Assert.assertTrue(
                "the bounded buffer must have forced at least one mid-write flush; partialFlushes="
                        + bounded.partialFlushes,
                bounded.partialFlushes > 0);
    }

    @Test
    public void testStateNameUppercase() throws Exception {
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        components.add(new LifecycleSnapshot.ComponentSnapshot("min-http", State.READY, 100L, null, new ObjList<>(), new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(2000L, components);
        String body = captureResponseBody(snap);
        Assert.assertTrue("state must be uppercase READY", body.contains("\"state\":\"READY\""));
    }

    @Test
    public void testTerminalFlushParkResumesAndNeverStrands() throws Exception {
        // A slow reader can park the terminal sendChunk(true) with PeerIsSlowToReadException. The
        // serializer must keep the per-connection state resumable through that park so the framework's
        // later resumeSend re-issues exactly that final flush, instead of nulling the snapshot before
        // the flush and stranding the buffered terminal chunk until idle-timeout.
        ObjList<LifecycleSnapshot.ComponentSnapshot> components = new ObjList<>();
        components.add(new LifecycleSnapshot.ComponentSnapshot("min-http", State.READY, 100L, null, listOf("factory-provider"), new ObjList<>()));
        LifecycleSnapshot snap = new LifecycleSnapshot(1000L, components);

        // Canonical full body via the non-resumable writer.
        StringBuilder full = new StringBuilder();
        LifecycleProcessor.writeSnapshot(new FakeHttpChunkedResponse(full), snap);

        // A buffer large enough to hold the whole body in one pass, so the only sendChunk is the
        // terminal one -- isolating the slow-reader park to the final flush.
        BoundedFakeHttpChunkedResponse bounded = new BoundedFakeHttpChunkedResponse(4096);
        bounded.throwOnceOnTerminal = true;

        // Caller-held state shared across the park and the resume (a fresh-state-per-call writer cannot
        // model this).
        LifecycleProcessor.LifecycleState state = LifecycleProcessor.LifecycleState.newTestState(snap);

        // First drive: the terminal flush parks. The state must stay resumable -- snapshot non-null and
        // the terminal not yet marked flushed -- so the framework's resumeSend can re-enter and finish.
        try {
            LifecycleProcessor.writeSnapshotResumable(bounded, state);
            Assert.fail("the terminal flush must park with PeerIsSlowToReadException");
        } catch (PeerIsSlowToReadException expected) {
            // The framework parks the connection here and calls resumeSend later.
        }
        Assert.assertTrue(
                "snapshot must remain non-null through the slow-reader park so resumeSend re-issues the terminal flush",
                state.hasSnapshot());
        Assert.assertFalse("the terminal flush must not be marked done while it is still parked", state.isTerminalFlushed());
        Assert.assertEquals("no terminal flush has succeeded yet", 0, bounded.terminalFlushes);

        // Resume: the reader has drained, the terminal flush succeeds, the full body is emitted, and the
        // state is finalized only after the successful flush.
        LifecycleProcessor.writeSnapshotResumable(bounded, state);
        Assert.assertEquals(
                "the resumed terminal flush must emit the full snapshot body",
                full.toString(), bounded.flushed.toString());
        Assert.assertEquals("the terminal flush must have succeeded exactly once on resume", 1, bounded.terminalFlushes);
        Assert.assertTrue("the terminal flush must be marked done after a successful flush", state.isTerminalFlushed());
        Assert.assertFalse("snapshot must be nulled only after the terminal flush succeeds", state.hasSnapshot());
    }

    private static String captureResponseBody(LifecycleSnapshot snap) throws Exception {
        StringBuilder buf = new StringBuilder();
        LifecycleProcessor.writeSnapshot(new FakeHttpChunkedResponse(buf), snap);
        return buf.toString();
    }

    private static ObjList<String> listOf(String... items) {
        ObjList<String> list = new ObjList<>();
        for (String s : items) {
            list.add(s);
        }
        return list;
    }

    private static final class FakeHttpChunkedResponse implements HttpChunkedResponse {
        private final StringBuilder buf;
        private int[] ryuScratch;

        FakeHttpChunkedResponse(StringBuilder buf) {
            this.buf = buf;
        }

        @Override
        public void bookmark() {
        }

        @Override
        public void done() throws PeerDisconnectedException, PeerIsSlowToReadException {
        }

        @Override
        public HttpResponseHeader headers() {
            return null;
        }

        @Override
        public @NotNull FakeHttpChunkedResponse put(byte b) {
            buf.append((char) (b & 0xFF));
            return this;
        }

        @Override
        public @NotNull FakeHttpChunkedResponse put(@Nullable Utf8Sequence us) {
            if (us != null) {
                buf.append(us.toString());
            }
            return this;
        }

        @Override
        public @NotNull FakeHttpChunkedResponse putNonAscii(long lo, long hi) {
            // not needed in tests; all strings in JSON output are ASCII
            return this;
        }

        @Override
        public int[] ryuScratch() {
            if (ryuScratch == null) {
                ryuScratch = new int[1];
            }
            return ryuScratch;
        }

        @Override
        public boolean resetToBookmark() {
            return false;
        }

        @Override
        public void sendChunk(boolean done) throws PeerDisconnectedException, PeerIsSlowToReadException {
        }

        @Override
        public void sendHeader() throws PeerDisconnectedException, PeerIsSlowToReadException {
        }

        @Override
        public void shutdownWrite() {
        }

        @Override
        public void status(int status, CharSequence contentType) {
        }

        @Override
        public int writeBytes(long srcAddr, int len) {
            return len;
        }
    }

    /**
     * A fake response with a fixed-size working buffer that throws
     * {@link NoSpaceLeftInResponseBufferException} once the buffer is full, mirroring the real
     * HttpChunkedResponse overflow contract so the resumable serializer's bookmark / reset /
     * sendChunk loop can be exercised without a live HTTP server. {@code bookmark} marks the
     * current write position; {@code resetToBookmark} rewinds to it and reports whether there is
     * committed content before the mark to flush; {@code sendChunk} flushes the buffer into
     * {@code flushed} and clears it.
     */
    private static final class BoundedFakeHttpChunkedResponse implements HttpChunkedResponse {
        final StringBuilder flushed = new StringBuilder();
        int partialFlushes;
        int terminalFlushes;
        // When true, the first terminal sendChunk(true) throws PeerIsSlowToReadException exactly once
        // (mirroring a slow reader whose TCP buffer is full on the final flush) and then clears the
        // flag, so a re-driven resume can complete the flush.
        boolean throwOnceOnTerminal;
        private final int cap;
        private final StringBuilder working = new StringBuilder();
        private int bookmark;
        private int[] ryuScratch;

        BoundedFakeHttpChunkedResponse(int cap) {
            this.cap = cap;
        }

        @Override
        public void bookmark() {
            bookmark = working.length();
        }

        @Override
        public void done() throws PeerDisconnectedException, PeerIsSlowToReadException {
        }

        @Override
        public HttpResponseHeader headers() {
            return null;
        }

        @Override
        public @NotNull BoundedFakeHttpChunkedResponse put(byte b) {
            if (working.length() + 1 > cap) {
                throw NoSpaceLeftInResponseBufferException.instance(1, cap - working.length(), cap);
            }
            working.append((char) (b & 0xFF));
            return this;
        }

        @Override
        public @NotNull BoundedFakeHttpChunkedResponse put(@Nullable Utf8Sequence us) {
            if (us != null) {
                final String s = us.toString();
                for (int i = 0, n = s.length(); i < n; i++) {
                    put((byte) s.charAt(i));
                }
            }
            return this;
        }

        @Override
        public @NotNull BoundedFakeHttpChunkedResponse putNonAscii(long lo, long hi) {
            return this;
        }

        @Override
        public int[] ryuScratch() {
            if (ryuScratch == null) {
                ryuScratch = new int[1];
            }
            return ryuScratch;
        }

        @Override
        public boolean resetToBookmark() {
            working.setLength(bookmark);
            // True only when there is committed content before the bookmark that flushing can clear;
            // false when the bookmark is at the buffer start (the unit alone exceeds capacity).
            return bookmark > 0;
        }

        @Override
        public void sendChunk(boolean done) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (done) {
                if (throwOnceOnTerminal) {
                    throwOnceOnTerminal = false;
                    // The slow reader's TCP buffer is full on the terminal flush; the framework parks
                    // the connection and later calls resumeSend. The working buffer is left intact so
                    // the re-driven terminal flush emits the same residual.
                    throw PeerIsSlowToReadException.INSTANCE;
                }
                terminalFlushes++;
            } else {
                partialFlushes++;
            }
            flushed.append(working);
            working.setLength(0);
            bookmark = 0;
        }

        @Override
        public void sendHeader() throws PeerDisconnectedException, PeerIsSlowToReadException {
        }

        @Override
        public void shutdownWrite() {
        }

        @Override
        public void status(int status, CharSequence contentType) {
        }

        @Override
        public int writeBytes(long srcAddr, int len) {
            return len;
        }
    }
}
