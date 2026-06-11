package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.ProgressEvent;
import io.questdb.lifecycle.RestoreProgress;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.ObjList;

import java.util.function.Supplier;

/**
 * HTTP request handler for the {@code GET /lifecycle} endpoint.
 * <p>
 * Serializes the current {@link LifecycleSnapshot} as camelCase JSON. The
 * snapshot is fetched from the provided supplier on every request -- no
 * shared mutable state, zero GC on the data path.
 * <p>
 * This serializer uses {@code putAsciiQuoted} which does NOT escape JSON
 * strings. Correctness relies on the invariant that all component names match
 * {@code [a-z0-9-]+} -- enforced at registration in
 * {@code LifecycleOrchestrator.register}. State names and field labels are emitted
 * from compile-time literals / enum constants, so they are JSON-safe by construction.
 * Dependency-list entries are component names and therefore also satisfy the invariant.
 * <p>
 * JSON shape (stable; adding fields later is fine, renaming is not):
 * <pre>
 * {
 *   "capturedAtMicros": 1234567890,
 *   "components": [
 *     {
 *       "name": "min-http",
 *       "state": "READY",
 *       "lastTransitionMicros": 1000,
 *       "latestProgress": null,
 *       "hardRequiredDependencies": ["factory-provider"],
 *       "softDependencies": []
 *     }
 *   ]
 * }
 * </pre>
 */
public class LifecycleProcessor implements HttpRequestHandler, HttpRequestProcessor {
    private static final LocalValue<LifecycleState> LV = new LocalValue<>();
    private final byte requiredAuthType;
    private final Supplier<LifecycleSnapshot> snapshotSupplier;

    public LifecycleProcessor(HttpServerConfiguration configuration, Supplier<LifecycleSnapshot> snapshotSupplier) {
        this.requiredAuthType = configuration.getRequiredAuthType();
        this.snapshotSupplier = snapshotSupplier;
    }

    @Override
    public HttpRequestProcessor getDefaultProcessor() {
        return this;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpChunkedResponse response = context.getChunkedResponse();

        // Fetch the snapshot holder once (a subclass may return an extended type) and initialise the
        // per-connection cursor BEFORE sending the header. If sendHeader() parks the connection on a
        // slow reader (PeerIsSlowToReadException), the framework later calls resumeSend(); that path
        // returns early when the per-connection state is null, so initialising the state first lets a
        // slow reader's response actually complete instead of hanging with no response ever. The same
        // holder is reused across every resume call so the response cannot tear mid-flight.
        final Object holder = captureSnapshot();
        LifecycleState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new LifecycleState());
        }
        state.reset(holder, toBaseSnapshot(holder));

        response.status(200, "application/json; charset=utf-8");
        response.sendHeader();

        resumeSnapshot(response, state);
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final LifecycleState state = LV.get(context);
        if (state == null || state.snapshot == null) {
            // No pending serialisation: nothing to resume.
            return;
        }
        resumeSnapshot(context.getChunkedResponse(), state);
    }

    /**
     * Captures the snapshot to serialize. The base captures a {@link LifecycleSnapshot} from the
     * supplier; a subclass may return an extended holder type (carrying extra header fields) as long
     * as {@link #toBaseSnapshot(Object)} can recover the base from it. Called once per request so the
     * resume loop reuses the same holder across every {@link #resumeSend} call.
     */
    protected Object captureSnapshot() {
        return snapshotSupplier.get();
    }

    /**
     * Recovers the base {@link LifecycleSnapshot} (whose components the loop iterates) from the holder
     * produced by {@link #captureSnapshot()}. The base holder IS a {@link LifecycleSnapshot}.
     */
    protected LifecycleSnapshot toBaseSnapshot(Object holder) {
        return (LifecycleSnapshot) holder;
    }

    /**
     * Hook for a subclass to inject extra JSON header fields between {@code capturedAtMicros} and the
     * {@code components} array. Each field this writes MUST end with a trailing comma. The base writes
     * nothing. The header is one bookmark unit, so an overflow rewinds and re-runs this hook -- keep
     * it side-effect free.
     */
    protected void writeHeaderExtraFields(HttpChunkedResponse response, Object holder) {
        // Base lifecycle JSON has no extra header fields.
    }

    /**
     * Serializes the lifecycle snapshot into the response buffer, resuming from the cursor saved in
     * {@code state} across multiple calls if the TCP send buffer fills. A snapshot of a
     * fully-populated server can exceed the default {@code http.min.send.buffer.size} of 1024 bytes,
     * so each phase places a bookmark before each write and, on
     * {@link NoSpaceLeftInResponseBufferException}, rewinds to the bookmark and flushes the buffered
     * chunk with {@code sendChunk(false)} before retrying. If the TCP send buffer is also full,
     * {@code sendChunk(false)} throws {@link PeerIsSlowToReadException}, which propagates out so the
     * HTTP framework parks the connection and calls {@link #resumeSend} when the socket drains. This
     * mirrors the OSS JSON query processor and the enterprise {@code EntLifecycleProcessor} overlay.
     */
    private void resumeSnapshot(
            HttpChunkedResponse response,
            LifecycleState state
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        resumeSnapshot(response, state, this::writeHeaderExtraFields);
    }

    private static void resumeSnapshot(
            HttpChunkedResponse response,
            LifecycleState state,
            HeaderExtraWriter headerExtra
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final LifecycleSnapshot snap = state.snapshot;
        final ObjList<LifecycleSnapshot.ComponentSnapshot> components = snap.components();
        final int n = components.size();

        // Phase 1: header — the preamble before the components array.
        if (!state.headerWritten) {
            while (true) {
                try {
                    response.bookmark();
                    response.putAscii('{')
                            .putAsciiQuoted("capturedAtMicros").putAscii(':').put(snap.capturedAtMicros()).putAscii(',');
                    // A subclass may inject extra header fields here (each followed by a trailing
                    // comma) before the components array. The whole header is one bookmark unit, so
                    // an overflow rewinds and re-runs the hook -- it must stay side-effect free.
                    headerExtra.write(response, state.holder);
                    response.putAsciiQuoted("components").putAscii(':').putAscii('[');
                    state.headerWritten = true;
                    break;
                } catch (NoSpaceLeftInResponseBufferException ignored) {
                    if (response.resetToBookmark()) {
                        response.sendChunk(false);
                        // sendChunk(false) throws PeerIsSlowToReadException if the TCP buffer is full;
                        // that propagates out and the framework calls resumeSend later. If it returns
                        // normally, the buffer is clear — retry the header write.
                    } else {
                        // Header is larger than the buffer capacity: should not happen for the
                        // fixed-layout lifecycle JSON, but guard defensively.
                        break;
                    }
                }
            }
        }

        // Phase 2: components — one entry per cursor position.
        while (state.cursor < n) {
            while (true) {
                try {
                    response.bookmark();
                    if (state.cursor > 0) {
                        response.putAscii(',');
                    }
                    writeComponent(response, components.getQuick(state.cursor));
                    state.cursor++;
                    break;
                } catch (NoSpaceLeftInResponseBufferException ignored) {
                    if (response.resetToBookmark()) {
                        response.sendChunk(false);
                    } else {
                        // Component exceeds buffer capacity: skip (defensive, should not occur).
                        state.cursor++;
                        break;
                    }
                }
            }
        }

        // Phase 3: footer.
        if (!state.footerWritten) {
            while (true) {
                try {
                    response.bookmark();
                    response.putAscii(']').putAscii('}');
                    state.footerWritten = true;
                    break;
                } catch (NoSpaceLeftInResponseBufferException ignored) {
                    if (response.resetToBookmark()) {
                        response.sendChunk(false);
                    } else {
                        break;
                    }
                }
            }
        }

        // All phases complete: close the chunked transfer.
        state.snapshot = null;
        response.sendChunk(true);
    }

    // Visible for testing: drives the resumable serialization in a single pass with a fresh cursor,
    // so a test can exercise the bookmark/overflow/flush loop without a live HTTP server. Exercises
    // the base header (no extra fields).
    public static void writeSnapshotResumable(HttpChunkedResponse r, LifecycleSnapshot snap)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        final LifecycleState state = new LifecycleState();
        state.reset(snap, snap);
        resumeSnapshot(r, state, LifecycleProcessor::writeNoHeaderExtraFields);
    }

    // Visible for testing: allows LifecycleProcessorTest to invoke without a live HTTP server.
    public static void writeSnapshot(HttpChunkedResponse r, LifecycleSnapshot snap) {
        r.putAscii('{')
                .putAsciiQuoted("capturedAtMicros").putAscii(':').put(snap.capturedAtMicros()).putAscii(',')
                .putAsciiQuoted("components").putAscii(':').putAscii('[');
        final ObjList<LifecycleSnapshot.ComponentSnapshot> components = snap.components();
        for (int i = 0, n = components.size(); i < n; i++) {
            if (i > 0) {
                r.putAscii(',');
            }
            writeComponent(r, components.getQuick(i));
        }
        r.putAscii(']').putAscii('}');
    }

    protected static void encodeProgressEvent(HttpChunkedResponse r, ProgressEvent event) {
        switch (event) {
            case RestoreProgress rp -> r.putAscii('{')
                    .putAsciiQuoted("type").putAscii(':').putAsciiQuoted("restore").putAscii(',')
                    .putAsciiQuoted("tablesDone").putAscii(':').put(rp.tablesDone()).putAscii(',')
                    .putAsciiQuoted("tablesTotal").putAscii(':').put(rp.tablesTotal()).putAscii(',')
                    .putAsciiQuoted("bytesDone").putAscii(':').put(rp.bytesDone()).putAscii(',')
                    .putAsciiQuoted("bytesTotal").putAscii(':').put(rp.bytesTotal())
                    .putAscii('}');
            case ProgressEvent.NoOpProgress np -> r.putAscii("null");
            case ProgressEvent.TestOnly to -> r.putAscii("null");
        }
    }

    protected static void encodeStringList(HttpChunkedResponse r, ObjList<String> list) {
        r.putAscii('[');
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i > 0) {
                r.putAscii(',');
            }
            r.putAsciiQuoted(list.getQuick(i));
        }
        r.putAscii(']');
    }

    protected static void writeComponent(HttpChunkedResponse r, LifecycleSnapshot.ComponentSnapshot c) {
        r.putAscii('{')
                .putAsciiQuoted("name").putAscii(':').putAsciiQuoted(c.name()).putAscii(',')
                .putAsciiQuoted("state").putAscii(':').putAsciiQuoted(c.state().name()).putAscii(',')
                .putAsciiQuoted("lastTransitionMicros").putAscii(':').put(c.lastTransitionMicros()).putAscii(',')
                .putAsciiQuoted("latestProgress").putAscii(':');
        if (c.latestProgress() == null) {
            r.putAscii("null");
        } else {
            encodeProgressEvent(r, c.latestProgress());
        }
        r.putAscii(',')
                .putAsciiQuoted("hardRequiredDependencies").putAscii(':');
        encodeStringList(r, c.hardRequiredDependencies());
        r.putAscii(',')
                .putAsciiQuoted("softDependencies").putAscii(':');
        encodeStringList(r, c.softDependencies());
        r.putAscii('}');
    }

    private static void writeNoHeaderExtraFields(HttpChunkedResponse response, Object holder) {
        // Base lifecycle JSON has no extra header fields.
    }

    /**
     * Writes any extra header fields a subclass injects between {@code capturedAtMicros} and the
     * {@code components} array. Lets the resume loop stay a single static method while the instance
     * dispatches to {@link #writeHeaderExtraFields(HttpChunkedResponse, Object)}.
     */
    @FunctionalInterface
    protected interface HeaderExtraWriter {
        void write(HttpChunkedResponse response, Object holder);
    }

    /**
     * Per-connection serialization cursor for resumable GET /lifecycle responses.
     */
    static final class LifecycleState {
        int cursor;
        boolean footerWritten;
        boolean headerWritten;
        // The original snapshot holder (a LifecycleSnapshot in the base, an extended type in a
        // subclass) passed to the header-extra hook. Held alongside the base snapshot the loop iterates.
        Object holder;
        LifecycleSnapshot snapshot;

        void reset(Object holder, LifecycleSnapshot base) {
            cursor = 0;
            footerWritten = false;
            headerWritten = false;
            this.holder = holder;
            snapshot = base;
        }
    }
}
