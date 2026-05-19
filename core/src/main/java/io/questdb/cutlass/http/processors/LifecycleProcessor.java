package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.ProgressEvent;
import io.questdb.lifecycle.RestoreProgress;
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
 * WR-01: this serializer uses {@code putAsciiQuoted} which does NOT escape JSON
 * strings. Correctness relies on the invariant that all component names match
 * {@code [a-z0-9-]+} -- enforced at registration in
 * {@code LifecycleOrchestrator.register}. State names and field labels are emitted
 * from compile-time literals / enum constants, so they are JSON-safe by construction.
 * Dependency-list entries are component names and therefore also satisfy the invariant.
 * <p>
 * JSON shape (stable from Phase 3; adding fields later is fine, renaming is not):
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
        response.status(200, "application/json; charset=utf-8");
        response.sendHeader();
        writeSnapshot(response, snapshotSupplier.get());
        response.sendChunk(true);
    }

    // Visible for testing: allows LifecycleProcessorTest to invoke without a live HTTP server.
    public static void writeSnapshot(HttpChunkedResponse r, LifecycleSnapshot snap) {
        r.putAscii('{')
                .putAsciiQuoted("capturedAtMicros").putAscii(':').put(snap.capturedAtMicros()).putAscii(',')
                .putAsciiQuoted("currentRole").putAscii(':').putAsciiQuoted(snap.currentRole().name()).putAscii(',')
                .putAsciiQuoted("switchInFlight").putAscii(':').putAscii(snap.switchInFlight() ? "true" : "false").putAscii(',')
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

    private static void encodeProgressEvent(HttpChunkedResponse r, ProgressEvent event) {
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

    private static void encodeStringList(HttpChunkedResponse r, ObjList<String> list) {
        r.putAscii('[');
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i > 0) {
                r.putAscii(',');
            }
            r.putAsciiQuoted(list.getQuick(i));
        }
        r.putAscii(']');
    }

    private static void writeComponent(HttpChunkedResponse r, LifecycleSnapshot.ComponentSnapshot c) {
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
}
