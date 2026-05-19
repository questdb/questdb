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

package io.questdb.cutlass.http.processors;

import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.SwitchInFlightException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_PUT;
import static io.questdb.cutlass.http.HttpRequestValidator.MULTIPART_REQUEST;
import static io.questdb.cutlass.http.HttpRequestValidator.NON_MULTIPART_REQUEST;

/**
 * POST /lifecycle/switch handler. Accepts JSON body {@code {role, timeout_ms?}},
 * threads {@code timeout_ms} (when present) into the orchestrator's role-switch
 * timeout consumer (see {@link LifecycleOrchestrator#registerRoleSwitchTimeoutConsumer}),
 * submits the switch via {@link LifecycleOrchestrator#submitSwitch}, and returns:
 * <ul>
 *   <li>202 {@code {"accepted":true}} on success
 *   <li>400 {@code {"error":"invalid_request","message":"..."}} on body parse / validation failure
 *   <li>405 {@code {"error":"method_not_allowed","message":"POST required"}} on non-POST verbs
 *   <li>409 {@code {"error":"switch_in_flight","current_role":"...","target_role":"..."}} when a switch is already running
 * </ul>
 * Authentication piggybacks on existing min-http access control
 * ({@link HttpServerConfiguration#getRequiredAuthType()}). Phase 6 D6-02/D6-03/D6-04/D6-05.
 */
public class SwitchProcessor implements HttpRequestHandler {
    private static final Log LOG = LogFactory.getLog(SwitchProcessor.class);
    private static final LocalValue<SwitchProcessorState> LV_STATE = new LocalValue<>();
    private final LifecycleOrchestrator orchestrator;
    private final OtherMethodProcessor otherProcessor;
    private final PostProcessor postProcessor;
    private final byte requiredAuthType;

    public SwitchProcessor(HttpServerConfiguration cfg, LifecycleOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
        this.requiredAuthType = cfg.getRequiredAuthType();
        this.postProcessor = new PostProcessor();
        this.otherProcessor = new OtherMethodProcessor();
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return requestHeader.isPostRequest() ? postProcessor : otherProcessor;
    }

    /**
     * Test seam: emit the 405 method-not-allowed response body. Mirrors
     * {@link OtherMethodProcessor#onRequestComplete} without the {@link HttpConnectionContext}
     * dependency. SwitchProcessorTest drives this directly against a FakeHttpChunkedResponse.
     */
    @TestOnly
    public static void writeMethodNotAllowed(HttpChunkedResponse r)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        r.status(405, "application/json; charset=utf-8");
        r.sendHeader();
        r.putAscii("{\"error\":\"method_not_allowed\",\"message\":\"POST required\"}");
        r.sendChunk(true);
    }

    /**
     * Test seam: parse the body, validate fields, forward {@code timeout_ms} to the
     * orchestrator (when present), submit the switch, and emit the 202/400/409 response.
     * Mirrors {@link PostProcessor#onRequestComplete} without the {@link HttpConnectionContext}
     * dependency. SwitchProcessorTest drives this directly with a stub LifecycleOrchestrator and
     * a FakeHttpChunkedResponse so the full body-parsing + dispatch + emission path is covered
     * HTTP-free.
     */
    @TestOnly
    public static void handlePost(LifecycleOrchestrator orchestrator, DirectUtf8Sink body, HttpChunkedResponse r)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        // WR-04: clear the body sink unconditionally before returning, regardless of which response
        // branch fires (success, validation 400, 409, or an unchecked throw from the parser stack).
        // PostProcessor.onHeadersReady also resets the sink on the NEXT request, so this is
        // belt-and-braces protection in case a future change (or an unchecked exception escaping the
        // JsonException catch) leaks request N's body into request N+1's parse view on the same
        // keep-alive connection.
        try {
            SwitchRequestParser parser = new SwitchRequestParser();
            try (JsonLexer lexer = new JsonLexer(64, 1024)) {
                lexer.parse(body.lo(), body.hi(), parser);
                lexer.parseLast();
            } catch (JsonException e) {
                sendInvalidRequest(r, "malformed JSON");
                return;
            }
            if (parser.unknownField != null) {
                sendInvalidRequest(r, "unknown field: " + parser.unknownField);
                return;
            }
            if (parser.parsedRole == null) {
                sendInvalidRequest(r, "role required and must be one of [primary, replica]");
                return;
            }
            if (parser.parsedTimeoutMs != null && parser.parsedTimeoutMs < 0) {
                sendInvalidRequest(r, "timeout_ms must be non-negative");
                return;
            }
            try {
                orchestrator.submitSwitch(parser.parsedRole);
            } catch (SwitchInFlightException ex) {
                LOG.info()
                        .$("switch in flight; rejecting current=").$(ex.currentRole())
                        .$(" target=").$(ex.targetRole())
                        .I$();
                r.status(409, "application/json; charset=utf-8");
                r.sendHeader();
                r.putAscii("{\"error\":\"switch_in_flight\",\"current_role\":\"")
                        .putAscii(ex.currentRole().name())
                        .putAscii("\",\"target_role\":\"")
                        .putAscii(ex.targetRole().name())
                        .putAscii("\"}");
                r.sendChunk(true);
                return;
            }
            // BL-02: publish the per-request timeout only AFTER the CAS in submitSwitch succeeds.
            // A rejected (409) POST must not mutate the live drainTimeoutMs that the in-flight switch
            // reads inside EntEngineInitEnvelope.switchRole. This still races with the lifecycle thread
            // (the executor task may already be running by the time we get here), but at least a
            // rejected request can no longer retune an unrelated in-flight switch's drain budget.
            if (parser.parsedTimeoutMs != null) {
                orchestrator.setRoleSwitchTimeoutMs(parser.parsedTimeoutMs);
            }
            LOG.info()
                    .$("switch accepted role=").$(parser.parsedRole)
                    .$(" timeout_ms=").$(parser.parsedTimeoutMs == null ? -1L : (long) parser.parsedTimeoutMs)
                    .I$();
            r.status(202, "application/json; charset=utf-8");
            r.sendHeader();
            r.putAscii("{\"accepted\":true}");
            r.sendChunk(true);
        } finally {
            body.clear();
        }
    }

    private static void sendInvalidRequest(HttpChunkedResponse r, String message)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        r.status(400, "application/json; charset=utf-8");
        r.sendHeader();
        r.putAscii("{\"error\":\"invalid_request\",\"message\":\"").putAscii(message).putAscii("\"}");
        r.sendChunk(true);
    }

    /**
     * Handles non-POST verbs (GET, PUT, DELETE, etc.) by returning 405. Advertises every non-POST
     * verb in {@link #getSupportedRequestTypes()} so the HTTP framework dispatcher routes
     * non-POST requests here instead of failing with a generic 400/415 earlier in the pipeline.
     */
    class OtherMethodProcessor implements HttpRequestProcessor {
        @Override
        public byte getRequiredAuthType() {
            return requiredAuthType;
        }

        @Override
        public short getSupportedRequestTypes() {
            return (short) (METHOD_GET | METHOD_PUT | NON_MULTIPART_REQUEST | MULTIPART_REQUEST);
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            writeMethodNotAllowed(context.getChunkedResponse());
        }
    }

    /**
     * Per-connection state held in {@link #LV_STATE}: a small native buffer accumulating the
     * POST body across multiple {@code onChunk} invocations.
     * <p>
     * Implements {@link Closeable} so {@link io.questdb.cutlass.http.LocalValueMap} frees the
     * underlying {@link DirectUtf8Sink} via {@code Misc.freeIfCloseable} on connection close,
     * rehash, and same-key replacement paths. Without {@code Closeable} the native buffer would
     * survive every HTTP keep-alive connection that POSTs to {@code /lifecycle/switch} for the
     * lifetime of the JVM (BL-01).
     */
    static final class SwitchProcessorState implements Mutable, Closeable {
        private final DirectUtf8Sink body = new DirectUtf8Sink(1024);

        DirectUtf8Sink body() {
            return body;
        }

        @Override
        public void clear() {
            body.clear();
        }

        @Override
        public void close() {
            body.close();
        }

        void reset() {
            body.clear();
        }
    }

    /**
     * JSON listener for the {role, timeout_ms?} body. The lexer reuses a single CharSequence
     * sink for both EVT_NAME and EVT_VALUE tags, so {@code pendingName} captures the name
     * as a String via {@code tag.toString()}. One small allocation per field is acceptable
     * on this cold path (a single POST per switch).
     * <p>
     * WR-02: tracks JSON object depth and rejects nested objects / arrays. Without depth
     * tracking, a body like {@code {"role":{"x":"y"}}} silently overwrote pendingName on the
     * inner NAME event and produced the misleading 400 "unknown field: x"; worse, a body like
     * {@code {"role":{"role":"primary"}}} accidentally parsed as Role.PRIMARY and succeeded.
     * Now: only EVT_NAME / EVT_VALUE at depth 1 are honored; OBJ_START at depth &gt; 0 or any
     * ARRAY event raises a clear "expected flat object" error.
     */
    static final class SwitchRequestParser implements JsonParser {
        private static final String NAME_ROLE = "role";
        private static final String NAME_TIMEOUT_MS = "timeout_ms";
        int objDepth = 0;
        Role parsedRole = null;
        Long parsedTimeoutMs = null;
        String pendingName = null;
        String unknownField = null;

        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            switch (code) {
                case JsonLexer.EVT_OBJ_START -> {
                    objDepth++;
                    if (objDepth > 1) {
                        throw JsonException.$(position, "expected flat object; nested objects not allowed");
                    }
                }
                case JsonLexer.EVT_OBJ_END -> objDepth--;
                case JsonLexer.EVT_ARRAY_START, JsonLexer.EVT_ARRAY_END, JsonLexer.EVT_ARRAY_VALUE ->
                        throw JsonException.$(position, "expected flat object; arrays not allowed");
                case JsonLexer.EVT_NAME -> {
                    // Only honour names at depth 1 (i.e. inside the top-level object). Names emitted
                    // at depth 0 (impossible for well-formed JSON) or > 1 (already rejected above)
                    // never reach a value-binding path.
                    if (objDepth == 1) {
                        pendingName = tag.toString();
                    }
                }
                case JsonLexer.EVT_VALUE -> {
                    if (objDepth != 1 || pendingName == null) {
                        return;
                    }
                    if (NAME_ROLE.equals(pendingName)) {
                        if ("primary".contentEquals(tag)) {
                            parsedRole = Role.PRIMARY;
                        } else if ("replica".contentEquals(tag)) {
                            parsedRole = Role.REPLICA;
                        }
                    } else if (NAME_TIMEOUT_MS.equals(pendingName)) {
                        try {
                            parsedTimeoutMs = Numbers.parseLong(tag);
                        } catch (NumericException e) {
                            throw JsonException.$(position, "timeout_ms must be integer");
                        }
                    } else if (unknownField == null) {
                        unknownField = pendingName;
                    }
                    pendingName = null;
                }
            }
        }
    }

    class PostProcessor implements HttpPostPutProcessor {
        private SwitchProcessorState transientState;

        @Override
        public byte getRequiredAuthType() {
            return requiredAuthType;
        }

        @Override
        public void onChunk(long lo, long hi) {
            if (hi > lo) {
                transientState.body().putNonAscii(lo, hi);
            }
        }

        @Override
        public void onHeadersReady(HttpConnectionContext context) {
            transientState = LV_STATE.get(context);
            if (transientState == null) {
                LV_STATE.set(context, transientState = new SwitchProcessorState());
            }
            transientState.reset();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            handlePost(orchestrator, transientState.body(), context.getChunkedResponse());
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) {
            transientState = LV_STATE.get(context);
        }
    }
}
