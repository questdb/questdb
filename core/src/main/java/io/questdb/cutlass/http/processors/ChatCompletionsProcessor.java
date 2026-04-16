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

import io.questdb.cairo.SecurityContext;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.afm.AppleFoundationModel;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sink;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;

public class ChatCompletionsProcessor implements HttpRequestHandler {
    private static final String CHAT_COMPLETION = "chat.completion";
    private static final String CHAT_COMPLETION_CHUNK = "chat.completion.chunk";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String CONTENT_TYPE_SSE = "text/event-stream";
    private static final Log LOG = LogFactory.getLog(ChatCompletionsProcessor.class);
    private static final LocalValue<ChatCompletionsProcessorState> LV_STATE = new LocalValue<>();
    private static final String MODEL_NAME = "afm";
    private final PostProcessor postProcessor = new PostProcessor();
    private final int requestBufferSize;

    public ChatCompletionsProcessor(int requestBufferSize) {
        this.requestBufferSize = requestBufferSize;
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return postProcessor;
    }

    private static void appendJsonEscaped(Utf8Sink sink, CharSequence s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sink.putAscii("\\\"");
                    break;
                case '\\':
                    sink.putAscii("\\\\");
                    break;
                case '\n':
                    sink.putAscii("\\n");
                    break;
                case '\r':
                    sink.putAscii("\\r");
                    break;
                case '\t':
                    sink.putAscii("\\t");
                    break;
                case '\b':
                    sink.putAscii("\\b");
                    break;
                case '\f':
                    sink.putAscii("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        sink.putAscii("\\u00");
                        sink.putAscii(hex((c >> 4) & 0xF));
                        sink.putAscii(hex(c & 0xF));
                    } else {
                        sink.put(c);
                    }
            }
        }
    }

    private static char hex(int v) {
        return (char) (v < 10 ? '0' + v : 'a' + (v - 10));
    }

    private static final class StreamInterruptedException extends RuntimeException {
        StreamInterruptedException(Throwable cause) {
            super(cause);
        }

        Throwable unwrap() {
            return getCause();
        }
    }

    private class PostProcessor implements HttpPostPutProcessor {
        private ChatCompletionsProcessorState transientState;

        @Override
        public byte getRequiredAuthType() {
            return SecurityContext.AUTH_TYPE_NONE;
        }

        @Override
        public void onChunk(long lo, long hi) {
            if (hi > lo) {
                transientState.getRequestBody().putNonAscii(lo, hi);
            }
        }

        @Override
        public void onHeadersReady(HttpConnectionContext context) {
            transientState = LV_STATE.get(context);
            if (transientState == null) {
                transientState = new ChatCompletionsProcessorState(requestBufferSize);
                LV_STATE.set(context, transientState);
            }
            transientState.clear();
        }

        @Override
        public void onRequestComplete(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
            try {
                transientState.parseRequest();
            } catch (JsonException e) {
                LOG.error().$("chat/completions: invalid JSON [err=").$(e.getMessage()).I$();
                sendErrorJson(context, HTTP_BAD_REQUEST, "invalid JSON body");
                return;
            }

            final StringSink prompt = transientState.getPrompt();
            if (prompt.length() == 0) {
                sendErrorJson(context, HTTP_BAD_REQUEST, "messages[] is empty");
                return;
            }

            if (transientState.isStream()) {
                sendStream(context, prompt.toString());
            } else {
                final String reply;
                try {
                    reply = AppleFoundationModel.generate(prompt.toString());
                } catch (Throwable t) {
                    LOG.error().$("chat/completions: generation failed [err=").$(t.getMessage()).I$();
                    sendErrorJson(context, HTTP_INTERNAL_ERROR, t.getMessage());
                    return;
                }
                sendJson(context, reply);
            }
        }

        @Override
        public void resumeRecv(HttpConnectionContext context) {
            transientState = LV_STATE.get(context);
        }

        @Override
        public void resumeSend(HttpConnectionContext context)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            // Response fits in the socket buffer for a prototype; no resumable path.
        }

        private long createdSeconds() {
            return System.currentTimeMillis() / 1000L;
        }

        private void sendErrorJson(HttpConnectionContext context, int code, String msg)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            final HttpChunkedResponse r = context.getChunkedResponse();
            r.status(code, CONTENT_TYPE_JSON);
            r.sendHeader();
            r.putAscii("{\"error\":{\"message\":\"");
            appendJsonEscaped(r, msg != null ? msg : "unknown error");
            r.putAscii("\",\"type\":\"afm_error\"}}");
            r.sendChunk(true);
        }

        private void sendJson(HttpConnectionContext context, String reply)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            final HttpChunkedResponse r = context.getChunkedResponse();
            r.status(HTTP_OK, CONTENT_TYPE_JSON);
            r.sendHeader();
            r.putAscii("{\"id\":\"chatcmpl-afm\",\"object\":\"");
            r.putAscii(CHAT_COMPLETION);
            r.putAscii("\",\"created\":").put(createdSeconds());
            r.putAscii(",\"model\":\"").putAscii(MODEL_NAME);
            r.putAscii("\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"");
            appendJsonEscaped(r, reply);
            r.putAscii("\"},\"finish_reason\":\"stop\"}]");
            r.putAscii(",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}}");
            r.sendChunk(true);
        }

        private void sendStream(HttpConnectionContext context, String prompt)
                throws PeerDisconnectedException, PeerIsSlowToReadException {
            final HttpChunkedResponse r = context.getChunkedResponse();
            r.status(HTTP_OK, CONTENT_TYPE_SSE);
            r.sendHeader();
            final long created = createdSeconds();

            // Opening chunk: announce the assistant role with an empty delta so the
            // OpenAI SDK can wire up the message before content starts flowing.
            writeRoleChunk(r, created);
            r.sendChunk(false);

            try {
                AppleFoundationModel.generateStream(prompt, token -> {
                    writeContentChunk(r, created, token);
                    try {
                        r.sendChunk(false);
                    } catch (PeerDisconnectedException | PeerIsSlowToReadException e) {
                        throw new StreamInterruptedException(e);
                    }
                });
            } catch (StreamInterruptedException e) {
                final Throwable cause = e.unwrap();
                if (cause instanceof PeerDisconnectedException pde) {
                    throw pde;
                }
                if (cause instanceof PeerIsSlowToReadException psre) {
                    throw psre;
                }
                throw new RuntimeException(cause);
            } catch (Throwable t) {
                LOG.error().$("chat/completions stream failed [err=").$(t.getMessage()).I$();
                // Best effort: emit an error-shaped final chunk so the client can close cleanly.
                writeErrorChunk(r, created, t.getMessage());
                r.sendChunk(false);
            }

            writeFinalChunk(r, created);
            r.sendChunk(false);

            r.putAscii("data: [DONE]\n\n");
            r.sendChunk(true);
        }

        private void writeContentChunk(HttpChunkedResponse r, long created, String token) {
            r.putAscii("data: {\"id\":\"chatcmpl-afm\",\"object\":\"");
            r.putAscii(CHAT_COMPLETION_CHUNK);
            r.putAscii("\",\"created\":").put(created);
            r.putAscii(",\"model\":\"").putAscii(MODEL_NAME);
            r.putAscii("\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"");
            appendJsonEscaped(r, token);
            r.putAscii("\"},\"finish_reason\":null}]}\n\n");
        }

        private void writeErrorChunk(HttpChunkedResponse r, long created, String message) {
            r.putAscii("data: {\"id\":\"chatcmpl-afm\",\"object\":\"");
            r.putAscii(CHAT_COMPLETION_CHUNK);
            r.putAscii("\",\"created\":").put(created);
            r.putAscii(",\"model\":\"").putAscii(MODEL_NAME);
            r.putAscii("\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"");
            appendJsonEscaped(r, "[error] " + (message != null ? message : "unknown"));
            r.putAscii("\"},\"finish_reason\":null}]}\n\n");
        }

        private void writeFinalChunk(HttpChunkedResponse r, long created) {
            r.putAscii("data: {\"id\":\"chatcmpl-afm\",\"object\":\"");
            r.putAscii(CHAT_COMPLETION_CHUNK);
            r.putAscii("\",\"created\":").put(created);
            r.putAscii(",\"model\":\"").putAscii(MODEL_NAME);
            r.putAscii("\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]");
            r.putAscii(",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}}\n\n");
        }

        private void writeRoleChunk(HttpChunkedResponse r, long created) {
            r.putAscii("data: {\"id\":\"chatcmpl-afm\",\"object\":\"");
            r.putAscii(CHAT_COMPLETION_CHUNK);
            r.putAscii("\",\"created\":").put(created);
            r.putAscii(",\"model\":\"").putAscii(MODEL_NAME);
            r.putAscii("\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n");
        }
    }

}
