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

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

final class ChatCompletionsProcessorState implements Mutable, Closeable {
    private static final int EXPECT_ROOT_OBJ = 0;
    private static final int IN_IGNORED = 4;
    private static final int IN_MESSAGE_OBJ = 3;
    private static final int IN_MESSAGES_ARRAY = 2;
    private static final int IN_ROOT = 1;
    private final StringSink currentContent = new StringSink();
    private final StringSink currentRole = new StringSink();
    private final JsonLexer lexer = new JsonLexer(1024, 1024 * 1024);
    private final RequestParser parser = new RequestParser();
    private final StringSink prompt = new StringSink();
    private final DirectUtf8Sink requestBody;
    private boolean hasMessage;
    private int ignoreDepth;
    private int ignoreReturnState;
    private boolean isStream;
    private int postState;
    private String rootKey;

    ChatCompletionsProcessorState(int bodyBufferSize) {
        this.requestBody = new DirectUtf8Sink(bodyBufferSize);
    }

    @Override
    public void clear() {
        requestBody.clear();
        prompt.clear();
        currentRole.clear();
        currentContent.clear();
        lexer.clear();
        postState = EXPECT_ROOT_OBJ;
        ignoreDepth = 0;
        ignoreReturnState = IN_ROOT;
        rootKey = null;
        isStream = false;
        hasMessage = false;
    }

    @Override
    public void close() {
        Misc.free(requestBody);
        Misc.free(lexer);
    }

    StringSink getPrompt() {
        return prompt;
    }

    DirectUtf8Sink getRequestBody() {
        return requestBody;
    }

    boolean isStream() {
        return isStream;
    }

    void parseRequest() throws JsonException {
        lexer.parse(requestBody.lo(), requestBody.hi(), parser);
        lexer.parseLast();
    }

    private void appendMessageToPrompt() {
        if (prompt.length() > 0) {
            prompt.putAscii('\n');
        }
        prompt.putAscii('[');
        if (currentRole.length() > 0) {
            prompt.put(currentRole);
        } else {
            prompt.putAscii("user");
        }
        prompt.putAscii(']').putAscii('\n');
        prompt.put(currentContent);
    }

    private void enterIgnored(int returnState) {
        ignoreReturnState = returnState;
        ignoreDepth = 1;
        postState = IN_IGNORED;
    }

    private class RequestParser implements JsonParser {
        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            switch (postState) {
                case EXPECT_ROOT_OBJ:
                    if (code == JsonLexer.EVT_OBJ_START) {
                        postState = IN_ROOT;
                    }
                    return;
                case IN_ROOT:
                    handleRoot(code, tag);
                    return;
                case IN_MESSAGES_ARRAY:
                    handleMessagesArray(code);
                    return;
                case IN_MESSAGE_OBJ:
                    handleMessageObj(code, tag);
                    return;
                case IN_IGNORED:
                    if (code == JsonLexer.EVT_OBJ_START || code == JsonLexer.EVT_ARRAY_START) {
                        ignoreDepth++;
                    } else if (code == JsonLexer.EVT_OBJ_END || code == JsonLexer.EVT_ARRAY_END) {
                        if (--ignoreDepth == 0) {
                            postState = ignoreReturnState;
                        }
                    }
            }
        }

        private void handleMessageObj(int code, CharSequence tag) {
            switch (code) {
                case JsonLexer.EVT_NAME:
                    rootKey = tag.toString();
                    break;
                case JsonLexer.EVT_VALUE:
                    if ("role".equals(rootKey)) {
                        currentRole.clear();
                        currentRole.put(tag);
                    } else if ("content".equals(rootKey)) {
                        currentContent.clear();
                        currentContent.put(tag);
                        hasMessage = true;
                    }
                    break;
                case JsonLexer.EVT_OBJ_END:
                    if (hasMessage) {
                        appendMessageToPrompt();
                        hasMessage = false;
                    }
                    currentRole.clear();
                    currentContent.clear();
                    postState = IN_MESSAGES_ARRAY;
                    break;
                case JsonLexer.EVT_OBJ_START:
                case JsonLexer.EVT_ARRAY_START:
                    enterIgnored(IN_MESSAGE_OBJ);
            }
        }

        private void handleMessagesArray(int code) {
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    postState = IN_MESSAGE_OBJ;
                    currentRole.clear();
                    currentContent.clear();
                    hasMessage = false;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    postState = IN_ROOT;
            }
        }

        private void handleRoot(int code, CharSequence tag) {
            switch (code) {
                case JsonLexer.EVT_NAME:
                    rootKey = tag.toString();
                    break;
                case JsonLexer.EVT_VALUE:
                    if ("stream".equals(rootKey)) {
                        isStream = "true".contentEquals(tag);
                    }
                    break;
                case JsonLexer.EVT_ARRAY_START:
                    if ("messages".equals(rootKey)) {
                        postState = IN_MESSAGES_ARRAY;
                    } else {
                        enterIgnored(IN_ROOT);
                    }
                    break;
                case JsonLexer.EVT_OBJ_START:
                    enterIgnored(IN_ROOT);
                    break;
                case JsonLexer.EVT_OBJ_END:
                    postState = EXPECT_ROOT_OBJ;
            }
        }
    }
}
