/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.lang.parser;

public class MetadataParser {

    private final TokenStream ts;

    public MetadataParser() {
        ts = new TokenStream();
        ts.defineSymbol(" ");
        ts.defineSymbol(",");
        ts.defineSymbol("{");
        ts.defineSymbol("}");
        ts.defineSymbol("=");
        ts.defineSymbol("[");
        ts.defineSymbol("]");
    }

    public <T> T parse(String value, Callback<T> callback) {
        State state = State.EXPECT_OBJECT_NAME;
        ts.setContent(value);

        for (String s : ts) {

            if (" ".equals(s)) {
                continue;
            }

            switch (state) {
                case EXPECT_OBJECT_NAME:
                    callback.onObjectBegin(s);
                    state = State.EXPECT_OBJECT_BEGIN;
                    break;
                case EXPECT_OBJECT_BEGIN:
                    assert s.equals("{");
                    state = State.EXPECT_ATTRIBUTE_NAME_OR_END;
                    break;
                case EXPECT_ATTRIBUTE_NAME_OR_END:
                    switch (s) {
                        case "}":
                            // keep state
                            callback.onObjectEnd();
                            break;
                        default:
                            state = callback.onAttributeName(s);
                    }
                    break;
                case EXPECT_EQ_FOLLOWED_BY_VALUE:
                    assert s.equals("=");
                    state = State.EXPECT_ATTRIBUTE_VALUE;
                    break;
                case EXPECT_EQ_FOLLOWED_BY_OBJECT:
                    assert s.equals("=");
                    state = State.EXPECT_OBJECT_NAME;
                    break;
                case EXPECT_ATTRIBUTE_VALUE:
                    // consume value
                    callback.onAttributeValue(s);
                    state = State.EXPECT_NEXT_ATTRIBUTE_OR_END;
                    break;
                case EXPECT_NEXT_ATTRIBUTE_OR_END:
                    switch (s) {
                        case ",":
                            state = State.EXPECT_ATTRIBUTE_NAME;
                            break;
                        case "}":
                            callback.onObjectEnd();
                            // keep state
                            break;
                        default:
                            System.out.println("expected , or }, got: " + s);
                    }
                    break;
                case EXPECT_ATTRIBUTE_NAME:
                    state = callback.onAttributeName(s);
                    break;
                default:
                    System.out.println("unknown state");
            }
        }
        return callback.value();
    }

    public static enum State {
        EXPECT_OBJECT_NAME,
        EXPECT_OBJECT_BEGIN,
        EXPECT_ATTRIBUTE_NAME_OR_END,
        EXPECT_ATTRIBUTE_VALUE,
        EXPECT_EQ_FOLLOWED_BY_VALUE,
        EXPECT_EQ_FOLLOWED_BY_OBJECT,
        EXPECT_NEXT_ATTRIBUTE_OR_END,
        EXPECT_ATTRIBUTE_NAME
    }

    public static interface Callback<T> {

        void onObjectBegin(String name);

        State onAttributeName(String s);

        void onAttributeValue(String s);

        T value();

        void onObjectEnd();
    }
}
