package com.questdb.json;

import com.questdb.misc.Unsafe;
import com.questdb.std.IntStack;
import com.questdb.std.Mutable;
import com.questdb.std.str.DirectByteCharSequence;

public class JsonLexer implements Mutable {
    public static final int EVT_OBJ_START = 1;
    public static final int EVT_OBJ_END = 2;
    public static final int EVT_ARRAY_START = 3;
    public static final int EVT_ARRAY_END = 4;
    public static final int EVT_NAME = 5;
    public static final int EVT_VALUE = 6;
    public static final int EVT_ARRAY_VALUE = 7;

    public static final int S_START = 0;
    public static final int S_EXPECT_NAME = 1;
    public static final int S_EXPECT_FIRST_NAME = 5;
    public static final int S_EXPECT_VALUE = 2;
    public static final int S_EXPECT_COMMA = 3;
    private static final int S_EXPECT_COLON = 4;
    private final IntStack objDepthStack = new IntStack();
    private final IntStack arrayDepthStack = new IntStack();
    private final DirectByteCharSequence dbcs = new DirectByteCharSequence();
    private int state = S_START;
    private int objDepth = 0;
    private int arrayDepth = 0;

    @Override
    public void clear() {
        objDepthStack.clear();
        arrayDepthStack.clear();
        state = S_START;
        objDepth = 0;
        arrayDepth = 0;
    }

    public void parse(long lo, long len, JsonListener listener) throws JsonException {
        long p = lo;
        long hi = lo + len;
        long valueStart = 0;
        boolean quoted = false;
        boolean ignoreNext = false;
        while (p < hi) {
            char c = (char) Unsafe.getUnsafe().getByte(p++);

            if (ignoreNext) {
                ignoreNext = false;
                continue;
            }

            if (valueStart > 0 && ((quoted && c != '"') || (!quoted && (c == '-' || c == '.' || c == 'e' || c == 'E' || (c >= '0' && c <= '9'))))) {
                if (quoted && c == '\\') {
                    ignoreNext = true;
                }
                continue;
            }

            if (valueStart > 0) {
                if (state == S_EXPECT_NAME || state == S_EXPECT_FIRST_NAME) {
                    listener.onEvent(EVT_NAME, dbcs.of(valueStart, p - 1));
                    state = S_EXPECT_COLON;
                } else {
                    listener.onEvent(arrayDepth > 0 ? EVT_ARRAY_VALUE : EVT_VALUE, dbcs.of(valueStart, p - 1));
                    state = S_EXPECT_COMMA;
                }
                valueStart = 0;

                if (quoted) {
                    // skip the quote mark
                    continue;
                }
            }


            switch (c) {
                case '{':
                    if (state != S_START && state != S_EXPECT_VALUE) {
                        throw JsonException.with("{ is not expected here", (int) (p - lo));
                    }
                    arrayDepthStack.push(arrayDepth);
                    arrayDepth = 0;

                    listener.onEvent(EVT_OBJ_START, null);
                    objDepth++;
                    state = S_EXPECT_FIRST_NAME;
                    break;
                case '}':
                    if (arrayDepth > 0) {
                        throw JsonException.with("} is not expected here. You have non-terminated array", (int) (p - lo));
                    }

                    if (objDepth > 0) {
                        switch (state) {
                            case S_EXPECT_VALUE:
                                throw JsonException.with("Attribute value expected", (int) (p - lo - 1));
                            case S_EXPECT_NAME:
                                throw JsonException.with("Attribute name expected", (int) (p - lo - 1));
                            default:
                                break;
                        }
                        listener.onEvent(EVT_OBJ_END, null);
                        objDepth--;
                        arrayDepth = arrayDepthStack.pop();
                        state = S_EXPECT_COMMA;
                    } else {
                        throw JsonException.with("Dangling }", (int) (p - lo));
                    }
                    break;
                case '[':
                    if (state != S_START && state != S_EXPECT_VALUE) {
                        throw JsonException.with("[ is not expected here", (int) (p - lo));
                    }

                    listener.onEvent(EVT_ARRAY_START, null);
                    objDepthStack.push(objDepth);
                    objDepth = 0;
                    arrayDepth++;
                    state = S_EXPECT_VALUE;
                    break;
                case ']':
                    if (objDepth > 0) {
                        throw JsonException.with("] is not expected here. You have non-terminated object", (int) (p - lo));
                    }

                    if (arrayDepth == 0) {
                        throw JsonException.with("Dangling ]", (int) (p - lo));
                    }

                    listener.onEvent(EVT_ARRAY_END, null);
                    arrayDepth--;
                    objDepth = objDepthStack.pop();
                    state = S_EXPECT_COMMA;
                    break;
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;
                case ',':
                    if (state != S_EXPECT_COMMA) {
                        throw JsonException.with("Unexpected comma", (int) (p - lo));
                    }
                    if (arrayDepth > 0) {
                        state = S_EXPECT_VALUE;
                    } else {
                        state = S_EXPECT_NAME;
                    }
                    break;
                case ':':
                    if (state != S_EXPECT_COLON) {
                        throw JsonException.with("Misplaced ':'", (int) (p - lo));
                    }
                    state = S_EXPECT_VALUE;
                    break;
                case '"':
                    if (state != S_EXPECT_NAME && state != S_EXPECT_FIRST_NAME && state != S_EXPECT_VALUE) {
                        throw JsonException.with("Unexpected quote '\"'", (int) (p - lo));
                    }
                    valueStart = p;
                    quoted = true;
                    break;
                default:
                    if (state != S_EXPECT_VALUE) {
                        throw JsonException.with("Unexpected symbol", (int) (p - lo));
                    }
                    // this isn't a quote, include this character
                    valueStart = p - 1;
                    quoted = false;
            }
        }
    }
}
