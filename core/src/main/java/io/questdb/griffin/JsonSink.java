/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.MillsTimestampDriver;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

/**
 * A flyweight generic JSON document builder that outputs JSON.
 * This is a simpler alternative to JsonPlanSink that is not tied to SQL execution plans.
 * <p>
 * Usage example:
 * <pre>
 * JsonSink json = new JsonSink();
 * json.of(mySink, false)
 *     .startObject()
 *     .key("name").val("John")
 *     .key("age").val(30)
 *     .endObject();
 * </pre>
 */
public class JsonSink {
    private int depth;
    private boolean formatted;
    private String indent;
    private boolean isFirstInContainer; // whether we just opened an object/array
    private int lastState; // 0=initial, 1=in object, 2=in array
    private boolean needsComma; // whether we need a comma before the next element
    private CharSink<?> sink;
    private int[] stateStack; // stack to track nested container states
    private int stateStackPos; // current position in state stack

    public JsonSink() {
        this.stateStack = new int[32]; // Initial capacity
        this.stateStackPos = 0;
    }

    /**
     * Resets the sink to its initial state.
     * Only works if the underlying sink is a StringSink.
     */
    public JsonSink clear() {
        if (sink instanceof StringSink) {
            ((StringSink) sink).clear();
        }
        depth = 0;
        lastState = 0;
        needsComma = false;
        isFirstInContainer = true;
        stateStackPos = 0;
        return this;
    }

    /**
     * Ends the current JSON array.
     */
    public JsonSink endArray() {
        if (formatted) {
            if (lastState == 2 && !isFirstInContainer) {
                sink.put('\n');
                depth--;
                appendIndentation();
            } else if (lastState == 2) {
                depth--;
            }
        }
        sink.put(']');

        // Restore previous state
        lastState = stateStack[--stateStackPos];
        needsComma = true;
        isFirstInContainer = false;
        return this;
    }

    /**
     * Ends the current JSON object.
     */
    public JsonSink endObject() {
        if (formatted) {
            if (lastState == 1 && !isFirstInContainer) {
                sink.put('\n');
                depth--;
                appendIndentation();
            } else if (lastState == 1) {
                depth--;
            }
        }
        sink.put('}');

        // Restore previous state
        lastState = stateStack[--stateStackPos];
        needsComma = true;
        isFirstInContainer = false;
        return this;
    }

    /**
     * Returns the underlying CharSink.
     */
    public CharSink<?> getSink() {
        return sink;
    }

    /**
     * Adds a key to the current object.
     */
    public JsonSink key(CharSequence key) {
        if (needsComma) {
            sink.put(',');
        }
        if (formatted) {
            sink.put('\n');
            appendIndentation();
        }
        sink.put('"');
        escapeString(key);
        sink.put("\":");
        needsComma = false;
        isFirstInContainer = false;
        return this;
    }

    public JsonSink key(Path value) {
        sink.put(value);
        return this;
    }

    /**
     * Initialize the JSON sink with a CharSink and formatting option.
     * Non-formatted output is more efficient and suitable for production use.
     * Formatted output is useful for debugging and testing.
     */
    public JsonSink of(CharSink<?> sink, boolean formatted) {
        this.sink = sink;
        this.formatted = formatted;
        this.indent = formatted ? "  " : "";
        this.depth = 0;
        this.lastState = 0;
        this.needsComma = false;
        this.isFirstInContainer = true;
        this.stateStackPos = 0;
        return this;
    }

    /**
     * Non-formatted by default
     */
    public JsonSink of(CharSink<?> sink) {
        return of(sink, false);
    }

    /**
     * Starts a JSON array.
     */
    public JsonSink startArray() {
        // Save current state on stack
        if (stateStackPos >= stateStack.length) {
            int[] newStack = new int[stateStack.length * 2];
            System.arraycopy(stateStack, 0, newStack, 0, stateStack.length);
            stateStack = newStack;
        }
        stateStack[stateStackPos++] = lastState;

        if (needsComma) {
            sink.put(',');
            if (formatted) {
                sink.put('\n');
                appendIndentation();
            }
        } else if (formatted) {
            if (lastState == 1) {
                // Value after a key in an object - add space
                sink.put(' ');
            } else if (lastState == 2) {
                // Array element - add newline and indentation
                sink.put('\n');
                appendIndentation();
            }
        }
        sink.put('[');
        if (formatted) {
            depth++;
        }
        lastState = 2;
        needsComma = false;
        isFirstInContainer = true;
        return this;
    }

    /**
     * Starts a JSON object.
     */
    public JsonSink startObject() {
        // Save current state on stack
        if (stateStackPos >= stateStack.length) {
            int[] newStack = new int[stateStack.length * 2];
            System.arraycopy(stateStack, 0, newStack, 0, stateStack.length);
            stateStack = newStack;
        }
        stateStack[stateStackPos++] = lastState;

        if (needsComma) {
            sink.put(',');
            if (formatted) {
                sink.put('\n');
                appendIndentation();
            }
        } else if (formatted) {
            if (lastState == 1) {
                // Value after a key in an object - add space
                sink.put(' ');
            } else if (lastState == 2) {
                // Array element - add newline and indentation
                sink.put('\n');
                appendIndentation();
            }
        }
        sink.put('{');
        if (formatted) {
            depth++;
        }
        lastState = 1;
        needsComma = false;
        isFirstInContainer = true;
        return this;
    }

    /**
     * Returns the JSON string.
     */
    @Override
    public String toString() {
        return sink.toString();
    }

    /**
     * Adds a string value.
     */
    public JsonSink val(CharSequence value) {
        if (value == null) {
            appendValue();
            sink.put("null");
        } else {
            appendValue();
            sink.put('"');
            escapeString(value);
            sink.put('"');
        }
        return this;
    }

    /**
     * Adds a string value.
     */
    public JsonSink val(String value) {
        return val((CharSequence) value);
    }

    /**
     * Adds a UTF-8 string value.
     */
    public JsonSink val(Utf8Sequence utf8) {
        if (utf8 == null) {
            appendValue();
            sink.put("null");
        } else {
            appendValue();
            sink.put('"');
            escapeUtf8String(utf8);
            sink.put('"');
        }
        return this;
    }

    /**
     * Adds a Sinkable value.
     */
    public JsonSink val(Sinkable sinkable) {
        if (sinkable == null) {
            appendValue();
            sink.put("null");
        } else {
            appendValue();
            sink.put('"');
            sink.put(sinkable);
            sink.put('"');
        }
        return this;
    }

    /**
     * Adds an integer value.
     */
    public JsonSink val(int value) {
        appendValue();
        sink.put(value);
        return this;
    }

    /**
     * Adds a long value.
     */
    public JsonSink val(long value) {
        appendValue();
        sink.put(value);
        return this;
    }

    /**
     * Adds a float value.
     */
    public JsonSink val(float value) {
        appendValue();
        sink.put(value);
        return this;
    }

    /**
     * Adds a double value.
     */
    public JsonSink val(double value) {
        appendValue();
        sink.put(value);
        return this;
    }

    /**
     * Adds a boolean value.
     */
    public JsonSink val(boolean value) {
        appendValue();
        sink.put(value);
        return this;
    }

    /**
     * Adds a char value.
     */
    public JsonSink val(char c) {
        appendValue();
        sink.put('"');
        escape(c);
        sink.put('"');
        return this;
    }

    /**
     * Adds a decimal value.
     */
    public JsonSink valDecimal(long value, int precision, int scale) {
        appendValue();
        Decimals.append(value, precision, scale, sink);
        return this;
    }

    /**
     * Adds a decimal value.
     */
    public JsonSink valDecimal(long hi, long lo, int precision, int scale) {
        appendValue();
        Decimals.append(hi, lo, precision, scale, sink);
        return this;
    }

    /**
     * Adds a decimal value.
     */
    public JsonSink valDecimal(long hh, long hl, long lh, long ll, int precision, int scale) {
        appendValue();
        Decimals.append(hh, hl, lh, ll, precision, scale, sink);
        return this;
    }

    /**
     * Adds a GeoHash value.
     */
    public JsonSink valGeoHash(long hash, int geoHashBits) {
        appendValue();
        sink.put('"');
        GeoHashes.append(hash, geoHashBits, sink);
        sink.put('"');
        return this;
    }

    /**
     * Adds an IPv4 value.
     */
    public JsonSink valIPv4(int ip) {
        appendValue();
        sink.put('"');
        if (ip == Numbers.IPv4_NULL) {
            sink.put("null");
        } else {
            Numbers.intToIPv4Sink(sink, ip);
        }
        sink.put('"');
        return this;
    }

    /**
     * Adds a Long256 value.
     */
    public JsonSink valLong256(long long0, long long1, long long2, long long3) {
        appendValue();
        sink.put('"');
        Numbers.appendLong256(long0, long1, long2, long3, sink);
        sink.put('"');
        return this;
    }

    public JsonSink valMillis(long value) {
        appendValue();
        sink.put('"');
        MillsTimestampDriver.INSTANCE.append(sink, value);
        sink.put('"');
        return this;
    }

    /**
     * Adds a null value.
     */
    public JsonSink valNull() {
        appendValue();
        sink.put("null");
        return this;
    }

    public JsonSink valUtf8Escaped(long addr) {
        for (long i = addr; ; i++) {
            byte b = Unsafe.getUnsafe().getByte(i);
            if (b == 0) break;
            if (b == (byte) '\"' || b == (byte) '\\') {
                sink.putAscii('\\');
            }
            sink.putAscii((char) b);
        }
        return this;
    }

    /**
     * Adds a UUID value.
     */
    public JsonSink valUuid(long lo, long hi) {
        appendValue();
        sink.put('"');
        if (Uuid.isNull(lo, hi)) {
            sink.put("null");
        } else {
            Numbers.appendUuid(lo, hi, sink);
        }
        sink.put('"');
        return this;
    }

    // Private helper methods

    private void appendIndentation() {
        if (!formatted) {
            return;
        }
        for (int i = 0; i < depth; i++) {
            sink.put(indent);
        }
    }

    private void appendValue() {
        if (formatted) {
            if (lastState == 2) { // in array
                if (needsComma) {
                    sink.put(',');
                }
                sink.put('\n');
                appendIndentation();
            } else if (lastState == 1) { // in object
                sink.put(' ');
            }
        } else {
            // Non-formatted: just add comma between elements
            if (needsComma) {
                sink.put(',');
            }
        }
        needsComma = true;
        isFirstInContainer = false;
    }

    private void escape(char c) {
        switch (c) {
            case '"':
                sink.put("\\\"");
                break;
            case '\\':
                sink.put("\\\\");
                break;
            case '\b':
                sink.put("\\b");
                break;
            case '\f':
                sink.put("\\f");
                break;
            case '\n':
                sink.put("\\n");
                break;
            case '\r':
                sink.put("\\r");
                break;
            case '\t':
                sink.put("\\t");
                break;
            default:
                if (c < 32) {
                    sink.put("\\u00");
                    sink.put(Numbers.hexDigits[c >> 4]);
                    sink.put(Numbers.hexDigits[c & 15]);
                } else {
                    sink.put(c);
                }
        }
    }

    private void escapeString(CharSequence cs) {
        if (cs != null) {
            for (int i = 0; i < cs.length(); i++) {
                escape(cs.charAt(i));
            }
        }
    }

    private void escapeUtf8String(Utf8Sequence utf8) {
        final int size = utf8.size();
        for (int i = 0; i < size; i++) {
            byte b = utf8.byteAt(i);
            // For UTF-8, we need to handle multi-byte sequences properly
            // For now, treat each byte as a character and escape as needed
            escape((char) (0xFF & b));
        }
    }
}
