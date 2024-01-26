package io.questdb.client.impl;

import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;

public final class ConfigurationStringParser {
    private CharSequence input;
    private int pos = -1;

    public boolean hasNext() {
        if (pos == -1) {
            return false;
        }
        return pos != input.length();
    }

    public boolean nextKey(StringSink output) {
        output.clear();
        if (pos == -1) {
            return false;
        }
        int n = input.length();
        int start = pos;
        for (; pos < n; pos++) {
            char c = input.charAt(pos);
            if (c == '=') {
                Chars.toLowerCase(input, start, pos, output);
                pos++;
                return true;
            } else if (c == ';') {
                output.put("missing '='");
                pos = -1;
                return false;
            }
        }
        output.clear();
        output.put("missing '='");
        pos = -1;
        return false;
    }

    public boolean nextValue(StringSink output) {
        // todo: escaping ;
        output.clear();
        if (pos == -1) {
            return false;
        }
        int n = input.length();
        int start = pos;
        for (; pos < n; pos++) {
            char c = input.charAt(pos);
            if (c == ';') {
                output.put(input, start, pos);
                pos++;
                return true;
            }
        }

        output.put("missing trailing ';'");
        pos = -1;
        return false;
    }

    public boolean startFrom(CharSequence input, StringSink output) {
        output.clear();
        this.input = input;
        this.pos = 0;
        char lastChar = 0;
        for (int i = 0, n = input.length(); i < n; i++) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) {
                pos = -1;
                output.put("schema contains a whitespace");
                return false;
            }
            if (lastChar == ':') {
                if (c == ':') {
                    if (i == 1) {
                        output.put("schema is empty");
                        pos = -1;
                        return false;
                    }
                    if (i == n - 1) {
                        output.put("missing trailing ';'");
                        pos = -1;
                        return false;
                    }
                    Chars.toLowerCase(input, 0, i - 1, output);
                    pos = i + 1;
                    return true;
                } else {
                    output.put("schema name must start with schema type, e.g. http::");
                    pos = -1;
                    return false;
                }
            } else {
                lastChar = c;
            }
        }
        pos = -1;
        output.put("schema name must start with schema type, e.g. http::");
        return false;
    }
}
