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

package io.questdb.griffin;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

/**
 * Variables a client declares for one statement outside its text, such as the
 * {@code variables} parameter of the HTTP {@code /exec} endpoint.
 * <p>
 * {@link #of(CharSequence)} reads them from a JSON array of objects with a {@code name} and a
 * {@code value}, in order:
 * <pre>
 * [{"name": "from", "value": "'2026-01-01'"}, {"name": "to", "value": "dateadd('d', 1, @from)"}]
 * </pre>
 * A name has no leading {@code @}; a value is an SQL expression, as on the right of {@code :=} in
 * a DECLARE block, and may read the variables before it.
 * <p>
 * The variables are written out as the assignments of a DECLARE block, see
 * {@link #getDeclarations()}. The parser parses them from a text of their own that follows the
 * statement's text, so the position of anything parsed from them lies past the statement's text,
 * and {@link #indexOf(int)} tells which variable it lies in.
 */
public class QueryVariables implements Mutable {
    // Separates two assignments. The newline ends a line comment the value before it ends with,
    // which would otherwise take the comma with it.
    private static final String ASSIGNMENT_SEPARATOR = "\n,\n";
    private static final int FIELD_NAME_HI = 1;
    private static final int FIELD_NAME_LO = 0;
    private static final int FIELD_VALUE_HI = 3;
    private static final int FIELD_VALUE_LO = 2;
    private static final int FIELDS_PER_VARIABLE = 4;
    // name, value, and the offsets of each in the declarations
    private final IntList bounds = new IntList();
    private final StringSink declarations = new StringSink();
    private final StringSink field = new StringSink();
    private final StringSink name = new StringSink();
    private final StringSink value = new StringSink();
    private boolean isValueMissing;
    private CharSequence json;
    private int pos;
    // position in the JSON text of the value of the variable being read
    private int valuePosition;

    private static boolean isNameChar(char c, boolean isFirst) {
        return c == '_'
                || (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || c >= 0x80
                || (!isFirst && c >= '0' && c <= '9');
    }

    private static boolean equalsIgnoreCase(CharSequence cs, int lo, int hi, CharSequence that) {
        if (hi - lo != that.length()) {
            return false;
        }
        for (int i = lo; i < hi; i++) {
            if (Character.toLowerCase(cs.charAt(i)) != Character.toLowerCase(that.charAt(i - lo))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isBlank(CharSequence cs) {
        for (int i = 0, n = cs.length(); i < n; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        bounds.clear();
        declarations.clear();
        field.clear();
        name.clear();
        value.clear();
        json = null;
        pos = 0;
    }

    /**
     * Returns the assignments of a DECLARE block, without the DECLARE keyword:
     * {@code @name := value}, one per variable, separated by commas.
     */
    public CharSequence getDeclarations() {
        return declarations;
    }

    /**
     * Returns the offset in {@link #getDeclarations()} just past the name of the variable at the
     * index.
     */
    public int getNameHi(int index) {
        return bounds.getQuick(index * FIELDS_PER_VARIABLE + FIELD_NAME_HI);
    }

    /**
     * Returns the offset in {@link #getDeclarations()} of the name of the variable at the index,
     * past its {@code @}.
     */
    public int getNameLo(int index) {
        return bounds.getQuick(index * FIELDS_PER_VARIABLE + FIELD_NAME_LO);
    }

    /**
     * Returns the offset in {@link #getDeclarations()} just past the value of the variable at the
     * index.
     */
    public int getValueHi(int index) {
        return bounds.getQuick(index * FIELDS_PER_VARIABLE + FIELD_VALUE_HI);
    }

    /**
     * Returns the offset in {@link #getDeclarations()} of the value of the variable at the index.
     */
    public int getValueLo(int index) {
        return bounds.getQuick(index * FIELDS_PER_VARIABLE + FIELD_VALUE_LO);
    }

    /**
     * Returns the index of the variable whose assignment holds the offset in
     * {@link #getDeclarations()}, counting the separator after an assignment as part of it, or -1
     * when there are no variables or the offset lies before the first one.
     */
    public int indexOf(int offset) {
        for (int i = size() - 1; i > -1; i--) {
            // the assignment starts at the '@' before the name
            if (getNameLo(i) - 1 <= offset) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Reads the variables from a JSON array, replacing the ones read before. Throws with the
     * position in the JSON text of what it cannot read, and holds no variables then.
     */
    public void of(CharSequence json) throws SqlException {
        clear();
        this.json = json;
        try {
            parseArray();
        } catch (SqlException e) {
            clear();
            throw e;
        } finally {
            this.json = null;
        }
    }

    public int size() {
        return bounds.size() / FIELDS_PER_VARIABLE;
    }

    private void addVariable(int objectPosition, int namePosition) throws SqlException {
        if (namePosition < 0) {
            throw SqlException.$(objectPosition, "variable has no name");
        }
        if (isValueMissing) {
            throw SqlException.$(objectPosition, "variable has no value [name=").put(name).put(']');
        }
        if (name.length() == 0) {
            throw SqlException.$(namePosition, "variable name is empty");
        }
        for (int i = 0, n = name.length(); i < n; i++) {
            if (!isNameChar(name.charAt(i), i == 0)) {
                throw SqlException.$(namePosition, "invalid variable name [name=").put(name)
                        .put("]; a name starts with a letter or '_' and holds letters, digits and '_' only");
            }
        }
        if (isBlank(value)) {
            throw SqlException.$(valuePosition, "variable value is empty [name=").put(name).put(']');
        }
        for (int i = 0, n = size(); i < n; i++) {
            if (equalsIgnoreCase(declarations, getNameLo(i), getNameHi(i), name)) {
                throw SqlException.$(namePosition, "duplicate variable [name=").put(name).put(']');
            }
        }

        if (size() > 0) {
            declarations.put(ASSIGNMENT_SEPARATOR);
        }
        declarations.put('@');
        bounds.add(declarations.length());
        declarations.put(name);
        bounds.add(declarations.length());
        declarations.put(" := ");
        bounds.add(declarations.length());
        declarations.put(value);
        bounds.add(declarations.length());
    }

    private void expect(char c, String what) throws SqlException {
        skipWhitespace();
        if (pos >= json.length() || json.charAt(pos) != c) {
            throw SqlException.$(pos, what).put(" expected");
        }
        pos++;
    }

    private void parseArray() throws SqlException {
        skipWhitespace();
        if (pos >= json.length() || json.charAt(pos) != '[') {
            throw SqlException.$(pos, "variables must be a JSON array of {\"name\": ..., \"value\": ...} objects");
        }
        pos++;
        skipWhitespace();
        if (pos < json.length() && json.charAt(pos) == ']') {
            pos++;
        } else {
            while (true) {
                parseVariable();
                skipWhitespace();
                if (pos >= json.length()) {
                    throw SqlException.$(pos, "',' or ']' expected");
                }
                final char c = json.charAt(pos);
                if (c == ']') {
                    pos++;
                    break;
                }
                if (c != ',') {
                    throw SqlException.$(pos, "',' or ']' expected");
                }
                pos++;
            }
        }
        skipWhitespace();
        if (pos < json.length()) {
            throw SqlException.$(pos, "unexpected text after the variables array");
        }
    }

    /**
     * Reads a JSON string at the current position into the sink, without the quotes, and moves
     * past it.
     */
    private void parseString(StringSink sink) throws SqlException {
        sink.clear();
        final int start = pos;
        if (pos >= json.length() || json.charAt(pos) != '"') {
            throw SqlException.$(pos, "string expected");
        }
        pos++;
        while (pos < json.length()) {
            final char c = json.charAt(pos++);
            if (c == '"') {
                return;
            }
            if (c < 0x20) {
                throw SqlException.$(pos - 1, "control character in string");
            }
            if (c != '\\') {
                sink.put(c);
                continue;
            }
            if (pos >= json.length()) {
                break;
            }
            final char escaped = json.charAt(pos++);
            switch (escaped) {
                case '"', '\\', '/' -> sink.put(escaped);
                case 'b' -> sink.put('\b');
                case 'f' -> sink.put('\f');
                case 'n' -> sink.put('\n');
                case 'r' -> sink.put('\r');
                case 't' -> sink.put('\t');
                case 'u' -> {
                    if (pos + 4 > json.length()) {
                        throw SqlException.$(pos - 2, "invalid unicode escape");
                    }
                    int code = 0;
                    for (int i = 0; i < 4; i++) {
                        final int digit = Character.digit(json.charAt(pos + i), 16);
                        if (digit < 0) {
                            throw SqlException.$(pos - 2, "invalid unicode escape");
                        }
                        code = (code << 4) | digit;
                    }
                    pos += 4;
                    sink.put((char) code);
                }
                default -> throw SqlException.$(pos - 2, "invalid escape sequence");
            }
        }
        throw SqlException.$(start, "unterminated string");
    }

    private void parseVariable() throws SqlException {
        skipWhitespace();
        final int objectPosition = pos;
        if (pos >= json.length() || json.charAt(pos) != '{') {
            throw SqlException.$(pos, "variable must be a JSON object with a \"name\" and a \"value\"");
        }
        pos++;
        int namePosition = -1;
        valuePosition = -1;
        isValueMissing = true;
        name.clear();
        value.clear();
        skipWhitespace();
        if (pos < json.length() && json.charAt(pos) == '}') {
            pos++;
        } else {
            while (true) {
                skipWhitespace();
                final int fieldPosition = pos;
                parseString(field);
                expect(':', "':'");
                skipWhitespace();
                final int fieldValuePosition = pos;
                if ("name".contentEquals(field)) {
                    if (namePosition > -1) {
                        throw SqlException.$(fieldPosition, "duplicate field [field=name]");
                    }
                    namePosition = fieldValuePosition;
                    parseString(name);
                } else if ("value".contentEquals(field)) {
                    if (!isValueMissing) {
                        throw SqlException.$(fieldPosition, "duplicate field [field=value]");
                    }
                    isValueMissing = false;
                    valuePosition = fieldValuePosition;
                    parseString(value);
                } else {
                    throw SqlException.$(fieldPosition, "unknown field [field=").put(field)
                            .put("]; a variable has a \"name\" and a \"value\"");
                }
                skipWhitespace();
                if (pos >= json.length()) {
                    throw SqlException.$(pos, "',' or '}' expected");
                }
                final char c = json.charAt(pos);
                if (c == '}') {
                    pos++;
                    break;
                }
                if (c != ',') {
                    throw SqlException.$(pos, "',' or '}' expected");
                }
                pos++;
            }
        }
        addVariable(objectPosition, namePosition);
    }

    private void skipWhitespace() {
        while (pos < json.length()) {
            final char c = json.charAt(pos);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            pos++;
        }
    }
}
