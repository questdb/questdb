/*******************************************************************************
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

package io.questdb.test.cairo.types;

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * Compares kit output with the recording of an existing type.
 * <p>
 * A recording is a text block per type and kit class, split into sections, one per path: a line
 * {@code ## <path>} opens a section. A test computes one section per path and mode and asserts it
 * against the section of the same name; the expected text is the same in every mode. Lines are
 * tab-separated and start with the value row's label, so a failure names the type, the value row,
 * the path and the mode.
 * <p>
 * Output is escaped before it is compared ({@link #escape}): a character that does not print
 * (controls other than tab and newline, U+FFFE, U+FFFF, lone surrogates) becomes {@code \\uXXXX},
 * so recordings hold no invisible characters.
 * <p>
 * There is no switch that rewrites a recording: a difference is a behaviour change to explain.
 */
public final class TypeConformanceRecording {
    private static final String SECTION_PREFIX = "## ";

    private TypeConformanceRecording() {
    }

    /**
     * Asserts one section of a type's recording.
     *
     * @param type      the kit type
     * @param path      the section name, which is the kit path
     * @param mode      the mode the output was produced in, for the failure message
     * @param recording the type's whole recording, or null when the class has none for it
     * @param actual    the output, escaped with {@link #escape}
     */
    public static void assertSection(TypeConformanceTypes.Entry type, String path, String mode, @Nullable String recording, CharSequence actual) {
        if (recording == null) {
            Assert.fail("type=" + type.label + " path=" + path + " mode=" + mode + ": no recording for this type");
        }
        final String expected = sections(recording).get(path);
        if (expected == null) {
            Assert.fail("type=" + type.label + " path=" + path + " mode=" + mode + ": the recording has no section " + path
                    + "\nactual:\n" + actual);
        }
        final String actualText = actual.toString();
        if (expected.equals(actualText)) {
            return;
        }
        final String[] expectedLines = expected.split("\n", -1);
        final String[] actualLines = actualText.split("\n", -1);
        int line = 0;
        while (line < expectedLines.length && line < actualLines.length && expectedLines[line].equals(actualLines[line])) {
            line++;
        }
        final String differing = line < expectedLines.length ? expectedLines[line] : actualLines[line];
        final int tab = differing.indexOf('\t');
        final String row = tab > -1 ? differing.substring(0, tab) : differing;
        Assert.assertEquals(
                "type=" + type.label + " row=" + row + " path=" + path + " mode=" + mode + " (first difference at line " + (line + 1) + ")",
                expected,
                actualText
        );
    }

    /**
     * Makes output printable: every character that does not print becomes {@code \\uXXXX}.
     */
    public static String escape(CharSequence text) {
        final StringSink sink = new StringSink();
        for (int i = 0, n = text.length(); i < n; i++) {
            final char c = text.charAt(i);
            final boolean isLoneSurrogate = Character.isSurrogate(c) && !(
                    Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(text.charAt(i + 1))
                            || Character.isLowSurrogate(c) && i > 0 && Character.isHighSurrogate(text.charAt(i - 1))
            );
            if ((c < 0x20 && c != '\t' && c != '\n') || (c >= 0x7F && c < 0xA0) || c == 0xFFFE || c == 0xFFFF || isLoneSurrogate) {
                sink.put("\\u");
                final String hex = Integer.toHexString(c);
                for (int pad = hex.length(); pad < 4; pad++) {
                    sink.put('0');
                }
                sink.put(hex);
            } else {
                sink.put(c);
            }
        }
        return sink.toString();
    }

    /**
     * Splits a recording into its sections, by name. Text before the first section header is
     * ignored.
     */
    public static Map<String, String> sections(String recording) {
        final Map<String, String> sections = new HashMap<>();
        String name = null;
        final StringBuilder body = new StringBuilder();
        for (String line : recording.split("\n", -1)) {
            if (line.startsWith(SECTION_PREFIX)) {
                if (name != null) {
                    sections.put(name, body.toString());
                }
                name = line.substring(SECTION_PREFIX.length());
                body.setLength(0);
            } else if (name != null) {
                body.append(line).append('\n');
            }
        }
        if (name != null) {
            // a text block ends with a newline, which the split turns into a last empty line
            sections.put(name, body.substring(0, Math.max(0, body.length() - 1)));
        }
        return sections;
    }
}
