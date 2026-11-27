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

package io.questdb.test.cairo.lv;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Every timestamp the live view code logs is a view or base designated timestamp, in the unit of
 * the view's base table. {@code LogRecord.$ts(long)} always reads microseconds, so a view over a
 * TIMESTAMP_NS base printed each of them some 56,000 years out. The precision-aware
 * {@code $ts(TimestampDriver, long)} prints the value in the driver's unit, and the view's driver
 * is {@code ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType())}.
 * <p>
 * The scan pins that shape at every call site under {@code io/questdb/cairo/lv}, comments and
 * literals skipped: each {@code $ts} call takes two arguments, the first a local or parameter
 * named {@code driver} or the view's driver expression itself. In each file that logs a timestamp,
 * a {@code TimestampDriver driver} local is initialized with that expression, and every call of a
 * method that takes a {@code TimestampDriver driver} parameter passes {@code driver} to it. A
 * constant driver anywhere along the way fails the scan.
 */
public class LiveViewLogTimestampSourceTest {
    private static final Pattern DRIVER_DECLARATION = Pattern.compile("\\bTimestampDriver\\s+driver\\b");
    private static final String DRIVER_NAME = "driver";
    private static final Pattern TS_CALL = Pattern.compile("\\$ts\\s*\\(");
    private static final String VIEW_DRIVER = "ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType())";

    @Test
    public void testEveryLoggedTimestampTakesTheViewsDriver() throws IOException {
        final Path sourceRoot = LiveViewNoGcSourceHygieneTest.findSourceRoot();
        final List<String> violations = new ArrayList<>();
        int calls = 0;
        try (Stream<Path> files = Files.walk(sourceRoot.resolve("io/questdb/cairo/lv"))) {
            for (Path file : (Iterable<Path>) files.filter(path -> path.toString().endsWith(".java")).sorted()::iterator) {
                final String source = Files.readString(file, StandardCharsets.UTF_8);
                calls += findViolations(sourceRoot.relativize(file).toString(), source, violations);
            }
        }
        Assert.assertTrue("the scan must find the job's timestamp log calls [calls=" + calls + ']', calls > 80);
        Assert.assertTrue(
                "live view timestamps logged without the view's driver:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testScannerAcceptsTheViewsDriver() {
        final List<String> violations = new ArrayList<>();
        final int calls = findViolations("C.java", """
                class C {
                    void log(LiveViewInstance instance, long ts) {
                        final TimestampDriver driver = ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType());
                        LOG.info().$("ts=").$ts(driver, ts).$(", next=").$ts(
                                ColumnType.getTimestampDriver(instance.getDefinition().getBaseTimestampType()),
                                ts + 1
                        ).I$();
                        logKept(instance.getDefinition().getViewName(), driver, ts);
                        // $ts(ts) in a comment
                        LOG.info().$("$ts(ts) in a literal").I$();
                    }
                
                    static void logKept(CharSequence viewName, final TimestampDriver driver, long ts) {
                        LOG.info().$("kept [view=").$(viewName).$(", ts=").$ts(driver, ts).I$();
                    }
                }
                """, violations);
        Assert.assertEquals(3, calls);
        Assert.assertEquals(List.of(), violations);
    }

    @Test
    public void testScannerFlagsEveryOtherDriver() {
        final List<String> violations = new ArrayList<>();
        final int calls = findViolations("C.java", """
                class C {
                    void log(LiveViewInstance instance, long ts) {
                        LOG.info().$ts(ts).I$();
                        LOG.info().$ts(MicrosTimestampDriver.INSTANCE, ts).I$();
                        LOG.info().$ts(ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO), ts).I$();
                        LOG.info().$ts(driver, ts, 1).I$();
                    }
                
                    void logLocal(CharSequence viewName, long ts) {
                        final TimestampDriver driver = MicrosTimestampDriver.INSTANCE;
                        LOG.info().$ts(driver, ts).I$();
                        logKept(viewName, MicrosTimestampDriver.INSTANCE, ts);
                        logKept(viewName);
                    }
                
                    static void logKept(CharSequence viewName, TimestampDriver driver, long ts) {
                        LOG.info().$ts(driver, ts).I$();
                    }
                }
                """, violations);
        Assert.assertEquals(6, calls);
        Assert.assertEquals(
                List.of(
                        "C.java:3: $ts(ts)",
                        "C.java:4: $ts(MicrosTimestampDriver.INSTANCE, ts)",
                        "C.java:5: $ts(ColumnType.getTimestampDriver(ColumnType.TIMESTAMP_MICRO), ts)",
                        "C.java:6: $ts(driver, ts, 1)",
                        "C.java:10: TimestampDriver driver = MicrosTimestampDriver.INSTANCE",
                        "C.java:12: logKept(viewName, MicrosTimestampDriver.INSTANCE, ts) passes no driver as argument 2",
                        "C.java:13: logKept(viewName) passes no driver as argument 2"
                ),
                violations
        );
    }

    @Test
    public void testScannerFlagsADriverNeitherLocalNorParameter() {
        final List<String> violations = new ArrayList<>();
        findViolations("C.java", """
                class C {
                    private final TimestampDriver driver;
                    void log(long ts) {
                        LOG.info().$ts(driver, ts).I$();
                    }
                }
                """, violations);
        Assert.assertEquals(List.of("C.java:2: TimestampDriver driver is neither a local nor a parameter"), violations);
    }

    // Adds what source, the file at fileName, gets wrong to violations, and returns how many $ts
    // calls it holds. The rules about driver declarations apply only to a file that logs a
    // timestamp: another one may hold a driver for arithmetic in any unit it needs.
    private static int findViolations(String fileName, String source, List<String> violations) {
        // Kept line for line, so an offset into code is one into source.
        final String code = LiveViewNoGcSourceHygieneTest.stripCommentsAndLiterals(source);
        int calls = 0;
        final Matcher call = TS_CALL.matcher(code);
        while (call.find()) {
            calls++;
            final int open = call.end() - 1;
            final List<String> arguments = arguments(code, open);
            if (arguments.size() != 2 || !isViewDriver(arguments.get(0))) {
                violations.add(location(fileName, code, call.start()) + "$ts(" + String.join(", ", arguments) + ')');
            }
        }
        if (calls == 0) {
            return 0;
        }
        final Matcher declaration = DRIVER_DECLARATION.matcher(code);
        while (declaration.find()) {
            final int next = skipWhitespace(code, declaration.end());
            final char c = code.charAt(next);
            if (c == '=') {
                final String initializer = singleLine(code.substring(next + 1, code.indexOf(';', next)));
                if (!VIEW_DRIVER.equals(initializer)) {
                    violations.add(location(fileName, code, declaration.start()) + "TimestampDriver driver = " + initializer);
                }
            } else if (c == ',' || c == ')') {
                findCallsPassingAnotherDriver(fileName, code, declaration.start(), violations);
            } else {
                violations.add(location(fileName, code, declaration.start()) + "TimestampDriver driver is neither a local nor a parameter");
            }
        }
        return calls;
    }

    // The arguments of the call whose opening parenthesis is at open, each on one line.
    private static List<String> arguments(String code, int open) {
        final List<String> arguments = new ArrayList<>();
        int depth = 0;
        int start = open + 1;
        for (int i = open; i < code.length(); i++) {
            final char c = code.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                final String last = singleLine(code.substring(start, i));
                if (!last.isEmpty() || !arguments.isEmpty()) {
                    arguments.add(last);
                }
                return arguments;
            } else if (c == ',' && depth == 1) {
                arguments.add(singleLine(code.substring(start, i)));
                start = i + 1;
            }
        }
        throw new AssertionError("unbalanced parentheses after offset " + open);
    }

    // The parameter declared at parameter takes the view's driver: every call of its method passes
    // the caller's own driver at its position.
    private static void findCallsPassingAnotherDriver(String fileName, String code, int parameter, List<String> violations) {
        int open = parameter;
        int position = 0;
        for (int depth = 0; ; open--) {
            final char c = code.charAt(open);
            if (c == ')') {
                depth++;
            } else if (c == '(' && depth-- == 0) {
                break;
            } else if (c == ',' && depth == 0) {
                position++;
            }
        }
        int nameEnd = open;
        while (Character.isWhitespace(code.charAt(nameEnd - 1))) {
            nameEnd--;
        }
        int nameStart = nameEnd;
        while (Character.isJavaIdentifierPart(code.charAt(nameStart - 1))) {
            nameStart--;
        }
        final String method = code.substring(nameStart, nameEnd);
        final Matcher call = Pattern.compile("\\b" + Pattern.quote(method) + "\\s*\\(").matcher(code);
        while (call.find()) {
            final int callOpen = call.end() - 1;
            if (callOpen == open) {
                continue;
            }
            final List<String> arguments = arguments(code, callOpen);
            if (arguments.size() <= position || !DRIVER_NAME.equals(arguments.get(position))) {
                violations.add(location(fileName, code, call.start()) + method + '(' + String.join(", ", arguments)
                        + ") passes no driver as argument " + (position + 1));
            }
        }
    }

    private static boolean isViewDriver(String argument) {
        return DRIVER_NAME.equals(argument) || VIEW_DRIVER.equals(argument);
    }

    private static String location(String fileName, String code, int offset) {
        int line = 1;
        for (int i = 0; i < offset; i++) {
            if (code.charAt(i) == '\n') {
                line++;
            }
        }
        return fileName + ':' + line + ": ";
    }

    // The text with every run of whitespace removed but one between two identifier characters, so
    // an expression wrapped over lines reads as it does written on one.
    private static String singleLine(String text) {
        final StringBuilder sink = new StringBuilder(text.length());
        final String trimmed = text.trim();
        for (int i = 0, n = trimmed.length(); i < n; i++) {
            final char c = trimmed.charAt(i);
            if (Character.isWhitespace(c)) {
                int next = i;
                while (Character.isWhitespace(trimmed.charAt(next))) {
                    next++;
                }
                if (Character.isJavaIdentifierPart(trimmed.charAt(i - 1)) && Character.isJavaIdentifierPart(trimmed.charAt(next))) {
                    sink.append(' ');
                }
                i = next - 1;
            } else {
                sink.append(c);
            }
        }
        return sink.toString();
    }

    private static int skipWhitespace(String code, int offset) {
        while (Character.isWhitespace(code.charAt(offset))) {
            offset++;
        }
        return offset;
    }
}
