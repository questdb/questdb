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

package io.questdb.test;

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every query assertion in the guarded test class must run under one leak check: the
 * {@link QueryAssertion} builder's own, which runs unless {@link QueryAssertion#noLeakCheck()} turns
 * it off, or an {@code assertMemoryLeak(...)} wrapper that several statements share. This test reads
 * the source of the class and fails on:
 * <ul>
 * <li>a query assertion that runs with no leak check: an {@code assertQuery(...).noLeakCheck()}
 * chain, a {@code ...NoLeakCheck(...)} call, or a call to a helper that makes one, written in a test
 * method outside any {@code assertMemoryLeak(...)} wrapper</li>
 * <li>an {@code assertMemoryLeak(() -> {...})} wrapper that holds a lone {@code assertQuery(...)}
 * chain, and otherwise only setup that can move into {@link QueryAssertion#ddl} or ahead of the
 * chain. The wrapper swaps the builder's check for a hand-rolled one, or runs both.</li>
 * </ul>
 * No other test can catch these: a query returns the same rows whichever check looks for leaks, and
 * whether one does at all.
 */
public class QueryAssertionLeakCheckUsageTest {
    private static final Pattern CHAIN = Pattern.compile("(?<![\\w.])assertQuery\\s*\\(");
    private static final Pattern CLASS = Pattern.compile("\\bclass\\s+\\w+[^{]*\\{");
    private static final String GUARDED_SOURCE = "io/questdb/test/griffin/engine/functions/InTimestampTimestampTest.java";
    private static final Pattern LONE_WRAPPER = Pattern.compile("(?<![\\w.])assertMemoryLeak\\s*\\(\\s*\\(\\s*\\)\\s*->\\s*\\{");
    private static final Pattern METHOD = Pattern.compile("(\\w+)\\s*\\([^()]*\\)\\s*(throws\\s+[\\w.,\\s]+)?$");
    private static final Pattern NO_LEAK_CHECK_CALL = Pattern.compile("(?<![\\w.])(\\w+NoLeakCheck)\\s*\\(");
    // setup that can run inside ddl(...) or ahead of the chain
    private static final ObjList<Pattern> SETUP = new ObjList<>();
    private static final Pattern TEST = Pattern.compile("@Test\\b");
    private static final Pattern WRAPPER = Pattern.compile("(?<![\\w.])assertMemoryLeak\\s*\\(");

    @Test
    public void testGuardedSourceRunsEveryQueryAssertionUnderOneLeakCheck() throws Exception {
        final Path path = testSourceRoot().resolve(GUARDED_SOURCE);
        final String className = GUARDED_SOURCE.substring(GUARDED_SOURCE.lastIndexOf('/') + 1, GUARDED_SOURCE.length() - ".java".length());
        final StringSink problems = new StringSink();
        final int testCount = scan(className, Files.readString(path), problems);
        // a scan that finds no test method would pass for the wrong reason
        Assert.assertTrue("no test method found in " + path, testCount > 0);
        if (!problems.isEmpty()) {
            Assert.fail("A query assertion must run under one leak check: the builder's own, or an"
                    + " assertMemoryLeak() wrapper that several statements share. Found:\n" + problems);
        }
    }

    @Test
    public void testScanFlagsMissingAndRedundantLeakChecks() {
        // Braces, semicolons and assertQuery( in strings, text blocks and comments do not count. A
        // wrapper keeps its place once it holds anything the scan cannot prove movable: another
        // assertion, a loop, a resource, or a custom FilesFacade. A helper that runs an assertion
        // with no leak check, directly or through another helper, needs a wrapper around its call.
        final String source = """
                class Sample {
                    @Test
                    public void lone() throws Exception {
                        assertMemoryLeak(() -> {
                            execute(\"""
                                    CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts)
                                    \""");
                            setCurrentMicros(0);
                            final ObjList<BindVarTuple> cases = new ObjList<>();
                            cases.add(BindVarTuple.ok("{ ; }", "}", bindVariableService -> bindVariableService.setStr(0, "a")));
                            assertQuery("SELECT 1 -- assertQuery(").noLeakCheck().assertBinds(cases);
                        });
                    }

                    @Test
                    public void nested() throws Exception {
                        assertMemoryLeak(() -> {
                            execute("CREATE TABLE t (x INT)");
                            assertQuery("SELECT 1").returns("1\\n");
                        });
                    }

                    @Test
                    public void unwrapped() throws Exception {
                        execute("CREATE TABLE t (x INT)");
                        assertQuery("SELECT 1").noLeakCheck().returns("1\\n");
                        assertExceptionNoLeakCheck("SELECT", 0, "error");
                        assertRows("x");
                        assertQuery("SELECT 2").returns("2\\n");
                    }

                    @Test
                    public void shared() throws Exception {
                        assertMemoryLeak(() -> {
                            assertQuery("a").noLeakCheck().returns("a");
                            assertRows("b");
                        });
                        assertMemoryLeak(ff, () -> assertQuery("b").noLeakCheck().returns("b"));
                    }

                    @Test
                    public void wrappedLoop() throws Exception {
                        assertMemoryLeak(() -> {
                            for (int i = 0; i < 2; i++) {
                                assertQuery("b").noLeakCheck().returns("b");
                            }
                        });
                    }

                    @Test
                    public void wrappedCursor() throws Exception {
                        assertMemoryLeak(() -> {
                            try (RecordCursorFactory factory = select("x")) {
                                Assert.assertNotNull(factory);
                            }
                            // assertQuery("x") in a comment does not count
                            assertQuery("b").noLeakCheck().returns("}");
                        });
                    }

                    private void assertRows(String sql) throws Exception {
                        assertCount(sql);
                    }

                    private void assertCount(String sql) throws Exception {
                        assertQuery(sql).noLeakCheck().returns("count\\n1\\n");
                    }
                }
                """;
        final StringSink problems = new StringSink();
        Assert.assertEquals(6, scan("Sample", source, problems));
        TestUtils.assertEquals(
                """
                        Sample.lone (line 4): assertMemoryLeak() holds a lone assertQuery(...) chain, which turns its own leak check off
                        Sample.nested (line 17): assertMemoryLeak() holds a lone assertQuery(...) chain, so the builder checks for leaks twice
                        Sample.unwrapped (line 26): assertQuery(...).noLeakCheck() runs with no leak check
                        Sample.unwrapped (line 27): assertExceptionNoLeakCheck(...) runs with no leak check
                        Sample.unwrapped (line 28): assertRows(...) runs a query assertion with no leak check
                        """,
                problems
        );
    }

    private static void addStatement(ObjList<String> statements, String body, int lo, int hi) {
        final String statement = body.substring(lo, hi).strip();
        if (!statement.isEmpty()) {
            statements.add(statement);
        }
    }

    // Adds a problem for each query assertion in the method that runs with no leak check
    private static void addUnchecked(String masked, Method method, ObjList<String> uncheckedHelpers, ObjList<Problem> problems) {
        final IntList wrapped = new IntList();
        final Matcher wrapper = WRAPPER.matcher(masked).region(method.lo(), method.hi());
        while (wrapper.find()) {
            wrapped.add(wrapper.start());
            wrapped.add(closing(masked, wrapper.end(), '(', ')'));
        }
        final Matcher chain = CHAIN.matcher(masked).region(method.lo(), method.hi());
        while (chain.find()) {
            if (!isWrapped(wrapped, chain.start())
                    && masked.substring(chain.start(), statementEnd(masked, chain.end(), method.hi())).contains(".noLeakCheck()")) {
                problems.add(new Problem(chain.start(), "assertQuery(...).noLeakCheck() runs with no leak check"));
            }
        }
        final Matcher call = NO_LEAK_CHECK_CALL.matcher(masked).region(method.lo(), method.hi());
        while (call.find()) {
            if (!isWrapped(wrapped, call.start())) {
                problems.add(new Problem(call.start(), call.group(1) + "(...) runs with no leak check"));
            }
        }
        for (int i = 0, n = uncheckedHelpers.size(); i < n; i++) {
            final String helper = uncheckedHelpers.getQuick(i);
            if (helper.endsWith("NoLeakCheck")) {
                // counted above
                continue;
            }
            final Matcher helperCall = Pattern.compile("(?<![\\w.])" + helper + "\\s*\\(").matcher(masked).region(method.lo(), method.hi());
            while (helperCall.find()) {
                if (!isWrapped(wrapped, helperCall.start())) {
                    problems.add(new Problem(helperCall.start(), helper + "(...) runs a query assertion with no leak check"));
                }
            }
        }
    }

    private static void blank(char[] chars, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (chars[i] != '\n') {
                chars[i] = ' ';
            }
        }
    }

    // Returns the index of the bracket that closes the one just before from
    private static int closing(String masked, int from, char open, char close) {
        int depth = 1;
        for (int i = from, n = masked.length(); i < n; i++) {
            final char c = masked.charAt(i);
            if (c == open) {
                depth++;
            } else if (c == close && --depth == 0) {
                return i;
            }
        }
        return masked.length();
    }

    // Reports whether the code after a closing brace carries on the same statement, as catch,
    // finally, else, the while of a do loop, or the rest of an expression do
    private static boolean continuesStatement(String body, int pos) {
        int i = pos;
        while (i < body.length() && Character.isWhitespace(body.charAt(i))) {
            i++;
        }
        if (i == body.length()) {
            return false;
        }
        final char c = body.charAt(i);
        return c == ')' || c == ',' || c == '.' || c == ';'
                || body.startsWith("catch", i) || body.startsWith("finally", i)
                || body.startsWith("else", i) || body.startsWith("while", i);
    }

    // Reports whether the lone assertMemoryLeak(() -> {...}) wrapper whose body starts at lo holds a
    // single assertQuery(...) chain and otherwise only setup, and returns the chain, or null
    private static String loneChain(String masked, int lo) {
        final ObjList<String> statements = statements(masked.substring(lo, closing(masked, lo, '{', '}')));
        String chain = null;
        for (int i = 0, n = statements.size(); i < n; i++) {
            final String statement = statements.getQuick(i);
            if (CHAIN.matcher(statement).lookingAt()) {
                if (chain != null) {
                    return null;
                }
                chain = statement;
            } else if (!isSetup(statement)) {
                return null;
            }
        }
        return chain;
    }

    private static boolean isSetup(String statement) {
        for (int i = 0, n = SETUP.size(); i < n; i++) {
            if (SETUP.getQuick(i).matcher(statement).find()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isWrapped(IntList wrapped, int pos) {
        for (int i = 0, n = wrapped.size(); i < n; i += 2) {
            if (pos > wrapped.getQuick(i) && pos < wrapped.getQuick(i + 1)) {
                return true;
            }
        }
        return false;
    }

    // Blanks out comments and the contents of string, text block and char literals, keeping the
    // quotes and the line breaks, so that the brackets and semicolons in them do not count
    private static String mask(String source) {
        final char[] chars = source.toCharArray();
        final int n = chars.length;
        int i = 0;
        while (i < n) {
            final char c = source.charAt(i);
            if (source.startsWith("//", i)) {
                final int end = source.indexOf('\n', i);
                final int hi = end < 0 ? n : end;
                blank(chars, i, hi);
                i = hi;
            } else if (source.startsWith("/*", i)) {
                final int end = source.indexOf("*/", i + 2);
                final int hi = end < 0 ? n : end + 2;
                blank(chars, i, hi);
                i = hi;
            } else if (source.startsWith("\"\"\"", i)) {
                int end = i + 3;
                while (end < n && !(source.startsWith("\"\"\"", end) && source.charAt(end - 1) != '\\')) {
                    end++;
                }
                blank(chars, i + 3, end);
                i = Math.min(n, end + 3);
            } else if (c == '"' || c == '\'') {
                int end = i + 1;
                while (end < n && source.charAt(end) != c) {
                    end += source.charAt(end) == '\\' ? 2 : 1;
                }
                blank(chars, i + 1, Math.min(end, n));
                i = end + 1;
            } else {
                i++;
            }
        }
        return new String(chars);
    }

    // Lists the methods of the top-level class, with the bounds of their bodies
    private static ObjList<Method> methods(String masked) {
        final ObjList<Method> methods = new ObjList<>();
        final Matcher classDeclaration = CLASS.matcher(masked);
        if (!classDeclaration.find()) {
            return methods;
        }
        int memberStart = classDeclaration.end();
        int i = memberStart;
        while (i < masked.length()) {
            final char c = masked.charAt(i);
            if (c == '}') {
                // the end of the class
                break;
            }
            if (c == ';') {
                memberStart = i + 1;
            } else if (c == '{') {
                final int end = closing(masked, i + 1, '{', '}');
                final String header = masked.substring(memberStart, i).strip();
                final Matcher name = METHOD.matcher(header);
                if (name.find()) {
                    methods.add(new Method(name.group(1), TEST.matcher(header).find(), i + 1, end));
                }
                memberStart = end + 1;
                i = end;
            }
            i++;
        }
        return methods;
    }

    private static String methodName(ObjList<Method> methods, int pos) {
        for (int i = 0, n = methods.size(); i < n; i++) {
            final Method method = methods.getQuick(i);
            if (pos >= method.lo() && pos < method.hi()) {
                return method.name();
            }
        }
        return "?";
    }

    // Appends a line for each query assertion that runs with no leak check, or under a wrapper that
    // holds nothing else, and returns how many test methods the source has
    private static int scan(CharSequence className, String source, StringSink problems) {
        final String masked = mask(source);
        final ObjList<Method> methods = methods(masked);

        // the helpers that run a query assertion with no leak check, directly or through another helper
        final ObjList<String> uncheckedHelpers = new ObjList<>();
        final ObjList<Problem> found = new ObjList<>();
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int i = 0, n = methods.size(); i < n; i++) {
                final Method method = methods.getQuick(i);
                if (!method.isTest() && !uncheckedHelpers.contains(method.name())) {
                    found.clear();
                    addUnchecked(masked, method, uncheckedHelpers, found);
                    if (found.size() > 0) {
                        uncheckedHelpers.add(method.name());
                        isChanged = true;
                    }
                }
            }
        }

        found.clear();
        int testCount = 0;
        for (int i = 0, n = methods.size(); i < n; i++) {
            final Method method = methods.getQuick(i);
            if (method.isTest()) {
                testCount++;
                addUnchecked(masked, method, uncheckedHelpers, found);
            }
        }
        final Matcher wrapper = LONE_WRAPPER.matcher(masked);
        while (wrapper.find()) {
            final String chain = loneChain(masked, wrapper.end());
            if (chain != null) {
                found.add(new Problem(
                        wrapper.start(),
                        chain.contains(".noLeakCheck()")
                                ? "assertMemoryLeak() holds a lone assertQuery(...) chain, which turns its own leak check off"
                                : "assertMemoryLeak() holds a lone assertQuery(...) chain, so the builder checks for leaks twice"
                ));
            }
        }

        found.sort((a, b) -> Integer.compare(a.pos(), b.pos()));
        for (int i = 0, n = found.size(); i < n; i++) {
            final Problem problem = found.getQuick(i);
            int line = 1;
            for (int j = 0; j < problem.pos(); j++) {
                if (source.charAt(j) == '\n') {
                    line++;
                }
            }
            problems.put(className).put('.').put(methodName(methods, problem.pos()))
                    .put(" (line ").put(line).put("): ").put(problem.text()).put('\n');
        }
        return testCount;
    }

    // Returns the index of the semicolon that ends the statement, from a position inside its first
    // bracket, or of the bracket that closes the expression around it
    private static int statementEnd(String masked, int from, int limit) {
        int depth = 1;
        for (int i = from; i < limit; i++) {
            switch (masked.charAt(i)) {
                case '(', '[', '{' -> depth++;
                case ')', ']', '}' -> {
                    if (--depth < 0) {
                        return i;
                    }
                }
                case ';' -> {
                    if (depth == 0) {
                        return i;
                    }
                }
                default -> {
                }
            }
        }
        return limit;
    }

    // Splits a block body into its top-level statements
    private static ObjList<String> statements(String body) {
        final ObjList<String> statements = new ObjList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0, n = body.length(); i < n; i++) {
            switch (body.charAt(i)) {
                case '(', '[', '{' -> depth++;
                case ')', ']' -> depth--;
                case '}' -> {
                    // a block statement, such as try or for, ends with its closing brace
                    if (--depth == 0 && !continuesStatement(body, i + 1)) {
                        addStatement(statements, body, start, i + 1);
                        start = i + 1;
                    }
                }
                case ';' -> {
                    if (depth == 0) {
                        addStatement(statements, body, start, i + 1);
                        start = i + 1;
                    }
                }
                default -> {
                }
            }
        }
        addStatement(statements, body, start, body.length());
        return statements;
    }

    private static Path testSourceRoot() throws Exception {
        // Maven and the IDE run tests from the core module directory
        final Path fromWorkingDir = Paths.get("src", "test", "java");
        if (Files.exists(fromWorkingDir.resolve(GUARDED_SOURCE))) {
            return fromWorkingDir;
        }
        // otherwise go from the compiled test classes, core/target/test-classes, to core/src/test/java
        final Path testClasses = Paths.get(QueryAssertionLeakCheckUsageTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        return testClasses.getParent().getParent().resolve(Paths.get("src", "test", "java"));
    }

    static {
        // a statement in a literal, or in a constant
        SETUP.add(Pattern.compile("^execute\\s*\\(\\s*(\"|[A-Za-z_][\\w.]*\\s*\\)\\s*;$)"));
        SETUP.add(Pattern.compile("^setCurrentMicros\\s*\\("));
        // a local variable, such as the list of bind variable cases, and the cases added to it
        SETUP.add(Pattern.compile("^(final\\s+)?[A-Za-z_][\\w.]*(<.*>)?(\\[])*\\s+[A-Za-z_]\\w*\\s*="));
        SETUP.add(Pattern.compile("^[A-Za-z_]\\w*\\.add\\s*\\("));
        SETUP.add(Pattern.compile("^bindVariableService\\.\\w+\\s*\\("));
    }

    private record Method(String name, boolean isTest, int lo, int hi) {
    }

    private record Problem(int pos, String text) {
    }
}
