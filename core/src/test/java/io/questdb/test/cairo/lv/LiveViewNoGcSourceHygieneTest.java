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

import io.questdb.cairo.lv.LiveViewCheckpointCompaction;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class LiveViewNoGcSourceHygieneTest {
    private static final Pattern BOXED_PRIMITIVE = Pattern.compile(
            "\\b(?:Boolean|Byte|Character|Double|Float|Integer|Long|Short)\\b"
    );
    private static final Pattern COLLECTION_GENERIC_START = Pattern.compile(
            "\\b(?:[A-Za-z_$][A-Za-z0-9_$]*\\.)*"
                    + "(?:[A-Za-z_$][A-Za-z0-9_$]*(?:Collection|Deque|List|Map|Queue|Set)"
                    + "|Collection|Deque|List|Map|Queue|Set)\\s*<"
    );
    private static final Pattern ENUM_DECLARATION = Pattern.compile(
            "\\benum\\s+([A-Za-z_$][A-Za-z0-9_$]*)\\b"
    );
    private static final Pattern ENUM_VALUES = Pattern.compile(
            "\\b(?:[A-Za-z_$][A-Za-z0-9_$]*\\.)*([A-Za-z_$][A-Za-z0-9_$]*)\\.values\\s*\\(\\s*\\)"
    );
    private static final Pattern FORBIDDEN_TYPE = Pattern.compile(
            "\\b(?:ArrayList|ByteBuffer|HashMap|HashSet|LinkedList|ObjectInputStream|ObjectOutputStream|TreeMap|TreeSet)\\b"
    );
    private static final Pattern PATH_DECLARATION = Pattern.compile(
            "\\b(?:[A-Za-z_\\x24][A-Za-z0-9_\\x24]*\\.)*Path\\s+([A-Za-z_\\x24][A-Za-z0-9_\\x24]*)\\b"
    );
    private static final Pattern PATH_TO_STRING = Pattern.compile(
            "\\b([A-Za-z_\\x24][A-Za-z0-9_\\x24]*)\\s*\\.\\s*toString\\s*\\("
    );
    private static final Pattern COMPILED_ARRAY_COPY = Pattern.compile(
            "\\bArrays\\s*\\.\\s*(?:copyOf|copyOfRange)\\s*\\(|\\bSystem\\s*\\.\\s*arraycopy\\s*\\("
                    + "|\\b[A-Za-z_$][A-Za-z0-9_$]*\\s*\\.\\s*clone\\s*\\(\\s*\\)"
    );
    private static final Pattern RECURRING_COMPILED_ENCODING = Pattern.compile(
            "\\b(?:encodeKeySchema|encodeUtf8|putUtf8)\\s*\\(|\\.\\s*(?:getEncoded|getWindowIdentity)\\s*\\("
                    + "|\\.\\s*getBytes\\s*\\(\\s*(?:StandardCharsets\\s*\\.\\s*)?UTF_8\\s*\\)"
    );
    private static final Pattern METHOD_INVOCATION = Pattern.compile(
            "\\b([A-Za-z_$][A-Za-z0-9_$]*)\\s*\\("
    );
    private static final Pattern NEW_PATH_TO_STRING = Pattern.compile(
            "\\bnew\\s+(?:[A-Za-z_\\x24][A-Za-z0-9_\\x24]*\\.)*Path\\s*\\([^;]*?\\)\\s*\\.\\s*toString\\s*\\("
    );
    private static final Pattern PRIVATE_OR_STATIC = Pattern.compile("\\b(?:private|static)\\b");
    private static final Pattern RECURRING_CALLBACK_COMMIT = Pattern.compile(
            "\\b(?:[Ff]enced[A-Za-z0-9_$]*[Cc]ommit|[Cc]ommit[A-Za-z0-9_$]*)\\s*\\([^)]*"
                    + "\\b(?:Callable|Consumer|Function|Runnable|Supplier)\\b",
            Pattern.DOTALL
    );
    private static final Pattern STATIC_FINAL = Pattern.compile("\\bstatic\\s+final\\b");
    private static final Pattern TYPE_DECLARATION = Pattern.compile(
            "\\b(?:class|enum|interface|record)\\s+[A-Za-z_$][A-Za-z0-9_$]*[^;{]*\\{"
    );

    @Test
    public void testCompactionSourcesAvoidGcConstructs() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        final String[] files = {
                "io/questdb/cairo/lv/LiveViewCheckpointCompaction.java",
                "io/questdb/cairo/lv/LiveViewCheckpointCompactionPlan.java",
                "io/questdb/cairo/lv/LiveViewCheckpointDataStore.java"
        };
        for (int i = 0; i < files.length; i++) {
            final Path file = sourceRoot.resolve(files[i]);
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            findForbiddenTypes(sourceRoot, file, code, violations);
            findBoxedCollectionTypes(sourceRoot, file, code, violations);
        }
        Assert.assertTrue(
                "compaction no-GC source violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testTimelineLifecycleSourcesAvoidGcConstructs() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        final String[] files = {
                "io/questdb/cairo/CairoEngine.java",
                "io/questdb/cairo/lv/LiveViewInstance.java",
                "io/questdb/cairo/lv/LiveViewCheckpointLifecycleState.java",
                "io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java",
                "io/questdb/cairo/lv/LiveViewCheckpointCompaction.java",
                "io/questdb/cairo/lv/LiveViewRefreshJob.java"
        };
        for (int i = 0; i < files.length; i++) {
            final Path file = sourceRoot.resolve(files[i]);
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            findForbiddenTypes(sourceRoot, file, code, violations);
            findBoxedCollectionTypes(sourceRoot, file, code, violations);
            findPathToStringCalls(sourceRoot, file, code, violations);
        }
        Assert.assertTrue(
                "timeline lifecycle no-GC source violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testProductionSourcesAvoidGcConstructs() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<Path> manifest = buildManifest(sourceRoot);
        final Set<String> enumTypes = findDeclaredEnumTypes(manifest);
        final List<String> violations = new ArrayList<>();

        for (int i = 0, n = manifest.size(); i < n; i++) {
            final Path file = manifest.get(i);
            final String source = Files.readString(file, StandardCharsets.UTF_8);
            final String code = stripCommentsAndLiterals(source);
            findForbiddenTypes(sourceRoot, file, code, violations);
            findBoxedCollectionTypes(sourceRoot, file, code, violations);
            findRecurringEnumValues(sourceRoot, file, code, enumTypes, violations);
            findPathToStringCalls(sourceRoot, file, code, violations);
        }

        Assert.assertTrue(
                "live-view no-GC source violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testLifecyclePublicationApisRequireExplicitIdentity() throws IOException {
        final Path sourceRoot = findSourceRoot();
        assertExplicitLifecycleSignatures(
                sourceRoot.resolve("io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java"),
                LiveViewCheckpointTimelineStoreWriter.class,
                "append",
                "publishCompaction",
                "publishRepair",
                "publishTruncate",
                "sweep"
        );
        assertExplicitLifecycleSignatures(
                sourceRoot.resolve("io/questdb/cairo/lv/LiveViewCheckpointCompaction.java"),
                LiveViewCheckpointCompaction.class,
                "compact"
        );
    }

    @Test
    public void testCheckpointCompiledEncodingsAreBorrowedOnPublicationAndRestorePaths() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        final String[] hotFiles = {
                "io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreReader.java",
                "io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java"
        };
        for (int i = 0; i < hotFiles.length; i++) {
            final Path file = sourceRoot.resolve(hotFiles[i]);
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            final String[] methodNames = i == 0
                    ? new String[]{"restoreFunctions", "validateAnchor", "validateFunction", "validateFunctions",
                    "validateWindowStateShape"}
                    : new String[]{"buildRoot", "freezeBoundary", "freezeWindowState"};
            findCompiledEncodingViolationsInMethods(sourceRoot, file, code, methodNames, violations);
        }
        final String[] builderFiles = {
                "io/questdb/cairo/lv/LiveViewCheckpointAnchorRoot.java",
                "io/questdb/cairo/lv/LiveViewCheckpointAnchorRootBuilder.java",
                "io/questdb/cairo/lv/LiveViewCheckpointFunctionRoot.java",
                "io/questdb/cairo/lv/LiveViewCheckpointFunctionRootBuilder.java",
                "io/questdb/cairo/lv/LiveViewCheckpointWindowRoot.java",
                "io/questdb/cairo/lv/LiveViewCheckpointWindowRootBuilder.java"
        };
        for (int i = 0; i < builderFiles.length; i++) {
            final Path file = sourceRoot.resolve(builderFiles[i]);
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            final String[] methodNames = builderFiles[i].endsWith("Builder.java")
                    ? new String[]{"of0"}
                    : new String[]{"ofBuilder", "writeTo"};
            findCompiledEncodingViolationsInMethods(sourceRoot, file, code, methodNames, violations);
        }
        Assert.assertTrue(
                "checkpoint compiled-encoding source violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testRecurringCheckpointSourcesAvoidCallbacks() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        final String[] files = {
                "io/questdb/cairo/lv/LiveViewCheckpointAnchorRootBuilder.java",
                "io/questdb/cairo/lv/LiveViewCheckpointCompaction.java",
                "io/questdb/cairo/lv/LiveViewCheckpointDataStore.java",
                "io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreReader.java",
                "io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java",
                "io/questdb/cairo/lv/LiveViewCheckpointWindowRootBuilder.java",
                "io/questdb/cairo/lv/LiveViewRefreshJob.java"
        };
        for (int i = 0; i < files.length; i++) {
            final Path file = sourceRoot.resolve(files[i]);
            findRecurringCallbacks(
                    sourceRoot,
                    file,
                    stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8)),
                    violations
            );
        }
        Assert.assertTrue(
                "recurring live-view callback source violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testRefreshCommitsUseSpecializedFences() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final Path file = sourceRoot.resolve("io/questdb/cairo/lv/LiveViewRefreshJob.java");
        final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
        assertSpecializedCommit(
                file,
                code,
                "commitLiveViewFenced",
                "walWriter.commitLiveView(seqTxn);",
                2
        );
        assertSpecializedCommit(
                file,
                code,
                "commitLiveViewWithoutDedupFenced",
                "walWriter.commitLiveViewWithoutDedup(seqTxn);",
                2
        );
        assertSpecializedCommit(
                file,
                code,
                "commitLiveViewWithUpsertFenced",
                "walWriter.commitLiveViewWithUpsert(seqTxn);",
                3
        );
        assertSpecializedCommit(
                file,
                code,
                "commitLiveViewWithReplaceRangeFenced",
                "walWriter.commitLiveViewWithReplaceRange(seqTxn, replaceLowTs, replaceHighTs);",
                4
        );
        Assert.assertFalse(code.contains("fencedLiveViewCommit"));
    }

    @Test
    public void testScannerSelfCoverage() {
        assertBoxedCollectionDetected("Map<Long, Value> values;");
        assertBoxedCollectionDetected("Map<String, Integer> values;");
        assertBoxedCollectionDetected("List<Short> values;");
        assertBoxedCollectionDetected("Set<Byte> values;");
        assertBoxedCollectionDetected("Queue<Character> values;");
        assertBoxedCollectionDetected("Deque<Double> values;");
        assertBoxedCollectionDetected("Collection<Float> values;");
        assertBoxedCollectionDetected("java.util.Map<Boolean, Value> values;");
        assertBoxedCollectionDetected("java.util.HashMap<String, Long> values;");
        assertBoxedCollectionDetected("Map<String, List<Integer>> values;");
        assertBoxedCollectionDetected("Map<\n    String,\n    Short\n> values;");

        assertNoBoxedCollectionDetected("LongObjHashMap<Value> values;");
        assertNoBoxedCollectionDetected("ObjList<Value> values;");
        assertNoBoxedCollectionDetected("LongList values;");
        assertNoBoxedCollectionDetected("IntList values;");

        assertRecurringEnumValuesDetected("enum Stage { VALUE } class C { void run() { Stage.values(); } }");
        assertRecurringEnumValuesDetected("enum stage { VALUE } class C { void run() { stage.values(); } }");
        assertNoRecurringEnumValuesDetected(
                "enum Stage { VALUE } class C { private static final Stage[] STAGES = Stage.values(); }"
        );
        assertNoRecurringEnumValuesDetected("class C { void close() { registry.values(); REGISTRY.values(); } }");
        assertPathToStringDetected("Path checkpointsDir; String key = checkpointsDir.toString();");
        assertPathToStringDetected(
                "io.questdb.std.str.Path checkpointsDir; String key = this.checkpointsDir . toString ( );"
        );
        assertPathToStringDetected("Path path; String key = path\n    .toString();");
        assertPathToStringDetected("String key = new Path().toString();");
        // An unrelated cold object stringification is not a Path-key allocation.
        assertNoPathToStringDetected("String coldDescription; String key = coldDescription.toString();");
        assertRecurringCompiledEncodingDetected("LiveViewCheckpointMetadata.encodeKeySchema(types);");
        assertRecurringCompiledEncodingDetected("checkpointFunctionIdentity().getEncoded();");
        assertRecurringCompiledEncodingDetected("plan.getManifest().getEncoded();");
        assertArrayCopyDetected("Arrays.copyOf(identity, identity.length);");
        assertMethodScopedCompiledEncodingDetected("byte[] x = functionIdentity.getEncoded();");
        assertMethodScopedCompiledEncodingDetected("byte[] x = windowStatePlan.getWindowIdentity();");
        assertMethodScopedCompiledEncodingDetected("byte[] x = manifest.getEncoded();");
        assertMethodScopedCompiledEncodingDetected("byte[] x = identity.clone();");
        assertMethodScopedCompiledEncodingDetected("byte[] x = Arrays.copyOfRange(identity, 0, identity.length);");
        assertMethodScopedCompiledEncodingDetected(
                "byte[] x = new byte[identity.length]; System.arraycopy(identity, 0, x, 0, identity.length);"
        );
        assertMethodScopedCompiledEncodingDetected("byte[] x = encodeKeySchema(types);");
        assertMethodScopedCompiledEncodingDetected("byte[] x = encodeUtf8(name);");
        assertMethodScopedCompiledEncodingDetected("putUtf8(sink, 0, name);");
        assertMethodScopedCompiledEncodingDetected("byte[] x = name.getBytes(StandardCharsets.UTF_8);");
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { helper(); } private void helper() { identity.getEncoded(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { first(); } private void first() { second(); } "
                        + "private void second() { plan.getWindowIdentity(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { copy(); } private void copy() { identity.clone(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { first(); } private void first() { copy(); } "
                        + "private static void copy() { Arrays.copyOf(identity, identity.length); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { first(); } private void first() { reencode(); } "
                        + "private void reencode() { encodeKeySchema(types); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { overloaded(1); } private void overloaded(int value) { } "
                        + "private void overloaded(String value) { identity.getEncoded(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { first(); } private void first() { second(); } "
                        + "private void second() { first(); identity.getEncoded(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { helper(); } "
                        + "@Annotation(value = \"x\", flag = true) private <K, V> void helper() { "
                        + "identity.getEncoded(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { helper(); } @Outer(value = @Inner(name = \"x\")) "
                        + "private static <T> void helper() { plan.getWindowIdentity(); }",
                "hot"
        );
        assertMethodScopedCompiledEncodingDetected(
                "void hot() { helper(); } @Annotation(values = {\"x\", \"y\"}) "
                        + "private static void helper() { encodeKeySchema(types); }",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void hot() { field.run(); } "
                        + "@Annotation(value = \"x\") private Runnable field = () -> identity.getEncoded();",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void hot() { use(field); } "
                        + "@Annotation(value = \"x\") private Object field = factory(() -> identity.getEncoded());",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void hot() { first(); } private void first() { second(); } "
                        + "private void second() { first(); }",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "byte[] readBytes() { byte[] bytes = new byte[length]; return bytes; }",
                "readBytes"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void hot() { load(); } private void load() { readBytes(); } "
                        + "private byte[] readBytes() { return new byte[length]; }",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void of(byte[] x) { this.x = x.clone(); } "
                        + "void ofBorrowedCompiled(byte[] x) { this.x = x; }",
                "ofBorrowedCompiled"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void hot() { of(x); } public void of(byte[] value) { this.x = value.clone(); }",
                "hot"
        );
        assertNoMethodScopedCompiledEncodingDetected(
                "void of0(byte[] x, boolean isBorrowed) { this.x = isBorrowed ? x : x.clone(); }",
                "of0"
        );

        assertRecurringCallbackDetected("class C { void refresh() { visit(entry -> use(entry)); } }");
        assertRecurringCallbackDetected("class C { void refresh() { visit(this::use); } }");
        assertRecurringCallbackDetected(
                "class C { void refresh() { commitThroughFence(() -> writer.commit()); } "
                        + "private void commitThroughFence(Runnable action) { action.run(); } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int k) { switch (k) { "
                        + "case 1 -> visit(entry -> use(entry)); default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                2,
                "class C { Callback refresh(int k) { return switch (k) { "
                        + "case 1 -> entry -> use(entry); default -> entry -> ignore(entry); }; } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int k) { switch (k) { "
                        + "case 1 -> { visit(entry -> use(entry)); } default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                2,
                "class C { void refresh(int k) { switch (k) { "
                        + "case 1 -> combine(left -> use(left), right -> use(right)); default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int outer, int inner) { use(switch (outer) { "
                        + "case 1 -> switch (inner) { case 2 -> visit(entry -> use(entry)); default -> 0; }; "
                        + "default -> 0; }); } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case String text -> visit(entry -> use(text, entry)); default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case String text when !text.isEmpty() -> visit(entry -> use(text, entry)); "
                        + "default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case Comparable<?> comparable: Runnable action = () -> use(comparable); "
                        + "action.run(); break; default: run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case Comparable<@Mark ?> comparable: Runnable action = () -> use(comparable); "
                        + "action.run(); break; default: run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case Comparable<@Mark(1) ?> comparable: Runnable action = () -> use(comparable); "
                        + "action.run(); break; default: run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value) { switch (value) { "
                        + "case Map<? super String, List<? extends Number>> map: "
                        + "Runnable action = () -> use(map); action.run(); break; default: run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int value) { switch (value) { "
                        + "case FLAG ? ONE : TWO: Runnable action = () -> run(); "
                        + "action.run(); break; default: stop(); } } }"
        );
        assertNoRecurringCallbackDetected(
                "class C { int refresh(int value) { return switch (value) { "
                        + "case LOW < HIGH ? ONE : TWO -> 1; default -> 0; }; } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(Object value, boolean flag) { switch (value) { "
                        + "case Comparable<?> comparable when flag ? comparable != null : false -> "
                        + "visit(entry -> use(comparable, entry)); default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C extends Base { void refresh(Object value, boolean flag) { switch (value) { "
                        + "case Object object when flag ? super.accept(object) : false -> "
                        + "visit(entry -> use(object, entry)); default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                2,
                "class C { void refresh(Object value, int nested) { switch (value) { "
                        + "case Comparable<?> comparable: use(switch (nested) { case 1 -> "
                        + "visit(entry -> use(comparable, entry)); default -> 0; }); "
                        + "Runnable action = () -> use(comparable); action.run(); break; default: run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int k) { switch (k) { "
                        + "case 1 -> values[factory.apply(entry -> use(entry))]; default -> run(); } } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int k) { Runnable action = () -> { switch (k) { "
                        + "case 1 -> run(); default -> stop(); } }; } }"
        );
        assertRecurringCallbackCount(
                1,
                "class C { void refresh(int k) { switch (k) { case 1 -> "
                        + "visit(/* fake -> */ entry -> use(\"literal ->\", entry)); default -> run(); } } }"
        );
        assertNoRecurringCallbackDetected("class C { int kind(int value) { return switch (value) { case 1 -> 2; default -> 3; }; } }");
        assertNoRecurringCallbackDetected(
                "class C { int kind(Object value) { return switch (value) { "
                        + "case null, default -> 0; case String text when !text.isEmpty() -> 1; }; } }"
        );
        assertNoRecurringCallbackDetected(
                "class C { String text = \"entry -> use(entry); this::use\"; "
                        + "/* entry -> use(entry); */ // this::use\n }"
        );
        assertNoRecurringCallbackDetected(
                "class C { private static final Runnable SINGLETON = C::run; private static void run() { } }"
        );
    }

    private static List<Path> buildManifest(Path sourceRoot) throws IOException {
        final List<Path> manifest = new ArrayList<>();
        addJavaFiles(manifest, sourceRoot.resolve("io/questdb/cairo/lv"));
        addJavaFiles(manifest, sourceRoot.resolve("io/questdb/griffin/engine/lv"));
        addRequiredFile(manifest, sourceRoot.resolve("io/questdb/cairo/CairoEngine.java"));
        addRequiredFile(
                manifest,
                sourceRoot.resolve("io/questdb/griffin/engine/window/LiveViewCheckpointFunctionCompiler.java")
        );

        final Path operations = sourceRoot.resolve("io/questdb/griffin/engine/ops");
        try (Stream<Path> files = Files.list(operations)) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("CreateLiveViewOperation"))
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(manifest::add);
        }

        addRequiredFile(
                manifest,
                sourceRoot.resolve("io/questdb/griffin/engine/functions/catalogue/LiveViewsFunctionFactory.java")
        );
        addRequiredFile(
                manifest,
                sourceRoot.resolve("io/questdb/griffin/engine/table/ShowCreateLiveViewRecordCursorFactory.java")
        );
        manifest.sort(Comparator.naturalOrder());
        return manifest;
    }

    private static void addJavaFiles(List<Path> manifest, Path directory) throws IOException {
        Assert.assertTrue("missing source directory: " + directory, Files.isDirectory(directory));
        try (Stream<Path> files = Files.walk(directory)) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(manifest::add);
        }
    }

    private static void addRequiredFile(List<Path> manifest, Path file) {
        Assert.assertTrue("missing source file: " + file, Files.isRegularFile(file));
        manifest.add(file);
    }

    private static void assertExplicitLifecycleSignatures(
            Path file,
            Class<?> type,
            String... methodNames
    ) throws IOException {
        final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
        final Pattern methodPattern = Pattern.compile(
                "\\bpublic\\s+(?:static\\s+)?[A-Za-z_$][A-Za-z0-9_$<>.?]*\\s+"
                        + "(append|compact|publishCompaction|publishRepair|publishTruncate|sweep)\\s*\\((.*?)\\)\\s*\\{",
                Pattern.DOTALL
        );
        for (int i = 0; i < methodNames.length; i++) {
            final String methodName = methodNames[i];
            int compiledCount = 0;
            for (java.lang.reflect.Method method : type.getDeclaredMethods()) {
                if (method.getName().equals(methodName)
                        && java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                    compiledCount++;
                }
            }
            Assert.assertEquals("production must expose exactly one public " + methodName + " method", 1, compiledCount);

            int sourceCount = 0;
            final Matcher matcher = methodPattern.matcher(code);
            while (matcher.find()) {
                if (methodName.equals(matcher.group(1))) {
                    sourceCount++;
                    Assert.assertTrue(
                            methodName + " must require explicit lifecycleIdentity",
                            Pattern.compile("\\blong\\s+lifecycleIdentity\\b").matcher(matcher.group(2)).find()
                    );
                }
            }
            Assert.assertEquals("source must declare exactly one public " + methodName + " method", 1, sourceCount);
        }
    }

    private static void assertMethodScopedCompiledEncodingDetected(String statement) {
        assertMethodScopedCompiledEncodingDetected("void hot() { " + statement + " }", "hot");
    }

    private static void assertMethodScopedCompiledEncodingDetected(String methods, String methodName) {
        final String source = "class C { byte[] identity; byte[] x; Object plan; Object types; " + methods + " }";
        final List<String> violations = new ArrayList<>();
        findCompiledEncodingViolationsInMethods(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                new String[]{methodName},
                violations
        );
        Assert.assertFalse("expected method-scoped compiled encoding violation for: " + methods, violations.isEmpty());
    }

    private static void assertNoMethodScopedCompiledEncodingDetected(String methods, String methodName) {
        final String source = "class C { byte[] x; int length; " + methods + " }";
        final List<String> violations = new ArrayList<>();
        findCompiledEncodingViolationsInMethods(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                new String[]{methodName},
                violations
        );
        Assert.assertTrue("unexpected method-scoped compiled encoding violation: " + violations, violations.isEmpty());
    }

    private static void assertArrayCopyDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findArrayCopies(Path.of("source"), Path.of("source/Snippet.java"), source, violations);
        Assert.assertFalse("expected byte-array copy violation for: " + source, violations.isEmpty());
    }

    private static void assertSpecializedCommit(
            Path file,
            String code,
            String methodName,
            String writerCall,
            int expectedInvocationCount
    ) {
        final List<MethodRegion> methods = findMethodRegions(code, file);
        MethodRegion found = null;
        for (int i = 0, n = methods.size(); i < n; i++) {
            final MethodRegion method = methods.get(i);
            if (methodName.equals(method.name)) {
                Assert.assertNull("duplicate specialized commit method " + methodName, found);
                found = method;
            }
        }
        Assert.assertNotNull("missing specialized commit method " + methodName, found);
        final String declaration = code.substring(findDeclarationStart(code, found.openBrace), found.openBrace);
        Assert.assertFalse("commit fence must not accept a callback: " + declaration,
                Pattern.compile("\\b(?:Callable|Consumer|Function|Runnable|Supplier)\\b").matcher(declaration).find());
        final String body = singleLine(code.substring(found.openBrace, found.closeBrace + 1));
        assertInOrder(
                body,
                "final Lock lock = engine.getRoleSwitchReadLock();",
                "lock.lock();",
                "try {",
                "engine.fireRoleSwitchMintObserver();",
                writerCall,
                "windowStateDirty = false;",
                "instance.setWindowStateDirty(false);",
                "} finally {",
                "lock.unlock();"
        );
        int invocationCount = 0;
        final Matcher invocation = Pattern.compile("\\b" + Pattern.quote(methodName) + "\\s*\\(").matcher(code);
        while (invocation.find()) {
            invocationCount++;
        }
        Assert.assertEquals("unexpected call-site count for " + methodName, expectedInvocationCount, invocationCount);
    }

    private static void assertInOrder(String text, String... fragments) {
        int offset = 0;
        for (int i = 0; i < fragments.length; i++) {
            final int found = text.indexOf(fragments[i], offset);
            Assert.assertTrue("missing or out-of-order fragment " + fragments[i] + " in " + text, found > -1);
            offset = found + fragments[i].length();
        }
    }

    private static void assertBoxedCollectionDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findBoxedCollectionTypes(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertFalse("expected boxed collection violation for: " + source, violations.isEmpty());
    }

    private static void assertNoBoxedCollectionDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findBoxedCollectionTypes(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertTrue("unexpected boxed collection violation: " + violations, violations.isEmpty());
    }

    private static void assertNoRecurringEnumValuesDetected(String source) {
        final String code = stripCommentsAndLiterals(source);
        final List<String> violations = new ArrayList<>();
        findRecurringEnumValues(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                code,
                findDeclaredEnumTypes(code),
                violations
        );
        Assert.assertTrue("unexpected recurring enum values violation: " + violations, violations.isEmpty());
    }

    private static void assertNoPathToStringDetected(String source) {
        Assert.assertFalse("unexpected lifecycle Path.toString violation for: " + source, hasLifecyclePathToString(source));
    }

    private static void assertNoRecurringCallbackDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findRecurringCallbacks(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertTrue("unexpected recurring callback violation: " + violations, violations.isEmpty());
    }

    private static void assertPathToStringDetected(String source) {
        Assert.assertTrue("expected lifecycle Path.toString violation for: " + source, hasLifecyclePathToString(source));
    }

    private static void assertRecurringEnumValuesDetected(String source) {
        final String code = stripCommentsAndLiterals(source);
        final List<String> violations = new ArrayList<>();
        findRecurringEnumValues(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                code,
                findDeclaredEnumTypes(code),
                violations
        );
        Assert.assertEquals("expected one recurring enum values violation for: " + source, 1, violations.size());
    }

    private static void assertRecurringCompiledEncodingDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findRecurringCompiledEncodings(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                source,
                violations
        );
        Assert.assertFalse("expected recurring compiled encoding violation for: " + source, violations.isEmpty());
    }

    private static void assertRecurringCallbackCount(int expected, String source) {
        final List<String> violations = new ArrayList<>();
        findRecurringCallbacks(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertEquals(
                "unexpected recurring callback violations for: " + source + ": " + violations,
                expected,
                violations.size()
        );
    }

    private static void assertRecurringCallbackDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findRecurringCallbacks(
                Path.of("source"),
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertFalse("expected recurring callback violation for: " + source, violations.isEmpty());
    }

    private static boolean hasLifecyclePathToString(String source) {
        final String code = stripCommentsAndLiterals(source);
        final List<String> violations = new ArrayList<>();
        findPathToStringCalls(Path.of("source"), Path.of("source/Snippet.java"), code, violations);
        return !violations.isEmpty();
    }

    private static Path findSourceRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            final Path repositorySourceRoot = current.resolve("core/src/main/java");
            if (Files.isDirectory(repositorySourceRoot.resolve("io/questdb/cairo/lv"))) {
                return repositorySourceRoot;
            }

            final Path moduleSourceRoot = current.resolve("src/main/java");
            if (Files.isDirectory(moduleSourceRoot.resolve("io/questdb/cairo/lv"))) {
                return moduleSourceRoot;
            }
            current = current.getParent();
        }
        throw new AssertionError("cannot find core/src/main/java from " + System.getProperty("user.dir"));
    }

    private static void findBoxedCollectionTypes(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        final Matcher collectionMatcher = COLLECTION_GENERIC_START.matcher(code);
        while (collectionMatcher.find()) {
            final int genericStart = code.indexOf('<', collectionMatcher.start());
            final int genericEnd = findGenericEnd(code, genericStart);
            if (genericEnd > genericStart
                    && BOXED_PRIMITIVE.matcher(code.substring(genericStart + 1, genericEnd)).find()) {
                addViolation(
                        sourceRoot,
                        file,
                        code,
                        collectionMatcher.start(),
                        "boxed primitive collection type: "
                                + singleLine(code.substring(collectionMatcher.start(), genericEnd + 1)),
                        violations
                );
            }
        }
    }

    private static void findCompiledEncodingViolationsInMethods(
            Path sourceRoot,
            Path file,
            String code,
            String[] methodNames,
            List<String> violations
    ) {
        final List<MethodRegion> methods = findMethodRegions(code, file);
        final boolean[] visited = new boolean[methods.size()];
        final List<MethodRegion> reachable = new ArrayList<>();
        for (int i = 0; i < methodNames.length; i++) {
            final String methodName = methodNames[i];
            int methodCount = 0;
            for (int j = 0, n = methods.size(); j < n; j++) {
                final MethodRegion method = methods.get(j);
                if (methodName.equals(method.name)) {
                    methodCount++;
                    if (!visited[j]) {
                        visited[j] = true;
                        reachable.add(method);
                    }
                }
            }
            Assert.assertTrue("missing scanned method " + methodName + " in " + file, methodCount > 0);
        }

        for (int i = 0; i < reachable.size(); i++) {
            final MethodRegion method = reachable.get(i);
            final Matcher encoding = RECURRING_COMPILED_ENCODING.matcher(code)
                    .region(method.openBrace, method.closeBrace + 1);
            while (encoding.find()) {
                addViolation(sourceRoot, file, code, encoding.start(), "recurring compiled encoding", violations);
            }
            final Matcher copy = COMPILED_ARRAY_COPY.matcher(code)
                    .region(method.openBrace, method.closeBrace + 1);
            while (copy.find()) {
                if (!isAllowedBorrowOrPublicClone(code, method.openBrace, copy.start(), copy.end())) {
                    addViolation(sourceRoot, file, code, copy.start(), "compiled byte-array copy", violations);
                }
            }

            final Matcher invocation = METHOD_INVOCATION.matcher(code)
                    .region(method.openBrace + 1, method.closeBrace);
            while (invocation.find()) {
                final String calledName = invocation.group(1);
                for (int j = 0, n = methods.size(); j < n; j++) {
                    final MethodRegion candidate = methods.get(j);
                    if (!visited[j]
                            && candidate.isHelper
                            && candidate.ownerOpenBrace == method.ownerOpenBrace
                            && candidate.declarationDepth == method.declarationDepth
                            && calledName.equals(candidate.name)) {
                        visited[j] = true;
                        reachable.add(candidate);
                    }
                }
            }
        }
    }

    private static int findDeclarationStart(String code, int offset) {
        int parenthesisDepth = 0;
        int bracketDepth = 0;
        for (int i = offset - 1; i > -1; i--) {
            final char c = code.charAt(i);
            if (c == ')') {
                parenthesisDepth++;
            } else if (c == '(') {
                if (parenthesisDepth > 0) {
                    parenthesisDepth--;
                }
            } else if (c == ']') {
                bracketDepth++;
            } else if (c == '[') {
                if (bracketDepth > 0) {
                    bracketDepth--;
                }
            } else if (parenthesisDepth == 0
                    && bracketDepth == 0
                    && (c == ';' || c == '{' || c == '}')) {
                return i + 1;
            }
        }
        return 0;
    }

    private static List<MethodRegion> findMethodRegions(String code, Path file) {
        final int[] braceDepth = new int[code.length()];
        int depth = 0;
        for (int i = 0, n = code.length(); i < n; i++) {
            braceDepth[i] = depth;
            final char c = code.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            }
        }

        final List<TypeRegion> types = findTypeRegions(code, file);
        final List<MethodRegion> methods = new ArrayList<>();
        final Matcher declaration = METHOD_INVOCATION.matcher(code);
        while (declaration.find()) {
            final String methodName = declaration.group(1);
            if (isDeclarationKeyword(methodName)) {
                continue;
            }
            final int openParenthesis = code.indexOf('(', declaration.end(1));
            final int closeParenthesis = findMatchingDelimiter(code, openParenthesis, '(', ')');
            if (closeParenthesis < 0) {
                continue;
            }
            int openBrace = skipWhitespace(code, closeParenthesis + 1);
            if (openBrace >= code.length() || code.charAt(openBrace) != '{') {
                if (!startsWithWord(code, openBrace, "throws")) {
                    continue;
                }
                openBrace = code.indexOf('{', openBrace);
                if (openBrace < 0) {
                    continue;
                }
                final String suffix = code.substring(closeParenthesis + 1, openBrace);
                if (suffix.indexOf(';') > -1 || suffix.indexOf('=') > -1 || suffix.contains("->")) {
                    continue;
                }
            }

            final int declarationStart = findDeclarationStart(code, declaration.start(1));
            final String head = code.substring(declarationStart, declaration.start(1));
            if (!isPossibleDeclarationHead(head)) {
                continue;
            }
            final int closeBrace = findMatchingDelimiter(code, openBrace, '{', '}');
            Assert.assertTrue("unterminated method " + methodName + " in " + file, closeBrace > openBrace);
            final int ownerOpenBrace = findOwnerOpenBrace(types, openBrace);
            if (ownerOpenBrace > -1) {
                methods.add(new MethodRegion(
                        methodName,
                        openBrace,
                        closeBrace,
                        ownerOpenBrace,
                        braceDepth[openBrace],
                        PRIVATE_OR_STATIC.matcher(head).find()
                ));
            }
        }
        return methods;
    }

    private static int findOwnerOpenBrace(List<TypeRegion> types, int methodOpenBrace) {
        int ownerOpenBrace = -1;
        for (int i = 0, n = types.size(); i < n; i++) {
            final TypeRegion type = types.get(i);
            if (type.openBrace < methodOpenBrace
                    && methodOpenBrace < type.closeBrace
                    && type.openBrace > ownerOpenBrace) {
                ownerOpenBrace = type.openBrace;
            }
        }
        return ownerOpenBrace;
    }

    private static List<TypeRegion> findTypeRegions(String code, Path file) {
        final List<TypeRegion> types = new ArrayList<>();
        final Matcher declaration = TYPE_DECLARATION.matcher(code);
        while (declaration.find()) {
            final int openBrace = declaration.end() - 1;
            final int closeBrace = findMatchingDelimiter(code, openBrace, '{', '}');
            Assert.assertTrue("unterminated type declaration in " + file, closeBrace > openBrace);
            types.add(new TypeRegion(openBrace, closeBrace));
        }
        return types;
    }

    private static boolean isDeclarationKeyword(String name) {
        return "catch".equals(name)
                || "do".equals(name)
                || "for".equals(name)
                || "if".equals(name)
                || "switch".equals(name)
                || "synchronized".equals(name)
                || "try".equals(name)
                || "while".equals(name);
    }

    private static boolean isPossibleDeclarationHead(String head) {
        return !head.trim().isEmpty()
                && !hasTopLevelDeclarationDisqualifier(head)
                && !Pattern.compile("\\b(?:new|return|throw)\\b").matcher(head).find();
    }

    private static boolean hasTopLevelDeclarationDisqualifier(String head) {
        int parenthesisDepth = 0;
        int bracketDepth = 0;
        int braceDepth = 0;
        for (int i = 0, n = head.length(); i < n; i++) {
            final char c = head.charAt(i);
            if (c == '(') {
                parenthesisDepth++;
            } else if (c == ')') {
                parenthesisDepth--;
            } else if (c == '[') {
                bracketDepth++;
            } else if (c == ']') {
                bracketDepth--;
            } else if (c == '{') {
                braceDepth++;
            } else if (c == '}') {
                braceDepth--;
            } else if (parenthesisDepth == 0 && bracketDepth == 0 && braceDepth == 0) {
                if (c == '=' || c == '-' && i + 1 < n && head.charAt(i + 1) == '>') {
                    return true;
                }
            }
        }
        return false;
    }

    private static int skipWhitespace(String code, int offset) {
        int result = offset;
        while (result < code.length() && Character.isWhitespace(code.charAt(result))) {
            result++;
        }
        return result;
    }

    private static boolean startsWithWord(String code, int offset, String word) {
        final int end = offset + word.length();
        return offset > -1
                && end <= code.length()
                && code.regionMatches(offset, word, 0, word.length())
                && (end == code.length() || !Character.isJavaIdentifierPart(code.charAt(end)));
    }

    private static int findMatchingDelimiter(String code, int open, char left, char right) {
        int depth = 0;
        for (int i = open, n = code.length(); i < n; i++) {
            final char c = code.charAt(i);
            if (c == left) {
                depth++;
            } else if (c == right && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isAllowedBorrowOrPublicClone(String code, int methodStart, int copyStart, int copyEnd) {
        final String construct = code.substring(Math.max(methodStart, copyStart - 96), copyEnd);
        return Pattern.compile("isBorrowed\\s*\\?[^:;{}]+:\\s*"
                + "[A-Za-z_$][A-Za-z0-9_$]*\\s*\\.\\s*clone\\s*\\(\\s*\\)\\s*$")
                .matcher(construct)
                .find();
    }

    private static void findArrayCopies(Path sourceRoot, Path file, String code, List<String> violations) {
        final Matcher matcher = COMPILED_ARRAY_COPY.matcher(code);
        while (matcher.find()) {
            addViolation(sourceRoot, file, code, matcher.start(), "compiled byte-array copy", violations);
        }
    }

    private static Set<String> findDeclaredEnumTypes(List<Path> manifest) throws IOException {
        final Set<String> enumTypes = new HashSet<>();
        for (int i = 0, n = manifest.size(); i < n; i++) {
            final String source = Files.readString(manifest.get(i), StandardCharsets.UTF_8);
            enumTypes.addAll(findDeclaredEnumTypes(stripCommentsAndLiterals(source)));
        }
        return enumTypes;
    }

    private static Set<String> findDeclaredEnumTypes(String code) {
        final Set<String> enumTypes = new HashSet<>();
        final Matcher matcher = ENUM_DECLARATION.matcher(code);
        while (matcher.find()) {
            enumTypes.add(matcher.group(1));
        }
        return enumTypes;
    }

    private static void findForbiddenTypes(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        final Matcher matcher = FORBIDDEN_TYPE.matcher(code);
        while (matcher.find()) {
            addViolation(sourceRoot, file, code, matcher.start(), matcher.group(), violations);
        }
    }

    private static void findPathToStringCalls(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        final Set<String> pathVariables = new HashSet<>();
        final Matcher declarationMatcher = PATH_DECLARATION.matcher(code);
        while (declarationMatcher.find()) {
            pathVariables.add(declarationMatcher.group(1));
        }

        final Matcher invocationMatcher = PATH_TO_STRING.matcher(code);
        while (invocationMatcher.find()) {
            if (pathVariables.contains(invocationMatcher.group(1))) {
                addViolation(
                        sourceRoot,
                        file,
                        code,
                        invocationMatcher.start(),
                        "Path.toString() key creation: " + singleLine(invocationMatcher.group()),
                        violations
                );
            }
        }

        final Matcher constructionMatcher = NEW_PATH_TO_STRING.matcher(code);
        while (constructionMatcher.find()) {
            addViolation(
                    sourceRoot,
                    file,
                    code,
                    constructionMatcher.start(),
                    "Path.toString() key creation: " + singleLine(constructionMatcher.group()),
                    violations
            );
        }
    }

    private static void findRecurringCompiledEncodings(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        final Matcher matcher = RECURRING_COMPILED_ENCODING.matcher(code);
        while (matcher.find()) {
            addViolation(sourceRoot, file, code, matcher.start(), "recurring compiled encoding", violations);
        }
    }

    private static void findRecurringCallbacks(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        for (int offset = 0; offset < code.length() - 1; offset++) {
            final char first = code.charAt(offset);
            final char second = code.charAt(offset + 1);
            if (first == '-' && second == '>') {
                if (!isSwitchArrow(code, offset) && !isStaticFinalInitializer(code, offset)) {
                    addViolation(sourceRoot, file, code, offset, "recurring lambda", violations);
                }
                offset++;
            } else if (first == ':' && second == ':') {
                if (!isStaticFinalInitializer(code, offset)) {
                    addViolation(sourceRoot, file, code, offset, "recurring method reference", violations);
                }
                offset++;
            }
        }
        final Matcher callbackCommit = RECURRING_CALLBACK_COMMIT.matcher(code);
        while (callbackCommit.find()) {
            addViolation(
                    sourceRoot,
                    file,
                    code,
                    callbackCommit.start(),
                    "callback-based commit API: " + singleLine(callbackCommit.group()),
                    violations
            );
        }
    }

    private static void findRecurringEnumValues(
            Path sourceRoot,
            Path file,
            String code,
            Set<String> enumTypes,
            List<String> violations
    ) {
        final Matcher matcher = ENUM_VALUES.matcher(code);
        while (matcher.find()) {
            if (enumTypes.contains(matcher.group(1)) && !isStaticFinalInitializer(code, matcher.start())) {
                addViolation(
                        sourceRoot,
                        file,
                        code,
                        matcher.start(),
                        "recurring enum array clone: " + matcher.group(),
                        violations
                );
            }
        }
    }

    private static int findGenericEnd(String code, int genericStart) {
        int depth = 0;
        for (int i = genericStart, n = code.length(); i < n; i++) {
            final char c = code.charAt(i);
            if (c == '<') {
                depth++;
            } else if (c == '>' && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static void addViolation(
            Path sourceRoot,
            Path file,
            String code,
            int offset,
            String construct,
            List<String> violations
    ) {
        int line = 1;
        for (int i = 0; i < offset; i++) {
            if (code.charAt(i) == '\n') {
                line++;
            }
        }
        violations.add(sourceRoot.relativize(file) + ":" + line + ": " + construct);
    }

    private static boolean isStaticFinalInitializer(String code, int offset) {
        int statementStart = -1;
        for (int i = offset - 1; i > -1; i--) {
            final char c = code.charAt(i);
            if (c == ';' || c == '{' || c == '}') {
                statementStart = i;
                break;
            }
        }
        return STATIC_FINAL.matcher(code.substring(statementStart + 1, offset)).find();
    }

    private static int findEnclosingSwitchBodyStart(String code, int offset) {
        int nestedBraceDepth = 0;
        for (int i = offset - 1; i > -1; i--) {
            final char c = code.charAt(i);
            if (c == '}') {
                nestedBraceDepth++;
            } else if (c == '{') {
                if (nestedBraceDepth > 0) {
                    nestedBraceDepth--;
                } else if (isSwitchBodyStart(code, i)) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean isSwitchArrow(String code, int offset) {
        final int switchBodyStart = findEnclosingSwitchBodyStart(code, offset);
        if (switchBodyStart < 0) {
            return false;
        }
        int braceDepth = 0;
        int bracketDepth = 0;
        int conditionalDepth = 0;
        int parenthesisDepth = 0;
        boolean isLabel = false;
        for (int i = switchBodyStart + 1; i < offset; i++) {
            final char c = code.charAt(i);
            final boolean isTopLevel = braceDepth == 0 && bracketDepth == 0 && parenthesisDepth == 0;
            if (isTopLevel && Character.isJavaIdentifierStart(c)) {
                final int tokenStart = i++;
                while (i < offset && Character.isJavaIdentifierPart(code.charAt(i))) {
                    i++;
                }
                final String token = code.substring(tokenStart, i--);
                if ("case".equals(token) || "default".equals(token)) {
                    isLabel = true;
                    conditionalDepth = 0;
                }
            } else if (isTopLevel && c == '?' && !isGenericWildcard(code, i, offset)) {
                conditionalDepth++;
            } else if (isTopLevel && c == ':' && (i == 0 || code.charAt(i - 1) != ':')
                    && (i + 1 >= offset || code.charAt(i + 1) != ':')) {
                if (conditionalDepth > 0) {
                    conditionalDepth--;
                } else {
                    isLabel = false;
                }
            } else if (isTopLevel && c == '-' && i + 1 < offset && code.charAt(i + 1) == '>') {
                isLabel = false;
                i++;
            } else if (isTopLevel && c == ';') {
                isLabel = false;
                conditionalDepth = 0;
            } else if (c == '(') {
                parenthesisDepth++;
            } else if (c == ')') {
                parenthesisDepth--;
            } else if (c == '[') {
                bracketDepth++;
            } else if (c == ']') {
                bracketDepth--;
            } else if (c == '{') {
                braceDepth++;
            } else if (c == '}') {
                braceDepth--;
            }
        }
        return isLabel && braceDepth == 0 && bracketDepth == 0 && parenthesisDepth == 0;
    }

    private static boolean isGenericWildcard(String code, int offset, int limit) {
        int next = offset + 1;
        while (next < limit && Character.isWhitespace(code.charAt(next))) {
            next++;
        }
        if (next >= limit) {
            return false;
        }
        if (code.charAt(next) == '>' || code.charAt(next) == ',') {
            return true;
        }
        if (Character.isJavaIdentifierStart(code.charAt(next))) {
            final int tokenStart = next++;
            while (next < limit && Character.isJavaIdentifierPart(code.charAt(next))) {
                next++;
            }
            final String token = code.substring(tokenStart, next);
            return "extends".equals(token)
                    || ("super".equals(token) && hasGenericArgumentPrefix(code, offset));
        }
        return false;
    }

    private static boolean hasGenericArgumentPrefix(String code, int offset) {
        int cursor = offset - 1;
        while (cursor > -1 && Character.isWhitespace(code.charAt(cursor))) {
            cursor--;
        }
        while (cursor > -1) {
            final char c = code.charAt(cursor);
            if (c == '<' || c == ',') {
                return true;
            }
            if (c == ')') {
                int depth = 1;
                while (--cursor > -1 && depth > 0) {
                    final char nested = code.charAt(cursor);
                    if (nested == ')') {
                        depth++;
                    } else if (nested == '(') {
                        depth--;
                    }
                }
                if (depth != 0) {
                    return false;
                }
                cursor--;
                while (cursor > -1 && Character.isWhitespace(code.charAt(cursor))) {
                    cursor--;
                }
            }
            if (cursor < 0 || !Character.isJavaIdentifierPart(code.charAt(cursor))) {
                return false;
            }
            do {
                while (cursor > -1 && Character.isJavaIdentifierPart(code.charAt(cursor))) {
                    cursor--;
                }
                if (cursor < 0 || code.charAt(cursor) != '.') {
                    break;
                }
                cursor--;
            } while (cursor > -1 && Character.isJavaIdentifierPart(code.charAt(cursor)));
            if (cursor < 0 || code.charAt(cursor) != '@') {
                return false;
            }
            cursor--;
            while (cursor > -1 && Character.isWhitespace(code.charAt(cursor))) {
                cursor--;
            }
        }
        return false;
    }

    private static boolean isSwitchBodyStart(String code, int braceOffset) {
        int selectorEnd = braceOffset - 1;
        while (selectorEnd > -1 && Character.isWhitespace(code.charAt(selectorEnd))) {
            selectorEnd--;
        }
        if (selectorEnd < 0 || code.charAt(selectorEnd) != ')') {
            return false;
        }
        int parenthesisDepth = 1;
        for (int i = selectorEnd - 1; i > -1; i--) {
            final char c = code.charAt(i);
            if (c == ')') {
                parenthesisDepth++;
            } else if (c == '(' && --parenthesisDepth == 0) {
                int keywordEnd = i - 1;
                while (keywordEnd > -1 && Character.isWhitespace(code.charAt(keywordEnd))) {
                    keywordEnd--;
                }
                int keywordStart = keywordEnd;
                while (keywordStart > -1 && Character.isJavaIdentifierPart(code.charAt(keywordStart))) {
                    keywordStart--;
                }
                return keywordEnd > keywordStart
                        && "switch".regionMatches(0, code, keywordStart + 1, keywordEnd - keywordStart);
            }
        }
        return false;
    }

    private static String singleLine(String value) {
        return value.replaceAll("\\s+", " ").trim();
    }

    private static String stripCommentsAndLiterals(String source) {
        final StringBuilder code = new StringBuilder(source.length());
        int state = 0;
        for (int i = 0, n = source.length(); i < n; i++) {
            final char c = source.charAt(i);
            final char next = i + 1 < n ? source.charAt(i + 1) : 0;
            if (state == 0) {
                if (c == '/' && next == '/') {
                    code.append("  ");
                    i++;
                    state = 1;
                } else if (c == '/' && next == '*') {
                    code.append("  ");
                    i++;
                    state = 2;
                } else if (c == '"' && next == '"' && i + 2 < n && source.charAt(i + 2) == '"') {
                    code.append("   ");
                    i += 2;
                    state = 5;
                } else if (c == '"') {
                    code.append(' ');
                    state = 3;
                } else if (c == '\'') {
                    code.append(' ');
                    state = 4;
                } else {
                    code.append(c);
                }
            } else if (state == 1) {
                if (c == '\n') {
                    code.append('\n');
                    state = 0;
                } else {
                    code.append(' ');
                }
            } else if (state == 2) {
                if (c == '*' && next == '/') {
                    code.append("  ");
                    i++;
                    state = 0;
                } else {
                    code.append(c == '\n' ? '\n' : ' ');
                }
            } else if (state == 3 || state == 4) {
                final char delimiter = state == 3 ? '"' : '\'';
                if (c == '\\' && next != 0) {
                    code.append("  ");
                    i++;
                } else {
                    code.append(c == '\n' ? '\n' : ' ');
                    if (c == delimiter) {
                        state = 0;
                    }
                }
            } else if (c == '"' && next == '"' && i + 2 < n && source.charAt(i + 2) == '"') {
                code.append("   ");
                i += 2;
                state = 0;
            } else {
                code.append(c == '\n' ? '\n' : ' ');
            }
        }
        return code.toString();
    }

    private static final class MethodRegion {
        private final int closeBrace;
        private final int declarationDepth;
        private final boolean isHelper;
        private final String name;
        private final int openBrace;
        private final int ownerOpenBrace;

        private MethodRegion(
                String name,
                int openBrace,
                int closeBrace,
                int ownerOpenBrace,
                int declarationDepth,
                boolean isHelper
        ) {
            this.name = name;
            this.openBrace = openBrace;
            this.closeBrace = closeBrace;
            this.ownerOpenBrace = ownerOpenBrace;
            this.declarationDepth = declarationDepth;
            this.isHelper = isHelper;
        }
    }

    private static final class TypeRegion {
        private final int closeBrace;
        private final int openBrace;

        private TypeRegion(int openBrace, int closeBrace) {
            this.openBrace = openBrace;
            this.closeBrace = closeBrace;
        }
    }
}
