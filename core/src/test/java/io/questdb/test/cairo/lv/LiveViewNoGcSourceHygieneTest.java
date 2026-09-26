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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
    private static final Pattern ENTRY_SET = Pattern.compile(
            "\\.\\s*entrySet\\s*\\(\\s*\\)"
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
    /**
     * A heap object per repair key: a String-keyed collection, a String or sink, or a copy
     * of a character sequence into one. The repair's key collection holds the pinned base
     * reader's symbol integers, so none of these has a reason to appear in it.
     */
    private static final Pattern HEAP_REPAIR_KEY = Pattern.compile(
            "\\b(?:CharSequence[A-Za-z]*(?:Set|Map|List)|String|StringSink)\\b"
                    + "|\\bChars\\s*\\.\\s*toString\\s*\\(|\\.\\s*toString\\s*\\("
    );
    /**
     * A heap byte array in Java-style form: the {@code byte} element type followed by an array
     * bracket or by varargs, annotations allowed between them. It matches whatever the array
     * is - a field, parameter, local or method type of any rank, a {@code new byte[n]}, a type
     * argument of any container, a cast or a class literal - and whatever it is called.
     * {@link #HEAP_BYTE_DECLARATION} finds the C-style form.
     */
    private static final Pattern HEAP_BYTE_ARRAY = Pattern.compile(
            "\\bbyte\\s*(?:@[A-Za-z_$][A-Za-z0-9_$.]*\\s*)*(?:\\[|\\.\\.\\.)"
    );
    /**
     * Every heap byte array the live-view package may declare or allocate, file by file:
     * each indented line is how many times one site appears, then the site as
     * {@link #findHeapByteSites} renders it. Each holds per-root or per-function metadata -
     * an identity, a key schema, a manifest - that a root, directory or compiled plan reads
     * or encodes once, or is a test-only copy; none holds a partition key or a state
     * payload. A site not listed, or listed fewer times than the source holds it, fails
     * {@link #testCheckpointKeysAndPayloadsAreNeverHeapArrays}, and so does a count the
     * source no longer reaches, so the list stays exactly what the package holds. A file in a
     * subpackage goes by its path below {@code io/questdb/cairo/lv}, such as
     * {@code sub/File.java}.
     */
    private static final String HEAP_BYTE_ARRAY_ALLOWLIST = """
            LiveViewAccumulatorDescriptor.java
                # The component identity, encoded once per compiled descriptor and compared
                # whole; the component's state image is native.
                1 byte[] a
                1 byte[] b
                1 byte[] borrowEncoded()
                1 byte[] encode()
                2 byte[] encoded
                1 byte[] getEncoded()
                1 new byte[]
            LiveViewCheckpointBinaryKeyIndex.java
                # The function-identity index of one publication, one entry per function.
                4 byte[] key
                1 byte[] slot
                1 byte[][] keys
                1 byte[][] oldKeys
                1 new byte[]
            LiveViewCheckpointByteArrayPool.java
                # The pool of decoded per-root metadata images.
                1 LiveViewCheckpointByteArrayPool
                1 ObjList<byte[]> arrays
                1 byte[] copy()
                1 byte[] next()
                1 byte[] out
                1 byte[] source
                2 byte[] value
                1 new byte[]
            LiveViewCheckpointFunctionDirectory.java
                # One function identity per function root the directory names.
                2 LiveViewCheckpointByteArrayPool
                2 ObjList<byte[]> identities
                3 byte[] identity
                1 byte[] previous
            LiveViewCheckpointFunctionIdentity.java
                # A compiled function's identity and key schema, encoded once.
                1 byte[] borrowEncoded()
                1 byte[] borrowEncodedKeySchema()
                1 byte[] encode()
                2 byte[] encoded
                1 byte[] encodedKeySchema
                1 byte[] getEncoded()
                1 byte[] sink
                1 new byte[]
            LiveViewCheckpointFunctionRoot.java
                # The identity and key schema a function root decodes once per root.
                2 LiveViewCheckpointByteArrayPool
                3 byte[] functionIdentity
                1 byte[] getFunctionIdentity()
                1 byte[] getKeySchema()
                3 byte[] keySchema
                2 new byte[]
            LiveViewCheckpointFunctionRootBuilder.java
                # The identity and key schema a function root is built under.
                5 byte[] functionIdentity
                5 byte[] keySchema
                2 new byte[]
            LiveViewCheckpointMetadata.java
                # The metadata codec's encoders and decoders for identities, key schemas
                # and names.
                1 LiveViewCheckpointByteArrayPool
                1 byte[] EMPTY_KEY_SCHEMA
                5 byte[] bytes
                1 byte[] encodeKeySchema()
                1 byte[] encodeUtf8()
                1 byte[] encoded
                1 byte[] left
                2 byte[] readBytes()
                1 byte[] right
                3 byte[] sink
                3 new byte[]
            LiveViewCheckpointPartitionMapEntry.java
                # The two @TestOnly heap copies of the entry's native key and scalar.
                2 byte[] copy
                1 byte[] copyKeyForTest()
                1 byte[] copyScalarStateForTest()
                2 new byte[]
            LiveViewCheckpointRootBuilder.java
                # One function identity per function root a checkpoint root names.
                2 LiveViewCheckpointByteArrayPool
                1 ObjList<byte[]> functionIdentities
                1 byte[] identity
            LiveViewCheckpointTimelineStoreReader.java
                # The compiled function identity a restore looks its root up by.
                1 byte[] identity
            LiveViewCheckpointTimelineStoreWriter.java
                # Borrowed compiled identities, key schemas and manifests a seal names its
                # function and window roots by, and looks predecessors up by.
                17 byte[] functionIdentity
                2 byte[] identity
                9 byte[] keySchema
                6 byte[] manifest
                1 byte[] resolvedIdentity
                6 byte[] windowIdentity
            LiveViewCheckpointWindowRoot.java
                # The identity, key schema and manifest a window root decodes once per root.
                2 LiveViewCheckpointByteArrayPool
                1 byte[] borrowWindowIdentity()
                1 byte[] getKeySchema()
                1 byte[] getManifest()
                1 byte[] getWindowIdentity()
                3 byte[] keySchema
                3 byte[] manifest
                3 byte[] windowIdentity
                3 new byte[]
            LiveViewCheckpointWindowRootBuilder.java
                # The identity, key schema and manifest a window root is built under.
                6 byte[] keySchema
                6 byte[] manifest
                6 byte[] windowIdentity
                3 new byte[]
            LiveViewWindow.java
                # The window's compiled key schema and name, encoded once.
                1 byte[] borrowCheckpointKeySchema()
                1 byte[] borrowCheckpointWindowNameUtf8()
                1 byte[] checkpointKeySchema
                1 byte[] checkpointWindowNameUtf8
            LiveViewWindowStateManifest.java
                # The compiled manifest, encoded once from its component identities.
                1 byte[] borrowEncoded()
                1 byte[] encode()
                2 byte[] encoded
                1 byte[] getEncoded()
                1 byte[] identity
                1 new byte[]
            LiveViewWindowStatePlan.java
                # The compiled window identity, encoded once and compared whole.
                1 byte[] borrowWindowIdentity()
                1 byte[] candidateWindowIdentity
                1 byte[] encodeWindowIdentity()
                1 byte[] encoded
                1 byte[] getWindowIdentity()
                1 byte[] other
                1 byte[] sink
                4 byte[] windowIdentity
                1 new byte[]
            """;
    /**
     * One array rank, {@code []}, annotations allowed: a further rank of an array type after
     * the first, or a C-style rank after a declared name or a method's parameters.
     */
    private static final Pattern HEAP_BYTE_ARRAY_RANK = Pattern.compile(
            "(?:\\s*@[A-Za-z_$][A-Za-z0-9_$.]*)*\\s*\\[\\s*\\]"
    );
    /**
     * Heap bytes that spell no {@code byte[]}: a boxed byte (but not a {@code Byte.}
     * constant or helper, nor a {@code Byte::} helper reference; {@code Byte::new} still
     * boxes), a buffer or stream over an array, a call that returns one, and the pool that
     * hands them out.
     */
    private static final Pattern HEAP_BYTE_CONTAINER = Pattern.compile(
            "\\bByte\\b(?!\\s*\\.|\\s*::(?!\\s*new\\b))"
                    + "|\\b(?:ByteBuffer|ByteArrayInputStream|ByteArrayOutputStream|LiveViewCheckpointByteArrayPool)\\b"
                    + "|\\.\\s*(?:getBytes|toByteArray)\\s*\\("
    );
    /**
     * A declaration of the scalar {@code byte} type; group 1 is its first name. A declarator
     * of it is an array only when C-style brackets follow its name, as in
     * {@code byte payload[]}, {@code byte flags, payload[]} or {@code byte payload()[]}.
     */
    private static final Pattern HEAP_BYTE_DECLARATION = Pattern.compile(
            "\\bbyte\\s+([A-Za-z_$][A-Za-z0-9_$]*)"
    );
    private static final Pattern METHOD_INVOCATION = Pattern.compile(
            "\\b([A-Za-z_$][A-Za-z0-9_$]*)\\s*\\("
    );
    private static final Pattern NEW_PATH_TO_STRING = Pattern.compile(
            "\\bnew\\s+(?:[A-Za-z_\\x24][A-Za-z0-9_\\x24]*\\.)*Path\\s*\\([^;]*?\\)\\s*\\.\\s*toString\\s*\\("
    );
    private static final Pattern PRIVATE_OR_STATIC = Pattern.compile("\\b(?:private|static)\\b");
    /**
     * Any construction of a class inside a publication entry point. A publication
     * runs on shells its owner retains, so the body of one must not build a store,
     * reader, writer, reference, list, result or path of its own.
     */
    private static final Pattern PUBLICATION_CONSTRUCTION = Pattern.compile(
            "\\bnew\\s+(?:[A-Z][A-Za-z0-9_$]*(?:\\.[A-Z][A-Za-z0-9_$]*)*"
                    + "|boolean|byte|char|double|float|int|long|short)\\s*[(<\\[]"
    );
    private static final Pattern RECURRING_CALLBACK_COMMIT = Pattern.compile(
            "\\b(?:[Ff]enced[A-Za-z0-9_$]*[Cc]ommit|[Cc]ommit[A-Za-z0-9_$]*)\\s*\\([^)]*"
                    + "\\b(?:Callable|Consumer|Function|Runnable|Supplier)\\b",
            Pattern.DOTALL
    );
    // The tokens a specialized commit fence must show. Each names an API the fence cannot drop
    // without losing the property it exists for. Two of them pin a name instead of an API, and
    // deliberately: the fence clears two distinct dirty flags, and nothing but the object each
    // one lives on tells them apart. JOB_WINDOW_STATE_CLEAR pins windowStateDirty, the job's own
    // field, and INSTANCE_PARAMETER pins the LiveViewInstance type so the check can read the
    // fence's instance reference out of its declaration. Every other identifier - the lock local,
    // the parameters, the engine field - the check reads out of the source rather than pinning.
    // assertSpecializedCommit() orders them.
    private static final Pattern CALLBACK_PARAMETER = Pattern.compile(
            "\\b(?:Callable|Consumer|Function|Runnable|Supplier)\\b"
    );
    private static final Pattern FINALLY_BLOCK = Pattern.compile("\\}\\s*finally\\s*\\{");
    private static final Pattern INSTANCE_PARAMETER = Pattern.compile(
            "\\b(?:[A-Za-z_$][A-Za-z0-9_$]*\\s*\\.\\s*)*LiveViewInstance\\s+([A-Za-z_$][A-Za-z0-9_$]*)\\b"
    );
    // The job's own field, so a bare or this-qualified assignment only. A clear reached through
    // any other receiver leaves this job's flag set.
    private static final Pattern JOB_WINDOW_STATE_CLEAR = Pattern.compile(
            "(?:\\bthis\\s*\\.\\s*|(?<![.\\w$]))windowStateDirty\\s*=\\s*false\\s*;"
    );
    private static final Pattern MINT_OBSERVER_CALL = Pattern.compile(
            "\\bfireRoleSwitchMintObserver\\s*\\(\\s*\\)\\s*;"
    );
    private static final Pattern ROLE_SWITCH_READ_LOCK = Pattern.compile(
            "(?:\\bfinal\\s+)?(?:[A-Za-z_$][A-Za-z0-9_$.]*\\s+)?([A-Za-z_$][A-Za-z0-9_$]*)\\s*=\\s*"
                    + "(?:[A-Za-z_$][A-Za-z0-9_$.]*\\s*\\.\\s*)?getRoleSwitchReadLock\\s*\\(\\s*\\)\\s*;"
    );
    private static final Pattern TRY_BLOCK = Pattern.compile("\\btry\\s*\\{");
    private static final Pattern STATIC_FINAL = Pattern.compile("\\bstatic\\s+final\\b");
    private static final Pattern TYPE_DECLARATION = Pattern.compile(
            "\\b(?:class|enum|interface|record)\\s+[A-Za-z_$][A-Za-z0-9_$]*[^;{]*\\{"
    );
    private static final String FENCE_NAME = "commitLiveViewFenced";
    private static final String FENCE_SIGNATURE =
            "private void " + FENCE_NAME + "(LiveViewInstance instance, WalWriter walWriter, long seqTxn)";

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
    public void testRepairKeyCollectionHoldsNoHeapKeys() throws IOException {
        // The change-set decomposition runs on every out-of-order repair and collects up to
        // the per-segment key budget for each of up to 64 segments. It used to copy each key
        // off the WAL's flyweight into a String; it now keeps the base reader's symbol
        // integers in native lists, and the segment loop carries them as ints.
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        final String[] files = {
                "io/questdb/cairo/lv/LiveViewCheckpointSegmentChangeSet.java",
                "io/questdb/cairo/lv/LiveViewCheckpointSegmentLoop.java"
        };
        for (int i = 0; i < files.length; i++) {
            final Path file = sourceRoot.resolve(files[i]);
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            final Matcher matcher = HEAP_REPAIR_KEY.matcher(code);
            while (matcher.find()) {
                addViolation(sourceRoot, file, code, matcher.start(), matcher.group(), violations);
            }
            findForbiddenTypes(sourceRoot, file, code, violations);
            findBoxedCollectionTypes(sourceRoot, file, code, violations);
        }
        Assert.assertTrue(
                "repair key collection heap-key violations:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testCheckpointKeysAndPayloadsAreNeverHeapArrays() throws IOException {
        // A partition key or a state payload used to be a byte[] at every hop: frozen per key
        // per seal, re-copied into holders, shared by reference into a parked capture's key
        // domain, pooled per width on the worker. Every key and payload is now native - an
        // (address, length) pair for the duration of a call, or a handle into an arena its
        // owner frees - so the package's heap byte arrays are exactly the metadata and
        // test-only copies HEAP_BYTE_ARRAY_ALLOWLIST lists. The rule scans every file under
        // io/questdb/cairo/lv, subpackages included, and matches the type as the source spells
        // it, not the name. A C-style declarator and each further declarator of a list count
        // like any other holder, a container of arrays counts whatever the container is, and a
        // second holder under an allowlisted name fails on the count. Only a var bound to a
        // call that returns an array spells no type, so the scan cannot see that holder; an
        // allocation inside the package still counts where it happens.
        final Path sourceRoot = findSourceRoot();
        final Path packageRoot = sourceRoot.resolve("io/questdb/cairo/lv");
        final Map<String, Integer> allowed = parseHeapByteArrayAllowlist();
        final Map<String, List<String>> found = new TreeMap<>();
        final List<Path> files;
        try (Stream<Path> tree = Files.walk(packageRoot)) {
            files = tree.filter(file -> Files.isRegularFile(file) && file.toString().endsWith(".java"))
                    .sorted()
                    .toList();
        }
        Assert.assertTrue("the live-view package must be scanned", files.size() > 100);
        for (int i = 0, n = files.size(); i < n; i++) {
            final Path file = files.get(i);
            final String fileName = packageRoot.relativize(file).toString().replace(File.separatorChar, '/');
            final String code = stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8));
            final List<String> sites = new ArrayList<>();
            final List<Integer> offsets = new ArrayList<>();
            findHeapByteSites(code, sites, offsets);
            for (int j = 0, m = sites.size(); j < m; j++) {
                final List<String> located = new ArrayList<>();
                addViolation(sourceRoot, file, code, offsets.get(j), sites.get(j), located);
                found.computeIfAbsent(fileName + ": " + sites.get(j), k -> new ArrayList<>()).addAll(located);
            }
        }
        final List<String> violations = new ArrayList<>();
        for (Map.Entry<String, List<String>> site : found.entrySet()) {
            final int allowedCount = allowed.getOrDefault(site.getKey(), 0);
            if (site.getValue().size() > allowedCount) {
                for (String location : site.getValue()) {
                    violations.add(location + " (" + site.getValue().size() + " found, " + allowedCount + " allowed)");
                }
            }
        }
        for (Map.Entry<String, Integer> site : allowed.entrySet()) {
            final List<String> locations = found.get(site.getKey());
            final int foundCount = locations == null ? 0 : locations.size();
            if (foundCount < site.getValue()) {
                violations.add("allowlisted more often than the source holds it: " + site.getKey()
                        + " (" + foundCount + " found, " + site.getValue() + " allowed)");
            }
        }
        Assert.assertTrue(
                "live-view heap byte sites differ from HEAP_BYTE_ARRAY_ALLOWLIST. Partition keys and state"
                        + " payloads stay native; only per-root or per-function metadata, such as an identity,"
                        + " a key schema or a manifest, may be allowlisted, under a line that justifies it."
                        + " An over-count lists every location of its site, the allowlisted ones included:"
                        + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
    }

    @Test
    public void testHeapByteArrayScannerSelfCoverage() {
        // Every shape the old name-matching rule let through.
        assertHeapByteSites("private final ObjList<byte[]> payloads = new ObjList<>();", "ObjList<byte[]> payloads");
        assertHeapByteSites("final ObjList<byte[]> images = memberImages.getQuick(m);", "ObjList<byte[]> images");
        assertHeapByteSites("private final ObjList<ObjList<byte[]>> completeMemberImages;", "ObjList<ObjList<byte[]>> completeMemberImages");
        assertHeapByteSites("final byte[] keyBytes = copy(address, length);", "byte[] keyBytes");
        assertHeapByteSites("void put(byte[] k) { }", "byte[] k");
        assertHeapByteSites("void restore(byte @NotNull [] payload, byte[] scalarState) { }", "byte[] payload", "byte[] scalarState");
        // Other containers, and arrays that name nothing.
        assertHeapByteSites("private final ObjObjHashMap<CharSequence, byte[]> payloadsByKey;", "ObjObjHashMap<CharSequence,byte[]> payloadsByKey");
        assertHeapByteSites("private final ArrayList < byte [ ] > sink = new ArrayList<>();", "ArrayList<byte[]> sink");
        assertHeapByteSites("ObjList<byte[]> images() { return null; }", "ObjList<byte[]> images()");
        assertHeapByteSites("private byte[][] keys;", "byte[][] keys");
        assertHeapByteSites("private byte @NotNull [] @Nullable [] keys;", "byte[][] keys");
        assertHeapByteSites("void of(byte... parts) { }", "byte... parts");
        assertHeapByteSites("var image = new byte[length];", "new byte[]");
        assertHeapByteSites("final Object image = (byte[]) holder;", "(byte[])");
        assertHeapByteSites("final IntFunction<byte[]> factory = byte[]::new;", "IntFunction<byte[]> factory", "byte[]::new");
        assertHeapByteSites("register(byte[].class);", "byte[].class");
        assertHeapByteSites("byte[] getKey() { return null; }", "byte[] getKey()");
        assertHeapByteSites("private final ObjList<Byte> boxed = new ObjList<>();", "Byte");
        assertHeapByteSites("private ByteBuffer buffer;", "ByteBuffer");
        assertHeapByteSites("final var copy = name.toString().getBytes(StandardCharsets.UTF_8);", ".getBytes(");
        assertHeapByteSites("private final LiveViewCheckpointByteArrayPool pool = null;", "LiveViewCheckpointByteArrayPool");
        assertHeapByteSites("final Function<String, Byte> parse = Byte::new;", "Byte", "Byte");
        // C-style brackets after a name, on a field, a parameter, a later declarator or a method.
        assertHeapByteSites("private byte transplantPayloads[];", "byte[] transplantPayloads");
        assertHeapByteSites("void put(byte k[]) { }", "byte[] k");
        assertHeapByteSites("private byte keys @NotNull [] [];", "byte[][] keys");
        assertHeapByteSites("private byte flags, payload[] = null;", "byte[] payload");
        assertHeapByteSites("byte payload()[] { return null; }", "byte[] payload()");
        // Every further declarator of a list, past any initializer, and C-style ranks on top.
        assertHeapByteSites("private byte[] keySchema, frozenPayload;", "byte[] keySchema", "byte[] frozenPayload");
        assertHeapByteSites(
                "private byte[] keySchema = encode(a, b), images[] = {{1}, {2}}, k;",
                "byte[] keySchema", "byte[][] images", "byte[] k"
        );
        assertHeapByteSites(
                "private final ObjList<byte[]> identities = new ObjList<>(), payloads = new ObjList<>();",
                "ObjList<byte[]> identities", "ObjList<byte[]> payloads"
        );
        assertHeapByteSites(
                "private ObjObjHashMap<CharSequence, byte[]> names"
                        + " = new ObjObjHashMap<CharSequence, byte[]>(), payloads;",
                "ObjObjHashMap<CharSequence,byte[]> names", "ObjObjHashMap<CharSequence,byte[]>",
                "ObjObjHashMap<CharSequence,byte[]> payloads"
        );
        assertHeapByteSites("private byte[] a = n < 0 ? x : y, b;", "byte[] a", "byte[] b");
        assertHeapByteSites("void f() { for (byte[] a = x, b = y; k < 2; k++) { } }", "byte[] a", "byte[] b");

        assertHeapByteSites("// byte[] key\nlong keyHandle;");
        assertHeapByteSites("final String s = \"byte[] key\";");
        assertHeapByteSites("private final LongList keys = new LongList(); int byteCount = Byte.BYTES; long bytes;");
        assertHeapByteSites("final IntUnaryOperator unsigned = Byte::toUnsignedInt;");
        // A scalar byte, and the parameters after an array, which are not further declarators.
        assertHeapByteSites("byte b = (byte) x[0], c = values[1]; for (byte v : bytes) { }");
        assertHeapByteSites("void f(byte[] a, int b, byte c, Foo d[]) { }", "byte[] a");
        assertHeapByteSites("void f(byte a, byte... b) { }", "byte... b");
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
            findEntrySetIteration(sourceRoot, file, code, violations);
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
                    ? new String[]{"restoreFunctions", "validateFunction", "validateFunctions",
                    "validateWindowStateShape"}
                    : new String[]{"buildRoot", "freezeBoundary", "freezeWindowState"};
            findCompiledEncodingViolationsInMethods(sourceRoot, file, code, methodNames, violations);
        }
        final String[] builderFiles = {
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
        assertSpecializedCommit(file, code, "commitLiveViewFenced", "commitLiveView");
        assertSpecializedCommit(file, code, "commitLiveViewWithoutDedupFenced", "commitLiveViewWithoutDedup");
        assertSpecializedCommit(file, code, "commitLiveViewWithUpsertFenced", "commitLiveViewWithUpsert");
        assertSpecializedCommit(file, code, "commitLiveViewWithReplaceRangeFenced", "commitLiveViewWithReplaceRange");
        Assert.assertFalse(code.contains("fencedLiveViewCommit"));
    }

    @Test
    public void testSpecializedCommitFenceSelfCoverage() {
        final String fencedBody = """
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """;
        assertSpecializedCommitAccepted(specializedCommitSource(fencedBody));
        // A behaviour-preserving rename of every identifier the fence touches - declaration and
        // body together - plus a this-qualified field clear, still describes the same fence.
        assertSpecializedCommitAccepted(specializedCommitSource(
                "private void " + FENCE_NAME + "(LiveViewInstance view, WalWriter writer, long txn)",
                """
                        final Lock roleSwitchReadLock = this.cairoEngine.getRoleSwitchReadLock();
                        roleSwitchReadLock.lock();
                        try {
                            this.cairoEngine.fireRoleSwitchMintObserver();
                            writer.commitLiveView(txn);
                            this.windowStateDirty = false;
                            view.setWindowStateDirty(false);
                        } finally {
                            roleSwitchReadLock.unlock();
                        }
                        """,
                "private void drain() { " + FENCE_NAME + "(view, writer, txn); }"
        ));
        // A third legitimate caller is an ordinary refactor, not a broken fence.
        assertSpecializedCommitAccepted(specializedCommitSource(
                FENCE_SIGNATURE,
                fencedBody,
                """
                        private void drain() { commitLiveViewFenced(instance, walWriter, seqTxn); }
                        private void flush() { commitLiveViewFenced(instance, walWriter, seqTxn); }
                        private void sweep() { commitLiveViewFenced(instance, walWriter, seqTxn); }
                        """
        ));

        // The commit itself escapes the read lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                walWriter.commitLiveView(seqTxn);
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """));
        // The fence clears the job's dirty flag after it releases the lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                windowStateDirty = false;
                """));
        // The fence clears the instance's dirty flag after it releases the lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                } finally {
                    lock.unlock();
                }
                instance.setWindowStateDirty(false);
                """));
        // The release is not in a finally, so a throwing commit leaks the lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                    lock.unlock();
                }
                """));
        // The mint observer no longer fires under the lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """));
        // A second, unfenced commit path opens elsewhere in the class.
        assertSpecializedCommitRejected(specializedCommitSource(
                FENCE_SIGNATURE,
                fencedBody,
                """
                        private void drain() { commitLiveViewFenced(instance, walWriter, seqTxn); }
                        private void shortcut() { walWriter.commitLiveView(seqTxn); }
                        """
        ));
        // The fence is orphaned: nothing routes a commit through it any more.
        assertSpecializedCommitRejected(specializedCommitSource(
                FENCE_SIGNATURE,
                fencedBody,
                "private void drain() { }"
        ));
        // The fence takes a callback, so its caller decides what runs under the lock.
        assertSpecializedCommitRejected(specializedCommitSource(
                "private void commitLiveViewFenced(LiveViewInstance instance, WalWriter walWriter, "
                        + "long seqTxn, Runnable after)",
                fencedBody,
                "private void drain() { commitLiveViewFenced(instance, walWriter, seqTxn, after); }"
        ));
        // The fence clears the instance dirty flag on the job's own cached field rather than on
        // the instance it was handed, which is a different object whenever the two disagree, so
        // the committing instance still believes it must rebuild rows that are already durable.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    this.instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """));
        // The fence clears a dirty flag the instance carries instead of the job's own, so the
        // job's stays set.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    instance.windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """));
        // The release sits below the finally block rather than inside it, so a throwing commit
        // leaks the read lock.
        assertSpecializedCommitRejected(specializedCommitSource("""
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    engine.getConfiguration();
                }
                lock.unlock();
                """));
        // A second commit opens inside the fence itself, above the acquisition.
        assertSpecializedCommitRejected(specializedCommitSource("""
                walWriter.commitLiveView(seqTxn);
                final Lock lock = engine.getRoleSwitchReadLock();
                lock.lock();
                try {
                    engine.fireRoleSwitchMintObserver();
                    walWriter.commitLiveView(seqTxn);
                    windowStateDirty = false;
                    instance.setWindowStateDirty(false);
                } finally {
                    lock.unlock();
                }
                """));
    }

    @Test
    public void testPublicationEntryPointsConstructNothing() throws IOException {
        final Path sourceRoot = findSourceRoot();
        final List<String> violations = new ArrayList<>();
        assertMethodsConstructNothing(
                sourceRoot,
                sourceRoot.resolve("io/questdb/cairo/lv/LiveViewCheckpointTimelineStoreWriter.java"),
                new String[]{
                        "append0",
                        "ensureDirectories",
                        "persistRetirementQueue",
                        "publishCompaction",
                        "publishRepair",
                        "publishTruncate",
                        "skipPublishedSegmentIds",
                        "sweep"
                },
                violations
        );
        assertMethodsConstructNothing(
                sourceRoot,
                sourceRoot.resolve("io/questdb/cairo/lv/LiveViewCheckpointCompaction.java"),
                new String[]{"compact", "nextFreeSegmentId"},
                violations
        );
        Assert.assertTrue(
                "publication entry point construction:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations),
                violations.isEmpty()
        );
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

        assertEntrySetDetected("for (Map.Entry<CharSequence, V> e : map.entrySet()) { }");
        assertEntrySetDetected("map . entrySet ( ) ;");
        assertNoEntrySetDetected("for (V v : map.values()) { }");
        assertPublicationConstructionDetected("void publish() { final Path p = new Path(); }", "publish");
        assertPublicationConstructionDetected("void publish() { return new Result(1); }", "publish");
        assertPublicationConstructionDetected("void publish() { final int[] a = new int[4]; }", "publish");
        assertPublicationConstructionDetected("void publish() { final ObjList<V> l = new ObjList<>(); }", "publish");
        assertNoPublicationConstructionDetected("void publish() { shells.path.of(dir); }", "publish");
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

    private static void assertEntrySetDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findEntrySetIteration(
                Paths.get("."),
                Paths.get("./Scanner.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertFalse("expected an entrySet violation for: " + source, violations.isEmpty());
    }

    private static void assertNoEntrySetDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findEntrySetIteration(
                Paths.get("."),
                Paths.get("./Scanner.java"),
                stripCommentsAndLiterals(source),
                violations
        );
        Assert.assertTrue("unexpected entrySet violation for: " + source, violations.isEmpty());
    }

    private static void assertPublicationConstructionDetected(String source, String methodName) {
        final List<String> violations = new ArrayList<>();
        findPublicationConstruction(
                Paths.get("."),
                Paths.get("./Scanner.java"),
                stripCommentsAndLiterals("class C { " + source + " }"),
                new String[]{methodName},
                violations
        );
        Assert.assertFalse("expected a construction violation for: " + source, violations.isEmpty());
    }

    private static void assertNoPublicationConstructionDetected(String source, String methodName) {
        final List<String> violations = new ArrayList<>();
        findPublicationConstruction(
                Paths.get("."),
                Paths.get("./Scanner.java"),
                stripCommentsAndLiterals("class C { " + source + " }"),
                new String[]{methodName},
                violations
        );
        Assert.assertTrue("unexpected construction violation for: " + source, violations.isEmpty());
    }

    private static void assertMethodsConstructNothing(
            Path sourceRoot,
            Path file,
            String[] methodNames,
            List<String> violations
    ) throws IOException {
        findPublicationConstruction(
                sourceRoot,
                file,
                stripCommentsAndLiterals(Files.readString(file, StandardCharsets.UTF_8)),
                methodNames,
                violations
        );
    }

    private static void findEntrySetIteration(
            Path sourceRoot,
            Path file,
            String code,
            List<String> violations
    ) {
        final Matcher matcher = ENTRY_SET.matcher(code);
        while (matcher.find()) {
            addViolation(
                    sourceRoot,
                    file,
                    code,
                    matcher.start(),
                    "Map.Entry iteration allocates one wrapper per entry",
                    violations
            );
        }
    }

    private static void findPublicationConstruction(
            Path sourceRoot,
            Path file,
            String code,
            String[] methodNames,
            List<String> violations
    ) {
        final List<MethodRegion> methods = findMethodRegions(code, file);
        for (int i = 0; i < methodNames.length; i++) {
            final String methodName = methodNames[i];
            int methodCount = 0;
            for (int j = 0, n = methods.size(); j < n; j++) {
                final MethodRegion method = methods.get(j);
                if (!methodName.equals(method.name)) {
                    continue;
                }
                methodCount++;
                final Matcher construction = PUBLICATION_CONSTRUCTION.matcher(code)
                        .region(method.openBrace, method.closeBrace + 1);
                while (construction.find()) {
                    addViolation(
                            sourceRoot,
                            file,
                            code,
                            construction.start(),
                            "construction inside publication entry point " + methodName,
                            violations
                    );
                }
            }
            Assert.assertTrue("missing scanned method " + methodName + " in " + file, methodCount > 0);
        }
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

    private static void assertHeapByteSites(String source, String... expectedSites) {
        final List<String> sites = new ArrayList<>();
        findHeapByteSites(stripCommentsAndLiterals("class C { " + source + " }"), sites, new ArrayList<>());
        Assert.assertEquals("heap byte sites of: " + source, List.of(expectedSites), sites);
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

    private static void assertSpecializedCommitAccepted(String source) {
        assertSpecializedCommit(
                Path.of("source/Snippet.java"),
                stripCommentsAndLiterals(source),
                FENCE_NAME,
                "commitLiveView"
        );
    }

    private static void assertSpecializedCommitRejected(String source) {
        boolean isRejected = false;
        try {
            assertSpecializedCommit(
                    Path.of("source/Snippet.java"),
                    stripCommentsAndLiterals(source),
                    FENCE_NAME,
                    "commitLiveView"
            );
        } catch (AssertionError expected) {
            isRejected = true;
        }
        Assert.assertTrue("expected specialized commit violation for: " + source, isRejected);
    }

    private static String specializedCommitSource(String fenceBody) {
        return specializedCommitSource(
                FENCE_SIGNATURE,
                fenceBody,
                "private void drain() { " + FENCE_NAME + "(instance, walWriter, seqTxn); }"
        );
    }

    private static String specializedCommitSource(String signature, String fenceBody, String extraMembers) {
        return "class Job {" + System.lineSeparator()
                + signature + " {" + System.lineSeparator()
                + fenceBody
                + "}" + System.lineSeparator()
                + extraMembers + System.lineSeparator()
                + "}" + System.lineSeparator();
    }

    private static void assertArrayCopyDetected(String source) {
        final List<String> violations = new ArrayList<>();
        findArrayCopies(Path.of("source"), Path.of("source/Snippet.java"), source, violations);
        Assert.assertFalse("expected byte-array copy violation for: " + source, violations.isEmpty());
    }

    /**
     * Pins the shape of one specialized commit fence: the job reads the role-switch read
     * lock into a local, takes it, fires the mint observer, commits through
     * {@code writerCommitMethod}, clears both window-state dirty flags while it still holds
     * the lock, and releases the lock from inside a {@code finally}.
     * <p>
     * The check matches token patterns rather than verbatim statements, so renaming the
     * local, the parameters or the engine field, or qualifying a field with {@code this.},
     * leaves it green. Both dirty-flag clears keep their receiver, though: the job's flag
     * takes a bare or {@code this.}-qualified assignment, and the instance's takes the very
     * reference the declaration binds the {@code LiveViewInstance} to. A fence that clears
     * either flag on some other object leaves the flag it was handed set.
     * <p>
     * It counts no call sites. In their place it asserts the two properties a call site can
     * actually break: every {@code writerCommitMethod} call in the file sits inside this
     * fence, and inside its lock - no commit escapes the role-switch read lock - and at
     * least one caller still reaches the fence, so an orphaned fence still shows up.
     */
    private static void assertSpecializedCommit(
            Path file,
            String code,
            String methodName,
            String writerCommitMethod
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
        final int declarationStart = findDeclarationStart(code, found.openBrace);
        final String declaration = code.substring(declarationStart, found.openBrace);
        Assert.assertFalse("commit fence must not accept a callback: " + declaration,
                CALLBACK_PARAMETER.matcher(declaration).find());

        final Matcher instanceParameter = INSTANCE_PARAMETER.matcher(declaration);
        Assert.assertTrue(
                "commit fence " + methodName + " must take the live-view instance it clears: " + declaration,
                instanceParameter.find()
        );
        // The instance clear names this reference and no other. Anything else clears a flag on
        // an object the fence was not handed, and the instance it was handed stays dirty.
        final Pattern instanceWindowStateClear = Pattern.compile(
                "(?<![.\\w$])" + Pattern.quote(instanceParameter.group(1))
                        + "\\s*\\.\\s*setWindowStateDirty\\s*\\(\\s*false\\s*\\)\\s*;"
        );

        final String body = singleLine(code.substring(found.openBrace, found.closeBrace + 1));
        final Matcher acquisition = ROLE_SWITCH_READ_LOCK.matcher(body);
        Assert.assertTrue(
                "commit fence " + methodName + " must read the role-switch read lock into a local: " + body,
                acquisition.find()
        );
        final String lockName = acquisition.group(1);
        final Pattern lockCall = Pattern.compile(
                "\\b" + Pattern.quote(lockName) + "\\s*\\.\\s*lock\\s*\\(\\s*\\)\\s*;"
        );
        final Pattern unlockCall = Pattern.compile(
                "\\b" + Pattern.quote(lockName) + "\\s*\\.\\s*unlock\\s*\\(\\s*\\)\\s*;"
        );
        final Pattern writerCall = Pattern.compile("\\.\\s*" + Pattern.quote(writerCommitMethod) + "\\s*\\(");

        final int locked = matchAfter(body, lockCall, acquisition.end(), methodName).end();
        int offset = matchAfter(body, TRY_BLOCK, locked, methodName).end();
        offset = matchAfter(body, MINT_OBSERVER_CALL, offset, methodName).end();
        final int committed = matchAfter(body, writerCall, offset, methodName).end();
        final Matcher release = matchAfter(body, FINALLY_BLOCK, committed, methodName);
        final int released = release.start();
        // The release sits inside that finally, not merely after it: an unlock below the whole
        // block leaks the lock on the throwing commit the block exists to cover.
        final int releaseBlockEnd = findBlockEnd(body, release.end() - 1, methodName);
        final Matcher unlock = matchAfter(body, unlockCall, released, methodName);
        Assert.assertTrue(
                "commit fence " + methodName + " releases the role-switch read lock outside its finally"
                        + " block, so a throwing commit leaks it: " + body,
                unlock.end() <= releaseBlockEnd
        );

        // Both dirty flags fall between the commit and the release. A clear that escapes the
        // lock lets a role switch observe durable rows the job still believes it must rebuild.
        assertWithin(body, JOB_WINDOW_STATE_CLEAR, committed, released, methodName);
        assertWithin(body, instanceWindowStateClear, committed, released, methodName);

        // Every commit the fence itself makes runs under the lock. Sitting inside the method is
        // not enough: a call above the acquisition or below the release commits unfenced from
        // within the fence.
        final Matcher fencedCommit = writerCall.matcher(body);
        while (fencedCommit.find()) {
            Assert.assertTrue(
                    "commit fence " + methodName + " calls " + writerCommitMethod
                            + " outside the role-switch read lock: " + body,
                    fencedCommit.start() >= locked && fencedCommit.end() <= released
            );
        }

        // No unfenced commit path: every writer commit in the file lives inside this fence.
        final Matcher writerInvocation = writerCall.matcher(code);
        while (writerInvocation.find()) {
            Assert.assertTrue(
                    "unfenced " + writerCommitMethod + " call at offset " + writerInvocation.start()
                            + " in " + file + ": every commit must run inside " + methodName,
                    writerInvocation.start() > found.openBrace && writerInvocation.end() <= found.closeBrace
            );
        }

        // The fence stays reachable. Any number of call sites is legitimate; none is not.
        int callSiteCount = 0;
        final Matcher invocation = Pattern.compile("\\b" + Pattern.quote(methodName) + "\\s*\\(").matcher(code);
        while (invocation.find()) {
            if (invocation.start() >= declarationStart && invocation.start() < found.openBrace) {
                continue;
            }
            callSiteCount++;
        }
        Assert.assertTrue(
                "specialized commit fence " + methodName + " in " + file + " has no call site",
                callSiteCount > 0
        );
    }

    /**
     * Returns the offset of the brace that closes the block opening at {@code openBrace}. The
     * caller has already stripped comments and literals, so counting braces is exact here.
     */
    private static int findBlockEnd(String body, int openBrace, String methodName) {
        int depth = 0;
        for (int i = openBrace, n = body.length(); i < n; i++) {
            final char c = body.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        Assert.fail("unbalanced block in " + methodName + ": " + body);
        return -1;
    }

    /**
     * Asserts the first match at or after {@code offset} and hands the matcher back, so the
     * caller can chain the next fragment off its end and pin the order without pinning text.
     */
    private static Matcher matchAfter(String body, Pattern pattern, int offset, String methodName) {
        final Matcher matcher = pattern.matcher(body);
        Assert.assertTrue(
                "missing or out-of-order fragment /" + pattern.pattern() + "/ in " + methodName + ": " + body,
                matcher.find(offset)
        );
        return matcher;
    }

    /**
     * Asserts the fragment appears in {@code [from, to)} - the window this check uses for the
     * region the fence still holds the lock over.
     */
    private static void assertWithin(String body, Pattern pattern, int from, int to, String methodName) {
        final Matcher matcher = pattern.matcher(body);
        Assert.assertTrue(
                "missing fragment /" + pattern.pattern() + "/ in " + methodName + ": " + body,
                matcher.find(from)
        );
        Assert.assertTrue(
                "fragment /" + pattern.pattern() + "/ escapes the role-switch read lock in "
                        + methodName + ": " + body,
                matcher.end() <= to
        );
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

    /**
     * Collects every heap byte site in {@code code}, in source order, with its offset. An
     * array renders as what it is: {@code byte[] name} for a field, parameter or local, one
     * site per declarator of a list, {@code byte[] name()} for a method,
     * {@code Container<byte[]> name} for a container of arrays, {@code new byte[]}, a cast, a
     * class literal or a constructor reference. A C-style declarator renders as its
     * Java-style twin, so {@code byte payload[]} is {@code byte[] payload}. A
     * {@link #HEAP_BYTE_CONTAINER} match renders as the matched word or call.
     */
    private static void findHeapByteSites(String code, List<String> sites, List<Integer> offsets) {
        final TreeMap<Integer, String> ordered = new TreeMap<>();
        final Matcher arrayMatcher = HEAP_BYTE_ARRAY.matcher(code);
        while (arrayMatcher.find()) {
            putHeapByteArraySites(code, arrayMatcher.start(), arrayMatcher.end(), ordered);
        }
        final Matcher declarationMatcher = HEAP_BYTE_DECLARATION.matcher(code);
        while (declarationMatcher.find()) {
            putDeclaredSites(code, "byte", false, declarationMatcher.start(), declarationMatcher.start(1), ordered);
        }
        final Matcher containerMatcher = HEAP_BYTE_CONTAINER.matcher(code);
        while (containerMatcher.find()) {
            ordered.put(containerMatcher.start(), containerMatcher.group().replaceAll("\\s+", ""));
        }
        for (Map.Entry<Integer, String> site : ordered.entrySet()) {
            offsets.add(site.getKey());
            sites.add(site.getValue());
        }
    }

    /**
     * Puts the site of the Java-style heap byte array whose element type and first bracket
     * span {@code [start, end)}, and a site for each further declarator of its declaration.
     */
    private static void putHeapByteArraySites(String code, int start, int end, TreeMap<Integer, String> ordered) {
        int before = start - 1;
        while (before > -1 && Character.isWhitespace(code.charAt(before))) {
            before--;
        }
        if (before > 1 && startsWithWord(code, before - 2, "new")
                && (before < 3 || !Character.isJavaIdentifierPart(code.charAt(before - 3)))) {
            ordered.put(start, "new byte[]");
            return;
        }
        final StringBuilder type = new StringBuilder("byte");
        int offset = end;
        if (code.charAt(end - 1) == '.') {
            type.append("...");
        } else {
            offset = skipWhitespace(code, offset);
            if (offset == code.length() || code.charAt(offset) != ']') {
                ordered.put(start, "byte[");
                return;
            }
            type.append("[]");
            offset = skipArrayRanks(code, offset + 1, type);
        }
        offset = skipWhitespace(code, offset);
        if (code.startsWith(".class", offset)) {
            ordered.put(start, type + ".class");
        } else if (code.startsWith("::", offset)) {
            ordered.put(start, type + "::new");
        } else if (offset < code.length() && code.charAt(offset) == ')') {
            ordered.put(start, "(" + type + ")");
        } else if (offset < code.length() && (code.charAt(offset) == '>' || code.charAt(offset) == ',')) {
            // A type argument: render the outermost generic type around it, then its names.
            int genericStart = -1;
            int depth = 0;
            for (int i = start - 1; i > -1; i--) {
                final char c = code.charAt(i);
                if (c == '>') {
                    depth++;
                } else if (c == '<') {
                    if (depth == 0) {
                        genericStart = i;
                    } else {
                        depth--;
                    }
                } else if (depth == 0 && (c == ';' || c == '{' || c == '}' || c == '(' || c == '=')) {
                    break;
                }
            }
            final int genericEnd = genericStart < 0 ? -1 : findGenericEnd(code, genericStart);
            if (genericEnd < 0) {
                ordered.put(start, type.toString());
                return;
            }
            int nameEnd = genericStart;
            while (nameEnd > 0 && Character.isWhitespace(code.charAt(nameEnd - 1))) {
                nameEnd--;
            }
            int nameStart = nameEnd;
            while (nameStart > 0 && Character.isJavaIdentifierPart(code.charAt(nameStart - 1))) {
                nameStart--;
            }
            final String container = code.substring(nameStart, genericEnd + 1).replaceAll("\\s+", "");
            putDeclaredSites(code, container, true, start, genericEnd + 1, ordered);
        } else {
            putDeclaredSites(code, type.toString(), true, start, offset, ordered);
        }
    }

    /**
     * Puts the sites of a declaration of {@code type} whose first name follows
     * {@code offset}: {@code type name()} for a method, and {@code type name} for each
     * declarator of a field, parameter or local, with the C-style brackets that follow the
     * name, or the method's parameters, added to the type. The first site sits at
     * {@code siteStart}, each further declarator at its name. For the scalar {@code byte}
     * only a name its brackets make an array puts a site; an array type with no name puts the
     * bare type.
     */
    private static void putDeclaredSites(
            String code,
            String type,
            boolean isArrayType,
            int siteStart,
            int offset,
            TreeMap<Integer, String> ordered
    ) {
        int nameStart = skipWhitespace(code, offset);
        int nameEnd = identifierEnd(code, nameStart);
        if (nameEnd == nameStart) {
            if (isArrayType) {
                ordered.put(siteStart, type);
            }
            return;
        }
        final StringBuilder declaredType = new StringBuilder(type);
        final int next = skipWhitespace(code, nameEnd);
        if (next < code.length() && code.charAt(next) == '(') {
            final int parametersEnd = findMatchingDelimiter(code, next, '(', ')');
            if (parametersEnd > -1) {
                skipArrayRanks(code, parametersEnd + 1, declaredType);
            }
            if (isArrayType || declaredType.length() > type.length()) {
                ordered.put(siteStart, declaredType + " " + code.substring(nameStart, nameEnd) + "()");
            }
            return;
        }
        int site = siteStart;
        while (true) {
            declaredType.setLength(type.length());
            int i = skipWhitespace(code, skipArrayRanks(code, nameEnd, declaredType));
            if (isArrayType || declaredType.length() > type.length()) {
                ordered.put(site, declaredType + " " + code.substring(nameStart, nameEnd));
            }
            if (i < code.length() && code.charAt(i) == '=') {
                i = skipInitializer(code, i + 1);
            }
            if (i == code.length() || code.charAt(i) != ',') {
                return;
            }
            // A further declarator is a name that ends the list, starts an initializer or
            // precedes another declarator. A name followed by anything else is the type of the
            // next parameter.
            nameStart = skipWhitespace(code, i + 1);
            nameEnd = identifierEnd(code, nameStart);
            final int after = skipWhitespace(code, skipArrayRanks(code, nameEnd, null));
            if (nameEnd == nameStart || after == code.length() || ",;=".indexOf(code.charAt(after)) < 0) {
                return;
            }
            site = nameStart;
        }
    }

    /**
     * @return the offset past the array ranks that start at {@code offset}, each of which
     * {@code type}, unless null, gains as {@code []}
     */
    private static int skipArrayRanks(String code, int offset, StringBuilder type) {
        final Matcher rank = HEAP_BYTE_ARRAY_RANK.matcher(code);
        int result = offset;
        while (true) {
            rank.region(result, code.length());
            if (!rank.lookingAt()) {
                return result;
            }
            result = rank.end();
            if (type != null) {
                type.append("[]");
            }
        }
    }

    /**
     * @return the offset of the comma, semicolon or unmatched closing bracket that ends the
     * initializer starting at {@code offset}, or the length of {@code code}
     */
    private static int skipInitializer(String code, int offset) {
        int depth = 0;
        for (int i = offset, n = code.length(); i < n; i++) {
            final char c = code.charAt(i);
            if (c == '(' || c == '[' || c == '{') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}') {
                if (depth-- == 0) {
                    return i;
                }
            } else if (c == '<') {
                // Type arguments, as in new ObjObjHashMap<CharSequence, byte[]>(), hold commas
                // of their own. A comparison or a shift is not one.
                final int typeArgumentsEnd = findTypeArgumentsEnd(code, i);
                if (typeArgumentsEnd > -1) {
                    i = typeArgumentsEnd;
                }
            } else if (depth == 0 && (c == ',' || c == ';')) {
                return i;
            }
        }
        return code.length();
    }

    /**
     * @return the offset of the {@code >} that closes the type arguments opening at
     * {@code offset}, or -1 when a character that no type argument holds comes first
     */
    private static int findTypeArgumentsEnd(String code, int offset) {
        int depth = 0;
        for (int i = offset, n = code.length(); i < n; i++) {
            final char c = code.charAt(i);
            if (c == '<') {
                depth++;
            } else if (c == '>') {
                if (--depth == 0) {
                    return i;
                }
            } else if (!Character.isJavaIdentifierPart(c) && !Character.isWhitespace(c) && ".,?[]&@".indexOf(c) < 0) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * @return the end of the identifier at {@code offset}, or {@code offset} when none
     * starts there
     */
    private static int identifierEnd(String code, int offset) {
        if (offset == code.length() || !Character.isJavaIdentifierStart(code.charAt(offset))) {
            return offset;
        }
        int end = offset + 1;
        while (end < code.length() && Character.isJavaIdentifierPart(code.charAt(end))) {
            end++;
        }
        return end;
    }

    /**
     * @return {@link #HEAP_BYTE_ARRAY_ALLOWLIST} as {@code "File.java: site"} to its count
     */
    private static Map<String, Integer> parseHeapByteArrayAllowlist() {
        final Map<String, Integer> allowed = new TreeMap<>();
        String file = null;
        for (String line : HEAP_BYTE_ARRAY_ALLOWLIST.split("\n")) {
            final String entry = line.trim();
            if (entry.isEmpty() || entry.startsWith("#")) {
                continue;
            }
            if (!Character.isWhitespace(line.charAt(0))) {
                Assert.assertTrue("allowlist file line: " + line, entry.endsWith(".java"));
                file = entry;
                continue;
            }
            Assert.assertNotNull("allowlist site before any file: " + line, file);
            final int space = entry.indexOf(' ');
            Assert.assertTrue("allowlist site needs a count: " + line, space > 0);
            final int count = Integer.parseInt(entry.substring(0, space));
            Assert.assertTrue("allowlist count must be positive: " + line, count > 0);
            Assert.assertNull(
                    "allowlist site listed twice: " + line,
                    allowed.put(file + ": " + entry.substring(space + 1), count)
            );
        }
        return allowed;
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
