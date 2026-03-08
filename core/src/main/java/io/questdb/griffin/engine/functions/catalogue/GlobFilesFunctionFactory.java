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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation for directory traversal with glob pattern matching.
 * <p>
 * Supported glob patterns:
 * - * matches any sequence of characters (except path separator)
 * - ** matches any sequence of characters including path separators (recursive)
 * - ? matches any single character
 * - [abc] matches any character in the set
 * - [a-z] matches any character in the range
 * - [!abc] matches any character not in the set
 * - \ escapes the next character
 * <p>
 * Note: ~ (home directory) expansion is not supported.
 */
public class GlobFilesFunctionFactory implements FunctionFactory {

    /**
     * Find all files matching the glob pattern.
     *
     * @param ff             FilesFacade for file operations
     * @param glob           the glob pattern (e.g., "/data/**&#47;*.parquet")
     * @param workingPath    reusable Path instance for file system operations
     * @param defaultRoot    default root directory for relative glob patterns (e.g., cairo.sql.copy.root)
     * @param fileNameSink   reusable sink for reading file names during directory iteration
     * @param resultFiles    output list for matched file paths; also used as working buffer in double-buffering
     * @param tempPaths      temp list for double-buffering swap; cleared and reused internally
     * @param patternOffsets pre-parsed segment offsets from {@link #parseGlobPattern}, stores (start, end) pairs
     */
    public static void globFiles(
            FilesFacade ff,
            Utf8Sequence glob,
            Path workingPath,
            CharSequence defaultRoot,
            Utf8StringSink fileNameSink,
            DirectUtf8StringList resultFiles,
            DirectUtf8StringList tempPaths,
            IntList patternOffsets
    ) {
        resultFiles.clear();
        tempPaths.clear();

        int n = patternOffsets.size() / 2;
        if (n == 0) {
            return;
        }

        int firstGlobSegment = n;
        for (int i = 0; i < n; i++) {
            int low = patternOffsets.getQuick(i * 2);
            int high = patternOffsets.getQuick(i * 2 + 1);
            if (hasGlob(glob, low, high) || isGlobStar(glob, low, high)) {
                firstGlobSegment = i;
                break;
            }
        }

        Utf8Sequence rootPath = buildRootPath(glob, patternOffsets, firstGlobSegment, defaultRoot);
        if (firstGlobSegment == n) {
            workingPath.of(rootPath).$();
            if (ff.exists(workingPath.$())) {
                if (!ff.isDirOrSoftLinkDir(workingPath.$())) {
                    resultFiles.put(rootPath);
                }
            }
            return;
        }

        // Calculate parity based on remaining segments to process
        int remainingSegments = n - firstGlobSegment;
        DirectUtf8StringList currentPaths;
        DirectUtf8StringList nextPaths;
        if ((remainingSegments & 1) == 1) {
            currentPaths = tempPaths;
            nextPaths = resultFiles;
        } else {
            currentPaths = resultFiles;
            nextPaths = tempPaths;
        }
        currentPaths.put(rootPath);

        for (int i = firstGlobSegment; i < n; i++) {
            boolean isLastSegment = (i == n - 1);
            nextPaths.clear();
            int low = patternOffsets.getQuick(i * 2);
            int high = patternOffsets.getQuick(i * 2 + 1);

            if (isGlobStar(glob, low, high)) {
                // ** pattern: recursive directory matching
                if (!isLastSegment) {
                    // ** can match zero directories, so keep current paths
                    for (int j = 0, m = currentPaths.size(); j < m; j++) {
                        nextPaths.put(currentPaths.getQuick(j));
                    }
                    // Recursively find all subdirectories
                    for (int j = 0, m = currentPaths.size(); j < m; j++) {
                        recursiveGlobDirectories(ff, currentPaths.getQuick(j), nextPaths, workingPath, fileNameSink);
                    }
                } else {
                    // ** is last segment: find all files recursively
                    for (int j = 0, m = currentPaths.size(); j < m; j++) {
                        recursiveGlobFiles(ff, currentPaths.getQuick(j), nextPaths, workingPath, fileNameSink);
                    }
                }
            } else if (hasGlob(glob, low, high)) {
                for (int j = 0, m = currentPaths.size(); j < m; j++) {
                    globFilesInternal(ff, currentPaths.getQuick(j), glob, low, high, !isLastSegment, nextPaths, workingPath, fileNameSink);
                }
            } else {
                for (int j = 0, m = currentPaths.size(); j < m; j++) {
                    Utf8Sequence basePath = currentPaths.getQuick(j);
                    Utf8Sequence newPath = joinPath(basePath, glob, low, high);
                    workingPath.of(newPath).$();
                    if (ff.exists(workingPath.$())) {
                        boolean isDir = ff.isDirOrSoftLinkDir(workingPath.$());
                        if (isLastSegment) {
                            if (!isDir) {
                                nextPaths.put(newPath);
                            }
                        } else {
                            if (isDir) {
                                nextPaths.put(newPath);
                            }
                        }
                    }
                }
            }

            DirectUtf8StringList tmp = currentPaths;
            currentPaths = nextPaths;
            nextPaths = tmp;
        }

        // Literal fallback: if no matches found, try treating the pattern as a literal path
        if (resultFiles.size() == 0) {
            Utf8Sequence literalPath = buildLiteralPath(glob, patternOffsets, defaultRoot);
            workingPath.of(literalPath).$();
            if (ff.exists(workingPath.$()) && !ff.isDirOrSoftLinkDir(workingPath.$())) {
                resultFiles.put(literalPath);
            }
        }
    }

    /**
     * Check if name matches the glob pattern.
     * Supports: *, ?, [abc], [a-z], [!abc], \ escape
     *
     * @param name    the string to match (e.g., file name)
     * @param pattern the glob pattern to match against
     * @return true if name matches the pattern, false otherwise
     */
    public static boolean globMatch(@NotNull Utf8Sequence name, @NotNull Utf8Sequence pattern, int low, int high) {
        return globMatchInternal(name, 0, name.size(), pattern, low, high);
    }

    /**
     * Check if a segment contains glob characters.
     */
    public static boolean hasGlob(Utf8Sequence segment, int low, int high) {
        for (int i = low; i < high; i++) {
            byte c = segment.byteAt(i);
            if (c == '\\' && Files.SEPARATOR != '\\' && i + 1 < high) {
                byte next = segment.byteAt(i + 1);
                if (next == '*' || next == '?' || next == '[' || next == ']' || next == '\\') {
                    i++;
                    continue;
                }
            }
            if (c == '*' || c == '?' || c == '[') {
                return true;
            }
        }
        return false;
    }

    /**
     * Parse glob pattern into segments separated by '/' or Files.SEPARATOR.
     * Stores (start, end) offset pairs in IntList.
     * Segment i: start = offsets.get(2*i), end = offsets.get(2*i + 1)
     */
    public static void parseGlobPattern(Utf8Sequence pattern, IntList offsets) {
        int lastPos = 0;
        int n = pattern.size();
        for (int i = 0; i < n; i++) {
            byte c = pattern.byteAt(i);
            if (c == '\\') {
                if (Files.SEPARATOR == '\\') {
                    if (i > lastPos) {
                        offsets.add(lastPos);
                        offsets.add(i);
                    }
                    lastPos = i + 1;
                } else {
                    if (i + 1 < n) {
                        byte next = pattern.byteAt(i + 1);
                        if (next == '*' || next == '?' || next == '[' || next == ']' || next == '\\') {
                            i++;
                        }
                    }
                }
                continue;
            }
            if (c == '/' || c == Files.SEPARATOR) {
                if (i > lastPos) {
                    offsets.add(lastPos);
                    offsets.add(i);
                }
                lastPos = i + 1;
            }
        }
        if (lastPos < n) {
            offsets.add(lastPos);
            offsets.add(n);
        }
    }

    @Override
    public String getSignature() {
        return "glob(s)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function arg = args.getQuick(0);
        assert arg.isConstant();
        Utf8Sequence glob = arg.getVarcharA(null);
        if (glob == null || glob.size() == 0) {
            throw SqlException.$(argPositions.getQuick(0), "glob pattern cannot be null or empty");
        }

        if (!isAbsolutePathQuick(glob)) {
            if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
                throw SqlException.$(position, "'cairo.sql.copy.root' is not set");
            }
        }

        IntList globOffsets = new IntList();
        parseGlobPattern(glob, globOffsets);
        validateNoPathTraversal(glob, globOffsets, argPositions.getQuick(0));
        int crawlCount = 0;
        for (int i = 0, n = globOffsets.size() / 2; i < n; i++) {
            if (isGlobStar(glob, globOffsets.getQuick(i * 2), globOffsets.getQuick(i * 2 + 1))) {
                crawlCount++;
            }
        }

        if (crawlCount > 1) {
            throw SqlException.$(argPositions.getQuick(0), "cannot use multiple '**' in one path");
        }
        return new CursorFunction(new GlobFilesCursorFactory(configuration, glob, globOffsets));
    }

    private static void appendSeparatorIfNeeded(Utf8StringSink sink) {
        int n = sink.size();
        if (n > 0) {
            byte lastChar = sink.byteAt(n - 1);
            if (lastChar != '/' && lastChar != Files.SEPARATOR) {
                sink.putAscii(Files.SEPARATOR);
            }
        }
    }

    private static Utf8Sequence buildLiteralPath(Utf8Sequence glob, IntList patternOffsets, CharSequence defaultRoot) {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        boolean isAbsolute = isAbsolutePath(glob, patternOffsets);

        if (isAbsolute) {
            sink.put(glob);
        } else {
            sink.put(defaultRoot);
            appendSeparatorIfNeeded(sink);
            sink.put(glob);
        }

        return sink;
    }

    private static Utf8Sequence buildRootPath(Utf8Sequence glob, IntList patternOffsets, int firstGlobSegment, CharSequence defaultRoot) {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        int firstSegmentStart = patternOffsets.getQuick(0);
        boolean isAbsolute = isAbsolutePath(glob, patternOffsets);

        if (firstGlobSegment == 0) {
            if (isAbsolute) {
                if (firstSegmentStart > 0) {
                    sink.put(glob, 0, firstSegmentStart);
                } else {
                    int firstSegmentEnd = patternOffsets.getQuick(1);
                    sink.put(glob, 0, firstSegmentEnd);
                }
            } else {
                sink.put(defaultRoot);
            }
        } else {
            int lastSegmentEnd = patternOffsets.getQuick(firstGlobSegment * 2 - 1);
            if (!isAbsolute) {
                sink.put(defaultRoot);
                appendSeparatorIfNeeded(sink);
            }
            sink.put(glob, 0, lastSegmentEnd);
        }

        return sink;
    }

    private static void globFilesInternal(
            FilesFacade ff,
            Utf8Sequence basePath,
            Utf8Sequence pattern,
            int low,
            int high,
            boolean matchDirectory,
            DirectUtf8StringList resultPaths,
            Path workingPath,
            Utf8StringSink fileNameSink
    ) {
        workingPath.of(basePath).$();
        long findPtr = ff.findFirst(workingPath.$());
        if (findPtr <= 0) {
            return;
        }

        try {
            do {
                long namePtr = ff.findName(findPtr);
                if (namePtr == 0) {
                    break;
                }

                fileNameSink.clear();
                Utf8s.utf8ZCopy(namePtr, fileNameSink);

                if (!Files.notDots(fileNameSink)) {
                    continue;
                }

                int type = ff.findType(findPtr);
                // Skip symbolic links for security and to avoid cycles
                if (type == Files.DT_LNK) {
                    continue;
                }
                if (matchDirectory) {
                    if (type != Files.DT_DIR) {
                        continue;
                    }
                } else {
                    if (type != Files.DT_FILE) {
                        continue;
                    }
                }

                // Match using glob pattern
                if (globMatch(fileNameSink, pattern, low, high)) {
                    resultPaths.put(joinPath(basePath, fileNameSink, 0, fileNameSink.size()));
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
    }

    private static boolean globMatchInternal(Utf8Sequence name, int ni, int nLen, Utf8Sequence pattern, int pi, int plen) {
        while (ni < nLen && pi < plen) {
            byte n = name.byteAt(ni);
            byte p = pattern.byteAt(pi);

            switch (p) {
                case '*': {
                    // Asterisk: match any set of characters
                    // Skip any subsequent asterisks
                    do {
                        pi++;
                    } while (pi < plen && pattern.byteAt(pi) == '*');
                    // If the asterisk is the last character, the pattern always matches
                    if (pi == plen) {
                        return true;
                    }
                    // Recursively match the remainder of the pattern
                    for (; ni < nLen; ni++) {
                        if (globMatchInternal(name, ni, nLen, pattern, pi, plen)) {
                            return true;
                        }
                    }
                    return false;
                }
                case '?':
                    // Matches any single character
                    ni++;
                    pi++;
                    break;
                case '[': {
                    // Bracket expression: returns new pi if matched, -1 if not matched
                    pi++;
                    int newPi = parseBracket(name, ni, nLen, pattern, pi, plen);
                    if (newPi < 0) {
                        return false;
                    }
                    pi = newPi;
                    ni++;
                    break;
                }
                case '\\':
                    if (Files.SEPARATOR == '\\') {
                        if (n != '\\' && n != '/') {
                            return false;
                        }
                        ni++;
                        pi++;
                    } else {
                        if (pi + 1 < plen) {
                            byte next = pattern.byteAt(pi + 1);
                            if (next == '*' || next == '?' || next == '[' || next == ']' || next == '\\') {
                                pi++;
                                p = pattern.byteAt(pi);
                                if (n != p) {
                                    return false;
                                }
                                ni++;
                                pi++;
                                break;
                            }
                        }
                        if (n != '\\') {
                            return false;
                        }
                        ni++;
                        pi++;
                    }
                    break;
                case '/':
                    // Forward slash is a path separator - match both / and \
                    if (n != '/' && n != '\\') {
                        return false;
                    }
                    ni++;
                    pi++;
                    break;
                default:
                    // Not a control character: characters need to match literally
                    if (n != p) {
                        return false;
                    }
                    ni++;
                    pi++;
                    break;
            }
        }

        // Skip any trailing asterisks in pattern
        while (pi < plen && pattern.byteAt(pi) == '*') {
            pi++;
        }

        // We are finished only if we have consumed the full pattern and name
        return pi == plen && ni == nLen;
    }

    private static boolean isAbsolutePath(Utf8Sequence glob, IntList patternOffsets) {
        if (patternOffsets.size() < 2) {
            return false;
        }

        int firstSegmentStart = patternOffsets.getQuick(0);
        if (firstSegmentStart > 0) {
            return true;
        }
        int firstSegmentEnd = patternOffsets.getQuick(1);
        return isWindowsDriveLetter(glob, firstSegmentStart, firstSegmentEnd);
    }

    private static boolean isAbsolutePathQuick(Utf8Sequence glob) {
        if (glob.size() == 0) {
            return false;
        }
        byte firstChar = glob.byteAt(0);

        // Unix absolute path
        if (firstChar == '/' || firstChar == Files.SEPARATOR) {
            return true;
        }

        // Windows absolute path
        if (glob.size() >= 2) {
            byte secondChar = glob.byteAt(1);
            return secondChar == ':' && ((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z'));
        }
        return false;
    }

    private static boolean isGlobStar(Utf8Sequence segment, int low, int high) {
        return high - low == 2 && segment.byteAt(low) == '*' && segment.byteAt(low + 1) == '*';
    }

    private static boolean isWindowsDriveLetter(Utf8Sequence glob, int start, int end) {
        if (end - start != 2) {
            return false;
        }
        byte firstChar = glob.byteAt(start);
        byte secondChar = glob.byteAt(start + 1);
        return ((firstChar >= 'A' && firstChar <= 'Z') || (firstChar >= 'a' && firstChar <= 'z')) && secondChar == ':';
    }

    private static Utf8Sequence joinPath(Utf8Sequence base, Utf8Sequence name, int low, int high) {
        Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
        int n = base.size();
        if (n == 0) {
            sink.put(name, low, high);
            return sink;
        }

        byte lastChar = base.byteAt(n - 1);
        if (lastChar == '/' || lastChar == Files.SEPARATOR) {
            sink.put(base).put(name, low, high);
            return sink;
        }
        sink.put(base).putAscii(Files.SEPARATOR).put(name, low, high);
        return sink;
    }

    /**
     * Parse bracket expression [abc] or [a-z] or [!abc].
     *
     * @return new pattern index if matched, -1 if not matched
     */
    private static int parseBracket(Utf8Sequence name, int ni, int nLen, Utf8Sequence pattern, int pi, int plen) {
        if (pi == plen || ni >= nLen) {
            return -1;
        }

        byte n = name.byteAt(ni);

        // Check if first character is '!' for negation
        boolean invert = false;
        if (pattern.byteAt(pi) == '!') {
            invert = true;
            pi++;
        }

        boolean foundMatch = invert;
        int startPos = pi;
        boolean foundClosingBracket = false;

        // Now check the remainder of the pattern
        while (pi < plen) {
            byte p = pattern.byteAt(pi);

            // If the first character is a closing bracket, we match it literally
            // Otherwise it indicates an end of bracket
            if (p == ']' && pi > startPos) {
                foundClosingBracket = true;
                pi++;
                break;
            }

            // Check if next character is a dash (range)
            if (pi + 1 < plen && pattern.byteAt(pi + 1) == '-' && pi + 2 < plen && pattern.byteAt(pi + 2) != ']') {
                byte rangeEnd = pattern.byteAt(pi + 2);
                boolean matches = n >= p && n <= rangeEnd;
                pi += 3;

                if (foundMatch == invert && matches) {
                    foundMatch = !invert;
                }
            } else {
                // Direct match
                boolean matches = (p == n);
                pi++;

                if (foundMatch == invert && matches) {
                    foundMatch = !invert;
                }
            }
        }

        if (!foundClosingBracket || !foundMatch) {
            return -1;
        }

        return pi;
    }

    private static void recursiveGlobDirectories(
            FilesFacade ff,
            Utf8Sequence basePath,
            DirectUtf8StringList resultPaths,
            Path workingPath,
            Utf8StringSink fileNameSink
    ) {
        Utf8StringSink basePathCopy = Misc.acquireUtf8Sink();
        try {
            basePathCopy.put(basePath);
            workingPath.of(basePathCopy).$();
            long findPtr = ff.findFirst(workingPath.$());
            if (findPtr <= 0) {
                return;
            }

            try {
                do {
                    long namePtr = ff.findName(findPtr);
                    if (namePtr == 0) {
                        break;
                    }

                    fileNameSink.clear();
                    Utf8s.utf8ZCopy(namePtr, fileNameSink);

                    if (!Files.notDots(fileNameSink)) {
                        continue;
                    }

                    int type = ff.findType(findPtr);
                    // Skip symbolic links
                    if (type == Files.DT_LNK) {
                        continue;
                    }
                    if (type == Files.DT_DIR) {
                        Utf8Sequence fullPath = joinPath(basePathCopy, fileNameSink, 0, fileNameSink.size());
                        resultPaths.put(fullPath);
                        recursiveGlobDirectories(ff, fullPath, resultPaths, workingPath, fileNameSink);
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        } finally {
            basePathCopy.clear();
            Misc.releaseUtf8Sink();
        }
    }

    private static void recursiveGlobFiles(
            FilesFacade ff,
            Utf8Sequence basePath,
            DirectUtf8StringList resultPaths,
            Path workingPath,
            Utf8StringSink fileNameSink
    ) {
        Utf8StringSink basePathCopy = Misc.acquireUtf8Sink();
        try {
            basePathCopy.put(basePath);
            workingPath.of(basePathCopy).$();
            long findPtr = ff.findFirst(workingPath.$());
            if (findPtr <= 0) {
                return;
            }

            try {
                do {
                    long namePtr = ff.findName(findPtr);
                    if (namePtr == 0) {
                        break;
                    }

                    fileNameSink.clear();
                    Utf8s.utf8ZCopy(namePtr, fileNameSink);

                    if (!Files.notDots(fileNameSink)) {
                        continue;
                    }

                    int type = ff.findType(findPtr);
                    // Skip symbolic links
                    if (type == Files.DT_LNK) {
                        continue;
                    }

                    Utf8Sequence fullPath = joinPath(basePathCopy, fileNameSink, 0, fileNameSink.size());
                    if (type == Files.DT_DIR) {
                        recursiveGlobFiles(ff, fullPath, resultPaths, workingPath, fileNameSink);
                    } else if (type == Files.DT_FILE) {
                        resultPaths.put(fullPath);
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        } finally {
            basePathCopy.clear();
            Misc.releaseUtf8Sink();
        }
    }

    private static void validateNoPathTraversal(Utf8Sequence glob, IntList offsets, int position) throws SqlException {
        for (int i = 0, n = offsets.size() / 2; i < n; i++) {
            int start = offsets.getQuick(i * 2);
            int end = offsets.getQuick(i * 2 + 1);
            if (end - start == 2 && glob.byteAt(start) == '.' && glob.byteAt(start + 1) == '.') {
                throw SqlException.$(position, "path traversal '..' is not allowed in glob pattern");
            }
        }
    }

    static class GlobFileRecord implements Record {
        private static final int MODIFIED_TIME_COLUMN = 3;
        private static final int PATH_COLUMN = 0;
        private static final int SIZE_COLUMN = 1;
        private static final int SIZE_HUMAN_COLUMN = 2;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();
        private final StringSink sizeSink = new StringSink();
        protected Utf8Sequence fileName;
        protected long modifiedTime;
        protected long size;

        @Override
        public long getDate(int col) {
            if (col == MODIFIED_TIME_COLUMN) {
                return modifiedTime;
            }
            return 0;
        }

        @Override
        public long getLong(int col) {
            if (col == SIZE_COLUMN) {
                return size;
            }
            return 0;
        }

        @Override
        public CharSequence getStrA(int col) {
            if (col == SIZE_HUMAN_COLUMN) {
                return sizeSink;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStrA(col);
        }

        @Override
        public int getStrLen(int col) {
            if (col == SIZE_HUMAN_COLUMN) {
                return sizeSink.length();
            }
            return TableUtils.NULL_LEN;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            if (col == PATH_COLUMN) {
                sinkA.clear();
                sinkA.put(fileName);
                return sinkA;
            }
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            if (col == PATH_COLUMN) {
                sinkB.clear();
                sinkB.put(fileName);
                return sinkB;
            }
            return null;
        }

        public void of(Utf8Sequence fileName, long size, long modifiedTime) {
            this.fileName = fileName;
            this.size = size;
            this.modifiedTime = modifiedTime;
            this.sizeSink.clear();
            SizePrettyFunctionFactory.toSizePretty(sizeSink, size);
        }
    }

    static class GlobFilesCursorFactory extends AbstractRecordCursorFactory {
        private final GlobFilesRecordCursor cursor;
        private final Utf8Sequence glob;

        public GlobFilesCursorFactory(CairoConfiguration configuration, Utf8Sequence glob, IntList globOffsets) {
            super(ImportFilesFunctionFactory.METADATA);
            this.glob = glob;
            cursor = new GlobFilesRecordCursor(configuration.getFilesFacade(), configuration.getSqlCopyInputRoot(), glob, globOffsets);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("glob").val(glob);
        }

        @Override
        protected void _close() {
            Misc.free(cursor);
        }
    }

    static class GlobFilesRecordCursor implements NoRandomAccessRecordCursor {
        private final CharSequence defaultRoot;
        private final FilesFacade ff;
        private final Utf8StringSink fileNameSink = new Utf8StringSink();
        private final Utf8Sequence glob;
        private final IntList globOffsets;
        private final DirectUtf8StringList pendingFiles = new DirectUtf8StringList(128, 8);
        private final GlobFileRecord record = new GlobFileRecord();
        private final DirectUtf8StringList tempPaths = new DirectUtf8StringList(128, 8);
        private final Path workingPath = new Path(MemoryTag.NATIVE_PATH);
        private boolean initialized = false;
        private int matchIndex = 0;

        public GlobFilesRecordCursor(FilesFacade ff, CharSequence defaultRoot, Utf8Sequence glob, IntList globOffsets) {
            this.ff = ff;
            this.globOffsets = globOffsets;
            this.glob = glob;
            this.defaultRoot = defaultRoot;
        }

        @Override
        public void close() {
            Misc.free(workingPath);
            Misc.free(pendingFiles);
            Misc.free(tempPaths);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            ensureInitialize();
            if (matchIndex < pendingFiles.size()) {
                Utf8Sequence path = pendingFiles.getQuick(matchIndex++);
                workingPath.of(path).$();
                long size = ff.length(workingPath.$());
                long modifiedTime = ff.getLastModified(workingPath.$());
                record.of(path, size, modifiedTime);
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            ensureInitialize();
            return pendingFiles.size();
        }

        @Override
        public void toTop() {
            matchIndex = 0;
            initialized = false;
        }

        private void ensureInitialize() {
            if (!initialized) {
                pendingFiles.reopen();
                tempPaths.reopen();
                globFiles(ff, glob, workingPath, defaultRoot, fileNameSink, pendingFiles, tempPaths, globOffsets);
                initialized = true;
            }
        }
    }
}
