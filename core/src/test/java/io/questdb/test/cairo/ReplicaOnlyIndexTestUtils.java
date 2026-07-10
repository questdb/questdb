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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Shared on-disk index-file scanner used by the REPLICA ONLY index test suite. The whole suite
 * rests on the invariant that "is this index materialized on disk?" is decided by exactly the same
 * byte-identical scan everywhere, so the logic lives here once instead of being copy-pasted into
 * every test class.
 * <p>
 * Scans every partition directory under the table root for any bitmap key file ("&lt;col&gt;.k.*"),
 * bitmap value file ("&lt;col&gt;.v.*"), or posting index file ("&lt;col&gt;.pk.*" / "&lt;col&gt;.pv.*")
 * for the given column. Per-column index files are named "&lt;col&gt;.k"/"&lt;col&gt;.v" in the
 * PARTITION dir; the symbol dictionary's own "&lt;col&gt;.k"/"&lt;col&gt;.v" live at the TABLE ROOT and
 * are deliberately not scanned here.
 */
public final class ReplicaOnlyIndexTestUtils {

    private ReplicaOnlyIndexTestUtils() {
    }

    // Removes every per-partition index file (bitmap "<col>.k"/"<col>.v" and posting
    // "<col>.pk"/"<col>.pv", with or without a columnNameTxn suffix) for the given column.
    // Inverts indexFilesExist below.
    public static void deleteIndexFiles(CairoEngine engine, String table, String col) {
        forEachIndexFile(engine, table, col, (ff, fullPath) -> ff.removeQuiet(fullPath.$()));
    }

    // Invokes action for every per-partition index file (bitmap "<col>.k"/"<col>.v" and posting
    // "<col>.pk"/"<col>.pv", with or without a columnNameTxn suffix) of the given column.
    public static void forEachIndexFile(CairoEngine engine, String table, String col, IndexFileAction action) {
        final TableToken token = engine.verifyTableName(table);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final StringSink dirName = new StringSink();
        final String keyPrefix = col + ".k";
        final String valPrefix = col + ".v";
        final String postingKeyPrefix = col + ".pk";
        final String postingValPrefix = col + ".pv";
        try (Path tablePath = new Path(); Path partPath = new Path(); Path filePath = new Path()) {
            tablePath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName());
            ff.iterateDir(tablePath.$(), (pUtf8NameZ, type) -> {
                if (type != Files.DT_DIR) {
                    return;
                }
                dirName.clear();
                Utf8s.utf8ToUtf16Z(pUtf8NameZ, dirName);
                // skip "." and ".." plus non-partition dirs (wal*, txn_seq, etc.)
                if (Chars.equals(dirName, '.') || Chars.equals(dirName, "..")
                        || Chars.startsWith(dirName, "wal") || Chars.startsWith(dirName, "txn_seq")) {
                    return;
                }
                partPath.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(dirName);
                final StringSink inner = new StringSink();
                ff.iterateDir(partPath.$(), (pInnerZ, innerType) -> {
                    if (innerType != Files.DT_FILE && innerType != Files.DT_UNKNOWN) {
                        return;
                    }
                    inner.clear();
                    Utf8s.utf8ToUtf16Z(pInnerZ, inner);
                    // exact "<col>.k" / "<col>.v" or with a columnNameTxn suffix "<col>.k.N"
                    if (matchesIndexFile(inner, postingKeyPrefix)
                            || matchesIndexFile(inner, postingValPrefix)
                            || matchesIndexFile(inner, keyPrefix)
                            || matchesIndexFile(inner, valPrefix)) {
                        filePath.of(engine.getConfiguration().getDbRoot())
                                .concat(token.getDirName()).concat(dirName).concat(inner);
                        action.apply(ff, filePath);
                    }
                });
            });
        }
    }

    // True if any per-partition index file exists for the column (the symbol dictionary's own
    // "<col>.k"/"<col>.v" live at the TABLE ROOT and are deliberately not scanned here).
    public static boolean indexFilesExist(CairoEngine engine, String table, String col) {
        final boolean[] found = {false};
        forEachIndexFile(engine, table, col, (ff, fullPath) -> found[0] = true);
        return found[0];
    }

    // True if name == prefix, or name == prefix + "." + <suffix> (the columnNameTxn-suffixed form).
    // The next char after the prefix must be '.' (txn suffix) to avoid matching e.g. "s.kx".
    private static boolean matchesIndexFile(CharSequence name, String prefix) {
        if (!Chars.startsWith(name, prefix)) {
            return false;
        }
        if (name.length() == prefix.length()) {
            return true;
        }
        return name.charAt(prefix.length()) == '.';
    }

    @FunctionalInterface
    public interface IndexFileAction {
        void apply(FilesFacade ff, Path fullPath);
    }
}
