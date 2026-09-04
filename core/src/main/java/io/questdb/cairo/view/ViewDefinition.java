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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a database view's metadata, including its SQL definition and dependencies.
 * <p>
 * This class is used to persist and load view definitions from disk, tracking which
 * tables and columns a view references for dependency management and schema validation.
 */
public class ViewDefinition implements Mutable {
    public static final String VIEW_DEFINITION_FILE_NAME = "_view";
    public static final int VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE = 1;
    public static final int VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;
    /**
     * Maps table names to the set of column names referenced from each table.
     * <p>
     * For example, for a view defined as:
     * <pre>SELECT a, b FROM table1 JOIN table2 ON table1.x = table2.y</pre>
     * the dependencies would contain:
     * <ul>
     *   <li>"table1" → {"a", "x"}</li>
     *   <li>"table2" → {"b", "y"}</li>
     * </ul>
     * Used for tracking view invalidation when underlying tables change,
     * cycle detection, and schema validation.
     */
    private final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
    private boolean audited;
    private long seqTxn = -1L;
    private String viewSql;
    private TableToken viewToken;

    public static void append(@NotNull ViewDefinition viewDefinition, @NotNull AppendableBlock block) {
        block.putStr(viewDefinition.getViewSql());
        block.putLong(viewDefinition.getSeqTxn());

        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = viewDefinition.getDependencies();
        final ObjList<CharSequence> tableNames = dependencies.keys();
        final int numOfDependencies = tableNames.size();
        block.putInt(numOfDependencies);
        for (int i = 0; i < numOfDependencies; i++) {
            final CharSequence tableName = tableNames.getQuick(i);
            block.putStr(tableName);
            final LowerCaseCharSequenceHashSet columns = dependencies.get(tableName);
            block.putInt(columns.size());
            for (int j = 0; j < columns.getKeyCount(); j++) {
                final CharSequence key = columns.getKey(j);
                if (key != null) {
                    block.putStr(key);
                }
            }
        }
    }

    public static void append(@NotNull ViewDefinition viewDefinition, @NotNull BlockFileWriter writer) {
        AppendableBlock block = writer.append();
        append(viewDefinition, block);
        block.commit(VIEW_DEFINITION_FORMAT_MSG_TYPE);
        block = writer.append();
        appendExtra(viewDefinition, block);
        block.commit(VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE);
        writer.commit();
    }

    public static void appendExtra(@NotNull ViewDefinition viewDefinition, @NotNull AppendableBlock block) {
        block.putBool(viewDefinition.isAudited());
    }

    public static void readFrom(
            @NotNull ViewDefinition destDefinition,
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken viewToken
    ) {
        path.trimTo(rootLen).concat(viewToken.getDirName()).concat(VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        boolean definitionBlockFound = false;
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                definitionBlockFound = true;
                readDefinitionBlock(destDefinition, block, viewToken);
                // keep going, the extra block may follow
                continue;
            }
            if (block.type() == VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE) {
                readExtraBlock(destDefinition, block);
                return;
            }
        }

        if (!definitionBlockFound) {
            throw CairoException.critical(0)
                    .put("cannot read view definition, block not found [path=").put(path)
                    .put(']');
        }
        // Views created before auditing existed carry no extra block. readDefinitionBlock()
        // has already defaulted the flag to false for them, which is the correct reading:
        // a view that never opted in is not audited.
    }

    @Override
    public void clear() {
        viewToken = null;
        viewSql = null;
        seqTxn = -1L;
        audited = false;
        dependencies.clear();
    }

    public LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> getDependencies() {
        return dependencies;
    }

    public long getSeqTxn() {
        return seqTxn;
    }

    public String getViewSql() {
        return viewSql;
    }

    public TableToken getViewToken() {
        return viewToken;
    }

    public void init(
            @NotNull TableToken viewToken,
            @NotNull String viewSql,
            long seqTxn,
            boolean audited
    ) {
        this.viewToken = viewToken;
        this.viewSql = viewSql;
        this.seqTxn = seqTxn;
        this.audited = audited;
    }

    public void init(
            @NotNull TableToken viewToken,
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            long seqTxn,
            boolean audited
    ) {
        init(viewToken, viewSql, seqTxn, audited);

        // shallow copy, all table and column names should be string objects in the dependencies map
        this.dependencies.putAll(dependencies);
    }

    /**
     * Reports whether queries reading this view emit a row into the view audit table.
     * The flag is set by {@code CREATE VIEW ... WITH AUDIT} and lives in the definition's
     * extra block, so a view created before auditing existed reads back as not audited.
     */
    public boolean isAudited() {
        return audited;
    }

    private static void readDefinitionBlock(
            ViewDefinition destDefinition,
            ReadableBlock block,
            TableToken viewToken
    ) {
        assert block.type() == VIEW_DEFINITION_FORMAT_MSG_TYPE;

        long offset = 0;

        final CharSequence viewSql = block.getStr(offset);
        if (viewSql == null || viewSql.isEmpty()) {
            throw CairoException.critical(0)
                    .put("view SQL is empty [view=")
                    .put(viewToken.getTableName())
                    .put(']');
        }
        offset += Vm.getStorageLength(viewSql);
        final String viewSqlStr = Chars.toString(viewSql);

        final long seqTxn = block.getLong(offset);
        offset += Long.BYTES;

        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = destDefinition.getDependencies();
        final int numOfDependencies = block.getInt(offset);
        offset += Integer.BYTES;

        for (int i = 0; i < numOfDependencies; i++) {
            final String tableName = Chars.toString(block.getStr(offset));
            offset += Vm.getStorageLength(tableName);

            final int numOfColumns = block.getInt(offset);
            offset += Integer.BYTES;

            final LowerCaseCharSequenceHashSet columns = new LowerCaseCharSequenceHashSet();
            for (int j = 0; j < numOfColumns; j++) {
                final CharSequence columnName = block.getStr(offset);
                offset += Vm.getStorageLength(columnName);
                columns.add(Chars.toString(columnName));
            }

            dependencies.put(tableName, columns);
        }

        destDefinition.init(viewToken, viewSqlStr, seqTxn, false);
    }

    private static void readExtraBlock(ViewDefinition destDefinition, ReadableBlock block) {
        assert block.type() == VIEW_DEFINITION_FORMAT_EXTRA_MSG_TYPE;
        destDefinition.audited = block.getBool(0);
    }
}
