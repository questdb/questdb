/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.catalogue.FilesFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.FilesRecordCursor;
import io.questdb.griffin.engine.functions.catalogue.ImportFilesFunctionFactory;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.regex.GlobStrFunctionFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;


// SELECT * FROM glob('./import/table/file_*.parquet');
// files('/import') WHERE glob(filename, 'table/file_*.parquet');
// SelectedRecordCursor <-- GlobStrFunctionFactory <-- FilesRecordCursor

/**
 * Provides a pseudo table returning file data based on a glob pattern.
 */
public class GlobFilesFunctionFactory implements FunctionFactory {
    StringSink sink = new StringSink();

    /**
     * Extracts the non-glob prefix from a glob pattern.
     * <p>
     * Examples:
     * <ul>
     *   <li>"pattern/file_*.parquet" -&gt; "pattern/"</li>
     *   <li>"/path/to/data/{@literal *}/file_?.csv" -&gt; "/path/to/data/"</li>
     *   <li>"/abs/path/to/file*.txt" -&gt; "/abs/path/to/"</li>
     * </ul>
     * <p>
     * The function stops at the first glob metacharacter: {@literal *}, ?, [, or ]
     * It includes the directory separator before the first glob pattern.
     *
     * @param globPattern the glob pattern string
     * @return the non-glob prefix (everything up to and including the last separator before the first glob char)
     */
    public static CharSequence extractNonGlobPrefix(CharSequence globPattern) {
        if (globPattern == null || globPattern.length() == 0) {
            return "";
        }

        int lastSlashIndex = -1;
        for (int i = 0; i < globPattern.length(); i++) {
            char c = globPattern.charAt(i);

            // Check if this is a glob metacharacter
            if (c == '*' || c == '?' || c == '[' || c == ']') {
                // We found a glob pattern, return excluding the last slash
                if (lastSlashIndex >= 0) {
                    return globPattern.subSequence(0, lastSlashIndex);
                } else {
                    // No slash before the glob, return empty
                    return "";
                }
            }

            // Track the last slash we've seen
            if (c == '/' || c == '\\') {
                lastSlashIndex = i;
            }
        }

        // No glob patterns found - return the entire pattern
        return globPattern;
    }

    @Override
    public String getSignature() {
        return "glob(s)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function arg = args.getQuick(0);
        assert arg.isConstant();

        CharSequence glob = arg.getStrA(null);
        CharSequence root = extractNonGlobPrefix(glob);
        if (!Chars.equals(glob, root)) {
            glob = glob.subSequence(root.length(), glob.length());
        }
        if (glob.charAt(0) == '/' || glob.charAt(0) == '\\') {
            glob = glob.subSequence(1, glob.length());
        }

        ObjList<Function> newArgs = new ObjList<>();
        newArgs.add(new StrConstant(root));

        RecordCursorFactory filesCursor = new FilesFunctionFactory().newInstance(position, newArgs, argPositions, configuration, sqlExecutionContext).getRecordCursorFactory();

        newArgs.set(0, new VarcharColumn(0));
        newArgs.add(new StrConstant(glob));

        Function globFunc = new GlobStrFunctionFactory().newInstance(position, newArgs, argPositions, configuration, sqlExecutionContext);

        return new CursorFunction(new FilteredRecordCursorFactory(filesCursor, globFunc));
    }

    /*
     * Factory for single-threaded read_parquet() SQL function.
     */
    public static class GlobFilesRecordCursorFactory extends AbstractRecordCursorFactory {
        private GlobFilesRecordCursor cursor;
        private CharSequence glob;
        private Path path;


        public GlobFilesRecordCursorFactory(CharSequence glob) {
            super(ImportFilesFunctionFactory.METADATA);
            this.glob = glob;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
//            cursor.of(path.$());
            return cursor;
        }

        public Path getPath() {
            return path;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("glob filesystem sequential scan");
        }

        @Override
        protected void _close() {
            cursor = Misc.free(cursor);
            path = Misc.free(path);
        }

        static {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("path", ColumnType.STRING));

//            METADATA = metadata;
        }
    }

    public class GlobFilesRecordCursor extends FilesRecordCursor {

        public GlobFilesRecordCursor(FilesFacade ff, Path rootPath, int rootPathLen) {
            super(ff, rootPath, rootPathLen);
        }

        @Override
        public boolean hasNext() {
            if (super.hasNext()) {
//                final CharSequence filepath = getRecord().getStrA()

            }
            return false;
        }
    }
}
