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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

public class ImportFilesFunctionFactory implements FunctionFactory {
    public static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "import_files()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (Chars.isBlank(configuration.getSqlCopyInputRoot())) {
            throw SqlException.$(position, "import_files() is disabled ['cairo.sql.copy.root' is not set?]");
        }
        return new CursorFunction(new ImportFilesCursorFactory(configuration)) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class ImportFilesCursorFactory extends AbstractRecordCursorFactory {
        public static final Log LOG = LogFactory.getLog(ImportFilesCursorFactory.class);
        private final FilesRecordCursor cursor;
        private final Path importPath = new Path(MemoryTag.NATIVE_PATH);

        public ImportFilesCursorFactory(CairoConfiguration configuration) {
            super(METADATA);
            importPath.of(configuration.getSqlCopyInputRoot());
            int rootPathLen = importPath.size();
            cursor = new FilesRecordCursor(configuration.getFilesFacade(), importPath, rootPathLen);
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
            sink.type("import_files()");
        }

        @Override
        protected void _close() {
            Misc.free(importPath);
            cursor.close();
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("path", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("diskSize", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("diskSizeHuman", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("modifiedTime", ColumnType.DATE));
        METADATA = metadata;
    }
}