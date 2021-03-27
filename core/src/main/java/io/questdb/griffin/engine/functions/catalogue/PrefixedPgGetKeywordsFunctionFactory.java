/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.ObjList;

public class PrefixedPgGetKeywordsFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_catalog.pg_get_keywords()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(
                position,
                new GenericRecordCursorFactory(METADATA, new KeywordsCatalogueCursor(), false)
        );
    }

    private static class KeywordsCatalogueCursor implements NoRandomAccessRecordCursor {
        private static final int rowCount = Constants.KEYWORDS.length;
        private final KeywordCatalogueRecord record = new KeywordCatalogueRecord();
        private int row = -1;

        @Override
        public void close() {
            row = -1;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return ++row < rowCount;
        }

        @Override
        public void toTop() {
            row = -1;
        }

        @Override
        public long size() {
            return rowCount;
        }

        class KeywordCatalogueRecord implements Record {

            @Override
            public CharSequence getStr(int col) {
                if (col == 0) {
                    return Constants.KEYWORDS[row];
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                if (col == 0) {
                    return getStr(col).length();
                }
                return -1;
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("word", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("catcode", ColumnType.STRING, null));
        metadata.add(new TableColumnMetadata("catdesc", ColumnType.STRING, null));
        METADATA = metadata;
    }
}
