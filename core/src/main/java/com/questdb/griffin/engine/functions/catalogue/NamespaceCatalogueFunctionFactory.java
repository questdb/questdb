/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.catalogue;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.NoRandomAccessRecordCursor;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.CursorFunction;
import com.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import com.questdb.std.ObjList;

public class NamespaceCatalogueFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_catalog.pg_namespace()";
    }

    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        return new CursorFunction(
                position,
                new GenericRecordCursorFactory(
                        METADATA,
                        new NamespaceCatalogueCursor(),
                        false
                )
        );
    }

    private static class NamespaceCatalogueCursor implements NoRandomAccessRecordCursor {
        private int row = 0;

        @Override
        public void close() {
            row = 0;
        }

        @Override
        public Record getRecord() {
            return NamespaceCatalogueRecord.INSTANCE;
        }

        @Override
        public boolean hasNext() {
            return row++ == 0;
        }

        @Override
        public void toTop() {
            row = 0;
        }
    }

    private static class NamespaceCatalogueRecord implements Record {
        private static final NamespaceCatalogueRecord INSTANCE = new NamespaceCatalogueRecord();

        @Override
        public int getInt(int col) {
            return 0;
        }

        @Override
        public CharSequence getStr(int col) {
            return "public";
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("nspname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        METADATA = metadata;
    }
}
