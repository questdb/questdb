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
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.ObjList;

public class NamespaceCatalogueFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("nspname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        METADATA = metadata;
    }

    @Override
    public String getSignature() {
        return "pg_catalog.pg_namespace()";
    }

    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
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
            return 1;
        }

        @Override
        public CharSequence getStr(int col) {
            return "public";
        }
    }
}
