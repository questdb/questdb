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

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.NoRandomAccessRecordCursor;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.CursorFunction;
import com.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import com.questdb.std.Chars;
import com.questdb.std.Files;
import com.questdb.std.FilesFacade;
import com.questdb.std.ObjList;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;

public class ClassCatalogueFunctionFactory implements FunctionFactory {

    private static final RecordMetadata METADATA;

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("relname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("relnamespace", ColumnType.INT));
        metadata.add(new TableColumnMetadata("relkind", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("relonwer", ColumnType.INT));
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        METADATA = metadata;
    }

    @Override
    public String getSignature() {
        return "pg_class()";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        return new CursorFunction(
                position,
                new GenericRecordCursorFactory(
                        METADATA,
                        new ClassCatalogueCursor(configuration),
                        false
                )
        );
    }

    private static class ClassCatalogueCursor implements NoRandomAccessRecordCursor {
        private final Path path = new Path();
        private final FilesFacade ff;
        private final ClassCatalogueRecord record = new ClassCatalogueRecord();
        private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
        private final int plimit;
        private long findFileStruct = 0;

        public ClassCatalogueCursor(CairoConfiguration configuration) {
            this.ff = configuration.getFilesFacade();
            this.path.of(configuration.getRoot()).$();
            this.plimit = this.path.length();
        }

        @Override
        public void close() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
            path.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (findFileStruct == 0) {
                findFileStruct = ff.findFirst(path.trimTo(plimit).$());
                if (findFileStruct > 0) {
                    return next0();
                }

                findFileStruct = 0;
                return false;
            }

            if (ff.findNext(findFileStruct) > 0) {
                return next0();
            }
            return false;
        }

        private boolean next0() {
            do {
                final long pname = ff.findName(findFileStruct);
                nativeLPSZ.of(pname);
                if (
                        ff.findType(findFileStruct) == Files.DT_DIR
                                && !Chars.equals(nativeLPSZ, '.')
                                && !Chars.equals(nativeLPSZ, "..")
                ) {

                    path.trimTo(plimit);
                    if (ff.exists(path.concat(pname).concat(TableUtils.TXN_FILE_NAME).$())) {
                        return true;
                    }
                }
            } while (ff.findNext(findFileStruct) > 0);

            ff.findClose(findFileStruct);
            findFileStruct = 0;
            return false;
        }

        @Override
        public void toTop() {
            if (findFileStruct != 0) {
                ff.findClose(findFileStruct);
                findFileStruct = 0;
            }
        }

        private class ClassCatalogueRecord implements Record {
            @Override
            public CharSequence getStr(int col) {
                return nativeLPSZ.of(ff.findName(findFileStruct));
            }

            @Override
            public char getChar(int col) {
                return 't';
            }

            @Override
            public int getInt(int col) {
                return col == 1 ? 1 : 0;
            }
        }
    }
}
