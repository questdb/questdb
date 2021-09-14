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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GeoHashFunction;

public class GeoHashColumn {
    public static GeoHashFunction newInstance(int columnIndex, int columnType) throws SqlException {
        switch (ColumnType.sizeOf(columnType)) {
            case 8:
                return new GeoHashColumnLong(columnIndex, columnType);
            case 4:
                return new GeoHashColumnInt(columnIndex, columnType);
            case 2:
                return new GeoHashColumnShort(columnIndex, columnType);
            case 1:
                return new GeoHashColumnByte(columnIndex, columnType);
            default:
                throw SqlException.position(0).put("unsupported column type ").put(ColumnType.nameOf(columnType));
        }
    }

    private static class GeoHashColumnByte extends GeoHashNotSupportedFunction {
        private final int columnIndex;

        public GeoHashColumnByte(int columnIndex, int columnType) {
            super(columnType);
            this.columnIndex = columnIndex;
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            return rec.getGeoHashByte(columnIndex);
        }
    }

    private static class GeoHashColumnShort extends GeoHashNotSupportedFunction {
        private final int columnIndex;

        public GeoHashColumnShort(int columnIndex, int columnType) {
            super(columnType);
            this.columnIndex = columnIndex;
        }

        @Override
        public short getGeoHashShort(Record rec) {
            return rec.getGeoHashShort(columnIndex);
        }
    }

    private static class GeoHashColumnInt extends GeoHashNotSupportedFunction {
        private final int columnIndex;

        public GeoHashColumnInt(int columnIndex, int columnType) {
            super(columnType);
            this.columnIndex = columnIndex;
        }

        @Override
        public int getGeoHashInt(Record rec) {
            return rec.getGeoHashInt(columnIndex);
        }
    }

    private static class GeoHashColumnLong extends GeoHashNotSupportedFunction {
        private final int columnIndex;

        public GeoHashColumnLong(int columnIndex, int columnType) {
            super(columnType);
            this.columnIndex = columnIndex;
        }

        @Override
        public long getGeoHashLong(Record rec) {
            return rec.getGeoHashLong(columnIndex);
        }
    }

    private static class GeoHashNotSupportedFunction extends GeoHashFunction implements ScalarFunction {
        public GeoHashNotSupportedFunction(int columnType) {
            super(columnType);
        }

        @Override
        public byte getGeoHashByte(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getGeoHashShort(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getGeoHashInt(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getGeoHashLong(Record rec) {
            throw new UnsupportedOperationException();
        }
    }
}
