/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;

public final class GeoHashAdapter extends AbstractTypeAdapter {

    private static final IntObjHashMap<GeoHashAdapter> typeToAdapterMap = new IntObjHashMap<>();

    private final int type;

    private GeoHashAdapter(int type) {
        this.type = type;
    }

    public static GeoHashAdapter getInstance(int columnType) {
        final int index = typeToAdapterMap.keyIndex(columnType);
        if (index > -1) {
            return null;
        }
        return typeToAdapterMap.valueAtQuick(index);
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) {
        row.putGeoStr(column, SqlKeywords.isNullKeyword(value) ? null : value.asAsciiCharSequence());
    }

    static {
        for (int b = 1; b <= ColumnType.GEOLONG_MAX_BITS; b++) {
            int type = ColumnType.getGeoHashTypeWithBits(b);
            typeToAdapterMap.put(type, new GeoHashAdapter(type));
        }
    }
}
