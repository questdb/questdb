/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.map;

import com.questdb.std.IntList;
import com.questdb.std.ThreadLocal;
import com.questdb.store.RecordMetadata;

public class MetadataTypeResolver implements ColumnTypeResolver {
    private RecordMetadata metadata;
    private IntList columnIndices;

    @Override
    public int count() {
        return columnIndices.size();
    }

    @Override
    public int getColumnType(int index) {
        assert index < columnIndices.size();
        return metadata.getColumnQuick(columnIndices.getQuick(index)).getType();
    }

    public MetadataTypeResolver of(RecordMetadata metadata, IntList columnIndices) {
        this.metadata = metadata;
        this.columnIndices = columnIndices;
        return this;
    }

    public static final class MetadataTypeResolverThreadLocal extends ThreadLocal<MetadataTypeResolver> {
        public MetadataTypeResolverThreadLocal() {
            super(MetadataTypeResolver::new);
        }
    }
}
