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


package io.questdb.legacy.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.Misc;

/**
 * Master's {@link JoinRecordMetadata}: it counts the factories that share it and closes when the last one does, and it
 * holds hidden columns.
 */
public class LegacyJoinRecordMetadata extends JoinRecordMetadata {
    private int refCount = 1;

    public LegacyJoinRecordMetadata(CairoConfiguration configuration, int columnCount) {
        super(configuration, columnCount);
    }

    // The column keeps its record position and qualified name, but no name resolves to it.
    public void addHidden(CharSequence tableAlias, TableColumnMetadata m) {
        final CharSequence columnName = m.getColumnName();
        final int dot = Chars.indexOfLastUnquoted(columnName, '.');
        columnMetadata.add(qualify(tableAlias, protectDottedColumnName(tableAlias, columnName, dot), m));
        columnCount++;
    }

    @Override
    public void close() {
        if (--refCount < 1) {
            super.close();
        }
    }

    public void incrementRefCount() {
        refCount++;
    }

    private CharSequence protectDottedColumnName(CharSequence tableAlias, CharSequence columnName, int dot) {
        if (tableAlias != null && dot > -1) {
            return Misc.getThreadLocalSink().put('"').put(columnName).put('"').toString();
        }
        return columnName;
    }

    private TableColumnMetadata qualify(CharSequence tableAlias, CharSequence columnName, TableColumnMetadata m) {
        if (tableAlias == null) {
            return m;
        }
        final TableColumnMetadata cm = new TableColumnMetadata(
                Misc.getThreadLocalSink().put(tableAlias).put('.').put(columnName).toString(),
                m.getColumnType(),
                m.getIndexType(),
                m.getIndexValueBlockCapacity(),
                m.isSymbolTableStatic(),
                m.getMetadata()
        );
        cm.setParquetEncodingConfig(m.getParquetEncodingConfig());
        return cm;
    }
}
