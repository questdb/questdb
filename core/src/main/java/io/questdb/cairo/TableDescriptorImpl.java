/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;

class TableDescriptorImpl extends BaseRecordMetadata implements TableDescriptor {

    private int schemaVersion;

    TableDescriptorImpl() {
        columnMetadata = new ObjList<>();
        columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    @Override
    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void of(SequencerMetadata source) {
        schemaVersion = source.getSchemaVersion();
        timestampIndex = source.getTimestampIndex();
        columnCount = source.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            final String name = source.getColumnName(i);
            final int type = source.getColumnType(i);
            columnNameIndexMap.put(name, columnNameIndexMap.size());
            columnMetadata.add(new TableColumnMetadata(name, -1L, type, false, 0, false, null, i));
        }
    }
}
