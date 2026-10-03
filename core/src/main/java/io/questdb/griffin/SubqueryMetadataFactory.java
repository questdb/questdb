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

package io.questdb.griffin;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;

/**
 * The output metadata of a sub-query whose factory is not generated yet; it opens no cursor.
 */
final class SubqueryMetadataFactory implements RecordCursorFactory {
    private final GenericRecordMetadata metadata = new GenericRecordMetadata();

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    // A consumer that validates this capability while binding is rebuilt over the generated factory, which decides.
    @Override
    public boolean supportsPageFrameCursor() {
        return true;
    }

    SubqueryMetadataFactory of(OutputSchema output) {
        metadata.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            metadata.add(new TableColumnMetadata(Chars.toString(output.getColumnName(i)), output.getColumnType(i),
                    IndexType.NONE, 0, output.isSymbolTableStatic(i), null));
        }
        metadata.setTimestampIndex(output.getTimestampIndex());
        return this;
    }
}
