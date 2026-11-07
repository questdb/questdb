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


package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

/**
 * One table's last schema section and its parse, kept in a {@link QwpSchemaCache}. A
 * byte-identical section with the same column count is the same schema, because the
 * section is exactly columnCount self-delimiting definitions.
 */
public final class QwpSchemaCacheEntry {
    private static final int INITIAL_BYTES = 64;
    byte[] bytes = new byte[INITIAL_BYTES];
    int columnCount = -1;
    int len;
    // Null until a parse of bytes succeeds.
    QwpSchema schema;

    @TestOnly
    public ObjList<QwpColumnDef> getColumns() {
        return schema != null ? schema.getColumns() : null;
    }

    /**
     * Bytes the entry holds: the schema section's copy; the parse behind it is of the
     * same order.
     */
    public long getRetainedBytes() {
        return schema != null ? bytes.length : 0;
    }
}
