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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;

/**
 * Presents a factory's output as {@link ColumnTypes}, adding the INT width stability that a bare
 * {@link RecordMetadata} cannot express. Type tags still come from the metadata, so this stays a
 * faithful view of what the cursor produces; only {@link #isColumnIntWidthStable(int)} asks the
 * factory.
 * <p>
 * {@link RecordToRowCopierUtils} reads it so INSERT ... SELECT over an overflowing INT expression
 * stores what an explicit cast of that expression reads. This borrows the factory - it never owns
 * or closes it - so it must not outlive it.
 *
 * @see RecordCursorFactory#isColumnIntWidthStable(int)
 */
public class FactoryColumnTypes implements ColumnTypes {
    private final RecordCursorFactory factory;
    private final RecordMetadata metadata;

    public FactoryColumnTypes(RecordCursorFactory factory) {
        this.factory = factory;
        this.metadata = factory.getMetadata();
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        return factory.isColumnIntWidthStable(columnIndex);
    }

    // Nothing reads this today - the row copiers only need the width flag - but the two answers are
    // a pair, and a ColumnTypes that forwards one while inheriting the other's default would report
    // a width-unstable column as row-unstable. See RecordCursorFactory#isColumnRowStable.
    @Override
    public boolean isColumnRowStable(int columnIndex) {
        return factory.isColumnRowStable(columnIndex);
    }
}
