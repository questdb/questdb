/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.factory.configuration;

import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;

public class SymbolBuilder<T> extends AbstractMetadataBuilder<T> {
    public SymbolBuilder(JournalMetadataBuilder<T> parent, ColumnMetadata meta) {
        super(parent, meta);
        if (meta.type != ColumnType.STRING && meta.type != ColumnType.SYMBOL) {
            throw new JournalConfigurationException("Invalid column type for symbol: %s", meta.name);
        }
        meta.type = ColumnType.SYMBOL;
        meta.size = 4;
    }

    public SymbolBuilder<T> sameAs(String name) {
        this.meta.sameAs = name;
        return this;
    }

    public SymbolBuilder<T> valueCountHint(int valueCountHint) {
        this.meta.distinctCountHint = valueCountHint;
        return this;
    }

    public SymbolBuilder<T> size(int size) {
        this.meta.avgSize = size;
        return this;
    }

    public SymbolBuilder<T> index() {
        this.meta.indexed = true;
        return this;
    }
}
