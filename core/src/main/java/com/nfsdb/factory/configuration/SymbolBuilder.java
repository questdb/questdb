/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.factory.configuration;

import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.misc.Numbers;
import com.nfsdb.storage.ColumnType;

public class SymbolBuilder<T> extends AbstractMetadataBuilder<T> {
    public SymbolBuilder(JournalMetadataBuilder<T> parent, ColumnMetadata meta) {
        super(parent, meta);
        if (meta.type != ColumnType.STRING && meta.type != ColumnType.SYMBOL) {
            throw new JournalConfigurationException("Invalid column type for symbol: %s", meta.name);
        }
        meta.type = ColumnType.SYMBOL;
        meta.size = 4;
    }

    public SymbolBuilder<T> index() {
        this.meta.indexed = true;
        return this;
    }

    public SymbolBuilder<T> noCache() {
        this.meta.noCache = true;
        return this;
    }

    public SymbolBuilder<T> sameAs(String name) {
        this.meta.sameAs = name;
        return this;
    }

    public SymbolBuilder<T> size(int size) {
        this.meta.avgSize = size;
        return this;
    }

    public SymbolBuilder<T> valueCountHint(int valueCountHint) {
        this.meta.distinctCountHint = Numbers.ceilPow2(valueCountHint) - 1;
        return this;
    }
}
