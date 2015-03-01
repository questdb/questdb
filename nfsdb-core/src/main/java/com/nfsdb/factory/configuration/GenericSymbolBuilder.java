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

package com.nfsdb.factory.configuration;

import com.nfsdb.storage.ColumnType;

public class GenericSymbolBuilder extends AbstractGenericMetadataBuilder {
    public GenericSymbolBuilder(JournalStructure parent, ColumnMetadata meta) {
        super(parent, meta);
        meta.type = ColumnType.SYMBOL;
        meta.size = 4;
    }

    public GenericSymbolBuilder index() {
        this.meta.indexed = true;
        return this;
    }

    public GenericSymbolBuilder noCache() {
        this.meta.noCache = true;
        return this;
    }

    public GenericSymbolBuilder sameAs(String name) {
        this.meta.sameAs = name;
        return this;
    }

    public GenericSymbolBuilder size(int size) {
        this.meta.avgSize = size;
        return this;
    }

    public GenericSymbolBuilder valueCountHint(int valueCountHint) {
        this.meta.distinctCountHint = valueCountHint;
        return this;
    }
}
