/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

public abstract class AbstractGenericMetadataBuilder {
    protected final ColumnMetadata meta;
    private final JournalStructure parent;

    public AbstractGenericMetadataBuilder(JournalStructure parent, ColumnMetadata meta) {
        this.parent = parent;
        this.meta = meta;
    }

    public GenericStringBuilder $str(String name) {
        return parent.$str(name);
    }

    public GenericBinaryBuilder $bin(String name) {
        return parent.$bin(name);
    }

    public GenericSymbolBuilder $sym(String name) {
        return parent.$sym(name);
    }

    public GenericIntBuilder $int(String name) {
        return parent.$int(name);
    }

    public JournalStructure $ts(String name) {
        return parent.$ts(name);
    }

    public JournalStructure $ts() {
        return parent.$ts();
    }

    public JournalStructure $date(String name) {
        return parent.$date(name);
    }

    public JournalStructure $double(String name) {
        return parent.$double(name);
    }
}
