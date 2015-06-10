/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.factory.configuration;

public abstract class AbstractMetadataBuilder<T> {
    final ColumnMetadata meta;
    private final JournalMetadataBuilder<T> parent;

    AbstractMetadataBuilder(JournalMetadataBuilder<T> parent, ColumnMetadata meta) {
        this.parent = parent;
        this.meta = meta;
    }

    public BinaryBuilder $bin(String name) {
        return parent.$bin(name);
    }

    public JournalMetadataBuilder<T> $date(String name) {
        return parent.$date(name);
    }

    public IntBuilder $int(String name) {
        return parent.$int(name);
    }

    public StringBuilder $str(String name) {
        return parent.$str(name);
    }

    public SymbolBuilder $sym(String name) {
        return parent.$sym(name);
    }

    public JournalMetadataBuilder<T> $ts() {
        return parent.$ts();
    }

    public JournalMetadataBuilder<T> $ts(String name) {
        return parent.$ts(name);
    }
}
