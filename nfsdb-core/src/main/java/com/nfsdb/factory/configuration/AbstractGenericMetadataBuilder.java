/*
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
 */

package com.nfsdb.factory.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractGenericMetadataBuilder {
    protected final ColumnMetadata meta;
    protected final JournalStructure parent;

    @SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY"})
    public AbstractGenericMetadataBuilder(JournalStructure parent, ColumnMetadata meta) {
        this.parent = parent;
        this.meta = meta;
    }

    public JournalStructure $() {
        return parent;
    }

    public GenericBinaryBuilder $bin(String name) {
        return parent.$bin(name);
    }

    public JournalStructure $date(String name) {
        return parent.$date(name);
    }

    public JournalStructure $double(String name) {
        return parent.$double(name);
    }

    public GenericIntBuilder $int(String name) {
        return parent.$int(name);
    }

    public JournalStructure $long(String name) {
        return parent.$long(name);
    }

    public GenericStringBuilder $str(String name) {
        return parent.$str(name);
    }

    public GenericSymbolBuilder $sym(String name) {
        return parent.$sym(name);
    }

    public JournalStructure $ts(String name) {
        return parent.$ts(name);
    }

    public JournalStructure $ts() {
        return parent.$ts();
    }
}
