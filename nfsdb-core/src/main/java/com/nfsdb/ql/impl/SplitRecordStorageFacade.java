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

package com.nfsdb.ql.impl;

import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.SymbolTable;

public class SplitRecordStorageFacade implements StorageFacade {
    private final RecordMetadata metadata;
    private final int split;
    private JournalReaderFactory factory;
    private StorageFacade a;
    private StorageFacade b;

    public SplitRecordStorageFacade(RecordMetadata metadata, int split) {
        this.metadata = metadata;
        this.split = split;
    }

    @Override
    public JournalReaderFactory getFactory() {
        return factory;
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        return index < split ? a.getSymbolTable(index) : b.getSymbolTable(index - split);
    }

    @Override
    public SymbolTable getSymbolTable(String name) {
        return getSymbolTable(metadata.getColumnIndex(name));
    }

    public void prepare(JournalReaderFactory factory, StorageFacade a, StorageFacade b) {
        this.factory = factory;
        this.a = a;
        this.b = b;
    }
}
