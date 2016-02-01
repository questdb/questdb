/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.select;

import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.SymbolTable;

public class SelectedColumnsStorageFacade implements StorageFacade {

    private final int reindex[];
    private final RecordMetadata metadata;
    private StorageFacade delegate;

    public SelectedColumnsStorageFacade(RecordMetadata parentMetadata, RecordMetadata metadata, ObjList<CharSequence> names) {
        this.metadata = metadata;
        int k = names.size();
        this.reindex = new int[k];

        for (int i = 0; i < k; i++) {
            reindex[i] = parentMetadata.getColumnIndex(names.getQuick(i));
        }
    }

    @Override
    public JournalReaderFactory getFactory() {
        return delegate.getFactory();
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        return delegate.getSymbolTable(reindex[index]);
    }

    @Override
    public SymbolTable getSymbolTable(String name) {
        return getSymbolTable(metadata.getColumnIndex(name));
    }

    public void of(StorageFacade delegate) {
        this.delegate = delegate;
    }
}
