/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.select;

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.StorageFacade;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import com.questdb.store.MMappedSymbolTable;

public class SelectedColumnsStorageFacade implements StorageFacade {

    private final int reindex[];
    private StorageFacade delegate;

    public SelectedColumnsStorageFacade(RecordMetadata parentMetadata, @Transient ObjList<CharSequence> names) {
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
    public MMappedSymbolTable getSymbolTable(int index) {
        return delegate.getSymbolTable(reindex[index]);
    }


    public void of(StorageFacade delegate) {
        this.delegate = delegate;
    }
}
