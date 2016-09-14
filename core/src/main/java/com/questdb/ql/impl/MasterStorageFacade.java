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

package com.questdb.ql.impl;

import com.questdb.Journal;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.StorageFacade;
import com.questdb.store.SymbolTable;

public class MasterStorageFacade implements StorageFacade {
    private RecordMetadata metadata;
    private JournalReaderFactory factory;

    @Override
    public JournalReaderFactory getFactory() {
        return factory;
    }

    @Override
    public SymbolTable getSymbolTable(int index) {
        // do not call journal.getSymbolTable() because it uses different indexing system
        return metadata.getColumnQuick(index).getSymbolTable();
    }

    public void setFactory(JournalReaderFactory factory) {
        this.factory = factory;
    }

    public void setJournal(Journal journal) {
        this.metadata = journal.getMetadata();
    }
}
