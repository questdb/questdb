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

package com.questdb.factory.configuration;

import com.questdb.ex.JournalConfigurationException;
import com.questdb.std.ObjObjHashMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class JournalConfigurationBuilder {
    private final List<MetadataBuilder> builders = new ArrayList<>();

    public <T> JournalMetadataBuilder<T> $(Class<T> clazz) {
        JournalMetadataBuilder<T> builder = new JournalMetadataBuilder<>(clazz);
        builders.add(builder);
        return builder;
    }

    public <T> JournalMetadataBuilder<T> $(Class<T> clazz, String name) {
        JournalMetadataBuilder<T> builder = new JournalMetadataBuilder<>(clazz, name);
        builders.add(builder);
        return builder;
    }

    public JournalStructure $(String name) {
        JournalStructure builder = new JournalStructure(name);
        builders.add(builder);
        return builder;
    }

    public JournalConfiguration build(String journalBase) {
        return build(new File(journalBase));
    }

    public JournalConfiguration build(File journalBase) {
        if (!journalBase.isDirectory()) {
            throw new JournalConfigurationException("Not a directory: %s", journalBase);
        }

        if (!journalBase.canRead()) {
            throw new JournalConfigurationException("Not readable: %s", journalBase);
        }


        ObjObjHashMap<String, JournalMetadata> metadata = new ObjObjHashMap<>(builders.size());
        for (int i = 0, sz = builders.size(); i < sz; i++) {
            JournalMetadata meta = builders.get(i).build();
            metadata.put(meta.getModelClassName(), meta);
        }
        return new JournalConfigurationImpl(journalBase, metadata);
    }
}
