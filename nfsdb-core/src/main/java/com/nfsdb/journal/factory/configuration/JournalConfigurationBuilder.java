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

package com.nfsdb.journal.factory.configuration;

import com.nfsdb.journal.exceptions.JournalConfigurationException;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JournalConfigurationBuilder {
    private final List<JMetadataBuilder> builders = new ArrayList<>();

    public <T> JournalMetadataBuilder<T> $(Class<T> clazz) {
        JournalMetadataBuilder<T> builder = new JournalMetadataBuilder<>(clazz);
        builders.add(builder);
        return builder;
    }

    public GenericJournalMetadataBuilder $(String location) {
        GenericJournalMetadataBuilder builder = new GenericJournalMetadataBuilder(location);
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


        Map<String, JournalMetadata> metadata = new HashMap<>(builders.size());
        for (int i = 0, sz = builders.size(); i < sz; i++) {
            JournalMetadata meta = builders.get(i).build();
            metadata.put(meta.getId(), meta);
        }
        return new JournalConfigurationImpl(journalBase, metadata);
    }
}
