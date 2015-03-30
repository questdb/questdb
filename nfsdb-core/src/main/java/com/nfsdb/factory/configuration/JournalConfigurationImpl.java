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

import com.nfsdb.JournalKey;
import com.nfsdb.JournalMode;
import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalMetadataException;
import com.nfsdb.storage.HugeBuffer;

import java.io.File;

public class JournalConfigurationImpl implements JournalConfiguration {

    private final ObjObjHashMap<String, JournalMetadata> journalMetadata;
    private final File journalBase;

    public JournalConfigurationImpl(File journalBase, ObjObjHashMap<String, JournalMetadata> journalMetadata) {
        this.journalBase = journalBase;
        this.journalMetadata = journalMetadata;
    }

    public <T> JournalMetadata<T> augmentMetadata(MetadataBuilder<T> builder) throws JournalException {
        File journalLocation = new File(getJournalBase(), builder.getLocation());

        JournalMetadata<T> mo = readMetadata(journalLocation);
        JournalMetadata<T> mn = builder.location(journalLocation).build();

//        if (mo == null || Checksum.eq(Checksum.getChecksum(mo), Checksum.getChecksum(mn))) {
        if (mo == null || mn.isCompatible(mo)) {
            return mn;
        }

        throw new JournalMetadataException(mo, mn);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> JournalMetadata<T> createMetadata(JournalKey<T> key) throws JournalException {
        File journalLocation = new File(getJournalBase(), getLocation(key));

        JournalMetadata<T> mo = readMetadata(journalLocation);
        JournalMetadata<T> mn = journalMetadata.get(key.getId());

        if (mo == null) {
            // no existing journal
            // can create one on either of two conditions:
            // 1. we have new metadata to create journal from
            // 2. key represents a class that can be introspected

            if (mn == null && key.getModelClass() == null) {
                throw new JournalException("There is not enough information to create journal: " + key.getId());
            }

            MetadataBuilder<T> builder;

            if (mn == null) {
                builder = new JournalMetadataBuilder<>(key.getModelClass());
            } else {
                if (key.getModelClass() == null) {
                    builder = (MetadataBuilder<T>) new JournalStructure(mn);
                } else {
                    builder = new JournalMetadataBuilder<>(mn);
                }
            }
            return builder.partitionBy(key.getPartitionType()).recordCountHint(key.getRecordHint()).location(journalLocation).build();
        } else {
            // journal exists on disk
            if (mn == null) {
                // we have on-disk metadata and no in-app meta
                if (key.getModelClass() == null) {
                    // if this is generic access request
                    // return metadata as is, nothing more to do
                    return (JournalMetadata<T>) new JournalStructure(mo).location(journalLocation).recordCountHint(key.getRecordHint()).build();
                }
                // if this is request to map class on existing journal
                // check compatibility and map to class (calc offsets and constructor)
                return new JournalStructure(mo).location(journalLocation).recordCountHint(key.getRecordHint()).map(key.getModelClass());
            }

            // we have both on-disk and in-app meta
            // check if in-app meta matches on-disk meta
            if (mn.isCompatible(mo)) {
                if (mn.getModelClass() == null) {
                    return (JournalMetadata<T>) new JournalStructure(mn).recordCountHint(key.getRecordHint()).location(journalLocation).build();
                }
                return new JournalMetadataBuilder<>(mn).location(journalLocation).recordCountHint(key.getRecordHint()).build();
            }

            throw new JournalMetadataException(mo, mn);
        }
    }

    @Override
    public File getJournalBase() {
        return journalBase;
    }

    private String getLocation(JournalKey key) {
        String loc = key.getLocation();
        if (loc != null) {
            return loc;
        }

        JournalMetadata m = journalMetadata.get(key.getId());
        if (m == null) {
            if (key.getModelClass() == null) {
                return key.getId();
            }

            return key.getModelClass().getName();
        }

        return m.getLocation();
    }

    private <T> JournalMetadata<T> readMetadata(File location) throws JournalException {
        if (location.exists()) {
            File metaFile = new File(location, FILE_NAME);
            if (!metaFile.exists()) {
                // todo: read old meta file for compatibility
                throw new JournalException(location + " is not a recognised journal");
            }

            try (HugeBuffer hb = new HugeBuffer(metaFile, 12, JournalMode.READ)) {
                return new JournalMetadata<>(hb);
            }
        }
        return null;
    }
}
