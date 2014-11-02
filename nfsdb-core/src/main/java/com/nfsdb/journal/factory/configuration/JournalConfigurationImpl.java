/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.JournalMode;
import com.nfsdb.journal.column.HugeBuffer;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.Base64;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Files;

import java.io.File;
import java.util.Map;

public class JournalConfigurationImpl implements JournalConfiguration {

    public static final int NULL_RECORD_HINT = 0;
    private final Map<String, JournalMetadata> journalMetadata;
    private final File journalBase;

    public JournalConfigurationImpl(File journalBase, Map<String, JournalMetadata> journalMetadata) {
        this.journalBase = journalBase;
        this.journalMetadata = journalMetadata;
    }

    private String getLocation(JournalKey key) {
        if (key.getLocation() != null) {
            return key.getLocation();
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> JournalMetadata<T> createMetadata(JournalKey<T> key) throws JournalException {
        File journalLocation = new File(getJournalBase(), getLocation(key));
        JournalMetadata<T> mo = null;

        if (journalLocation.exists()) {
            File metaFile = new File(journalLocation, "_meta2");
            if (!metaFile.exists()) {
                // todo: read old meta file for compatibility
                throw new JournalException(journalLocation + " is not a recognised journal");
            }

            try (HugeBuffer hb = new HugeBuffer(metaFile, 12, JournalMode.READ)) {
                mo = new JournalMetadataImpl<>(hb);
            }
        }

        JournalMetadata<T> mn = journalMetadata.get(key.getId());

        if (mo == null) {
            // no existing journal
            // can create one on either of two conditions:
            // 1. we have new metadata to create journal from
            // 2. key represents a class that can be introspected

            if (mn == null && key.getModelClass() == null) {
                throw new JournalException("There is not enough information to create journal: " + key.getId());
            }

            JMetadataBuilder<T> builder;

            if (mn == null) {
                builder = new JournalMetadataBuilder<>(key.getModelClass());
            } else {
                if (key.getModelClass() == null) {
                    builder = new GenericJournalMetadataBuilder<>(mn);
                } else {
                    builder = new JournalMetadataBuilder<>(mn);
                }
            }
            return apply(builder, key, journalLocation).build();
        } else {
            // journal exists on disk
            if (mn == null) {
                // we have on-disk metadata and no in-app meta
                if (key.getModelClass() == null) {
                    // if this is generic access request
                    // return metadata as is, nothing more to do
                    return new GenericJournalMetadataBuilder<T>(mo).location(journalLocation.getAbsolutePath()).build();
                }
                // if this is request to map class on existing journal
                // check compatibility and map to class (calc offsets and constructor)
                return new GenericJournalMetadataBuilder<T>(mo).location(journalLocation.getAbsolutePath()).map(key.getModelClass());
            }

            // we have both on-disk and in-app meta
            // check if in-app meta matches on-disk meta
            if (eq(Checksum.getChecksum(mo), Checksum.getChecksum(mn))) {
                if (mn.getModelClass() == null) {
                    return new GenericJournalMetadataBuilder<T>(mn).location(journalLocation.getAbsolutePath()).build();
                }
                return new JournalMetadataBuilder<>(mn).location(journalLocation.getAbsolutePath()).build();
            }

            throw new JournalException("Checksum mismatch");
        }

    }

    private boolean eq(byte[] expected, byte[] actual) {
        if (expected.length != actual.length) {
            return false;
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                return false;
            }
        }

        return true;
    }

    private <T> JMetadataBuilder<T> apply(JMetadataBuilder<T> builder, JournalKey<T> key, File journalLocation) {
        if (key.getPartitionType() != null) {
            switch (key.getPartitionType()) {
                case NONE:
                case DAY:
                case MONTH:
                case YEAR:
                    builder.partitionBy(key.getPartitionType());
                    break;
                case DEFAULT:
                    break;
            }
        }

        if (key.getRecordHint() > NULL_RECORD_HINT) {
            builder.recordCountHint(key.getRecordHint());
        }

        return builder.location(journalLocation.getAbsolutePath());
    }


    @SuppressWarnings("unchecked")
    public <T> JournalMetadata<T> createMetadata2(JournalKey<T> key) throws JournalException {

        boolean newMeta = false;
        JournalMetadata<T> meta = journalMetadata.get(key.getId());
        JMetadataBuilder<T> builder;
        if (meta == null) {
            if (key.getModelClass() != null) {
                builder = new JournalMetadataBuilder<>(key.getModelClass());
            } else {
                builder = new GenericJournalMetadataBuilder(key.getId());
            }
            newMeta = true;
        } else {
            if (meta.getModelClass() != null) {
                builder = new JournalMetadataBuilder<>(meta);
            } else {
                builder = new GenericJournalMetadataBuilder(meta);
            }
        }

        if (meta != null && meta.getLocation() == null && key.getLocation() == null) {
            throw new JournalException("There is no defaultPath for %s", key.getId());
        }

        if (key.getPartitionType() != null) {
            switch (key.getPartitionType()) {
                case NONE:
                case DAY:
                case MONTH:
                case YEAR:
                    builder.partitionBy(key.getPartitionType());
                    break;
                case DEFAULT:
                    break;
            }
        }

        if (key.getRecordHint() > NULL_RECORD_HINT) {
            builder.recordCountHint(key.getRecordHint());
        }

        File journalLocation;
        if (key.getLocation() != null) {
            journalLocation = new File(getJournalBase(), key.getLocation());
        } else {
            journalLocation = new File(getJournalBase(), builder.getLocation());
        }

        if (newMeta) {
            JournalMetadata<T> m = builder.build();
            journalMetadata.put(key.getId(), m);
        }

        builder.location(journalLocation.getAbsolutePath());

        JournalMetadata<T> result = builder.build();

        File f = new File(journalLocation, Constants.JOURNAL_META_FILE);
        if (f.exists()) {
            String metaStr = Files.readStringFromFile(f);
            String pattern = "SHA='";
            int lo = metaStr.indexOf(pattern);
            if (lo == -1) {
                throw new JournalRuntimeException("Cannot find journal metadata checksum. Corrupt journal?");
            }
            int hi = metaStr.indexOf("'", lo + pattern.length());
            if (hi == -1) {
                throw new JournalRuntimeException("Cannot find journal metadata checksum. Corrupt journal?");
            }
            String existingChecksum = metaStr.substring(lo + pattern.length(), hi);
            String requestedChecksum = Base64._printBase64Binary(Checksum.getChecksum(result));

            if (!existingChecksum.equals(requestedChecksum)) {
                throw new JournalRuntimeException("Wrong metadata. Compare config on disk:\n\r" + metaStr + "\n\r with what you trying to use to open journal:\n\r" + result.toString() + "\n\rImportant fields are marked with *");
            }
        }

        return result;
    }

    @Override
    public File getJournalBase() {
        return journalBase;
    }
}
