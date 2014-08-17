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
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.Base64;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Files;

import java.io.File;
import java.util.Map;

public class JournalConfigurationImpl implements JournalConfiguration {

    public static final int NULL_RECORD_HINT = 0;
    public static final String JOURNAL_META_FILE = "_meta";
    private final Map<String, JournalMetadata> journalMetadata;
    private final File journalBase;

    public JournalConfigurationImpl(File journalBase, Map<String, JournalMetadata> journalMetadata) {
        this.journalBase = journalBase;
        this.journalMetadata = journalMetadata;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> JournalMetadata<T> createMetadata(JournalKey<T> key) {

        boolean newMeta = false;
        JournalMetadata<T> meta = journalMetadata.get(key.getModelClassName());
        JournalMetadataBuilder<T> builder;
        if (meta == null) {
            builder = new JournalMetadataBuilder<>(key.getModelClass());
            newMeta = true;
        } else {
            builder = new JournalMetadataBuilder<>(meta);
        }

        if (meta != null && meta.getLocation() == null && key.getLocation() == null) {
            throw new JournalRuntimeException("There is no defaultPath for %s", key.getModelClassName());
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
            journalMetadata.put(m.getModelClass().getName(), m);
        }

        builder.location(journalLocation.getAbsolutePath());

        JournalMetadata<T> result = builder.build();

        File f = new File(journalLocation, JOURNAL_META_FILE);
        if (f.exists()) {
            try {
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
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }

        return result;
    }

    @Override
    public File getJournalBase() {
        return journalBase;
    }
}
