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

package com.nfsdb.journal.factory;

import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.parser.ParserDefaults;
import com.nfsdb.journal.factory.parser.XmlParser;
import com.nfsdb.journal.factory.parser.XmlStreamParser;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.utils.Base64;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Files;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class JournalConfiguration {
    public static final String TEMP_DIRECTORY_PREFIX = "temp";
    public static final String DEFAULT_CONFIG_FILE = "/nfsdb.xml";
    public static final String JOURNAL_META_FILE = "_meta";
    public static final int PIPE_BIT_HINT = 16;
    public static final int VARCHAR_INDEX_COLUMN_WIDTH = 8;
    public static final int VARCHAR_LARGE_HEADER_LENGTH = 4;
    public static final int DEFAULT_RECORD_HINT = 1000000;
    public static final int DEFAULT_STRING_AVG_SIZE = 12;
    public static final int DEFAULT_STRING_MAX_SIZE = 255;
    public static final int DEFAULT_SYMBOL_MAX_SIZE = 128;
    public static final int DEFAULT_DISTINCT_COUNT_HINT = 1;
    public static final int NULL_RECORD_HINT = 0;
    public static final int OPEN_PARTITION_TTL = 60; // seconds
    public static final int DEFAULT_LAG_HOURS = 0;
    private static final Logger LOGGER = Logger.getLogger(JournalConfiguration.class);
    private final Map<String, JournalMetadata> metadataMap = new HashMap<>();
    private final File journalBase;
    private final String configurationFile;
    private final ParserDefaults parserDefaults;
    private boolean configured = false;


    public JournalConfiguration(File journalBase) {
        this(DEFAULT_CONFIG_FILE, journalBase);
    }

    public JournalConfiguration(String configurationFile, File journalBase) {
        this(configurationFile, journalBase, -1);
    }

    public JournalConfiguration(String configurationFile, File journalBase, int globalRecordHint) {
        this(configurationFile, journalBase, globalRecordHint, null);
    }

    public JournalConfiguration(String configurationFile, File journalBase, int globalRecordHint, NullsAdaptorFactory nullsAdaptorFactory) {
        this.journalBase = journalBase;
        this.configurationFile = configurationFile;
        this.parserDefaults = new ParserDefaults();
        this.parserDefaults.setNullsAdaptorFactory(nullsAdaptorFactory);
        this.parserDefaults.setDistinctCount(DEFAULT_DISTINCT_COUNT_HINT);
        this.parserDefaults.setGlobalRecordHint(globalRecordHint);
        this.parserDefaults.setLagHours(DEFAULT_LAG_HOURS);
        this.parserDefaults.setOpenPartitionTTL(OPEN_PARTITION_TTL);
        this.parserDefaults.setRecordHint(DEFAULT_RECORD_HINT);
        this.parserDefaults.setStringAvgSize(DEFAULT_STRING_AVG_SIZE);
        this.parserDefaults.setStringMaxSize(DEFAULT_STRING_MAX_SIZE);
        this.parserDefaults.setSymbolMaxSize(DEFAULT_SYMBOL_MAX_SIZE);
    }

    public JournalConfiguration build() throws JournalConfigurationException {
        if (!configured) {
            if (!this.journalBase.isDirectory()) {
                throw new JournalConfigurationException("Not a directory: %s", journalBase);
            }

            if (!this.journalBase.canRead()) {
                throw new JournalConfigurationException("Not readable: %s", journalBase);
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Using: %s", JournalConfiguration.class.getResource(configurationFile));
            }
            InputStream is = JournalConfiguration.class.getResourceAsStream(configurationFile);
            if (is == null) {
                throw new JournalConfigurationException("Cannot load configuration: %s", configurationFile);
            }
            try {
                XmlParser parser = new XmlStreamParser(parserDefaults);
                metadataMap.putAll(parser.parse(is));
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    //
                }
            }
            configured = true;
        }
        return this;
    }

    public <T> JournalMetadata<T> getMetadata(JournalKey<T> key) throws JournalException {

        checkConfigured();

        JournalMetadata<T> metadata = new JournalMetadata<>(getMetadataInternal(key));
        if (key.getPartitionType() != null) {
            switch (key.getPartitionType()) {
                case NONE:
                case DAY:
                case MONTH:
                case YEAR:
                    metadata.setPartitionType(key.getPartitionType());
                    break;
                case DEFAULT:
                    break;
            }
        }

        if (key.getRecordHint() > NULL_RECORD_HINT) {
            metadata.setRecordHint(key.getRecordHint());
        }

        if (metadata.getLocation() == null && key.getLocation() == null) {
            throw new JournalException("There is no defaultPath for %s", key.getClazz());
        }


        File journalLocation;
        if (key.getLocation() != null) {
            journalLocation = new File(getJournalBase(), key.getLocation());
        } else {
            journalLocation = new File(getJournalBase(), metadata.getLocation());
        }

        metadata.setLocation(journalLocation.getAbsolutePath());

        File meta = new File(journalLocation, JOURNAL_META_FILE);
        if (meta.exists()) {
            String metaStr = Files.readStringFromFile(meta);
            String pattern = "SHA='";
            int lo = metaStr.indexOf(pattern);
            if (lo == -1) {
                throw new JournalException("Cannot find journal metadata checksum. Corrupt journal?");
            }
            int hi = metaStr.indexOf("'", lo + pattern.length());
            if (hi == -1) {
                throw new JournalException("Cannot find journal metadata checksum. Corrupt journal?");
            }
            String existingChecksum = metaStr.substring(lo + pattern.length(), hi);
            String requestedChecksum = Base64._printBase64Binary(Checksum.getChecksum(metadata));

            if (!existingChecksum.equals(requestedChecksum)) {
                throw new JournalException("Wrong metadata. Compare config on disk:\n\r" + metaStr + "\n\r with what you trying to use to open journal:\n\r" + metadata.toString() + "\n\rImportant fields are marked with *");
            }
        }

        return metadata;
    }

    public File getJournalBase() {
        return journalBase;
    }

    private void checkConfigured() {
        if (!configured) {
            throw new JournalRuntimeException("Not configured: %s", this);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> JournalMetadata<T> getMetadataInternal(JournalKey<T> key) {
        JournalMetadata<T> metadata = metadataMap.get(key.getClazz());
        if (metadata == null) {
            throw new JournalRuntimeException("There is no meta data for %s", key.getClazz());
        }
        return metadata;
    }
}

