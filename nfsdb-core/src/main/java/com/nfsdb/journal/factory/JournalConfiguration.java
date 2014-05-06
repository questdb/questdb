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
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.utils.Checksum;
import com.nfsdb.journal.utils.Files;

import javax.xml.bind.DatatypeConverter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
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
    public static final int VARCHAR_SHORT_HEADER_LENGTH = 1;
    public static final int VARCHAR_MEDIUM_HEADER_LENGTH = 2;
    public static final int VARCHAR_LARGE_HEADER_LENGTH = 4;

    public static final int DEFAULT_RECORD_HINT = 1000000;
    public static final int DEFAULT_STRING_AVG_SIZE = 12;
    public static final int DEFAULT_STRING_MAX_SIZE = 255;
    public static final int DEFAULT_SYMBOL_MAX_SIZE = 128;
    public static final int DEFAULT_DISTINCT_COUNT_HINT = 255;
    public static final int NULL_RECORD_HINT = 0;
    public static final int OPEN_PARTITION_TTL = 60; // seconds
    public static final int DEFAULT_LAG_HOURS = 0;
    private static final Logger LOGGER = Logger.getLogger(JournalConfiguration.class);
    private final Map<String, JournalMetadata> metadataMap = new HashMap<>();
    private final File journalBase;
    private final String configurationFile;
    private final int globalRecordHint;
    private boolean configured = false;
    private NullsAdaptorFactory nullsAdaptorFactory;

    public JournalConfiguration(File journalBase) {
        this(DEFAULT_CONFIG_FILE, journalBase);
    }

    public JournalConfiguration(String configurationFile, File journalBase) {
        this(configurationFile, journalBase, -1);
    }

    public JournalConfiguration(String configurationFile, File journalBase, int globalRecordHint) {
        this.globalRecordHint = globalRecordHint;
        this.journalBase = journalBase;
        this.configurationFile = configurationFile;
    }

    public JournalConfiguration setNullsAdaptorFactory(NullsAdaptorFactory nullsAdaptorFactory) {
        this.nullsAdaptorFactory = nullsAdaptorFactory;
        return this;
    }

    public JournalConfiguration build() throws JournalConfigurationException {
        if (!configured) {
            if (!this.journalBase.isDirectory()) {
                throw new JournalConfigurationException("Not a directory: " + journalBase);
            }

            if (!this.journalBase.canRead()) {
                throw new JournalConfigurationException("Not readable: " + journalBase);
            }

            InputStream is = JournalConfiguration.class.getResourceAsStream(configurationFile);
            if (is == null) {
                throw new JournalConfigurationException("Cannot load configuration: " + configurationFile);
            }
            try {
                metadataMap.putAll(parseConfiguration(is));
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
            String requestedChecksum = DatatypeConverter.printBase64Binary(Checksum.getChecksum(metadata));

            if (!existingChecksum.equals(requestedChecksum)) {
                throw new JournalException("Wrong metadata. Compare config on disk:\n\r" + metaStr + "\n\r with what you trying to use to open journal:\n\r" + metadata.toString() + "\n\rImportant fields are marked with *");
            }
        }

        return metadata;
    }

    public File getJournalBase() {
        return journalBase;
    }

    /////////////////////// PUBLIC API ///////////////////////////

    private Map<String, JournalMetadata> parseConfiguration(InputStream is) throws JournalConfigurationException {
        XMLInputFactory xmlif = XMLInputFactory.newInstance();
        try {
            XMLStreamReader xmlr = xmlif.createXMLStreamReader(is);
            try {
                return parseConfiguration(xmlr);
            } finally {
                xmlr.close();
            }
        } catch (XMLStreamException e) {
            throw new JournalConfigurationException("Cannot parse database configuration", e);
        } catch (JournalConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new JournalConfigurationException("exception throw in constructor", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, JournalMetadata> parseConfiguration(XMLStreamReader xmlr) throws XMLStreamException, JournalConfigurationException {
        JournalMetadata metadata = null;
        Map<String, JournalMetadata> metaMap = new HashMap<>();

        while (xmlr.hasNext()) {
            xmlr.next();

            // <journal>
            if (xmlr.isStartElement() && "journal".equals(xmlr.getLocalName())) {
                if (metadata != null) {
                    throw new JournalConfigurationException("Nested journal elements are not allowed");
                }

                String className = getStringAttr(xmlr, "class");
                try {
                    Class type = Class.forName(className);
                    metadata = new JournalMetadata(
                            type,
                            getStringAttr(xmlr, "defaultPath"),
                            getStringAttr(xmlr, "timestampColumn"),
                            PartitionType.valueOf(getStringAttr(xmlr, "partitionType")),
                            globalRecordHint == -1 ? getIntAttr(xmlr, "recordHint", DEFAULT_RECORD_HINT) : globalRecordHint,
                            getIntAttr(xmlr, "openPartitionTTL", OPEN_PARTITION_TTL),
                            getIntAttr(xmlr, "lagHours", DEFAULT_LAG_HOURS),
                            getStringAttr(xmlr, "key"),
                            nullsAdaptorFactory == null ? null : nullsAdaptorFactory.getInstance(type)
                    );
                } catch (ClassNotFoundException e) {
                    throw new JournalConfigurationException("Cannot load class: " + className, e);
                }

                metaMap.put(className, metadata);
                continue;
            }

            // <sym>
            if (xmlr.isStartElement() && "sym".equals(xmlr.getLocalName())) {
                if (metadata == null) {
                    throw new JournalConfigurationException("<sym> element must be a child of <journal>");
                }
                parseSymbol(xmlr, metadata);
                continue;
            }

            // <string>
            if (xmlr.isStartElement() && "string".equals(xmlr.getLocalName())) {
                if (metadata == null) {
                    throw new JournalConfigurationException("<string> element must be a child of <journal>");
                }
                JournalMetadata.ColumnMetadata ccm = metadata.getColumnMetadata(getStringAttr(xmlr, "name"));
                ccm.maxSize = getIntAttr(xmlr, "maxsize", DEFAULT_STRING_MAX_SIZE);
                ccm.avgSize = getIntAttr(xmlr, "avgsize", DEFAULT_STRING_AVG_SIZE);
            }

            // </journal>
            if (xmlr.isEndElement() && "journal".equals(xmlr.getLocalName())) {
                if (metadata == null) {
                    throw new JournalConfigurationException("Unbalanced </journal> element");
                }
                metadata.updateVariableSizes();
                LOGGER.debug("Loaded metadata for: " + metadata.getModelClass().getName());
                metadata = null;
            }
        }
        return metaMap;
    }

    private String getStringAttr(XMLStreamReader xmlr, String name) {
        return xmlr.getAttributeValue("", name);
    }

    private int getIntAttr(XMLStreamReader xmlr, String name, int defaultValue) {
        String s = getStringAttr(xmlr, name);
        return s == null || s.length() == 0 ? defaultValue : Integer.parseInt(s);
    }

    private void parseSymbol(XMLStreamReader xmlr, JournalMetadata metadata) throws JournalConfigurationException {
        String columnName = getStringAttr(xmlr, "name");
        JournalMetadata.ColumnMetadata ccm = metadata.getColumnMetadata(columnName);
        if (ccm.type != ColumnType.STRING) {
            throw new JournalConfigurationException("Column '" + columnName + "' is of type " + ccm.type + " and cannot be a symbol in class " + metadata.getModelClass().getName());
        }
        ccm.type = ColumnType.SYMBOL;
        ccm.indexed = "true".equals(xmlr.getAttributeValue("", "indexed"));
        ccm.maxSize = getIntAttr(xmlr, "maxsize", DEFAULT_SYMBOL_MAX_SIZE);
        ccm.distinctCountHint = getIntAttr(xmlr, "hintDistinctCount", DEFAULT_DISTINCT_COUNT_HINT);
        ccm.sameAs = getStringAttr(xmlr, "sameAs");
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

