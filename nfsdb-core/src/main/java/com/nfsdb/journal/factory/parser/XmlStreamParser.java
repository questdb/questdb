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

package com.nfsdb.journal.factory.parser;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.factory.JournalMetadata;
import com.nfsdb.journal.logging.Logger;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class XmlStreamParser implements XmlParser {

    private static final Logger LOGGER = Logger.getLogger(XmlStreamParser.class);

    private final ParserDefaults defaults;

    public XmlStreamParser(ParserDefaults defaults) {
        this.defaults = defaults;
    }

    @Override
    public Map<String, JournalMetadata> parse(InputStream is) throws JournalConfigurationException {
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
                    int recordHint = defaults.getGlobalRecordHint() == -1 ? getIntAttr(xmlr, "recordCountHint", defaults.getRecordHint()) : defaults.getGlobalRecordHint();
                    metadata = new JournalMetadata(
                            type,
                            getStringAttr(xmlr, "defaultPath"),
                            getStringAttr(xmlr, "timestampColumn"),
                            PartitionType.valueOf(getStringAttr(xmlr, "partitionType")),
                            recordHint,
                            getIntAttr(xmlr, "txCountHint", (recordHint) / 100),
                            getIntAttr(xmlr, "openPartitionTTL", defaults.getOpenPartitionTTL()),
                            getIntAttr(xmlr, "lagHours", defaults.getLagHours()),
                            getStringAttr(xmlr, "key"),
                            defaults.getNullsAdaptorFactory() == null ? null : defaults.getNullsAdaptorFactory().getInstance(type)
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
            if (xmlr.isStartElement() && ("string".equals(xmlr.getLocalName()))) {
                if (metadata == null) {
                    throw new JournalConfigurationException("<string> element must be a child of <journal>");
                }
                JournalMetadata.ColumnMetadata ccm = metadata.getColumnMetadata(getStringAttr(xmlr, "name"));
                ccm.avgSize = getIntAttr(xmlr, "avgsize", defaults.getStringAvgSize());
                ccm.indexed = "true".equals(getStringAttr(xmlr, "indexed"));
                ccm.distinctCountHint = getIntAttr(xmlr, "hintDistinctCount", metadata.getRecordHint() / 2);
                if (ccm.indexed && ccm.distinctCountHint <= 1) {
                    throw new JournalConfigurationException("hintDistinctCount for " + metadata.getModelClass().getName() + "." + ccm.name + " must be > 1 for index to make sense. Either review hintDistinctCount or set indexed=\"false\"");
                }
                continue;
            }

            // <binary>
            if (xmlr.isStartElement() && ("binary".equals(xmlr.getLocalName()))) {
                if (metadata == null) {
                    throw new JournalConfigurationException("<binary> element must be a child of <journal>");
                }
                JournalMetadata.ColumnMetadata ccm = metadata.getColumnMetadata(getStringAttr(xmlr, "name"));
                ccm.size = getIntAttr(xmlr, "avgsize", defaults.getStringAvgSize());
                ccm.avgSize = ccm.size;
                continue;
            }

            // <index>
            if (xmlr.isStartElement() && ("index".equals(xmlr.getLocalName()))) {
                if (metadata == null) {
                    throw new JournalConfigurationException("<index> element must be a child of <journal>");
                }
                JournalMetadata.ColumnMetadata ccm = metadata.getColumnMetadata(getStringAttr(xmlr, "name"));
                ccm.distinctCountHint = getIntAttr(xmlr, "hintDistinctCount", metadata.getRecordHint() / 2);
                ccm.indexed = true;
                continue;
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
            throw new JournalConfigurationException("Column '%s' is of type %s and cannot be a symbol in class %s", columnName, ccm.type, metadata.getModelClass().getName());
        }
        ccm.type = ColumnType.SYMBOL;
        ccm.indexed = "true".equals(getStringAttr(xmlr, "indexed"));
        ccm.distinctCountHint = getIntAttr(xmlr, "hintDistinctCount", metadata.getRecordHint() / 2);
        if (ccm.indexed && ccm.distinctCountHint <= 1) {
            throw new JournalConfigurationException("hintDistinctCount for " + metadata.getModelClass().getName() + "." + columnName + " must be > 1 for index to make sense. Either review hintDistinctCount or set indexed=\"false\"");
        }
        ccm.sameAs = getStringAttr(xmlr, "sameAs");
    }
}
