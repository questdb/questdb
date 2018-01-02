/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store.factory.configuration;

import com.questdb.ex.JournalDoesNotExistException;
import com.questdb.ex.JournalMetadataException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Files;
import com.questdb.std.Misc;
import com.questdb.std.ObjObjHashMap;
import com.questdb.std.ThreadLocal;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.Path;
import com.questdb.store.JournalKey;
import com.questdb.store.JournalMode;
import com.questdb.store.TxLog;
import com.questdb.store.UnstructuredFile;

import java.io.File;

class JournalConfigurationImpl implements JournalConfiguration {

    private static final Log LOG = LogFactory.getLog(JournalConfigurationImpl.class);

    private final static ThreadLocal<Path> tlPath = new ThreadLocal<>(Path::new);
    private final ObjObjHashMap<String, JournalMetadata> journalMetadata;
    private final File journalBase;

    JournalConfigurationImpl(File journalBase, ObjObjHashMap<String, JournalMetadata> journalMetadata) {
        this.journalBase = journalBase;
        this.journalMetadata = journalMetadata;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> JournalMetadata<T> createMetadata(JournalKey<T> key) throws JournalException {
        JournalMetadata<T> mo = readMetadata(key.getName());
        String className = key.getModelClassName();
        JournalMetadata<T> mn = className == null ? null : journalMetadata.get(className);

        if (mo == null) {
            // no existing journal
            // can create one on either of two conditions:
            // 1. we have new metadata to create journal from
            // 2. key represents a class that can be introspected

            if (mn == null && key.getModelClass() == null) {
                LOG.error().$("Journal does not exist: ").$(key.getName()).$();
                throw JournalDoesNotExistException.INSTANCE;
            }

            if (mn == null) {
                return new JournalMetadataBuilder<>(key.getModelClass(), key.getName()).partitionBy(key.getPartitionBy()).
                        recordCountHint(key.getRecordHint()).
                        ordered(key.isOrdered()).
                        build();
            }
            return (new JournalStructure(mn, key.getName()).
                    partitionBy(key.getPartitionBy()).
                    recordCountHint(key.getRecordHint()).
                    ordered(key.isOrdered()).
                    map(key.getModelClass()));
        } else {
            if (mn == null || mn.isCompatible(mo, false)) {
                return new JournalStructure(mo, key.getName()).recordCountHint(key.getRecordHint()).map(key.getModelClass());
            }
            throw new JournalMetadataException(mo, mn);
        }
    }

    public int exists(CharSequence location) {
        Path path = tlPath.get();
        String base = getJournalBase().getAbsolutePath();

        if (!Files.exists(path.of(base).concat(location).$())) {
            return JournalConfiguration.DOES_NOT_EXIST;
        }

        if (Files.exists(path.of(base).concat(location).concat(TxLog.FILE_NAME).$())
                && Files.exists(path.of(base).concat(location).concat(JournalConfiguration.FILE_NAME).$())) {
            return JournalConfiguration.EXISTS;
        }

        return JournalConfiguration.EXISTS_FOREIGN;
    }

    @Override
    public File getJournalBase() {
        return journalBase;
    }

    public <T> JournalMetadata<T> readMetadata(String name) throws JournalException {
        File dir = new File(getJournalBase(), name);
        if (dir.exists()) {
            File metaFile = new File(dir, FILE_NAME);
            if (!metaFile.exists()) {
                throw new JournalException(dir + " is not a recognised journal");
            }

            try (UnstructuredFile hb = new UnstructuredFile(metaFile, 12, JournalMode.READ)) {
                return new JournalMetadata<>(hb, name);
            }
        }
        return null;
    }

    @Override
    public void releaseThreadLocals() {
        Misc.free(tlPath.get());
        tlPath.remove();
    }
}
