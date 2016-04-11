/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.factory.configuration;

import com.nfsdb.JournalKey;
import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalMetadataException;
import com.nfsdb.misc.Files;
import com.nfsdb.std.CompositePath;
import com.nfsdb.std.ObjObjHashMap;
import com.nfsdb.std.ThreadLocal;
import com.nfsdb.store.TxLog;
import com.nfsdb.store.UnstructuredFile;

import javax.annotation.concurrent.Immutable;
import java.io.File;

@Immutable
class JournalConfigurationImpl implements JournalConfiguration {

    private final static ThreadLocal<CompositePath> tlPath = new ThreadLocal<>(CompositePath.FACTORY);
    private final ObjObjHashMap<String, JournalMetadata> journalMetadata;
    private final File journalBase;

    JournalConfigurationImpl(File journalBase, ObjObjHashMap<String, JournalMetadata> journalMetadata) {
        this.journalBase = journalBase;
        this.journalMetadata = journalMetadata;
    }

    public <T> JournalMetadata<T> buildWithRootLocation(MetadataBuilder<T> builder) throws JournalException {
        File journalLocation = new File(getJournalBase(), builder.getLocation());

        JournalMetadata<T> mo = readMetadata(journalLocation);
        JournalMetadata<T> mn = builder.location(journalLocation).build();

        if (mo == null || mo.isCompatible(mn, false)) {
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
            if (mn.isCompatible(mo, false)) {
                if (mn.getModelClass() == null) {
                    return (JournalMetadata<T>) new JournalStructure(mn).recordCountHint(key.getRecordHint()).location(journalLocation).build();
                }
                return new JournalMetadataBuilder<>(mn).location(journalLocation).recordCountHint(key.getRecordHint()).build();
            }

            throw new JournalMetadataException(mo, mn);
        }
    }

    public JournalExistenceCheck exists(CharSequence location) {
        CompositePath path = tlPath.get();

        if (!Files.exists(path.of(getJournalBase().getAbsolutePath()).concat(location).$())) {
            return JournalExistenceCheck.DOES_NOT_EXIST;
        }

        if (Files.exists(path.of(path.of(getJournalBase().getAbsolutePath()).concat(location).concat(TxLog.FILE_NAME).$()))
                && Files.exists(path.of(path.of(getJournalBase().getAbsolutePath()).concat(location).concat(JournalConfiguration.FILE_NAME).$()))) {
            return JournalExistenceCheck.EXISTS;
        }

        return JournalExistenceCheck.EXISTS_FOREIGN;
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
                throw new JournalException(location + " is not a recognised journal");
            }

            try (UnstructuredFile hb = new UnstructuredFile(metaFile, 12, JournalMode.READ)) {
                return new JournalMetadata<>(hb);
            }
        }
        return null;
    }
}
