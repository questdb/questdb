/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.factory;

import com.nfsdb.JournalBulkWriter;
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.MetadataBuilder;

public interface JournalWriterFactory {

    <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz) throws JournalException;

    <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz, String location) throws JournalException;

    <T> JournalBulkWriter<T> bulkWriter(Class<T> clazz, String location, int recordHint) throws JournalException;

    <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException;

    <T> JournalWriter<T> bulkWriter(MetadataBuilder<T> metadata) throws JournalException;

    JournalConfiguration getConfiguration();

    <T> JournalWriter<T> writer(Class<T> clazz) throws JournalException;

    <T> JournalWriter<T> writer(Class<T> clazz, String location) throws JournalException;

    <T> JournalWriter<T> writer(Class<T> clazz, String location, int recordHint) throws JournalException;

    JournalWriter writer(String location) throws JournalException;

    <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException;

    <T> JournalWriter<T> writer(MetadataBuilder<T> metadata) throws JournalException;
}
