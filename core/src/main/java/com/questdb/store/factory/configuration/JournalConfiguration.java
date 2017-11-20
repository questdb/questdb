/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.std.ex.JournalException;
import com.questdb.store.JournalKey;

import java.io.File;

public interface JournalConfiguration {

    String FILE_NAME = "_meta2";

    int EXISTS = 1;
    int DOES_NOT_EXIST = 2;
    int EXISTS_FOREIGN = 4;

    <T> JournalMetadata<T> createMetadata(JournalKey<T> key) throws JournalException;

    int exists(CharSequence location);

    File getJournalBase();

    <T> JournalMetadata<T> readMetadata(String name) throws JournalException;

    void releaseThreadLocals();
}
