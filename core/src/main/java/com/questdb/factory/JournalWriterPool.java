/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.factory;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalException;

import java.util.concurrent.ConcurrentHashMap;

public class JournalWriterPool {

    public static final int ALLOC_BEGIN = 1;
    public static final int ALLOC_FAILURE = 2;

    private final JournalWriterFactory factory;
    private final ConcurrentHashMap<String, Integer> allocMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JournalWriter> writerMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> busyMap = new ConcurrentHashMap<>();

    public JournalWriterPool(JournalWriterFactory factory) {
        this.factory = factory;
    }

    public JournalWriter get(String path) throws JournalException {

        if (allocMap.putIfAbsent(path, ALLOC_BEGIN) == null) {
            try {
                JournalWriter w = factory.writer(path);
                writerMap.put(path, w);
                busyMap.put(path, Thread.currentThread().getId());
                return w;
            } catch (JournalException e) {
                allocMap.put(path, ALLOC_FAILURE);
                throw e;
            }
        }


        if (allocMap.get(path) == ALLOC_BEGIN && busyMap.replace(path, -1L, Thread.currentThread().getId())) {
            return writerMap.get(path);
        }

        return null;
    }

    public void release(JournalWriter writer) {
        String path = writer.getName();
        if (writerMap.get(path) == writer && busyMap.get(path) == Thread.currentThread().getId()) {
            busyMap.put(path, -1L);
        }
    }


}
