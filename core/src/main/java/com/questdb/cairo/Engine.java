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

package com.questdb.cairo;

import com.questdb.cairo.pool.PoolListener;
import com.questdb.cairo.pool.ReaderPool;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.ex.JournalException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Misc;
import com.questdb.misc.Os;
import com.questdb.mp.Job;
import com.questdb.mp.SynchronizedJob;
import com.questdb.std.ObjHashSet;
import com.questdb.std.str.CompositePath;

import java.io.Closeable;

public class Engine implements Closeable {
    private static final Log LOG = LogFactory.getLog(Engine.class);

    private final WriterPool writerPool;
    private final ReaderPool readerPool;
    private final WriterMaintenanceJob writerMaintenanceJob;
    private final CairoConfiguration configuration;
    private final CompositePath path = new CompositePath();
    private final CompositePath other = new CompositePath();

    public Engine(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.writerPool = new WriterPool(configuration);
        this.readerPool = new ReaderPool(configuration);
        this.writerMaintenanceJob = new WriterMaintenanceJob(configuration.getIdleCheckInterval());
    }


    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(path);
        Misc.free(other);
    }

    public void exportJobs(ObjHashSet<Job> jobs) {
        jobs.add(writerMaintenanceJob);
    }

    public int getBusyReaderCount() {
        return readerPool.getBusyCount();
    }

    public int getBusyWriterCount() {
        return writerPool.getBusyCount();
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public void setPoolListener(PoolListener poolListener) {
        this.writerPool.setPoolListner(poolListener);
        this.readerPool.setPoolListner(poolListener);
    }

    public TableReader getReader(CharSequence tableName) {
        return readerPool.getReader(tableName);
    }

    public TableWriter getWriter(CharSequence tableName) {
        return writerPool.getWriter(tableName);
    }

    public boolean lock(CharSequence tableName) {
        if (writerPool.lock(tableName)) {
            boolean locked = readerPool.lock(tableName);
            if (locked) {
                return true;
            }
            writerPool.unlock(tableName);
        }
        return false;
    }

    public void remove(CharSequence tableName) {
        if (lock(tableName)) {
            try {
                path.of(configuration.getRoot()).concat(tableName).$();
                if (!configuration.getFilesFacade().rmdir(path)) {
                    int error = configuration.getFilesFacade().errno();
                    LOG.error().$("remove failed [tableName='").$(tableName).$("', error=").$(error).$(']').$();
                    throw CairoException.instance(error).put("Table remove failed");
                }
            } finally {
                unlock(tableName);
            }
        }
        throw CairoException.instance(configuration.getFilesFacade().errno()).put("Cannot lock ").put(tableName);
    }

    public void rename(CharSequence tableName, String newName) throws JournalException {
        lock(tableName);
        try {
            rename0(tableName, newName);
        } finally {
            unlock(tableName);
        }
    }

    public void unlock(CharSequence tableName) {
        readerPool.unlock(tableName);
        writerPool.unlock(tableName);
    }

    private void rename0(CharSequence tableName, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        try (CompositePath oldName = new CompositePath()) {
            try (CompositePath newName = new CompositePath()) {

                if (TableUtils.exists(ff, oldName, root, tableName) != 0) {
                    LOG.error().$('\'').$(tableName).$("' does not exist. Rename failed.").$();
                    throw CairoException.instance(0).put("Rename failed. Table '").put(tableName).put("' does not exist");
                }

                if (Os.type == Os.WINDOWS) {
                    oldName.of("\\\\?\\").concat(root).concat(tableName).$();
                    newName.of("\\\\?\\").concat(root).concat(to).$();
                } else {
                    oldName.of(root).concat(tableName).$();
                    newName.of(root).concat(to).$();
                }

                if (ff.exists(newName)) {
                    LOG.error().$("rename target exists [from='").$(tableName).$("', to='").$(newName).$("']").$();
                    throw CairoException.instance(0).put("Rename target exists");
                }

                if (!getConfiguration().getFilesFacade().rename(oldName, newName)) {
                    int error = ff.errno();
                    LOG.error().$("rename failed [from='").$(oldName).$("', to='").$(newName).$("', error=").$(error).$(']').$();
                    throw CairoException.instance(error).put("Rename failed");
                }
            }
        }
    }

    private static abstract class PeriodicSynchronizedJob extends SynchronizedJob {
        private final long checkInterval;
        private long last = 0;

        public PeriodicSynchronizedJob(long checkInterval) {
            this.checkInterval = checkInterval;
        }

        abstract boolean doRun();

        @Override
        protected boolean runSerially() {
            long t = System.currentTimeMillis();
            if (last + checkInterval < t) {
                last = t;
                return doRun();
            }
            return false;
        }
    }

    private class WriterMaintenanceJob extends PeriodicSynchronizedJob {

        public WriterMaintenanceJob(long checkInterval) {
            super(checkInterval);
        }

        @Override
        protected boolean doRun() {
            return writerPool.releaseInactive() || readerPool.releaseInactive();
        }
    }
}
