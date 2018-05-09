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

package com.questdb.cairo;

import com.questdb.cairo.pool.PoolListener;
import com.questdb.cairo.pool.ReaderPool;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.SynchronizedJob;
import com.questdb.std.FilesFacade;
import com.questdb.std.Misc;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class Engine implements Closeable, CairoEngine {
    private static final Log LOG = LogFactory.getLog(Engine.class);

    private final WriterPool writerPool;
    private final ReaderPool readerPool;
    private final CairoConfiguration configuration;
    private final Path path = new Path();
    private final Path other = new Path();

    public Engine(CairoConfiguration configuration) {
        this(configuration, null);
    }

    public Engine(CairoConfiguration configuration, CairoWorkScheduler workScheduler) {
        this.configuration = configuration;
        this.writerPool = new WriterPool(configuration, workScheduler);
        this.readerPool = new ReaderPool(configuration);
        if (workScheduler != null) {
            workScheduler.addJob(new WriterMaintenanceJob(configuration));
            workScheduler.addJob(new ColumnIndexerJob(workScheduler));
        }
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(path);
        Misc.free(other);
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

    @Override
    public TableReader getReader(CharSequence tableName) {
        return readerPool.get(tableName);
    }

    @Override
    public int getStatus(CharSequence tableName, int lo, int hi) {
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), tableName, lo, hi);
    }

    @Override
    public boolean releaseAllReaders() {
        return readerPool.releaseAll();
    }

    @Override
    public boolean releaseAllWriters() {
        return writerPool.releaseAll();
    }

    @Override
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
                    LOG.error().$("remove failed [tableName='").utf8(tableName).$("', error=").$(error).$(']').$();
                    throw CairoException.instance(error).put("Table remove failed");
                }
                return;
            } finally {
                unlock(tableName, null);
            }
        }
        throw CairoException.instance(configuration.getFilesFacade().errno()).put("Cannot lock ").put(tableName);
    }

    @Override
    public TableWriter getWriter(CharSequence tableName) {
        return writerPool.get(tableName);
    }

    public void rename(CharSequence tableName, String newName) {
        if (lock(tableName)) {
            try {
                rename0(tableName, newName);
            } finally {
                unlock(tableName, null);
            }
        } else {
            LOG.error().$("cannot lock and rename [from='").$(tableName).$("', to='").$(newName).$("']").$();
            throw CairoException.instance(0).put("Cannot lock [table=").put(tableName).put(']');
        }
    }

    @Override
    public void unlock(CharSequence tableName, @Nullable TableWriter writer) {
        readerPool.unlock(tableName);
        writerPool.unlock(tableName, writer);
    }

    private void rename0(CharSequence tableName, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        if (TableUtils.exists(ff, path, root, tableName) != TableUtils.TABLE_EXISTS) {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.instance(0).put("Rename failed. Table '").put(tableName).put("' does not exist");
        }

        path.of(root).concat(tableName).$();
        other.of(root).concat(to).$();

        if (ff.exists(other)) {
            LOG.error().$("rename target exists [from='").$(tableName).$("', to='").$(other).$("']").$();
            throw CairoException.instance(0).put("Rename target exists");
        }

        if (!ff.rename(path, other)) {
            int error = ff.errno();
            LOG.error().$("rename failed [from='").$(path).$("', to='").$(other).$("', error=").$(error).$(']').$();
            throw CairoException.instance(error).put("Rename failed");
        }
    }

    private class WriterMaintenanceJob extends SynchronizedJob {

        private final MicrosecondClock clock;
        private final long checkInterval;
        private long last = 0;

        public WriterMaintenanceJob(CairoConfiguration configuration) {
            this.clock = configuration.getMicrosecondClock();
            this.checkInterval = configuration.getIdleCheckInterval() * 1000;
        }

        protected boolean doRun() {
            boolean w = writerPool.releaseInactive();
            boolean r = readerPool.releaseInactive();
            return w || r;
        }

        @Override
        protected boolean runSerially() {
            long t = clock.getTicks();
            if (last + checkInterval < t) {
                last = t;
                return doRun();
            }
            return false;
        }
    }
}
