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

import com.questdb.common.PartitionBy;
import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.str.LPSZ;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReadFailTest extends AbstractCairoTest {
    @Test
    public void testMetaFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testMetaFileMissingConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.META_FILE_NAME) && super.exists(path);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testTodoPresentConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableUtils.TODO_FILE_NAME) || super.exists(path);
            }
        };

        assertConstructorFail(ff);
    }

    @Test
    public void testTxnFileCannotOpenConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TXN_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertConstructorFail(ff);
    }

    @Test
    public void testTxnFileMissingConstructor() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.TXN_FILE_NAME) && super.exists(path);
            }
        };
        assertConstructorFail(ff);
    }

    private void assertConstructorFail(FilesFacade ff) throws Exception {
        CairoTestUtils.createAllTable(configuration, PartitionBy.DAY);
        TestUtils.assertMemoryLeak(() -> {
            try {
                new TableReader(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, "all");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
