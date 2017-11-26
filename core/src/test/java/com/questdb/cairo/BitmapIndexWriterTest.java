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

import com.questdb.test.tools.TestUtils;
import org.junit.Test;

public class BitmapIndexWriterTest extends AbstractCairoTest {
    @Test
    public void testAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (BitmapIndexWriter writer = new BitmapIndexWriter(configuration.getFilesFacade(), root, "x", 64)) {
                writer.add(0, 1000);
                writer.add(256, 1234);

                writer.add(64, 10);
                writer.add(64, 987);
                writer.add(256, 5567);
            }
        });
    }
}