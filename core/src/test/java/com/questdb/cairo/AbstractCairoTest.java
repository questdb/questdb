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

import com.questdb.parser.sql.AbstractOptimiserTest;
import com.questdb.std.Files;
import com.questdb.std.str.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class AbstractCairoTest extends AbstractOptimiserTest {
    protected static CharSequence root;
    protected static CairoConfiguration configuration;

    @BeforeClass
    public static void setUp() {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
        configuration = new DefaultCairoConfiguration(root);
    }

    @Before
    public void setUp0() {
        try (Path path = new Path().of(root).$()) {
            if (Files.exists(path)) {
                return;
            }
            Files.mkdirs(path.of(root).put(Files.SEPARATOR).$(), configuration.getMkDirMode());
        }
    }

    @After
    public void tearDown0() {
        try (Path path = new Path().of(root)) {
            Files.rmdir(path.$());
        }
    }
}
