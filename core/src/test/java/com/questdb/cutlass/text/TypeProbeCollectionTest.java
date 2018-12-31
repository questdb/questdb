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

package com.questdb.cutlass.text;

import com.questdb.cutlass.text.typeprobe.*;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Os;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;
import org.junit.Assert;
import org.junit.Test;

public class TypeProbeCollectionTest {
    @Test
    public void testTypeProbeCollectionInstantiation() {
        String fileName = this.getClass().getResource("/date_test.formats").getFile();
        if (Os.type == Os.WINDOWS && fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        try (Path path = new Path()) {
            TypeProbeCollection typeProbeCollection = new TypeProbeCollection(FilesFacadeImpl.INSTANCE, path, fileName, new DateFormatFactory(), DateLocaleFactory.INSTANCE);

            Assert.assertEquals(7, typeProbeCollection.getProbeCount());
            Assert.assertTrue(typeProbeCollection.getProbe(0) instanceof IntProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(1) instanceof LongProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(2) instanceof DoubleProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(3) instanceof BooleanProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(4) instanceof DateProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(5) instanceof DateProbe);
            Assert.assertTrue(typeProbeCollection.getProbe(6) instanceof DateProbe);

            DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();

            Assert.assertEquals("dd/MM/y", typeProbeCollection.getProbe(4).getFormat());
            Assert.assertEquals(defaultLocale.getId(), typeProbeCollection.getProbe(4).getDateLocale().getId());

            Assert.assertEquals("yyyy-MM-dd HH:mm:ss", typeProbeCollection.getProbe(5).getFormat());
            Assert.assertEquals("es-PA", typeProbeCollection.getProbe(5).getDateLocale().getId());

            Assert.assertEquals("MM/dd/y", typeProbeCollection.getProbe(6).getFormat());
            Assert.assertEquals(defaultLocale.getId(), typeProbeCollection.getProbe(6).getDateLocale().getId());
        }
    }
}