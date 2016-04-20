/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.misc.Dates;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JournalLagTest extends AbstractJournalTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T00:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T01:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-02-01T13:00:00.000Z"), 100);
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-05-01T00:00:00.000Z"), 100);
    }

    @Test
    public void testLagDetach() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 500));
        master.mergeAppend(origin.query().all().asResultSet().subset(500, 600));
        master.commit();

        executeSequence(true);
        master.removeIrregularPartition();
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testLagOnlyPropagation() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 500));
        master.mergeAppend(origin.query().all().asResultSet().subset(500, 600));
        master.commit();
        String lagName = master.getIrregularPartition().getName();

        executeSequence(true);

        master.mergeAppend(origin.query().all().asResultSet().subset(600, 700));
        master.commit();
        Assert.assertEquals(lagName, master.getIrregularPartition().getName());

        executeSequence(true);
    }
}
