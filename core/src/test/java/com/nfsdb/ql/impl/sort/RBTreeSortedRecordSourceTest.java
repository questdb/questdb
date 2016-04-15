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
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.sort;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.misc.Rnd;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import com.nfsdb.ql.parser.QueryError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RBTreeSortedRecordSourceTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUp() throws Exception {
        Rnd rnd = new Rnd();
        try (JournalWriter w = factory.bulkWriter(new JournalStructure("xyz")
                .$int("i")
                .$str("str")
                .$())) {
            int n = 100;

            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, rnd.nextInt());
                ew.putStr(1, rnd.nextChars(2));
                ew.append();
            }
            w.commit();
        }

        try (JournalWriter w = factory.bulkWriter(new JournalStructure("dupes")
                .$int("x")
                .$()
        )) {
            for (int i = 0; i < 10; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, i % 2 == 0 ? 10 : 20);
                ew.append();
            }
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, 30);
            ew.append();
            w.commit();
        }
    }

    @Test
    public void testEqualRows() throws Exception {
        assertThat("10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "20\n" +
                        "30\n",
                "dupes order by x");

    }

    @Test
    public void testFirstColumnOrderDescending() throws Exception {
        assertThat("-10505757\tCC\n" +
                        "-27395319\tOJ\n" +
                        "-120660220\tQE\n" +
                        "-147343840\tLD\n" +
                        "-230430837\tZZ\n" +
                        "-235358133\tMY\n" +
                        "-246923735\tGL\n" +
                        "-283321892\tJO\n" +
                        "-292438036\tPG\n" +
                        "-370796356\tNZ\n" +
                        "-409854405\tZS\n" +
                        "-422941535\tPD\n" +
                        "-483853667\tHR\n" +
                        "-530317703\tTJ\n" +
                        "-623471113\tQM\n" +
                        "-636975106\tZE\n" +
                        "-661194722\tZO\n" +
                        "-720881601\tQC\n" +
                        "-727724771\tCP\n" +
                        "-731466113\tLY\n" +
                        "-847531048\tRX\n" +
                        "-876466531\tOL\n" +
                        "-907794648\tSS\n" +
                        "-916132123\tYC\n" +
                        "-942999384\tVV\n" +
                        "-1121895896\tVD\n" +
                        "-1125169127\tEY\n" +
                        "-1148479920\tTJ\n" +
                        "-1153445279\tYU\n" +
                        "-1165635863\tMV\n" +
                        "-1172180184\tYL\n" +
                        "-1204245663\tPJ\n" +
                        "-1234141625\tND\n" +
                        "-1252906348\tQE\n" +
                        "-1269042121\tEK\n" +
                        "-1270731285\tEO\n" +
                        "-1271909747\tYS\n" +
                        "-1272693194\tED\n" +
                        "-1311366306\tML\n" +
                        "-1418341054\tJG\n" +
                        "-1424048819\tVS\n" +
                        "-1436881714\tEH\n" +
                        "-1465751763\tUS\n" +
                        "-1515787781\tGO\n" +
                        "-1533414895\tTM\n" +
                        "-1538602195\tDZ\n" +
                        "-1613687261\tBE\n" +
                        "-1768335227\tSW\n" +
                        "-1810676855\tLO\n" +
                        "-1844391305\tWF\n" +
                        "-1870444467\tRY\n" +
                        "-1871994006\tZS\n" +
                        "-1960168360\tUO\n" +
                        "-2002373666\tQQ\n" +
                        "-2043803188\tVI\n" +
                        "-2088317486\tSS\n" +
                        "-2108151088\tXP\n" +
                        "-2119387831\tBH\n" +
                        "-2132716300\tEO\n",
                "xyz where i < 100 order by i desc");
    }

    @Test
    public void testNestedOrderBy() throws Exception {
        final String expected = "-1613687261\tBE\n" +
                "-2119387831\tBH\n" +
                "-10505757\tCC\n" +
                "-727724771\tCP\n" +
                "-1538602195\tDZ\n" +
                "-1272693194\tED\n" +
                "-1436881714\tEH\n" +
                "-1269042121\tEK\n" +
                "-2132716300\tEO\n" +
                "-1270731285\tEO\n" +
                "-1125169127\tEY\n" +
                "-246923735\tGL\n" +
                "-1515787781\tGO\n" +
                "-483853667\tHR\n" +
                "-1418341054\tJG\n" +
                "-283321892\tJO\n" +
                "-147343840\tLD\n" +
                "-1810676855\tLO\n" +
                "-731466113\tLY\n" +
                "-1311366306\tML\n" +
                "-1165635863\tMV\n" +
                "-235358133\tMY\n" +
                "-1234141625\tND\n" +
                "-370796356\tNZ\n" +
                "-27395319\tOJ\n" +
                "-876466531\tOL\n" +
                "-422941535\tPD\n" +
                "-292438036\tPG\n" +
                "-1204245663\tPJ\n" +
                "-720881601\tQC\n" +
                "-1252906348\tQE\n" +
                "-120660220\tQE\n" +
                "-623471113\tQM\n" +
                "-2002373666\tQQ\n" +
                "-847531048\tRX\n" +
                "-1870444467\tRY\n" +
                "-2088317486\tSS\n" +
                "-907794648\tSS\n" +
                "-1768335227\tSW\n" +
                "-1148479920\tTJ\n" +
                "-530317703\tTJ\n" +
                "-1533414895\tTM\n" +
                "-1960168360\tUO\n" +
                "-1465751763\tUS\n" +
                "-1121895896\tVD\n" +
                "-2043803188\tVI\n" +
                "-1424048819\tVS\n" +
                "-942999384\tVV\n" +
                "-1844391305\tWF\n" +
                "-2108151088\tXP\n" +
                "-916132123\tYC\n" +
                "-1172180184\tYL\n" +
                "-1271909747\tYS\n" +
                "-1153445279\tYU\n" +
                "-636975106\tZE\n" +
                "-661194722\tZO\n" +
                "-1871994006\tZS\n" +
                "-409854405\tZS\n" +
                "-230430837\tZZ\n";

        assertThat(expected, "(xyz where i < 100 order by i) order by str, i");
    }

    @Test
    public void testNestedOrderByDescending() throws Exception {
        final String expected = "-1613687261\tBE\n" +
                "-2119387831\tBH\n" +
                "-10505757\tCC\n" +
                "-727724771\tCP\n" +
                "-1538602195\tDZ\n" +
                "-1272693194\tED\n" +
                "-1436881714\tEH\n" +
                "-1269042121\tEK\n" +
                "-1270731285\tEO\n" +
                "-2132716300\tEO\n" +
                "-1125169127\tEY\n" +
                "-246923735\tGL\n" +
                "-1515787781\tGO\n" +
                "-483853667\tHR\n" +
                "-1418341054\tJG\n" +
                "-283321892\tJO\n" +
                "-147343840\tLD\n" +
                "-1810676855\tLO\n" +
                "-731466113\tLY\n" +
                "-1311366306\tML\n" +
                "-1165635863\tMV\n" +
                "-235358133\tMY\n" +
                "-1234141625\tND\n" +
                "-370796356\tNZ\n" +
                "-27395319\tOJ\n" +
                "-876466531\tOL\n" +
                "-422941535\tPD\n" +
                "-292438036\tPG\n" +
                "-1204245663\tPJ\n" +
                "-720881601\tQC\n" +
                "-120660220\tQE\n" +
                "-1252906348\tQE\n" +
                "-623471113\tQM\n" +
                "-2002373666\tQQ\n" +
                "-847531048\tRX\n" +
                "-1870444467\tRY\n" +
                "-907794648\tSS\n" +
                "-2088317486\tSS\n" +
                "-1768335227\tSW\n" +
                "-530317703\tTJ\n" +
                "-1148479920\tTJ\n" +
                "-1533414895\tTM\n" +
                "-1960168360\tUO\n" +
                "-1465751763\tUS\n" +
                "-1121895896\tVD\n" +
                "-2043803188\tVI\n" +
                "-1424048819\tVS\n" +
                "-942999384\tVV\n" +
                "-1844391305\tWF\n" +
                "-2108151088\tXP\n" +
                "-916132123\tYC\n" +
                "-1172180184\tYL\n" +
                "-1271909747\tYS\n" +
                "-1153445279\tYU\n" +
                "-636975106\tZE\n" +
                "-661194722\tZO\n" +
                "-409854405\tZS\n" +
                "-1871994006\tZS\n" +
                "-230430837\tZZ\n";

        assertThat(expected, "(xyz where i < 100 order by i) order by str, i desc");
    }

    @Test
    public void testOrderByExpression() throws Exception {
        try {
            assertThat("", "(xyz where i < 100 order by i) order by str, i+i");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(47, QueryError.getPosition());
        }
    }

    @Test
    public void testOrderByString() throws Exception {
        try {
            assertThat("", "(xyz where i < 100 order by i) order by str, 'i+i'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
        }
    }

}