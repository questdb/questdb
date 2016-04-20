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

package com.questdb.ql;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalConfigurationException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.JournalConfigurationBuilder;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Files;
import com.questdb.model.Album;
import com.questdb.model.Band;
import com.questdb.ql.impl.AllRowSource;
import com.questdb.ql.impl.JournalPartitionSource;
import com.questdb.ql.impl.JournalSource;
import com.questdb.ql.impl.join.CrossJoinRecordSource;
import com.questdb.ql.impl.join.NestedLoopJoinRecordSource;
import com.questdb.ql.impl.unused.KvIndexTopRowSource;
import com.questdb.ql.impl.unused.StatefulJournalSourceImpl;
import com.questdb.ql.impl.unused.SymLookupKeySource;
import com.questdb.ql.ops.StrGlue;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.col.StrRecordSourceColumn;
import com.questdb.ql.ops.col.SymRecordSourceColumn;
import com.questdb.ql.ops.constant.StrConstant;
import com.questdb.ql.ops.eq.StrEqualsOperator;
import com.questdb.std.ObjList;
import com.questdb.test.tools.JournalTestFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JoinStringToSymbolTest {
    @Rule
    public final JournalTestFactory factory;

    private JournalWriter<Band> bw;
    private JournalWriter<Album> aw;

    public JoinStringToSymbolTest() {
        try {
            this.factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                        $(Band.class)
                                .$sym("name").index()
                                .$sym("type")
                                .$bin("image")
                                .$ts()
                        ;

                        $(Album.class)
                                .$str("band").index()
                                .$sym("name").index()
                                .$ts("releaseDate");

                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Before
    public void setUp() throws Exception {
        bw = factory.writer(Band.class);
        aw = factory.writer(Album.class);
    }

    @Test
    public void testCrossJoin() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));
        bw.append(new Band().setName("band1").setType("jazz").setUrl("http://new.band1.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(
                new CrossJoinRecordSource(
                        new JournalSource(
                                new JournalPartitionSource(aw.getMetadata(), false), new AllRowSource()
                        ),

                        new JournalSource(
                                new JournalPartitionSource(bw.getMetadata(), false), new AllRowSource()
                        )
                ).prepareCursor(factory)
        );

        final String expected = "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\n" +
                "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\n" +
                "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\n" +
                "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://new.band1.com\tjazz\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://new.band1.com\tjazz\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://new.band1.com\tjazz\t\n";

        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testOuterOneToOneHead() throws Exception {

        final String expected = "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://new.band1.com\tjazz\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://new.band1.com\tjazz\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\n";

        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));
        bw.append(new Band().setName("band1").setType("jazz").setUrl("http://new.band1.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(
                        new JournalPartitionSource(aw.getMetadata(), false), new AllRowSource()
                )
        );

        StrGlue glue = new StrGlue(master, new StrRecordSourceColumn(master.getMetadata().getColumnIndex("band")));
        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(
                new NestedLoopJoinRecordSource(
                        master,
                        new JournalSource(
                                new JournalPartitionSource(bw.getMetadata(), false),
                                new KvIndexTopRowSource(
                                        "name"
                                        , new SymLookupKeySource(bw.getSymbolTable("name"), glue)
                                        , null
                                ))
                ).prepareCursor(factory)
        );
//        System.out.println(sink.toString());
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testOuterOneToOneHeadAndFilter() throws Exception {

        final String expected = "band1\talbum X\tpop\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://old.band1.com\trock\t\n" +
                "band1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\t1970-01-01T00:00:00.000Z\tband1\thttp://old.band1.com\trock\t\n" +
                "band3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\t\tnull\t\tnull\t\n";

        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://old.band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));
        bw.append(new Band().setName("band1").setType("jazz").setUrl("http://new.band1.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(
                        new JournalPartitionSource(aw.getMetadata(), false), new AllRowSource()
                )
        );

        StrGlue glue = new StrGlue(master, new StrRecordSourceColumn(master.getMetadata().getColumnIndex("band")));

        ObjList<VirtualColumn> cols = new ObjList<>();
        cols.add(new SymRecordSourceColumn(bw.getMetadata().getColumnIndex("type")));
        cols.add(new StrConstant("rock"));

        StrEqualsOperator filter = (StrEqualsOperator) StrEqualsOperator.FACTORY.newInstance();
        filter.setLhs(cols.get(0));
        filter.setRhs(cols.get(1));

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(
                new NestedLoopJoinRecordSource(
                        master,
                        new JournalSource(
                                new JournalPartitionSource(bw.getMetadata(), false),
                                new KvIndexTopRowSource(
                                        "name"
                                        , new SymLookupKeySource(bw.getSymbolTable("name"), glue)
                                        , filter
                                ))
                ).prepareCursor(factory)
        );
        Assert.assertEquals(expected, sink.toString());
    }
}
