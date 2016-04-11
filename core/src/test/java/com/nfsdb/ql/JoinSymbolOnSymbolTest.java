/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.ql;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalConfigurationException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.misc.Files;
import com.nfsdb.model.Album;
import com.nfsdb.model.Band;
import com.nfsdb.ql.impl.AllRowSource;
import com.nfsdb.ql.impl.JournalPartitionSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.join.NestedLoopJoinRecordSource;
import com.nfsdb.ql.impl.unused.*;
import com.nfsdb.ql.ops.SymGlue;
import com.nfsdb.ql.ops.col.SymRecordSourceColumn;
import com.nfsdb.test.tools.JournalTestFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JoinSymbolOnSymbolTest {

    @Rule
    public final JournalTestFactory factory;
    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter out = new RecordSourcePrinter(sink);
    private JournalWriter<Band> bw;
    private JournalWriter<Album> aw;

    public JoinSymbolOnSymbolTest() {
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
                                .$sym("band").index()
                                .$sym("name").index()
                                .$ts("releaseDate");

                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }

//        out = new JournalEntryPrinter(new FlexBufferSink(new FileOutputStream(FileDescriptor.out).getChannel()), false);
    }

    @Before
    public void setUp() throws Exception {
        bw = factory.writer(Band.class);
        aw = factory.writer(Album.class);
    }

    @Test
    public void testOuterOneToMany() throws Exception {

        final String expected = "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum X\tpop\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\tnull\tnull\t\t\n" +
                "1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\tband3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\n";

        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        // from band outer join album
        // this is data-driven one to many
        out.printCursor(buildSource(bw, aw).prepareCursor(factory));
        Assert.assertEquals(expected, sink.toString());
    }

    /**
     * Band and Album are joined on symbol. We want to do outer join so that
     * it shows Bands without Albums. Also Albums can be versioned on album name symbol.
     * Here we select Bands and latest versions of their Albums.
     *
     * @throws Exception
     */
    @Test
    public void testOuterOneToManyHead() throws Exception {

        final String expected = "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum X\tpop\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\tnull\tnull\t\t\n" +
                "1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\tband3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\n";

        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();


        // from band outer join album head by name
        // **head by name is applied after join

        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(new JournalPartitionSource(bw.getMetadata(), false),
                        new AllRowSource()
                )
        );

        SymGlue glue = new SymGlue(master, new SymRecordSourceColumn(master.getMetadata().getColumnIndex("name")));

        out.printCursor(
                new NestedLoopJoinRecordSource(
                        master,
                        new JournalSource(
                                new JournalPartitionSource(aw.getMetadata(), false),
                                new DistinctSymbolRowSource(
                                        new KvIndexRowSource(
                                                "band",
                                                new SymLookupKeySource(aw.getSymbolTable("band"), glue)
                                        )
                                        , "name"
                                ))
                ).prepareCursor(factory)
        );
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testOuterOneToOne() throws Exception {

        final String expected = "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum X\tpop\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband2\thttp://band2.com\thiphop\t\tnull\tnull\t\t\n" +
                "1970-01-01T00:00:00.000Z\tband3\thttp://band3.com\tjazz\t\tband3\talbum Y\tmetal\t1970-01-01T00:00:00.000Z\n";


        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        // from band outer join album
        out.printCursor(buildSource(bw, aw).prepareCursor(factory));
        Assert.assertEquals(expected, sink.toString());
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

        // from album join band head by name
        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(new JournalPartitionSource(aw.getMetadata(), false), new AllRowSource())
        );

        SymGlue glue = new SymGlue(master, new SymRecordSourceColumn(master.getMetadata().getColumnIndex("band")));
        out.printCursor(new NestedLoopJoinRecordSource(
                        master,
                        new JournalSource(
                                new JournalPartitionSource(bw.getMetadata(), false),
                                new KvIndexTopRowSource(
                                        "name"
                                        , new SymLookupKeySource(bw.getSymbolTable("name"), glue)
                                        , null
                                )
                        )
                ).prepareCursor(factory)
        );

        Assert.assertEquals(expected, sink.toString());
    }

    private RecordSource buildSource(Journal<Band> bw, Journal<Album> aw) {
        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(
                        new JournalPartitionSource(bw.getMetadata(), true),
                        new AllRowSource())
        );

        SymGlue glue = new SymGlue(master, new SymRecordSourceColumn(master.getMetadata().getColumnIndex("name")));
        return new NestedLoopJoinRecordSource(
                master,
                new JournalSource(
                        new JournalPartitionSource(aw.getMetadata(), false),
                        new KvIndexRowSource("band"
                                , new SymLookupKeySource(aw.getSymbolTable("band"), glue)
                        )
                )
        );
    }
}
