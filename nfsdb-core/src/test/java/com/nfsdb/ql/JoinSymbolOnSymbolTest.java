/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Album;
import com.nfsdb.model.Band;
import com.nfsdb.ql.impl.AllRowSource;
import com.nfsdb.ql.impl.JournalPartitionSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.join.InnerSkipNullJoinRecordSource;
import com.nfsdb.ql.impl.join.NestedLoopJoinRecordSource;
import com.nfsdb.ql.impl.unused.*;
import com.nfsdb.ql.ops.SymGlue;
import com.nfsdb.ql.ops.col.SymRecordSourceColumn;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.utils.Files;
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
    public void testInnerOneToManyHead() throws Exception {

        final String expected = "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum X\tpop\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\n" +
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

        // from band join album head by name
        // **inner join
        // **join first head after
        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(new JournalPartitionSource(bw.getMetadata(), true), new AllRowSource())
        );

        SymGlue glue = new SymGlue(master, new SymRecordSourceColumn(master.getMetadata().getColumnIndex("name")));

        out.printCursor(
                new InnerSkipNullJoinRecordSource(
                        new NestedLoopJoinRecordSource(
                                master,
                                new JournalSource(new JournalPartitionSource(aw.getMetadata(), false),
                                        new DistinctSymbolRowSource(
                                                new KvIndexRowSource(
                                                        "band"
                                                        , new SymLookupKeySource(aw.getSymbolTable("band"), glue)
                                                )
                                                , "name"
                                        )
                                )
                        )
                ).prepareCursor(factory)
        );
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testInnerOneToManyHeadFilter() throws Exception {

        final String expected = "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum X\tpop\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum BZ\trock\t1970-01-01T00:00:00.000Z\n" +
                "1970-01-01T00:00:00.000Z\tband1\thttp://band1.com\trock\t\tband1\talbum BZ\tpop\t1970-01-01T00:00:00.000Z\n" +
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

        // from band join album head by name
        StatefulJournalSourceImpl master = new StatefulJournalSourceImpl(
                new JournalSource(new JournalPartitionSource(bw.getMetadata(), false), new AllRowSource())
        );

        SymGlue glue = new SymGlue(master, new SymRecordSourceColumn(master.getMetadata().getColumnIndex("name")));

        out.printCursor(
                new InnerSkipNullJoinRecordSource(
                        new NestedLoopJoinRecordSource(
                                master,
                                new JournalSource(new JournalPartitionSource(aw.getMetadata(), false),
                                        new KvIndexRowSource(
                                                "band"
                                                , new SymLookupKeySource(aw.getSymbolTable("band"), glue)
                                        ))
                        )
                ).prepareCursor(factory)
        );
        Assert.assertEquals(expected, sink.toString());
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

    private RecordSource<? extends Record> buildSource(Journal<Band> bw, Journal<Album> aw) {
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
