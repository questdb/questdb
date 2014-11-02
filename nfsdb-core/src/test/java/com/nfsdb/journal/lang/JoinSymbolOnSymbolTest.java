/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.lang;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedSource;
import com.nfsdb.journal.lang.cst.Q;
import com.nfsdb.journal.lang.cst.StatefulJournalSource;
import com.nfsdb.journal.lang.cst.impl.QImpl;
import com.nfsdb.journal.lang.cst.impl.dfrm.MapHeadDataFrameSource;
import com.nfsdb.journal.lang.cst.impl.join.InnerSkipJoin;
import com.nfsdb.journal.lang.cst.impl.join.SlaveResetOuterJoin;
import com.nfsdb.journal.lang.cst.impl.join.SymbolToFrameOuterJoin;
import com.nfsdb.journal.lang.cst.impl.jsrc.StatefulJournalSourceImpl;
import com.nfsdb.journal.lang.cst.impl.ref.StringRef;
import com.nfsdb.journal.lang.cst.impl.ref.SymbolXTabVariableSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.KvIndexTopRowSource;
import com.nfsdb.journal.lang.cst.impl.rsrc.SkipSymbolRowSource;
import com.nfsdb.journal.model.Album;
import com.nfsdb.journal.model.Band;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class JoinSymbolOnSymbolTest extends AbstractTest {

    @Rule
    public final JournalTestFactory factory;
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
    }

    @Before
    public void setUp() throws Exception {
        bw = factory.writer(Band.class);
        aw = factory.writer(Album.class);
    }

    @Test
    public void testOuterOneToOne() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        JoinedSource src = buildSource(bw, aw);

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                case 2:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    break;
                case 1:
                    Assert.assertNull(a);
                    Assert.assertEquals("band2", b.getName());
                    break;
                default:
                    Assert.fail("Do not expect more than 3 rows");
            }
        }
    }

    @Test
    public void testOuterOneToMany() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        JoinedSource src = buildSource(bw, aw);

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                case 1:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("band1", b.getName());
                    break;
                case 2:
                    Assert.assertNull(a);
                    Assert.assertEquals("band2", b.getName());
                    break;
                case 3:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("band3", b.getName());
                    break;
                default:
                    Assert.fail("expect 4 rows");

            }
        }
    }

    @Test
    public void testOuterOneToOneHead() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));
        bw.append(new Band().setName("band1").setType("jazz").setUrl("http://new.band1.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef name = new StringRef("name");
        StatefulJournalSource master;
        JoinedSource src = new SlaveResetOuterJoin(
                master = new StatefulJournalSourceImpl(
                        q.forEachPartition(
                                q.source(aw, false)
                                , q.all()
                        )
                )
                ,
                q.forEachPartition(
                        q.source(bw, false)
                        ,
                        new KvIndexTopRowSource(
                                name
                                , q.singleKeySource(new SymbolXTabVariableSource(master, "band", "name"))
                                , null
                        )

                )
        );


        int count = 0;
        for (DataItem d : src) {
            Album a = (Album) d.partition.read(d.rowid);
            Band b = null;
            if (d.slave != null) {
                b = (Band) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                case 1:
                    Assert.assertNotNull(b);
                    Assert.assertEquals(a.getBand(), b.getName());
                    Assert.assertEquals("http://new.band1.com", b.getUrl());
                    break;
                case 2:
                    Assert.assertNotNull(b);
                    Assert.assertEquals(a.getBand(), b.getName());
                    break;
                default:
                    Assert.fail("expected 3 rows");
            }
        }
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
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef band = new StringRef("band");
        StringRef name = new StringRef("name");

        StatefulJournalSource master;
        JoinedSource src = new SlaveResetOuterJoin(
                master = new StatefulJournalSourceImpl(
                        q.forEachPartition(
                                q.source(bw, false)
                                , q.all()
                        )
                )
                ,
                q.forEachPartition(
                        q.source(aw, false)
                        ,
                        new SkipSymbolRowSource(
                                q.kvSource(
                                        band
                                        , q.singleKeySource(new SymbolXTabVariableSource(master, "name", "band"))
                                )
                                , name
                        )
                )
        );

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album BZ", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 1:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album X", a.getName());
                    break;
                case 2:
                    Assert.assertNull(a);
                    Assert.assertEquals("band2", b.getName());
                    break;
                case 3:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("band3", b.getName());
                    break;
                default:
                    Assert.fail("expected 4 rows");
            }
        }
    }

    @Test
    public void testOuterOneToManyMapHead() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));
        aw.append(new Album().setName("album Y").setBand("band2").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef band = new StringRef("band");
        StringRef name = new StringRef("name");

        JoinedSource src = new SymbolToFrameOuterJoin(
                q.forEachPartition(
                        q.source(bw, false)
                        , q.all()
                )
                , name
                ,
                new MapHeadDataFrameSource(
                        q.forEachPartition(
                                q.source(aw, false)
                                , q.kvSource(name, q.symbolTableSource(name), 1, 0, null)
                        )
                        , band
                )
                , band
        );

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album X", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 1:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album BZ", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 2:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album Y", a.getName());
                    Assert.assertEquals("band2", b.getName());
                    break;
                case 3:
                    Assert.assertNull(a);
                    Assert.assertEquals("band3", b.getName());
                    break;
                default:
                    Assert.fail("expected 4 rows");
            }
        }
    }

    @Test
    public void testInnerOneToManyMapHead() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));
        aw.append(new Album().setName("album Y").setBand("band2").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef band = new StringRef("band");
        StringRef name = new StringRef("name");

        JoinedSource src = new InnerSkipJoin(
                new SymbolToFrameOuterJoin(
                        q.forEachPartition(
                                q.source(bw, false)
                                , q.all()
                        )
                        , name
                        ,
                        new MapHeadDataFrameSource(
                                q.forEachPartition(
                                        q.source(aw, false)
                                        , q.kvSource(name, q.symbolTableSource(name), 1, 0, null)
                                )
                                , band
                        )
                        , band
                )
        );

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album X", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 1:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album BZ", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 2:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album Y", a.getName());
                    Assert.assertEquals("band2", b.getName());
                    break;
                default:
                    Assert.fail("expected 3 rows");
            }
        }
    }

    @Test
    public void testInnerOneToManyHead() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef band = new StringRef("band");
        StringRef name = new StringRef("name");

        StatefulJournalSourceImpl master;
        JoinedSource src = new InnerSkipJoin(
                new SlaveResetOuterJoin(
                        master = new StatefulJournalSourceImpl(
                                q.forEachPartition(
                                        q.source(bw, false)
                                        , q.all()
                                )
                        )
                        ,
                        q.forEachPartition(
                                q.source(aw, false)
                                ,
                                new SkipSymbolRowSource(
                                        q.kvSource(
                                                band
                                                , q.singleKeySource(new SymbolXTabVariableSource(master, "name", "band"))
                                        )
                                        , name
                                )
                        )
                )
        );

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            switch (count++) {
                case 0:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album BZ", a.getName());
                    Assert.assertEquals("pop", a.getGenre());
                    break;
                case 1:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("album X", a.getName());
                    break;
                case 2:
                    Assert.assertNotNull(a);
                    Assert.assertEquals(b.getName(), a.getBand());
                    Assert.assertEquals("band3", b.getName());
                    break;
                default:
                    Assert.fail("expected 3 rows");
            }
        }
    }

    @Test
    public void testInnerOneToManyHeadFilter() throws Exception {
        bw.append(new Band().setName("band1").setType("rock").setUrl("http://band1.com"));
        bw.append(new Band().setName("band2").setType("hiphop").setUrl("http://band2.com"));
        bw.append(new Band().setName("band3").setType("jazz").setUrl("http://band3.com"));

        bw.commit();

        aw.append(new Album().setName("album X").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("rock"));
        aw.append(new Album().setName("album BZ").setBand("band1").setGenre("pop"));
        aw.append(new Album().setName("album Y").setBand("band3").setGenre("metal"));

        aw.commit();

        Q q = new QImpl();
        StringRef band = new StringRef("band");

        StatefulJournalSource master;
        JoinedSource src = new InnerSkipJoin(
                new SlaveResetOuterJoin(
                        master = new StatefulJournalSourceImpl(
                                q.forEachPartition(
                                        q.source(bw, false)
                                        , q.all()
                                )
                        )
                        ,
                        q.forEachPartition(
                                q.source(aw, false)
                                ,
                                q.kvSource(
                                        band
                                        , q.singleKeySource(new SymbolXTabVariableSource(master, "name", "band"))
                                )
                        )
                )
        );

        int count = 0;
        for (DataItem d : src) {
            Band b = (Band) d.partition.read(d.rowid);
            Album a = null;
            if (d.slave != null) {
                a = (Album) d.slave.partition.read(d.slave.rowid);
            }

            System.out.println(a + "=" + b);
        }
    }

    private JoinedSource buildSource(Journal<Band> bw, Journal<Album> aw) {
        Q q = new QImpl();
        StringRef band = new StringRef("band");
        StatefulJournalSource master;
        return new SlaveResetOuterJoin(
                master = new StatefulJournalSourceImpl(
                        q.forEachPartition(
                                q.source(bw, false)
                                , q.all()
                        )
                )
                ,
                q.forEachPartition(
                        q.source(aw, false)
                        , q.kvSource(band
                                , q.singleKeySource(new SymbolXTabVariableSource(master, "name", "band"))
                        )
                )
        );
    }

}
