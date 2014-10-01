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

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.model.Album;
import com.nfsdb.journal.model.Band;
import com.nfsdb.journal.model.Name;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.Rnd;
import com.nfsdb.journal.utils.Dates;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ModelGenerationTest extends AbstractTest {

    @Test
    @Ignore
    public void testGenModel() throws Exception {
        int bandCount = 100000;
        int albumCount = 1000000;
        int genreCount = 200;

        JournalWriter<Name> w1 = factory.writer(Name.class, "band-name", bandCount);
        JournalWriter<Name> w2 = factory.writer(Name.class, "album-name", albumCount);
        JournalWriter<Name> w3 = factory.writer(Name.class, "types", genreCount);

        Name name = new Name();
        Rnd rnd = new Rnd();

        // 100,000 band names
        for (int i = 0; i < bandCount; i++) {
            name.name = rnd.randomString(15);
            w1.append(name);
        }
        w1.commit();

        // 1,000,000 album names
        for (int i = 0; i < albumCount; i++) {
            name.name = rnd.randomString(30);
            w2.append(name);
        }
        w2.commit();

        // 200 band types/genres
        for (int i = 0; i < genreCount; i++) {
            name.name = rnd.randomString(10);
            w3.append(name);
        }
        w3.commit();


        Partition<Name> pt = w3.getLastPartition();
        Band band = new Band();

        // generate 100,000 bands
        // at rate of 5-10 per day

        long ts = Dates.toMillis("2014-03-01T00:00:00.000Z");
        long ts0 = ts;

        int nextBump = rnd.nextPositiveInt() % 6 + 5;
        JournalWriter<Band> bw = factory.writer(Band.class);
        for (Name n : w1.bufferedIterator()) {
            band.setTimestamp(ts0);
            band.setName(n.name);
            pt.read(rnd.nextPositiveInt() % 200, name);
            band.setType(name.name);
            bw.append(band);
            nextBump--;

            if (nextBump == 0) {
                nextBump = rnd.nextPositiveInt() % 6 + 5;
                ts += TimeUnit.DAYS.toMillis(1);
                ts0 = ts;
            } else {
                ts0 += TimeUnit.MINUTES.toMillis(15);
            }
        }
        bw.commit();

        Partition<Name> pbn = w1.getLastPartition();
        int pcn = (bandCount / 100) * (rnd.nextPositiveInt() % 100);
        int cnt = rnd.nextPositiveInt() % 9;

        System.out.println(pcn);
        System.out.println(cnt);

        // update "pcn"  bands "cnt" number of times
        nextBump = rnd.nextPositiveInt() % 6 + 5;
        for (int k = 0; k < cnt; k++) {
            for (int i = 0; i < pcn; i++) {
                band.setTimestamp(ts0);
                band.setName(pbn.getString(rnd.nextPositiveInt() % 100000, 0));
                pt.read(rnd.nextPositiveInt() % 200, name);
                band.setType(name.name);
                bw.append(band);
                nextBump--;

                if (nextBump == 0) {
                    nextBump = rnd.nextPositiveInt() % 6 + 5;
                    ts += TimeUnit.DAYS.toMillis(1);
                    ts0 = ts;
                } else {
                    ts0 += TimeUnit.MINUTES.toMillis(15);
                }
            }
        }
        bw.commit();

        JournalWriter<Album> aw = factory.writer(Album.class);


        Album album = new Album();
        nextBump = rnd.nextPositiveInt() % 6 + 5;
        for (Name n : w2.bufferedIterator()) {
            album.setName(n.name);
            album.setReleaseDate(ts0);
            pt.read(rnd.nextPositiveInt() % 200, name);
            album.setGenre(name.name);
            pbn.read(rnd.nextPositiveInt() % 100000, name);
            album.setBand(name.name);
            aw.append(album);
            nextBump--;

            if (nextBump == 0) {
                nextBump = rnd.nextPositiveInt() % 6 + 5;
                ts += TimeUnit.DAYS.toMillis(1);
                ts0 = ts;
            } else {
                ts0 += TimeUnit.MINUTES.toMillis(15);
            }
        }
        aw.commit();

        pcn = (albumCount / 100) * (rnd.nextPositiveInt() % 100);

//        Partition ap = w2.getLastPartition();
//        Partition bp = bw.getLastPartition();
//
//        for (int i = 0; i < pcn; i++) {
//            String an = ap.getString(rnd.nextPositiveInt() % albumCount, 0);
//            album.setName(an);
//
//        }

        System.out.println(bw.size());
        System.out.println(bw.query().head().withSymValues("name").asResultSet().size());
        System.out.println(aw.size());

    }
}
