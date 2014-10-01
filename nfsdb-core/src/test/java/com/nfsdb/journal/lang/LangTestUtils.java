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
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.model.Album;
import com.nfsdb.journal.model.Band;

public class LangTestUtils {

    public static void generateBands(JournalWriter<Band> w, int count) throws JournalException {
        Band band = new Band();
        for (int i = 0; i < count; i++) {
            for (int k = 0; k < 5; k++) {
                band.setName("BAND_" + i);
                band.setType("t_" + k);
                band.setTimestamp(System.currentTimeMillis());
                w.append(band);
            }
        }
        w.commit();
    }

    public static void generateAlbums(JournalWriter<Album> w, int bandCount) throws JournalException {
        Album album = new Album();
        for (int i = 0; i < bandCount; i++) {
            for (int k = 0; k < 10; k++) {
                for (int j = 0; j < 5; j++) {
                    album.setBand("BAND_" + i);
                    album.setName("ALB_" + i + "_" + k);
                    album.setGenre("g_" + i + "_" + k + "_" + j);
                    album.setReleaseDate(System.currentTimeMillis());
                    w.append(album);
                }
            }
        }
        w.commit();
    }
}
