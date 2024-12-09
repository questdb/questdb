/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Verifies execution plans for <a href="https://github.com/ClickHouse/ClickBench">ClickBench</a> queries.
 */
public class ClickBenchTest extends AbstractCairoTest {
    private static final String DDL = "CREATE TABLE hits\n" +
            "(\n" +
            "    WatchID long,\n" +
            "    JavaEnable byte,\n" +
            "    Title varchar,\n" +
            "    GoodEvent byte,\n" +
            "    EventTime timestamp,\n" +
            "    Eventdate timestamp,\n" +
            "    CounterID int,\n" +
            "    ClientIP int,\n" +
            "    RegionID int,\n" +
            "    UserID long,\n" +
            "    CounterClass byte,\n" +
            "    OS short,\n" +
            "    UserAgent short,\n" +
            "    URL varchar,\n" +
            "    Referer varchar,\n" +
            "    IsRefresh byte,\n" +
            "    RefererCategoryID short,\n" +
            "    RefererRegionID int,\n" +
            "    URLCategoryID short,\n" +
            "    URLRegionID int,\n" +
            "    ResolutionWidth short,\n" +
            "    ResolutionHeight short,\n" +
            "    ResolutionDepth short,\n" +
            "    FlashMajor byte,\n" +
            "    FlashMinor byte,\n" +
            "    FlashMinor2 symbol,\n" +
            "    NetMajor byte,\n" +
            "    NetMinor byte,\n" +
            "    UserAgentMajor short,\n" +
            "    UserAgentMinor symbol,\n" +
            "    CookieEnable byte,\n" +
            "    JavascriptEnable byte,\n" +
            "    IsMobile byte,\n" +
            "    MobilePhone short,\n" +
            "    MobilePhoneModel symbol,\n" +
            "    Params symbol,\n" +
            "    IPNetworkID int,\n" +
            "    TraficSourceID int,\n" +
            "    SearchEngineID short,\n" +
            "    SearchPhrase varchar,\n" +
            "    AdvEngineID short,\n" +
            "    IsArtifical byte,\n" +
            "    WindowClientWidth short,\n" +
            "    WindowClientHeight short,\n" +
            "    ClientTimeZone short,\n" +
            "    ClientEventTime timestamp,\n" +
            "    SilverlightVersion1 byte,\n" +
            "    SilverlightVersion2 byte,\n" +
            "    SilverlightVersion3 short,\n" +
            "    SilverlightVersion4 byte,\n" +
            "    PageCharset symbol,\n" +
            "    CodeVersion short,\n" +
            "    IsLink byte,\n" +
            "    IsDownload byte,\n" +
            "    IsNotBounce byte,\n" +
            "    FUniqID long,\n" +
            "    OriginalURL varchar,\n" +
            "    HID int,\n" +
            "    IsOldCounter byte,\n" +
            "    IsEvent byte,\n" +
            "    IsParameter byte,\n" +
            "    DontCountHits byte,\n" +
            "    WithHash byte,\n" +
            "    HitColor char,\n" +
            "    LocalEventTime timestamp,\n" +
            "    Age byte,\n" +
            "    Sex byte,\n" +
            "    Income byte,\n" +
            "    Interests short,\n" +
            "    Robotness short,\n" +
            "    RemoteIP int,\n" +
            "    WindowName int,\n" +
            "    OpenerName int,\n" +
            "    HistoryLength short,\n" +
            "    BrowserLanguage symbol,\n" +
            "    BrowserCountry symbol,\n" +
            "    SocialNetwork symbol,\n" +
            "    SocialAction symbol,\n" +
            "    HTTPError byte,\n" +
            "    SendTiming int,\n" +
            "    DNSTiming int,\n" +
            "    ConnectTiming int,\n" +
            "    ResponseStartTiming int,\n" +
            "    ResponseEndTiming int,\n" +
            "    FetchTiming int,\n" +
            "    SocialSourceNetworkID short,\n" +
            "    SocialSourcePage varchar,\n" +
            "    ParamPrice long,\n" +
            "    ParamOrderID symbol,\n" +
            "    ParamCurrency symbol,\n" +
            "    ParamCurrencyID short,\n" +
            "    OpenstatServiceName symbol,\n" +
            "    OpenstatCampaignID symbol,\n" +
            "    OpenstatAdID varchar,\n" +
            "    OpenstatSourceID symbol,\n" +
            "    UTMSource symbol,\n" +
            "    UTMMedium symbol,\n" +
            "    UTMCampaign symbol,\n" +
            "    UTMContent symbol,\n" +
            "    UTMTerm symbol,\n" +
            "    FromTag symbol,\n" +
            "    HasGCLID byte,\n" +
            "    RefererHash long,\n" +
            "    URLHash long,\n" +
            "    CLID int\n" +
            ") TIMESTAMP(EventTime) PARTITION BY DAY;\n";
    private static final Log LOG = LogFactory.getLog(ClickBenchTest.class);
    private static final TestCase[] TEST_CASES = new TestCase[]{
            new TestCase(
                    "Q0",
                    "SELECT COUNT(*) FROM hits;",
                    "Count\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q1",
                    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;",
                    "Count\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      filter: AdvEngineID!=0\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q2",
                    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;",
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [sum(AdvEngineID),count(*),avg(ResolutionWidth)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q3",
                    "SELECT AVG(UserID) FROM hits;",
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [avg(UserID)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q4",
                    "SELECT count_distinct(UserID) FROM hits;",
                    "Count\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [UserID]\n" +
                            "      filter: UserID!=null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q5",
                    "SELECT count_distinct(SearchPhrase) FROM hits;",
                    "Count\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [SearchPhrase]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q6",
                    "SELECT MIN(EventDate), MAX(EventDate) FROM hits;",
                    "GroupBy vectorized: true workers: 1\n" +
                            "  values: [min(EventDate),max(EventDate)]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q7",
                    "SELECT AdvEngineID, COUNT(*) AS c FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC;",
                    "Radix sort light\n" +
                            "  keys: [c desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [AdvEngineID]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: AdvEngineID!=0\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q8",
                    "SELECT RegionID, count_distinct(UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [u desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [RegionID]\n" +
                            "      values: [count_distinct(UserID)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q9",
                    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), count_distinct(UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [RegionID]\n" +
                            "      values: [sum(AdvEngineID),count(*),avg(ResolutionWidth),count_distinct(UserID)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q10",
                    "SELECT MobilePhoneModel, count_distinct(UserID) AS u FROM hits WHERE MobilePhoneModel IS NOT NULL GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [u desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [MobilePhoneModel]\n" +
                            "      values: [count_distinct(UserID)]\n" +
                            "      filter: MobilePhoneModel is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q11",
                    "SELECT MobilePhone, MobilePhoneModel, count_distinct(UserID) AS u FROM hits WHERE MobilePhoneModel IS NOT NULL GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [u desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [MobilePhone,MobilePhoneModel]\n" +
                            "      values: [count_distinct(UserID)]\n" +
                            "      filter: MobilePhoneModel is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q12",
                    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase IS NOT NULL GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [SearchPhrase]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q13",
                    "SELECT SearchPhrase, count_distinct(UserID) AS u FROM hits WHERE SearchPhrase IS NOT NULL GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [u desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [SearchPhrase]\n" +
                            "      values: [count_distinct(UserID)]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q14",
                    "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase IS NOT NULL GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [SearchEngineID,SearchPhrase]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q15",
                    "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [UserID]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4085
            new TestCase(
                    "Q16",
                    "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [UserID,SearchPhrase]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q17",
                    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;",
                    "Limit lo: 10\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [UserID,SearchPhrase]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4085
            new TestCase(
                    "Q18",
                    "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Sort light lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [UserID,m,SearchPhrase,c]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [UserID,m,SearchPhrase]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q19",
                    "SELECT UserID FROM hits WHERE UserID = 435090932899640449;",
                    "Async JIT Filter workers: 1\n" +
                            "  filter: UserID=435090932899640449L\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q20",
                    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';",
                    "Count\n" +
                            "    Async Filter workers: 1\n" +
                            "      filter: URL like %google%\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q21",
                    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase IS NOT NULL GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [SearchPhrase]\n" +
                            "      values: [min(URL),count(*)]\n" +
                            "      filter: (URL like %google% and SearchPhrase is not null)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q22",
                    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, count_distinct(UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase IS NOT NULL GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [SearchPhrase]\n" +
                            "      values: [min(URL),min(Title),count(*),count_distinct(UserID)]\n" +
                            "      filter: (Title like %Google% and not (URL like %.google.%) and SearchPhrase is not null)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q23",
                    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;",
                    "Async Filter workers: 1\n" +
                            "  limit: 10\n" +
                            "  filter: URL like %google%\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q24",
                    "SELECT SearchPhrase FROM hits WHERE SearchPhrase IS NOT NULL ORDER BY EventTime LIMIT 10;",
                    "SelectedRecord\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      limit: 10\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4133
            new TestCase(
                    "Q25",
                    "SELECT SearchPhrase FROM hits WHERE SearchPhrase IS NOT NULL ORDER BY SearchPhrase LIMIT 10;",
                    "Sort light lo: 10\n" +
                            "  keys: [SearchPhrase]\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q26",
                    "SELECT SearchPhrase FROM hits WHERE SearchPhrase IS NOT NULL ORDER BY EventTime, SearchPhrase LIMIT 10;",
                    "SelectedRecord\n" +
                            "    Sort light lo: 10 partiallySorted: true\n" +
                            "      keys: [EventTime, SearchPhrase]\n" +
                            "        Async JIT Filter workers: 1\n" +
                            "          filter: SearchPhrase is not null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q27",
                    "SELECT * FROM (SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL IS NOT NULL GROUP BY CounterID) WHERE c > 100000 ORDER BY l DESC LIMIT 25;",
                    "Sort light lo: 25\n" +
                            "  keys: [l desc]\n" +
                            "    Filter filter: 100000<c\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [CounterID]\n" +
                            "          values: [avg(length(URL)),count(*)]\n" +
                            "          filter: URL is not null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q28",
                    "SELECT * FROM (SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '$1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer IS NOT NULL GROUP BY k) WHERE c > 100000 ORDER BY l DESC LIMIT 25;",
                    "Sort light lo: 25\n" +
                            "  keys: [l desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [k,l,c,MIN]\n" +
                            "        Filter filter: 100000<c\n" +
                            "            Async JIT Group By workers: 1\n" +
                            "              keys: [k]\n" +
                            "              values: [avg(length(Referer)),count(*),min(Referer)]\n" +
                            "              filter: Referer is not null\n" +
                            "                PageFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q29",
                    "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;",
                    "VirtualRecord\n" +
                            "  functions: [SUM,SUM+COUNT*1,SUM+COUNT*2,SUM+COUNT*3,SUM+COUNT*4,SUM+COUNT*5,SUM+COUNT*6,SUM+COUNT*7,SUM+COUNT*8,SUM+COUNT*9,SUM+COUNT*10,SUM+COUNT*11,SUM+COUNT*12,SUM+COUNT*13,SUM+COUNT*14,SUM+COUNT*15,SUM+COUNT*16,SUM+COUNT*17,SUM+COUNT*18,SUM+COUNT*19,SUM+COUNT*20,SUM+COUNT*21,SUM+COUNT*22,SUM+COUNT*23,SUM+COUNT*24,SUM+COUNT*25,SUM+COUNT*26,SUM+COUNT*27,SUM+COUNT*28,SUM+COUNT*29,SUM+COUNT*30,SUM+COUNT*31,SUM+COUNT*32,SUM+COUNT*33,SUM+COUNT*34,SUM+COUNT*35,SUM+COUNT*36,SUM+COUNT*37,SUM+COUNT*38,SUM+COUNT*39,SUM+COUNT*40,SUM+COUNT*41,SUM+COUNT*42,SUM+COUNT*43,SUM+COUNT*44,SUM+COUNT*45,SUM+COUNT*46,SUM+COUNT*47,SUM+COUNT*48,SUM+COUNT*49,SUM+COUNT*50,SUM+COUNT*51,SUM+COUNT*52,SUM+COUNT*53,SUM+COUNT*54,SUM+COUNT*55,SUM+COUNT*56,SUM+COUNT*57,SUM+COUNT*58,SUM+COUNT*59,SUM+COUNT*60,SUM+COUNT*61,SUM+COUNT*62,SUM+COUNT*63,SUM+COUNT*64,SUM+COUNT*65,SUM+COUNT*66,SUM+COUNT*67,SUM+COUNT*68,SUM+COUNT*69,SUM+COUNT*70,SUM+COUNT*71,SUM+COUNT*72,SUM+COUNT*73,SUM+COUNT*74,SUM+COUNT*75,SUM+COUNT*76,SUM+COUNT*77,SUM+COUNT*78,SUM+COUNT*79,SUM+COUNT*80,SUM+COUNT*81,SUM+COUNT*82,SUM+COUNT*83,SUM+COUNT*84,SUM+COUNT*85,SUM+COUNT*86,SUM+COUNT*87,SUM+COUNT*88,SUM+COUNT*89]\n" +
                            "    GroupBy vectorized: true workers: 1\n" +
                            "      values: [sum(ResolutionWidth),count(*)]\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q30",
                    "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase IS NOT NULL GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [SearchEngineID,ClientIP]\n" +
                            "      values: [count(*),sum(IsRefresh),avg(ResolutionWidth)]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4085
            new TestCase(
                    "Q31",
                    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase IS NOT NULL GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [WatchID,ClientIP]\n" +
                            "      values: [count(*),sum(IsRefresh),avg(ResolutionWidth)]\n" +
                            "      filter: SearchPhrase is not null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4085
            new TestCase(
                    "Q32",
                    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [WatchID,ClientIP]\n" +
                            "      values: [count(*),sum(IsRefresh),avg(ResolutionWidth)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q33",
                    "SELECT URL, COUNT(*) AS c FROM hits ORDER BY c DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [URL]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/5198
            new TestCase(
                    "Q34",
                    "SELECT 1, URL, COUNT(*) AS c FROM hits ORDER BY c DESC LIMIT 10;",
                    "Sort light lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [1,URL,c]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [URL]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: hits\n"
            ),
            // TODO: https://github.com/questdb/questdb/issues/4141
            new TestCase(
                    "Q35",
                    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;",
                    "Sort light lo: 10\n" +
                            "  keys: [c desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [ClientIP,column,column1,column2,c]\n" +
                            "        Async Group By workers: 1\n" +
                            "          keys: [ClientIP,column,column1,column2]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: null\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: hits\n"
            ),
            new TestCase(
                    "Q36",
                    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND DontCountHits = 0 AND IsRefresh = 0 AND URL IS NOT NULL GROUP BY URL ORDER BY PageViews DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [PageViews desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [URL]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and DontCountHits=0 and IsRefresh=0 and URL is not null)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q37",
                    "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND DontCountHits = 0 AND IsRefresh = 0 AND Title IS NOT NULL GROUP BY Title ORDER BY PageViews DESC LIMIT 10;",
                    "Long top K lo: 10\n" +
                            "  keys: [PageViews desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [Title]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and DontCountHits=0 and IsRefresh=0 and Title is not null)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q38",
                    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 1000, 1010;",
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [URL]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and IsRefresh=0 and IsLink!=0 and IsDownload=0)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q39",
                    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 1000, 1010;",
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst,PageViews]\n" +
                            "        Async JIT Group By workers: 1\n" +
                            "          keys: [TraficSourceID,SearchEngineID,AdvEngineID,Src,Dst]\n" +
                            "          values: [count(*)]\n" +
                            "          filter: (CounterID=62 and IsRefresh=0)\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: hits\n" +
                            "                  intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q40",
                    "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 100, 110;",
                    "Sort light lo: 100 hi: 110\n" +
                            "  keys: [PageViews desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [URLHash,EventDate]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and IsRefresh=0 and TraficSourceID in [-1,6] and RefererHash=3594120000172545465L)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q41",
                    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-01T00:00:00Z' AND EventTime <= '2013-07-31T23:59:59Z' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10000, 10010;",
                    "Sort light lo: 10000 hi: 10010\n" +
                            "  keys: [PageViews desc]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [WindowClientWidth,WindowClientHeight]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and IsRefresh=0 and DontCountHits=0 and URLHash=2868770270353813622L)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-01T00:00:00.000000Z\",\"2013-07-31T23:59:59.000000Z\")]\n"
            ),
            new TestCase(
                    "Q42",
                    "SELECT EventTime AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventTime >= '2013-07-14T00:00:00Z' AND EventTime <= '2013-07-15T23:59:59Z' AND IsRefresh = 0 AND DontCountHits = 0 SAMPLE BY 1m ALIGN TO CALENDAR ORDER BY M LIMIT 1000, 1010;",
                    "Sort light lo: 1000 hi: 1010\n" +
                            "  keys: [M]\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      keys: [M]\n" +
                            "      values: [count(*)]\n" +
                            "      filter: (CounterID=62 and IsRefresh=0 and DontCountHits=0)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: hits\n" +
                            "              intervals: [(\"2013-07-14T00:00:00.000000Z\",\"2013-07-15T23:59:59.000000Z\")]\n"
            ),
    };

    @Test
    public void testExecutionPlans() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);

            for (TestCase testCase : TEST_CASES) {
                LOG.info().$("verifying exec plan for ").$(testCase.name).$();
                assertPlanNoLeakCheck(testCase.query, testCase.expectedPlan);
            }
        });
    }

    private static class TestCase {
        final String expectedPlan;
        final String name;
        final String query;

        private TestCase(String name, String query, String expectedPlan) {
            this.name = name;
            this.query = query;
            this.expectedPlan = expectedPlan;
        }
    }
}
