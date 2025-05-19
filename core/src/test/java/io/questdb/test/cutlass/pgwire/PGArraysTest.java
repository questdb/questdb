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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.PGConnection;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class PGArraysTest extends BasePGTest {

    private final Rnd bufferSizeRnd = TestUtils.generateRandom(LOG);
    private final boolean walEnabled;

    public PGArraysTest(WalMode walMode) {
        super(LegacyMode.MODERN);
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL},
                {WalMode.NO_WAL},
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        selectCacheBlockCount = -1;
        sendBufferSize = 512 * (1 + bufferSizeRnd.nextInt(15));
        forceSendFragmentationChunkSize = (int) (10 + bufferSizeRnd.nextInt(Math.min(512, sendBufferSize) - 10) * bufferSizeRnd.nextDouble() * 1.2);

        recvBufferSize = 512 * (1 + bufferSizeRnd.nextInt(15));
        forceRecvFragmentationChunkSize = (int) (10 + bufferSizeRnd.nextInt(Math.min(512, recvBufferSize) - 10) * bufferSizeRnd.nextDouble() * 1.2);

        LOG.info().$("fragmentation params [sendBufferSize=").$(sendBufferSize)
                .$(", forceSendFragmentationChunkSize=").$(forceSendFragmentationChunkSize)
                .$(", recvBufferSize=").$(recvBufferSize)
                .$(", forceRecvFragmentationChunkSize=").$(forceRecvFragmentationChunkSize)
                .I$();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, true);
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testArrayBind() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[], ts timestamp) timestamp(ts) partition by hour")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?, ?)")) {
                Array arr = connection.createArrayOf("int8", new Double[]{1d, 2d, 3d, 4d, 5d});
                stmt.setArray(1, arr);
                stmt.setTimestamp(2, new java.sql.Timestamp(0));
                stmt.execute();
            }

            drainWalQueue();

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY],ts[TIMESTAMP]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0},1970-01-01 00:00:00.0\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testArrayBindWithNull() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                Array arr = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d, null, 5d});
                stmt.setArray(1, arr);
                stmt.execute();
                Assert.fail("Nulls in arrays are not supported");
            } catch (SQLException e) {
                String msg = e.getMessage();
                // why asserting 2 different messages?
                // in some modes PG JDBC sends array as string and relies in implicit casting. in this case we get a more generic 'inconvertible value' error
                // in other modes PG JDBC sends array as binary array and server does not do implicit casting. in this case we get a more specific 'nulls not supported in arrays' error
                Assert.assertTrue("'" + msg + "' does not contain the expected error", msg.contains("null elements are not supported in arrays") || msg.contains("inconvertible value"));
            }
        });
    }

    @Test
    public void testArrayInsertWrongDimensionCount() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            // fewer dimensions than expected
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                Array arr = connection.createArrayOf("float8", new Double[]{1d, 2d, 3d, 4d, 5d});
                stmt.setArray(1, arr);
                stmt.execute();
                Assert.fail("Wrong array dimension count should fail");
            } catch (SQLException ex) {
                TestUtils.assertContainsEither(ex.getMessage(), "inconvertible value", // text mode: implicit cast from string
                        "array type mismatch [expected=DOUBLE[][], actual=DOUBLE[]]" // binary array
                );
            }

            // more dimensions than expected
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                Array arr = connection.createArrayOf("float8", new Double[][][]{{{1d, 2d, 3d, 4d, 5d}}});
                stmt.setArray(1, arr);
                stmt.execute();
                Assert.fail("Wrong array dimension count should fail");
            } catch (SQLException ex) {
                TestUtils.assertContainsEither(ex.getMessage(), "inconvertible value", // text mode: implicit cast from string
                        "array type mismatch [expected=DOUBLE[][], actual=DOUBLE[][][]]" // binary array
                );
            }
        });
    }

    @Test
    public void testArrayMaxDimensions() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            int dimCount = ColumnType.ARRAY_NDIMS_LIMIT + 1;
            try (PreparedStatement statement = connection.prepareStatement("select ? as arr from long_sequence(1);")) {
                sink.clear();
                int[] dims = new int[dimCount];
                Arrays.fill(dims, 1);
                final Object arr = java.lang.reflect.Array.newInstance(double.class, dims);

                Object lastArray = arr;
                for (; ; ) {
                    Object element = java.lang.reflect.Array.get(lastArray, 0);
                    if (!element.getClass().isArray()) {
                        break;
                    }
                    lastArray = element;
                }
                java.lang.reflect.Array.set(lastArray, 0, 1.0);


                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                statement.setArray(1, pgConnection.createArrayOf("float8", arr));

                try (ResultSet rs = statement.executeQuery()) {
                    // in some modes PG JDBC sends array as string without any type information
                    // in this case the query execution may succeed.
                    int columnType = rs.getMetaData().getColumnType(1);
                    Assert.assertEquals(Types.VARCHAR, columnType);
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().contains("array dimensions cannot be greater than maximum array dimensions [dimensions=33, max=32]"));
                }
            }
        });
    }

    @Test
    public void testArrayNonFinite() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {

            // we have to explicitly cast to double[] since in the Simple Mode PG JDBC sends array as string:
            // `select ('{"NaN","-Infinity","Infinity","0.0"}') as arr from long_sequence(1)`
            // and the server has no way to tell it should be an array. casting forces server to treat it as an array
            try (PreparedStatement statement = connection.prepareStatement("select ?::double[] as arr from long_sequence(1);")) {
                sink.clear();
                double[] arr = new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.0};
                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                statement.setArray(1, pgConnection.createArrayOf("float8", arr));

                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet("arr[ARRAY]\n" +
                                    "{NaN,-Infinity,Infinity,0.0}\n",
                            sink, rs);
                }
            }
        });
    }

    @Test
    public void testArrayResultSet() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table xd as (select rnd_double_array(2, 9) from long_sequence(5))");

            try (PreparedStatement statement = connection.prepareStatement("select * from xd;")) {
                sink.clear();
                try (ResultSet rs = statement.executeQuery()) {
                    assertResultSet("rnd_double_array[ARRAY]\n" +
                                    "{{0.12966659791573354,0.299199045961845,0.9344604857394011,0.8423410920883345,NaN,0.22452340856088226,0.3491070363730514,0.7611029514995744,0.4217768841969397,0.0367581207471136,0.6276954028373309,0.6778564558839208,0.8756771741121929,0.8799634725391621,NaN}}\n" +
                                    "{{0.7675673070796104,0.21583224269349388,0.15786635599554755,0.1911234617573182,0.5793466326862211,0.9687423276940171,0.6761934857077543,0.4882051101858693,0.42281342727402726,0.810161274171258},{0.5298405941762054,NaN,0.8445258177211064,0.29313719347837397,0.8847591603509142,0.4900510449885239,0.8258367614088108,0.04142812470232493,0.92050039469858,NaN},{0.456344569609078,NaN,0.40455469747939254,0.8837421918800907,NaN,0.8828228366697741,NaN,0.45659895188239796,0.9566236549439661,0.5406709846540508},{0.8164182592467494,NaN,0.5449155021518948,0.1202416087573498,0.9640289041849747,0.7133910271555843,0.6551335839796312,0.4971342426836798,0.48558682958070665,NaN},{0.44804689668613573,0.34947269997137365,0.19751370382305056,0.812339703450908,0.7176053468281931,0.5797447096307482,0.9455893004802433,0.7446000371089992,0.3045253310626277,0.24079155981438216},{0.10643046345788132,0.5244255672762055,0.0171850098561398,0.09766834710724581,0.6697969295620055,0.9759534636690222,0.7632615004324503,0.8816905018995145,NaN,0.5357010561860446},{0.8595900073631431,NaN,0.7458169804091256,0.33746104579374825,0.18740488620384377,NaN,0.7777024823107295,0.8221637568563206,0.22631523434159562,0.18336217509438513},{0.9862476361578772,0.8693768930398866,0.8189713915910615,NaN,0.7365115215570027,0.9859070322196475,0.9884011094887449,0.9457212646911386,0.05024615679069011,0.9946372046359034},{0.6940904779678791,0.5391626621794673,0.7668146556860689,0.2065823085842221,0.750281471677565,0.6590829275055244,0.5708643723875381,0.3568111021227658,0.05758228485190853,0.6729405590773638},{0.1010501916946902,0.35731092171284307,0.9583687530177664,0.4609277382153818,NaN,0.40791879008699594,0.7694744648762927,0.8720995238279701,0.892454783921197,NaN},{0.706473302224657,0.7422641630544511,0.04173263630897883,0.5677191487344088,0.2677326840703891,0.5425297056895126,0.09618589590900506,0.4835256202036067,0.868788610834602,0.49154607371672154},{0.4167781163798937,0.3454148777596554,NaN,0.11951216959925692,NaN,0.10227682008381178,0.7873229912811514,0.04001697462715281,0.03973283003449557,0.3460851141092931},{0.8321000514308267,0.18852800970933203,0.6226001464598434,0.4346135812930124,0.8786111112537701,0.996637725831904,0.6334964081687151,0.6721404635638454,0.7707249647497968,0.8813290192134411}}\n" +
                                    "{{NaN,0.823395724427589,0.007985454958725269,0.5090837921075583,NaN,NaN,0.2394591643144588,0.9067923725015784,0.19736767249829557,0.7165847318191405,0.7530490055752911,0.8878046922699878,0.49199001716312474,0.6292086569587337,0.37873228328689634,0.9958979595782157},{0.33976095270593043,NaN,0.8402964708129546,0.7732229848518976,0.587752738240427,0.4667778758533798,0.17498425722537903,0.9797944775606992,0.7795623293844108,0.29242748475227853,0.7527907209539796,0.9934423708117267,0.4834201611292943,NaN,0.4698648140712085,0.8911615631017953},{0.32093405888189597,0.8406396365644468,0.34224858614452547,0.27068535446692277,0.0031075670450616544,NaN,0.865629565918467,0.8574212636138532,0.8280460741052847,0.7842455970681089,0.31617860377666984,0.3889200123396954,0.933609514582851,0.6379992093447574,0.8514849800664227,0.5010137919618508},{0.34804764389663523,0.02958396857215828,0.13312214396754163,0.9435138098640453,0.5025413806877073,0.15369837085455984,NaN,0.5449970817079417,NaN,0.8405815493567417,0.3058008320091107,0.31852531484741486,0.5598187089718925,0.7566252942139543,NaN,NaN},{0.7717552767944976,NaN,NaN,0.3153349572730255,0.4627885105398635,0.4028291715584078,0.2000682450929353,0.6021005466885047,0.5501133139397699,0.7134500775259477,NaN,0.734728770956117,0.33828954246335896,NaN,0.18967967822948184,0.48422587819911567},{0.2970515836513553,0.959524136522573,0.5160053477987824,0.136546465910663,0.8796413468565342,0.9469700813926907,0.41496612044075665,NaN,0.36078878996232167,0.600707072503926,0.7397816490927717,0.743599174001969,0.8353079103853974,0.011099265671968506,0.6671244607804027,0.11947100943679911}}\n" +
                                    "{{0.09977691656157406,0.5335953576307257},{0.0652033813358841,0.1353529674614602},{0.5788151025779464,0.733837988805042},{0.7468602267994937,0.55200903114214},{0.3489278573518253,0.012228951216584294},{0.9316283568969537,0.3663509090570607}}\n" +
                                    "{{0.6940917925148332,0.33924820213821316,0.30062011052460846,0.4412051102084278,0.7783351753890267,0.33046819455237,0.97613283653158,0.3124458010612313,0.1350821238488883,0.3397922134720558,0.8376372223926546,0.365427022047211},{0.5921457770297527,0.8486538207666282,0.15121120303896474,0.9370193388878216,0.39201296350741366,0.5700419290086554,0.16064467510169633,0.0846754178136283,0.5765797240495835,0.4913342104187668,NaN,0.3189857960358504},{0.020390884194626757,0.38881940598288367,0.4444125234732249,NaN,0.9531459048178456,0.5261234649527643,0.030750139424332357,0.20921704056371593,0.681606585145203,0.11134244333117826,0.08109202364673884,0.2362963290561556},{NaN,0.3242526975448907,0.42558021324800144,0.7903520704337446,0.9534844124580377,NaN,0.6315327885922489,0.9926343068414145,0.6361737673041902,0.4523282839107191,0.442095410281938,0.5394562515552983},{0.7999403044078355,0.5675831821917149,0.4634306737694158,0.7259967771911617,NaN,0.8151917094201774,0.39211484750712344,0.16979644136429572,0.28122627418701307,0.2088152045027989,0.3504695674352035,0.40609845936584743},{0.041230021906704994,NaN,0.6852762111021103,0.08039440728458325,0.7600550885615773,0.05890936334115593,0.023600615130049185,0.13525597398079747,0.10663485323987387,0.05995797344646303,0.7600677648426976,0.8887793365099876},{0.06820168647245783,0.9750738231283522,0.8277715252854949,0.3838563277145236,0.647875746786617,0.33435665839497086,0.5929911960174489,NaN,0.09854153834719315,0.2328552828087207,0.8335063783919325,0.9321703394650436},{0.48465267676967827,0.14756353014849555,0.25604136769205754,0.44172683242560085,0.2696094902942793,0.899050586403365,0.3503522147575858,0.7389772880219149,0.22371932699681862,0.019529452719755813,0.5330584032999529,NaN},{0.23846285137007717,0.3219671642299694,0.8922034386034273,0.7910659228440695,0.9578716688144072,0.9674352881185491,0.30878646825073175,0.7291265477629812,0.5079751443209725,0.9021521846995424,0.4104855595304533,0.9183493071613609},{0.011263511839942453,NaN,0.9176263114713273,0.3981872443575455,0.6504194217741501,0.23387203820756874,0.27144997281940675,0.858967821197869,0.1900488162112337,0.3074049506535589,0.8658616916564643,0.6382226254026024}}\n",
                            sink, rs);
                }
            }
        }, () -> {
            sendBufferSize = 1000 * 1024; // use large enough buffer, otherwise we will get fragmented messages and this currently leads to non-deterministic results of rnd_double_array
        });
    }

    @Test
    public void testArrayStringResult() throws Exception {
        skipOnWalRun();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("select current_schemas(true) from long_sequence(1)")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("current_schemas[VARCHAR]\n" +
                                    "{public}\n",
                            sink,
                            rs
                    );
                }
            }
        });

    }

    @Test
    public void testArrayUpdateBind() throws Exception {
        // todo: binding array vars in UPDATE statement does not work in WAL mode!
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[], i int, ts timestamp) timestamp(ts) partition by hour")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?, ?, ?)")) {
                stmt.setArray(1, connection.createArrayOf("int8", new Double[]{1d, 2d, 3d, 4d, 5d}));
                stmt.setInt(2, 0);
                stmt.setTimestamp(3, new java.sql.Timestamp(0));
                stmt.execute();

                stmt.setArray(1, connection.createArrayOf("int8", new Double[]{6d, 7d, 8d, 9d, 10d}));
                stmt.setInt(2, 1);
                stmt.setTimestamp(3, new java.sql.Timestamp(1));
                stmt.execute();
            }

            drainWalQueue();

            // sanity check
            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY],i[INTEGER],ts[TIMESTAMP]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0},0,1970-01-01 00:00:00.0\n" +
                                    "{6.0,7.0,8.0,9.0,10.0},1,1970-01-01 00:00:00.001\n",
                            sink,
                            rs
                    );
                }
            }

            try (PreparedStatement stmt = connection.prepareStatement("update x set al = ? where i = ?")) {
                stmt.setArray(1, connection.createArrayOf("int8", new Double[]{11d, 12d, 13d, 14d, 15d}));
                stmt.setInt(2, 1);
                stmt.execute();
            }
            drainWalQueue();

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY],i[INTEGER],ts[TIMESTAMP]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0},0,1970-01-01 00:00:00.0\n" +
                                    "{11.0,12.0,13.0,14.0,15.0},1,1970-01-01 00:00:00.001\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testExplicitCastInsertStringToArrayColum() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values ('{1,2,3,4,5}'::double[])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0}\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    @Ignore("todo: this currently fail with binary encoding since client BIND message includes the number of dimensions: 1, " +
            "but the table is created with 2 dimensions. should we allow implicit casting of empty arrays to any dimension?")
    public void testImplicitCastingOfEmptyArraysToDifferentDimension() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                stmt.setObject(1, new double[0]);
                stmt.execute();

                assertPgWireQuery(connection,
                        "select * from x",
                        "al[ARRAY]\n" +
                                "{}\n");
            }
        });
    }

    @Test
    public void testInsertEmptyArray() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                Array arr = connection.createArrayOf("int8", new Double[]{});
                stmt.setArray(1, arr);
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY]\n" +
                                    "{}\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testInsertJaggedArray() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {

            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6, 7]] arr FROM long_sequence(1)");
                fail("jagged array should not be allowed");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "element counts in sub-arrays don't match");
            }

            // explicit cast of a jagged array produces null
            assertPgWireQuery(connection, "SELECT '{{1.0, 2}, {3, 4}, {5, 6, 7}}'::double[] arr FROM long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "null\n");


            execute("create table tab (arr double[][])");

            // implicit cast should fail
            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into tab values ('{{1.0, 2}, {3, 4}, {5, 6, 7}}')");
                fail("jagged array should not be allowed");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value");
            }

            // explicit cast of a bad array should insert null
            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into tab values ('{{1.0, 2}, {3, 4}, {5, 6, 7}}'::double[][])");
            }
            assertPgWireQuery(connection, "SELECT * from tab",
                    "arr[ARRAY]\n" +
                            "null\n");


            // Issue: PostgreSQL JDBC driver doesn't validate jagged arrays (https://github.com/pgjdbc/pgjdbc/issues/3567)
            // when used as a bind variable in a prepared statement.
            // QuestDB server must validate and reject them instead.
            //
            // Validation approaches:
            // 1. Text-encoded arrays: Simple - casting from string to array always includes jagged array checks
            // 2. Binary protocol: More complex - we can only verify that the received binary array has the expected
            //    number of elements based on the declared array shape.
            //
            // Binary protocol limitation example:
            // For a jagged array like {{1.0, 2.0}, {3.0}, {5.0, 6.0, 7.0}}:
            // - PG JDBC reports: 2D array, first dimension has 3 elements, second dimension has 2 elements
            // - Server expects 6 elements total (3×2), and client message contains 6 elements
            // - Server accepts the message and inserts as {{1.0, 2.0}, {3.0, 4.0}, {5.0, 6.0}}
            // - No way to detect the original jaggedness :(
            // Conclusion: Clients should validate arrays before sending to server
            try (PreparedStatement stmt = connection.prepareStatement("insert into tab values (?)")) {
                Array arr = connection.createArrayOf("double", new double[][]{{1.0, 2.0}, {3.0}, {3.0}});
                stmt.setArray(1, arr);
                try {
                    stmt.execute();
                    Assert.fail("jagged array should not be allowed");
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(Chars.contains(msg, "inconvertible value") || Chars.contains(msg, "unexpected array size"));
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement("insert into tab values (?)")) {
                Array arr = connection.createArrayOf("double", new double[][]{{1.0}, {2.0, 3.0}, {4.0, 5.0}});
                stmt.setArray(1, arr);
                try {
                    stmt.execute();
                    Assert.fail("jagged array should not be allowed");
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(Chars.contains(msg, "inconvertible value") || Chars.contains(msg, "unexpected array size"));
                }
            }
        });
    }

    @Test
    public void testInsertStringToArrayColum() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("insert into x values ('{{1,2},{3,4}}')")) {
                stmt.execute();
            }

            // insert null, this is easy since casting inform the server about the type we are about to insert
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (null::string)")) {
                stmt.execute();
            }

            try (PreparedStatement stmt = connection.prepareStatement("select * from x")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("al[ARRAY]\n" +
                                    "{{1.0,2.0},{3.0,4.0}}\n" +
                                    "null\n",
                            sink,
                            rs
                    );
                }
            }

            // a null array can be inserted using setObject - this is harder since the server doesn't know the type
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                // force client to send PARSE with an unknown type
                stmt.setObject(1, null);
                stmt.execute();

                assertPgWireQuery(connection,
                        "select * from x",
                        "al[ARRAY]\n" +
                                "{{1.0,2.0},{3.0,4.0}}\n" +
                                "null\n" +
                                "null\n"); // the null we just inserted
            }

            // now try an empty array explicitly serialized as a string
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values (?)")) {
                stmt.setObject(1, "{}");
                stmt.execute();

                assertPgWireQuery(connection,
                        "select * from x",
                        "al[ARRAY]\n" +
                                "{{1.0,2.0},{3.0,4.0}}\n" +
                                "null\n" +
                                "null\n" +
                                "{}\n"); // the empty array we just inserted
            }
        });
    }

    @Test
    public void testInsertStringToArrayColum_negativeScenarios() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("create table x (al double[][])")) {
                stmt.execute();
            }

            // bad dimension count
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values ('{1,2,3,4}')")) {
                stmt.execute();
                Assert.fail("inserted 1D array into 2D column");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: `{1,2,3,4}` [STRING -> DOUBLE[][]]");
            }

            // inconsistent row sizes
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values ('{{1,2},{3,4,5}}')")) {
                stmt.execute();
                Assert.fail("inserted 2D array with different row sizes");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: `{{1,2},{3,4,5}}` [STRING -> DOUBLE[][]]");
            }

            // bad literal
            try (PreparedStatement stmt = connection.prepareStatement("insert into x values ('{{1,2},{3,a}}')")) {
                stmt.execute();
                Assert.fail("inserted bad array literal");
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: `{{1,2},{3,a}}` [STRING -> DOUBLE[][]]");
            }
        });
    }

    @Test
    public void testSendBufferOverflowNonVanilla() throws Exception {
        Assume.assumeTrue(walEnabled);
        int dimLen1 = 10 + bufferSizeRnd.nextInt(90);
        int dimLen2 = 10 + bufferSizeRnd.nextInt(90);
        String literal = buildArrayLiteral2d(dimLen1, dimLen2);
        String result = buildArrayResult2d(1, dimLen1, 1, dimLen2);
        assertWithPgServer(Mode.EXTENDED, true, -1, (conn, binary, mode, port) -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tango AS (SELECT x n, " + literal + " arr FROM long_sequence(1))");
            }
            try (PreparedStatement stmt = conn.prepareStatement("SELECT arr[2:,2:] arr FROM tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("arr[ARRAY]\n" +
                                    result + "\n",
                            sink, rs);
                }
            }
        }, () -> {
            recvBufferSize = 5 * dimLen1 * dimLen2;
            forceRecvFragmentationChunkSize = Integer.MAX_VALUE;
        });
    }

    @Test
    public void testSendBufferOverflowVanilla() throws Exception {
        Assume.assumeTrue(walEnabled);
        int elemCount = 100 + bufferSizeRnd.nextInt(900);
        String arrayLiteral = buildArrayLiteral1d(elemCount);
        assertWithPgServer(Mode.EXTENDED, true, -1, (conn, binary, mode, port) -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT x n, " + arrayLiteral + " arr FROM long_sequence(1)")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("n[BIGINT],arr[ARRAY]\n" +
                                    "1," + buildArrayResult1d(elemCount) + "\n",
                            sink, rs);
                }
            }
        }, () -> {
            recvBufferSize = 4 * elemCount;
            forceRecvFragmentationChunkSize = Integer.MAX_VALUE;
        });
    }

    @Test
    public void testSliceArray() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertPgWireQuery(connection,
                    "SELECT arr[1:2] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{{1.0,2.0}}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[2:] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{{3.0,4.0},{5.0,6.0}}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[3:, 1:2] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{{5.0}}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[3:, 2] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{6.0}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[1:3] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{{1.0,2.0},{3.0,4.0}}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[1:3, 1:2] slice FROM tango",
                    "slice[ARRAY]\n" +
                            "{{1.0},{3.0}}\n");
            assertPgWireQuery(connection,
                    "SELECT arr[2, 2] element FROM tango",
                    "element[DOUBLE]\n" +
                            "4.0\n");
        });
    }

    @Test
    public void testStringToArrayCast() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement stmt = connection.prepareStatement("select '{\"1\",\"2\",\"3\",\"4\",\"5\"}'::double[] from long_sequence(1)")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("cast[ARRAY]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0}\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }

    @Test
    public void testTypeRewrites() throws Exception {
        skipOnWalRun();

        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            assertPgWireQuery(connection,
                    "select '{1}'::double[] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{1.0}\n");

            assertPgWireQuery(connection,
                    "select '{1}'::float[] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{1.0}\n");

            assertPgWireQuery(connection,
                    "select '{1}'::float8[] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{1.0}\n");

            assertPgWireQuery(connection,
                    "select '{1}'::double precision[] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{1.0}\n");

            assertPgWireQuery(connection,
                    "select '{{1},{2}}'::double[][] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{{1.0},{2.0}}\n");

            assertPgWireQuery(connection,
                    "select '{{1},{2}}'::douBLE pREciSioN[][] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{{1.0},{2.0}}\n");

            assertPgWireQuery(connection,
                    "select '{{1},{2}}'::float8[][] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{{1.0},{2.0}}\n");

            assertPgWireQuery(connection,
                    "select '{{1},{2}}'::fLOAt[][] as arr from long_sequence(1)",
                    "arr[ARRAY]\n" +
                            "{{1.0},{2.0}}\n");
        });
    }

    private void assertPgWireQuery(Connection conn, String query, CharSequence expected) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            sink.clear();
            try (ResultSet rs = stmt.executeQuery()) {
                assertResultSet(expected, sink, rs);
            }
        }
    }

    private @NotNull String buildArrayLiteral1d(int elemCount) {
        StringBuilder b = new StringBuilder();
        b.append("ARRAY");
        buildArrayLiteralInner(b, 0, elemCount);
        return b.toString();
    }

    private @NotNull String buildArrayLiteral2d(int dimLen1, int dimLen2) {
        StringBuilder b = new StringBuilder();
        b.append("ARRAY[");
        String comma = "";
        for (int i = 0; i < dimLen1; i++) {
            b.append(comma);
            comma = ",";
            buildArrayLiteralInner(b, i * dimLen2, (i + 1) * dimLen2);
        }
        b.append(']');
        return b.toString();
    }

    private void buildArrayLiteralInner(StringBuilder b, int lowerBound, int upperBound) {
        b.append('[');
        String comma = "";
        for (int i = lowerBound; i < upperBound; i++) {
            b.append(comma);
            comma = ",";
            b.append(i);
        }
        b.append(']');
    }

    private @NotNull String buildArrayResult1d(int elemCount) {
        StringBuilder b = new StringBuilder();
        buildArrayResultInner(0, elemCount, b);
        return b.toString();
    }

    private @NotNull String buildArrayResult2d(int start1, int dimLen1, int start2, int dimLen2) {
        StringBuilder b = new StringBuilder();
        b.append("{");
        String comma = "";
        for (int i = start1; i < dimLen1; i++) {
            b.append(comma);
            comma = ",";
            buildArrayResultInner(i * dimLen2 + start2, (i + 1) * dimLen2, b);
        }
        b.append("}");
        return b.toString();
    }

    private void buildArrayResultInner(int lowerBound, int upperBound, StringBuilder b) {
        b.append("{");
        String comma = "";
        for (int i = lowerBound; i < upperBound; i++) {
            b.append(comma);
            comma = ",";
            b.append(i).append(".0");
        }
        b.append("}");
    }

    private void skipOnWalRun() {
        Assume.assumeTrue("Test disabled during WAL run.", !walEnabled);
    }


}
