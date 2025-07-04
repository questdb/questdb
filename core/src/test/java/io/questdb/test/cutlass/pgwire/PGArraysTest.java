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
            try (PreparedStatement stmt = connection.prepareStatement(
                    "create table tango (arr double[], ts timestamp) timestamp(ts) partition by hour")
            ) {
                stmt.execute();
            }
            try (PreparedStatement stmt = connection.prepareStatement("insert into tango values (?, ?)")) {
                Array arr = connection.createArrayOf("int8", new Double[]{1d, 2d, 3d, 4d, 5d});
                int pos = 1;
                stmt.setArray(pos++, arr);
                stmt.setTimestamp(pos, new java.sql.Timestamp(0));
                stmt.execute();
            }
            drainWalQueue();
            try (PreparedStatement stmt = connection.prepareStatement("tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("arr[ARRAY],ts[TIMESTAMP]\n" +
                                    "{1.0,2.0,3.0,4.0,5.0},1970-01-01 00:00:00.0\n",
                            sink,
                            rs
                    );
                }
            }
            try (PreparedStatement stmt = connection.prepareStatement("update tango set arr = ?")) {
                Array arr = connection.createArrayOf("int8", new Double[]{9d, 8d, 7d, 6d, 5d});
                int pos = 1;
                stmt.setArray(pos, arr);
                stmt.execute();
            }
            drainWalQueue();
            try (PreparedStatement stmt = connection.prepareStatement("tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("arr[ARRAY],ts[TIMESTAMP]\n" +
                                    "{9.0,8.0,7.0,6.0,5.0},1970-01-01 00:00:00.0\n",
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
                    assertResultSet(
                            "rnd_double_array[ARRAY]\n" +
                                    "{{0.12966659791573354,0.299199045961845,0.9344604857394011,0.8423410920883345,0.19202208853547864,0.5093827001617407,0.11427984775756228,0.5243722859289777,0.8072372233384567,0.2390529010846525,0.6693837147631712,0.3100545983862456,0.1985581797355932,NaN,0.5249321062686694}}\n" +
                                    "{{0.9771103146051203,0.24808812376657652,0.6381607531178513,0.12503042190293423,0.9038068796506872,0.13450170570900255,NaN,0.4882051101858693,NaN,0.7883065830055033,0.7664256753596138,0.3762501709498378},{0.8445258177211064,0.29313719347837397,0.8847591603509142,0.4900510449885239,0.8258367614088108,0.04142812470232493,0.92050039469858,0.5182451971820676,0.8664158914718532,0.17370570324289436,0.5659429139861241,0.8828228366697741},{0.7230015763133606,0.12105630273556178,0.11585982949541473,0.9703060808244087,0.8685154305419587,0.325403220015421,0.769238189433781,0.6230184956534065,0.42020442539326086,0.5891216483879789,NaN,0.6752509547112409},{NaN,0.9047642416961028,0.03167026265669903,0.14830552335848957,0.9441658975532605,0.3456897991538844,0.24008362859107102,0.619291960382302,0.17833722747266334,0.2185865835029681,0.3901731258748704,0.7056586460237274},{NaN,NaN,0.07246172621937097,NaN,NaN,0.09766834710724581,0.6697969295620055,0.9759534636690222,NaN,0.22895725920713628,0.9820662735672192,0.5357010561860446},{0.8595900073631431,0.6583311519893554,0.8259739777067459,0.8593131480724349,0.33747075654972813,0.11785316212653119,0.7445998836567925,0.2825582712777682,0.2711532808184136,0.48524046868499715,0.6797562990945702,0.7381752894013154},{0.7365115215570027,0.9859070322196475,0.9884011094887449,0.9457212646911386,0.05024615679069011,0.9946372046359034,0.6940904779678791,0.5391626621794673,0.7668146556860689,0.2065823085842221,0.750281471677565,0.6590829275055244},{0.5708643723875381,0.3568111021227658,0.05758228485190853,0.6729405590773638,0.1010501916946902,0.35731092171284307,0.9583687530177664,NaN,0.8977236684869918,0.40791879008699594,0.7694744648762927,0.8720995238279701},{0.892454783921197,0.09303344348778264,0.5913874468544745,0.08890450062949395,0.1264215196329228,0.7215959171612961,0.4440250924606578,0.6810852005509421,NaN,NaN,0.08675950660182763,0.7292482367451514},{0.6107894368996438,0.9303144555389662,0.05514933756198426,0.11951216959925692,0.7404912278395417,0.08909442703907178,0.8439276969435359,NaN,NaN,0.08712007604601191,0.8551850405049611,0.18586435581637295},{0.5637742551872849,NaN,0.6213434403332111,0.2559680920632348,0.23493793601747937,0.5150229280217947,0.18158967304439033,0.8196554745841765,0.9130151105125102,0.7877587105938131,0.4729022357373792,0.7665029914376952},{NaN,0.5090837921075583,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858,0.3521084750492214,0.1511578096923386,0.18746631995449403,NaN},{0.5779007672652298,0.5692090442741059,0.7467013668130107,0.5794665369115236,0.13210005359166366,0.5762044047105472,0.988853350870454,0.7202789791127316,0.34257201464152764,NaN,NaN,0.29242748475227853},{0.7527907209539796,0.9934423708117267,NaN,0.848083900630095,0.4698648140712085,0.8911615631017953,NaN,0.11047315214793696,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025},{0.865629565918467,NaN,0.970570224065161,0.37286547899075506,0.11624252077059061,0.9205584285421768,0.21498295033639603,0.943246566467627,0.17202485647400034,0.7253202715679453,0.4268921400209912,0.9997797234031688}}\n" +
                                    "{{0.13312214396754163,0.9435138098640453,0.5025413806877073,0.15369837085455984,0.5083087912946505,0.537020248377422,0.8766908646423737,0.44638626240707313,0.034652347087289925,0.605050319285447,0.9683642405595932,0.3549235578142891,0.04211401699125483,0.3153349572730255},{0.4627885105398635,0.4028291715584078,0.2000682450929353,0.6021005466885047,0.5501133139397699,0.7134500775259477,NaN,0.734728770956117,NaN,0.8977957942059742,0.18967967822948184,0.48422587819911567,0.2970515836513553,0.959524136522573}}\n" +
                                    "{{0.3218450864634881,0.8034049105590781,0.04404000858917945,0.14295673988709012,0.03314618075579956,0.8383060222517912,NaN,0.15241451173695408,0.10799057399629297,0.0742752229468211,0.9001273812517414,0.5629104624260136,0.6001215594928115},{0.8920252905736616,0.09977691656157406,0.5335953576307257,0.0652033813358841,0.1353529674614602,0.5788151025779464,0.733837988805042,0.7468602267994937,0.55200903114214,0.3489278573518253,0.012228951216584294,0.9316283568969537,0.3663509090570607},{0.88982264111644,NaN,0.10820602386069589,0.4564667537900823,0.5380626833618448,0.23231935170792306,0.533524384058538,0.6749208267946962,0.2625424312419562,0.9153044839960652,NaN,0.3397922134720558,0.8376372223926546},{0.365427022047211,0.5921457770297527,0.8486538207666282,0.15121120303896474,0.9370193388878216,0.39201296350741366,0.5700419290086554,0.16064467510169633,0.0846754178136283,0.5765797240495835,NaN,0.7055404165623212,0.3189857960358504},{0.020390884194626757,0.38881940598288367,0.4444125234732249,0.42044603754797416,0.47603861281459736,0.9815126662068089,NaN,0.20921704056371593,0.681606585145203,0.11134244333117826,0.08109202364673884,NaN,0.2103287968720018},{0.3242526975448907,0.42558021324800144,0.7903520704337446,NaN,0.7617663592833062,0.6315327885922489,0.9926343068414145,0.6361737673041902,0.4523282839107191,0.442095410281938,0.5394562515552983,0.7999403044078355,NaN},{0.5823910118974169,0.05942010834028011,0.9849599785483799,0.8151917094201774,0.39211484750712344,0.16979644136429572,0.28122627418701307,0.2088152045027989,0.3504695674352035,NaN,NaN,0.45516613187238597,0.3228786903275197},{0.053286806650773566,0.3448217091983955,NaN,0.6884149023727977,0.829011977070579,0.026319297183393875,0.9305245743190473,0.6246882387989457,0.0049253368387782714,0.8973562700864572,0.5916419486433518,0.7280036952357564,0.6404197786416339},{0.828928908465152,0.40462097000890584,NaN,0.33435665839497086,0.5929911960174489,0.4758209004780879,NaN,0.2328552828087207,0.8335063783919325,0.9321703394650436,0.48465267676967827,0.14756353014849555,0.25604136769205754},{0.44172683242560085,0.2696094902942793,0.899050586403365,0.3503522147575858,0.7389772880219149,0.22371932699681862,0.019529452719755813,0.5330584032999529,0.9766284858951397,NaN,0.3219671642299694,0.8922034386034273,NaN},{NaN,0.7128505998532723,0.5940502728139653,0.7861254708288083,0.1319044042993568,NaN,0.5079751443209725,0.9021521846995424,0.4104855595304533,0.9183493071613609,0.011263511839942453,0.37418657418528656,0.8457748651234394},{0.6951385535362276,0.2824076895992761,0.9109198044456538,0.7573042043889733,NaN,0.1900488162112337,0.3074049506535589,0.8658616916564643,0.6382226254026024,NaN,0.6039174661672239,0.7874929839944909,0.6320839159367109}}\n" +
                                    "{{0.7659949103776245,0.8404550497056366,0.9379038084870472,0.8074158211799941,0.4613501223216129,0.056099346819408535,0.6927480038605662,0.6355534187114189,0.19073234832401043,0.14105044938124922,0.236380596505666,0.009379969318743853,0.7564214859398338,0.06970926959068269},{0.45388767393986074,0.8645117110218422,0.7298540433653912,0.588707402066503,0.4701492486769596,0.7407568814442186,0.3123904307505546,0.19188599215569557,0.43990342764801993,0.6130518815428464,NaN,NaN,0.2875739269292986,0.9347912212983339},{0.4960818612914606,0.5089437674017353,0.9154548873622441,0.8470755372946043,0.8061988461374605,NaN,0.770831153825552,NaN,NaN,0.6434872763996672,0.13607574134551215,0.4510483042269017,0.1779779439502811,0.4338972476284021},{0.8810670433021828,0.09671321049914583,NaN,0.8440228885347915,0.4374309393168063,0.8151906661765794,0.7902682918274309,0.8740701330165472,0.4176571781712538,0.47486309648420666,NaN,NaN,0.9325892008297832,0.7200031730502818},{0.5133738535522754,0.9112430321917995,0.046172168224761334,0.6391251028594114,0.5140909855075729,0.08977959272389147,0.5613174142074612,0.48432558936820347,0.7729111631116361,0.19245855538083634,0.8245822920507528,0.8235056484964091,0.8645536237512511,0.8096078909402364},{0.7222951979294405,0.2017974971999763,0.9720791156050784,NaN,0.42955172504398687,0.9940353811420282,0.839536764405929,0.03316384292922936,0.1602347023161631,0.5625345667824141,0.8542659024913913,0.1761365701299611,0.8312229606937763,0.7820780604033565},{0.11594908641822632,0.865090011761414,0.4530346474923951,NaN,0.0396096812427591,0.07828852114693607,0.844088760011128,0.1767960157639903,0.17857143325827407,0.2522102209201954,0.9930633230891175,0.687941299680927,0.27289838138048383,0.15115804374180808},{0.7436419445622273,0.11423618345717534,0.9925168599192057,0.9610804010198164,NaN,NaN,NaN,0.47768252726422167,0.9622544279671161,0.05985593677636569,NaN,0.5179378181896932,0.23290767295012593,0.5892857157920004},{0.0983023224719013,0.6338382817759625,0.11493568065083815,NaN,NaN,0.6068213466031156,0.8503316000896455,0.8671405978559277,0.42774600405593644,0.5582188416249704,0.9520909221021127,0.4718682265067845,0.3622980885814183,0.20229159120610396},{0.2917796053045747,0.9529176213353792,0.7771965216814184,0.9909302218178493,0.39303711474170466,0.10999942140960017,0.5443908248350061,0.47833856882332415,0.19649587463949536,0.6205741260241843,0.7852944513125647,0.908196794155201,0.5354981873381928,0.838362961151687},{NaN,0.13382390261452382,0.9859525796947011,0.2869636576132818,0.9028381160965113,NaN,0.6953408763887957,0.0692522027228214,0.2748234652841348,NaN,NaN,0.9482299514046391,0.7852927728037424,0.6958887335679982},{NaN,0.9965895851849988,0.18563835622052482,0.3619798633033373,0.6756270004261794,NaN,0.2717670505640706,0.20943619156614035,NaN,0.5460896792544052,0.2763714033301786,0.5507381683150634,NaN,0.6438029616302258},{0.74737256038487,0.37618126545774533,0.35890489392104297,0.6678075536746959,NaN,0.4953196080826836,0.43941704009365956,0.8651187221525168,0.05059835909941457,0.5900812634557904,0.26395539211353813,0.30063945128466696,0.5758275756074844,0.06472928478265472}}\n",
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
        String result = buildArrayResult2d(dimLen1, dimLen2) + '\n';
        assertWithPgServer(Mode.EXTENDED, true, -1, (conn, binary, mode, port) -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tango AS (SELECT x n, " + literal + " arr FROM long_sequence(9))");
            }
            try (PreparedStatement stmt = conn.prepareStatement("SELECT n, arr[2:,2:] arr FROM tango")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("n[BIGINT],arr[ARRAY]\n" +
                                    "1," + result +
                                    "2," + result +
                                    "3," + result +
                                    "4," + result +
                                    "5," + result +
                                    "6," + result +
                                    "7," + result +
                                    "8," + result +
                                    "9," + result,
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
        String literal = buildArrayLiteral1d(elemCount);
        String result = buildArrayResult1d(elemCount) + '\n';
        assertWithPgServer(Mode.EXTENDED, true, -1, (conn, binary, mode, port) -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT x n, " + literal + " arr FROM long_sequence(9)")) {
                sink.clear();
                try (ResultSet rs = stmt.executeQuery()) {
                    assertResultSet("n[BIGINT],arr[ARRAY]\n" +
                                    "1," + result +
                                    "2," + result +
                                    "3," + result +
                                    "4," + result +
                                    "5," + result +
                                    "6," + result +
                                    "7," + result +
                                    "8," + result +
                                    "9," + result,
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

    private @NotNull String buildArrayResult2d(int dimLen1, int dimLen2) {
        StringBuilder b = new StringBuilder();
        b.append("{");
        String comma = "";
        for (int i = 1; i < dimLen1; i++) {
            b.append(comma);
            comma = ",";
            buildArrayResultInner(i * dimLen2 + 1, (i + 1) * dimLen2, b);
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
