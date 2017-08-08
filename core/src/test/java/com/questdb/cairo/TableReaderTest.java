package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.*;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.test.tools.TestUtils;
import com.questdb.txt.RecordSourcePrinter;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TableReaderTest extends AbstractOptimiserTest {
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @After
    public void tearDown0() throws Exception {
        try (CompositePath path = new CompositePath().of(root)) {
            Files.rmdir(path.$());
        }
    }

    @Test
    public void testNonPartitionedRead() throws Exception {
        createAllTable(PartitionBy.NONE);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReadByDay() throws Exception {
        createAllTable(PartitionBy.DAY);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReadByMonth() throws Exception {
        createAllTable(PartitionBy.MONTH);
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tVPMIUPLYJV\t\ttrue\t\t\n" +
                "NaN\t18857\t0\tNaN\tNaN\t-6541603390072946675\tVWKXPLTILO\t\ttrue\t\t2013-03-11T12:00:00.000Z\n" +
                "1272362811\t0\t-90\t495.601562500000\t0.5251\tNaN\tLJOYMXFXNU\t\ttrue\t\t2013-03-14T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "973040195\t0\t0\t-836.145385742188\t0.8502\t6229799201228951554\t\t\tfalse\t\t2013-03-19T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "308459589\t14311\t0\t0.001284251484\tNaN\t-8736036776035146161\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t45.488159179688\t0.8366\t-8989605389992287841\t\t\tfalse\t\t\n" +
                "NaN\t15403\t0\t0.000145471880\t0.8795\t-8930817457343668796\tBTPFUKBVTK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6590\t-7047591949709937141\t\t\tfalse\t\t\n" +
                "NaN\t0\t-105\tNaN\tNaN\t-4753799567364252473\t\t\tfalse\t\t2013-04-05T12:00:00.000Z\n" +
                "562010564\t7262\t0\tNaN\tNaN\t-6563980004387365579\t\t\ttrue\t\t\n" +
                "NaN\t0\t3\tNaN\t0.5520\tNaN\tZJGZUMUKYU\t\tfalse\t\t2013-04-10T12:00:00.000Z\n" +
                "293774095\t0\t0\t0.000082347786\t0.3634\t-3190830651760796509\t\t\tfalse\t\t\n" +
                "-1604751729\t-9938\t20\tNaN\tNaN\t-6935015060172657067\t\t\ttrue\t\t2013-04-15T12:00:00.000Z\n" +
                "NaN\t-8125\t-57\t849.075531005859\tNaN\tNaN\tKNGEBETLTK\t\tfalse\t\t2013-04-18T00:00:00.000Z\n" +
                "1720228024\t-23409\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6895\t6004620688013050571\tTVVZBKUVBK\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000087999168\t0.4351\tNaN\tFYGCYVIOMB\t\tfalse\t\t\n" +
                "NaN\t0\t-80\t0.003153746715\tNaN\t9098346554960907337\tQRHFCHKEEV\t\tfalse\t\t\n" +
                "NaN\t-32743\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-04-30T12:00:00.000Z\n" +
                "NaN\t0\t0\t9.205261230469\t0.9173\t-4685006213170370882\t\t\tfalse\t\t2013-05-03T00:00:00.000Z\n" +
                "598851620\t0\t0\t21.701147556305\t0.2727\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t-25550\t0\tNaN\t0.3322\tNaN\t\t\tfalse\t\t\n" +
                "-778922608\t24861\t0\t0.000000720363\tNaN\t8259848643433335285\t\t\ttrue\t\t\n" +
                "NaN\t0\t102\tNaN\tNaN\tNaN\tZXMGEZDCJY\t\ttrue\t\t2013-05-13T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-1570697358\t0\t0\t0.000000024249\tNaN\tNaN\tPOPUMZRZFP\t\tfalse\t\t2013-05-18T00:00:00.000Z\n" +
                "1281789059\t-28328\t0\t8.375915527344\tNaN\tNaN\t\t\tfalse\t\t2013-05-20T12:00:00.000Z\n" +
                "773205381\t2761\t-107\t-357.250000000000\t0.4628\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t-80.000000000000\t0.7919\t7815756683824312484\t\t\ttrue\t\t\n" +
                "1263151569\t-11048\t-73\tNaN\tNaN\tNaN\tKRCFYRTLRK\t\tfalse\t\t2013-05-28T00:00:00.000Z\n" +
                "439223132\t3987\t86\t87.750000000000\t0.4815\t-9036776975577124044\tDXKUIKUEMT\t\tfalse\t\t\n" +
                "NaN\t-27442\t0\t0.000147545263\t0.7297\t-8297333826176735232\t\t\ttrue\t\t2013-06-02T00:00:00.000Z\n" +
                "NaN\t9003\t0\t0.006835407345\tNaN\tNaN\t\t\tfalse\t\t2013-06-04T12:00:00.000Z\n" +
                "NaN\t0\t16\tNaN\t0.3239\tNaN\tBSOGXJMFKP\t\tfalse\t\t\n" +
                "-139580709\t13211\t0\tNaN\tNaN\tNaN\tUBPURNKTQH\t\ttrue\t\t2013-06-09T12:00:00.000Z\n" +
                "NaN\t0\t-9\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.001820561971\t0.4415\t-6102982263510933075\tEKLWGBGEOG\t\ttrue\t\t2013-06-14T12:00:00.000Z\n" +
                "NaN\t12750\t0\tNaN\tNaN\tNaN\tPBQQLZFHZD\t\tfalse\t\t\n" +
                "NaN\t2196\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "386502586\t0\t122\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "439448790\t10173\t90\t0.000008843858\t0.4117\tNaN\t\t\tfalse\t\t\n" +
                "-2039588344\t20814\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-70\t0.003472472192\t0.8169\t-4210500280316527754\tDHXLLWYISI\t\tfalse\t\t2013-06-29T12:00:00.000Z\n" +
                "NaN\t18551\t-63\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t0.024644770660\tNaN\tNaN\tMWPZCHTCCX\t\ttrue\t\t\n" +
                "NaN\t-6604\t9\tNaN\tNaN\tNaN\tTLXKVTQULM\t\tfalse\t\t2013-07-07T00:00:00.000Z\n" +
                "NaN\t0\t-79\t-256.000000000000\tNaN\t7702983503801189270\t\t\ttrue\t\t\n" +
                "-1553509839\t0\t0\t238.259117126465\tNaN\t-1012659436286375174\t\t\tfalse\t\t2013-07-12T00:00:00.000Z\n" +
                "NaN\t-14133\t0\t423.937385559082\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.3018\tNaN\tGQQLTSYDNF\t\tfalse\t\t2013-07-17T00:00:00.000Z\n" +
                "1416328685\t9161\t0\t0.123783446848\t0.6768\t-5457019364697877056\tLUBDODEPXS\t\ttrue\t\t2013-07-19T12:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t19513\t-9\t0.000000038397\t0.8405\t-7740238432567045181\t\t\tfalse\t\t2013-07-24T12:00:00.000Z\n" +
                "NaN\t-17207\t0\tNaN\tNaN\t3608850123411200285\tUJEYJNNYPK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tKFBGJJWDPP\t\tfalse\t\t2013-07-29T12:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2013-08-03T12:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2013-08-06T00:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\t\t\tfalse\t\t\n" +
                "352676096\t2921\t106\tNaN\tNaN\t-7166847293288140192\t\t\ttrue\t\t2013-08-11T00:00:00.000Z\n" +
                "NaN\t0\t0\t-891.255187988281\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "2013386777\t-4913\t0\t0.096109641716\tNaN\t-9134728405009464739\t\t\ttrue\t\t2013-08-16T00:00:00.000Z\n" +
                "1537359183\t0\t18\t50.457431793213\tNaN\tNaN\tZLXSRQKBCP\t\tfalse\t\t\n" +
                "-1887247278\t0\t60\tNaN\t0.5281\t8892699517500908585\t\t\ttrue\t\t\n" +
                "NaN\t0\t87\t0.000086571285\t0.5349\t8284471997786423504\t\t\tfalse\t\t2013-08-23T12:00:00.000Z\n" +
                "NaN\t0\t0\t0.000000004778\t0.8130\t8439036057773114882\tGDLSMQPYNU\t\tfalse\t\t2013-08-26T00:00:00.000Z\n" +
                "NaN\t0\t-77\t829.437500000000\tNaN\t8885314950764600193\tOEIGBTHYVP\t\ttrue\t\t\n" +
                "NaN\t0\t0\t129.767963409424\t0.5434\tNaN\tWWRGGWVNTO\t\tfalse\t\t\n" +
                "1162820296\t-13379\t42\t0.000602753847\t0.7151\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t-86\tNaN\t0.7216\t8601477333412382641\tLDCCOELJWI\t\tfalse\t\t\n" +
                "NaN\t0\t-26\t-512.000000000000\tNaN\tNaN\tTDJOCZRMZX\t\tfalse\t\t\n" +
                "NaN\t0\t122\t0.011416638270\t0.4929\tNaN\tXGLGFIBWXN\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8885925444061796471\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.4037\t-6546280368212658058\t\t\tfalse\t\t\n" +
                "NaN\t0\t-112\tNaN\tNaN\t-1432130118181766760\t\t\tfalse\t\t2013-09-17T12:00:00.000Z\n" +
                "NaN\t0\t54\t818.979553222656\t0.3012\t8559643124608722337\tIGBWZXSJJN\t\tfalse\t\t\n" +
                "NaN\t0\t0\t8.801370620728\t0.3609\tNaN\tKBEWLYZPZZ\t\tfalse\t\t2013-09-22T12:00:00.000Z\n" +
                "NaN\t0\t0\t0.000046097467\tNaN\t-1334416388074838565\tTFWLGSFUJG\t\tfalse\t\t2013-09-25T00:00:00.000Z\n" +
                "NaN\t7519\t-114\tNaN\t0.5528\tNaN\tTNWDLJQPKW\t\ttrue\t\t\n" +
                "140497042\t0\t0\tNaN\t0.3144\tNaN\tOMZZKGTBCQ\t\tfalse\t\t\n" +
                "634971665\t0\t0\t-896.000000000000\t0.5061\t8333807780608495713\t\t\tfalse\t\t\n" +
                "1045373397\t5459\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1286159186\t0\t0\t0.001565168088\t0.4512\tNaN\t\t\tfalse\t\t2013-10-07T12:00:00.000Z\n" +
                "NaN\t0\t0\t260.000000000000\tNaN\tNaN\t\t\ttrue\t\t2013-10-10T00:00:00.000Z\n" +
                "-577895670\t0\t0\t899.281250000000\t0.5314\tNaN\tPCHFNMWRKF\t\tfalse\t\t\n" +
                "NaN\t3608\t-70\t0.001165638154\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t18027\t-91\tNaN\tNaN\tNaN\t\t\ttrue\t\t2013-10-17T12:00:00.000Z\n" +
                "NaN\t0\t0\t-213.468750000000\t0.4905\t-8940639164947475955\tYROHKIIIIH\t\ttrue\t\t2013-10-20T00:00:00.000Z\n" +
                "NaN\t-32404\t0\tNaN\tNaN\t-3801609434519024772\t\t\ttrue\t\t2013-10-22T12:00:00.000Z\n" +
                "-211958640\t0\t0\tNaN\t0.8148\t-9014192604691332491\t\t\tfalse\t\t2013-10-25T00:00:00.000Z\n" +
                "-335922823\t-21381\t-3\t0.000000050588\t0.3811\tNaN\t\t\ttrue\t\t2013-10-27T12:00:00.000Z\n" +
                "444826054\t0\t0\tNaN\tNaN\tNaN\tTKPWVTUTPC\t\tfalse\t\t2013-10-30T00:00:00.000Z\n" +
                "NaN\t0\t-112\t0.000078579949\t0.4149\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.5317\tNaN\tXMWDLPWECV\t\ttrue\t\t\n" +
                "-462127181\t-27713\t57\tNaN\t0.7427\t8433285365657224839\t\t\ttrue\t\t\n" +
                "NaN\t23894\t0\tNaN\tNaN\t9153770087233785213\tVURUTOHPVN\t\ttrue\t\t\n";
        TestUtils.assertMemoryLeak(() -> testTableCursor(60 * 60 * 60000, expected));
    }

    @Test
    public void testReadByYear() throws Exception {
        createAllTable(PartitionBy.YEAR);
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tVPMIUPLYJV\t\ttrue\t\t\n" +
                "NaN\t18857\t0\tNaN\tNaN\t-6541603390072946675\tVWKXPLTILO\t\ttrue\t\t2013-08-31T00:00:00.000Z\n" +
                "1272362811\t0\t-90\t495.601562500000\t0.5251\tNaN\tLJOYMXFXNU\t\ttrue\t\t2013-10-30T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "973040195\t0\t0\t-836.145385742188\t0.8502\t6229799201228951554\t\t\tfalse\t\t2014-02-27T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "308459589\t14311\t0\t0.001284251484\tNaN\t-8736036776035146161\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t45.488159179688\t0.8366\t-8989605389992287841\t\t\tfalse\t\t\n" +
                "NaN\t15403\t0\t0.000145471880\t0.8795\t-8930817457343668796\tBTPFUKBVTK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6590\t-7047591949709937141\t\t\tfalse\t\t\n" +
                "NaN\t0\t-105\tNaN\tNaN\t-4753799567364252473\t\t\tfalse\t\t2015-04-23T00:00:00.000Z\n" +
                "562010564\t7262\t0\tNaN\tNaN\t-6563980004387365579\t\t\ttrue\t\t\n" +
                "NaN\t0\t3\tNaN\t0.5520\tNaN\tZJGZUMUKYU\t\tfalse\t\t2015-08-21T00:00:00.000Z\n" +
                "293774095\t0\t0\t0.000082347786\t0.3634\t-3190830651760796509\t\t\tfalse\t\t\n" +
                "-1604751729\t-9938\t20\tNaN\tNaN\t-6935015060172657067\t\t\ttrue\t\t2015-12-19T00:00:00.000Z\n" +
                "NaN\t-8125\t-57\t849.075531005859\tNaN\tNaN\tKNGEBETLTK\t\tfalse\t\t2016-02-17T00:00:00.000Z\n" +
                "1720228024\t-23409\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6895\t6004620688013050571\tTVVZBKUVBK\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000087999168\t0.4351\tNaN\tFYGCYVIOMB\t\tfalse\t\t\n" +
                "NaN\t0\t-80\t0.003153746715\tNaN\t9098346554960907337\tQRHFCHKEEV\t\tfalse\t\t\n" +
                "NaN\t-32743\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2016-12-13T00:00:00.000Z\n" +
                "NaN\t0\t0\t9.205261230469\t0.9173\t-4685006213170370882\t\t\tfalse\t\t2017-02-11T00:00:00.000Z\n" +
                "598851620\t0\t0\t21.701147556305\t0.2727\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t-25550\t0\tNaN\t0.3322\tNaN\t\t\tfalse\t\t\n" +
                "-778922608\t24861\t0\t0.000000720363\tNaN\t8259848643433335285\t\t\ttrue\t\t\n" +
                "NaN\t0\t102\tNaN\tNaN\tNaN\tZXMGEZDCJY\t\ttrue\t\t2017-10-09T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-1570697358\t0\t0\t0.000000024249\tNaN\tNaN\tPOPUMZRZFP\t\tfalse\t\t2018-02-06T00:00:00.000Z\n" +
                "1281789059\t-28328\t0\t8.375915527344\tNaN\tNaN\t\t\tfalse\t\t2018-04-07T00:00:00.000Z\n" +
                "773205381\t2761\t-107\t-357.250000000000\t0.4628\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t-80.000000000000\t0.7919\t7815756683824312484\t\t\ttrue\t\t\n" +
                "1263151569\t-11048\t-73\tNaN\tNaN\tNaN\tKRCFYRTLRK\t\tfalse\t\t2018-10-04T00:00:00.000Z\n" +
                "439223132\t3987\t86\t87.750000000000\t0.4815\t-9036776975577124044\tDXKUIKUEMT\t\tfalse\t\t\n" +
                "NaN\t-27442\t0\t0.000147545263\t0.7297\t-8297333826176735232\t\t\ttrue\t\t2019-02-01T00:00:00.000Z\n" +
                "NaN\t9003\t0\t0.006835407345\tNaN\tNaN\t\t\tfalse\t\t2019-04-02T00:00:00.000Z\n" +
                "NaN\t0\t16\tNaN\t0.3239\tNaN\tBSOGXJMFKP\t\tfalse\t\t\n" +
                "-139580709\t13211\t0\tNaN\tNaN\tNaN\tUBPURNKTQH\t\ttrue\t\t2019-07-31T00:00:00.000Z\n" +
                "NaN\t0\t-9\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.001820561971\t0.4415\t-6102982263510933075\tEKLWGBGEOG\t\ttrue\t\t2019-11-28T00:00:00.000Z\n" +
                "NaN\t12750\t0\tNaN\tNaN\tNaN\tPBQQLZFHZD\t\tfalse\t\t\n" +
                "NaN\t2196\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "386502586\t0\t122\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "439448790\t10173\t90\t0.000008843858\t0.4117\tNaN\t\t\tfalse\t\t\n" +
                "-2039588344\t20814\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-70\t0.003472472192\t0.8169\t-4210500280316527754\tDHXLLWYISI\t\tfalse\t\t2020-11-22T00:00:00.000Z\n" +
                "NaN\t18551\t-63\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t0.024644770660\tNaN\tNaN\tMWPZCHTCCX\t\ttrue\t\t\n" +
                "NaN\t-6604\t9\tNaN\tNaN\tNaN\tTLXKVTQULM\t\tfalse\t\t2021-05-21T00:00:00.000Z\n" +
                "NaN\t0\t-79\t-256.000000000000\tNaN\t7702983503801189270\t\t\ttrue\t\t\n" +
                "-1553509839\t0\t0\t238.259117126465\tNaN\t-1012659436286375174\t\t\tfalse\t\t2021-09-18T00:00:00.000Z\n" +
                "NaN\t-14133\t0\t423.937385559082\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.3018\tNaN\tGQQLTSYDNF\t\tfalse\t\t2022-01-16T00:00:00.000Z\n" +
                "1416328685\t9161\t0\t0.123783446848\t0.6768\t-5457019364697877056\tLUBDODEPXS\t\ttrue\t\t2022-03-17T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t19513\t-9\t0.000000038397\t0.8405\t-7740238432567045181\t\t\tfalse\t\t2022-07-15T00:00:00.000Z\n" +
                "NaN\t-17207\t0\tNaN\tNaN\t3608850123411200285\tUJEYJNNYPK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tKFBGJJWDPP\t\tfalse\t\t2022-11-12T00:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2023-03-12T00:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2023-05-11T00:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\t\t\tfalse\t\t\n" +
                "352676096\t2921\t106\tNaN\tNaN\t-7166847293288140192\t\t\ttrue\t\t2023-09-08T00:00:00.000Z\n" +
                "NaN\t0\t0\t-891.255187988281\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "2013386777\t-4913\t0\t0.096109641716\tNaN\t-9134728405009464739\t\t\ttrue\t\t2024-01-06T00:00:00.000Z\n" +
                "1537359183\t0\t18\t50.457431793213\tNaN\tNaN\tZLXSRQKBCP\t\tfalse\t\t\n" +
                "-1887247278\t0\t60\tNaN\t0.5281\t8892699517500908585\t\t\ttrue\t\t\n" +
                "NaN\t0\t87\t0.000086571285\t0.5349\t8284471997786423504\t\t\tfalse\t\t2024-07-04T00:00:00.000Z\n" +
                "NaN\t0\t0\t0.000000004778\t0.8130\t8439036057773114882\tGDLSMQPYNU\t\tfalse\t\t2024-09-02T00:00:00.000Z\n" +
                "NaN\t0\t-77\t829.437500000000\tNaN\t8885314950764600193\tOEIGBTHYVP\t\ttrue\t\t\n" +
                "NaN\t0\t0\t129.767963409424\t0.5434\tNaN\tWWRGGWVNTO\t\tfalse\t\t\n" +
                "1162820296\t-13379\t42\t0.000602753847\t0.7151\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t-86\tNaN\t0.7216\t8601477333412382641\tLDCCOELJWI\t\tfalse\t\t\n" +
                "NaN\t0\t-26\t-512.000000000000\tNaN\tNaN\tTDJOCZRMZX\t\tfalse\t\t\n" +
                "NaN\t0\t122\t0.011416638270\t0.4929\tNaN\tXGLGFIBWXN\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8885925444061796471\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.4037\t-6546280368212658058\t\t\tfalse\t\t\n" +
                "NaN\t0\t-112\tNaN\tNaN\t-1432130118181766760\t\t\tfalse\t\t2026-02-24T00:00:00.000Z\n" +
                "NaN\t0\t54\t818.979553222656\t0.3012\t8559643124608722337\tIGBWZXSJJN\t\tfalse\t\t\n" +
                "NaN\t0\t0\t8.801370620728\t0.3609\tNaN\tKBEWLYZPZZ\t\tfalse\t\t2026-06-24T00:00:00.000Z\n" +
                "NaN\t0\t0\t0.000046097467\tNaN\t-1334416388074838565\tTFWLGSFUJG\t\tfalse\t\t2026-08-23T00:00:00.000Z\n" +
                "NaN\t7519\t-114\tNaN\t0.5528\tNaN\tTNWDLJQPKW\t\ttrue\t\t\n" +
                "140497042\t0\t0\tNaN\t0.3144\tNaN\tOMZZKGTBCQ\t\tfalse\t\t\n" +
                "634971665\t0\t0\t-896.000000000000\t0.5061\t8333807780608495713\t\t\tfalse\t\t\n" +
                "1045373397\t5459\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1286159186\t0\t0\t0.001565168088\t0.4512\tNaN\t\t\tfalse\t\t2027-06-19T00:00:00.000Z\n" +
                "NaN\t0\t0\t260.000000000000\tNaN\tNaN\t\t\ttrue\t\t2027-08-18T00:00:00.000Z\n" +
                "-577895670\t0\t0\t899.281250000000\t0.5314\tNaN\tPCHFNMWRKF\t\tfalse\t\t\n" +
                "NaN\t3608\t-70\t0.001165638154\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t18027\t-91\tNaN\tNaN\tNaN\t\t\ttrue\t\t2028-02-14T00:00:00.000Z\n" +
                "NaN\t0\t0\t-213.468750000000\t0.4905\t-8940639164947475955\tYROHKIIIIH\t\ttrue\t\t2028-04-14T00:00:00.000Z\n" +
                "NaN\t-32404\t0\tNaN\tNaN\t-3801609434519024772\t\t\ttrue\t\t2028-06-13T00:00:00.000Z\n" +
                "-211958640\t0\t0\tNaN\t0.8148\t-9014192604691332491\t\t\tfalse\t\t2028-08-12T00:00:00.000Z\n" +
                "-335922823\t-21381\t-3\t0.000000050588\t0.3811\tNaN\t\t\ttrue\t\t2028-10-11T00:00:00.000Z\n" +
                "444826054\t0\t0\tNaN\tNaN\tNaN\tTKPWVTUTPC\t\tfalse\t\t2028-12-10T00:00:00.000Z\n" +
                "NaN\t0\t-112\t0.000078579949\t0.4149\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.5317\tNaN\tXMWDLPWECV\t\ttrue\t\t\n" +
                "-462127181\t-27713\t57\tNaN\t0.7427\t8433285365657224839\t\t\ttrue\t\t\n" +
                "NaN\t23894\t0\tNaN\tNaN\t9153770087233785213\tVURUTOHPVN\t\ttrue\t\t\n";
        TestUtils.assertMemoryLeak(() -> testTableCursor(24 * 60 * 60 * 60000L, expected));
    }

    private void createAllTable(int partitionBy) {
        createTable(FF, new JournalStructure("all").
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date").partitionBy(partitionBy));
    }

    private void createTable(FilesFacade ff, JournalStructure struct) {
        String name = struct.getName();
        try (TableUtils tabU = new TableUtils(ff)) {
            if (tabU.exists(root, name) == 1) {
                tabU.create(root, struct.build(), 509);
            } else {
                throw CairoException.instance(0).put("Table ").put(name).put(" already exists");
            }
        }
    }

    private void testAppendNulls(Rnd rnd, FilesFacade ff, long ts, int count, long inc) throws NumericException {
        final int blobLen = 64 * 1024;
        long blob = Unsafe.malloc(blobLen);
        try (TableWriter writer = new TableWriter(ff, root, "all")) {
            long size = writer.size();
            for (int i = 0; i < count; i++) {
                TableWriter.Row r = writer.newRow(ts += inc);
                if (rnd.nextBoolean()) {
                    r.putByte(2, rnd.nextByte());
                }

                if (rnd.nextBoolean()) {
                    r.putBool(8, rnd.nextBoolean());
                }

                if (rnd.nextBoolean()) {
                    r.putShort(1, rnd.nextShort());
                }

                if (rnd.nextBoolean()) {
                    r.putInt(0, rnd.nextInt());
                }

                if (rnd.nextBoolean()) {
                    r.putDouble(3, rnd.nextDouble());
                }

                if (rnd.nextBoolean()) {
                    r.putFloat(4, rnd.nextFloat());
                }

                if (rnd.nextBoolean()) {
                    r.putLong(5, rnd.nextLong());
                }

                if (rnd.nextBoolean()) {
                    r.putDate(10, ts);
                }

                if (rnd.nextBoolean()) {
                    rnd.nextChars(blob, blobLen);
                    r.putBin(9, blob, blobLen);
                }

                if (rnd.nextBoolean()) {
                    r.putStr(6, rnd.nextChars(10));
                }

                r.append();
            }
            writer.commit();

            Assert.assertEquals(size + count, writer.size());
        } finally {
            Unsafe.free(blob, blobLen);
        }
    }

    private void testTableCursor() throws IOException, NumericException {
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tVPMIUPLYJV\t\ttrue\t\t\n" +
                "NaN\t18857\t0\tNaN\tNaN\t-6541603390072946675\tVWKXPLTILO\t\ttrue\t\t2013-03-04T03:00:00.000Z\n" +
                "1272362811\t0\t-90\t495.601562500000\t0.5251\tNaN\tLJOYMXFXNU\t\ttrue\t\t2013-03-04T04:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "973040195\t0\t0\t-836.145385742188\t0.8502\t6229799201228951554\t\t\tfalse\t\t2013-03-04T06:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "308459589\t14311\t0\t0.001284251484\tNaN\t-8736036776035146161\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t45.488159179688\t0.8366\t-8989605389992287841\t\t\tfalse\t\t\n" +
                "NaN\t15403\t0\t0.000145471880\t0.8795\t-8930817457343668796\tBTPFUKBVTK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6590\t-7047591949709937141\t\t\tfalse\t\t\n" +
                "NaN\t0\t-105\tNaN\tNaN\t-4753799567364252473\t\t\tfalse\t\t2013-03-04T13:00:00.000Z\n" +
                "562010564\t7262\t0\tNaN\tNaN\t-6563980004387365579\t\t\ttrue\t\t\n" +
                "NaN\t0\t3\tNaN\t0.5520\tNaN\tZJGZUMUKYU\t\tfalse\t\t2013-03-04T15:00:00.000Z\n" +
                "293774095\t0\t0\t0.000082347786\t0.3634\t-3190830651760796509\t\t\tfalse\t\t\n" +
                "-1604751729\t-9938\t20\tNaN\tNaN\t-6935015060172657067\t\t\ttrue\t\t2013-03-04T17:00:00.000Z\n" +
                "NaN\t-8125\t-57\t849.075531005859\tNaN\tNaN\tKNGEBETLTK\t\tfalse\t\t2013-03-04T18:00:00.000Z\n" +
                "1720228024\t-23409\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6895\t6004620688013050571\tTVVZBKUVBK\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000087999168\t0.4351\tNaN\tFYGCYVIOMB\t\tfalse\t\t\n" +
                "NaN\t0\t-80\t0.003153746715\tNaN\t9098346554960907337\tQRHFCHKEEV\t\tfalse\t\t\n" +
                "NaN\t-32743\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-04T23:00:00.000Z\n" +
                "NaN\t0\t0\t9.205261230469\t0.9173\t-4685006213170370882\t\t\tfalse\t\t2013-03-05T00:00:00.000Z\n" +
                "598851620\t0\t0\t21.701147556305\t0.2727\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t-25550\t0\tNaN\t0.3322\tNaN\t\t\tfalse\t\t\n" +
                "-778922608\t24861\t0\t0.000000720363\tNaN\t8259848643433335285\t\t\ttrue\t\t\n" +
                "NaN\t0\t102\tNaN\tNaN\tNaN\tZXMGEZDCJY\t\ttrue\t\t2013-03-05T04:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-1570697358\t0\t0\t0.000000024249\tNaN\tNaN\tPOPUMZRZFP\t\tfalse\t\t2013-03-05T06:00:00.000Z\n" +
                "1281789059\t-28328\t0\t8.375915527344\tNaN\tNaN\t\t\tfalse\t\t2013-03-05T07:00:00.000Z\n" +
                "773205381\t2761\t-107\t-357.250000000000\t0.4628\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t-80.000000000000\t0.7919\t7815756683824312484\t\t\ttrue\t\t\n" +
                "1263151569\t-11048\t-73\tNaN\tNaN\tNaN\tKRCFYRTLRK\t\tfalse\t\t2013-03-05T10:00:00.000Z\n" +
                "439223132\t3987\t86\t87.750000000000\t0.4815\t-9036776975577124044\tDXKUIKUEMT\t\tfalse\t\t\n" +
                "NaN\t-27442\t0\t0.000147545263\t0.7297\t-8297333826176735232\t\t\ttrue\t\t2013-03-05T12:00:00.000Z\n" +
                "NaN\t9003\t0\t0.006835407345\tNaN\tNaN\t\t\tfalse\t\t2013-03-05T13:00:00.000Z\n" +
                "NaN\t0\t16\tNaN\t0.3239\tNaN\tBSOGXJMFKP\t\tfalse\t\t\n" +
                "-139580709\t13211\t0\tNaN\tNaN\tNaN\tUBPURNKTQH\t\ttrue\t\t2013-03-05T15:00:00.000Z\n" +
                "NaN\t0\t-9\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.001820561971\t0.4415\t-6102982263510933075\tEKLWGBGEOG\t\ttrue\t\t2013-03-05T17:00:00.000Z\n" +
                "NaN\t12750\t0\tNaN\tNaN\tNaN\tPBQQLZFHZD\t\tfalse\t\t\n" +
                "NaN\t2196\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "386502586\t0\t122\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "439448790\t10173\t90\t0.000008843858\t0.4117\tNaN\t\t\tfalse\t\t\n" +
                "-2039588344\t20814\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-70\t0.003472472192\t0.8169\t-4210500280316527754\tDHXLLWYISI\t\tfalse\t\t2013-03-05T23:00:00.000Z\n" +
                "NaN\t18551\t-63\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\t0.024644770660\tNaN\tNaN\tMWPZCHTCCX\t\ttrue\t\t\n" +
                "NaN\t-6604\t9\tNaN\tNaN\tNaN\tTLXKVTQULM\t\tfalse\t\t2013-03-06T02:00:00.000Z\n" +
                "NaN\t0\t-79\t-256.000000000000\tNaN\t7702983503801189270\t\t\ttrue\t\t\n" +
                "-1553509839\t0\t0\t238.259117126465\tNaN\t-1012659436286375174\t\t\tfalse\t\t2013-03-06T04:00:00.000Z\n" +
                "NaN\t-14133\t0\t423.937385559082\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.3018\tNaN\tGQQLTSYDNF\t\tfalse\t\t2013-03-06T06:00:00.000Z\n" +
                "1416328685\t9161\t0\t0.123783446848\t0.6768\t-5457019364697877056\tLUBDODEPXS\t\ttrue\t\t2013-03-06T07:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t19513\t-9\t0.000000038397\t0.8405\t-7740238432567045181\t\t\tfalse\t\t2013-03-06T09:00:00.000Z\n" +
                "NaN\t-17207\t0\tNaN\tNaN\t3608850123411200285\tUJEYJNNYPK\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tKFBGJJWDPP\t\tfalse\t\t2013-03-06T11:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2013-03-06T13:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2013-03-06T14:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\t\t\tfalse\t\t\n" +
                "352676096\t2921\t106\tNaN\tNaN\t-7166847293288140192\t\t\ttrue\t\t2013-03-06T16:00:00.000Z\n" +
                "NaN\t0\t0\t-891.255187988281\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "2013386777\t-4913\t0\t0.096109641716\tNaN\t-9134728405009464739\t\t\ttrue\t\t2013-03-06T18:00:00.000Z\n" +
                "1537359183\t0\t18\t50.457431793213\tNaN\tNaN\tZLXSRQKBCP\t\tfalse\t\t\n" +
                "-1887247278\t0\t60\tNaN\t0.5281\t8892699517500908585\t\t\ttrue\t\t\n" +
                "NaN\t0\t87\t0.000086571285\t0.5349\t8284471997786423504\t\t\tfalse\t\t2013-03-06T21:00:00.000Z\n" +
                "NaN\t0\t0\t0.000000004778\t0.8130\t8439036057773114882\tGDLSMQPYNU\t\tfalse\t\t2013-03-06T22:00:00.000Z\n" +
                "NaN\t0\t-77\t829.437500000000\tNaN\t8885314950764600193\tOEIGBTHYVP\t\ttrue\t\t\n" +
                "NaN\t0\t0\t129.767963409424\t0.5434\tNaN\tWWRGGWVNTO\t\tfalse\t\t\n" +
                "1162820296\t-13379\t42\t0.000602753847\t0.7151\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t-86\tNaN\t0.7216\t8601477333412382641\tLDCCOELJWI\t\tfalse\t\t\n" +
                "NaN\t0\t-26\t-512.000000000000\tNaN\tNaN\tTDJOCZRMZX\t\tfalse\t\t\n" +
                "NaN\t0\t122\t0.011416638270\t0.4929\tNaN\tXGLGFIBWXN\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8885925444061796471\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.4037\t-6546280368212658058\t\t\tfalse\t\t\n" +
                "NaN\t0\t-112\tNaN\tNaN\t-1432130118181766760\t\t\tfalse\t\t2013-03-07T07:00:00.000Z\n" +
                "NaN\t0\t54\t818.979553222656\t0.3012\t8559643124608722337\tIGBWZXSJJN\t\tfalse\t\t\n" +
                "NaN\t0\t0\t8.801370620728\t0.3609\tNaN\tKBEWLYZPZZ\t\tfalse\t\t2013-03-07T09:00:00.000Z\n" +
                "NaN\t0\t0\t0.000046097467\tNaN\t-1334416388074838565\tTFWLGSFUJG\t\tfalse\t\t2013-03-07T10:00:00.000Z\n" +
                "NaN\t7519\t-114\tNaN\t0.5528\tNaN\tTNWDLJQPKW\t\ttrue\t\t\n" +
                "140497042\t0\t0\tNaN\t0.3144\tNaN\tOMZZKGTBCQ\t\tfalse\t\t\n" +
                "634971665\t0\t0\t-896.000000000000\t0.5061\t8333807780608495713\t\t\tfalse\t\t\n" +
                "1045373397\t5459\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1286159186\t0\t0\t0.001565168088\t0.4512\tNaN\t\t\tfalse\t\t2013-03-07T15:00:00.000Z\n" +
                "NaN\t0\t0\t260.000000000000\tNaN\tNaN\t\t\ttrue\t\t2013-03-07T16:00:00.000Z\n" +
                "-577895670\t0\t0\t899.281250000000\t0.5314\tNaN\tPCHFNMWRKF\t\tfalse\t\t\n" +
                "NaN\t3608\t-70\t0.001165638154\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t18027\t-91\tNaN\tNaN\tNaN\t\t\ttrue\t\t2013-03-07T19:00:00.000Z\n" +
                "NaN\t0\t0\t-213.468750000000\t0.4905\t-8940639164947475955\tYROHKIIIIH\t\ttrue\t\t2013-03-07T20:00:00.000Z\n" +
                "NaN\t-32404\t0\tNaN\tNaN\t-3801609434519024772\t\t\ttrue\t\t2013-03-07T21:00:00.000Z\n" +
                "-211958640\t0\t0\tNaN\t0.8148\t-9014192604691332491\t\t\tfalse\t\t2013-03-07T22:00:00.000Z\n" +
                "-335922823\t-21381\t-3\t0.000000050588\t0.3811\tNaN\t\t\ttrue\t\t2013-03-07T23:00:00.000Z\n" +
                "444826054\t0\t0\tNaN\tNaN\tNaN\tTKPWVTUTPC\t\tfalse\t\t2013-03-08T00:00:00.000Z\n" +
                "NaN\t0\t-112\t0.000078579949\t0.4149\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\t0.5317\tNaN\tXMWDLPWECV\t\ttrue\t\t\n" +
                "-462127181\t-27713\t57\tNaN\t0.7427\t8433285365657224839\t\t\ttrue\t\t\n" +
                "NaN\t23894\t0\tNaN\tNaN\t9153770087233785213\tVURUTOHPVN\t\ttrue\t\t\n";
        testTableCursor(60 * 60000, expected);
    }

    private void testTableCursor(long increment, String expected) throws IOException, NumericException {
        Rnd rnd = new Rnd();
        int N = 100;
        testAppendNulls(rnd, FF, DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z"), N, increment);

        final StringSink sink = new StringSink();
        final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
        try (TableReader reader = new TableReader(FF, root, "all")) {
            Assert.assertEquals(N, reader.size());
            printer.print(reader, true, reader.getMetadata());
            TestUtils.assertEquals(expected, sink);

            sink.clear();
            reader.toTop();

            printer.print(reader, true, reader.getMetadata());
            TestUtils.assertEquals(expected, sink);
        }
    }
}