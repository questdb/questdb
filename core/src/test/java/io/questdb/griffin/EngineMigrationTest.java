/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class EngineMigrationTest extends AbstractGriffinTest {

    public static void replaceDbContent(String path) throws IOException {

        engine.releaseAllReaders();
        engine.releaseAllWriters();

        final byte[] buffer = new byte[1024 * 1024];
        URL resource = EngineMigrationTest.class.getResource(path);
        Assert.assertNotNull(resource);
        try (final InputStream is = EngineMigrationTest.class.getResourceAsStream(path)) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File((String) root, ze.getName());
                    if (!ze.isDirectory()) {
                        copyInputStream(buffer, dest, zip);
                    }
                    zip.closeEntry();
                }
            }
        }
    }

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void test416() throws IOException, SqlException {
        doMigration("/migration/data_416.zip", false);
    }

    @Test
    public void test417() throws IOException, SqlException {
        doMigration("/migration/data_417.zip", true);
    }

    @Test
    public void test419() throws IOException, SqlException {
        doMigration("/migration/data_419.zip", true);
    }

    @Test
    public void test420() throws IOException, SqlException {
        doMigration("/migration/data_420.zip", true);
    }

    @Test
    public void testGenerateTables() throws SqlException {
        generateMigrationTables();
        engine.releaseAllWriters();
        assertData();
    }

    private static void copyInputStream(byte[] buffer, File out, InputStream is) throws IOException {
        File dir = out.getParentFile();
        Assert.assertTrue(dir.exists() || dir.mkdirs());
        try (FileOutputStream fos = new FileOutputStream(out)) {
            int n;
            while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                fos.write(buffer, 0, n);
            }
        }
    }

    private void assertData() throws SqlException {
        assertNoneNts();
        assertNone();
        assertDay();
        assertMonth();
        assertYear();
    }

    private void assertDay() throws SqlException {

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_day where m = null",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "77\tW\t-29635\t36806\t39204\t0.0244\t0.4452645911659644\t1974-10-14T05:22:22.780Z\t1970-01-01T00:00:00.181107Z\tJJDSR\tbbbbbb\ttrue\t\t0xd89fec5fab3f4af1a0ca0ec5d6448e2d798d79cb982de744b96a662a0b9f32d7\t00000000 aa 41 c5 55 ef\t1970-06-12T00:56:40.000000Z\n" +
                        "63\tI\t-32736\t467385\t-76685\tNaN\t0.31674150508412846\t1975-03-23T06:58:43.118Z\t1970-01-01T00:00:00.400171Z\tVIRB\t\tfalse\t\t0xf6f008b8e0bd93ffe884685ca40e1575d3389fdb74d0af7b8e349d4900aaf080\t00000000 1e 18\t1970-07-28T08:03:20.000000Z\n" +
                        "93\tI\t-2323\t726142\tNaN\t0.8123\t0.9067116554825199\t1979-05-11T13:11:40.594Z\t1969-12-31T23:59:57.415320Z\t\t\ttrue\t\t0xc1b67a610f845c38bf73e9dd66895579d14a19b0f4078a02f5aceb288984dbc4\t00000000 48 ef 10 1b\t1970-10-28T22:16:40.000000Z\n" +
                        "40\tS\t-20754\t640026\tNaN\t0.0678\tNaN\t1974-08-22T14:47:36.113Z\t1969-12-31T23:59:57.637072Z\tMXCV\tbbbbbb\tfalse\t\t0x99addce59fcc0d3d9830ab914dab674c60bb78c3b0ee86df40da1c6ee057e8d1\t00000000 36 ab\t1970-11-21T01:50:00.000000Z\n" +
                        "62\tZ\t-5605\t897677\t-25929\tNaN\t0.5082431508355564\t1980-04-18T20:36:30.440Z\t1969-12-31T23:59:54.628420Z\tSPTT\tbbbbbb\ttrue\t\t0xa240ef161b45a1e48cf44c1c5bb6f9ab9d0f3bc8b1362801a7f6d25047c701e6\t00000000 cf b3\t1970-12-14T05:23:20.000000Z\n" +
                        "117\tY\t-14561\tNaN\t-89408\t0.0626\t0.6810629367436306\t1980-11-24T14:57:59.806Z\t1969-12-31T23:59:59.009732Z\tXVBHB\taaa\tfalse\t\t0xc8d5d8e5f9c2007489b7325f72e6f3d0a2402a0318a5834ee45764d96505b53b\t\t1971-11-03T07:10:00.000000Z\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select distinct k from t_day",
                sink,
                "k\n" +
                        "c\n" +
                        "aaa\n" +
                        "bbbbbb\n" +
                        "\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_day",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "108\tJ\t-11076\tNaN\t29326\t0.1656\t0.18227834647927288\t1973-07-03T11:43:25.390Z\t1969-12-31T23:59:58.122080Z\t\tc\tfalse\tbbbbbb\t0xef8b237502911d607b2c40a4af457c366e7571b9df3a7c23e8da1ba561cec298\t\t1970-01-01T00:03:20.000000Z\n" +
                        "56\tJ\t-20167\t278104\t-3552\t0.5223\tNaN\t1976-09-08T12:29:54.721Z\t1969-12-31T23:59:58.405688Z\tLEOOT\t\tfalse\tbbbbbb\t0x778a900a82a7b55e93023940ab7f1c09b5899bf0ee7c97de971dba16373fc809\t00000000 5d fd 71 5b f4\t1970-01-24T03:36:40.000000Z\n" +
                        "84\tW\t22207\t119693\t-53159\t0.7673\t0.9031596755527048\t1981-06-03T08:19:47.717Z\t1969-12-31T23:59:53.440321Z\tXSFNG\t\tfalse\tc\t0x61554422489276b898793b266f0b9ea889eb18c5658e6b101acb3e4b0ac462ad\t00000000 bb e2\t1970-02-16T07:10:00.000000Z\n" +
                        "21\tQ\t3496\t578025\t-98876\t0.4248\t0.09648061880838898\t\t1969-12-31T23:59:55.291958Z\tKHPDG\taaa\tfalse\tc\t0x8312419bf7d5dca6c0cd3d0426f714237af2b1e5f23f205b6c80faa3b741b282\t\t1970-03-11T10:43:20.000000Z\n" +
                        "112\tO\t-3429\t239209\t36115\t0.2531\t0.985179782114092\t1970-10-03T03:15:48.703Z\t1969-12-31T23:59:53.582491Z\tYBDS\tbbbbbb\ttrue\tc\t0x4599ece7e898dd5324fb604ddf70c54037966205b94caf995a585e64bbd4204d\t00000000 93 b2 7b c7 55\t1970-04-03T14:16:40.000000Z\n" +
                        "80\tB\t-14851\t618043\t29379\t0.5751\t0.06115382042103079\t1970-10-11T15:55:16.483Z\t1969-12-31T23:59:58.741262Z\t\tc\tfalse\taaa\t0x63ca8f11fefb381c5034365a1ad8cfbc6351b32c301693a89ec31d67e4bc804a\t\t1970-04-26T17:50:00.000000Z\n" +
                        "59\tD\t-25955\t982077\tNaN\t0.9965\t0.023780164968562834\t1976-04-09T11:44:34.276Z\t1970-01-01T00:00:00.214711Z\tPBJC\tc\ttrue\tc\t0x168045b95793bbeb0ee8ff621c5e48fd3e811a742f3bb51c78292859b45b4767\t\t1970-05-19T21:23:20.000000Z\n" +
                        "77\tW\t-29635\t36806\t39204\t0.0244\t0.4452645911659644\t1974-10-14T05:22:22.780Z\t1970-01-01T00:00:00.181107Z\tJJDSR\tbbbbbb\ttrue\t\t0xd89fec5fab3f4af1a0ca0ec5d6448e2d798d79cb982de744b96a662a0b9f32d7\t00000000 aa 41 c5 55 ef\t1970-06-12T00:56:40.000000Z\n" +
                        "83\tU\t-11505\tNaN\t12242\t0.3216\t0.01267185841652596\t\t1969-12-31T23:59:52.165524Z\tVVWM\tbbbbbb\ttrue\taaa\t0x996a433b566bb6584cb8c2e5513491232a1690ea6ff5f1863f80bc189977dfe3\t\t1970-07-05T04:30:00.000000Z\n" +
                        "63\tI\t-32736\t467385\t-76685\tNaN\t0.31674150508412846\t1975-03-23T06:58:43.118Z\t1970-01-01T00:00:00.400171Z\tVIRB\t\tfalse\t\t0xf6f008b8e0bd93ffe884685ca40e1575d3389fdb74d0af7b8e349d4900aaf080\t00000000 1e 18\t1970-07-28T08:03:20.000000Z\n" +
                        "71\tO\t-18571\t699017\tNaN\t0.7827\t0.9308109024121684\t1978-05-28T23:57:59.482Z\t1970-01-01T00:00:00.678374Z\t\tbbbbbb\tfalse\tbbbbbb\t0x44e5f7a335ee650a0a8b38ca752d04b41ccd99af986677b82e1c703573ce9d4f\t00000000 cb 9e 2f 0c 42 fa 8c b9 cc 4b\t1970-08-20T11:36:40.000000Z\n" +
                        "91\tH\t18079\t892554\t91585\t0.4595\t0.9156453066150101\t1970-10-13T14:26:59.351Z\t1969-12-31T23:59:55.314601Z\tPCXYZ\taaa\ttrue\tc\t0xcd1953974ba8d46feb17cc29c85b213a9c522fadfd26d20430f1e9aa665fa3f7\t00000000 22 ac a3 2d\t1970-09-12T15:10:00.000000Z\n" +
                        "53\tT\t12219\t703085\t-38022\t0.3588\t0.6485577517124145\t\t1969-12-31T23:59:59.580244Z\tHGPKX\t\ttrue\taaa\t0xa3d07224463d9738b1a6665ab4644be3bbfad2a44a30f84aa70288296446782c\t00000000 3e 99 60\t1970-10-05T18:43:20.000000Z\n" +
                        "93\tI\t-2323\t726142\tNaN\t0.8123\t0.9067116554825199\t1979-05-11T13:11:40.594Z\t1969-12-31T23:59:57.415320Z\t\t\ttrue\t\t0xc1b67a610f845c38bf73e9dd66895579d14a19b0f4078a02f5aceb288984dbc4\t00000000 48 ef 10 1b\t1970-10-28T22:16:40.000000Z\n" +
                        "40\tS\t-20754\t640026\tNaN\t0.0678\tNaN\t1974-08-22T14:47:36.113Z\t1969-12-31T23:59:57.637072Z\tMXCV\tbbbbbb\tfalse\t\t0x99addce59fcc0d3d9830ab914dab674c60bb78c3b0ee86df40da1c6ee057e8d1\t00000000 36 ab\t1970-11-21T01:50:00.000000Z\n" +
                        "62\tZ\t-5605\t897677\t-25929\tNaN\t0.5082431508355564\t1980-04-18T20:36:30.440Z\t1969-12-31T23:59:54.628420Z\tSPTT\tbbbbbb\ttrue\t\t0xa240ef161b45a1e48cf44c1c5bb6f9ab9d0f3bc8b1362801a7f6d25047c701e6\t00000000 cf b3\t1970-12-14T05:23:20.000000Z\n" +
                        "95\tC\t-15985\t176110\t70084\t0.6934\t0.15363439252599098\t\t1969-12-31T23:59:57.580964Z\tJBWUL\tbbbbbb\ttrue\tc\t0xa7c0b11aa7ddf33f6c595c65977627ea0bfead10d478f3ee22cdbc98872d39f6\t\t1971-01-06T08:56:40.000000Z\n" +
                        "125\tZ\t-10713\t371844\t84132\t0.8621\t0.2508999003038844\t\t1969-12-31T23:59:57.403164Z\tCDUI\tbbbbbb\tfalse\tc\t0x34f81ebac91b1d1a2ae9bef212d736c25a08b548519baff07ab873d0d0a9fd3d\t00000000 7b 1e\t1971-01-29T12:30:00.000000Z\n" +
                        "76\tE\t16948\tNaN\t-51683\t0.2293\tNaN\t\t1969-12-31T23:59:56.380382Z\tVKYQJ\t\tfalse\taaa\t0xcba8fd1a62c96efebb77d1385fe652d28915affdb5c63060cbebb5fea81ad62e\t00000000 6d 46 ad 16 79 58 cd\t1971-02-21T16:03:20.000000Z\n" +
                        "123\tC\t-5778\t967519\t-75175\t0.1399\t0.010165385801274796\t1973-08-17T06:06:47.663Z\t1969-12-31T23:59:54.612524Z\tKJKNZ\taaa\ttrue\tc\t0x71031551e5cf812198c2fb5501ed706de28a27ae45b942b2cbaf06bbb06e7456\t00000000 20 b9 97 5a 8a 80\t1971-03-16T19:36:40.000000Z\n" +
                        "103\tU\t-1923\t479823\t-30531\tNaN\tNaN\t1981-07-09T21:16:23.406Z\t1969-12-31T23:59:57.889468Z\tIMYS\t\ttrue\taaa\t0xaf3dd1ee4746fc4994282c8ee2d7086f5ece732b9624346416451f8e583fb972\t00000000 c1 0e 21 fd 77\t1971-04-08T23:10:00.000000Z\n" +
                        "45\tW\t-425\t875599\t15597\tNaN\tNaN\t1972-09-05T18:56:20.615Z\t1969-12-31T23:59:59.743987Z\tVYRZO\tbbbbbb\tfalse\tbbbbbb\t0xa3a258166f137cd67dee650ead11bfea6d4aaac202727c064541228329d53e80\t\t1971-05-02T02:43:20.000000Z\n" +
                        "126\tT\t-12465\t427824\t-25987\t0.5469\tNaN\t1973-08-27T12:11:42.793Z\t1969-12-31T23:59:54.102211Z\t\tbbbbbb\tfalse\tbbbbbb\t0x5d07e75ccc568037500e39f10ee3b8f8c0f5c2a3e10d06e286c6aa16cec3af38\t00000000 3a d8\t1971-05-25T06:16:40.000000Z\n" +
                        "55\tG\t2234\t662718\t-61238\t0.0668\tNaN\t1977-09-11T20:16:36.619Z\t1969-12-31T23:59:55.378625Z\tECVFF\t\ttrue\taaa\t0xab1c5ce7dcb50cb4ce0fdf8d23f841a2e3fcb395ad3f16d4c103d859de058032\t\t1971-06-17T09:50:00.000000Z\n" +
                        "74\tJ\t22479\tNaN\t41900\t0.0591\t0.6092038333046033\t1971-04-02T20:36:22.096Z\t1969-12-31T23:59:55.594215Z\tBWZRJ\tbbbbbb\ttrue\tc\t0xd7f4199375d9353ea191edc98a6167778483177c32851224a01f3e8c5cbc7a82\t00000000 56 cc a1 80 09 f0\t1971-07-10T13:23:20.000000Z\n" +
                        "79\tF\t-14578\t450030\t38187\tNaN\t0.3883845787817346\t\t\t\taaa\tfalse\taaa\t0x8055ab03d5d5c47d62b1d77660c32e8e7e5278c7ef6bde237ffa86c6b59779db\t00000000 2d e8 95 30 44 e2 5d\t1971-08-02T16:56:40.000000Z\n" +
                        "43\tC\t-8135\t213809\t-1275\tNaN\t0.5418085488978492\t1971-07-29T01:51:45.288Z\t1969-12-31T23:59:54.180636Z\tEMPP\tbbbbbb\tfalse\tc\t0x9b862233a0c38afca5ba600b164aa6b871cc06618b2b88cc88a4933315f646ec\t00000000 4e a5 4b 2b 97 cc\t1971-08-25T20:30:00.000000Z\n" +
                        "54\tU\t-30570\t374221\tNaN\t0.7460\t0.7437656766929067\t1980-12-01T08:11:10.449Z\t1969-12-31T23:59:57.538081Z\t\tbbbbbb\ttrue\tbbbbbb\t0x1edcdcb17865610639479e2958bfb643aa4861ba1f6724e5ae3ac4819410bc23\t00000000 ce a4 ea 3d c9\t1971-09-18T00:03:20.000000Z\n" +
                        "89\tX\t-5823\t133431\tNaN\t0.1119\t0.16347836851309816\t1982-07-31T14:53:51.284Z\t1969-12-31T23:59:57.661336Z\tYEKNP\t\ttrue\taaa\t0x21ef7ce14b550b25832181a273bc6a92948314655a3b28352acb6fd98d895a10\t\t1971-10-11T03:36:40.000000Z\n" +
                        "117\tY\t-14561\tNaN\t-89408\t0.0626\t0.6810629367436306\t1980-11-24T14:57:59.806Z\t1969-12-31T23:59:59.009732Z\tXVBHB\taaa\tfalse\t\t0xc8d5d8e5f9c2007489b7325f72e6f3d0a2402a0318a5834ee45764d96505b53b\t\t1971-11-03T07:10:00.000000Z\n"
        );
    }

    private void assertMonth() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_month where m = null",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "119\tV\t3178\t837756\t-81585\t0.7627\t0.5219904713000894\t1978-05-08T19:14:22.368Z\t1969-12-31T23:59:59.756314Z\tNCPVU\tbbbbbb\tfalse\t\t0x92e216e13e061220bcc73ae25c8f5957ced00e989db8c99c77b67aaf034c0258\t00000000 5f a3 32\t1970-01-01T00:03:20.000000Z\n" +
                        "121\tR\t-32248\t396050\t-35751\t0.7817\t0.9959337662774681\t\t1969-12-31T23:59:58.588233Z\tDVQMI\tc\ttrue\t\t0xa5bb89788829b308c718693d412ea1d96036838211b943c2609c2d1104b89752\t00000000 31 5e e0\t1970-08-20T11:36:40.000000Z\n" +
                        "127\tU\t-2321\t463796\t18370\t0.1165\t0.9443378372150231\t1977-08-17T04:40:59.569Z\t1969-12-31T23:59:52.196177Z\tWURWB\tc\tfalse\t\t0x6f25b86bb472b338ab35729b5cee1b3871d3cc3f2f8d645824bf6d6e797dfc3c\t00000000 21 57 eb c4 9c\t1971-11-26T10:43:20.000000Z\n" +
                        "112\tP\t9802\tNaN\t75166\t0.1184\t0.3494250368753934\t1975-09-30T10:09:31.504Z\t1969-12-31T23:59:55.762374Z\t\taaa\tfalse\t\t0x5c0d7d492392e518bc746464a1bc90a182d3dc20f47b3454237a6eab48804528\t00000000 cd b2 fa d5 2a 19 1b c9\t1978-11-15T17:50:00.000000Z\n" +
                        "124\tI\t-30723\t159684\t-45800\t0.0573\t0.8248769037655503\t1975-09-08T12:04:39.272Z\t1969-12-31T23:59:57.670332Z\tYNOR\taaa\ttrue\t\t0xcd186db59e62b037b98b79aef5acec1fdb8cfee61427b2cc7d13df361ce20ad9\t\t1979-07-05T05:23:20.000000Z\n" +
                        "25\tZ\t-6353\t96442\t-49636\tNaN\t0.40549501732612403\t1971-07-13T02:33:26.128Z\t1969-12-31T23:59:55.204464Z\t\t\tfalse\t\t0xb9b7551ff3f855ab6cf5a8f96caceffc3ed28138e7ede3ed9ec0ec50fa4f863b\t00000000 bb f4 7c 7a c2 a4 77 a6 8f aa\t1987-02-11T00:03:20.000000Z\n" +
                        "71\tD\t694\t772203\t-32603\t0.9552\t0.2769130518011955\t1978-12-25T19:08:51.960Z\t1969-12-31T23:59:55.754471Z\t\tc\tfalse\t\t0x310d0e7a171e7f3c97f6387cdfad01f8cd2b0e4fd8b234fba3804ab8de1f8951\t00000000 80 24 24 8c d4 48 55 31 89\t1987-09-30T11:36:40.000000Z\n" +
                        "103\tL\t18557\tNaN\t-19361\t0.1200\tNaN\t1970-08-08T18:27:58.955Z\t1969-12-31T23:59:58.439595Z\t\tc\ttrue\t\t0xbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e85524b903924c9c2\t00000000 18 e2 56 c6 ce\t1988-05-18T23:10:00.000000Z\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select distinct k from t_month",
                sink,
                "k\n" +
                        "bbbbbb\n" +
                        "c\n" +
                        "aaa\n" +
                        "\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_month",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "119\tV\t3178\t837756\t-81585\t0.7627\t0.5219904713000894\t1978-05-08T19:14:22.368Z\t1969-12-31T23:59:59.756314Z\tNCPVU\tbbbbbb\tfalse\t\t0x92e216e13e061220bcc73ae25c8f5957ced00e989db8c99c77b67aaf034c0258\t00000000 5f a3 32\t1970-01-01T00:03:20.000000Z\n" +
                        "121\tR\t-32248\t396050\t-35751\t0.7817\t0.9959337662774681\t\t1969-12-31T23:59:58.588233Z\tDVQMI\tc\ttrue\t\t0xa5bb89788829b308c718693d412ea1d96036838211b943c2609c2d1104b89752\t00000000 31 5e e0\t1970-08-20T11:36:40.000000Z\n" +
                        "116\tS\t11789\t285245\tNaN\t0.2749\t0.48199798371959646\t1982-01-20T22:04:01.395Z\t1969-12-31T23:59:59.374384Z\t\t\ttrue\tbbbbbb\t0x675acee433526bf1d88d74b0004aefb5c821d25caad824b8917d8cbbb2aedd63\t00000000 ee ae 90 2b b8 c3 15 8d\t1971-04-08T23:10:00.000000Z\n" +
                        "127\tU\t-2321\t463796\t18370\t0.1165\t0.9443378372150231\t1977-08-17T04:40:59.569Z\t1969-12-31T23:59:52.196177Z\tWURWB\tc\tfalse\t\t0x6f25b86bb472b338ab35729b5cee1b3871d3cc3f2f8d645824bf6d6e797dfc3c\t00000000 21 57 eb c4 9c\t1971-11-26T10:43:20.000000Z\n" +
                        "74\tD\t25362\t682088\t37064\tNaN\t0.09929966140707747\t1980-04-11T15:49:40.209Z\t\tLVCFY\tbbbbbb\ttrue\tc\t0x7759fff2e17726f537c5aa6210177ec0322db0e47bccc1339c7a7acc9ce0d624\t00000000 83 a5 d3\t1972-07-14T22:16:40.000000Z\n" +
                        "62\tE\t10511\tNaN\t-99912\tNaN\t0.8431363786541575\t1974-01-22T15:44:54.116Z\t1969-12-31T23:59:56.458163Z\t\taaa\ttrue\tbbbbbb\t0xa5ee4544ec1bf467758e2a1d53764b76714175f1867c790ad148a849a06b6d92\t00000000 af 1f 6d 01 87 02\t1973-03-03T09:50:00.000000Z\n" +
                        "65\tW\t-2885\t851731\t-79233\t0.6637\t0.0032702345951136635\t1978-03-26T02:39:47.370Z\t1969-12-31T23:59:53.607294Z\tEQXY\tc\ttrue\taaa\t0x2a2d8dfbb6b68e3385a2cfc3f2af8639ac5f33609469d81353bc6da60030d8c4\t\t1973-10-20T21:23:20.000000Z\n" +
                        "41\tC\t-16308\t320735\t-8435\t0.3306\t0.4365314986211333\t1972-07-10T12:25:13.365Z\t1969-12-31T23:59:57.792714Z\tOMCD\tbbbbbb\ttrue\tc\t0x9408a2099e8f9970956e71dfbbc6c2589f95d733600f062ce1631618978acea7\t00000000 08 72\t1974-06-09T08:56:40.000000Z\n" +
                        "122\tO\t17929\t499038\t85526\t0.8251\t0.7117525079805059\t1978-06-27T11:47:04.188Z\t1969-12-31T23:59:55.096132Z\tZQWOZ\tc\tfalse\taaa\t0xb29789a0923b219b7aad19b3ada4cbd13129ef29aa7547dc37c477321055e4ae\t00000000 03 a6\t1975-01-26T20:30:00.000000Z\n" +
                        "97\tY\t7715\t873148\t-42788\t0.9385\t0.28698925385676677\t\t1969-12-31T23:59:54.146676Z\tGUSNT\t\ttrue\taaa\t0xc703358a596b6695b93045abac9b3264c417d9b6495ba67e9ee699fb01f7932a\t00000000 34 03 98 a6 0b 6d\t1975-09-15T08:03:20.000000Z\n" +
                        "53\tH\t1057\t496919\tNaN\t0.6305\t0.4861198096781342\t\t1969-12-31T23:59:56.674949Z\tEDVJQ\t\tfalse\taaa\t0xce33ca110168c203965a2a2c32bff717790ccc4f2c72b5dfb13efba1586d6a49\t00000000 08 06 7d d7 68 a7 02\t1976-05-03T19:36:40.000000Z\n" +
                        "56\tG\t-14345\t178321\t-69806\t0.4987\t0.6847998121758173\t1974-01-20T21:14:56.131Z\t1970-01-01T00:00:00.463647Z\tNSIID\tbbbbbb\tfalse\taaa\t0xca42b6d6997edbf080188ba249467388ad8e3c136f645a57895df157b1395e4a\t00000000 6c 80 c7 bd 2d 37 3e 30\t1976-12-21T07:10:00.000000Z\n" +
                        "113\tC\t24888\t343744\tNaN\tNaN\t0.16178961011999748\t1975-03-28T01:50:13.399Z\t1969-12-31T23:59:55.139343Z\tVOJM\tbbbbbb\ttrue\tbbbbbb\t0x9049f886c9351b5cdaa5dffda51d3576abf19dc190f69f2b4073ebcdd52b5e55\t00000000 66 7d 27 8c e6 ef 80\t1977-08-09T18:43:20.000000Z\n" +
                        "87\tZ\t-22371\t505981\t99587\tNaN\t0.2410835757980544\t1974-04-20T17:12:15.237Z\t1969-12-31T23:59:53.138973Z\tSPMP\tc\ttrue\tc\t0x32bc4a46e7af0a4f6aba310c55d4a66b726939a19e0078d75d0fae7542ac9d47\t00000000 98 26 52 23 06 24\t1978-03-29T06:16:40.000000Z\n" +
                        "112\tP\t9802\tNaN\t75166\t0.1184\t0.3494250368753934\t1975-09-30T10:09:31.504Z\t1969-12-31T23:59:55.762374Z\t\taaa\tfalse\t\t0x5c0d7d492392e518bc746464a1bc90a182d3dc20f47b3454237a6eab48804528\t00000000 cd b2 fa d5 2a 19 1b c9\t1978-11-15T17:50:00.000000Z\n" +
                        "124\tI\t-30723\t159684\t-45800\t0.0573\t0.8248769037655503\t1975-09-08T12:04:39.272Z\t1969-12-31T23:59:57.670332Z\tYNOR\taaa\ttrue\t\t0xcd186db59e62b037b98b79aef5acec1fdb8cfee61427b2cc7d13df361ce20ad9\t\t1979-07-05T05:23:20.000000Z\n" +
                        "35\tV\t10152\t937720\tNaN\t0.2342\t0.24387410849395863\t1980-09-02T06:34:03.436Z\t1969-12-31T23:59:56.811075Z\t\tbbbbbb\ttrue\taaa\t0xb892843928ce6cf655b1e0cdb76095985530678f628cf540ac51fd65e909dd2b\t00000000 31 18 f8 f7\t1980-02-21T16:56:40.000000Z\n" +
                        "41\tZ\t26037\tNaN\t89462\t0.0602\tNaN\t\t1969-12-31T23:59:54.904714Z\tMMIF\t\tfalse\tbbbbbb\t0x710cc7c02bc588156c43b9f262cc9e0f6931f3fb92b26a54523533e24a77670a\t00000000 58 d7 0b ee a4 44 3d 4e 9d 5e\t1980-10-10T04:30:00.000000Z\n" +
                        "46\tK\t14022\t64332\t-60781\tNaN\t0.18417308856834846\t1971-01-26T11:21:09.657Z\t1969-12-31T23:59:55.195798Z\tGVGG\t\ttrue\tc\t0xec20eb4da5ad065ad9caaa437b378e7cc5bf7a38aaeb64829a20ed2c585143a3\t00000000 b9 02 a2 6d c8 d1 3f\t1981-05-29T16:03:20.000000Z\n" +
                        "39\tX\t-8230\tNaN\t35694\tNaN\t0.3269701313081723\t\t1969-12-31T23:59:54.970857Z\t\taaa\tfalse\tbbbbbb\t0x7af6bbe78536821a8bf4811e7aed057998a23d19950a827feabe384366e6192e\t00000000 ae e3 d4 a9 9f e2 80 4a 8f\t1982-01-16T03:36:40.000000Z\n" +
                        "108\tO\t-4769\t694856\t85538\t0.1997\t0.8749274453087652\t\t1969-12-31T23:59:55.522272Z\tGQQG\tc\tfalse\tbbbbbb\t0x6146ac1de762ff9b1c4ff53c1a7e75c348a8542e784d0b46894e90960c2b1566\t00000000 0e 9f 19 7a a5 59 1c 46 73 13\t1982-09-04T15:10:00.000000Z\n" +
                        "50\tX\t-27123\t560566\t-82780\t0.4146\t0.9759549117828917\t1981-07-18T09:19:11.677Z\t1969-12-31T23:59:59.898755Z\t\taaa\tfalse\tbbbbbb\t0x67508865e9fd829377fcbc62132240bba42f8cb906dbfd678a531d95fe4daa38\t00000000 e8 0d d8 46\t1983-04-24T02:43:20.000000Z\n" +
                        "111\tW\t-6198\t884762\t5649\tNaN\t0.8884541492501596\t\t1969-12-31T23:59:53.963943Z\tNSEPE\t\tfalse\tbbbbbb\t0xd11cbdb698d1ef4aae1214cc77df7212862a8351d1db91556f2ecb55af0f815b\t00000000 37 6c 12\t1983-12-11T14:16:40.000000Z\n" +
                        "86\tI\t-18677\tNaN\t86707\t0.8186\t0.24968743442275476\t1971-01-31T08:29:50.740Z\t1969-12-31T23:59:57.896244Z\tDBDZ\t\tfalse\tbbbbbb\t0xb457723c814406d987e706dfb816e47365239b4387a4df1148a39f2d3190b0d7\t00000000 ee bc b6 13 7f 1a d1\t1984-07-30T01:50:00.000000Z\n" +
                        "23\tV\t9344\t937468\t83002\tNaN\t0.9087477391550631\t\t1969-12-31T23:59:52.816621Z\tDQSJE\taaa\tfalse\taaa\t0xbeed39c0ca2d6240b48eca1f317663704544fc9ecbcca1a7380e208857582bdb\t\t1985-03-18T13:23:20.000000Z\n" +
                        "23\tL\t17130\t50424\t-91882\t0.7099\t0.03485868034117512\t1974-10-10T00:42:59.711Z\t1969-12-31T23:59:56.393043Z\tDOVTB\taaa\tfalse\taaa\t0x0db35b850a0c86a863bf891222dca6dc7bcf843650f3904384f84eb517304fcd\t00000000 ae 18 29\t1985-11-05T00:56:40.000000Z\n" +
                        "27\tO\t-4792\tNaN\tNaN\t0.1914\t0.5861227201637977\t\t1969-12-31T23:59:55.767548Z\tYXVN\tc\ttrue\taaa\t0x7fd8b541faab2da1b7711dca20fee22bbfc672ef949ba13cb63f0a64aba2d306\t00000000 77 db 6a 54 0e 5a 51\t1986-06-24T12:30:00.000000Z\n" +
                        "25\tZ\t-6353\t96442\t-49636\tNaN\t0.40549501732612403\t1971-07-13T02:33:26.128Z\t1969-12-31T23:59:55.204464Z\t\t\tfalse\t\t0xb9b7551ff3f855ab6cf5a8f96caceffc3ed28138e7ede3ed9ec0ec50fa4f863b\t00000000 bb f4 7c 7a c2 a4 77 a6 8f aa\t1987-02-11T00:03:20.000000Z\n" +
                        "71\tD\t694\t772203\t-32603\t0.9552\t0.2769130518011955\t1978-12-25T19:08:51.960Z\t1969-12-31T23:59:55.754471Z\t\tc\tfalse\t\t0x310d0e7a171e7f3c97f6387cdfad01f8cd2b0e4fd8b234fba3804ab8de1f8951\t00000000 80 24 24 8c d4 48 55 31 89\t1987-09-30T11:36:40.000000Z\n" +
                        "103\tL\t18557\tNaN\t-19361\t0.1200\tNaN\t1970-08-08T18:27:58.955Z\t1969-12-31T23:59:58.439595Z\t\tc\ttrue\t\t0xbc7827bb6dc3ce15b85b002a9fea625b95bdcc28b3dfce1e85524b903924c9c2\t00000000 18 e2 56 c6 ce\t1988-05-18T23:10:00.000000Z\n"
        );
    }

    private void assertNone() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_none where m = null",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "35\tN\t-27680\t935010\t96731\t0.3982\t0.6951385535362276\t1977-06-23T09:50:16.611Z\t1969-12-31T23:59:56.584555Z\tUMMZS\taaa\tfalse\t\t0x6282dda91ca20ccda519bc9c0850a07eaa0106bdfb6d9cb66a8b4eb2174a93ff\t00000000 66 94 89\t1970-01-01T00:00:00.000100Z\n" +
                        "123\tW\t-9190\t4359\t53700\t0.5512\t0.7751886508004251\t\t1969-12-31T23:59:56.997792Z\t\taaa\ttrue\t\t0xb53a88f6d83626dbba48ceb2cd9f1f07ae01938cee1007258d8b20fb9ccc2ead\t00000000 33 6e\t1970-01-01T00:03:20.000100Z\n" +
                        "28\tX\t16531\t290127\tNaN\t0.9410\t0.19073234832401043\t1970-09-08T05:43:11.975Z\t1969-12-31T23:59:58.870246Z\tZNLC\taaa\ttrue\t\t0x54377431fb8f0a1d69f0d9820fd1cadec864e47ae90eb46081c811d8777fbd9e\t00000000 7e f3 04 4a 73 f0 31 3e 55 3e\t1970-01-01T00:06:40.000100Z\n" +
                        "105\tO\t-30317\t914327\t-61430\tNaN\t0.9510816389659975\t1970-10-21T18:26:17.382Z\t1970-01-01T00:00:00.156025Z\t\tc\ttrue\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\t00000000 a6 6b\t1970-01-01T00:10:00.000100Z\n" +
                        "111\tP\t-329\t311595\t-45587\t0.8941\t0.7579806386710018\t1978-03-20T18:13:35.843Z\t1969-12-31T23:59:54.837806Z\tBCZI\taaa\tfalse\t\t0x4099211c7746712f1eafc5dd81b883a70842384e566d4677022f1bcf3743bcd7\t\t1970-01-01T00:20:00.000100Z\n" +
                        "109\tC\t18436\t792419\t-4632\t0.4548\t0.4127332979349321\t1977-05-27T20:12:14.870Z\t1969-12-31T23:59:56.408102Z\tWXCYX\tc\ttrue\t\t0xeabafb21d80fdee5ee61d9b1164bab329cae4ab2116295ca3cb2022847cd5463\t00000000 52 c6 94 c3 18 c9 7c 70 9f\t1970-01-01T00:36:40.000100Z\n" +
                        "72\tR\t-30107\t357231\tNaN\t0.8622\t0.2232959099494619\t1980-03-14T04:19:57.116Z\t1969-12-31T23:59:58.790561Z\tTDTF\taaa\tfalse\t\t0xdb80a4d5776e596386f3c6ec12ee8cfe67efb684ddbe470d799808408f62675f\t\t1970-01-01T01:13:20.000100Z\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select distinct k from t_none",
                sink,
                "k\n" +
                        "aaa\n" +
                        "c\n" +
                        "bbbbbb\n" +
                        "\n"
        );
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_none",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "35\tN\t-27680\t935010\t96731\t0.3982\t0.6951385535362276\t1977-06-23T09:50:16.611Z\t1969-12-31T23:59:56.584555Z\tUMMZS\taaa\tfalse\t\t0x6282dda91ca20ccda519bc9c0850a07eaa0106bdfb6d9cb66a8b4eb2174a93ff\t00000000 66 94 89\t1970-01-01T00:00:00.000100Z\n" +
                        "123\tW\t-9190\t4359\t53700\t0.5512\t0.7751886508004251\t\t1969-12-31T23:59:56.997792Z\t\taaa\ttrue\t\t0xb53a88f6d83626dbba48ceb2cd9f1f07ae01938cee1007258d8b20fb9ccc2ead\t00000000 33 6e\t1970-01-01T00:03:20.000100Z\n" +
                        "28\tX\t16531\t290127\tNaN\t0.9410\t0.19073234832401043\t1970-09-08T05:43:11.975Z\t1969-12-31T23:59:58.870246Z\tZNLC\taaa\ttrue\t\t0x54377431fb8f0a1d69f0d9820fd1cadec864e47ae90eb46081c811d8777fbd9e\t00000000 7e f3 04 4a 73 f0 31 3e 55 3e\t1970-01-01T00:06:40.000100Z\n" +
                        "105\tO\t-30317\t914327\t-61430\tNaN\t0.9510816389659975\t1970-10-21T18:26:17.382Z\t1970-01-01T00:00:00.156025Z\t\tc\ttrue\t\t0x5b8def4e7a017e884a3c2c504403708b49fb8d5fe0ff283cbac6499e71ce5b30\t00000000 a6 6b\t1970-01-01T00:10:00.000100Z\n" +
                        "35\tD\t-5793\t89447\t-35803\t0.0300\t0.8061988461374605\t1980-01-22T14:53:03.123Z\t1969-12-31T23:59:55.867852Z\tEGHL\taaa\ttrue\tc\t0x82fa2d8ff66e01977f4f8e692631f943b2334e48803d047ea14e7377e6f5d520\t00000000 5c d9\t1970-01-01T00:13:20.000100Z\n" +
                        "118\tV\t16219\t688593\tNaN\t0.5956\t0.23956847762469535\t1977-07-31T16:16:34.689Z\t1969-12-31T23:59:59.130577Z\t\t\ttrue\tbbbbbb\t0x955a48b4c0d3e6b4ea7faedda2849361b9bcdfc30f4bd6234d9b57fb6d09ec03\t00000000 03 5b 11 44 83 06 63 2b 58\t1970-01-01T00:16:40.000100Z\n" +
                        "111\tP\t-329\t311595\t-45587\t0.8941\t0.7579806386710018\t1978-03-20T18:13:35.843Z\t1969-12-31T23:59:54.837806Z\tBCZI\taaa\tfalse\t\t0x4099211c7746712f1eafc5dd81b883a70842384e566d4677022f1bcf3743bcd7\t\t1970-01-01T00:20:00.000100Z\n" +
                        "22\tT\t-24267\t-10028\t63657\t0.7537\t0.5570298738371094\t\t1970-01-01T00:00:00.483602Z\t\tc\tfalse\taaa\t0xa2192a97fa3480616ba3f8da2d48aa34a5686de9830593f7c707cb8de0171211\t00000000 51 d7 eb b1 07 71\t1970-01-01T00:23:20.000100Z\n" +
                        "102\tS\t-1873\t804344\t-91510\t0.8670\t0.7374999472642795\t1972-09-16T00:05:16.136Z\t1969-12-31T23:59:58.908865Z\tMZCC\t\ttrue\tc\t0x6c05f272348619932296dd7689d89e416ae51cd947740a0e939e78ab607cc23f\t00000000 ca 10 2f 60 ce 59 1c 79 dd\t1970-01-01T00:26:40.000100Z\n" +
                        "118\tP\t-18809\t425874\t-75558\tNaN\t0.2522102209201954\t1980-07-27T11:56:48.011Z\t1969-12-31T23:59:59.168876Z\tKRZU\tbbbbbb\tfalse\tc\t0x73081d3e950671a5c12ecd4e28671aad67e71f507f89fb753923be5f519fda86\t\t1970-01-01T00:30:00.000100Z\n" +
                        "111\tY\t-14488\t952580\t22063\t0.4899\t0.47768252726422167\t\t1969-12-31T23:59:57.420903Z\tFFLTR\taaa\tfalse\tc\t0x72cc09749762925e4a98f54e6e543538451196db6d823470484849dd2d80d95e\t00000000 8f 10 c3 50 ce 4a 20 0f 7f\t1970-01-01T00:33:20.000100Z\n" +
                        "109\tC\t18436\t792419\t-4632\t0.4548\t0.4127332979349321\t1977-05-27T20:12:14.870Z\t1969-12-31T23:59:56.408102Z\tWXCYX\tc\ttrue\t\t0xeabafb21d80fdee5ee61d9b1164bab329cae4ab2116295ca3cb2022847cd5463\t00000000 52 c6 94 c3 18 c9 7c 70 9f\t1970-01-01T00:36:40.000100Z\n" +
                        "84\tL\t19528\tNaN\t-88960\t0.4783\t0.1061278014852981\t1979-05-27T12:29:17.272Z\t1969-12-31T23:59:55.725272Z\tRTGZ\tc\ttrue\tbbbbbb\t0x7429f999bffc9548aa3df14bfed429697199d69ef47f96aab53be4222160be9c\t00000000 d6 14 8b 7f 03 4f\t1970-01-01T00:40:00.000100Z\n" +
                        "118\tK\t12829\t858070\t-57757\t0.7400\t0.654226248740447\t1976-04-25T02:26:30.330Z\t1969-12-31T23:59:55.439991Z\tFWZSG\tbbbbbb\tfalse\tc\t0xa01929b632a0080165ffef9d779142923625ff207eb768073038742f72879e14\t00000000 a1 31\t1970-01-01T00:43:20.000100Z\n" +
                        "73\tR\t32450\t937836\t-72140\tNaN\t0.20943619156614035\t1972-02-08T06:20:20.488Z\t1969-12-31T23:59:54.714881Z\tKIBWF\tc\ttrue\taaa\t0xc95c1d5a4869bab982a8831f8a8662d35fa2bf53cef84c2997af9db84b80545e\t00000000 95 fa 1f 92 24 b1 b8 67 65\t1970-01-01T00:46:40.000100Z\n" +
                        "28\tU\t-13320\t939322\t-37762\t0.0506\t0.9030520258856007\t1974-12-19T14:37:33.525Z\t1970-01-01T00:00:00.692966Z\tKRPC\tbbbbbb\tfalse\tc\t0xe07f8fe692bcc063a85a5fc20776e82b36c1cdbfe34eb2636eec4ffc0b44f925\t00000000 1c 47 7d b6 46 ba bb 98\t1970-01-01T00:50:00.000100Z\n" +
                        "54\tY\t16062\t646278\tNaN\t0.4773\t0.3082260347287745\t1980-03-10T00:39:59.534Z\t1969-12-31T23:59:55.419704Z\tRMDB\tc\tfalse\tc\t0xe1d2020be2cb7be9c5b68f9ea1bd30c789e6d0729d44b64390678b574ed0f592\t00000000 54 02 9f c2 37\t1970-01-01T00:53:20.000100Z\n" +
                        "28\tY\t25787\t419229\t-25197\t0.4970\t0.48264093321778834\t1979-11-27T15:52:08.376Z\t1969-12-31T23:59:56.532200Z\tVUYGM\taaa\ttrue\tc\t0x8ea5fba6cf9bfc926616c7a12fd0faf9776d2b6ac26ea2865e890a15089598bc\t00000000 92 08 f1 96 7f a0 cf 00\t1970-01-01T00:56:40.000100Z\n" +
                        "84\tV\t8754\t-18574\t16222\t0.0954\t0.5143498202128743\t1972-10-30T22:56:25.960Z\t1969-12-31T23:59:55.022309Z\tHKKN\t\ttrue\tc\t0x8ef88e7b4b8c0f89e6185e455d7864ace44dd284cc61759cabb37a3474237eba\t00000000 e7 1f eb 30\t1970-01-01T01:00:00.000100Z\n" +
                        "96\tQ\t-16328\t963834\t91698\tNaN\t0.1211383169485164\t\t1969-12-31T23:59:52.690565Z\tZJGTB\tbbbbbb\tfalse\tbbbbbb\t0x647a0fb6a2550ff790dd7f3c6ce51e179717e2dae759fdb855724661cfcc811f\t00000000 c1 a7 5c c3 31 17 dd 8d c1 cf\t1970-01-01T01:03:20.000100Z\n" +
                        "20\tX\t-16590\t799945\tNaN\tNaN\t0.5501791172519537\t1976-06-04T12:13:35.262Z\t1969-12-31T23:59:54.629759Z\tFHXDB\taaa\ttrue\tbbbbbb\t0x49e63c20e929b2c452fa9d86a13a75d798896b6a7d7a7394dba36f0c738708ba\t00000000 fc d2 8e 79 ec\t1970-01-01T01:06:40.000100Z\n" +
                        "90\tB\t-21455\t481425\t-91115\t0.8090\t0.14319965942499036\t1972-04-24T19:08:46.637Z\t1969-12-31T23:59:54.151602Z\tEODD\tbbbbbb\ttrue\tc\t0x36394efec5b6ad384c7a93c9b229b5ef70e4d39b4fc580388fb2a24b0fac5693\t00000000 cb 8b 64 50 48\t1970-01-01T01:10:00.000100Z\n" +
                        "72\tR\t-30107\t357231\tNaN\t0.8622\t0.2232959099494619\t1980-03-14T04:19:57.116Z\t1969-12-31T23:59:58.790561Z\tTDTF\taaa\tfalse\t\t0xdb80a4d5776e596386f3c6ec12ee8cfe67efb684ddbe470d799808408f62675f\t\t1970-01-01T01:13:20.000100Z\n" +
                        "113\tV\t-14509\t351532\t34176\t0.7623\t0.24633823409315458\t1975-07-10T16:22:32.263Z\t1969-12-31T23:59:53.013535Z\tVTERO\tbbbbbb\ttrue\taaa\t0x93106ae5d36f7edcaddc44581a9b5083831267507abc5f248b4ca194333fe648\t00000000 b9 0f 97 f5 77 7e a3 2d\t1970-01-01T01:16:40.000100Z\n" +
                        "110\tE\t-6421\tNaN\t-28244\tNaN\t0.300574411191983\t1973-05-31T01:07:59.989Z\t1970-01-01T00:00:00.648812Z\tKNJGS\tbbbbbb\tfalse\taaa\t0x89cdfca4049617afc4ba3ddab10afad6c112d03b0d81666f95a1c05ce5a93104\t00000000 84 d5\t1970-01-01T01:20:00.000100Z\n" +
                        "30\tI\t-9871\t741546\t22371\t0.7673\tNaN\t1970-05-07T02:05:10.859Z\t1969-12-31T23:59:56.465828Z\tKMDC\tbbbbbb\ttrue\tc\t0xbd01cf83632884ae8b7083f888554b0c90a55c025349360f709e6f59acfd4c27\t00000000 a1 ce bf 46 36 0d 5b\t1970-01-01T01:23:20.000100Z\n" +
                        "85\tI\t-3438\tNaN\t-17738\t0.6571\t0.06194919049264147\t1971-10-28T15:11:37.298Z\t1969-12-31T23:59:57.991632Z\tHCTIV\tbbbbbb\ttrue\tbbbbbb\t0x4419df686878377072755f7fe4dcbef46c87ca839f7f85336695f15390d20b0f\t00000000 ba 0b 3a\t1970-01-01T01:26:40.000100Z\n" +
                        "124\tB\t-17121\t-5234\t-51881\t0.4302\tNaN\t1978-02-23T09:04:24.042Z\t1969-12-31T23:59:58.232565Z\t\t\ttrue\tc\t0x9dbf5fdd03586800c0c0031b11dca914a7f9a315f2636c92b3ab2c18e5048644\t\t1970-01-01T01:30:00.000100Z\n" +
                        "74\tP\t-6108\t692153\t-13210\t0.9183\t0.4972049796950656\t1975-12-19T18:26:40.375Z\t1970-01-01T00:00:00.503530Z\t\t\ttrue\tbbbbbb\t0x70a45fae3a920cb74e272e9dfde7bb12618178f7feba5021382a8c47a28fefa4\t\t1970-01-01T01:33:20.000100Z\n" +
                        "41\tS\t14835\t790240\t20957\tNaN\t0.30583440932161066\t1977-04-04T23:35:16.699Z\t1969-12-31T23:59:55.741431Z\tWKGZ\tbbbbbb\tfalse\tc\t0x266d0651dbeb8bcf8c484ee474dc1f93b352346df86ce19f9846b46e3dbd5e87\t00000000 54 67 7d 39 dc 8c 6c 6b ac\t1970-01-01T01:36:40.000100Z\n"
        );
    }

    private void assertNoneNts() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_none_nts where m = null",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\n" +
                        "120\tB\t10795\t469264\t11722\t0.6260\tNaN\t\t1969-12-31T23:59:55.466926Z\tQSRL\taaa\tfalse\t\t0x2f171b3f06f6387d2fd2b4a60ba2ba3b45430a38ef1cd9dc5bee3da4840085a6\t00000000 92 fe 69 38 e1 77 9a e7 0c 89\n" +
                        "71\tB\t18904\t857617\t31118\t0.1834\t0.6455967424250787\t1976-10-15T11:00:30.016Z\t1970-01-01T00:00:00.254417Z\tKRGII\taaa\ttrue\t\t0x3eef3f158e0843624d0fa2564c3517679a2dfd07dad695f78d5c4bed8432de98\t00000000 b0 ec 0b 92 58 7d 24 bc 2e 60\n" +
                        "28\tQ\t-11530\t640914\t-54143\t0.3229\t0.6852762111021103\t\t1969-12-31T23:59:52.759890Z\tJEUK\taaa\ttrue\t\t0x93e0ee36dbf0e422654cc358385f061661bd22a0228f78a299fe06bcdcb3a9e7\t00000000 1c 9c 1d 5c\n" +
                        "87\tL\t-2003\t842028\t12726\t0.7280\tNaN\t1982-02-21T09:56:08.687Z\t1969-12-31T23:59:52.977649Z\tFDYP\taaa\tfalse\t\t0x6fc129c805c2f5075fd086d7ede63e98492997ce455a2c2962e616e3a640fbca\t00000000 5b d6 cf 09 69 01\n" +
                        "115\tN\t312\t335480\t4602\t0.1476\t0.23673087740006105\t\t1969-12-31T23:59:57.132665Z\tRTPIQ\taaa\tfalse\t\t0xb139c160cb54bc065e565229e0e964b83e2dbd2d9d8d4081a00e467f6e96e622\t00000000 af 38 71 1f e1 e4 91 7d e9\n" +
                        "27\tJ\t-15254\t978974\t-36356\t0.7911\t0.7128505998532723\t1976-03-14T08:19:05.571Z\t1969-12-31T23:59:56.726487Z\tPZNYV\t\ttrue\t\t0x39af691594d0654567af4ec050eea188b8074532ac9f3c87c68ce6f3720e2b62\t00000000 20 13\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select distinct k from t_none_nts",
                sink,
                "k\n" +
                        "bbbbbb\n" +
                        "c\n" +
                        "aaa\n" +
                        "\n"
        );
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_none_nts",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\n" +
                        "76\tT\t-11455\t269293\t-12569\tNaN\t0.9344604857394011\t1975-06-01T05:39:43.711Z\t1969-12-31T23:59:54.300840Z\tXGZS\t\tfalse\tbbbbbb\t0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\t\n" +
                        "84\tG\t21781\t18201\t-29318\t0.8757\t0.1985581797355932\t1972-06-01T21:02:08.250Z\t1969-12-31T23:59:59.060058Z\tWLPDX\tbbbbbb\tfalse\taaa\t0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\t00000000 30 78\n" +
                        "70\tE\t-7374\t-56838\t84125\t0.7883\t0.810161274171258\t1971-03-15T07:13:33.865Z\t1969-12-31T23:59:54.241410Z\t\tbbbbbb\ttrue\tc\t0xb771e27f939096b9c356f99ae70523b585b80cec619f91784cd64b0b0a344f8e\t\n" +
                        "29\tM\t-5637\t-18625\t-72619\t0.3864\t0.92050039469858\t1973-11-03T02:24:00.817Z\t1969-12-31T23:59:54.204688Z\t\tc\tfalse\taaa\t0xa5f80be4b45bf437492990e1a29afcac07efe23cedb3250630d46a3a4749c41d\t00000000 82 89 2b 4d\n" +
                        "25\tS\t32326\t260025\t-94148\t0.8164\t0.8685154305419587\t1974-02-11T22:27:22.302Z\t1969-12-31T23:59:54.568746Z\tWCKY\taaa\tfalse\taaa\t0x8b4e4831499fc2a526567f4430b46b7f78c594c496995885aa1896d0ad3419d2\t00000000 4a 9d 46 7c 8d dd 93 e6\n" +
                        "120\tB\t10795\t469264\t11722\t0.6260\tNaN\t\t1969-12-31T23:59:55.466926Z\tQSRL\taaa\tfalse\t\t0x2f171b3f06f6387d2fd2b4a60ba2ba3b45430a38ef1cd9dc5bee3da4840085a6\t00000000 92 fe 69 38 e1 77 9a e7 0c 89\n" +
                        "32\tV\t21347\tNaN\t-51252\t0.5699\t0.9820662735672192\t1973-10-01T11:12:39.426Z\t1969-12-31T23:59:52.627140Z\tHRIPZ\taaa\ttrue\tbbbbbb\t0x89661af328d0e234d7eb56647bc4ff5794f1056ca855461d8c7d2ff9c4797b43\t\n" +
                        "71\tB\t18904\t857617\t31118\t0.1834\t0.6455967424250787\t1976-10-15T11:00:30.016Z\t1970-01-01T00:00:00.254417Z\tKRGII\taaa\ttrue\t\t0x3eef3f158e0843624d0fa2564c3517679a2dfd07dad695f78d5c4bed8432de98\t00000000 b0 ec 0b 92 58 7d 24 bc 2e 60\n" +
                        "70\tZ\t-26101\t222052\tNaN\t0.0516\t0.750281471677565\t1977-07-02T02:10:59.118Z\t1969-12-31T23:59:54.759805Z\t\t\ttrue\taaa\t0x7e25b91255572a8f86fd0ebdb6707e478b12abb31e08a62758e4b9de341dcbab\t\n" +
                        "55\tQ\t-24452\tNaN\t79091\t0.0509\tNaN\t1982-04-02T20:50:18.862Z\t1969-12-31T23:59:59.076086Z\tTWNWI\tbbbbbb\tfalse\tbbbbbb\t0x7db4b866b1f58ae47cb0e477eaa56479c23d0fe108be331ae7d356cb104e507a\t00000000 3a dc 5c 65 ff 27 67 77 12\n" +
                        "44\tV\t-16944\t506196\t62842\t0.6811\t0.20727557301543031\t1970-06-16T21:34:17.821Z\t1969-12-31T23:59:54.363025Z\t\tc\tfalse\tbbbbbb\t0x99904624c49b6d8a7d85ee2916b209c779406ab1f85e333a800f40e7a7d50d70\t00000000 cd 47 0b 0c 39\n" +
                        "114\tF\t-22523\t939724\t-3793\t0.7079\t0.03973283003449557\t1973-08-04T17:12:26.499Z\t1969-12-31T23:59:58.398062Z\tUQDYO\taaa\tfalse\tbbbbbb\t0x6d5992f2da279bf54f1ae0eca85e79fa9bae41871fd934427cbab83425e7712e\t\n" +
                        "105\tT\t29923\t529048\t-1987\t0.8197\t0.8407989131363496\t1970-01-16T02:45:17.567Z\t1969-12-31T23:59:55.475362Z\tGTNLE\taaa\tfalse\taaa\t0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\t00000000 49 96 cf 2b b3 71 a7\n" +
                        "107\tD\t-1263\t408527\tNaN\t0.7653\t0.1511578096923386\t1972-09-19T00:27:27.667Z\t1969-12-31T23:59:58.883048Z\tVFGP\tbbbbbb\ttrue\tbbbbbb\t0x13e2c5f1f106cfe2181d04a53e6dc020180c9457eb5fa83d71c1d71dab71b171\t00000000 64 43 84 55 a0 dd 44 11 e2\n" +
                        "21\tN\t22350\t944298\t14332\t0.4349\t0.11296257318851766\t1980-07-28T12:55:01.616Z\t1969-12-31T23:59:58.440889Z\tGRMDG\tc\tfalse\tbbbbbb\t0x32ee0b100004f6c45ec6d73428fb1c01b680be3ee552450eef8b1c47f7e7f9ec\t00000000 13 8f bb 2a 4b af 8f 89\n" +
                        "71\tE\t24975\t910466\t-49474\t0.3729\t0.7842455970681089\t1977-01-14T05:30:32.213Z\t1969-12-31T23:59:53.588953Z\t\tc\ttrue\tc\t0x94812c09d22d975f5220a353eab6ca9454ed8c2bc62a7a3bb1f4f1789b74c505\t00000000 dc f8 43 b2\n" +
                        "123\tJ\t-4254\t-22141\tNaN\t0.1331\tNaN\t\t1969-12-31T23:59:54.329404Z\t\taaa\ttrue\tc\t0x64a4822086748dc4b096d89b65baebefc4a411134408f49dae7e2758c6eca43b\t00000000 cf fb 9d\n" +
                        "81\tE\t8596\t395835\t79443\t0.5598\t0.5900836401674938\t1978-04-29T01:58:41.274Z\t1969-12-31T23:59:57.719809Z\tHHMGZ\taaa\ttrue\tbbbbbb\t0x6e87bac2e97465292db016df5ec483152f30671fd02ecc3517a745338e084738\t00000000 c3 2f ed b0 ba 08\n" +
                        "96\tL\t1774\t804030\tNaN\t0.5252\t0.8977957942059742\t1975-03-15T08:49:25.586Z\t1969-12-31T23:59:56.081422Z\tVZNCL\taaa\ttrue\taaa\t0x12ed35e1f90f6b8d5ccc22f4b59eaa47d256526470b1ff197ec1b56d70fe6ce9\t00000000 bc fe b9 52 dd 4d f3 f9 76\n" +
                        "54\tC\t2731\t913355\t52597\tNaN\t0.743599174001969\t1970-12-12T18:41:43.397Z\t1969-12-31T23:59:55.075088Z\tHWQXY\t\ttrue\tbbbbbb\t0x8f36ac9372219b207d34861c589dab59aebf198afad2484dcf111b837d7ecc13\t00000000 a1 00 f8 42\n" +
                        "119\tR\t32259\tNaN\tNaN\t0.7338\t0.0016532800623808575\t1982-02-19T13:22:55.280Z\t1969-12-31T23:59:54.024711Z\t\taaa\ttrue\taaa\t0x700aee7f321a695da0cd12e6d39f169a9f8806288f4b53ad6ff303216fe26ea0\t00000000 72 d7 97 cb f6\n" +
                        "92\tE\t-21435\t978972\t28032\t0.5381\t0.7370823954391381\t1979-04-11T04:18:46.284Z\t\tVOCUG\t\tfalse\tc\t0xe6a2713114b420cb73b256fc9f7245e364b4cb5876665e9b5a11e0d21f2a16a3\t00000000 8a b0 35\n" +
                        "120\tS\t-32705\t455919\tNaN\t0.3186\t0.06001827721556019\t1974-06-05T14:26:31.420Z\t1969-12-31T23:59:57.649378Z\tHNOJI\t\tfalse\taaa\t0x71be94dd9da614c69509495193ef4c819645939abaa44b209164f00487d05f10\t\n" +
                        "60\tW\t-27643\tNaN\t-15471\t0.0743\t0.38881940598288367\t1973-07-23T19:51:25.899Z\t1969-12-31T23:59:58.690432Z\tCEBYW\taaa\ttrue\tc\t0x9efeae7dc4ddb201971f4134354bfdc88055ebf2c14f61705f3f358f3f41ca27\t00000000 c9 3a 5b 7e 0e 98 0a 8a 0b 1e\n" +
                        "24\tD\t14242\t64863\t48044\t0.4755\t0.7617663592833062\t1981-06-04T06:48:30.566Z\t1969-12-31T23:59:58.908420Z\t\tc\ttrue\tbbbbbb\t0x8f5549cf32a4976c5dc373cbc950a49041457ebc5a02a2b542cbd49414e022a0\t\n" +
                        "45\tJ\t26761\t319767\tNaN\t0.0966\t0.5675831821917149\t1978-11-13T05:09:01.878Z\t1970-01-01T00:00:00.569800Z\tRGFI\tbbbbbb\ttrue\tbbbbbb\t0xbba8ab48c3defc985c174a8c1da880da3e292b77c7836d67a62eec8815a4ecb2\t00000000 bf 4f ea 5f\n" +
                        "28\tQ\t-11530\t640914\t-54143\t0.3229\t0.6852762111021103\t\t1969-12-31T23:59:52.759890Z\tJEUK\taaa\ttrue\t\t0x93e0ee36dbf0e422654cc358385f061661bd22a0228f78a299fe06bcdcb3a9e7\t00000000 1c 9c 1d 5c\n" +
                        "87\tL\t-2003\t842028\t12726\t0.7280\tNaN\t1982-02-21T09:56:08.687Z\t1969-12-31T23:59:52.977649Z\tFDYP\taaa\tfalse\t\t0x6fc129c805c2f5075fd086d7ede63e98492997ce455a2c2962e616e3a640fbca\t00000000 5b d6 cf 09 69 01\n" +
                        "115\tN\t312\t335480\t4602\t0.1476\t0.23673087740006105\t\t1969-12-31T23:59:57.132665Z\tRTPIQ\taaa\tfalse\t\t0xb139c160cb54bc065e565229e0e964b83e2dbd2d9d8d4081a00e467f6e96e622\t00000000 af 38 71 1f e1 e4 91 7d e9\n" +
                        "27\tJ\t-15254\t978974\t-36356\t0.7911\t0.7128505998532723\t1976-03-14T08:19:05.571Z\t1969-12-31T23:59:56.726487Z\tPZNYV\t\ttrue\t\t0x39af691594d0654567af4ec050eea188b8074532ac9f3c87c68ce6f3720e2b62\t00000000 20 13\n"
        );
    }

    private void assertYear() throws SqlException {
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_year where m = null",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "92\tF\t28028\tNaN\tNaN\t0.2554\t0.18371611611756045\t1981-02-16T02:44:16.585Z\t1969-12-31T23:59:59.627107Z\t\tc\ttrue\t\t0x9de662c110ea4073e8728187e8352d5fc9d79f015b315fe985b29ab573d60b7d\t\t1982-09-04T15:10:00.000000Z\n" +
                        "27\tZ\t16702\t241981\t-97518\tNaN\t0.11513682045365181\t1978-06-15T03:10:19.987Z\t1969-12-31T23:59:56.486114Z\tPWGBN\tc\tfalse\t\t0x57f7ca4006c25f277854b37a145d2d6eb1a2709c5a91109c72459255ef20260d\t\t2001-09-09T01:50:00.000000Z\n" +
                        "115\tV\t1407\t828287\t-19261\t0.5617\t0.6751819291749697\t1976-02-13T23:55:05.568Z\t1970-01-01T00:00:00.341973Z\tPOVR\taaa\tfalse\t\t0xa52d6aeb2bb452b5d536e81d23cc0ccccb3ab47e72e6c8d19f42388a80bda41e\t\t2033-05-18T03:36:40.000000Z\n" +
                        "51\tS\t51\tNaN\t3911\t0.5498\t0.21143168923450806\t1971-07-24T11:46:39.611Z\t1969-12-31T23:59:59.584289Z\tXORR\tc\tfalse\t\t0x793dc8ec3035a4d52f345b2d97c3d52c6cc96e4115ea53bb73f55836b900abc8\t00000000 ac d5 a5 c6 3a 4e 29 ca c3 65\t2090-06-01T11:36:40.000000Z\n" +
                        "83\tL\t-32289\t127321\t40837\t0.1335\t0.515824820198022\t\t1969-12-31T23:59:53.582959Z\tKFMO\taaa\tfalse\t\t0x8131875cd498c4b888762e985137f4e843b8167edcd59cf345c105202f875495\t\t2109-06-06T22:16:40.000000Z\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select distinct k from t_year",
                sink,
                "k\n" +
                        "bbbbbb\n" +
                        "aaa\n" +
                        "c\n" +
                        "\n"
        );

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "t_year",
                sink,
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tts\n" +
                        "112\tY\t18252\t527691\t-61919\t0.6010\t0.11234979325337158\t1979-08-14T20:45:18.393Z\t1969-12-31T23:59:59.440365Z\tLVKR\tbbbbbb\tfalse\taaa\t0x8d70ff24aa6bc1ef633fa3240281dbf15c141df8f68b896d654ad128c2cc260e\t\t1970-01-01T00:03:20.000000Z\n" +
                        "89\tM\t10579\t312660\t-34472\t0.4381\t0.9857252839045136\t1976-10-12T22:39:24.028Z\t1969-12-31T23:59:55.269914Z\tXWLUK\taaa\tfalse\tc\t0xead0f34ae4973cadeecfae4b49c48a80ebc4fdd2342465cac8cc5c88521b09ee\t\t1976-05-03T19:36:40.000000Z\n" +
                        "92\tF\t28028\tNaN\tNaN\t0.2554\t0.18371611611756045\t1981-02-16T02:44:16.585Z\t1969-12-31T23:59:59.627107Z\t\tc\ttrue\t\t0x9de662c110ea4073e8728187e8352d5fc9d79f015b315fe985b29ab573d60b7d\t\t1982-09-04T15:10:00.000000Z\n" +
                        "109\tS\t-3798\t880448\t40626\t0.3578\t0.5350104350364769\t1978-07-25T16:54:09.798Z\t1969-12-31T23:59:57.018965Z\tTXDEH\taaa\tfalse\taaa\t0x652829c06d3a3a999da1a84950a96f02d2f84050fd9b3cd5949d89fa0fa954fa\t\t1989-01-05T10:43:20.000000Z\n" +
                        "28\tJ\t-2192\t474110\t-90147\t0.3452\t0.5690127219513472\t1975-09-08T20:10:23.592Z\t1970-01-01T00:00:00.115667Z\tOVGNC\tbbbbbb\tfalse\tbbbbbb\t0xa65e9c91b87026ec86ad3659d535a73857e804d0b771f6f47998801853f0e22d\t\t1995-05-09T06:16:40.000000Z\n" +
                        "27\tZ\t16702\t241981\t-97518\tNaN\t0.11513682045365181\t1978-06-15T03:10:19.987Z\t1969-12-31T23:59:56.486114Z\tPWGBN\tc\tfalse\t\t0x57f7ca4006c25f277854b37a145d2d6eb1a2709c5a91109c72459255ef20260d\t\t2001-09-09T01:50:00.000000Z\n" +
                        "41\tT\t18688\t190380\tNaN\t0.4748\t0.3646620411241238\t1973-06-06T04:07:03.662Z\t1969-12-31T23:59:52.414292Z\tTMPCO\taaa\ttrue\tbbbbbb\t0x5f499e93050b5c576632a8d2d02c3bc99612611e7cf872ae99763daee67eb715\t00000000 2a 12 54 e8 8d d9 0d\t2008-01-10T21:23:20.000000Z\n" +
                        "120\tQ\t-13641\t798379\t66973\t0.0698\tNaN\t\t1969-12-31T23:59:58.766666Z\tMNSZV\tbbbbbb\ttrue\tc\t0xa3100ad5b8f483debaeabfd165574dccd4115bfdaa003943d754367b64aab4b8\t\t2014-05-13T16:56:40.000000Z\n" +
                        "120\tN\t-25478\t906251\t52444\t0.2174\t0.7214359500445924\t1972-01-14T15:50:49.976Z\t1969-12-31T23:59:53.500256Z\tVRVUO\tbbbbbb\ttrue\tbbbbbb\t0x225a9524d57602fa337e25a5e8454adc232847d409dbb3c6770928b479ba1539\t\t2020-09-13T12:30:00.000000Z\n" +
                        "97\tZ\t-22633\t584739\t80188\t0.2819\t0.531091271619974\t1972-11-05T01:47:18.571Z\t1969-12-31T23:59:57.518586Z\tHOKX\taaa\tfalse\tc\t0x7cc1de102e7a4ab6c760cfffc10d6b96c5d5a2ea72272b52b96a946df47c9238\t\t2027-01-15T08:03:20.000000Z\n" +
                        "115\tV\t1407\t828287\t-19261\t0.5617\t0.6751819291749697\t1976-02-13T23:55:05.568Z\t1970-01-01T00:00:00.341973Z\tPOVR\taaa\tfalse\t\t0xa52d6aeb2bb452b5d536e81d23cc0ccccb3ab47e72e6c8d19f42388a80bda41e\t\t2033-05-18T03:36:40.000000Z\n" +
                        "108\tR\t-10001\t603147\tNaN\t0.0983\tNaN\t1982-01-31T01:47:50.703Z\t1969-12-31T23:59:52.647352Z\tEHIOF\tbbbbbb\ttrue\taaa\t0x6f3213396842e60844fabb05f62eb19b39b02ad7b1317342735884fc062b28e6\t00000000 54 b9 30\t2039-09-18T23:10:00.000000Z\n" +
                        "101\tF\t13128\t689961\t37591\t0.2748\t0.9418087554045725\t\t1969-12-31T23:59:53.389231Z\t\taaa\ttrue\taaa\t0xf1a20ee3ef468ebc77fda24495e8ae8372eab3da74bf2433af04c21bc27d99d5\t\t2046-01-19T18:43:20.000000Z\n" +
                        "54\tC\t-6357\t275620\t-13434\t0.1873\t0.6096055633557564\t1974-03-05T02:17:06.113Z\t1969-12-31T23:59:54.708484Z\tXVFG\tbbbbbb\ttrue\tbbbbbb\t0xb155cc19f6c018ac9172b15e727f85ad9d81624b36e54a69deeeb0a6400a52b6\t00000000 8f 48 63 38 66 66 8a 2c f6\t2052-05-22T14:16:40.000000Z\n" +
                        "49\tK\t4850\tNaN\tNaN\t0.5489\t0.5606786012693848\t1973-12-30T03:59:24.202Z\t1969-12-31T23:59:57.366085Z\tDVCL\tbbbbbb\ttrue\tbbbbbb\t0x45dbdddf1cd99c527b3011c3aab8d452c66aef1d2d40aec3cbdaab013e4fed0e\t00000000 ca 99\t2058-09-23T09:50:00.000000Z\n" +
                        "91\tJ\t25494\t694565\t-10172\t0.3579\t0.7057107661934603\t1979-08-28T13:02:30.203Z\t1969-12-31T23:59:57.785346Z\tQNKCY\t\ttrue\taaa\t0xa5b7f979670f70bc99a1c14975e7aed9e2d309dfffa3bfbaad8c21f0583d522d\t00000000 0b 33 a3 d0 07 24 a1\t2065-01-24T05:23:20.000000Z\n" +
                        "20\tU\t7348\t676443\t-19665\tNaN\t0.4671328238025483\t1971-09-27T16:40:48.318Z\t1969-12-31T23:59:59.395697Z\tMBWS\tc\tfalse\tbbbbbb\t0x28f473a32e838adc36acb12866a5ddb5972ff0cf1298cf2bbfccf2c193715cb1\t00000000 d2 45 8b 39 ff\t2071-05-28T00:56:40.000000Z\n" +
                        "118\tX\t32736\tNaN\tNaN\t0.1477\t0.007535382752122954\t1974-05-12T18:29:30.460Z\t1969-12-31T23:59:53.264943Z\tPSWJ\tbbbbbb\tfalse\tc\t0x35c857e5b83d443313d82d7ebbe3e4cd18e73a1d6f040cf525f019bdde013aa2\t\t2077-09-27T20:30:00.000000Z\n" +
                        "98\tZ\t-8242\t740823\t97395\t0.6029\t0.7146891119224845\t\t1969-12-31T23:59:59.549076Z\tCUGWX\t\ttrue\taaa\t0x4b4eb5f8f81b00661f1aa9638e21fd380d8fba85878633e95e6bbb938ae6feef\t\t2084-01-29T16:03:20.000000Z\n" +
                        "51\tS\t51\tNaN\t3911\t0.5498\t0.21143168923450806\t1971-07-24T11:46:39.611Z\t1969-12-31T23:59:59.584289Z\tXORR\tc\tfalse\t\t0x793dc8ec3035a4d52f345b2d97c3d52c6cc96e4115ea53bb73f55836b900abc8\t00000000 ac d5 a5 c6 3a 4e 29 ca c3 65\t2090-06-01T11:36:40.000000Z\n" +
                        "109\tP\t-12455\t263934\t49960\t0.6793\t0.09831693674866282\t1977-08-08T19:44:03.856Z\t1969-12-31T23:59:55.049605Z\tHQBJP\tc\tfalse\taaa\t0xbe15104d1d36d615cac36ab298393e52b06836c8abd67a44787ce11d6fc88eab\t00000000 37 58 2c 0d b0 d0 9c 57 02 75\t2096-10-02T07:10:00.000000Z\n" +
                        "73\tB\t-1271\t-47644\t4999\t0.8584\t0.12392055368261845\t1975-06-03T19:26:19.012Z\t1969-12-31T23:59:55.604499Z\t\tc\ttrue\taaa\t0x4f669e76b0311ac3438ec9cc282caa7043a05a3edd41f45aa59f873d1c729128\t00000000 34 01 0e 4d 2b 00 fa 34\t2103-02-04T02:43:20.000000Z\n" +
                        "83\tL\t-32289\t127321\t40837\t0.1335\t0.515824820198022\t\t1969-12-31T23:59:53.582959Z\tKFMO\taaa\tfalse\t\t0x8131875cd498c4b888762e985137f4e843b8167edcd59cf345c105202f875495\t\t2109-06-06T22:16:40.000000Z\n" +
                        "51\tS\t-28311\tNaN\t-72973\t0.5957\t0.20897460269739654\t1973-03-28T21:58:08.545Z\t1969-12-31T23:59:54.332988Z\t\tc\tfalse\taaa\t0x50113ffcc219fb1a9bc4f6389de1764097e7bcd897ae8a54aa2883a41581608f\t00000000 83 94 b5\t2115-10-08T17:50:00.000000Z\n" +
                        "49\tN\t-11147\t392567\t-9830\t0.5248\t0.1095692511246914\t\t1969-12-31T23:59:56.849475Z\tIFBE\tc\tfalse\taaa\t0x36055358bd232c9d775e2e80754e5fcda2353931c7033ad5c38c294e9227895a\t\t2122-02-08T13:23:20.000000Z\n" +
                        "57\tI\t-22903\t874980\t-28069\tNaN\t0.016793228004843286\t1975-09-29T05:10:33.275Z\t1969-12-31T23:59:55.690794Z\tBZSM\t\ttrue\tc\t0xa2c84382c65eb07087cf6cb291b2c3e7a9ffe8560d2cec518dea50b88b87fe43\t00000000 b9 c4 18 2b aa 3d\t2128-06-11T08:56:40.000000Z\n" +
                        "127\tW\t-16809\t288758\t-22272\t0.0535\t0.5855510665931698\t\t1969-12-31T23:59:52.689490Z\t\taaa\tfalse\tc\t0x918ae2d78481070577c7d4c3a758a5ea3dd771714ac964ab4b350afc9b599b28\t\t2134-10-13T04:30:00.000000Z\n" +
                        "20\tM\t-7043\t251501\t-85499\t0.9403\t0.9135840078861264\t1977-05-12T19:20:06.113Z\t1969-12-31T23:59:54.045277Z\tHOXL\tbbbbbb\tfalse\tbbbbbb\t0xc3b0de059fff72dbd7b99af08ac0d1cddb2990725a3338e377155edb531cb644\t\t2141-02-13T00:03:20.000000Z\n" +
                        "26\tG\t-24830\t-56840\t-32956\t0.8282\t0.017280895313585898\t1982-07-16T03:52:53.454Z\t1969-12-31T23:59:54.115165Z\tJEJH\taaa\tfalse\tbbbbbb\t0x16f70de9c6af11071d35d9faec5d18fd1cf3bbbc825b72a92ecb8ff0286bf649\t00000000 c2 62 f8 53 7d 05 65\t2147-06-16T19:36:40.000000Z\n" +
                        "127\tY\t19592\t224361\t37963\t0.6930\t0.006817672510656014\t1975-11-29T09:47:45.706Z\t1969-12-31T23:59:56.186242Z\t\t\ttrue\taaa\t0x88926dd483caaf4031096402997f21c833b142e887fa119e380dc9b54493ff70\t00000000 23 c3 9d 75 26 f2 0d b5 7a 3f\t2153-10-17T15:10:00.000000Z\n"
        );
    }

    @NotNull
    private String commonColumns() {
        return " rnd_byte() a," +
                " rnd_char() b," +
                " rnd_short() c," +
                " rnd_int(-77888, 999001, 2) d," + // ensure we have nulls
                " rnd_long(-100000, 100000, 2) e," + // ensure we have nulls
                " rnd_float(2) f," + // ensure we have nulls
                " rnd_double(2) g," + // ensure we have nulls
                " rnd_date(199999999, 399999999999, 2) h," + // ensure we have nulls
                " cast(rnd_long(-7999999, 800000, 10) as timestamp)  i," + // ensure we have nulls
                " rnd_str(4,5,2) j," +
                " rnd_symbol('aaa','bbbbbb', 'c', null) k," +
                " rnd_boolean() l," +
                " rnd_symbol('aaa','bbbbbb', 'c', null) m," +
                " rnd_long256() n," +
                " rnd_bin(2,10, 2) o";
    }

    private void doMigration(String dataZip, boolean freeTableId) throws IOException, SqlException {
        if (freeTableId) {
            engine.freeTableId();
        }
        replaceDbContent(dataZip);
        EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, true);
        assertData();
    }

    private void generateMigrationTables() throws SqlException {
        compiler.compile(
                "create table t_none_nts as (" +
                        "select" +
                        commonColumns() +
                        " from long_sequence(30)," +
                        "), index(m)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table t_none as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(100, 200000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by NONE",
                sqlExecutionContext
        );

        compiler.compile(
                "create table t_day as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 2000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table t_month as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 20000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by MONTH",
                sqlExecutionContext
        );

        compiler.compile(
                "create table t_year as (" +
                        "select" +
                        commonColumns() +
                        ", timestamp_sequence(200000000L, 200000000000000L) ts" +
                        " from long_sequence(30)," +
                        "), index(m) timestamp(ts) partition by YEAR",
                sqlExecutionContext
        );
    }
}
