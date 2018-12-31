/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
 ******************************************************************************/

package com.questdb.tuck;

import com.questdb.std.Numbers;
import com.questdb.std.NumericException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

public class ServerConfiguration {
    private final SslConfig sslConfig = new SslConfig();
    private String httpIP = "0.0.0.0";
    private int httpPort = 9000;
    private int httpBufReqHeader = 64 * 1024;
    private int httpBufReqContent = 4 * 1024 * 1024;
    private int httpBufReqMultipart = 1024;
    private int httpBufRespHeader = 1024;
    private int httpBufRespContent = 1024 * 1024;
    private int httpThreads = 2;
    private int httpTimeout = 10000000;
    private int httpMaxConnections = 128;
    private int journalPoolSize = 128;
    private int httpQueueDepth = 1024;
    private int httpSoRcvSmall = 8 * 1024;
    private int httpSoRcvLarge = 4 * 1024 * 1024;
    private int httpSoRetries = 1000000;
    private boolean httpAbortBrokenUploads = true;
    private String httpIndexFile = "index.html";
    private int httpImportMaxJsonStringLen = 1024 * 64;
    private int httpImportInitialTextBuf = 4 * 1024;
    private int httpImportMaxTextBuf = 4 * 1024 * 1024;
    private int httpImportSampleSize = 100;
    private int dbAsOfDataPage = 4 * 1024 * 1024;
    private int dbAsOfIndexPage = 1024 * 1024;
    private int dbAsOfRowPage = 1024 * 1024;
    private int dbSortKeyPage = 1024 * 1024;
    private int dbSortDataPage = 4 * 1024 * 1024;
    private int dbAggregatePage = 4 * 1024 * 1024;
    private int dbHashKeyPage = 4 * 1024 * 1024;
    private int dbHashDataPage = 8 * 1024 * 1024;
    private int dbHashRowPage = 1024 * 1024;
    private int dbSysViewPage = 64 * 1024;
    private int dbSysMetaSize = 64 * 1024;
    private int dbSysMaxMetaSize = 8 * 1024 * 1024;
    private int dbCyclesBeforeCancel = 1024 * 1024;
    private int dbAnalyticFuncPage = 2 * 1024 * 1024;
    private int dbAnalyticWindowPage = 4 * 1024 * 1024;
    private int dbFnVarianceMeans = 1024 * 1024;
    private int dbFnVarianceData = 64 * 1024 * 1024;
    private long dbPoolIdleTimeout = 60000;
    private long dbPoolIdleCheckInterval = 60000;
    private int dbReaderPoolSize = 2;
    private File dbPath = new File("db");
    private File mimeTypes = new File("conf/mime.types");
    private File httpPublic = new File("public");
    private File accessLog = new File("log/access.log");
    private File errorLog = new File("log/error.log");

    public ServerConfiguration() {
    }

    public ServerConfiguration(File conf) throws Exception {

        final Properties props = new Properties();
        final File root = conf.getParentFile().getParentFile();

        try (FileInputStream fis = new FileInputStream(conf)) {
            props.load(fis);
        }

        int n;
        String s;

        if ((s = props.getProperty("http.ip")) != null) {
            this.httpIP = s;
        }

        if ((n = parseInt(props, "http.port")) > -1) {
            this.httpPort = n;
        }

        if ((n = parseInt(props, "http.threads")) > -1) {
            this.httpThreads = n;
        }

        if ((n = parseInt(props, "http.timeout")) > -1) {
            this.httpTimeout = n;
        }

        if ((n = parseSize(props, "http.buf.req.header")) > -1) {
            this.httpBufReqHeader = n;
        }

        if ((n = parseInt(props, "http.max.connections")) > -1) {
            this.httpMaxConnections = n;
        }

        if ((n = parseInt(props, "http.queue.depth")) > -1) {
            this.httpQueueDepth = Numbers.ceilPow2(n);
        }

        if ((n = parseSize(props, "http.so.rcv.small")) > -1) {
            this.httpSoRcvSmall = n;
        }

        if ((n = parseSize(props, "http.so.rcv.large")) > -1) {
            this.httpSoRcvLarge = n;
        }

        if ((n = parseInt(props, "http.so.retries")) > -1) {
            this.httpSoRetries = n;
        }

        if ((s = props.getProperty("http.abort.broken.uploads")) != null) {
            switch (s) {
                case "true":
                    httpAbortBrokenUploads = true;
                    break;
                case "false":
                    httpAbortBrokenUploads = false;
                    break;
                default:
                    throw new IllegalArgumentException("http.abort.broken.uploads");
            }
        }

        if ((n = parseSize(props, "http.buf.req.content")) > -1) {
            this.httpBufReqContent = n;
        }

        if ((n = parseSize(props, "http.buf.req.multipart")) > -1) {
            this.httpBufReqMultipart = n;
        }

        if ((n = parseSize(props, "http.buf.resp.header")) > -1) {
            this.httpBufRespHeader = n;
        }

        if ((n = parseSize(props, "http.buf.resp.content")) > -1) {
            this.httpBufRespContent = n;
        }

        if ((s = props.getProperty("http.index.file")) != null) {
            this.httpIndexFile = s;
        }

        if ((n = parseInt(props, "http.import.max.json.string")) > -1) {
            this.httpImportMaxJsonStringLen = n;
        }

        if ((n = parseInt(props, "http.import.initial.text.buf")) > -1) {
            this.httpImportInitialTextBuf = n;
        }

        if ((n = parseInt(props, "http.import.max.text.buf")) > -1) {
            this.httpImportMaxTextBuf = n;
        }

        if ((n = parseInt(props, "http.import.sample.size")) > -1) {
            this.httpImportSampleSize = n;
        }

        if ((s = props.getProperty("db.path")) != null) {
            this.dbPath = mkdirs(normalize(root, new File(s)));
        } else {
            this.dbPath = mkdirs(normalize(root, this.dbPath));
        }

        if ((n = parseSize(props, "db.max.pool.size")) > -1) {
            this.journalPoolSize = n;
        } else {
            this.journalPoolSize = httpMaxConnections;
        }

        if ((n = parseSize(props, "db.asof.datapage")) > -1) {
            this.dbAsOfDataPage = n;
        }

        if ((n = parseSize(props, "db.asof.indexpage")) > -1) {
            this.dbAsOfIndexPage = n;
        }

        if ((n = parseSize(props, "db.asof.rowpage")) > -1) {
            this.dbAsOfRowPage = n;
        }

        if ((n = parseSize(props, "db.sort.keypage")) > -1) {
            this.dbSortKeyPage = n;
        }

        if ((n = parseSize(props, "db.sort.datapage")) > -1) {
            this.dbSortDataPage = n;
        }

        if ((n = parseSize(props, "db.aggregate.page")) > -1) {
            this.dbAggregatePage = n;
        }

        if ((n = parseSize(props, "db.hash.keypage")) > -1) {
            this.dbHashKeyPage = n;
        }

        if ((n = parseSize(props, "db.hash.datapage")) > -1) {
            this.dbHashDataPage = n;
        }

        if ((n = parseSize(props, "db.hash.rowpage")) > -1) {
            this.dbHashRowPage = n;
        }

        if ((n = parseSize(props, "db.sys.viewpage")) > -1) {
            this.dbSysViewPage = n;
        }

        if ((n = parseSize(props, "db.sys.metasize")) > -1) {
            this.dbSysMetaSize = n;
        }

        if ((n = parseSize(props, "db.sys.maxmetasize")) > -1) {
            this.dbSysMaxMetaSize = n;
        }

        if ((n = parseInt(props, "db.cycles.before.cancel")) > -1) {
            this.dbCyclesBeforeCancel = Numbers.ceilPow2(n);
        }

        if ((n = parseSize(props, "db.analytic.func.page")) > -1) {
            this.dbAnalyticFuncPage = n;
        }

        if ((n = parseSize(props, "db.analytic.window.page")) > -1) {
            this.dbAnalyticWindowPage = n;
        }

        if ((n = parseSize(props, "db.fn.variance.means")) > -1) {
            this.dbFnVarianceMeans = n;
        }

        if ((n = parseSize(props, "db.fn.variance.data")) > -1) {
            this.dbFnVarianceData = n;
        }

        if ((s = props.getProperty("mime.types")) != null) {
            this.mimeTypes = normalize(root, new File(s));
        } else {
            this.mimeTypes = normalize(root, mimeTypes);
        }

        if ((s = props.getProperty("http.public")) != null) {
            this.httpPublic = mkdirs(normalize(root, new File(s)));
        } else {
            this.httpPublic = mkdirs(normalize(root, this.httpPublic));
        }

        if ((s = props.getProperty("http.log.access")) != null) {
            this.accessLog = normalize(root, new File(s));
        } else {
            this.accessLog = normalize(root, this.accessLog);
        }
        mkdirs(this.accessLog.getParentFile());

        if ((s = props.getProperty("http.log.error")) != null) {
            this.errorLog = normalize(root, new File(s));
        } else {
            this.errorLog = normalize(root, this.errorLog);
        }
        mkdirs(this.accessLog.getParentFile());

        if (this.httpMaxConnections > this.httpQueueDepth) {
            throw new IllegalArgumentException("http.max.connections must be less than http.queue.depth");
        }

        // SSL section
        sslConfig.setSecure("true".equals(props.getProperty("http.ssl.enabled")));
        if (sslConfig.isSecure()) {
            if ((s = props.getProperty("http.ssl.keystore.location")) == null) {
                throw new IllegalArgumentException("http.ssl.keystore.location is undefined");
            }

            File keystore = normalize(root, new File(s));

            if (!keystore.exists()) {
                throw new IllegalArgumentException("http.ssl.keystore.location does not exist");
            }

            if (!keystore.isFile()) {
                throw new IllegalArgumentException("http.ssl.keystore.location is not a file");
            }

            try (InputStream is = new FileInputStream(keystore)) {
                s = props.getProperty("http.ssl.keystore.password");
                sslConfig.setKeyStore(is, s == null ? "" : s);
            }
            sslConfig.setRequireClientAuth("true".equals(props.getProperty("http.ssl.auth")));
        }

        long l;

        if ((l = parseLong(props, "db.pool.idle.timeout")) > -1) {
            this.dbPoolIdleTimeout = l;
        }

        if ((l = parseLong(props, "db.pool.idle.check.interval")) > -1) {
            this.dbPoolIdleCheckInterval = l;
        }

        if ((n = parseInt(props, "db.reader.pool.size")) > -1) {
            this.dbReaderPoolSize = n;
        }
    }

    public File getAccessLog() {
        return accessLog;
    }

    public int getDbAggregatePage() {
        return dbAggregatePage;
    }

    public int getDbAnalyticFuncPage() {
        return dbAnalyticFuncPage;
    }

    public int getDbAnalyticWindowPage() {
        return dbAnalyticWindowPage;
    }

    public int getDbAsOfDataPage() {
        return dbAsOfDataPage;
    }

    public int getDbAsOfIndexPage() {
        return dbAsOfIndexPage;
    }

    public int getDbAsOfRowPage() {
        return dbAsOfRowPage;
    }

    public int getDbCyclesBeforeCancel() {
        return dbCyclesBeforeCancel;
    }

    public int getDbFnVarianceData() {
        return dbFnVarianceData;
    }

    public int getDbFnVarianceMeans() {
        return dbFnVarianceMeans;
    }

    public int getDbHashDataPage() {
        return dbHashDataPage;
    }

    public int getDbHashKeyPage() {
        return dbHashKeyPage;
    }

    public int getDbHashRowPage() {
        return dbHashRowPage;
    }

    public File getDbPath() {
        return dbPath;
    }

    public long getDbPoolIdleCheckInterval() {
        return dbPoolIdleCheckInterval;
    }

    public long getDbPoolIdleTimeout() {
        return dbPoolIdleTimeout;
    }

    public int getDbReaderPoolSize() {
        return dbReaderPoolSize;
    }

    public int getDbSortDataPage() {
        return dbSortDataPage;
    }

    public int getDbSortKeyPage() {
        return dbSortKeyPage;
    }

    public int getDbSysMaxMetaSize() {
        return dbSysMaxMetaSize;
    }

    public int getDbSysMetaSize() {
        return dbSysMetaSize;
    }

    public int getDbSysViewPage() {
        return dbSysViewPage;
    }

    public File getErrorLog() {
        return errorLog;
    }

    public int getHttpBufReqContent() {
        return httpBufReqContent;
    }

    public int getHttpBufReqHeader() {
        return httpBufReqHeader;
    }

    public int getHttpBufReqMultipart() {
        return httpBufReqMultipart;
    }

    public int getHttpBufRespContent() {
        return httpBufRespContent;
    }

    public void setHttpBufRespContent(int httpBufRespContent) {
        this.httpBufRespContent = httpBufRespContent;
    }

    public int getHttpBufRespHeader() {
        return httpBufRespHeader;
    }

    public String getHttpIP() {
        return httpIP;
    }

    public int getHttpImportInitialTextBuf() {
        return httpImportInitialTextBuf;
    }

    public int getHttpImportMaxJsonStringLen() {
        return httpImportMaxJsonStringLen;
    }

    public int getHttpImportMaxTextBuf() {
        return httpImportMaxTextBuf;
    }

    public int getHttpImportSampleSize() {
        return httpImportSampleSize;
    }

    public String getHttpIndexFile() {
        return httpIndexFile;
    }

    public int getHttpMaxConnections() {
        return httpMaxConnections;
    }

    public void setHttpMaxConnections(int httpMaxConnections) {
        this.httpMaxConnections = httpMaxConnections;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public File getHttpPublic() {
        return httpPublic;
    }

    public int getHttpQueueDepth() {
        return httpQueueDepth;
    }

    public int getHttpSoRcvLarge() {
        return httpSoRcvLarge;
    }

    public int getHttpSoRcvSmall() {
        return httpSoRcvSmall;
    }

    public int getHttpSoRetries() {
        return httpSoRetries;
    }

    public void setHttpSoRetries(int httpSoRetries) {
        this.httpSoRetries = httpSoRetries;
    }

    public int getHttpThreads() {
        return httpThreads;
    }

    public void setHttpThreads(int httpThreads) {
        this.httpThreads = httpThreads;
    }

    public int getHttpTimeout() {
        return httpTimeout;
    }

    public void setHttpTimeout(int httpTimeout) {
        this.httpTimeout = httpTimeout;
    }

    public int getJournalPoolSize() {
        return journalPoolSize;
    }

    public File getMimeTypes() {
        return mimeTypes;
    }

    public SslConfig getSslConfig() {
        return sslConfig;
    }

    public boolean isHttpAbortBrokenUploads() {
        return httpAbortBrokenUploads;
    }

    @Override
    public String toString() {
        return "ServerConfiguration{" +
                "sslConfig=" + sslConfig +
                ", httpIP='" + httpIP + '\'' +
                ", httpPort=" + httpPort +
                ", httpBufReqHeader=" + httpBufReqHeader +
                ", httpBufReqContent=" + httpBufReqContent +
                ", httpBufReqMultipart=" + httpBufReqMultipart +
                ", httpBufRespHeader=" + httpBufRespHeader +
                ", httpBufRespContent=" + httpBufRespContent +
                ", httpThreads=" + httpThreads +
                ", httpTimeout=" + httpTimeout +
                ", httpMaxConnections=" + httpMaxConnections +
                ", journalPoolSize=" + journalPoolSize +
                ", httpQueueDepth=" + httpQueueDepth +
                ", httpSoRcvSmall=" + httpSoRcvSmall +
                ", httpSoRcvLarge=" + httpSoRcvLarge +
                ", httpSoRetries=" + httpSoRetries +
                ", dbAsOfDataPage=" + dbAsOfDataPage +
                ", dbAsOfIndexPage=" + dbAsOfIndexPage +
                ", dbAsOfRowPage=" + dbAsOfRowPage +
                ", dbSortKeyPage=" + dbSortKeyPage +
                ", dbSortDataPage=" + dbSortDataPage +
                ", dbAggregatePage=" + dbAggregatePage +
                ", dbPath=" + dbPath +
                ", mimeTypes=" + mimeTypes +
                ", httpPublic=" + httpPublic +
                ", accessLog=" + accessLog +
                ", errorLog=" + errorLog +
                '}';
    }

    private File mkdirs(File dir) throws IOException {
        if (!dir.exists()) {
            Files.createDirectories(dir.toPath());
        }
        return dir;
    }

    private File normalize(File root, File file) {
        if (file.isAbsolute()) {
            return file;
        }
        return new File(root, file.getPath());
    }

    private int parseInt(Properties props, String name) {
        String val = props.getProperty(name);
        if (val != null) {
            try {
                return Numbers.parseInt(val);
            } catch (NumericException e) {
                System.out.println(name + ": invalid value");
            }
        }
        return -1;
    }

    private long parseLong(Properties props, String name) {
        String val = props.getProperty(name);
        if (val != null) {
            try {
                return Numbers.parseLong(val);
            } catch (NumericException e) {
                System.out.println(name + ": invalid value");
            }
        }
        return -1;
    }

    private int parseSize(Properties props, String name) {
        String val = props.getProperty(name);
        if (val != null) {
            try {
                return Numbers.ceilPow2(Numbers.parseIntSize(val));
            } catch (NumericException e) {
                System.out.println(name + ": invalid value");
            }
        }
        return -1;
    }
}
