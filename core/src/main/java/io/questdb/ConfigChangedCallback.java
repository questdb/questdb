package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigChangedCallback implements DirWatcherCallback {
    private final static Log LOG = LogFactory.getLog(ServerConfigurationChangeWatcherJob.class);
    Path fp;
    DynamicServerConfiguration config;
    long lastModified;
    Properties properties;

    public ConfigChangedCallback(Path fp, DynamicServerConfiguration config) throws IOException {
        this.fp = fp;
        this.lastModified = Files.getLastModified(fp);
        this.config = config;
        this.properties = loadProperties();

    }

    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        String tmp = this.fp.toString();
        java.nio.file.Path configFile = Paths.get(tmp);
        try (InputStream is = java.nio.file.Files.newInputStream(configFile)) {
            properties.load(is);
        }
        this.fp.of(tmp);
        return properties;

    }
    @Override
    public void onDirChanged() {
        long lastModified = Files.getLastModified(fp);
        if (lastModified > this.lastModified) {
            this.lastModified = lastModified;

            LOG.info().$("config file changed").$();
            Properties newProperties;

            try  {
                newProperties = loadProperties();
                if (!newProperties.equals(this.properties)) {
                    this.config.reload(newProperties);
                    this.properties = newProperties;
                    LOG.info().$("config successfully reloaded").$();
                } else {
                    LOG.info().$("skipping config reload").$();
                }
            }
            catch (IOException exc) {
                LOG.error().$("error loading properties").$();
            }
        }
    }
}
