package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigReloader implements Closeable, DirWatcherCallback {
    private static final Log LOG = LogFactory.getLog(ConfigReloader.class);
    DynamicServerConfiguration config;
    DirWatcher dirWatcher;
    java.nio.file.Path confPath;
    long lastModified;
    Properties properties;
    public ConfigReloader(DynamicServerConfiguration config) {
        this.config = config;
        this.confPath = Paths.get(this.config.getConfRoot().toString(), Bootstrap.CONFIG_FILE);
        this.dirWatcher = DirWatcherFactory.GetDirWatcher(this.config.getConfRoot().toString());
    }

    public void watch() {
        try (Path p = new Path()) {
            p.of(this.confPath.toString()).$();
            this.lastModified = Files.getLastModified(p);
        }

        this.properties = new Properties();
        try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
            this.properties.load(is);
        } catch (IOException exc) {
            LOG.error().$(exc).$();
            return;
        }

        this.dirWatcher.waitForChange(this);
    }

    @Override
    public void close() throws IOException {
        this.dirWatcher.close();
    }


    @Override
    public void onDirChanged() {
        try (Path p = new Path()) {
            p.of(this.confPath.toString()).$();

            // Check that the file has been modified since the last trigger
            long newLastModified = Files.getLastModified(p);
            if (newLastModified > this.lastModified) {
                // If it has, update the cached value
                this.lastModified = newLastModified;

                // Then load the config properties
                Properties newProperties = new Properties();
                try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
                    newProperties.load(is);
                } catch (IOException exc) {
                    LOG.error().$(exc).$();
                }

                // Compare the new and existing properties
                if (!newProperties.equals(this.properties)) {
                    // If they are different, reload the config in place
                    config.reload(newProperties);
                    this.properties = newProperties;
                    LOG.info().$("config reloaded!").$();
                }
            }
        }
    }
}
