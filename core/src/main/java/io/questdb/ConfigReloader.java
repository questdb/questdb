package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigReloader implements QuietCloseable, DirWatcherCallback {

    private static final Log LOG = LogFactory.getLog(ConfigReloader.class);
    private final Set<PropertyKey> reloadableProps = new HashSet<>(List.of(
            PropertyKey.QUERY_TIMEOUT_SEC
    ));
    DynamicServerConfiguration config;
    DirWatcher dirWatcher;
    java.nio.file.Path confPath;
    long lastModified;
    Properties properties;
    public ConfigReloader(DynamicServerConfiguration config) {
        this.config = config;
        this.confPath = Paths.get(this.config.getConfRoot().toString(), Bootstrap.CONFIG_FILE);
        this.dirWatcher = DirWatcherFactory.getDirWatcher(this.config.getConfRoot());
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
    public void close() {
        this.dirWatcher = Misc.free(dirWatcher);
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
                    AtomicBoolean changed = new AtomicBoolean(false);
                    newProperties.forEach((k, v) -> {
                        String key = (String)k;
                        String oldVal = properties.getProperty(key);
                        if (oldVal != v) {
                            Optional<PropertyKey> prop = PropertyKey.getByString(key);
                            if (prop.isEmpty()) {
                                return;
                            }

                            if (reloadableProps.contains(prop.get())) {
                                LOG.info().$("new value of ").$(k).$(" = ").$(v).$();
                                this.properties.setProperty(key, (String)v);
                                changed.set(true);
                            } else {
                                LOG.advisory().$("property ").$(k).$(" was modified in the config file but cannot be reloaded. ignoring new value").$();
                            }
                        }
                    });

                    // If they are different, reload the config in place
                    if (changed.get()) {
                        config.reload(this.properties);
                        LOG.info().$("config reloaded!").$();
                    }

                }
            }
        }
    }
}
