package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigReloader implements FileEventCallback {

    private static final Log LOG = LogFactory.getLog(ConfigReloader.class);
    private final java.nio.file.Path confPath;
    private final DynamicServerConfiguration config;
    private final Properties properties;
    private final Set<PropertyKey> reloadableProps = new HashSet<>(List.of(
            PropertyKey.PG_USER,
            PropertyKey.PG_PASSWORD,
            PropertyKey.PG_RO_USER_ENABLED,
            PropertyKey.PG_RO_USER,
            PropertyKey.PG_RO_PASSWORD
    ));
    private long lastModified;

    public ConfigReloader(DynamicServerConfiguration config) throws IOException {
        this.config = config;
        this.confPath = Paths.get(this.config.getCairoConfiguration().getConfRoot().toString(), Bootstrap.CONFIG_FILE);

        try (Path p = new Path()) {
            p.of(this.confPath.toString()).$();
            this.lastModified = Files.getLastModified(p);
        }

        this.properties = new Properties();
        try (InputStream is = java.nio.file.Files.newInputStream(this.confPath)) {
            this.properties.load(is);
        }
    }


    @Override
    public void onFileEvent() {
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


                if (!newProperties.equals(this.properties)) {
                    // Compare the new and existing properties
                    AtomicBoolean changed = new AtomicBoolean(false);
                    newProperties.forEach((k, v) -> {
                        String key = (String) k;
                        String oldVal = properties.getProperty(key);
                        if (oldVal == null || !oldVal.equals(newProperties.getProperty(key))) {
                            Optional<PropertyKey> prop = PropertyKey.getByString(key);
                            if (prop.isEmpty()) {
                                return;
                            }

                            if (reloadableProps.contains(prop.get())) {
                                LOG.info().$("loaded new value of ").$(k).$();
                                this.properties.setProperty(key, (String) v);
                                changed.set(true);
                            } else {
                                LOG.advisory().$("property ").$(k).$(" was modified in the config file but cannot be reloaded. ignoring new value").$();
                            }
                        }
                    });


                    // Check for any old reloadable properties that have been removed in the new config
                    properties.forEach((k, v) -> {
                        if (!newProperties.containsKey(k)) {
                            this.properties.remove(k);
                            changed.set(true);
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
