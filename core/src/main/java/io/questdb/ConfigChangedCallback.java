package io.questdb;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public interface ConfigChangedCallback {
    void onConfigChanged(ServerConfiguration config);
}
