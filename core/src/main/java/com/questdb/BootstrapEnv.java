package com.questdb;

import com.questdb.net.http.ServerConfiguration;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.store.factory.Factory;

public class BootstrapEnv {
    public ServerConfiguration configuration;
    public SimpleUrlMatcher matcher;
    public Factory factory;
    public TypeProbeCollection typeProbeCollection;
    public DateFormatFactory dateFormatFactory;
    public DateLocaleFactory dateLocaleFactory;
}
