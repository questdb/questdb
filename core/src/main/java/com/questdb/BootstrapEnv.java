package com.questdb;

import com.questdb.factory.Factory;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.txt.parser.listener.probe.TypeProbeCollection;

public class BootstrapEnv {
    public ServerConfiguration configuration;
    public SimpleUrlMatcher matcher;
    public Factory factory;
    public TypeProbeCollection typeProbeCollection;
}
