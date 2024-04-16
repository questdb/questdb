package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpMinServerConfiguration;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.metrics.MetricsConfiguration;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;


public class SteveServerConfiguration implements DynamicServerConfiguration {

    private final static Log LOG = LogFactory.getLog(SteveServerConfiguration.class);
    private AtomicReference<PropServerConfiguration> delegate;

    String root;
    @Nullable Map<String, String> env;
    Log log;
    BuildInformation buildInformation;
    FilesFacade filesFacade;
    MicrosecondClock microsecondClock;
    FactoryProviderFactory fpf;
    boolean loadAdditionalConfigurations;

    public SteveServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf,
            boolean loadAdditionalConfigurations
    ) throws ServerConfigurationException, JsonException {
        this.root = root;
        this.env = env;
        this.log = log;
        this.buildInformation = buildInformation;
        this.filesFacade = filesFacade;
        this.microsecondClock = microsecondClock;
        this.fpf = fpf;
        this.loadAdditionalConfigurations = loadAdditionalConfigurations;

        PropServerConfiguration serverConfig = new PropServerConfiguration(
                root,
                properties,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf ,
                loadAdditionalConfigurations
        );
        this.delegate = new AtomicReference<>(serverConfig);
    }

    public SteveServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation,
            FilesFacade filesFacade,
            MicrosecondClock microsecondClock,
            FactoryProviderFactory fpf
    ) throws ServerConfigurationException, JsonException {
        this(
                root,
                properties,
                env,
                log,
                buildInformation,
                filesFacade,
                microsecondClock,
                fpf,
                true
        );
    }

    public SteveServerConfiguration(
            String root,
            Properties properties,
            @Nullable Map<String, String> env,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        this(
                root,
                properties,
                env,
                log,
                buildInformation,
                FilesFacadeImpl.INSTANCE,
                MicrosecondClockImpl.INSTANCE,
                (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE,
                true
        );
    }

    public void reload(Properties properties) {
        PropServerConfiguration newConfig;
        try {
            newConfig = new PropServerConfiguration(
                    this.root,
                    properties,
                    this.env,
                    this.log,
                    this.buildInformation,
                    this.filesFacade,
                    this.microsecondClock,
                    this.fpf,
                    this.loadAdditionalConfigurations
            );
        }
        catch (ServerConfigurationException|JsonException e){
            this.log.error().$(e);
            return;
        }

        delegate.set(newConfig);
    }

    @Override
    public CharSequence getConfRoot() {
        return delegate.get().getCairoConfiguration().getConfRoot();
    }


    @Override
    public CairoConfiguration getCairoConfiguration() {
        return delegate.get().getCairoConfiguration();
    }

    @Override
    public FactoryProvider getFactoryProvider() {
        return delegate.get().getFactoryProvider();
    }

    @Override
    public HttpMinServerConfiguration getHttpMinServerConfiguration() {
        return delegate.get().getHttpMinServerConfiguration();
    }

    @Override
    public HttpServerConfiguration getHttpServerConfiguration() {
        return delegate.get().getHttpServerConfiguration();
    }

    @Override
    public LineTcpReceiverConfiguration getLineTcpReceiverConfiguration() {
        return delegate.get().getLineTcpReceiverConfiguration();
    }

    @Override
    public LineUdpReceiverConfiguration getLineUdpReceiverConfiguration() {
        return delegate.get().getLineUdpReceiverConfiguration();
    }

    @Override
    public MetricsConfiguration getMetricsConfiguration() {
        return delegate.get().getMetricsConfiguration();
    }

    @Override
    public PGWireConfiguration getPGWireConfiguration() {
        return delegate.get().getPGWireConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWalApplyPoolConfiguration() {
        return delegate.get().getWalApplyPoolConfiguration();
    }

    @Override
    public WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return delegate.get().getWorkerPoolConfiguration();
    }

    @Override
    public void init(CairoEngine engine, FreeOnExit freeOnExit) {
        delegate.get().init(engine, freeOnExit);
    }

    @Override
    public boolean isLineTcpEnabled() {
        return delegate.get().isLineTcpEnabled();
    }
}
