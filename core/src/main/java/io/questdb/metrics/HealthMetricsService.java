package io.questdb.metrics;

import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

public class HealthMetricsService {
    private final CairoEngine engine;
    private final ServerConfiguration configuration;

    public HealthMetricsService(CairoEngine engine, ServerConfiguration configuration) {
        this.engine = engine;
        this.configuration = configuration;
    }

    public Map<String, Object> collectHealthMetrics() {
        Map<String, Object> metrics = new HashMap<>();

        // System Metrics
        metrics.put("system", getSystemMetrics());
        
        // Database Metrics
        metrics.put("database", getDatabaseMetrics());
        
        // Performance Metrics
        metrics.put("performance", getPerformanceMetrics());
        
        // Configuration Overview
        metrics.put("configuration", getConfigurationMetrics());

        return metrics;
    }

    private Map<String, Object> getSystemMetrics() {
        Map<String, Object> systemMetrics = new HashMap<>();
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        systemMetrics.put("os_name", osBean.getName());
        systemMetrics.put("os_version", osBean.getVersion());
        systemMetrics.put("available_processors", osBean.getAvailableProcessors());
        systemMetrics.put("system_load_average", osBean.getSystemLoadAverage());
        systemMetrics.put("uptime_ms", runtimeBean.getUptime());

        return systemMetrics;
    }

    private Map<String, Object> getDatabaseMetrics() {
        Map<String, Object> dbMetrics = new HashMap<>();
        
        // Collect table count and row count
        try (SqlCompiler compiler = new SqlCompiler(engine);
             SqlExecutionContext executionContext = engine.getSqlExecutionContextFactory().createContext()) {
            
            // Example query to get table count
            long tableCount = compiler.compile("SELECT COUNT(*) FROM information_schema.tables", executionContext)
                .getRecordCount();
            
            dbMetrics.put("table_count", tableCount);
        } catch (Exception e) {
            dbMetrics.put("table_count_error", e.getMessage());
        }

        return dbMetrics;
    }

    private Map<String, Object> getPerformanceMetrics() {
        Map<String, Object> perfMetrics = new HashMap<>();
        
        // Memory usage
        Runtime runtime = Runtime.getRuntime();
        perfMetrics.put("total_memory_mb", runtime.totalMemory() / (1024 * 1024));
        perfMetrics.put("free_memory_mb", runtime.freeMemory() / (1024 * 1024));
        perfMetrics.put("max_memory_mb", runtime.maxMemory() / (1024 * 1024));

        return perfMetrics;
    }

    private Map<String, Object> getConfigurationMetrics() {
        Map<String, Object> configMetrics = new HashMap<>();
        
        configMetrics.put("http_port", configuration.getHttpServerConfiguration().getPort());
        configMetrics.put("postgres_port", configuration.getPgWireConfiguration().getPort());
        configMetrics.put("line_protocol_port", configuration.getLineUdpReceiverConfiguration().getPort());
        
        return configMetrics;
    }
} 