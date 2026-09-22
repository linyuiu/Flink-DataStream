package org.linyureal.realtime.config;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;

public final class PipelineConfig implements Serializable {
    private final Properties properties = new Properties();
    public PipelineConfig(Properties source) { properties.putAll(source); }
    public static PipelineConfig load(String[] args) throws IOException {
        Properties p = new Properties();
        try (InputStream in = PipelineConfig.class.getResourceAsStream("/linyureal/realtime/pipeline.properties")) {
            if (in == null) throw new FileNotFoundException("pipeline.properties");
            p.load(in);
        }
        if (args.length > 0) try (InputStream in = Files.newInputStream(Path.of(args[0]))) { p.load(in); }
        return new PipelineConfig(p);
    }
    public String get(String key) {
        String v = properties.getProperty(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing config: " + key);
        return v.trim();
    }
    public long positive(String key) {
        long n = Long.parseLong(get(key));
        if (n <= 0) throw new IllegalArgumentException(key + " must be positive");
        return n;
    }
    public String secret(String env) {
        String v = System.getenv(env);
        if (v == null) throw new IllegalArgumentException("Missing environment: " + env);
        return v;
    }
    public String topic(String table) { return get("topic." + table); }
}
