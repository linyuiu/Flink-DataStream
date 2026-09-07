package org.linyureal.config;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;

/** Immutable configuration loaded only at the application boundary. */
public final class AppConfig implements Serializable {
    private final Properties values;
    public AppConfig(Properties values) {
        this.values = new Properties();
        this.values.putAll(values);
    }
    public static AppConfig load(String[] args) throws IOException {
        Properties p = new Properties();
        try (InputStream in = AppConfig.class.getResourceAsStream("/linyureal/application.properties")) {
            if (in == null) throw new FileNotFoundException("linyureal/application.properties");
            p.load(in);
        }
        if (args.length > 0) {
            try (InputStream in = Files.newInputStream(Paths.get(args[0]))) { p.load(in); }
        }
        return new AppConfig(p);
    }
    public String get(String key) {
        String value = values.getProperty(key);
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("Missing config: " + key);
        return value.trim();
    }
    public long positive(String key) {
        long n = Long.parseLong(get(key));
        if (n <= 0) throw new IllegalArgumentException(key + " must be positive");
        return n;
    }
    public String secret(String environment) {
        String value = System.getenv(environment);
        if (value == null) throw new IllegalArgumentException("Missing environment: " + environment);
        return value;
    }
    public String topic(String table) { return get("topic." + table); }
}
