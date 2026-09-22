package org.commerce.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/** 每个作业独立持有配置：先读 jar 默认值，再由外部文件覆盖；不使用可变全局配置。 */
public final class CommerceConfig implements Serializable {
    private final Properties values = new Properties();

    public CommerceConfig(Properties properties) {
        values.putAll(properties);
    }

    public static CommerceConfig load(String[] args) throws IOException {
        Properties properties = new Properties();
        try (InputStream input =
                CommerceConfig.class.getResourceAsStream("/commerce/application.properties")) {
            if (input == null) {
                throw new FileNotFoundException("commerce/application.properties");
            }
            properties.load(input);
        }
        if (args.length > 0) {
            try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
                properties.load(input);
            }
        }
        CommerceConfig config = new CommerceConfig(properties);
        config.validate();
        return config;
    }

    public String get(String key) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing config: " + key);
        }
        return value.trim();
    }

    public String get(String key, String defaultValue) {
        return values.containsKey(key) ? get(key) : defaultValue;
    }

    public long positive(String key) {
        long value = Long.parseLong(get(key));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    public long positive(String key, long defaultValue) {
        return values.containsKey(key) ? positive(key) : defaultValue;
    }

    public String secret(String name) {
        String value = System.getenv(name);
        if (value == null || (isProduction() && value.isBlank())) {
            throw new IllegalArgumentException("Missing environment variable: " + name);
        }
        return value;
    }

    public String topic(String table) {
        return get("topic." + table);
    }

    public boolean isProduction() {
        return "production".equals(get("deployment.environment", "local"));
    }

    /** 在创建拓扑前发现不可运行的并行度、事务周期和持久化配置。 */
    public void validateJob(String jobName) {
        String environment = get("deployment.environment", "local");
        if (!List.of("local", "production").contains(environment)) {
            throw new IllegalArgumentException(
                    "deployment.environment must be local or production");
        }
        String mode = get("pipeline.mode", "split");
        if (!List.of("split", "integrated").contains(mode)) {
            throw new IllegalArgumentException("pipeline.mode must be split or integrated");
        }
        if (List.of("facts", "amounts", "distincts", "audit").contains(jobName)
                && !"split".equals(mode)) {
            throw new IllegalArgumentException(
                    "Separate fact/metric/audit jobs require pipeline.mode=split");
        }
        long maxParallelism = positive("flink.max.parallelism");
        if (maxParallelism > 32768 || positive("parallelism." + jobName) > maxParallelism) {
            throw new IllegalArgumentException("Job parallelism exceeds valid max parallelism");
        }
        if (List.of("amounts", "distincts", "metrics").contains(jobName)
                && positive("parallelism.doris") > maxParallelism) {
            throw new IllegalArgumentException("Doris parallelism exceeds max parallelism");
        }
        long interval =
                positive(
                        "checkpoint." + jobName + ".interval.ms",
                        positive("checkpoint.interval.ms"));
        long transactionBudget = Math.addExact(interval, positive("checkpoint.timeout.ms"));
        transactionBudget =
                Math.addExact(transactionBudget, positive("recovery.budget.ms", 120_000));
        if (transactionBudget >= positive("kafka.transaction.timeout.ms")) {
            throw new IllegalArgumentException(
                    "Kafka transaction timeout must exceed checkpoint interval, timeout and recovery budget");
        }
        if (isProduction()) {
            String scheme = URI.create(get("checkpoint.directory")).getScheme();
            if (scheme == null || "file".equalsIgnoreCase(scheme)) {
                throw new IllegalArgumentException(
                        "Production checkpoints require shared durable storage");
            }
            if (positive("kafka.replicas") < 3 || positive("kafka.min.insync.replicas") < 2) {
                throw new IllegalArgumentException(
                        "Production Kafka requires replicas >= 3 and min ISR >= 2");
            }
            for (String key : values.stringPropertyNames()) {
                if (values.getProperty(key).contains("REPLACE_")) {
                    throw new IllegalArgumentException("Unresolved production placeholder: " + key);
                }
            }
        }
    }

    private void validate() {
        if (positive("online.distinct.metric.days", positive("online.event.days") + 2)
                < positive("online.event.days") + 2) {
            throw new IllegalArgumentException(
                    "Distinct aggregate retention must cover the event horizon plus 2 days");
        }
        if (positive("kafka.min.insync.replicas") > positive("kafka.replicas")) {
            throw new IllegalArgumentException("Kafka min ISR cannot exceed replica count");
        }
        if (positive("online.metric.days") <= positive("online.event.days") + 2) {
            throw new IllegalArgumentException(
                    "Metric retention must exceed event admission horizon + 2 days");
        }
        if (positive("checkpoint.timeout.ms") >= positive("kafka.transaction.timeout.ms")) {
            throw new IllegalArgumentException(
                    "Kafka transaction timeout must exceed checkpoint timeout");
        }
    }

    /** 仿真参数只影响数据生成，不参与线上Job的启动校验。 */
    public void validateMock() {
        double paymentRate = Double.parseDouble(get("business.payment.rate"));
        if (!Double.isFinite(paymentRate) || paymentRate <= 0 || paymentRate > 1) {
            throw new IllegalArgumentException("business.payment.rate must be in (0, 1]");
        }
    }
}
