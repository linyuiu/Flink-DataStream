package org.commerce.ops;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.commerce.common.Contract;
import org.commerce.config.CommerceConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

public final class CreateTopics {
    public static void main(String[] args) throws Exception {
        CommerceConfig config = CommerceConfig.load(args);
        Properties adminProperties = new Properties();
        adminProperties.put("bootstrap.servers", config.get("kafka.bootstrap.servers"));
        List<String> names = new ArrayList<>(Contract.CDC_TABLES);
        names.addAll(List.of("quality", "repair"));
        names.addAll(List.of("trade_fact", "split_quality", "split_repair"));
        long replicas = config.positive("kafka.replicas");
        if (replicas > Short.MAX_VALUE) throw new IllegalArgumentException("Invalid replica count");
        try (AdminClient client = AdminClient.create(adminProperties)) {
            for (String key : names) {
                boolean control =
                        key.equals("quality")
                                || key.equals("repair")
                                || key.equals("split_quality")
                                || key.equals("split_repair");
                Map<String, String> settings = new HashMap<>();
                // Immutable facts expire; dimension topics retain the latest record for EVERY key.
                settings.put("cleanup.policy", key.startsWith("dim_") ? "compact" : "delete");
                if (!key.startsWith("dim_"))
                    settings.put(
                            "retention.ms",
                            config.get(
                                    control
                                            ? "kafka.repair.retention.ms"
                                            : "kafka.data.retention.ms"));
                settings.put("min.insync.replicas", config.get("kafka.min.insync.replicas"));
                NewTopic topic =
                        new NewTopic(
                                        config.topic(key),
                                        Math.toIntExact(config.positive("kafka.partitions")),
                                        (short) replicas)
                                .configs(settings);
                try {
                    client.createTopics(List.of(topic)).all().get();
                    System.out.println("Created " + topic.name());
                } catch (ExecutionException exception) {
                    if (!(exception.getCause() instanceof TopicExistsException)) throw exception;
                    System.out.println("Already exists; settings not changed: " + topic.name());
                }
            }
        }
    }
}
