package org.linyureal.realtime.mock;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.linyureal.realtime.common.Contracts;
import org.linyureal.realtime.config.PipelineConfig;
import java.util.*;
import java.util.concurrent.ExecutionException;

/** Explicit deployment command; never deletes topics or alters existing retention. */
public final class CreateTopics {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args); Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, c.get("kafka.bootstrap.servers"));
        List<String> keys = new ArrayList<>(Contracts.TRADE_TABLES); keys.addAll(Contracts.DIM_TABLES); keys.addAll(List.of("facts", "dirty"));
        long replicas = c.positive("kafka.topic.replicas");
        if (replicas > Short.MAX_VALUE) throw new IllegalArgumentException("Invalid replica count");
        try (AdminClient admin = AdminClient.create(p)) {
            for (String key : keys) {
                NewTopic topic = new NewTopic(c.topic(key), Math.toIntExact(c.positive("kafka.topic.partitions")), (short) replicas)
                        .configs(Map.of("cleanup.policy", "delete", "retention.ms", c.get("kafka.topic.retention.ms")));
                try { admin.createTopics(List.of(topic)).all().get(); System.out.println("Created " + topic.name()); }
                catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) throw e;
                    System.out.println("Already exists, verify config yourself: " + topic.name());
                }
            }
        }
    }
}
