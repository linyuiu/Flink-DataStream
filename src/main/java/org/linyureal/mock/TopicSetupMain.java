package org.linyureal.mock;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.linyureal.config.AppConfig;
import org.linyureal.validation.TradeValidator;
import org.linyureal.common.Tables;
import java.util.*;
import java.util.concurrent.ExecutionException;

/** Explicit operator command: creates missing data topics; never deletes or changes existing ones. */
public final class TopicSetupMain {
    public static void main(String[] args) throws Exception {
        AppConfig c=AppConfig.load(args); Properties p=new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,c.get("kafka.bootstrap.servers"));
        List<String> keys=new ArrayList<>(TradeValidator.TABLES); keys.addAll(Tables.DIMENSIONS);
        keys.addAll(Arrays.asList("payment_fact","refund_fact","dirty"));
        int partitions=Math.toIntExact(c.positive("kafka.topic.partitions"));
        long replicas=c.positive("kafka.topic.replication.factor");
        if(replicas>Short.MAX_VALUE) throw new IllegalArgumentException("Invalid replication factor");
        try(AdminClient admin=AdminClient.create(p)) {
            for(String key:keys) {
                NewTopic topic=new NewTopic(c.topic(key),partitions,(short)replicas);
                // Demo history retained in full so a fresh earliest replay can reconstruct balances.
                topic.configs(Map.of("cleanup.policy","delete","retention.ms",c.get("kafka.topic.retention.ms")));
                try { admin.createTopics(Collections.singleton(topic)).all().get();
                    System.out.println("Created "+topic.name());
                } catch(ExecutionException e) {
                    if(!(e.getCause() instanceof TopicExistsException)) throw e;
                    System.out.println("Exists (configuration unchanged): "+topic.name());
                }
            }
        }
    }
}
