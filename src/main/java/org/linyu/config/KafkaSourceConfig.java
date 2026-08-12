package org.linyu.config;

public final class KafkaSourceConfig {
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String startingOffsets;

    public KafkaSourceConfig(
            String bootstrapServers,
            String topic,
            String groupId,
            String startingOffsets) {
        this.bootstrapServers = requireText(bootstrapServers,"kafka.bootstrap.servers");
        this.topic = requireText(topic,"kafka.top.gmv");
        this.groupId = requireText(groupId,"kafka.group.id");
        this.startingOffsets = requireText(startingOffsets,"kafka.staring.offsets");
    }
    private static String requireText(
            String value,
            String name
    ) {
        if (value == null
                || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + "不能为空"
            );
        }

        return value.trim();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getStartingOffsets() {
        return startingOffsets;
    }
}
