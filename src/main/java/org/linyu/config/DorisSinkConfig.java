package org.linyu.config;

public final class DorisSinkConfig {
    private final String feNodes;
    private final String username;
    private final String password;
    private final String tableIdentifier;
    private final String labelPrefix;

    public DorisSinkConfig(Builder builder) {
        this.feNodes = requireText(builder.feNodes,"feNodes");
        this.username = requireText(builder.username,"username");
        this.password = builder.password == null ? "" : builder.password;
        this.tableIdentifier = requireText(builder.tableIdentifier, "tableIdentifier");
        this.labelPrefix = requireText(builder.labelPrefix, "labelPrefix");

    }


    public String getFeNodes() {
        return feNodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    private static String requireText(
            String value,
            String name
    ) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    name + "不为空"
            );
        }
        return value.trim();

    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String feNodes;
        private String username;
        private String password;
        private String tableIdentifier;
        private String labelPrefix;

        private Builder() {
        }

        public Builder feNodes(String feNodes) {
            this.feNodes = feNodes;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tableIdentifier(
                String tableIdentifier
        ) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public Builder labelPrefix(
                String labelPrefix
        ) {
            this.labelPrefix = labelPrefix;
            return this;
        }

        public DorisSinkConfig build() {
            return new DorisSinkConfig(this);
        }
    }
}

