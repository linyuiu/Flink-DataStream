package org.linyu.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class RealtimeGmvConfigLoader {

    private RealtimeGmvConfigLoader() {
    }

    public static RealtimeGmvConfig load(
            String profile
    ) {
        Properties properties = new Properties();

        String resourceName =
                "bigdata-" + profile + ".properties";

        loadClasspathProperties(
                resourceName,
                properties
        );

        /*
         * 可以使用：
         *
         * -Dapp.config=/path/application.properties
         *
         * 使用外部配置覆盖JAR中的环境配置。
         */
        String externalConfig =
                System.getProperty("app.config");

        if (!isBlank(externalConfig)) {
            loadExternalProperties(
                    externalConfig,
                    properties
            );
        }

        /*
         * 密码优先读取环境变量，
         * 不建议将生产密码打包进JAR。
         */
        String dorisPassword =
                System.getenv("DORIS_PASSWORD");

        if (isBlank(dorisPassword)) {
            dorisPassword =
                    properties.getProperty(
                            "doris.password",
                            ""
                    );
        }

        KafkaSourceConfig kafka =
                new KafkaSourceConfig(
                        required(
                                properties,
                                "kafka.bootstrap.servers"
                        ),
                        required(
                                properties,
                                "kafka.topic.gvm_realtime"
                        ),
                        required(
                                properties,
                                "kafka.group.id.dagGmv"
                        ),
                        properties.getProperty(
                                "kafka.starting.offsets",
                                "committed"
                        )
                );

        DorisSinkConfig doris =
                DorisSinkConfig.builder()
                        .feNodes(
                                required(
                                        properties,
                                        "doris.fenodes"
                                )
                        )
                        .username(
                                required(
                                        properties,
                                        "doris.username"
                                )
                        )
                        .password(dorisPassword)
                        .tableIdentifier(
                                required(
                                        properties,
                                        "doris.table.identifier.dailyGmvRealTime"
                                )
                        )
                        .labelPrefix(
                                required(
                                        properties,
                                        "doris.sink.label.prefix"
                                )
                        )
                        .build();

        GmvPolicy policy =
                new GmvPolicy(
                        getBoolean(
                                properties,
                                "gmv.deduct-refunds",
                                true
                        )
                );

        return new RealtimeGmvConfig(
                kafka,
                doris,
                policy,
                getInt(
                        properties,
                        "flink.parallelism",
                        1
                ),
                getLong(
                        properties,
                        "flink.checkpoint.interval.ms",
                        10_000L
                ),
                getLong(
                        properties,
                        "flink.checkpoint.timeout.ms",
                        120_000L
                ),
                required(
                        properties,
                        "flink.checkpoint.directory"
                ),
                getLong(
                        properties,
                        "gmv.output.interval.ms",
                        5_000L
                ),
                getInt(
                        properties,
                        "gmv.order-state.ttl.days",
                        100
                ),
                getInt(
                        properties,
                        "gmv.daily-state.ttl.days",
                        7
                )
        );
    }

    /**
     * 单元测试可以直接构造Properties调用这个方法，
     * 不需要依赖真实配置文件。
     */
    public static RealtimeGmvConfig from(
            Properties properties
    ) {
        KafkaSourceConfig kafka =
                new KafkaSourceConfig(
                        required(
                                properties,
                                "kafka.bootstrap.servers"
                        ),
                        required(
                                properties,
                                "kafka.topic.gvm_realtime"
                        ),
                        required(
                                properties,
                                "kafka.group.id.dagGmv"
                        ),
                        properties.getProperty(
                                "kafka.starting.offsets",
                                "committed"
                        )
                );

        DorisSinkConfig doris =
                DorisSinkConfig.builder()
                        .feNodes(
                                required(
                                        properties,
                                        "doris.fenodes"
                                )
                        )
                        .username(
                                required(
                                        properties,
                                        "doris.username"
                                )
                        )
                        .password(
                                properties.getProperty(
                                        "doris.password",
                                        ""
                                )
                        )
                        .tableIdentifier(
                                required(
                                        properties,
                                        "doris.table.identifier.dailyGmvRealTime"
                                )
                        )
                        .labelPrefix(
                                required(
                                        properties,
                                        "doris.sink.label.prefix"
                                )
                        )
                        .build();

        return new RealtimeGmvConfig(
                kafka,
                doris,
                new GmvPolicy(
                        getBoolean(
                                properties,
                                "gmv.deduct-refunds",
                                true
                        )
                ),
                getInt(
                        properties,
                        "flink.parallelism",
                        1
                ),
                getLong(
                        properties,
                        "flink.checkpoint.interval.ms",
                        10_000L
                ),
                getLong(
                        properties,
                        "flink.checkpoint.timeout.ms",
                        120_000L
                ),
                required(
                        properties,
                        "flink.checkpoint.directory"
                ),
                getLong(
                        properties,
                        "gmv.output.interval.ms",
                        5_000L
                ),
                getInt(
                        properties,
                        "gmv.order-state.ttl.days",
                        100
                ),
                getInt(
                        properties,
                        "gmv.daily-state.ttl.days",
                        7
                )
        );
    }

    private static void loadClasspathProperties(
            String resourceName,
            Properties target
    ) {
        try (InputStream inputStream =
                     RealtimeGmvConfigLoader.class
                             .getClassLoader()
                             .getResourceAsStream(
                                     resourceName
                             )) {

            if (inputStream == null) {
                throw new IllegalArgumentException(
                        "没有找到配置文件："
                                + resourceName
                );
            }

            target.load(inputStream);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "读取配置文件失败："
                            + resourceName,
                    exception
            );
        }
    }

    private static void loadExternalProperties(
            String file,
            Properties target
    ) {
        Path path = Paths.get(file);

        try (InputStream inputStream =
                     Files.newInputStream(path)) {
            target.load(inputStream);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "读取外部配置失败：" + path,
                    exception
            );
        }
    }

    private static String required(
            Properties properties,
            String key
    ) {
        String value =
                properties.getProperty(key);

        if (isBlank(value)) {
            throw new IllegalArgumentException(
                    "缺少配置项：" + key
            );
        }

        return value.trim();
    }

    private static int getInt(
            Properties properties,
            String key,
            int defaultValue
    ) {
        String value =
                properties.getProperty(key);

        return isBlank(value)
                ? defaultValue
                : Integer.parseInt(value.trim());
    }

    private static long getLong(
            Properties properties,
            String key,
            long defaultValue
    ) {
        String value =
                properties.getProperty(key);

        return isBlank(value)
                ? defaultValue
                : Long.parseLong(value.trim());
    }

    private static boolean getBoolean(
            Properties properties,
            String key,
            boolean defaultValue
    ) {
        String value =
                properties.getProperty(key);

        return isBlank(value)
                ? defaultValue
                : Boolean.parseBoolean(
                value.trim()
        );
    }

    private static boolean isBlank(
            String value
    ) {
        return value == null
                || value.trim().isEmpty();
    }
}