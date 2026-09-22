package org.linyureal.realtime.source;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.linyureal.realtime.common.Contracts;
import org.linyureal.realtime.config.PipelineConfig;
import java.util.*;

public final class MysqlCdcSourceFactory {
    private MysqlCdcSourceFactory() {}
    public static MySqlSource<String> create(PipelineConfig c) {
        String db = c.get("cdc.database");
        if (!db.matches("[A-Za-z][A-Za-z0-9_]*")) throw new IllegalArgumentException("Invalid CDC database");
        List<String> tables = new ArrayList<>(Contracts.TRADE_TABLES); tables.addAll(Contracts.DIM_TABLES);
        Properties debezium = new Properties();
        debezium.setProperty("time.precision.mode", "connect");
        debezium.setProperty("decimal.handling.mode", "string");
        return MySqlSource.<String>builder().hostname(c.get("cdc.hostname"))
                .port(Math.toIntExact(c.positive("cdc.port"))).databaseList(db)
                .tableList(tables.stream().map(t -> db + "." + t).toArray(String[]::new))
                .username(c.get("cdc.username")).password(c.secret("RTTRADE_CDC_PASSWORD"))
                .serverId(c.get("cdc.server.id.range")).serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial()).includeSchemaChanges(false)
                .debeziumProperties(debezium).deserializer(new JsonDebeziumDeserializationSchema(false)).build();
    }
}
