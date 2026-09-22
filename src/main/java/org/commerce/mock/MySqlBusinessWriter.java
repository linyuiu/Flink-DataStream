package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;
import org.commerce.model.TradeEvent;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** 事务计划的 JDBC 执行器：一个业务动作的表更新与 Outbox 写入必须一起成功或一起回滚。 */
public final class MySqlBusinessWriter implements AutoCloseable {
    private static final Set<String> TABLES =
            Set.of(
                    "trade_order",
                    "trade_order_line",
                    "pay_attempt",
                    "pay_ledger",
                    "pay_allocation",
                    "refund_request",
                    "refund_allocation");
    private final Connection connection;

    public MySqlBusinessWriter(CommerceConfig config) throws SQLException {
        connection =
                DriverManager.getConnection(
                        config.get("mock.jdbc.url"),
                        config.get("mock.jdbc.username"),
                        config.secret("COMMERCE_MYSQL_PASSWORD"));
        connection.setAutoCommit(false);
    }

    public void initializeCatalog() throws SQLException {
        try {
            for (var table : Catalog.rows().entrySet()) {
                for (ObjectNode row : table.getValue()) {
                    ensureCatalogRow(table.getKey(), row);
                }
            }
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            connection.rollback();
            throw exception;
        }
    }

    public void execute(TransactionPlan plan) throws SQLException {
        try {
            writeBusinessRows(plan);
            if (plan.event != null) {
                writeOutbox(plan.event);
            }
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            connection.rollback();
            throw exception;
        }
    }

    private void writeBusinessRows(TransactionPlan plan) throws SQLException {
        for (TransactionPlan.Row row : plan.rows) {
            if (!TABLES.contains(row.table)) {
                throw new IllegalArgumentException("Invalid business table");
            }
            if (row.update) {
                update(row.table, row.value);
            } else {
                insert(row.table, row.value);
            }
        }
    }

    private void writeOutbox(TradeEvent event) throws SQLException {
        insert("trade_outbox", MockCdcRecord.outboxRow(event));
    }

    private void ensureCatalogRow(String table, ObjectNode row) throws SQLException {
        try {
            insert(table, row);
        } catch (SQLException duplicate) {
            // 只允许“主键重复且内容完全一致”的种子数据，不能用 INSERT IGNORE 吞掉其他错误。
            if (duplicate.getErrorCode() != 1062) {
                throw duplicate;
            }
            verifyExistingCatalogRow(table, row, duplicate);
        }
    }

    private void verifyExistingCatalogRow(String table, ObjectNode expected, SQLException duplicate)
            throws SQLException {
        List<String> columnNames = columns(expected);
        String sql = "SELECT " + String.join(",", columnNames) + " FROM " + table + " WHERE id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, Json.text(expected, "id"));
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw duplicate;
                }
                for (String column : columnNames) {
                    boolean equal =
                            column.equals("attributes_json")
                                    ? Json.object(result.getString(column))
                                            .equals(Json.object(expected.get(column).asText()))
                                    : expected.get(column)
                                            .asText()
                                            .equals(result.getString(column));
                    if (!equal) {
                        throw new SQLException(
                                "Existing demo catalog differs: "
                                        + table
                                        + "/"
                                        + Json.text(expected, "id")
                                        + "; use an isolated demo database");
                    }
                }
            }
        }
    }

    private static List<String> columns(ObjectNode row) {
        List<String> columnNames = new ArrayList<>();
        row.fieldNames().forEachRemaining(columnNames::add);
        for (String column : columnNames) {
            if (!column.matches("[a-z_]+")) {
                throw new IllegalArgumentException("Invalid column");
            }
        }
        return columnNames;
    }

    /** 返回下一个可用的参数位置，便于 UPDATE 继续绑定 WHERE 条件。 */
    private static int bind(PreparedStatement statement, ObjectNode row, List<String> columnNames)
            throws SQLException {
        int parameterIndex = 1;
        for (String column : columnNames) {
            var value = row.get(column);
            if (value.isIntegralNumber()) {
                statement.setLong(parameterIndex++, value.longValue());
            } else {
                statement.setString(parameterIndex++, value.asText());
            }
        }
        return parameterIndex;
    }

    private void insert(String table, ObjectNode row) throws SQLException {
        List<String> columnNames = columns(row);
        String placeholders =
                columnNames.stream().map(ignored -> "?").collect(Collectors.joining(","));
        String sql =
                "INSERT INTO "
                        + table
                        + " ("
                        + String.join(",", columnNames)
                        + ") VALUES ("
                        + placeholders
                        + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, row, columnNames);
            statement.executeUpdate();
        }
    }

    private void update(String table, ObjectNode row) throws SQLException {
        List<String> columnNames = columns(row);
        columnNames.remove("id");
        String assignments =
                columnNames.stream().map(column -> column + "=?").collect(Collectors.joining(","));
        String sql = "UPDATE " + table + " SET " + assignments + " WHERE id=? AND version=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameterIndex = bind(statement, row, columnNames);
            statement.setString(parameterIndex++, Json.text(row, "id"));
            // 只允许从预期的旧版本推进，避免覆盖并发修改。
            statement.setLong(parameterIndex, Json.number(row, "version") - 1);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("Optimistic version conflict: " + table);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
