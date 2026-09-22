package org.linyureal.realtime.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.model.RowChange;
import org.linyureal.realtime.validation.RowValidator;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Only the simulator writes business tables. CDC jobs are read-only to MySQL. */
public final class MySqlWriter implements AutoCloseable {
    private final Connection connection;
    public MySqlWriter(PipelineConfig c) throws SQLException {
        connection = DriverManager.getConnection(c.get("mock.mysql.url"), c.get("mock.mysql.username"), c.secret("RTTRADE_MYSQL_PASSWORD"));
        connection.setAutoCommit(false);
    }
    public void seedDimensions() throws SQLException {
        try {
            for (Map.Entry<String, List<ObjectNode>> table : Catalog.rows().entrySet()) for (ObjectNode row : table.getValue()) {
                insert(table.getKey(), row, true);
                // Refuse incompatible catalog business attributes rather than generating mismatched snapshots.
                try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + table.getKey() + " WHERE id=?")) {
                    statement.setString(1, Jsons.text(row, "id"));
                    try (ResultSet result = statement.executeQuery()) {
                        if (!result.next()) throw new SQLException("Missing catalog row");
                        for (String field : List.of("parent_id", "category_id", "brand_id", "product_id", "unit_price_cent", "region_level"))
                            if (row.has(field) && !row.get(field).asText().equals(result.getString(field)))
                                throw new SQLException("Catalog differs from fixture: " + table.getKey() + "/" + row.get("id") + "/" + field);
                    }
                }
            }
            connection.commit();
        } catch (SQLException | RuntimeException e) { connection.rollback(); throw e; }
    }
    public void transaction(List<RowChange> changes) throws SQLException {
        try {
            for (RowChange c : changes) {
                RowValidator.validate(c); ObjectNode row = Jsons.object(c.rowJson);
                if (c.operation.equals("c")) insert(c.table, row, false);
                else {
                    List<String> columns = columns(row); columns.remove("id");
                    String sql = "UPDATE " + c.table + " SET " + columns.stream().map(x -> x + "=?").collect(Collectors.joining(","))
                            + " WHERE id=? AND version=?";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        int index = bind(statement, row, columns);
                        statement.setString(index++, Jsons.text(row, "id")); statement.setLong(index, Jsons.integer(row, "version") - 1);
                        if (statement.executeUpdate() != 1) throw new SQLException("Optimistic version conflict: " + c.table);
                    }
                }
            }
            connection.commit();
        } catch (SQLException | RuntimeException e) { connection.rollback(); throw e; }
    }
    public void renameDemoShop() throws SQLException {
        try (PreparedStatement s = connection.prepareStatement("UPDATE dim_shop SET name=?,version=version+1,updated_at=? WHERE id='S1'")) {
            s.setString(1, "杭州旗舰店（更新名称）");
            s.setString(2, java.time.LocalDateTime.now(java.time.ZoneId.of("Asia/Shanghai")).format(Jsons.TIME));
            s.executeUpdate(); connection.commit();
        } catch (SQLException e) { connection.rollback(); throw e; }
    }
    private void insert(String table, ObjectNode row, boolean ignoreDuplicate) throws SQLException {
        List<String> columns = columns(row);
        String sql = "INSERT " + (ignoreDuplicate ? "IGNORE " : "") + "INTO " + table + " (" + String.join(",", columns)
                + ") VALUES (" + columns.stream().map(x -> "?").collect(Collectors.joining(",")) + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, row, columns); int count = statement.executeUpdate();
            if (!ignoreDuplicate && count != 1) throw new SQLException("Insert row count mismatch");
        }
    }
    private static List<String> columns(ObjectNode row) {
        List<String> names = new ArrayList<>(); row.fieldNames().forEachRemaining(names::add);
        for (String name : names) if (!name.matches("[a-z_]+")) throw new IllegalArgumentException("Unsafe SQL column");
        return names;
    }
    private static int bind(PreparedStatement statement, ObjectNode row, List<String> columns) throws SQLException {
        int i = 1;
        for (String column : columns) {
            JsonNode v = row.get(column);
            if (v.isNull()) statement.setNull(i++, Types.NULL);
            else if (v.isIntegralNumber()) statement.setLong(i++, v.longValue());
            else statement.setString(i++, v.asText());
        }
        return i;
    }
    @Override public void close() throws SQLException { connection.close(); }
}
