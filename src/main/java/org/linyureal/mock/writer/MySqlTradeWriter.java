package org.linyureal.mock.writer;

import com.fasterxml.jackson.databind.JsonNode;
import org.linyureal.config.AppConfig;
import org.linyureal.model.cdc.Change;
import org.linyureal.validation.TradeValidator;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Writes only generator-owned tables/columns. Each lifecycle step is an atomic DB transaction.
 * Reusing mock.run.id is intentionally rejected by primary-key INSERT constraints. */
public final class MySqlTradeWriter implements TradeWriter {
    private final Connection connection;
    public MySqlTradeWriter(AppConfig c) throws SQLException {
        connection=DriverManager.getConnection(c.get("mysql.url"),c.get("mysql.username"),c.secret("LINYUREAL_MYSQL_PASSWORD"));
        connection.setAutoCommit(false);
    }
    @Override public void write(List<Change> transaction) throws Exception {
        try {
            for(Change c:transaction) {
                TradeValidator.validate(c);
                List<String> columns=new ArrayList<>(); c.after.fieldNames().forEachRemaining(columns::add);
                for(String column:columns) if(!column.matches("[a-z_]+")) throw new IllegalArgumentException("Invalid column");
                boolean insert="c".equals(c.op);
                List<String> values=columns.stream().filter(x->insert || !x.equals("id")).collect(Collectors.toList());
                String sql=insert
                        ? "INSERT INTO "+c.table+" ("+String.join(",",values)+") VALUES ("+values.stream().map(x->"?").collect(Collectors.joining(","))+")"
                        : "UPDATE "+c.table+" SET "+values.stream().map(x->x+"=?").collect(Collectors.joining(","))+" WHERE id=? AND version<?";
                try(PreparedStatement stmt=connection.prepareStatement(sql)) {
                    int i=1;
                    for(String column:values) {
                        JsonNode v=c.after.get(column);
                        if(v.isNull()) stmt.setNull(i++,Types.VARCHAR);
                        else if(v.isIntegralNumber()) stmt.setLong(i++,v.longValue());
                        else stmt.setString(i++,v.asText());
                    }
                    if(!insert) { stmt.setString(i++,c.after.path("id").asText()); stmt.setLong(i,c.after.path("version").asLong()); }
                    if(stmt.executeUpdate()!=1) throw new SQLException("Unexpected row count for "+c.table);
                }
            }
            connection.commit();
        } catch(Exception e) { connection.rollback(); throw e; }
    }
    @Override public void close() throws SQLException { connection.close(); }
}
