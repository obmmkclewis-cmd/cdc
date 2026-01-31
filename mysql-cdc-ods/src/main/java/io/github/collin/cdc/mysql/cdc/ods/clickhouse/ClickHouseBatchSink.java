package io.github.collin.cdc.mysql.cdc.ods.clickhouse;

import io.github.collin.cdc.mysql.cdc.ods.properties.ClickHouseProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 批量写入 ClickHouse（JSONEachRow）
 *
 * @author collin
 */
@Slf4j
public class ClickHouseBatchSink extends RichSinkFunction<String> {

    private static final int BATCH_SIZE = 500;
    private static final long FLUSH_INTERVAL_MS = 2000;

    private final ClickHouseProperties properties;
    private final String database;
    private final String tableName;

    private transient List<String> buffer;
    private transient long lastFlushTime;
    private transient Connection connection;

    public ClickHouseBatchSink(ClickHouseProperties properties, String database, String tableName) {
        this.properties = properties;
        this.database = database;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        this.buffer = new ArrayList<>(BATCH_SIZE);
        this.lastFlushTime = System.currentTimeMillis();
        this.connection = DriverManager.getConnection(
                properties.getJdbcUrl(),
                properties.getUser(),
                properties.getPassword() != null ? properties.getPassword() : ""
        );
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (StringUtils.isBlank(value)) {
            return;
        }
        synchronized (buffer) {
            buffer.add(value);
            long now = System.currentTimeMillis();
            if (buffer.size() >= BATCH_SIZE || (now - lastFlushTime) >= FLUSH_INTERVAL_MS) {
                flush();
                lastFlushTime = now;
            }
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<String> toFlush = new ArrayList<>(buffer);
        buffer.clear();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO `").append(database).append("`.`").append(tableName).append("` FORMAT JSONEachRow\n");
            for (String json : toFlush) {
                sb.append(json).append("\n");
            }
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sb.toString());
            }
        } catch (Exception e) {
            log.error("ClickHouse batch insert failed, db={}, table={}, size={}", database, tableName, toFlush.size(), e);
            for (String json : toFlush) {
                try {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("INSERT INTO `" + database + "`.`" + tableName + "` FORMAT JSONEachRow\n" + json);
                    }
                } catch (Exception ex) {
                    log.error("ClickHouse single insert failed: {}", json, ex);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (buffer != null && !buffer.isEmpty()) {
            flush();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("Close ClickHouse connection failed", e);
            }
        }
        super.close();
    }
}
