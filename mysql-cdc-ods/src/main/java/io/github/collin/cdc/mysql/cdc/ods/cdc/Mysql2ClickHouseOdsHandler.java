package io.github.collin.cdc.mysql.cdc.ods.cdc;

import io.github.collin.cdc.common.constants.CdcConstants;
import io.github.collin.cdc.common.enums.Env;
import io.github.collin.cdc.common.enums.Namespaces;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.common.dto.RowJson;
import io.github.collin.cdc.mysql.cdc.common.dto.TableDTO;
import io.github.collin.cdc.mysql.cdc.common.util.DbCommonUtil;
import io.github.collin.cdc.mysql.cdc.ods.cache.OutputTagCache;
import io.github.collin.cdc.mysql.cdc.ods.clickhouse.ClickHouseBatchSink;
import io.github.collin.cdc.mysql.cdc.ods.clickhouse.ClickHouseRowToJsonProcess;
import io.github.collin.cdc.mysql.cdc.ods.exception.PrimaryKeyStateException;
import io.github.collin.cdc.mysql.cdc.ods.properties.ClickHouseOdsProperties;
import io.github.collin.cdc.mysql.cdc.ods.properties.ClickHouseProperties;
import io.github.collin.cdc.mysql.cdc.ods.util.SinkClickHouseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.admin.AdminClient;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

/**
 * MySQL CDC 同步到 ClickHouse（ODS 层）
 *
 * @author collin
 */
@Slf4j
public class Mysql2ClickHouseOdsHandler extends AbstractMysqlCdcHandler<ClickHouseOdsProperties> {

    private final ClickHouseProperties clickHouseProperties;

    public Mysql2ClickHouseOdsHandler(ClickHouseOdsProperties odsProperties) throws IOException {
        super(odsProperties);
        this.clickHouseProperties = odsProperties.getClickhouse();
        if (this.clickHouseProperties == null) {
            throw new IllegalArgumentException("clickhouse config is required");
        }
    }

    @Override
    protected java.util.function.BiFunction<Env, String, Void> buildCreateDatabaseFunction() {
        return (env, dbNameTarget) -> {
            String database = Namespaces.getOdsPre(env) + dbNameTarget;
            String jdbcUrl = clickHouseProperties.getJdbcUrl();
            String baseUrl = jdbcUrl.contains("/" + clickHouseProperties.getDatabase())
                    ? jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"))
                    : jdbcUrl.replaceAll("/[^/]+$", "");
            log.info("准备创建ClickHouse数据库: {}, 连接URL: {}", database, baseUrl + "/default");
            try (Connection conn = DriverManager.getConnection(
                    baseUrl + "/default",
                    clickHouseProperties.getUser(),
                    clickHouseProperties.getPassword() != null ? clickHouseProperties.getPassword() : "")) {
                try (Statement stmt = conn.createStatement()) {
                    String createDbSql = "CREATE DATABASE IF NOT EXISTS `" + database + "`";
                    log.info("执行创建数据库SQL: {}", createDbSql);
                    stmt.execute(createDbSql);
                    log.info("成功创建数据库: {}", database);
                }
            } catch (Exception e) {
                log.error("创建ClickHouse数据库失败: {}", database, e);
                throw new RuntimeException("Create ClickHouse database failed: " + database, e);
            }
            return null;
        };
    }

    @Override
    protected void createTablesAndSink(Map<String, TableDTO> availableTables, Connection connection,
            SingleOutputStreamOperator<RowJson> wholeStream,
            ClickHouseOdsProperties odsProperties, AdminClient adminClient, Set<String> topics) throws Exception {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC driver not found", e);
        }
        String jdbcUrl = clickHouseProperties.getJdbcUrl();
        Set<String> processedTables = new HashSet<>();

        for (Map.Entry<String, TableDTO> entry : availableTables.entrySet()) {
            TableDTO tableDTO = entry.getValue();
            String targetDbNameAndTable = tableDTO.getDbName() + CdcConstants.DOT + tableDTO.getTableName();
            if (!processedTables.add(targetDbNameAndTable)) {
                continue;
            }

            //String targetDbName = Namespaces.getOdsPre(odsProperties.getEnv()) + tableDTO.getDbName();
            String targetDbName = "default";
            String[] keyParts = entry.getKey().split(CdcConstants.ESCAPE_DOT);
            String sourceDbName = keyParts[0];
            String sourceTableName = keyParts[1];

            try (Connection chConn = DriverManager.getConnection(jdbcUrl, clickHouseProperties.getUser(),
                    clickHouseProperties.getPassword() != null ? clickHouseProperties.getPassword() : "")) {
                log.info("成功连接到ClickHouse: {}", jdbcUrl);

                List<ColumnMetaDataDTO> columnMetaDatas = DbCommonUtil.getTableColumnMetaDatas(connection, sourceDbName,
                        sourceTableName);
                List<String> primaryKeyNames = SinkClickHouseUtil.getPrimaryKeyNames(columnMetaDatas,
                        tableDTO.isSharding());
                if (primaryKeyNames == null || primaryKeyNames.isEmpty()) {
                    throw new PrimaryKeyStateException(String.format("[%s.%s] primaryKeyNames is null",
                            tableDTO.getDbName(), tableDTO.getTableName()));
                }
                log.info("表 {}.{} 的主键: {}", sourceDbName, sourceTableName, primaryKeyNames);

                if (Boolean.TRUE.equals(odsProperties.getOpenCleanTable())) {
                    String dropSql = "DROP TABLE IF EXISTS `" + targetDbName + "`.`" + tableDTO.getTableName() + "`";
                    log.info("准备删除已存在的表: {}", dropSql);
                    try (Statement stmt = chConn.createStatement()) {
                        stmt.execute(dropSql);
                        log.info("成功删除表: {}.{}", targetDbName, tableDTO.getTableName());
                    }
                }

                String tableComment = DbCommonUtil.getTablesComment(connection, sourceDbName, sourceTableName);
                String createSql = SinkClickHouseUtil.buildCreateTableSql(
                        targetDbName, tableDTO.getTableName(), columnMetaDatas, primaryKeyNames, tableDTO.isSharding(),
                        tableComment);
                log.info("准备执行建表SQL: {}", createSql);
                try (Statement stmt = chConn.createStatement()) {
                    stmt.execute(createSql);
                    log.info("成功创建表: {}.{}", targetDbName, tableDTO.getTableName());
                } catch (Exception e) {
                    log.error("建表失败 - 数据库: {}, 表: {}, SQL: {}", targetDbName, tableDTO.getTableName(), createSql, e);
                    // 打印更详细的错误信息
                    if (e.getCause() != null) {
                        log.error("错误原因: {}", e.getCause().getMessage(), e.getCause());
                    }
                    throw e;
                }
            } catch (Exception e) {
                log.error("处理表 {}.{} 时发生错误", sourceDbName, sourceTableName, e);
                throw e;
            }

            OutputTag<RowJson> outputTag = OutputTagCache.getOutputTag(tableDTO.getDbName(), tableDTO.getTableName());
            DataStream<RowJson> tableStream = wholeStream.getSideOutput(outputTag).rebalance();
            DataStream<String> jsonStream = tableStream
                    .process(new ClickHouseRowToJsonProcess(tableDTO.isSharding()))
                    .uid(targetDbNameAndTable + " clickhouse convert")
                    .name(targetDbNameAndTable + " clickhouse convert");

            jsonStream
                    .addSink(new ClickHouseBatchSink(clickHouseProperties, targetDbName, tableDTO.getTableName()))
                    .setParallelism(odsProperties.getParallelism().getWrite())
                    .name(targetDbNameAndTable + " clickhouse sink");
        }
    }
}
