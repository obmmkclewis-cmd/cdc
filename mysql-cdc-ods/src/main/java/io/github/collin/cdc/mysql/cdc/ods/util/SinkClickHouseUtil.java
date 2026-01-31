package io.github.collin.cdc.mysql.cdc.ods.util;

import io.github.collin.cdc.common.constants.SchemaConstants;
import io.github.collin.cdc.mysql.cdc.common.constants.FieldConstants;
import io.github.collin.cdc.mysql.cdc.common.dto.ColumnMetaDataDTO;
import io.github.collin.cdc.mysql.cdc.ods.enums.MysqlType2ClickHouseMapping;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ClickHouse 建表与写入工具
 *
 * @author collin
 */
public class SinkClickHouseUtil {

    /**
     * 获取表主键字段名（用于 ORDER BY）
     */
    public static List<String> getPrimaryKeyNames(List<ColumnMetaDataDTO> columnMetaDatas, boolean isSharding) {
        LinkedHashSet<String> primaryKeyNames = null;
        if (isSharding) {
            primaryKeyNames = new LinkedHashSet<>();
            primaryKeyNames.add(FieldConstants.DB_NAME);
            primaryKeyNames.add(FieldConstants.TABLE_NAME);
        }
        for (ColumnMetaDataDTO columnMetaData : columnMetaDatas) {
            if (columnMetaData.isPrimaryKey()) {
                if (primaryKeyNames == null) {
                    primaryKeyNames = new LinkedHashSet<>();
                }
                primaryKeyNames.add(columnMetaData.getName());
            }
        }
        return primaryKeyNames == null ? new ArrayList<>() : new ArrayList<>(primaryKeyNames);
    }

    /**
     * 构建建表 DDL（ReplacingMergeTree，按 f_ts 去重）
     */
    public static String buildCreateTableSql(String database, String tableName, List<ColumnMetaDataDTO> columnMetaDatas,
            List<String> primaryKeyNames, boolean isSharding, String tableComment) {
        List<String> columnDefs = buildColumnDefs(columnMetaDatas, isSharding);
        String orderBy = primaryKeyNames.isEmpty() ? "tuple()"
                : primaryKeyNames.stream()
                        .collect(Collectors.joining(", "));
        String comment = StringUtils.isNotBlank(tableComment) ? " COMMENT '" + tableComment.replace("'", "\\'") + "'"
                : "";
        return String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) ENGINE = ReplacingMergeTree(f_ts) ORDER BY %s%s",
                database, tableName, String.join(", ", columnDefs), orderBy, comment);
    }

    /**
     * 构建列定义（含分表字段、业务列、f_ts、is_del）
     */
    public static List<String> buildColumnDefs(List<ColumnMetaDataDTO> columnMetaDatas, boolean isSharding) {
        List<String> defs = new ArrayList<>();
        if (isSharding) {
            defs.add("`" + FieldConstants.DB_NAME + "` String COMMENT '" + FieldConstants.COMMENT_DB_NAME + "'");
            defs.add("`" + FieldConstants.TABLE_NAME + "` String COMMENT '" + FieldConstants.COMMENT_TABLE_NAME + "'");
        }
        for (ColumnMetaDataDTO col : columnMetaDatas) {
            String chType = MysqlType2ClickHouseMapping.of(col.getMysqlType());
            String nullable = col.isNullable() ? " Nullable(" + chType + ")" : " " + chType;
            String comment = StringUtils.isNotBlank(col.getComment())
                    ? " COMMENT '" + col.getComment().replace("'", "\\'") + "'"
                    : "";
            defs.add("`" + col.getName() + "`" + nullable + comment);
        }
        defs.add("`" + SchemaConstants.SYNC_TS + "` Int64 COMMENT '同步时间戳'");
        defs.add("`" + SchemaConstants.IS_DEL + "` UInt8 DEFAULT 0 COMMENT '删除标记'");
        return defs;
    }

    /**
     * 转义表名/库名中的反引号
     */
    public static String escapeIdentifier(String name) {
        return name.replace("`", "\\`");
    }
}
