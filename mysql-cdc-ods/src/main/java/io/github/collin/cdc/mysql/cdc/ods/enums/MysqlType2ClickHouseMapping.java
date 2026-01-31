package io.github.collin.cdc.mysql.cdc.ods.enums;

import com.mysql.cj.MysqlType;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * MySQL 与 ClickHouse 类型映射
 *
 * @author collin
 */
@SuppressWarnings("AlibabaEnumConstantsMustHaveComment")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum MysqlType2ClickHouseMapping {

    DECIMAL(MysqlType.DECIMAL, "Decimal(38,9)"),
    DECIMAL_UNSIGNED(MysqlType.DECIMAL_UNSIGNED, "Decimal(38,9)"),
    TINYINT(MysqlType.TINYINT, "Int8"),
    TINYINT_UNSIGNED(MysqlType.TINYINT_UNSIGNED, "UInt8"),
    BOOLEAN(MysqlType.BOOLEAN, "UInt8"),
    SMALLINT(MysqlType.SMALLINT, "Int16"),
    SMALLINT_UNSIGNED(MysqlType.SMALLINT_UNSIGNED, "UInt16"),
    INT(MysqlType.INT, "Int32"),
    INT_UNSIGNED(MysqlType.INT_UNSIGNED, "UInt32"),
    MEDIUMINT(MysqlType.MEDIUMINT, "Int32"),
    MEDIUMINT_UNSIGNED(MysqlType.MEDIUMINT_UNSIGNED, "UInt32"),
    FLOAT(MysqlType.FLOAT, "Float32"),
    FLOAT_UNSIGNED(MysqlType.FLOAT_UNSIGNED, "Float32"),
    DOUBLE(MysqlType.DOUBLE, "Float64"),
    DOUBLE_UNSIGNED(MysqlType.DOUBLE_UNSIGNED, "Float64"),
    TIMESTAMP(MysqlType.TIMESTAMP, "DateTime64(3)"),
    DATETIME(MysqlType.DATETIME, "DateTime64(3)"),
    BIGINT(MysqlType.BIGINT, "Int64"),
    BIGINT_UNSIGNED(MysqlType.BIGINT_UNSIGNED, "UInt64"),
    DATE(MysqlType.DATE, "Date"),
    TIME(MysqlType.TIME, "String"),
    VARCHAR(MysqlType.VARCHAR, "String"),
    BIT(MysqlType.BIT, "UInt64"),
    TEXT(MysqlType.TEXT, "String"),
    TINYTEXT(MysqlType.TINYTEXT, "String"),
    MEDIUMTEXT(MysqlType.MEDIUMTEXT, "String"),
    LONGTEXT(MysqlType.LONGTEXT, "String"),
    CHAR(MysqlType.CHAR, "String"),
    BINARY(MysqlType.BINARY, "String"),
    BLOB(MysqlType.BLOB, "String"),
    TINYBLOB(MysqlType.TINYBLOB, "String"),
    MEDIUMBLOB(MysqlType.MEDIUMBLOB, "String"),
    LONGBLOB(MysqlType.LONGBLOB, "String");

    private final MysqlType mysqlType;
    private final String clickHouseType;

    public static String of(MysqlType mysqlType) {
        for (MysqlType2ClickHouseMapping value : MysqlType2ClickHouseMapping.values()) {
            if (value.mysqlType == mysqlType) {
                return value.clickHouseType;
            }
        }
        throw new UnsupportedOperationException(mysqlType.toString());
    }

    public static String of(String mysqlDataType) {
        for (MysqlType2ClickHouseMapping value : MysqlType2ClickHouseMapping.values()) {
            if (value.mysqlType.getName().equalsIgnoreCase(mysqlDataType)) {
                return value.clickHouseType;
            }
        }
        throw new UnsupportedOperationException(mysqlDataType);
    }
}
