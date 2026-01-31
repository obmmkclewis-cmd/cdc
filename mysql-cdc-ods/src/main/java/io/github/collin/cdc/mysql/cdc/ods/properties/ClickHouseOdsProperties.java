package io.github.collin.cdc.mysql.cdc.ods.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ClickHouseOdsProperties extends AbstractOdsProperties {

    /**
     * ClickHouse 配置
     */
    private ClickHouseProperties clickhouse;
    /**
     * 是否开启清空表（建表前 DROP TABLE）
     */
    private Boolean openCleanTable;
}
