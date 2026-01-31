package io.github.collin.cdc.mysql.cdc.ods.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * ClickHouse 配置属性
 *
 * @author collin
 */
@Getter
@Setter
@ToString
public class ClickHouseProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主机地址
     */
    private String host;
    /**
     * HTTP 端口，默认 8123
     */
    private int port = 8123;
    /**
     * 原生 TCP 端口，默认 9000
     */
    private int nativePort = 9000;
    /**
     * 用户名
     */
    private String user;
    /**
     * 密码
     */
    private String password;
    /**
     * 数据库名
     */
    private String database = "default";
    /**
     * 集群名称（可选）
     */
    private String cluster;
    /**
     * 最大空闲连接数
     */
    private int maxIdleConns = 10;
    /**
     * 最大打开连接数
     */
    private int maxOpenConns = 100;
    /**
     * 连接最大存活时间，如 1h
     */
    private String connMaxLifetime = "1h";
    /**
     * 连接超时，如 5s
     */
    private String dialTimeout = "5s";
    /**
     * 读超时，如 300s
     */
    private String readTimeout = "300s";
    /**
     * 写超时，如 300s
     */
    private String writeTimeout = "300s";
    /**
     * 是否启用压缩
     */
    private boolean compression = false;

    /**
     * 构建 JDBC URL（HTTP 接口）
     */
    public String getJdbcUrl() {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", host, port, database);
        if (compression) {
            url += "?compress=1";
        }
        return url;
    }
}
