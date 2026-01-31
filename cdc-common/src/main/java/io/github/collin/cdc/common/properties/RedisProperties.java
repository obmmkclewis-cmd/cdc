package io.github.collin.cdc.common.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * redis配置属性
 *
 * @author collin
 * @date 2023-05-06
 */
@Getter
@Setter
@ToString
public class RedisProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 地址，格式 host:port（与 host/port 二选一）
     */
    private String addr;
    private String host;
    private int port;
    private String password;
    /**
     * 库索引
     */
    private int database = 13;
    /**
     * 库索引（YAML 中可写 db，与 database 二选一）
     */
    private int db;
}