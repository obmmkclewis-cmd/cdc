package io.github.collin.cdc.common.adapter;

import io.github.collin.cdc.common.properties.RedisProperties;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

/**
 * redis工具类
 *
 * @author collin
 * @date 2023-05-06
 */
public class RedisAdapter {

    private RedissonClient redissonClient = null;

    public RedisAdapter(final RedisProperties redisProperties) {
        Config config = new Config();
        config.setCodec(new StringCodec());
        SingleServerConfig singleServerConfig = config.useSingleServer();
        String address;
        if (redisProperties.getAddr() != null && !redisProperties.getAddr().isEmpty()) {
            address = redisProperties.getAddr().startsWith("redis://") ? redisProperties.getAddr() : "redis://" + redisProperties.getAddr();
        } else {
            address = String.format("redis://%s:%s", redisProperties.getHost(), redisProperties.getPort());
        }
        int dbIndex = redisProperties.getDb() != 0 ? redisProperties.getDb() : redisProperties.getDatabase();
        singleServerConfig.setAddress(address)
                .setDatabase(dbIndex);
        // Only send AUTH when password is non-null and non-empty.
        // Empty password means no auth (for Redis instances without requirepass).
        String pwd = redisProperties.getPassword();
        if (pwd != null && !pwd.isEmpty()) {
            singleServerConfig.setPassword(pwd);
        }

        this.redissonClient = Redisson.create(config);
    }

    /**
     * 获取redis客户端实例
     *
     * @return
     */
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

}