package com.gree.util;

import com.gree.util.esconnect.EsTransportFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.transport.TransportClient;

public class ESTransportPoolUtil {
    // 对象池配置类，不写也可以，采用默认配置
    private static GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

    // 采用默认配置maxTotal是8，池中有8个client
    static {
        poolConfig.setMaxTotal(6);
        poolConfig.setMinIdle(3);
        poolConfig.setMinEvictableIdleTimeMillis(60000);//设置超时时间
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);//检查超时的间隔

    }

    // 要池化的对象的工厂类，这个是我们要实现的类
    private static EsTransportFactory esClientPoolFactory = new EsTransportFactory();
    // 利用对象工厂类和配置类生成对象池
    private static GenericObjectPool<TransportClient> clientPool = new GenericObjectPool<>(esClientPoolFactory,
            poolConfig);

    /**
     * 获得对象
     *
     * @return
     * @throws Exception
     */
    public static TransportClient getClient() throws Exception {
        // 从池中取一个对象
        TransportClient client = clientPool.borrowObject();
        return client;
    }

    /**
     * 归还对象
     *
     * @param client
     */
    public static void returnClient(TransportClient client) {
        // 使用完毕之后，归还对象
        clientPool.returnObject(client);
    }
}
