package com.gree.util.esconnect;

import com.gree.constant.Constant;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * es的DBCP连接池工厂模式create by180557
 */
public class EsTransportFactory implements PooledObjectFactory<TransportClient> {


    /**
     * 生产对象
     */
    @Override
    public PooledObject<TransportClient> makeObject() throws Exception {
        TransportClient client = null;

        final Settings settings = Settings.builder()
                .put("cluster.name", Constant.CLUSTER_NAME)
                .build();
        try {
            client = new PreBuiltTransportClient(settings).addTransportAddresses(
                    new TransportAddress(InetAddress.getByName(Constant.ES_HOST100), Constant.ES_PROT),
                    new TransportAddress(InetAddress.getByName(Constant.ES_HOST101), Constant.ES_PROT),
                    new TransportAddress(InetAddress.getByName(Constant.ES_HOST102), Constant.ES_PROT)
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new DefaultPooledObject<TransportClient>(client);
    }

    /**
     * 销毁对象
     */

    @Override
    public void destroyObject(PooledObject<TransportClient> pooledObject) throws Exception {
        TransportClient transportClient = pooledObject.getObject();
        //System.out.println("关闭链接es。。。。");
        transportClient.close();

    }

    @Override
    public boolean validateObject(PooledObject<TransportClient> pooledObject) {
        TransportClient transportClient = pooledObject.getObject();
        //通过es链接的节点是否为空判断链接是否有效
        if (transportClient.connectedNodes().isEmpty()){
            return false;
        }else {
            return true;
        }
    }

    @Override
    public void activateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        //激活es客户端

    }

    @Override
    public void passivateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        //钝化es客户端
    }
}
