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

public class EsTransportFactory implements PooledObjectFactory<TransportClient> {

    public static TransportAddress HOST209;
    public static TransportAddress HOST210;
    public static TransportAddress HOST211;
    public static TransportAddress HOST212;
    public static TransportAddress HOST213;
    public static TransportAddress HOST214;
    static {
        try {
            HOST209 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST209), Constant.ES_PROT);
            HOST210 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST210), Constant.ES_PROT);
            HOST211 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST211), Constant.ES_PROT);
            HOST212 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST212), Constant.ES_PROT);
            HOST213 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST213), Constant.ES_PROT);
            HOST214 = new TransportAddress(InetAddress.getByName(Constant.ES_HOST214), Constant.ES_PROT);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


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
        client = new PreBuiltTransportClient(settings)
                /*.addTransportAddresses(
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST209), Constant.ES_PROT),
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST210), Constant.ES_PROT),
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST211), Constant.ES_PROT),
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST212), Constant.ES_PROT),
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST213), Constant.ES_PROT),
                        new TransportAddress(InetAddress.getByName(Constant.ES_HOST214), Constant.ES_PROT)
                )*/;
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
        transportClient.close();

    }

    @Override
    public boolean validateObject(PooledObject<TransportClient> pooledObject) {
        TransportClient transportClient = pooledObject.getObject();
        if (transportClient.listedNodes().isEmpty()){
            return false;
        }else {
            return true;
        }
    }

    @Override
    public void activateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        TransportClient transportClient = pooledObject.getObject();
        transportClient.addTransportAddresses(
                HOST209,
                HOST210,
                HOST211,
                HOST212,
                HOST213,
                HOST214
        );
        //System.out.println("activateObject");

    }

    @Override
    public void passivateObject(PooledObject<TransportClient> pooledObject) throws Exception {
        TransportClient transportClient = pooledObject.getObject();
        transportClient.removeTransportAddress(HOST209);
        transportClient.removeTransportAddress(HOST210);
        transportClient.removeTransportAddress(HOST211);
        transportClient.removeTransportAddress(HOST212);
        transportClient.removeTransportAddress(HOST213);
        transportClient.removeTransportAddress(HOST214);
        //System.out.println("passivateObject");

    }
}
