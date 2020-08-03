package com.gree.constant;

import org.omg.CORBA.PUBLIC_MEMBER;

public class Constant {
    public static final int ES_PROT = 9300;
    public static final int ES_HTTP_PROT = 9200;
    public static final int ONE_HUNDRED = 5;
    public static final int FIVE = 5;
    public static final int TWO_THOUSAND = 1;

    public static final int KUAN_MAX_ACTIONS = 100;
    public static final int KUAN_MAZSIZE_MB = 5;
    public static final int KUAN_INTERVAL = 1000;

    public static final String ES_SCHEME = "http";
    public static final String CLUSTER_NAME = "gree_elasticsearch";
    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final String ES_HOST209="10.7.17.209";
    public static final String ES_HOST210="10.7.17.210";
    public static final String ES_HOST211="10.7.17.211";
    public static final String ES_HOST212="10.7.17.212";
    public static final String ES_HOST213="10.7.17.213";
    public static final String ES_HOST214="10.7.17.214";

    public static final String KAFKA_HOST = "kafka.host";
    public static final String KAFKA_GROUP_ID = "wxdatagroup1.0";
    public static final String KAFKA_AUTO_OFFSET_RESET = "latest";
    public static final boolean KAFKA_ENABLE_AUTO_COMMIT = true;
    public static final String ES_TYPE = "_doc";


    //kafka的brokeR list
    public static final String KAFKA_HOST_LIST = "10.7.17.206:9092,10.7.17.207:9092,10.7.17.208:9092";


    //维修索引以及各个表的主键名称
    public static final String QUANXIANLEVEL_WANGDIANLEVEL_INDEX = "quanxianlevel_wangdianlevel_v1";
    public static final String WXKB_TIMEOOUT_ORDERS = "wxkb_timeout_orders_v3";
    public static final String WXKB_MAX_BIG_KUANGBIAO = "wxkb_max_big_kuanbiao_v1";
    public static final String TBL_ASSIGN_INDEX = "default_server_greeshservice_tbl_assign_v1";
    public static final String TBL_ASSIGN_QUERY_ID = "pgid";
    public static final String TBL_ASSIGN_SATISFACTION_INDEX = "default_server_greeshservice_tbl_assign_satisfaction_v1";
    public static final String TBL_ASSIGN_SATISFACTION_QUERY_ID ="id";
    public static final String TBL_ASSIGN_MX_INDEX = "default_server_greeshservice_tbl_assign_mx_v1";
    public static final String TBL_ASSIGN_MX_QUERY_ID ="pgmxid";
    public static final String TBL_ASSIGN_FKMX_INDEX = "default_server_greeshservice_tbl_assign_fkmx_v1";
    public static final String TBL_ASSIGN_FKMX_QUERY_ID = "fkid";
    public static final String TBL_ASSIGN_APPOINTMENT_INDEX = "default_server_greeshservice_tbl_assign_appointment_v1";
    public static final String TBL_ASSIGN_APPOINTMENT_QUERY_ID = "id";
    public static final String TBL_ASSIGN_XZYD_INDEX ="default_server_greeshservice_tbl_assign_xzyd_v1";
    public static final String TBL_ASSIGN_XZYD_QUERY_ID = "xzid";
    public static final String TBL_ASSIGN_FEEDBACK_INDEX = "default_server_greeshservice_tbl_assign_feedback_v1";
    public static final String TBL_ASSIGN_FEEDBACK_QUERY_ID = "id";
    public static final String TBL_ASSIGN_DAIJIAN_INDEX = "default_server_greeshservice_tbl_assign_daijian_v1";
    public static final String TBL_ASSIGN_DAIJIAN_QUERY_ID = "id";
}
