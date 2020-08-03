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
    public static final String CLUSTER_NAME = "BI-elasticsearch";
    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final String ES_HOST100 = "10.2.13.100";
    public static final String ES_HOST101 = "10.2.13.101";
    public static final String ES_HOST102 = "10.2.13.102";
    //public static final String ES_HOST212 = "10.7.17.212";
    //public static final String ES_HOST213 = "10.7.17.213";
    //public static final String ES_HOST214 = "10.7.17.214";

    public static final String KAFKA_HOST = "kafka.host";
    public static final String KAFKA_GROUP_ID = "wxdata1";
    public static final String KAFKA_AUTO_OFFSET_RESET = "latest";
    public static final boolean KAFKA_ENABLE_AUTO_COMMIT = true;
    public static final String ES_TYPE = "_doc";


    //kafka的brokeR list
    public static final String KAFKA_HOST_LIST = "10.2.11.215:9092,10.2.11.214:9092,10.2.11.213:9092";


    //维修索引以及各个表的主键名称
    public static final String QUANXIANLEVEL_WANGDIANLEVEL_INDEX = "quanxianlevel_wangdianlevel_v1";
    public static final String WXKB_TIMEOOUT_ORDERS = "wxkb_timeout_orders_t1";
    public static final String WXKB_MAX_BIG_KUANGBIAO = "wxkb_max_big_kuanbiao_t1";
    public static final String TBL_ASSIGN_INDEX = "default_server_greeshservice_tbl_assign_t1";
    public static final String TBL_ASSIGN_QUERY_ID = "pgid";
    public static final String TBL_ASSIGN_SATISFACTION_INDEX = "default_server_greeshservice_tbl_assign_satisfaction_t1";
    public static final String TBL_ASSIGN_SATISFACTION_QUERY_ID ="id";
    public static final String TBL_ASSIGN_MX_INDEX = "default_server_greeshservice_tbl_assign_mx_t1";
    public static final String TBL_ASSIGN_MX_QUERY_ID ="pgmxid";
    public static final String TBL_ASSIGN_FKMX_INDEX = "default_server_greeshservice_tbl_assign_fkmx_t1";
    public static final String TBL_ASSIGN_FKMX_QUERY_ID = "fkid";
    public static final String TBL_ASSIGN_APPOINTMENT_INDEX = "default_server_greeshservice_tbl_assign_appointment_t1";
    public static final String TBL_ASSIGN_APPOINTMENT_QUERY_ID = "id";
    public static final String TBL_ASSIGN_XZYD_INDEX ="default_server_greeshservice_tbl_assign_xzyd_t1";
    public static final String TBL_ASSIGN_XZYD_QUERY_ID = "xzid";
    public static final String TBL_ASSIGN_FEEDBACK_INDEX = "default_server_greeshservice_tbl_assign_feedback_t1";
    public static final String TBL_ASSIGN_FEEDBACK_QUERY_ID = "id";
    public static final String TBL_ASSIGN_DAIJIAN_INDEX = "default_server_greeshservice_tbl_assign_daijian_t1";
    public static final String TBL_ASSIGN_DAIJIAN_QUERY_ID = "id";
}
