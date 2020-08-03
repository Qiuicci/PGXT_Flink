package com.gree.constant;

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

    public static final String KAFKA_BROKER_LIST = "kafka.broker.list";
    public static final String KAFKA_HOST = "kafka.host";
    public static final String KAFKA_GROUP_ID = "banben1";
    public static final String KAFKA_AUTO_OFFSET_RESET = "latest";
    public static final boolean KAFKA_ENABLE_AUTO_COMMIT = true;
    public static final String ES_TYPE = "_doc";

    //安装


    //维修kafka消费的topic名称
    public static final String TBL_ASSIGN_TOPIC = "app_greeshservice.tbl_assign";
    public static final String TBL_ASSIGN_APPOINTMENT_TOPIC = "app_greeshservice.tbl_assign_appointment";
    public static final String TBL_ASSIGN_DAIJIAN_TOPIC = "app_greeshservice.tbl_assign_daijian";
    public static final String TBL_ASSIGN_MX_TOPIC = "app_greeshservice.tbl_assign_mx";
    public static final String TBL_ASSIGN_SATISFACTION_TOPIC = "app_greeshservice.tbl_assign_satisfaction";
    public static final String TBL_ASSIGN_FKMX_TOPIC = "app_greeshservice.tbl_assign_fkmx";
    public static final String TBL_ASSIGN_FEEDBACK_TOPIC = "app_greeshservice.tbl_assign_feedback";
    public static final String TBL_ASSIGN_XZYD_TOPIC ="app_greeshservice.tbl_assign_xzyd";

     //维修索引以及各个表的主键名称
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
