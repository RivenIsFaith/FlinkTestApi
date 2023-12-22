package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo1 extends BaseSqlApi{
    public static void main(String[] args) {
        new Demo1().start(
            "Demo1"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "create table kfk_xms_operation_t_mailitem(" +
                        "`database` string, " +
                        "`table` string, " +
                        "`type` string, " +
                        "`data` map<string, string>, " +
                        "`old` map<string, string>, " +
                        "`ts` bigint," +
                        " `pt` as proctime()," +
                        " et as to_timestamp_ltz(ts, 0)," +
                        " watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaSourceSQL("test0",
                        "maxwell_xms_operation_t_mailitem")
        );


        //1.读取maxwell_xms_operation_t_mailitem

        Table dwd_xms_operation_t_mailitem = tEnv.sqlQuery(
                "select "+
                "data['bs_id'] bs_id,"+
                "data['fpx_track_no'] fpx_track_no,"+
                "data['customer_hawb_code'] customer_hawb_code,"+
                "data['service_hawb_code'] service_hawb_code,"+
                "data['channel_hawb_code'] channel_hawb_code,"+
                "data['system_order_code'] system_order_code,"+
                "data['hold_sign'] hold_sign,"+
                "data['serviceno_change_sign'] serviceno_change_sign,"+
                "data['pk_code'] pk_code,"+
                "data['chn_code'] chn_code,"+
                "data['is_battery'] is_battery,"+
                "data['cm_id'] cm_id,"+
                "data['cargo_type'] cargo_type,"+
                "data['is_dangerous_cargo'] is_dangerous_cargo,"+
                "data['is_return'] is_return,"+
                "data['is_arrears'] is_arrears,"+
                "data['is_payment'] is_payment,"+
                "data['os_code'] os_code,"+
                "data['is_declare'] is_declare,"+
                "data['vat_number'] vat_number,"+
                "data['total_value'] total_value,"+
                "data['gross_weight'] gross_weight,"+
                "data['charge_weight'] charge_weight,"+
                "data['volume_weight'] volume_weight,"+
                "data['customer_weight'] customer_weight,"+
                "data['revise_weight'] revise_weight,"+
                "data['ds_code'] ds_code,"+
                "data['warehouse_code'] warehouse_code,"+
                "data['biz_code'] biz_code,"+
                "data['is_code'] is_code,"+
                "data['route_count'] route_count,"+
                "data['arrival_mode'] arrival_mode,"+
                "data['actual_arrival_mode'] actual_arrival_mode,"+
                "data['created_time'] created_time,"+
                "data['last_modified_time'] last_modified_time,"+
                "data['last_modified_by'] last_modified_by,"+
                "data['parcel_source'] parcel_source,"+
                "data['chn_lock'] chn_lock,"+
                "data['sale_pk_code'] sale_pk_code,"+
                "data['oda_confirm'] oda_confirm,"+
                "data['business_type'] business_type,"+
                "data['extension'] extension,"+
                "data['version_num'] version_num "+
                "from kfk_xms_operation_t_mailitem");


        tEnv.createTemporaryView("dwd_xms_operation_t_mailitem",dwd_xms_operation_t_mailitem);

        tEnv.executeSql(
                "create table kfk_xms_operation_t_mailitem_shipper_consignee(" +
                        "`database` string, " +
                        "`table` string, " +
                        "`type` string, " +
                        "`data` map<string, string>, " +
                        "`old` map<string, string>, " +
                        "`ts` bigint," +
                        " `pt` as proctime()," +
                        " et as to_timestamp_ltz(ts, 0)," +
                        " watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaSourceSQL("test0",
                        "maxwell_xms_operation_t_mailitem_shipper_consignee")
        );


        //2.读取xms_operation_t_mailitem_shipper_consignee
        Table dwd_xms_operation_t_mailitem_shipper_consignee = tEnv.sqlQuery(
                "select "+
                        "data['id'] id,"+
                        "data['buyer_id'] buyer_id,"+
                        "data['seller_id'] seller_id,"+
                        "data['ct_code_initial'] ct_code_initial,"+
                        "data['ct_code_destination'] ct_code_destination,"+
                        "data['shipper_name'] shipper_name,"+
                        "data['shipper_company'] shipper_company,"+
                        "data['shipper_province'] shipper_province,"+
                        "data['shipper_city'] shipper_city,"+
                        "data['shipper_county'] shipper_county,"+
                        "data['shipper_address1'] shipper_address1,"+
                        "data['shipper_address2'] shipper_address2,"+
                        "data['shipper_address3'] shipper_address3,"+
                        "data['shipper_post_code'] shipper_post_code,"+
                        "data['shipper_tel1'] shipper_tel1,"+
                        "data['shipper_tel2'] shipper_tel2,"+
                        "data['consignee_name'] consignee_name,"+
                        "data['consignee_company'] consignee_company,"+
                        "data['consignee_province'] consignee_province,"+
                        "data['consignee_city'] consignee_city,"+
                        "data['consignee_county'] consignee_county,"+
                        "data['consignee_address1'] consignee_address1,"+
                        "data['consignee_address2'] consignee_address2,"+
                        "data['consignee_address3'] consignee_address3,"+
                        "data['consignee_post_code'] consignee_post_code,"+
                        "data['consignee_tel1'] consignee_tel1,"+
                        "data['consignee_tel2'] consignee_tel2,"+
                        "data['consignee_vatno'] consignee_vatno,"+
                        "data['eori_code'] eori_code,"+
                        "data['bs_id'] bs_id,"+
                        "data['created_time'] created_time,"+
                        "data['last_modified_time'] last_modified_time,"+
                        "data['created_by'] created_by,"+
                        "data['last_modified_by'] last_modified_by,"+
                        "data['shipper_email'] shipper_email,"+
                        "data['consignee_email'] consignee_email,"+
                        "data['shipper_vatno'] shipper_vatno,"+
                        "data['consignee_identify_code'] consignee_identify_code,"+
                        "data['shipper_identify_code'] shipper_identify_code "+
                        "from kfk_xms_operation_t_mailitem_shipper_consignee"
        );
        tEnv.createTemporaryView("dwd_xms_operation_t_mailitem_shipper_consignee",dwd_xms_operation_t_mailitem_shipper_consignee);

        tEnv.executeSql(
                "create table kfk_xms_operation_t_mailitem_extend(" +
                        "`database` string, " +
                        "`table` string, " +
                        "`type` string, " +
                        "`data` map<string, string>, " +
                        "`old` map<string, string>, " +
                        "`ts` bigint," +
                        " `pt` as proctime()," +
                        " et as to_timestamp_ltz(ts, 0)," +
                        " watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaSourceSQL("test3",
                        "maxwell_xms_operation_t_mailitem_extend")
        );

//
//        //3.maxwell_xms_operation_t_mailitem_extend
        Table dwd_xms_operation_t_mailitem_extend = tEnv.sqlQuery(
                "select "+
                        "data['bs_id'] bs_id,"+
                        "data['sort_code'] sort_code,"+
                        "data['low_declaration'] low_declaration,"+
                        "data['thickness_flag'] thickness_flag,"+
                        "data['in_warehouse_operator'] in_warehouse_operator,"+
                        "data['in_warehouse_og_code'] in_warehouse_og_code,"+
                        "data['in_warehouse_time'] in_warehouse_time,"+
                        "data['in_warehouse_code'] in_warehouse_code,"+
                        "data['sub_account_id'] sub_account_id,"+
                        "data['parcelinspect'] parcelinspect,"+
                        "data['sorting_partition_id'] sorting_partition_id,"+
                        "data['port_code'] port_code,"+
                        "data['cod_amount'] cod_amount,"+
                        "data['cod_currency'] cod_currency,"+
                        "data['distribution_type'] distribution_type,"+
                        "data['distributor_code'] distributor_code,"+
                        "data['ioss_no'] ioss_no,"+
                        "data['freight_stringges'] freight_stringges,"+
                        "data['declare_insurance'] declare_insurance,"+
                        "data['currency_customs'] currency_customs,"+
                        "data['exchange_rate_customs'] exchange_rate_customs,"+
                        "data['current_cp_rescode'] current_cp_rescode,"+
                        "data['logistics_order_createtime'] logistics_order_createtime,"+
                        "data['is_security_check'] is_security_check,"+
                        "data['dhlec_status'] dhlec_status,"+
                        "data['is_bws'] is_bws,"+
                        "data['bws_scene'] bws_scene,"+
                        "data['ec_code'] ec_code,"+
                        "data['declare_service'] declare_service,"+
                        "data['station_name'] station_name,"+
                        "data['station_code'] station_code,"+
                        "data['lucid_no'] lucid_no,"+
                        "data['lane_code'] lane_code,"+
                        "data['customer_stringge_weight'] customer_stringge_weight,"+
                        "data['customer_volume_weight'] customer_volume_weight,"+
                        "data['lvg_paid'] lvg_paid "+
                        "from kfk_xms_operation_t_mailitem_extend"
        );
        tEnv.createTemporaryView("dwd_xms_operation_t_mailitem_extend",dwd_xms_operation_t_mailitem_extend);

        //join
        Table result  = tEnv.sqlQuery(
                "select "+
                "a1.bs_id"+
                ",a1.fpx_track_no"+
                ",a1.customer_hawb_code"+
                ",a1.service_hawb_code"+
                ",a1.channel_hawb_code"+
                ",a1.system_order_code"+
                ",a1.cm_id"+
                ",a1.route_count"+
                ",a1.hold_sign"+
                ",a1.cargo_type"+
                ",a1.os_code"+
                ",a1.is_battery"+
                ",a1.is_dangerous_cargo"+
                ",a1.is_return"+
                ",a1.is_payment"+
                ",a1.is_declare"+
                ",a1.ds_code"+
                ",a1.warehouse_code"+
                ",a1.biz_code"+
                ",a1.is_code"+
                ",a1.arrival_mode"+
                ",a1.actual_arrival_mode"+
                ",a1.parcel_source"+
                ",cast(a1.total_value as decimal(16,4))/100 as total_value"+
                ",cast(a1.gross_weight as decimal(16,4))/1000 as gross_weight"+
                ",cast(a1.charge_weight as decimal(16,4))/1000 as charge_weight"+
                ",cast(a1.volume_weight as decimal(16,4))/1000 as volume_weight"+
                ",cast(a1.customer_weight as decimal(16,4))/1000 as customer_weight"+
                ",cast(a1.revise_weight as decimal(16,4))/1000 as revise_weight"+
                ",a1.created_time"+
                ",a2.ct_code_initial"+
                ",a2.ct_code_destination"+
                ",a2.buyer_id"+
                ",a2.seller_id"+
                ",a2.consignee_province"+
                ",a2.consignee_city"+
                ",a2.consignee_post_code"+
                ",a2.consignee_email"+
                ",a3.is_security_check"+
                ",a3.parcelinspect"+
                ",a3.in_warehouse_time"+
                ",a3.in_warehouse_og_code"+
                ",a3.in_warehouse_operator"+
                ",a3.sort_code"+
                " from dwd_xms_operation_t_mailitem a1"+
                " left join dwd_xms_operation_t_mailitem_shipper_consignee a2"+
                " on a1.bs_id = a2.bs_id"+
                " left join dwd_xms_operation_t_mailitem_extend a3"+
                " on a1.bs_id = a3.bs_id"
        );

//        result.execute().print();

        tEnv.executeSql(
"                create table dwd_fpx_lgt_did_order_pkg_info("+
"                        bs_id                      string,"+
"                        fpx_track_no               string,"+
"                        customer_hawb_code         string,"+
"                        service_hawb_code          string,"+
"                        channel_hawb_code          string,"+
"                        system_order_code          string,"+
"                        cm_id                      string,"+
"                        route_count                string,"+
"                        hold_sign                  string,"+
"                        cargo_type                 string,"+
"                        os_code                    string,"+
"                        is_battery                 string,"+
"                        is_dangerous_cargo         string,"+
"                        is_return                  string,"+
"                        is_payment                 string,"+
"                        is_declare                 string,"+
"                        ds_code                    string,"+
"                        warehouse_code             string,"+
"                        biz_code                   string,"+
"                        is_code                    string,"+
"                        arrival_mode               string,"+
"                        actual_arrival_mode        string,"+
"                        parcel_source              string,"+
"                        total_value                decimal(16,4),"+
"                        gross_weight               decimal(16,4),"+
"                        charge_weight              decimal(16,4),"+
"                        volume_weight              decimal(16,4),"+
"                        customer_weight            decimal(16,4),"+
"                        revise_weight              decimal(16,4),"+
"                        created_time			    string,"+
"                        ct_code_initial            string,"+
"                        ct_code_destination        string,"+
"                        buyer_id                   string,"+
"                        seller_id                  string,"+
"                        consignee_province         string,"+
"                        consignee_city             string,"+
"                        consignee_post_code        string,"+
"                        consignee_email            string,"+
"                        is_security_check		   string,"+
"                        parcelinspect			   string,"+
"                       in_warehouse_time		   string,"+
"                       in_warehouse_og_code	   string,"+
"                       in_warehouse_operator	   string,"+
"                       sort_code 				   string,"+
"                PRIMARY KEY (bs_id) NOT ENFORCED"+
") with ("+
"                'connector' = 'upsert-kafka',"+
"                'topic' = 'test_demo1_dwd_fpx_lgt_did_order_pkg_info',"+
"                'properties.bootstrap.servers' = '172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092',"+
"                'key.format' = 'json',"+
"                'value.format' = 'json'"+
"        );"
        );

        result.executeInsert("dwd_fpx_lgt_did_order_pkg_info");
    }
}
