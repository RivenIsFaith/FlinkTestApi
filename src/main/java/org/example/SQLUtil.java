package org.example;

public class SQLUtil {
    public static String getKafkaSourceSQL(
            String groupId,
            String topic,
            String... format
    ){
        String defaultFormat = "json";
        if(format.length > 0){
            defaultFormat = format[0];
        }

        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                ("json".equals(defaultFormat) ? " 'json.ignore-parse-errors' = 'true', " : "") +
                "  'format' = '" + defaultFormat + "'" +
                ")";
    }


    public static String getKafkaSinkSQL(String topic,String... format) {
        String defaultFormat = "json";
        if (format.length > 0) {
            defaultFormat = format[0];
        }
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092'" +
                "  'format' = '" + defaultFormat + "'" +
                ")";
    }

    public static String getUpsertKafkaSQL(String topic, String... format) {

        String defaultFormat = "json";
        if (format.length > 0) {
            defaultFormat = format[0];
        }

        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092'" +
                ("json".equals(defaultFormat) ? " 'key.json.ignore-parse-errors' = 'true', " : "") +
                ("json".equals(defaultFormat) ? " 'value.json.ignore-parse-errors' = 'true', " : "") +
                "  'key.format' = '" + defaultFormat + "', " +
                "  'value.format' = '" + defaultFormat + "'" +
                ")";
    }
}
