package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String groupId, String topic){
        return KafkaSource.<String>builder()
                .setBootstrapServers("172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092")
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink.<String>builder()
                .setBootstrapServers("172.20.0.230:9092,172.20.0.238:9092,172.20.0.239:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                                         .setTopic(topic)
                                         .setValueSerializationSchema(new SimpleStringSchema())
                                         .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(topic + System.currentTimeMillis())
                .build();
    }
}
