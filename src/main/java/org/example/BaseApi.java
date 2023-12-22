package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApi {
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void start(String ckAndGroupIdAndJobName, String topic) {


        Configuration conf = new Configuration();
        conf.setString("pipeline.name", ckAndGroupIdAndJobName);  // 给 job 设置名字

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        // 1. 设置状态后端: 1. hashmap(默认) 2. rocksdb
        env.setStateBackend(new HashMapStateBackend());
        // 2. 开启 checkpoint
        env.enableCheckpointing(60000);
        // 3. 设置状态的一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 4. 设置 checkpoint 存储的目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop2:8020/streampark/checkpoints/");
        // 5. 设置 checkpoint 的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 6. 设置两个 checkpoint 之间的最小间隔. 如果这设置了, 则可以忽略setMaxConcurrentCheckpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 7. 设置 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 8. 当 job 被取消的时候, 存储从 checkpoint 的数据是否要删除
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 9. 开启非对齐检查点
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        // env.getCheckpointConfig().setForceUnalignedCheckpoints(true);

        // 10. job 失败的时候重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        KafkaSource<String> source = KafkaUtil.getKafkaSource(ckAndGroupIdAndJobName, topic);
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");


        handle(env, stream);

        try{
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
