package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public abstract class BaseSqlApi {

    public abstract void handle(StreamExecutionEnvironment env,
                                StreamTableEnvironment tEnv);

    public void start(String ckAndJobName){
        Configuration conf = new Configuration();
        conf.setString("pipeline.name", ckAndJobName);  // 给 job 设置名字

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(2);

        // 1. 设置状态后端: 1. hashmap(默认) 2. rocksdb
        env.setStateBackend(new HashMapStateBackend());
        // 2. 开启 checkpoint
        env.enableCheckpointing(36000);
        // 3. 设置状态的一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //4. 设置 checkpoint 存储的目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop2:8020/streampark/checkpoints/");
        //5. 设置 checkpoint 的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //6. 设置两个 checkpoint 之间的最小间隔. 如果这设置了, 则可以忽略setMaxConcurrentCheckpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(36000);
        //7. 设置 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(36000);
        //8. 当 job 被取消的时候, 存储从 checkpoint 的数据是否要删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //9. 开启非对齐检查点
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        handle(env,tEnv);


    }
}
