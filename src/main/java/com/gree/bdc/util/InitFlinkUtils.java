package com.gree.bdc.util;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink 初始化工具类
 * @author hadoop
 * @date 202-03-27
 */
public class InitFlinkUtils {
    public static StreamExecutionEnvironment getEnv(){
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
       // env.enableCheckpointing(5*60*1000, CheckpointingMode.AT_LEAST_ONCE);
        return env;
    }
}
