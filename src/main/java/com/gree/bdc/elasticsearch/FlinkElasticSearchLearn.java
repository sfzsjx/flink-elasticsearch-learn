package com.gree.bdc.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.gree.bdc.util.InitFlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * flink 读取kafka数据写入elasticsearch
 * @author 260164
 * @date 2020-07-16
 */
public class FlinkElasticSearchLearn {
    public static void main(String[] args) {
        //①配置环境
        StreamExecutionEnvironment env = InitFlinkUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //②读取kafka数据源
        Properties properties = new Properties();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String paramProperties = parameterTool.get("paramProperties");
        ///加载类路径
        try {
            properties.load(new FileInputStream(paramProperties));
        } catch (IOException e) {
            e.printStackTrace();
        }

        FlinkKafkaConsumer<String> kafkaTopicDevice =
                new FlinkKafkaConsumer<>(properties.getProperty("topic"), new SimpleStringSchema(), properties);

        kafkaTopicDevice.setStartFromEarliest();

        DataStreamSource<String> sourceStream = env.addSource(kafkaTopicDevice);


        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200,"http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts, (ElasticsearchSinkFunction<String>) (location, runtimeContext, requestIndexer) -> {
                    // 数据保存在Elasticsearch中名称为index_customer的索引中，保存的类型名称为type_customer
                    requestIndexer.add(Requests.indexRequest().index("index_location").type("type_location")
                            .source(location, XContentType.JSON));
                });

        //设置批量写入缓存区大小
        esSinkBuilder.setBulkFlushMaxActions(2);

        //把转换后的数据写入es
        sourceStream.addSink(esSinkBuilder.build());
        try {
            env.execute("application.name");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
