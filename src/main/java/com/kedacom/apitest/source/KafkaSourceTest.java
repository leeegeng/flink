package com.kedacom.apitest.source;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka数据源测试
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","172.16.64.85:9092");//多个的话可以指定
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 指定kafka的消费者从哪里开始消费数据
        // 共有三种方式，
        // #earliest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，从头开始消费
        // #latest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，消费新产生的该分区下的数据
        // #none
        // topic各分区都存在已提交的offset时，
        // 从offset后开始消费；
        // 只要有一个分区不存在已提交的offset，则抛出异常
        prop.setProperty("auto.offset.reset","latest");

        //设置checkpoint后在提交offset，即oncheckpoint模式
        // 该值默认为true，
        FlinkKafkaConsumer<String> devinfoConsumer = new FlinkKafkaConsumer<>("deviceinfo", new SimpleStringSchema(), prop);
        devinfoConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStream<String> deviceStream = env.addSource(new FlinkKafkaConsumer<String>("deviceinfo", new SimpleStringSchema(), prop));

        DataStream<DeviceInfo> map = deviceStream.map(dev -> {
            String[] fields = dev.split(",");
            return new DeviceInfo(fields[0], new Integer(fields[1]), new Integer(fields[2]), new Long(fields[3]));
        });

        deviceStream.print("devicd").setParallelism(2);

        map.print("map");

        env.execute("device job");
    }
}
