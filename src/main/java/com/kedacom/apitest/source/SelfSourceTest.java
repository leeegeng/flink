package com.kedacom.apitest.source;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.xml.crypto.Data;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源测试
 */
public class SelfSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<DeviceInfo> device = env.addSource(new DeviceSource());

        DataStream<DeviceInfo> filterStream = device.
//                map( deviceInfo -> deviceInfo).
        filter(s -> s.getName().startsWith("device_1"));

        KeyedStream<DeviceInfo, String> keyedStream = device.
                filter(s -> s.getName().startsWith("device_1")).
                keyBy(deviceInfo -> deviceInfo.getName());
        DataStream<DeviceInfo> max = keyedStream.max("carNum");
        DataStream<DeviceInfo> sum = keyedStream.sum("carNum");

//        device.print("devicd").setParallelism(1);
        filterStream.print("filter");
//        keyedStream.print("keyby");
        max.print("max").setParallelism(1);
        sum.print("sum").setParallelism(1);


        env.execute("device job");
    }

    public static class DeviceSource implements SourceFunction<DeviceInfo> {

        private boolean running = true;

        @Override
        public void run(SourceContext<DeviceInfo> sourceContext) throws Exception {
            while (running) {
                Random random = new Random();

                Map<String, Integer> devsMap = new HashMap<>();
                for (int index = 0; index < 10; index++) {
                    devsMap.put("device_" + (index + 1), (int) random.nextInt(100));
                }
                for (String dev : devsMap.keySet()) {
                    int status = devsMap.get(dev) % 2 == 0 ? 0 : 1;
                    sourceContext.collect(new DeviceInfo(dev, status, devsMap.get(dev), Instant.now().getEpochSecond()));
                }
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
