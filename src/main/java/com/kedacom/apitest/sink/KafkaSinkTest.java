package com.kedacom.apitest.sink;

import com.kedacom.apitest.source.SelfSourceTest;
import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<DeviceInfo> device = env.addSource(new DeviceSource());

        DataStream<String> transformStream = device.map(dev -> dev.toString());

        transformStream.addSink(new FlinkKafkaProducer<String>("172.16.64.85:9092", "deviceout", new SimpleStringSchema()));

        transformStream.print();

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
