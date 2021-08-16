package com.kedacom.apitest.source;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<DeviceInfo> device = env.addSource(new DeviceSource());

        DataStream<DeviceInfo> reduce = device.
                filter(dev -> dev.getName().startsWith("device_2")).
                keyBy(dev -> dev.getName()).
                reduce((curStatus,newStatus) ->
                        new DeviceInfo(curStatus.getName(), newStatus.getStatus(), curStatus.getCarNum()+newStatus.getCarNum(), newStatus.getTime()));

        reduce.print("reduce");

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
