package com.kedacom.apitest.window;

import com.kedacom.apitest.source.SelfSourceTest;
import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WindowTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);


        DataStream<DeviceInfo> deviceStream = env.addSource(new DeviceSource());

        // 1 开窗测试，增量聚合
        SingleOutputStreamOperator<DeviceInfo> aggregate = deviceStream.keyBy(device -> device.getName()).
//                window(EventTimeSessionWindows.withGap(Time.seconds(30)));
//                countWindow(30);
        timeWindow(Time.seconds(5)).
//                    window(TumblingEventTimeWindows.of(Time.seconds(30))).
//                        min("carNum");
        aggregate(new AggregateFunction<DeviceInfo, DeviceInfo, DeviceInfo>() {

                @Override
                public DeviceInfo createAccumulator() {
                    return new DeviceInfo();
                }

                @Override
                public DeviceInfo add(DeviceInfo deviceInfo, DeviceInfo deviceInfo2) {
                    deviceInfo.setCarNum(deviceInfo.getCarNum() + deviceInfo2.getCarNum());
                    return deviceInfo;
                }

                @Override
                public DeviceInfo getResult(DeviceInfo deviceInfo) {
                    return deviceInfo;
                }

                @Override
                public DeviceInfo merge(DeviceInfo deviceInfo, DeviceInfo acc1) {
                    return null;
                }
            });
        deviceStream.print("data");
        aggregate.print("agg");

        // 2 全窗口测试
        SingleOutputStreamOperator<DeviceInfo> apply = deviceStream.keyBy(devic -> devic.getName())
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<DeviceInfo, DeviceInfo, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DeviceInfo> iterable, Collector<DeviceInfo> collector) throws Exception {
                        AtomicInteger total = new AtomicInteger();
                        AtomicReference<DeviceInfo> deviceInfo = new AtomicReference<>();
                        iterable.forEach(dev -> {
                            total.addAndGet(dev.getCarNum());
                            deviceInfo.set(dev);
                        });
                        if(deviceInfo != null){
                            deviceInfo.get().setCarNum(total.get());
                        }

                        collector.collect(deviceInfo.get());
                    }
                });
        apply.print("apply");

        env.execute("device job");
    }

    public static class DeviceSource implements SourceFunction<DeviceInfo> {

        private boolean running = true;

        @Override
        public void run(SourceContext<DeviceInfo> sourceContext) throws Exception {
            while (running) {
                Random random = new Random();

                Map<String, Integer> devsMap = new HashMap<>();
                for (int index = 0; index < 2; index++) {
                    devsMap.put("device_" + (index + 1), (int) random.nextInt(100));
                }
                for (String dev : devsMap.keySet()) {
                    int status = devsMap.get(dev) % 2 == 0 ? 0 : 1;
                    sourceContext.collect(new DeviceInfo(dev, status, devsMap.get(dev), Instant.now().getEpochSecond()));
                }
                TimeUnit.SECONDS.sleep(2);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
