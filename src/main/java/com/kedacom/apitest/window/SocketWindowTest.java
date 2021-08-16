package com.kedacom.apitest.window;

import com.kedacom.pojo.CarNumCount;
import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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

public class SocketWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 设置socket信息
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
        String host = "172.16.64.85";
        int port = 8888;


        // 2 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        DataStream<DeviceInfo> mapStream = inputDataStream.map(new MapFunction<String, DeviceInfo>() {
            @Override
            public DeviceInfo map(String s) throws Exception {
                String[] fields = s.split(",");
                return new DeviceInfo(fields[0], new Integer(fields[1]), new Integer(fields[2]), new Long(fields[3]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = mapStream
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<DeviceInfo>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(DeviceInfo deviceInfo) {
                        return deviceInfo.getTime() * 1000;
                    }
                })
                .keyBy(DeviceInfo::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<DeviceInfo, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(DeviceInfo deviceInfo, Tuple2<String, Long> value) {
                        value.f0 = deviceInfo.getName();
                        value.f1 += deviceInfo.getCarNum();
                        return value;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                        return stringLongTuple2;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                        stringLongTuple2.f1 += acc1.f1;
                        return stringLongTuple2;
                    }
                });
        mapStream.print("data");
        resultStream.print("sum");
//
//        // 2 全窗口测试
//        SingleOutputStreamOperator<DeviceInfo> apply = deviceStream.keyBy(devic -> devic.getName())
//                .timeWindow(Time.seconds(5))
//                .apply(new WindowFunction<DeviceInfo, DeviceInfo, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<DeviceInfo> iterable, Collector<DeviceInfo> collector) throws Exception {
//                        AtomicInteger total = new AtomicInteger();
//                        AtomicReference<DeviceInfo> deviceInfo = new AtomicReference<>();
//                        iterable.forEach(dev -> {
//                            total.addAndGet(dev.getCarNum());
//                            deviceInfo.set(dev);
//                        });
//                        if (deviceInfo != null) {
//                            deviceInfo.get().setCarNum(total.get());
//                        }
//
//                        collector.collect(deviceInfo.get());
//                    }
//                });
//        apply.print("apply");
//
        env.execute("device job");
    }
}
