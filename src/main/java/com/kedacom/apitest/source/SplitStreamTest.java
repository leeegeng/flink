package com.kedacom.apitest.source;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 分流测试
 */
public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<DeviceInfo> device = env.addSource(new DeviceSource());

        final OutputTag<DeviceInfo> onlineTag = new OutputTag<>("online", TypeInformation.of(DeviceInfo.class));
        final OutputTag<DeviceInfo> offlineTag = new OutputTag<>("offline", TypeInformation.of(DeviceInfo.class));

        // 1 分流，online、offline
        SingleOutputStreamOperator<DeviceInfo> process = device.process(new ProcessFunction<DeviceInfo, DeviceInfo>() {
            @Override
            public void processElement(DeviceInfo deviceInfo, Context context, Collector<DeviceInfo> collector) throws Exception {
                if (deviceInfo.getStatus() == 1) {
                    context.output(onlineTag, deviceInfo);
                } else {
                    context.output(offlineTag, deviceInfo);
                }
            }
        });

        process.getSideOutput(onlineTag).print("online");
        process.getSideOutput(offlineTag).print("offline");


        // 2 合流 connect，将在线流转换成元组
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> onlineStream = process.getSideOutput(onlineTag).map(new MapFunction<DeviceInfo, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(DeviceInfo deviceInfo) throws Exception {
                return new Tuple3<>(deviceInfo.getName(), deviceInfo.getCarNum(), deviceInfo.getTime());
            }
        });

        ConnectedStreams<Tuple3<String, Integer, Long>, DeviceInfo> connect = onlineStream.connect(process.getSideOutput(offlineTag));
        SingleOutputStreamOperator<Object> coMap = connect.map(new CoMapFunction<Tuple3<String, Integer, Long>, DeviceInfo, Object>() {
            @Override
            public Object map1(Tuple3<String, Integer, Long> value) throws Exception {
                return new Tuple3<>(value.f0, value.f2, "onlineItem");
            }

            @Override
            public Object map2(DeviceInfo deviceInfo) throws Exception {
                return new Tuple2<>(deviceInfo.getName(), "offlineItme");
            }
        });

        coMap.print("coMap");

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
