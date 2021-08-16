package com.kedacom.apitest.window;

import com.kedacom.pojo.CarNumCount;
import com.kedacom.pojo.DeviceInfo;
import com.kedacom.pojo.DeviceStatusStt;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.Iterable;

import java.awt.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DeviceInfo> deviceStream = env.addSource(new DeviceSource());
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.getConfig().setAutoWatermarkInterval(100);

//        // 1 开窗测试，按照process时间来开窗
//        SingleOutputStreamOperator<DeviceInfo> dataStream = deviceStream
//                .keyBy(dev -> dev.getName())
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .min("carNum");
//
//        deviceStream.print("data");
//        dataStream.print("window");

        // 2 按照eventTime开窗
//        SingleOutputStreamOperator<DeviceInfo> carNum = deviceStream.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<DeviceInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                        .withTimestampAssigner(new MyTimeAssigner()))
//                .keyBy(dev -> dev.getName())
////                .timeWindow(Time.seconds(10))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .min("carNum");
//
//        deviceStream.print("data");
//        carNum.print("window");

        // 3 自定义函数watermark
        SingleOutputStreamOperator<DeviceInfo> resultStream = deviceStream.assignTimestampsAndWatermarks(
                new MyTimestampAndWatermarks().withTimestampAssigner(new MyTimeAssigner())
        )
                .keyBy(dev -> dev.getName())
                .timeWindow(Time.seconds(60), Time.seconds(10))
//                .aggregate(new CarCountAgg(), new ResultWindow());
                .min("carNum");


        deviceStream.print("data");
        resultStream.print("min");

//        deviceStream.print("data");
//        carNum.print("window");

        // 4 延迟和侧输出流
//        OutputTag<DeviceInfo> outputTag = new OutputTag<>("late");
//        SingleOutputStreamOperator<DeviceInfo> outputStreamOperator = deviceStream.assignTimestampsAndWatermarks(
//                new MyTimestampAndWatermarks().withTimestampAssigner(new MyTimeAssigner()))
//                .keyBy(dev -> dev.getName())
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .allowedLateness(Time.minutes(1))
//                .sideOutputLateData(outputTag)
//                .min("carNum");
//
//        outputStreamOperator.getSideOutput(outputTag).print("output");


        env.execute("device job");
    }

    public static class MyTimeAssigner implements TimestampAssignerSupplier<DeviceInfo> {

        @Override
        public TimestampAssigner<DeviceInfo> createTimestampAssigner(Context context) {
            return new TimestampAssigner<DeviceInfo>() {
                @Override
                public long extractTimestamp(DeviceInfo deviceInfo, long l) {
                    return deviceInfo.getTime() * 1000;
                }
            };
        }
    }

    public static class MyTimestampAndWatermarks implements WatermarkStrategy<DeviceInfo> {

        @Override
        public WatermarkGenerator<DeviceInfo> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<DeviceInfo>() {

                private long delay = 10000;
                private long maxTimestamp = Long.MIN_VALUE + delay + 1L;

                @Override
                // 每条数据都会调用OnEvent，在这个方法里定义maxTimestamp
                public void onEvent(DeviceInfo deviceInfo, long l, WatermarkOutput watermarkOutput) {
                    maxTimestamp = Math.max(maxTimestamp, deviceInfo.getTime() * 1000);
                }

                // 周期性生成watermark
                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay));
                }
            };
        }
    }

    // 自定义聚合
    public static class CarCountAgg implements AggregateFunction<DeviceInfo, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(DeviceInfo deviceInfo, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //  自定义全窗口函数
    public static class ResultWindow implements WindowFunction<Long, CarNumCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, java.lang.Iterable<Long> iterable, Collector<CarNumCount> collector) throws Exception {
            String name = s;
            Long windowEnd = timeWindow.getEnd();
            Integer count = iterable.iterator().next().intValue();

            collector.collect(new CarNumCount(name, count, windowEnd));
        }
    }

    // 自定义processfunciton
    public static class TopNHotCarNum extends KeyedProcessFunction<Tuple, CarNumCount, String> {

        private int topN = 5;

        public TopNHotCarNum(int topN) {
            this.topN = topN;
        }

        // 保持当前窗口类所有输出的CarNumCount
        ListState<CarNumCount> carNumCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            carNumCountListState = getRuntimeContext().getListState(new ListStateDescriptor<CarNumCount>("carnum-count-list", CarNumCount.class));
        }

        @Override
        public void processElement(CarNumCount carNumCount, Context context, Collector<String> collector) throws Exception {
            carNumCountListState.add(carNumCount);

            context.timerService().registerEventTimeTimer(carNumCount.getTimewindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<CarNumCount> countArrayList = Lists.newArrayList(carNumCountListState.get().iterator());

            countArrayList.sort(new Comparator<CarNumCount>() {
                @Override
                public int compare(CarNumCount o1, CarNumCount o2) {
                    if (o1.getTotalCarNum() > o2.getTotalCarNum())
                        return -1;
                    else if (o1.getTotalCarNum() == o2.getTotalCarNum())
                        return 0;
                    else
                        return 1;
                }
            });

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("===================\n");
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            for (int index = 0; index < Math.min(topN, countArrayList.size()); index++) {
                CarNumCount carNumCount = countArrayList.get(index);
                stringBuilder.append("NO ").append(index + 1).append(":\n")
                        .append("name = ").append(carNumCount.getName()).append("\n")
                        .append("totalCatNum = ").append(carNumCount.getTotalCarNum())
                        .append("\n\n");
            }
            stringBuilder.append("==========================\n\n");

            TimeUnit.SECONDS.sleep(1);

            out.collect(stringBuilder.toString());

            carNumCountListState.clear();
        }
    }

    public static class DeviceSource implements SourceFunction<DeviceInfo> {

        private boolean running = true;

        @Override
        public void run(SourceContext<DeviceInfo> sourceContext) throws Exception {
            while (running) {
                Random random = new Random();

                Map<String, Integer> devsMap = new HashMap<>();
                for (int index = 0; index < 4; index++) {
                    if (random.nextInt(5) % 2 == 0) {
                        devsMap.put("device_" + (index + 1), (int) random.nextInt(100));
                    }
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
