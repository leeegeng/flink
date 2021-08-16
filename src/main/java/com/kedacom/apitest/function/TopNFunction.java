package com.kedacom.apitest.function;

import com.kedacom.pojo.CarNumCount;
import com.kedacom.pojo.DeviceInfo;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * topN 过车数据排名
 * 思路：1 aggregate聚合函数，采用两个参数：聚合和开窗，对窗口内数据进行统计
 * 2 然后再按窗口结束时间keyby，对过车数据按窗口结束时间排序
 */
public class TopNFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 文件转流
        URL resource = TopNFunction.class.getResource("/deviceinfo.txt");
        //String file = "E:\\practice\\flink-test\\src\\main\\resources\\deviceinfo.txt";
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 转换pojo
        DataStream<DeviceInfo> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new DeviceInfo(fields[0], new Integer(fields[1]), new Integer(fields[2]), new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DeviceInfo>(){

            @Override
            public long extractAscendingTimestamp(DeviceInfo deviceInfo) {
                return deviceInfo.getTime() * 1000;
            }
        });

        // 自定义keyed state，统计不同name个数，窗口聚合结果
        SingleOutputStreamOperator<CarNumCount> windowAggStream = dataStream.keyBy(DeviceInfo::getName)
                .timeWindow(Time.seconds(10))
                .aggregate(new CarCountAgg(), new ResultWindow());

        // 收集统一窗口所有设备过车数，输出topN
        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy(CarNumCount::getTimewindowEnd)
                .process(new TopNHotCarNum(5));

        resultStream.print();

        env.execute();
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
    public static class TopNHotCarNum extends KeyedProcessFunction<Long, CarNumCount, String> {

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
}
