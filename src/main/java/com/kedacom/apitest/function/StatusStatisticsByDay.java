package com.kedacom.apitest.function;

import com.kedacom.pojo.DeviceInfo;
import com.kedacom.pojo.DeviceStatusStt;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

public class StatusStatisticsByDay {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        // 文件转流
        URL resource = TopNFunction.class.getResource("/deviceinfo.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        // 转换pojo
        DataStream<DeviceInfo> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new DeviceInfo(fields[0], new Integer(fields[1]), new Integer(fields[2]), new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DeviceInfo>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(DeviceInfo deviceInfo) {
                return deviceInfo.getTime() * 1000;
            }
        });

        // 自定义keyed state，统计不同name个数，窗口聚合结果
        SingleOutputStreamOperator<Tuple2<String, DeviceStatusStt>> resultStrem = dataStream
                .keyBy(DeviceInfo::getName)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .process(new ProcessDayStt());

        resultStrem.print("daystt");

        env.execute("day statistics job");
    }

    public static class ProcessDayStt extends ProcessWindowFunction<DeviceInfo, Tuple2<String, DeviceStatusStt>, String, TimeWindow> {

        private static MapState<Long, DeviceStatusStt> daySttMap;   // 保持每一天统计数据，可用来做周、月统计。。。
        private static ValueState<DeviceStatusStt> dayStt;


        @Override
        public void open(Configuration parameters) throws Exception {
            daySttMap = getRuntimeContext().getMapState(new MapStateDescriptor<Long, DeviceStatusStt>("day-status-stt",
                    Long.class, DeviceStatusStt.class));

            dayStt = getRuntimeContext().getState(new ValueStateDescriptor<DeviceStatusStt>("day-status-latest-stt", DeviceStatusStt.class));
        }

        @Override
        public void process(String s, Context context, Iterable<DeviceInfo> iterable, Collector<Tuple2<String, DeviceStatusStt>> collector) throws Exception {
            DeviceStatusStt latestDayStt = null;
            DeviceStatusStt curDayStt = new DeviceStatusStt();

            boolean firstDayStt = false;

            // 获取统计时间，以窗口开始时间作为统计时间
            long beginTime = context.window().getStart();
            long countTime = context.window().getEnd();

            if(dayStt.value() == null){
                // 第一次状态填充
                firstDayStt = true;
            }else{
                // 赋值前一天日统计数据
                latestDayStt = dayStt.value();
            }

            // 先对集合按照时间升序排序
            ArrayList<DeviceInfo> deviceInfos = Lists.newArrayList(iterable);

            deviceInfos.sort(new Comparator<DeviceInfo>() {
                @Override
                public int compare(DeviceInfo o1, DeviceInfo o2) {
                    if (o1.getTime() > o2.getTime())
                        return 1;
                    else if (o1.getTime() == o2.getTime())
                        return 0;
                    else
                        return -1;
                }
            });

            for(int index = 0; index < deviceInfos.size(); index++){
                if(0 == index){
                    // 第一个状态，且为第一天统计
                    if(firstDayStt){
                        curDayStt.setContinueDurationTime(0);// 持续时间设置为0
                    }else {
                        // 需要和上次状态比较，计算持续时长
                        if(latestDayStt.getStatus() == deviceInfos.get(index).getStatus()){
                            // 持续时长为当前事件时间-上次时间 + 上次持续时长
                            curDayStt.setContinueDurationTime(deviceInfos.get(index).getTime() -
                                    beginTime/1000 + latestDayStt.getContinueDurationTime());
                        }else{
                            // 否则状态改变，设置时间为0
                            curDayStt.setContinueDurationTime(0);
                        }
                    }
                    curDayStt.setTotalCarNum(deviceInfos.get(index).getCarNum());// 过车总数设置为第一条数据过车数据
                }else{
                    // 和当前日统计上一次数据比较
                    if(curDayStt.getStatus() == deviceInfos.get(index).getStatus()){
                        // 状态不变，持续时长增加
                        curDayStt.setContinueDurationTime(deviceInfos.get(index).getTime() -
                                curDayStt.getTime() + curDayStt.getContinueDurationTime());
                    }else{
                        // 状态改变，持续时长为0
                        curDayStt.setContinueDurationTime(0);
                    }
                    // 过车统计
                    curDayStt.setTotalCarNum(curDayStt.getTotalCarNum() + deviceInfos.get(index).getCarNum());
                }
                // 赋值其它属性
                curDayStt.setName(deviceInfos.get(index).getName());
                curDayStt.setTime(deviceInfos.get(index).getTime());
                curDayStt.setStatus(deviceInfos.get(index).getStatus());
            }
            // 日统计，截止时间要到当天24点，需要计算最后一条记录和24点之间的状态
            if(deviceInfos.get(deviceInfos.size()-1).getTime() < countTime/1000){
                curDayStt.setContinueDurationTime(countTime/1000 - deviceInfos.get(deviceInfos.size()-1).getTime() + curDayStt.getContinueDurationTime());
            }

            // 输出集合
            collector.collect(new Tuple2<>(new Timestamp(countTime).toString(), curDayStt));
            // 更新状态数据
            dayStt.update(curDayStt);
        }
    }

}
