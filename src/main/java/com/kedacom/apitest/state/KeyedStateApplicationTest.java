package com.kedacom.apitest.state;

import com.kedacom.pojo.DeviceInfo;
import com.kedacom.pojo.DeviceStatusStt;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

/**
 * keyed state测试
 */
public class KeyedStateApplicationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 文件转流

        String file = "E:\\practice\\flink-test\\src\\main\\resources\\deviceinfo.txt";
        DataStreamSource<String> inputStream = env.readTextFile(file);

        // 转换pojo
        DataStream<DeviceInfo> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new DeviceInfo(fields[0], new Integer(fields[1]), new Integer(fields[2]), new Long(fields[3]));
        });

        // 自定义keyed state，统计不同name个数
        dataStream.keyBy(DeviceInfo::getName)
                .flatMap(new StatusSttFunction()).print();


        env.execute();
    }

    public static class StatusSttFunction extends RichFlatMapFunction<DeviceInfo, DeviceStatusStt>{

        private ValueState<DeviceStatusStt> deviceStatusSttValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            deviceStatusSttValueState = getRuntimeContext().getState(new ValueStateDescriptor<DeviceStatusStt>("status-stt", DeviceStatusStt.class));
        }

//        @Override
//        public void close() throws Exception {
//            deviceStatusSttValueState.clear();
//        }

        // 实时计算设备状态，持续时长、过车统计
        @Override
        public void flatMap(DeviceInfo deviceInfo, Collector<DeviceStatusStt> collector) throws Exception {
            DeviceStatusStt devStatusStt = deviceStatusSttValueState.value();
            if(devStatusStt == null){
                devStatusStt = new DeviceStatusStt();
                devStatusStt.setName(deviceInfo.getName());
                devStatusStt.setStatus(deviceInfo.getStatus());
                devStatusStt.setTotalCarNum(deviceInfo.getCarNum());
                devStatusStt.setContinueDurationTime(0);
                devStatusStt.setTime(deviceInfo.getTime());
            }else{
                if(devStatusStt.getTime() >= deviceInfo.getTime()){// 乱序数据，丢弃
                    log.println("time error!!");
                    return;
                }
                if(devStatusStt.getStatus() == deviceInfo.getStatus()){
                    // 状态相同，去重，持续时长相加
                    devStatusStt.setContinueDurationTime(devStatusStt.getContinueDurationTime() + deviceInfo.getTime() - devStatusStt.getTime());
                }else{
                    // 状态不同，改变状态，重置持续时长
                    devStatusStt.setContinueDurationTime(deviceInfo.getTime() - devStatusStt.getTime());
                    devStatusStt.setStatus(deviceInfo.getStatus());
                }
                // 过车总数增加
                devStatusStt.setTotalCarNum(devStatusStt.getTotalCarNum() + deviceInfo.getCarNum());
                // 时间设置为最新
                devStatusStt.setTime(deviceInfo.getTime());
            }
            deviceStatusSttValueState.update(devStatusStt);
            collector.collect(devStatusStt);
        }
    }
}
