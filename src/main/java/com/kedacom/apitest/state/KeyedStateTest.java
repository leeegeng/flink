package com.kedacom.apitest.state;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateTest {
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
        dataStream.keyBy("name")
                .map(new MyKeyedState()).print();


        env.execute();
    }

    public static class MyKeyedState extends RichMapFunction<DeviceInfo, Tuple2<String, Integer>> {

        private ValueState<Integer> keyCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
        }

        @Override
        public void close() throws Exception {
            keyCountState.clear();
        }

        @Override
        public Tuple2<String, Integer> map(DeviceInfo deviceInfo) throws Exception {
            Integer count = keyCountState.value();
            if(count == null){
                count = 0;
            }
            count++;
            keyCountState.update(count);

            return new Tuple2<>(deviceInfo.getName(), count);
        }
    }

}
