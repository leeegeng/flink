package com.kedacom.apitest.state;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

import java.util.Collections;
import java.util.List;

/**
 * operate state 测试
 */
public class OpetateStateTest {
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


        // 自定义mapper
        SingleOutputStreamOperator<Integer> countMap = dataStream
                .keyBy(DeviceInfo::getName)
                .map(new CountMapper());

        countMap.print();

        env.execute();
    }

    public static class MyCountMapper extends RichMapFunction<DeviceInfo, Tuple2<String, Integer>>{

        ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count-value", Integer.class, 0));
        }

        @Override
        public Tuple2<String, Integer> map(DeviceInfo deviceInfo) throws Exception {
            int count = countState.value();
            count++;
            countState.update(count);
            return new Tuple2<>(deviceInfo.getName(), count);
        }
    }

    // 聚合求和，采用ListCheckpointed
    public static class CountMapper implements MapFunction<DeviceInfo, Integer>, ListCheckpointed<Integer> {

        private Integer count = 0;

        @Override
        public Integer map(DeviceInfo deviceInfo) throws Exception {
            return count++;
        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for(Integer num : list){
                count += num;
            }
        }
    }
}
