package com.kedacom.apitest.source;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Array;
import java.util.Arrays;

public class DeviceInfoReading {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DeviceInfo> inputDevInfoStream = env.fromCollection(Arrays.asList(new DeviceInfo("dev1", 0, 10, 1625819869),
                new DeviceInfo("dev2", 1, 20, 1625819869),
                new DeviceInfo("dev3", 0, 40, 1625819869),
                new DeviceInfo("dev4", 1, 2, 1625819869),
                new DeviceInfo("dev5", 1, 5, 1625819869)));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 10, 100, 250, 500);


        inputDevInfoStream.print("device");

        integerDataStream.print("int").setParallelism(1);

        env.execute("device job");
    }
}
