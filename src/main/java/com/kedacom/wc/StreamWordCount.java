package com.kedacom.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 提交参数
// com.kedacom.wc.StreamWordCount
// --host 172.16.64.85 --port 9999
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        System.out.println("test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1 用文件模拟流数据
        String file = "E:\\practice\\flink-test\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(file);

        DataStream<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new MyFlatMap())
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String words[] = s.split(" ");
            for(String word : words){
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
