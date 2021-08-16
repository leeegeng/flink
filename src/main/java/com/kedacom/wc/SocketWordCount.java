package com.kedacom.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

@Slf4j
// 提交参数
// com.kedacom.wc.StreamWordCount
// --host 172.16.64.85 --port 9999
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        System.out.println("test");
        log.info("socket wrod count test!");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1 设置socket信息
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
//        String host = "172.16.64.85";
//        int port = 9999;


        // 2 从socket读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new MyFlatMap())
                .keyBy(0)
                .sum(1);

        sum.print("sum");

        env.execute("socket stream word count job");
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
