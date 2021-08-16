package com.kedacom.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * DataSet
 * 批处理文件
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        System.out.println("test");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String file = "E:\\practice\\flink-test\\src\\main\\resources\\hello.txt";
        DataSet<String> data = env.readTextFile(file);

        DataSet<Tuple2<String, Integer>> out = data
                .flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);

        out.print();
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
