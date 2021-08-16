package com.kedacom.apitest.tableapi.udf;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.codegen.GeneratedAggregationsFunction;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.types.Row;

/**
 * sql udf 测试
 */
public class UdfAggregateFunctionTest {
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

        // 创建表环境，定义时间特性
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "name, status, carNum, time as ts, pt.proctime");

        // 自定义聚合函数，求carNum平均值
        AverageCarNum averageCarNum = new AverageCarNum();
        // table Api
        tableEnv.registerFunction("avgCarNum", averageCarNum);
        Table resultTable = dataTable
                .groupBy("name")
                .aggregate("avgCarNum(carNum) as avgNum")
                .select("name, avgNum");

        // sql
        // 注册表
        tableEnv.createTemporaryView("device", dataTable);
        Table sqlResult = tableEnv.sqlQuery("select name, avgCarNum(carNum) from device group by name");

        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sql");

        env.execute();

    }

    public static class AverageCarNum extends AggregateFunction<Double, Tuple2<Double, Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> agg) {
            return agg.f0 / agg.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现accumulate方法，来数据后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Integer tmp){
            accumulator.f0 += tmp;
            accumulator.f1 ++;
        }
    }
}
