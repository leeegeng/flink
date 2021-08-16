package com.kedacom.apitest.tableapi.udf;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTableFunctionTest {
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

        // 将name拆分，拆成名字+数字
        SplitName splitName = new SplitName("_");
        // table Api
        tableEnv.registerFunction("splitName", splitName);
        Table resultTable = dataTable
                .joinLateral("splitName(name) as (word, length)")
                .select("name, carNum, word, length");

        // sql
        // 注册表
        tableEnv.createTemporaryView("device", dataTable);
        Table sqlResult = tableEnv.sqlQuery("select name, carNum, word, length, ts from device " +
                ", lateral table(splitName(name)) as split(word, length)");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        env.execute();

    }

    // 自定义table function
    public static class SplitName extends TableFunction<Tuple2<String, Integer>> {

        private String seprator = "_";

        public SplitName(String seprator) {
            this.seprator = seprator;
        }

        public void eval(String name) {
            String[] fields = name.split(seprator);
            for (String str : fields) {
                collect(new Tuple2<String, Integer>(str, str.length()));
            }
        }
    }
}
