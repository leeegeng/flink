package com.kedacom.apitest.tableapi.udf;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfScalarFunctionTest {
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

        // 自定义标量函数，实现name的hash值
        HashCode hashCode = new HashCode(5);
        // table Api
        tableEnv.registerFunction("hashCode", hashCode);
        Table resultTable = dataTable.select("name, carNum, hashCode(name), ts");

        // sql
        // 注册表
        tableEnv.createTemporaryView("device", dataTable);
        Table sqlResult = tableEnv.sqlQuery("select name, carNum, hashCode(name), ts from device");

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        env.execute();

    }

    // 自定义scalar function
    public static class HashCode extends ScalarFunction{
        private int factor = 3;

        public HashCode(int factor) {
            this.factor = factor;
        }

        // 必须实现 eval 函数
        public int eval(String name){
            return name.hashCode() * factor;
        }
    }
}
