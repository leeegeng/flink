package com.kedacom.apitest.tableapi;

import com.kedacom.pojo.DeviceInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1 {
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
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DeviceInfo>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(DeviceInfo deviceInfo) {
                return deviceInfo.getTime() * 1000;
            }
        });

        // 创建表环境，定义时间特性
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 采用pt.proctime
//        Table dataTable = tableEnv.fromDataStream(dataStream, "name, status, carNum, time as ts, pt.proctime");

        // 定义schema，采用rt作为rowtime
        Table dataTable = tableEnv.fromDataStream(dataStream, "name, status, carNum, time as ts, rt.rowtime");

        tableEnv.createTemporaryView("device", dataTable);

        // 窗口操作
        // group window
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("name, tw")
                .select("name, carNum.count, carNum.avg, tw.end");// 此处只能选择聚合字段
//
//        // sql
        Table resultQuery = tableEnv.sqlQuery("select name, count(name), avg(carNum) as avtCarNum, tumble_end(rt, interval '10' second) from" +
                " device group by name, tumble(rt, interval '10' second)");

        // over window
        Table overResult = dataTable.window(Over.partitionBy("name").orderBy("rt").preceding("2.rows").as("ow"))
                .select("name, carNum.count over ow, carNum.avg over ow");

        Table overSql = tableEnv.sqlQuery("select name, rt, count(name) over ow, avg(carNum) over ow " +
                "from device " +
                " window ow as (partition by name order by rt rows between 2 preceding and current row)");


        dataTable.printSchema();
        tableEnv.toAppendStream(dataTable, Row.class).print("stream");
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(resultQuery, Row.class).print("query");
        tableEnv.toRetractStream(overResult, Row.class).print("overResult");
        tableEnv.toRetractStream(overSql, Row.class).print("oversql");

        env.execute();
    }
}
