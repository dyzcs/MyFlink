package com.dyzcs.apitest.tabletest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

/**
 * Created by Administrator on 2021/2/1.
 */
object TableApiTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val tableEnv = StreamTableEnvironment.create(env)

        /*
        // 1.1 基于老版本planner的流处理
        val settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build()
        val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

        // 1.2 基于老版本的批处理
        val batchEnv = ExecutionEnvironment.getExecutionEnvironment
        val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

        // 1.3 基于blink planner的流处理
        val blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build()
        val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

        // 1.4 基于blink planner的批处理
        val blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build()
        val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
        */

        // 2 连接外部系统，读取数据，注册表
        // 2.1 读取文件
        val filePath = "src/main/resources/sensor.txt"

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable")

        val inputTable = tableEnv.from("inputTable")
        //        inputTable.toAppendStream[(String, Long, Double)].print("input table")
//        inputTable.toAppendStream[SensorReading].print("input table")

        // 2.2 从kafka读取数据
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .property("bootstrap.servers", "s183:9092")
                .property("group.id", "testGroup")
        ).withFormat(new Csv()).withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
        ).createTemporaryTable("kafkaInputTable")

        val kafkaInputTable = tableEnv.from("kafkaInputTable")
        kafkaInputTable.toAppendStream[(String, Long, Double)].print("kafka input table")

        env.execute("table api test")
    }
}
